#[path="a2a.rs"]
mod a2a;
use crate::host::GatewayHost;
use std::collections::HashMap;
use std::convert::Infallible;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use axum::extract::ws::{Message as WebSocketMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive};
use axum::response::{IntoResponse, Response, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::stream::{self, Stream};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinSet;

use agent_daemon_client::{DaemonClient, RpcStream};
use agent_daemon_protocol::{
    EventKind, JsonRpcRequest, JsonRpcResponse, MAX_FRAME_BYTES, RequestId, ServerFrame,
    decode_request,
};
use agent_entry_support::recovery;

struct QueuedWebSocketFrame {
    message: WebSocketMessage,
    bytes: usize,
}
#[derive(Clone)]
struct WebSocketOutgoing {
    sender: mpsc::Sender<QueuedWebSocketFrame>,
    bytes: Arc<std::sync::atomic::AtomicUsize>,
    detached: watch::Sender<bool>,
}
impl WebSocketOutgoing {
    fn send(&self, message: WebSocketMessage) -> std::result::Result<(), ()> {
        use std::sync::atomic::Ordering;
        let size = match &message {
            WebSocketMessage::Text(v) => v.len(),
            WebSocketMessage::Binary(v) | WebSocketMessage::Ping(v) | WebSocketMessage::Pong(v) => {
                v.len()
            }
            WebSocketMessage::Close(_) => 128,
        };
        if *self.detached.borrow()
            || size > MAX_FRAME_BYTES
            || self
                .bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                    n.checked_add(size).filter(|n| *n <= 8 * 1024 * 1024)
                })
                .is_err()
        {
            self.detached.send_replace(true);
            return Err(());
        }
        if self
            .sender
            .try_send(QueuedWebSocketFrame {
                message,
                bytes: size,
            })
            .is_err()
        {
            self.bytes.fetch_sub(size, Ordering::AcqRel);
            self.detached.send_replace(true);
            return Err(());
        }
        Ok(())
    }
}

#[derive(Clone)]
struct ApiState {
    a2a_url:Option<String>,
    workspaces: WorkspaceRouter,
    model: String,
    bearer_token: Option<String>,
    workspace: PathBuf,
    oidc: Option<Arc<agent_identity::OidcVerifier>>,
    connections: Arc<tokio::sync::Semaphore>,
    authentications: Arc<tokio::sync::Semaphore>,
    gateway_token_path: Option<PathBuf>,
    rate_limiter: Arc<Mutex<crate::rate_limit::RateLimiter>>,
    runtime_routes: Option<PathBuf>,
    scope_routes:Arc<std::collections::BTreeMap<String,Arc<crate::scope_routes::ScopeRoute>>>,
    selected_profile:Option<String>,
}

#[derive(Clone)]
struct ScopedGateway(ApiState);
struct ScopeState(ApiState);
impl axum::extract::FromRequestParts<ApiState> for ScopeState {
    type Rejection=Infallible;
    async fn from_request_parts(parts:&mut axum::http::request::Parts,state:&ApiState)->std::result::Result<Self,Self::Rejection> {
        Ok(Self(parts.extensions.get::<ScopedGateway>().map_or_else(||state.clone(),|s|s.0.clone())))
    }
}

tokio::task_local! {static HTTP_CREDENTIAL:String;}

#[derive(Clone)]
struct WorkspaceRouter {
    default_workspace: PathBuf,
    clients: Arc<Mutex<HashMap<PathBuf, DaemonClient>>>,
    host: Arc<dyn GatewayHost>,
}

impl ApiState {
    fn bound_to(&self,route:&crate::scope_routes::ScopeRoute)->Self {
        let mut state=self.clone();state.workspace=route.workspace.clone();state.workspaces.default_workspace=route.workspace.clone();
        state.oidc=Some(route.verifier.clone());state.gateway_token_path=Some(route.gateway_token.clone());state.runtime_routes=route.runtime_routes.clone();state.selected_profile=Some(route.name.clone());state
    }

    async fn route_table(&self) -> Result<std::collections::BTreeMap<String,PathBuf>> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Routes {endpoints: std::collections::BTreeMap<String,PathBuf>}
        let mut endpoints=if let Some(path)=&self.runtime_routes {
            let path=path.clone();
            let routes:Routes=tokio::task::spawn_blocking(move||agent_identity::oidc::read_protected_json(&path)).await?
                .map_err(|_|anyhow::anyhow!("端点路由配置无效"))?;
            routes.endpoints
        } else {std::collections::BTreeMap::new()};
        let default=self.oidc.as_ref().map_or("runtime-default",|v|v.runtime_endpoint());
        endpoints.entry(default.into()).or_insert_with(||self.workspace.clone());
        anyhow::ensure!(endpoints.len()<=16 && endpoints.iter().all(|(key,path)|!key.is_empty() && key.len()<=128 && key.bytes().all(|v|v.is_ascii_alphanumeric() || matches!(v,b'-'|b'_'|b'.')) && path.is_absolute()),"端点路由超过预算或无效");
        Ok(endpoints)
    }
    async fn routed_client(&self, endpoint: &str, token: &str, channel:agent_core::HookChannel) -> Result<DaemonClient> {
        let routes=self.route_table().await?;
        let workspace=routes.get(endpoint).context("运行端点未配置")?;
        let (_,client)=self.workspaces.client_for(Some(workspace)).await?;
        let client=self.gateway_client(client.with_bearer_token(token.into())?,channel).await?;
        let binding=client.request_result("runtime.endpoint",json!({})).await?;
        let scope=self.oidc.as_ref().context("企业身份缺失")?.scope();
        anyhow::ensure!(binding["scope"]==json!(scope) && binding["runtime_endpoint"]==json!(endpoint),"运行端点可信绑定不匹配");
        Ok(client)
    }
    async fn read_gateway_token(&self) -> Result<String> {
        let path = self.gateway_token_path.clone().context("Gateway凭据配置缺失")?;
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct TokenFile { token: String }
        let token: TokenFile = tokio::task::spawn_blocking(move || agent_identity::oidc::read_protected_json(&path)).await?
            .map_err(|_|anyhow::anyhow!("Gateway凭据不可读取"))?;
        Ok(token.token)
    }
    async fn service_client(&self) -> Result<DaemonClient> {
        let (_, client) = self.workspaces.client_for(None).await?;
        let token = self.read_gateway_token().await?;
        Ok(client.with_bearer_token(token.clone())?.with_gateway_token(token, agent_core::HookChannel::Http)?)
    }
    async fn gateway_client(
        &self,
        client: DaemonClient,
        channel: agent_core::HookChannel,
    ) -> Result<DaemonClient> {
        if self.oidc.is_none() {
            return Ok(client);
        }
        Ok(client.with_gateway_token(self.read_gateway_token().await?, channel)?)
    }
    async fn verify_bearer(
        &self,
        token: &str,
    ) -> std::result::Result<agent_identity::VerifiedToken, ()> {
        let _permit = self
            .authentications
            .clone()
            .try_acquire_owned()
            .map_err(|_| ())?;
        let verifier = self.oidc.as_ref().ok_or(())?;
        if token.starts_with("ak.") {
            let (_, client) = self.workspaces.client_for(None).await.map_err(|_| ())?;
            let client = client.with_bearer_token(token.into()).map_err(|_| ())?;
            let client = self
                .gateway_client(client, agent_core::HookChannel::Http)
                .await
                .map_err(|_| ())?;
            let value = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                client.request_result("credential.status", json!({})),
            )
            .await
            .map_err(|_| ())?
            .map_err(|_| ())?;
            let verified: agent_identity::VerifiedToken =
                serde_json::from_value(value).map_err(|_| ())?;
            if verified.identity.scope != *verifier.scope() || verified.expires_at <= unix_time() {
                return Err(());
            }
            return Ok(verified);
        }
        verifier.verify(token).await.map_err(|_| ())
    }
    fn active_model(&self) -> String {
        self.workspaces.host.active_model(&self.model)
    }
    async fn client_for(&self, requested: Option<&Path>) -> Result<(PathBuf, DaemonClient)> {
        if self.oidc.is_some() && requested.is_some_and(|p| p != self.workspace) {
            anyhow::bail!("企业入口不可选择宿主目录")
        }
        let (path, client) = self.workspaces.client_for(requested).await?;
        if self.oidc.is_some() {
            let token = HTTP_CREDENTIAL
                .try_with(Clone::clone)
                .map_err(|_| anyhow::anyhow!("认证上下文缺失"))?;
            return Ok((
                path,
                self.gateway_client(
                    client.with_bearer_token(token)?,
                    agent_core::HookChannel::Http,
                )
                .await?,
            ));
        }
        Ok((path, client))
    }
}

impl WorkspaceRouter {
    #[cfg(test)]
    fn new(default_workspace: PathBuf, default_client: DaemonClient) -> Self {
        Self::with_optional(
            default_workspace,
            Some(default_client.clone()),
            Arc::new(TestHost(default_client)),
        )
    }

    fn with_optional(
        default_workspace: PathBuf,
        default_client: Option<DaemonClient>,
        host: Arc<dyn GatewayHost>,
    ) -> Self {
        let mut clients = HashMap::new();
        if let Some(default_client) = default_client {
            clients.insert(default_workspace.clone(), default_client);
        }
        Self {
            default_workspace,
            host,
            clients: Arc::new(Mutex::new(clients)),
        }
    }

    async fn client_for(&self, requested: Option<&Path>) -> Result<(PathBuf, DaemonClient)> {
        let requested = requested.unwrap_or(&self.default_workspace);
        let candidate = if requested.is_absolute() {
            requested.to_path_buf()
        } else {
            self.default_workspace.join(requested)
        };
        let workspace = canonical_directory(&candidate)?;
        let cached = self.clients.lock().await.get(&workspace).cloned();
        if let Some(client) = cached.filter(|client|client.transport_alive()) {
            return Ok((workspace, client.protocol_adapter()));
        }

        self.clients.lock().await.remove(&workspace);
        {
            let mut clients=self.clients.lock().await;
            clients.retain(|_,client|client.transport_alive());
            anyhow::ensure!(clients.len()<256,"工作区连接缓存预算已耗尽");
        }
        let client = self.host.connect_workspace(&workspace).await?;
        let mut clients=self.clients.lock().await;
        anyhow::ensure!(clients.len()<256 || clients.contains_key(&workspace),"工作区连接缓存预算已耗尽");
        clients.insert(workspace.clone(),client.clone());
        Ok((workspace, client.protocol_adapter()))
    }
}

const WEB_INDEX: &str = include_str!("../../../web/index.html");
const WEB_APP: &str = include_str!("../../../web/app.js");
const WEB_STYLES: &str = include_str!("../../../web/styles.css");

#[derive(Deserialize)]
struct ChatCompletionRequest {
    model: Option<String>,
    messages: Vec<OpenAiMessage>,
    #[serde(default)]
    stream: bool,
}

#[derive(Deserialize)]
struct OpenAiMessage {
    role: String,
    content: Value,
}

#[derive(Deserialize)]
struct WebSocketConnect {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    scope:Option<String>,
    #[serde(default)]
    workspace: Option<PathBuf>,
    #[serde(default)]
    session_id: Option<String>,
}

#[derive(Deserialize)]
struct DirectoryQuery {
    path: Option<PathBuf>,
}

#[derive(Deserialize)]
struct ModelSaveRequest {
    profile: Value,
    #[serde(default)]
    activate: bool,
    #[serde(default)]
    workspace: Option<PathBuf>,
}

#[derive(Deserialize)]
struct ModelUseRequest {
    profile_id: String,
    #[serde(default)]
    workspace: Option<PathBuf>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PlanReadQuery {
    session_id: String,
    workspace: Option<PathBuf>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HookReadQuery {
    session_id: String,
    expected_lifetime: agent_core::SessionLifetimeId,
    workspace: Option<PathBuf>,
    #[serde(default)]
    after_cursor: u64,
    #[serde(default = "hook_read_limit")]
    limit: usize,
}
fn hook_read_limit() -> usize {
    32
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HttpPlanRequest<P> {
    workspace: Option<PathBuf>,
    params: P,
}

#[allow(dead_code)]
pub async fn run_http_server(
    client: DaemonClient,
    address: SocketAddr,
    model: String,
    bearer_token: Option<String>,
    workspace: PathBuf,
    host: Arc<dyn GatewayHost>,
) -> Result<()> {
    run_http_server_optional(Some(client), address, model, bearer_token, workspace, host).await
}

pub async fn run_http_server_optional(
    client: Option<DaemonClient>,
    address: SocketAddr,
    model: String,
    bearer_token: Option<String>,
    workspace: PathBuf,
    host: Arc<dyn GatewayHost>,
) -> Result<()> {
    let oidc = agent_identity::OidcVerifier::from_environment()?.map(Arc::new);
    let gateway_token_path = std::env::var_os("AGENT_GATEWAY_TOKEN").map(PathBuf::from);
    anyhow::ensure!(
        oidc.as_ref().is_none_or(|v| v.gateway_subject().is_some()
            && gateway_token_path.as_ref().is_some_and(|p| p.is_absolute())),
        "企业Gateway需要固定gateway_subject及受保护Gateway凭据文件"
    );
    anyhow::ensure!(
        oidc.is_none() || bearer_token.is_none(),
        "OIDC模式不能与Local静态bearer混用"
    );
    let runtime_routes=std::env::var_os("AGENT_GATEWAY_ROUTES").map(PathBuf::from);
    let default_scope=match &oidc {Some(v)=>Some(crate::scope_routes::ScopeRoute{name:"default".into(),workspace:workspace.clone(),verifier:v.clone(),gateway_token:gateway_token_path.clone().context("Gateway凭据配置缺失")?,runtime_routes:runtime_routes.clone()}),None=>None};
    let scope_routes=crate::scope_routes::load(default_scope).await?;
    let workspaces = WorkspaceRouter::with_optional(workspace.clone(), client, host);
    let a2a_url=std::env::var("AGENT_A2A_PUBLIC_URL").ok().or_else(||(oidc.is_none()&&address.ip().is_loopback()&&address.port()!=0).then(||format!("http://{address}/a2a")));
    if let Some(url)=&a2a_url {let uri:axum::http::Uri=url.parse().context("A2A公开URL无效")?;anyhow::ensure!(url.len()<=2048&&uri.authority().is_some_and(|a|!a.as_str().contains('@'))&&uri.path()=="/a2a"&&uri.query().is_none()&&!url.contains('#')&&(uri.scheme_str()==Some("https")||(oidc.is_none()&&uri.scheme_str()==Some("http")&&matches!(uri.host(),Some("127.0.0.1"|"localhost"|"[::1]")))),"A2A公开URL边界无效");}
    let state = ApiState {
        a2a_url,
        workspaces,
        model,
        bearer_token,
        workspace,
        oidc,
        connections: Arc::new(tokio::sync::Semaphore::new(256)),
        authentications: Arc::new(tokio::sync::Semaphore::new(32)),
        gateway_token_path,
        rate_limiter: Arc::new(Mutex::new(crate::rate_limit::RateLimiter::default())),
        runtime_routes,
        scope_routes,
        selected_profile:None,
    };
    let app = api_router(state);
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .with_context(|| format!("监听本地 API 失败: {address}"))?;
    let address = listener.local_addr().context("读取本地 API 监听地址失败")?;
    writeln!(
        std::io::stdout().lock(),
        "my-agent 本地 API 正在监听 http://{address}"
    )?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .context("本地 API 服务异常退出")
}

fn api_router(state: ApiState) -> Router {
    Router::new()
        .route("/", get(web_index))
        .route("/app.js", get(web_app))
        .route("/styles.css", get(web_styles))
        .route("/health", get(health))
        .route("/api/directories", get(list_directories))
        .route("/api/identity", get(identity_readback))
        .route("/api/runtime/doctor", get(runtime_doctor))
        .route("/api/outbox", get(outbox_readback))
        .route("/api/artifacts/read", post(artifact_readback))
        .route("/api/sessions/read", post(session_readback))
        .route("/api/runs/events", post(run_events_readback))
        .route("/api/runs/read", post(run_fact_readback))
        .route("/api/runs/tools", post(run_tools_readback))
        .route("/api/runs/provider", post(run_provider_readback))
        .route("/api/models", get(list_models).post(save_models))
        .route("/api/models/activate", post(activate_model))
        .route("/api/plans/readback", get(plan_readback))
        .route("/api/hooks/readback", get(hook_readback))
        .route("/api/context/readback", post(context_readback))
        .route("/api/compact/start", post(compact_start))
        .route("/api/plans/discard", post(plan_discard))
        .route("/api/plans/execute", post(plan_execute))
        .route("/v1/chat/completions", post(chat_completions))
        .route("/ws", get(websocket_upgrade))
        .route("/enterprise/sessions", post(directory_create))
        .route("/enterprise/rpc", post(directory_rpc))
        .route("/a2a",post(a2a::rpc))
        .route("/.well-known/agent-card.json",get(a2a::card))
        .route("/enterprise/workflows",post(workflow_command))
        .route("/enterprise/workflows/read",get(workflow_read))
        .route("/enterprise/workflows/control",post(workflow_control))
        .route("/enterprise/registry",get(registry_readback).post(registry_mutate))
        .route("/enterprise/registry/generation",get(registry_generation))
        .route("/enterprise/billing/configure",post(billing_configure))
        .route("/enterprise/billing",get(billing_readback))
        .route("/enterprise/billing/accounts",get(billing_accounts))
        .route("/enterprise/billing/usage",get(billing_usage))
        .route("/auth/login", get(oauth_login))
        .route("/auth/callback", get(oauth_callback))
        .route("/openapi.json", get(|| async {(
            [(axum::http::header::CONTENT_TYPE,"application/json")],
            include_str!("openapi.json"),
        )}))
        .route("/enterprise/credentials/issue", post(credential_issue))
        .route("/enterprise/credentials/revoke", post(credential_revoke))
        .layer(axum::extract::DefaultBodyLimit::max(MAX_FRAME_BYTES))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            enterprise_http,
        ))
        .with_state(state)
}

async fn enterprise_http(
    State(state): State<ApiState>,
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if state.oidc.is_none(){return next.run(request).await}
    let alias=request.headers().get("x-agent-scope").and_then(|v|v.to_str().ok()).map(str::to_owned);
    let alias=if matches!(request.uri().path(),"/auth/login"|"/auth/callback") {
        match Query::<crate::scope_routes::ScopeHint>::try_from_uri(request.uri()) {Ok(query)=>query.0.scope.or(alias),Err(_)=>return api_error(StatusCode::BAD_REQUEST,"scope提示无效")}
    }else{alias};
    let token=request.headers().get(axum::http::header::AUTHORIZATION).and_then(|v|v.to_str().ok()).and_then(|v|v.strip_prefix("Bearer "));
    let route=match crate::scope_routes::select(&state.scope_routes,token,alias.as_deref()) {Ok(route)=>route,Err(_)=>return api_error(StatusCode::UNAUTHORIZED,"身份认证失败")};
    let mut state=state.bound_to(&route);
    // WS身份在首帧到达；只有显式HTTP别名才能约束首帧选路。
    if request.uri().path()=="/ws" && alias.is_none() {state.selected_profile=None;}
    request.extensions_mut().insert(ScopedGateway(state.clone()));
    enterprise_http_bound(state,request,next).await
}
async fn enterprise_http_bound(
    state:ApiState,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let Some(_verifier) = &state.oidc else {
        return next.run(request).await;
    };
    let path = request.uri().path();
    // WebSocket首帧认证；只开放原 canonical RPC 投影，宿主目录/配置浏览不暴露。
    if path == "/ws" {
        return next.run(request).await;
    }
    if matches!(path,"/auth/login"|"/auth/callback"|"/openapi.json") {
        return next.run(request).await;
    }
    if !matches!(
        path,
        "/api/identity"
            | "/enterprise/sessions"
            | "/enterprise/rpc"
            | "/enterprise/registry" | "/enterprise/registry/generation" | "/enterprise/workflows" | "/enterprise/workflows/read" | "/enterprise/workflows/control" | "/a2a" | "/.well-known/agent-card.json"
            | "/enterprise/billing/configure" | "/enterprise/billing" | "/enterprise/billing/usage" | "/enterprise/billing/accounts"
            | "/enterprise/credentials/issue"
            | "/enterprise/credentials/revoke"
    ) {
        return api_error(StatusCode::NOT_FOUND, "入口不可用");
    }
    let token = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_owned);
    let Some(token) = token else {
        return api_error(StatusCode::UNAUTHORIZED, "身份认证失败");
    };
    let verified = match state.verify_bearer(&token).await {
        Ok(verified) => verified,
        Err(_) => return api_error(StatusCode::UNAUTHORIZED, "身份认证失败"),
    };
    if !state.rate_limiter.lock().await.admit(
        &verified.identity.scope, &verified.identity.principal.subject_id,
        path, std::time::Instant::now(),
    ) {
        return api_error(StatusCode::TOO_MANY_REQUESTS, "访问速率预算不可用");
    }
    let client = match state
        .workspaces
        .client_for(None)
        .await
        .and_then(|(_, client)| client.with_bearer_token(token.clone()).map_err(Into::into))
    {
        Ok(client) => client,
        Err(_) => return api_error(StatusCode::SERVICE_UNAVAILABLE, "运行端点不可用"),
    };
    let client = match state
        .gateway_client(client, agent_core::HookChannel::Http)
        .await
    {
        Ok(client) => client,
        Err(_) => return api_error(StatusCode::SERVICE_UNAVAILABLE, "Gateway凭据不可用"),
    };
    let permit = match AccessPermit::acquire(client, agent_core::AccessClass::Request,path).await {
        Ok(permit) => permit,
        Err(status) => return api_error(status, "共享访问预算不可用"),
    };
    let response = match tokio::time::timeout(
        std::time::Duration::from_secs(30),
        HTTP_CREDENTIAL.scope(token, next.run(request)),
    )
    .await
    {
        Ok(response) => response,
        Err(_) => return api_error(StatusCode::GATEWAY_TIMEOUT, "从持久cursor恢复请求"),
    };
    if response.status().is_client_error() || response.status().is_server_error() {
        return api_error(response.status(), "请求未能完成");
    }
    let (parts, body) = response.into_parts();
    let deadline = tokio::time::Instant::now()
        + std::time::Duration::from_secs(verified.expires_at.saturating_sub(unix_time()).min(240));
    let stream = stream::unfold(
        (body.into_data_stream(), permit, deadline, true),
        |(mut incoming, permit, deadline, first)| async move {
            let timeout = std::time::Duration::from_secs(if first { 30 } else { 15 });
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            match tokio::time::timeout(timeout.min(remaining), incoming.next()).await {
                Ok(Some(chunk)) => Some((chunk, (incoming, permit, deadline, false))),
                _ => None,
            }
        },
    );
    Response::from_parts(parts, axum::body::Body::from_stream(stream))
}

async fn credential_issue(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    Json(params): Json<agent_daemon_protocol::params::CredentialIssueParams>,
) -> Response {
    credential_command(&state, &headers, "credential.issue", json!(params)).await
}
async fn credential_revoke(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    Json(params): Json<agent_daemon_protocol::params::CredentialRevokeParams>,
) -> Response {
    credential_command(&state, &headers, "credential.revoke", json!(params)).await
}
async fn credential_command(
    state: &ApiState,
    headers: &HeaderMap,
    method: &str,
    params: Value,
) -> Response {
    if !is_authorized(headers, state.bearer_token.as_deref()) {
        return api_error(StatusCode::UNAUTHORIZED, "身份认证失败");
    }
    let client = match state.client_for(None).await {
        Ok((_, client)) => client,
        Err(_) => return api_error(StatusCode::SERVICE_UNAVAILABLE, "运行端点不可用"),
    };
    let mut response = match client.request_result(method, params).await {
        Ok(result) => Json(result).into_response(),
        Err(agent_daemon_client::ClientError::Rpc(error)) => api_error(
            StatusCode::from_u16(rpc_http_status(error.code))
                .unwrap_or(StatusCode::SERVICE_UNAVAILABLE),
            "凭据操作未确认；先用原operation_id读回",
        ),
        Err(_) => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "凭据操作未确认；先用原operation_id读回",
        ),
    };
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    response
}

async fn billing_configure(ScopeState(state):ScopeState,headers:HeaderMap,Json(params):Json<agent_daemon_protocol::params::BillingConfigureParams>)->Response {
    credential_command(&state,&headers,"billing.configure",json!(params)).await
}
async fn billing_readback(ScopeState(state):ScopeState,headers:HeaderMap)->Response {
    credential_command(&state,&headers,"billing.readback",json!({})).await
}
async fn billing_accounts(ScopeState(state):ScopeState,headers:HeaderMap)->Response {
    credential_command(&state,&headers,"billing.accounts",json!({})).await
}
async fn registry_generation(ScopeState(state):ScopeState,headers:HeaderMap,Query(params):Query<agent_daemon_protocol::params::RegistryGenerationParams>)->Response {credential_command(&state,&headers,"registry.generation",json!(params)).await}
async fn registry_readback(ScopeState(state):ScopeState,headers:HeaderMap)->Response {credential_command(&state,&headers,"registry.readback",json!({})).await}
async fn registry_mutate(ScopeState(state):ScopeState,headers:HeaderMap,Json(m):Json<agent_core::RegistryMutation>)->Response {credential_command(&state,&headers,"registry.mutate",json!(m)).await}
async fn billing_usage(ScopeState(state):ScopeState,headers:HeaderMap,Query(params):Query<agent_daemon_protocol::params::BillingUsageParams>)->Response {
    credential_command(&state,&headers,"billing.usage",json!(params)).await
}

fn login_cookie_name(mock:bool,profile:Option<&str>)->String {
    let base=if mock {"agent_login_mock"}else{"__Host-agent_login"};
    match profile.filter(|p|*p!="default"){Some(profile)=>format!("{base}_{profile}"),None=>base.into()}
}
fn login_cookie(mock: bool, value: &str, clear: bool,profile:Option<&str>) -> String {
    format!("{}={}; Path=/; HttpOnly; SameSite=Lax; Max-Age={}{}",
        login_cookie_name(mock,profile), value,
        if clear {0} else {300}, if mock {""} else {"; Secure"})
}
fn protect_login_response(mut response: Response, cookie: Option<String>) -> Response {
    let headers=response.headers_mut();
    headers.insert(axum::http::header::CACHE_CONTROL,axum::http::HeaderValue::from_static("no-store"));
    headers.insert(axum::http::header::REFERRER_POLICY,axum::http::HeaderValue::from_static("no-referrer"));
    if let Some(cookie)=cookie.and_then(|v|axum::http::HeaderValue::from_str(&v).ok()) {
        headers.insert(axum::http::header::SET_COOKIE,cookie);
    }
    response
}
async fn login_budget(state: &ApiState, path: &str) -> Result<(DaemonClient,AccessPermit)> {
    let verifier=state.oidc.as_ref().context("登录未配置")?;
    let gateway=verifier.gateway_subject().context("Gateway主体未配置")?;
    if !state.rate_limiter.lock().await.admit(verifier.scope(),gateway,path,std::time::Instant::now()) {
        anyhow::bail!("登录访问预算不可用")
    }
    let client=state.service_client().await?;
    let permit=AccessPermit::acquire(client.clone(),agent_core::AccessClass::Request,path).await.map_err(|_|anyhow::anyhow!("共享登录预算不可用"))?;
    Ok((client,permit))
}
async fn oauth_login(ScopeState(state): ScopeState) -> Response {
    let Some(verifier)=&state.oidc else {return api_error(StatusCode::NOT_FOUND,"登录未配置")};
    if verifier.oauth_config().is_none() {return api_error(StatusCode::NOT_FOUND,"登录未配置")}
    let result=async {
        let (client,_permit)=login_budget(&state,"/auth/login").await?;
        let login=agent_identity::oauth::BrowserLogin::generate()?;
        let url=verifier.authorization_url(&login)?;
        client.request_result("oauth.begin",json!(login.proof())).await?;
        let mut response=StatusCode::SEE_OTHER.into_response();
        response.headers_mut().insert(axum::http::header::LOCATION,axum::http::HeaderValue::from_str(&url)?);
        Ok::<_,anyhow::Error>(protect_login_response(response,Some(login_cookie(verifier.loopback_mock(),&login.cookie_value(),false,state.selected_profile.as_deref()))))
    }.await;
    result.unwrap_or_else(|_|protect_login_response(api_error(StatusCode::SERVICE_UNAVAILABLE,"登录未开始；发起新的登录"),None))
}
#[derive(Deserialize)]
struct OAuthCallback {code:String,state:String,iss:String}
async fn oauth_callback(ScopeState(state): ScopeState,headers:HeaderMap,query: std::result::Result<Query<OAuthCallback>,axum::extract::rejection::QueryRejection>) -> Response {
    let Some(verifier)=&state.oidc else {return api_error(StatusCode::NOT_FOUND,"登录未配置")};
    let clear=login_cookie(verifier.loopback_mock(),"",true,state.selected_profile.as_deref());
    let result=async {
        let Query(query)=query.map_err(|_|anyhow::anyhow!("回调参数无效"))?;
        let name=login_cookie_name(verifier.loopback_mock(),state.selected_profile.as_deref());
        let cookies=headers.get(axum::http::header::COOKIE).and_then(|v|v.to_str().ok()).context("登录cookie缺失")?;
        let matches=cookies.split(';').filter_map(|v|v.trim().split_once('=')).filter(|(key,_)|*key==name).collect::<Vec<_>>();
        anyhow::ensure!(matches.len()==1 && cookies.len()<=4096,"登录cookie无效");
        let login=agent_identity::oauth::BrowserLogin::from_cookie(matches[0].1)?;
        anyhow::ensure!(query.state==login.state && query.iss==verifier.issuer(),"state或issuer不匹配");
        let (client,_permit)=login_budget(&state,"/auth/callback").await?;
        // CAS成功后不重复兑换授权码；网络结果未知时必须发起新登录。
        client.request_result("oauth.consume",json!(login.proof())).await?;
        let (tokens,verified)=verifier.exchange_code(&query.code,&login).await?;
        let client=client.with_bearer_token(tokens.access_token.as_str().into())?;
        let current:agent_identity::VerifiedToken=serde_json::from_value(client.request_result("credential.status",json!({})).await?)?;
        anyhow::ensure!(current.identity==verified.identity,"主体已撤销");
        Ok::<_,anyhow::Error>(Json(tokens).into_response())
    }.await;
    protect_login_response(result.unwrap_or_else(|_|api_error(StatusCode::UNAUTHORIZED,"登录未完成；发起新的登录")),Some(clear))
}

async fn directory_create(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    Json(params): Json<agent_daemon_protocol::params::DirectoryCreateParams>,
) -> Response {
    if !is_authorized(&headers, state.bearer_token.as_deref()) {
        return api_error(StatusCode::UNAUTHORIZED, "身份认证失败");
    }
    let mut client = match state.client_for(None).await {
        Ok((_, client)) => client,
        Err(_) => return api_error(StatusCode::SERVICE_UNAVAILABLE, "运行端点不可用"),
    };
    if state.oidc.is_some() {
        match client.request_result("directory.find",json!({"operation_id":params.operation_id})).await {
            Ok(found) if !found.is_null() => return Json(json!({"session_id":found["external_id"]})).into_response(),
            Ok(_) => {},
            Err(error) => {tracing::debug!(%error,"持久目录按operation读回失败");return api_error(StatusCode::SERVICE_UNAVAILABLE,"目录操作未确认；按原operation_id读回")},
        }
        let routes=match state.route_table().await {Ok(routes)=>routes,Err(_)=>return api_error(StatusCode::SERVICE_UNAVAILABLE,"运行端点路由不可用")};
        let hash=params.operation_id.bytes().fold(0u64,|value,byte|value.wrapping_mul(131).wrapping_add(byte.into()));
        let Some(endpoint)=routes.keys().nth((hash as usize)%routes.len()) else {return api_error(StatusCode::SERVICE_UNAVAILABLE,"运行端点路由不可用")};
        let token=HTTP_CREDENTIAL.with(Clone::clone);
        client=match state.routed_client(endpoint,&token,agent_core::HookChannel::Http).await {Ok(client)=>client,Err(error)=>{tracing::debug!(%error,"运行端点可信绑定失败");return api_error(StatusCode::SERVICE_UNAVAILABLE,"运行端点可信绑定不可用")}};
    }
    match client
        .request_result("directory.create", json!(params))
        .await
    {
        Ok(route) => Json(json!({"session_id":route["external_id"]})).into_response(),
        Err(agent_daemon_client::ClientError::Rpc(error)) => api_error(
            StatusCode::from_u16(rpc_http_status(error.code))
                .unwrap_or(StatusCode::SERVICE_UNAVAILABLE),
            "目录操作未完成",
        ),
        Err(_) => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "先按原operation_id读回目录",
        ),
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectoryRpc {
    session_id: String,
    method: String,
    params: Value,
}
fn directory_method(method: &str) -> bool {
    matches!(
        method,
        "chat.send"
            | "run.read"
            | "session.load"
            | "session.clear"
            | "session.close"
            | "session.delete"
            | "compact.start"
            | "context.readback"
            | "sessions.plan.readback"
            | "sessions.plan.discard"
            | "hooks.readback"
    )
}
fn fact_in_route(fact: &Value, route:&agent_core::DirectorySession) -> bool {
    fact["session_id"]==json!(route.session_key)
        && fact["_my_agent_view"]["session_lifetime_id"]==json!(route.session_lifetime)
}
fn replace_route(value: &mut Value, from: &str, to: &str) {
    match value {
        Value::String(s) if s == from => *s = to.into(),
        Value::Array(values) => values.iter_mut().for_each(|v| replace_route(v, from, to)),
        Value::Object(values) => values.values_mut().for_each(|v| replace_route(v, from, to)),
        _ => {}
    }
}

fn public_projection(value: &mut Value) {
    match value {
        Value::Object(values) => {
            for field in [
                "path",
                "workspace",
                "config_path",
                "trace_path",
                "daemon_log_path",
                "runtime_endpoint",
                "cwd",
                "error_message",
            ] {
                values.remove(field);
            }
            values.values_mut().for_each(public_projection);
        }
        Value::Array(values) => values.iter_mut().for_each(public_projection),
        _ => {}
    }
}
async fn directory_rpc(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    Json(mut request): Json<DirectoryRpc>,
) -> Response {
    if !is_authorized(&headers, state.bearer_token.as_deref()) {
        return api_error(StatusCode::UNAUTHORIZED, "身份认证失败");
    }
    if !directory_method(&request.method)
        || !agent_core::valid_directory_id(&request.session_id)
        || !request.params.is_object()
    {
        return api_error(StatusCode::BAD_REQUEST, "目录RPC参数无效");
    }
    if request.params.get("session_id").is_some() {
        return api_error(StatusCode::BAD_REQUEST, "session_id由持久目录决定");
    }
    let mut client = match state.client_for(None).await {
        Ok((_, client)) => client,
        Err(_) => return api_error(StatusCode::SERVICE_UNAVAILABLE, "运行端点不可用"),
    };
    let route = match client
        .request_result(
            "directory.resolve",
            json!({"session_id":request.session_id}),
        )
        .await
        .and_then(|v| serde_json::from_value::<agent_core::DirectorySession>(v).map_err(Into::into))
    {
        Ok(route) => route,
        _ => return api_error(StatusCode::FORBIDDEN, "会话路由不可用"),
    };
    if state.oidc.is_some() {
        let token=HTTP_CREDENTIAL.with(Clone::clone);
        client=match state.routed_client(&route.runtime_endpoint,&token,agent_core::HookChannel::Http).await {
            Ok(client)=>client,
            Err(_)=>return api_error(StatusCode::SERVICE_UNAVAILABLE,"运行端点可信绑定不可用"),
        };
    }
    if request.method=="run.read" {
        return match client.request_result("run.read",request.params).await {
            Ok(mut fact) if fact_in_route(&fact,&route) => {
                if let Some(object)=fact.as_object_mut() {object.remove("snapshot");object.remove("compact");}
                replace_route(&mut fact,&route.session_key.0,&route.external_id);public_projection(&mut fact);
                Json(fact).into_response()
            },
            _=>api_error(StatusCode::FORBIDDEN,"run未获目录授权"),
        };
    }
    request.params["session_id"] = json!(route.session_key.0);
    if matches!(
        request.method.as_str(),
        "chat.send"
            | "session.clear"
            | "session.close"
            | "session.delete"
            | "compact.start"
            | "context.readback"
            | "hooks.readback"
    ) {
        if request
            .params
            .get("expected_lifetime")
            .is_some_and(|v| v != &json!(route.session_lifetime))
        {
            return api_error(StatusCode::CONFLICT, "会话lifetime已变更");
        }
        request.params["expected_lifetime"] = json!(route.session_lifetime);
    }
    let mut rpc = match client.request(&request.method, request.params).await {
        Ok(rpc) => rpc,
        Err(_) => return api_error(StatusCode::BAD_REQUEST, "RPC参数无效"),
    };
    while let Some(frame) = rpc.next().await {
        match frame {
            ServerFrame::Event(event) if event.event == EventKind::ApprovalRequired => {
                let mut pending = json!({"status":"waiting_approval","interaction":event.data,"run_id":event.run_id,"cursor":event.seq});
                replace_route(&mut pending, &route.session_key.0, &route.external_id);
                return (StatusCode::ACCEPTED, Json(pending)).into_response();
            }
            ServerFrame::Response(response) => {
                if let Some(error) = response.error {
                    return api_error(
                        StatusCode::from_u16(rpc_http_status(error.code))
                            .unwrap_or(StatusCode::SERVICE_UNAVAILABLE),
                        "请求未完成；从持久cursor读回",
                    );
                }
                let Some(mut result) = response.result else {
                    return api_error(StatusCode::BAD_GATEWAY, "响应不完整");
                };
                replace_route(&mut result, &route.session_key.0, &route.external_id);
                public_projection(&mut result);
                return Json(result).into_response();
            }
            _ => {}
        }
    }
    api_error(StatusCode::SERVICE_UNAVAILABLE, "从持久cursor恢复请求")
}

struct AccessPermit {
    client: DaemonClient,
    operation: String,
}
impl AccessPermit {
    async fn acquire(
        client: DaemonClient,
        class: agent_core::AccessClass,
        endpoint:&str,
    ) -> std::result::Result<Self, StatusCode> {
        let operation = format!(
            "gateway-{}-{}-{}",
            std::process::id(),
            unix_time(),
            NEXT_ERROR_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        client
            .request_result(
                "access.acquire",
                json!({"class":class,"endpoint":endpoint,"operation_id":operation}),
            )
            .await
            .map_err(|error| match error {
                agent_daemon_client::ClientError::Rpc(error) => {
                    StatusCode::from_u16(rpc_http_status(error.code))
                        .unwrap_or(StatusCode::SERVICE_UNAVAILABLE)
                }
                _ => StatusCode::SERVICE_UNAVAILABLE,
            })?;
        Ok(Self { client, operation })
    }
}
impl Drop for AccessPermit {
    fn drop(&mut self) {
        let client = self.client.clone();
        let operation = self.operation.clone();
        tokio::spawn(async move {
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(3),
                client.request_result("access.release", json!({"operation_id":operation})),
            )
            .await;
        });
    }
}
fn rpc_http_status(code: i64) -> u16 {
    match code {
        -32040 => 401,
        -32041 => 403,
        -32042 | -32043 => 403,
        -32044 | -32045 | -32046 | -32051 => 429,
        _ => agent_core::CoordinationError::from_rpc_code(code).map_or(503, |e| e.http_status()),
    }
}

async fn web_index() -> Response {
    static_asset("text/html; charset=utf-8", WEB_INDEX)
}

async fn hook_readback(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    Query(query): Query<HookReadQuery>,
) -> Response {
    plan_rpc(&state,&headers,query.workspace.as_deref(),"hooks.readback",serde_json::json!({"session_id":query.session_id,"expected_lifetime":query.expected_lifetime,"after_cursor":query.after_cursor,"limit":query.limit})).await
}

async fn context_readback(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    Json(request): Json<HttpPlanRequest<agent_daemon_protocol::params::ContextReadParams>>,
) -> Response {
    let params = match serde_json::to_value(request.params) {
        Ok(params) => params,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, error.to_string()),
    };
    plan_rpc(
        &state,
        &headers,
        request.workspace.as_deref(),
        "context.readback",
        params,
    )
    .await
}

async fn session_readback(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    Json(request): Json<HttpPlanRequest<agent_daemon_protocol::params::SessionSelectorParams>>,
) -> Response {
    let params = match serde_json::to_value(request.params) {
        Ok(params) => params,
        Err(_) => return api_error(StatusCode::BAD_REQUEST, "会话读取参数无效"),
    };
    plan_rpc(
        &state,
        &headers,
        request.workspace.as_deref(),
        "sessions.read",
        params,
    )
    .await
}

async fn compact_start(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    Json(mut request): Json<HttpPlanRequest<agent_daemon_protocol::params::CompactStartParams>>,
) -> Response {
    request.params.entry_channel = agent_core::HookChannel::Http;
    let params = match serde_json::to_value(request.params) {
        Ok(params) => params,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, error.to_string()),
    };
    plan_rpc(
        &state,
        &headers,
        request.workspace.as_deref(),
        "compact.start",
        params,
    )
    .await
}

async fn plan_readback(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    Query(query): Query<PlanReadQuery>,
) -> Response {
    plan_rpc(
        &state,
        &headers,
        query.workspace.as_deref(),
        "sessions.plan.readback",
        json!({"session_id":query.session_id}),
    )
    .await
}

async fn plan_discard(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    Json(request): Json<HttpPlanRequest<agent_daemon_protocol::params::PlanDiscardParams>>,
) -> Response {
    let params = match serde_json::to_value(request.params) {
        Ok(params) => params,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, error.to_string()),
    };
    plan_rpc(
        &state,
        &headers,
        request.workspace.as_deref(),
        "sessions.plan.discard",
        params,
    )
    .await
}

async fn plan_execute(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    Json(request): Json<HttpPlanRequest<agent_daemon_protocol::params::ChatSendParams>>,
) -> Response {
    if request.params.plan_execution.is_none() {
        return api_error(
            StatusCode::BAD_REQUEST,
            "计划执行必须携带 plan_execution 身份",
        );
    }
    let mut params = match serde_json::to_value(request.params) {
        Ok(params) => params,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, error.to_string()),
    };
    params["entry_channel"] = json!("http");
    plan_rpc(
        &state,
        &headers,
        request.workspace.as_deref(),
        "chat.send",
        params,
    )
    .await
}

async fn plan_rpc(
    state: &ApiState,
    headers: &HeaderMap,
    workspace: Option<&Path>,
    method: &str,
    params: Value,
) -> Response {
    if !is_authorized(headers, state.bearer_token.as_deref()) {
        return api_error(StatusCode::UNAUTHORIZED, "Bearer Token 无效或缺失");
    }
    let (_, client) = match state.client_for(workspace).await {
        Ok(connection) => connection,
        Err(error) => return api_error(StatusCode::BAD_GATEWAY, format!("{error:#}")),
    };
    let mut stream = match client.request(method, params).await {
        Ok(stream) => stream,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, format!("{error:#}")),
    };
    while let Some(frame) = stream.next().await {
        match frame {
            ServerFrame::Event(event) if event.event == EventKind::ApprovalRequired => {
                return (StatusCode::ACCEPTED,Json(json!({"status":"waiting_approval","interaction":event.data,"run_id":event.run_id,"cursor":event.seq}))).into_response();
            }
            ServerFrame::Event(_) => {}
            ServerFrame::Response(response) => {
                if let Some(error) = response.error {
                    if let Some(reason) = error
                        .data
                        .as_ref()
                        .and_then(|data| data.get("kind"))
                        .and_then(|kind| {
                            serde_json::from_value::<agent_core::AuthorizationError>(kind.clone())
                                .ok()
                        })
                    {
                        if error.code == reason.rpc_code() {
                            let status = StatusCode::from_u16(reason.http_status())
                                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                            return (status, Json(serde_json::json!({"error":error})))
                                .into_response();
                        }
                    }
                    if let Some(reason) = error
                        .data
                        .as_ref()
                        .and_then(|data| data.get("kind"))
                        .and_then(|kind| {
                            serde_json::from_value::<agent_core::CoordinationError>(kind.clone())
                                .ok()
                        })
                    {
                        if error.code == reason.rpc_code() {
                            let status = StatusCode::from_u16(reason.http_status())
                                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                            return (status, Json(serde_json::json!({"error":error})))
                                .into_response();
                        }
                    }
                    if error
                        .data
                        .as_ref()
                        .and_then(|data| data.get("domain"))
                        .and_then(Value::as_str)
                        == Some("artifact")
                    {
                        if let Some(reason) = error
                            .data
                            .as_ref()
                            .and_then(|data| data.get("kind"))
                            .and_then(|kind| {
                                serde_json::from_value::<agent_core::ArtifactError>(kind.clone())
                                    .ok()
                            })
                        {
                            if error.code == reason.rpc_code() {
                                let status = StatusCode::from_u16(reason.http_status())
                                    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                                return (status, Json(serde_json::json!({"error":error})))
                                    .into_response();
                            }
                        }
                    }
                    return api_error(
                        StatusCode::CONFLICT,
                        format!("daemon RPC {}: {}", error.code, error.message),
                    );
                }
                return match response.result {
                    Some(result) => Json(result).into_response(),
                    None => api_error(StatusCode::BAD_GATEWAY, "daemon response 缺少 result"),
                };
            }
        }
    }
    api_error(StatusCode::BAD_GATEWAY, "daemon 在终态响应前断开")
}

async fn web_app() -> Response {
    static_asset("text/javascript; charset=utf-8", WEB_APP)
}

async fn web_styles() -> Response {
    static_asset("text/css; charset=utf-8", WEB_STYLES)
}

fn static_asset(content_type: &'static str, body: &'static str) -> Response {
    (
        [
            (axum::http::header::CONTENT_TYPE, content_type),
            (axum::http::header::CACHE_CONTROL, "no-cache"),
            (axum::http::header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        body,
    )
        .into_response()
}

fn canonical_directory(path: &Path) -> Result<PathBuf> {
    let path = std::fs::canonicalize(path)
        .with_context(|| format!("无法解析工作目录: {}", path.display()))?;
    if !path.is_dir() {
        anyhow::bail!("工作目录不是文件夹: {}", path.display());
    }
    Ok(path)
}

async fn directory_listing(requested: Option<&Path>, default: &Path) -> Result<Value> {
    let requested = requested.unwrap_or(default);
    let candidate = if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        default.join(requested)
    };
    let path = canonical_directory(&candidate)?;
    let mut reader = tokio::fs::read_dir(&path)
        .await
        .with_context(|| format!("无法读取目录: {}", path.display()))?;
    let mut directories = Vec::new();
    while let Some(entry) = reader.next_entry().await? {
        let metadata = match entry.metadata().await {
            Ok(metadata) => metadata,
            Err(_) => continue,
        };
        if !metadata.is_dir() {
            continue;
        }
        directories.push(json!({
            "name": entry.file_name().to_string_lossy(),
            "path": entry.path(),
        }));
        if directories.len() >= 1_000 {
            break;
        }
    }
    directories.sort_by(|left, right| {
        left["name"]
            .as_str()
            .unwrap_or_default()
            .to_lowercase()
            .cmp(&right["name"].as_str().unwrap_or_default().to_lowercase())
    });

    let mut favorites = vec![default.to_path_buf()];
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .and_then(|home| canonical_directory(&home).ok());
    if let Some(home) = home
        && !favorites.contains(&home)
    {
        favorites.push(home);
    }
    let parent = path.parent().map(Path::to_path_buf);
    Ok(json!({
        "path": path,
        "parent": parent,
        "directories": directories,
        "favorites": favorites,
    }))
}

fn websocket_origin_allowed(headers: &HeaderMap) -> bool {
    let Some(origin) = headers
        .get(axum::http::header::ORIGIN)
        .and_then(|value| value.to_str().ok())
    else {
        return true;
    };
    let Some(host) = headers
        .get(axum::http::header::HOST)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let origin_host = origin
        .strip_prefix("http://")
        .or_else(|| origin.strip_prefix("https://"))
        .and_then(|value| value.split('/').next());
    origin_host.is_some_and(|origin_host| origin_host.eq_ignore_ascii_case(host))
}

async fn websocket_upgrade(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    if !websocket_origin_allowed(&headers) {
        return api_error(StatusCode::FORBIDDEN, "WebSocket Origin 与当前服务不一致");
    }
    let permit = match state.connections.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => return api_error(StatusCode::TOO_MANY_REQUESTS, "连接预算已耗尽"),
    };
    upgrade
        .max_message_size(MAX_FRAME_BYTES)
        .on_upgrade(move |socket| async move {
            let _permit = permit;
            handle_websocket(socket, state).await
        })
}

async fn handle_websocket(mut socket: WebSocket, state: ApiState) {
    let connect = match tokio::time::timeout(std::time::Duration::from_secs(10), socket.recv())
        .await
        .ok()
        .flatten()
    {
        Some(Ok(WebSocketMessage::Text(text))) if text.len() <= MAX_FRAME_BYTES => {
            serde_json::from_str::<WebSocketConnect>(&text)
        }
        Some(Ok(_)) => {
            send_websocket_control_error(&mut socket, "第一帧必须是文本 connect 帧").await;
            return;
        }
        Some(Err(error)) => {
            tracing::debug!(%error, "读取 WebSocket connect 帧失败");
            return;
        }
        None => return,
    };
    let connect = match connect {
        Ok(connect) if connect.kind == "connect" => connect,
        Ok(_) | Err(_) => {
            send_websocket_control_error(&mut socket, "第一帧必须是合法 connect 帧").await;
            return;
        }
    };
    let state=if state.oidc.is_some() {
        match crate::scope_routes::select(&state.scope_routes,connect.token.as_deref(),connect.scope.as_deref().or(state.selected_profile.as_deref())) {
            Ok(route)=>state.bound_to(&route),Err(_)=>{send_websocket_control_error(&mut socket,"身份认证失败").await;return;}
        }
    }else{state};
    if !token_matches(connect.token.as_deref(), state.bearer_token.as_deref()) {
        send_websocket_control_error(&mut socket, "Token 无效或缺失").await;
        return;
    }
    let verified = if state.oidc.is_some() {
        if connect
            .workspace
            .as_deref()
            .is_some_and(|p| p != state.workspace)
        {
            send_websocket_control_error(&mut socket, "路由不可用").await;
            return;
        }
        let Some(token) = &connect.token else {
            send_websocket_control_error(&mut socket, "身份认证失败").await;
            return;
        };
        match state.verify_bearer(token).await {
            Ok(verified) => Some(verified),
            Err(_) => {
                send_websocket_control_error(&mut socket, "身份认证失败").await;
                return;
            }
        }
    } else {
        None
    };
    if let Some(identity) = &verified {
        if !state.rate_limiter.lock().await.admit(
            &identity.identity.scope, &identity.identity.principal.subject_id,
            "/ws", std::time::Instant::now(),
        ) {
            send_websocket_control_error(&mut socket, "访问速率预算不可用").await;
            return;
        }
    }
    let (workspace, mut client) = match state
        .workspaces
        .client_for(connect.workspace.as_deref())
        .await
    {
        Ok(connection) => connection,
        Err(error) => {
            tracing::debug!(%error,"Gateway连接失败");
            send_websocket_control_error(&mut socket, "运行端点不可用").await;
            return;
        }
    };
    if verified.is_some() {
        match client.with_bearer_token(connect.token.clone().unwrap_or_default()) {
            Ok(authenticated) => client = authenticated,
            Err(_) => return,
        }
    }
    if verified.is_some() {
        match state
            .gateway_client(client.clone(), agent_core::HookChannel::WebSocket)
            .await
        {
            Ok(authenticated) => client = authenticated,
            Err(_) => {
                send_websocket_control_error(&mut socket, "Gateway凭据不可用").await;
                return;
            }
        }
    }
    let bound_route = if verified.is_some() {
        let Some(external) = connect.session_id else {
            send_websocket_control_error(&mut socket, "需要opaque session_id").await;
            return;
        };
        match client
            .request_result("directory.resolve", json!({"session_id":external}))
            .await
            .and_then(|v| {
                serde_json::from_value::<agent_core::DirectorySession>(v).map_err(Into::into)
            }) {
            Ok(route) => Some(route),
            _ => {
                send_websocket_control_error(&mut socket, "会话路由不可用").await;
                return;
            }
        }
    } else {
        None
    };
    if let Some(route)=&bound_route {
        let Some(token)=connect.token.as_deref() else {return};
        client=match state.routed_client(&route.runtime_endpoint,&token,agent_core::HookChannel::WebSocket).await {
            Ok(client)=>client,
            Err(_)=>{send_websocket_control_error(&mut socket,"运行端点可信绑定不可用").await;return;}
        };
    }
    let _access_permit = if verified.is_some() {
        match AccessPermit::acquire(client.clone(), agent_core::AccessClass::WebSocket,"/ws").await {
            Ok(permit) => Some(permit),
            Err(_) => {
                send_websocket_control_error(&mut socket, "共享连接预算不可用").await;
                return;
            }
        }
    } else {
        None
    };
    let expires = verified
        .as_ref()
        .map(|v| v.expires_at.saturating_sub(unix_time()).min(240))
        .unwrap_or(24 * 60 * 60);
    let authentication_deadline = tokio::time::sleep(std::time::Duration::from_secs(expires));
    tokio::pin!(authentication_deadline);
    if socket
        .send(WebSocketMessage::Text(
            json!({
                "type": "connected",
                "protocol": "my-agent-jsonrpc",
                "version": 1,
                "workspace": if verified.is_none(){Some(workspace)}else{None},
            })
            .to_string()
            .into(),
        ))
        .await
        .is_err()
    {
        return;
    }

    let (mut websocket_writer, mut websocket_reader) = socket.split();
    let (sender, mut outgoing_receiver) = mpsc::channel::<QueuedWebSocketFrame>(128);
    let (detached, mut detached_receiver) = watch::channel(false);
    let mut writer_detached = detached.subscribe();
    let writer_detach = detached.clone();
    let queued_bytes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let writer_bytes = queued_bytes.clone();
    let outgoing = WebSocketOutgoing {
        sender,
        bytes: queued_bytes,
        detached,
    };
    let writer = tokio::spawn(async move {
        loop {
            if *writer_detached.borrow() {
                break;
            }
            let message = tokio::select! {
                _=writer_detached.changed()=>break,
                message=outgoing_receiver.recv()=>match message {Some(message)=>message,None=>break},
            };
            writer_bytes.fetch_sub(message.bytes, std::sync::atomic::Ordering::AcqRel);
            let sent = tokio::select! {
                _=writer_detached.changed()=>break,
                result=tokio::time::timeout(std::time::Duration::from_secs(10),websocket_writer.send(message.message))=>result,
            };
            if !matches!(sent, Ok(Ok(()))) {
                break;
            }
        }
        writer_detach.send_replace(true);
    });
    let active_ids = std::sync::Arc::new(Mutex::new(std::collections::HashMap::<
        RequestId,
        RequestId,
    >::new()));
    let mut jobs = JoinSet::new();
    let recovery = if let Some(route) = &bound_route {
        match client
            .request_result("session.load", json!({"session_id":route.session_key}))
            .await
        {
            Ok(mut snapshot) => {
                replace_route(&mut snapshot, &route.session_key.0, &route.external_id);
                public_projection(&mut snapshot);
                let _ = outgoing.send(WebSocketMessage::Text(
                    json!({"type":"recovery","snapshot":snapshot})
                        .to_string()
                        .into(),
                ));
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    } else {
        start_websocket_recovery(&client, &active_ids, &outgoing, &mut jobs).await
    };
    if let Err(error) = recovery {
        tracing::debug!(%error,"Gateway恢复失败");
        let _ = outgoing.send(WebSocketMessage::Text(
            json!({"type": "recovery_error", "error": "持久读回暂不可用"})
                .to_string()
                .into(),
        ));
    }

    loop {
        if *detached_receiver.borrow() {
            break;
        }
        let message = tokio::select! {
            _=&mut authentication_deadline=>break,
            _=detached_receiver.changed()=>break,
            result=tokio::time::timeout(std::time::Duration::from_secs(60),websocket_reader.next())=>match result {Ok(Some(message))=>message,_=>break},
        };
        let message = match message {
            Ok(message) => message,
            Err(error) => {
                tracing::debug!(%error, "WebSocket 读取结束");
                break;
            }
        };
        match message {
            WebSocketMessage::Text(text) => {
                let mut request = match decode_request(text.as_bytes()) {
                    Ok(request) => request,
                    Err(error) => {
                        send_ws_server_frame(
                            &outgoing,
                            ServerFrame::Response(JsonRpcResponse::failure(
                                RequestId::String("protocol".to_owned()),
                                if text.len() > MAX_FRAME_BYTES {
                                    -32002
                                } else {
                                    -32700
                                },
                                error.to_string(),
                            )),
                        );
                        continue;
                    }
                };
                if let Some(route) = &bound_route {
                    if request.authorization.is_some()
                        || !directory_method(&request.method)
                            && !matches!(
                                request.method.as_str(),
                                "agent.subscribe"
                                    | "interaction.respond"
                                    | "interaction.reject"
                                    | "interaction.read"
                                    | "interaction.list"
                                    | "agent.cancel"
                            )
                    {
                        send_ws_server_frame(
                            &outgoing,
                            ServerFrame::Response(JsonRpcResponse::failure(
                                request.id,
                                -32041,
                                "方法未获目录授权",
                            )),
                        );
                        continue;
                    }
                    if !request.params.is_object()
                        || request
                            .params
                            .get("session_id")
                            .is_some_and(|id| id != &json!(route.external_id))
                    {
                        send_ws_server_frame(
                            &outgoing,
                            ServerFrame::Response(JsonRpcResponse::failure(
                                request.id,
                                -32602,
                                "需要当前opaque session_id",
                            )),
                        );
                        continue;
                    }
                    if request.gateway.is_some() || request.params.get("exact_owner").is_some_and(|owner| {
                        owner["session_key"]!=json!(route.external_id) || owner["session_lifetime_id"]!=json!(route.session_lifetime)
                    }) {
                        send_ws_server_frame(&outgoing,ServerFrame::Response(JsonRpcResponse::failure(request.id,-32041,"owner未获目录授权")));
                        continue;
                    }
                    request.params["session_id"] = json!(route.session_key);
                    replace_route(
                        &mut request.params,
                        &route.external_id,
                        &route.session_key.0,
                    );
                    if request.method == "interaction.read" {
                        request
                            .params
                            .as_object_mut()
                            .map(|object| object.remove("session_id"));
                        let read=tokio::time::timeout(std::time::Duration::from_secs(5),client.request_result("interaction.read",request.params.clone())).await;
                        let result=match read {
                            Ok(Ok(mut result)) if result["interaction"]["session_id"]==json!(route.session_key) => {
                                let run=result["interaction"]["owner_run_id"].clone();
                                let belongs=matches!(tokio::time::timeout(std::time::Duration::from_secs(5),client.request_result("run.read",json!({"run_id":run}))).await,Ok(Ok(fact)) if fact_in_route(&fact,route));
                                if !belongs {
                                    send_ws_server_frame(&outgoing,ServerFrame::Response(JsonRpcResponse::failure(request.id,-32041,"interaction lifetime未获目录授权")));
                                    continue;
                                }
                                replace_route(&mut result,&route.session_key.0,&route.external_id);
                                public_projection(&mut result);
                                JsonRpcResponse::success(request.id,result)
                            },
                            _=>JsonRpcResponse::failure(request.id,-32041,"interaction未获目录授权"),
                        };
                        send_ws_server_frame(&outgoing,ServerFrame::Response(result));
                        continue;
                    }
                    if request.method=="run.read" {
                        request.params.as_object_mut().map(|object|object.remove("session_id"));
                        let read=tokio::time::timeout(std::time::Duration::from_secs(5),client.request_result("run.read",request.params.clone())).await;
                        let result=match read {
                            Ok(Ok(mut fact)) if fact_in_route(&fact,route) => {
                                if let Some(object)=fact.as_object_mut() {object.remove("snapshot");object.remove("compact");}
                                replace_route(&mut fact,&route.session_key.0,&route.external_id);public_projection(&mut fact);
                                JsonRpcResponse::success(request.id,fact)
                            },
                            _=>JsonRpcResponse::failure(request.id,-32041,"run未获目录授权"),
                        };
                        send_ws_server_frame(&outgoing,ServerFrame::Response(result));continue;
                    }
                    if matches!(
                        request.method.as_str(),
                        "chat.send"
                            | "session.clear"
                            | "session.close"
                            | "session.delete"
                            | "compact.start"
                            | "context.readback"
                            | "hooks.readback"
                    ) {
                        request.params["expected_lifetime"] = json!(route.session_lifetime);
                    }
                }
                start_websocket_request(
                    &client,
                    request,
                    &active_ids,
                    &outgoing,
                    &mut jobs,
                    bound_route.clone(),
                )
                .await;
            }
            WebSocketMessage::Ping(payload) => {
                let _ = outgoing.send(WebSocketMessage::Pong(payload));
            }
            WebSocketMessage::Close(_) => break,
            WebSocketMessage::Pong(_) | WebSocketMessage::Binary(_) => {}
        }
    }

    jobs.abort_all();
    while jobs.join_next().await.is_some() {}
    drop(outgoing);
    let _ = writer.await;
}

async fn start_websocket_request(
    client: &DaemonClient,
    mut request: JsonRpcRequest,
    active_ids: &std::sync::Arc<Mutex<std::collections::HashMap<RequestId, RequestId>>>,
    outgoing: &WebSocketOutgoing,
    jobs: &mut JoinSet<()>,
    route: Option<agent_core::DirectorySession>,
) {
    while jobs.try_join_next().is_some() {}
    if jobs.len() >= 64 {
        send_ws_server_frame(
            outgoing,
            ServerFrame::Response(JsonRpcResponse::failure(
                request.id,
                -32051,
                "连接并发预算已耗尽",
            )),
        );
        return;
    }
    if request.method == "agent.cancel"
        && let Some(target) = request
            .params
            .get("request_id")
            .and_then(|value| serde_json::from_value::<RequestId>(value.clone()).ok())
        && let Some(internal) = active_ids.lock().await.get(&target).cloned()
    {
        request.params["request_id"] = json!(internal);
    }
    let external_id = request.id;
    let is_chat = request.method == "chat.send";
    if is_chat || matches!(request.method.as_str(), "compact.start" | "slash.execute") {
        request.params["entry_channel"] = json!("web_socket");
    }
    let stream = match client.request(&request.method, request.params).await {
        Ok(stream) => stream,
        Err(error) => {
            send_ws_server_frame(
                outgoing,
                ServerFrame::Response(JsonRpcResponse::failure(
                    external_id,
                    -32000,
                    format!("{error:#}"),
                )),
            );
            return;
        }
    };
    if is_chat {
        active_ids
            .lock()
            .await
            .insert(external_id.clone(), stream.request_id().clone());
    }
    spawn_websocket_stream(
        stream,
        external_id,
        is_chat,
        active_ids,
        outgoing,
        jobs,
        route,
    );
}

fn spawn_websocket_stream(
    mut stream: RpcStream,
    external_id: RequestId,
    is_chat: bool,
    active_ids: &std::sync::Arc<Mutex<std::collections::HashMap<RequestId, RequestId>>>,
    outgoing: &WebSocketOutgoing,
    jobs: &mut JoinSet<()>,
    route: Option<agent_core::DirectorySession>,
) {
    let task_ids = active_ids.clone();
    let task_outgoing = outgoing.clone();
    jobs.spawn(async move {
        while let Some(mut frame) = stream.next().await {
            let terminal = matches!(frame, ServerFrame::Response(_));
            remap_frame_id(&mut frame, &external_id);
            if let Some(route) = &route {
                if let ServerFrame::Response(response)=&mut frame {
                    if let Some(error)=&mut response.error {
                        error.message="请求未完成；从持久cursor读回".into();
                        error.data=Some(json!({"code":error.code,"message":error.message,"retryable":matches!(rpc_http_status(error.code),429|503),"safe_auto_retry":false,"request_id":external_id,"recovery_action":"read_durable_state_before_retry"}));
                    }
                }
                let mut value = match serde_json::to_value(&frame) {
                    Ok(value) => value,
                    Err(_) => break,
                };
                replace_route(&mut value, &route.session_key.0, &route.external_id);
                public_projection(&mut value);
                frame = match serde_json::from_value(value) {
                    Ok(frame) => frame,
                    Err(_) => break,
                };
            }
            send_ws_server_frame(&task_outgoing, frame);
            if terminal {
                break;
            }
        }
        if is_chat {
            task_ids.lock().await.remove(&external_id);
        }
    });
}

async fn start_websocket_recovery(
    client: &DaemonClient,
    active_ids: &std::sync::Arc<Mutex<std::collections::HashMap<RequestId, RequestId>>>,
    outgoing: &WebSocketOutgoing,
    jobs: &mut JoinSet<()>,
) -> Result<()> {
    let snapshot = recovery::load_snapshot(client).await?;
    let active_set = snapshot
        .active_requests
        .iter()
        .cloned()
        .collect::<std::collections::HashSet<RequestId>>();
    let _ = outgoing.send(WebSocketMessage::Text(
        json!({"type": "recovery", "snapshot": &snapshot})
            .to_string()
            .into(),
    ));
    for approval in &snapshot.pending_approvals {
        if active_set.contains(&approval.request_id) {
            continue;
        }
        send_ws_server_frame(
            outgoing,
            ServerFrame::Event(agent_daemon_protocol::EventFrame::new(
                RequestId::String("recovery".to_owned()),
                EventKind::ApprovalRequired,
                json!({"approval": approval}),
            )),
        );
    }
    for request_id in snapshot.active_requests {
        if jobs.len() >= 64 {
            break;
        }
        let stream = recovery::subscribe(client, &request_id).await?;
        active_ids
            .lock()
            .await
            .insert(request_id.clone(), request_id.clone());
        spawn_websocket_stream(stream, request_id, true, active_ids, outgoing, jobs, None);
    }
    Ok(())
}

fn remap_frame_id(frame: &mut ServerFrame, external_id: &RequestId) {
    match frame {
        ServerFrame::Event(event) => event.request_id = external_id.clone(),
        ServerFrame::Response(response) => response.id = external_id.clone(),
    }
}

fn send_ws_server_frame(outgoing: &WebSocketOutgoing, frame: ServerFrame) {
    let message = match serde_json::to_string(&frame) {
        Ok(message) if message.len() <= MAX_FRAME_BYTES => message,
        Ok(_) => serde_json::to_string(&ServerFrame::Response(JsonRpcResponse::failure(
            agent_daemon_protocol::server_frame_request_id(&frame).clone(),
            -32002,
            "WebSocket 响应超过帧大小限制",
        )))
        .unwrap_or_else(|_| "{\"type\":\"error\"}".to_owned()),
        Err(error) => {
            tracing::warn!(%error, "序列化 WebSocket daemon 帧失败");
            return;
        }
    };
    let _ = outgoing.send(WebSocketMessage::Text(message.into()));
}

async fn send_websocket_control_error(socket: &mut WebSocket, message: &str) {
    let _ = socket
        .send(WebSocketMessage::Text(
            json!({"type": "error", "error": message})
                .to_string()
                .into(),
        ))
        .await;
    let _ = socket.send(WebSocketMessage::Close(None)).await;
}

async fn health(ScopeState(state): ScopeState, headers: HeaderMap) -> Response {
    if !is_authorized(&headers, state.bearer_token.as_deref()) {
        return api_error(StatusCode::UNAUTHORIZED, "Bearer Token 无效或缺失");
    }
    let model = state.active_model();
    Json(json!({
        "status": "ok",
        "daemon": "managed",
        "workspace": state.workspace,
        "model": model,
    }))
    .into_response()
}

async fn list_directories(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    Query(query): Query<DirectoryQuery>,
) -> Response {
    if !is_authorized(&headers, state.bearer_token.as_deref()) {
        return api_error(StatusCode::UNAUTHORIZED, "Bearer Token 无效或缺失");
    }
    match directory_listing(query.path.as_deref(), &state.workspace).await {
        Ok(listing) => Json(listing).into_response(),
        Err(error) => api_error(StatusCode::BAD_REQUEST, format!("{error:#}")),
    }
}

async fn list_models(ScopeState(state): ScopeState, headers: HeaderMap) -> Response {
    if !is_authorized(&headers, state.bearer_token.as_deref()) {
        return api_error(StatusCode::UNAUTHORIZED, "Bearer Token 无效或缺失");
    }
    match state.workspaces.host.list_models() {
        Ok(value) => Json(value).into_response(),
        Err(error) => api_error(StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}")),
    }
}

async fn save_models(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    Json(request): Json<ModelSaveRequest>,
) -> Response {
    if !is_authorized(&headers, state.bearer_token.as_deref()) {
        return api_error(StatusCode::UNAUTHORIZED, "Bearer Token 无效或缺失");
    }
    let requested_workspace = request.workspace.clone();
    let profile = request.profile.clone();
    match state.client_for(requested_workspace.as_deref()).await {
        Ok((_, client)) => {
            let result = match agent_entry_support::rpc::request_result(
                &client,
                "models.save",
                json!({"profile": profile, "activate": request.activate}),
            )
            .await
            {
                Ok(result) => result,
                Err(error) => return api_error(StatusCode::CONFLICT, format!("{error:#}")),
            };
            let mut response = result;
            response["runtime_applied"] = json!(true);
            response["warning"] = Value::Null;
            response["config_path"] = json!(state.workspaces.host.config_path());
            Json(response).into_response()
        }
        Err(error) => {
            match state
                .workspaces
                .host
                .save_configuration(request.profile, request.activate)
            {
                Ok(mut response) => {
                    response["runtime_applied"] = json!(false);
                    response["warning"] =
                        json!(format!("配置已保存，daemon 将在下次启动时应用：{error:#}"));
                    Json(response).into_response()
                }
                Err(error) => api_error(StatusCode::BAD_REQUEST, format!("{error:#}")),
            }
        }
    }
}

async fn activate_model(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    Json(request): Json<ModelUseRequest>,
) -> Response {
    if !is_authorized(&headers, state.bearer_token.as_deref()) {
        return api_error(StatusCode::UNAUTHORIZED, "Bearer Token 无效或缺失");
    }
    let requested_workspace = request.workspace.clone();
    match state.client_for(requested_workspace.as_deref()).await {
        Ok((_, client)) => {
            let result = match agent_entry_support::rpc::request_result(
                &client,
                "models.use",
                json!({"profile_id": request.profile_id}),
            )
            .await
            {
                Ok(result) => result,
                Err(error) => return api_error(StatusCode::CONFLICT, format!("{error:#}")),
            };
            let mut response = result;
            response["runtime_applied"] = json!(true);
            response["warning"] = Value::Null;
            response["config_path"] = json!(state.workspaces.host.config_path());
            Json(response).into_response()
        }
        Err(error) => match state
            .workspaces
            .host
            .activate_configuration(&request.profile_id)
        {
            Ok(mut response) => {
                response["runtime_applied"] = json!(false);
                response["warning"] = json!(format!("已保存为下次启动的活动模型：{error:#}"));
                Json(response).into_response()
            }
            Err(error) => api_error(StatusCode::BAD_REQUEST, format!("{error:#}")),
        },
    }
}

async fn chat_completions(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    Json(request): Json<ChatCompletionRequest>,
) -> Response {
    if !is_authorized(&headers, state.bearer_token.as_deref()) {
        return api_error(StatusCode::UNAUTHORIZED, "Bearer Token 无效或缺失");
    }
    let active_model = state.active_model();
    if let Some(requested) = &request.model
        && requested != &active_model
    {
        return api_error(
            StatusCode::BAD_REQUEST,
            format!("当前 daemon 模型为 {active_model}，不支持请求模型 {requested}"),
        );
    }
    let Some(prompt) = extract_latest_user_prompt(&request.messages) else {
        return api_error(StatusCode::BAD_REQUEST, "messages 中缺少非空 user 消息");
    };
    let (_, client) = match state.client_for(None).await {
        Ok(connection) => connection,
        Err(error) => return api_error(StatusCode::BAD_GATEWAY, format!("{error:#}")),
    };
    let rpc = match client
        .request(
            "chat.send",
            json!({"message": prompt,"entry_channel":"http"}),
        )
        .await
    {
        Ok(rpc) => rpc,
        Err(error) => return api_error(StatusCode::BAD_GATEWAY, format!("{error:#}")),
    };
    let completion_id = format!("chatcmpl-{}", request_id_text(rpc.request_id()));
    let created = unix_time();
    if request.stream {
        let stream = completion_stream(rpc, client, active_model.clone(), completion_id, created);
        return Sse::new(stream)
            .keep_alive(KeepAlive::default())
            .into_response();
    }

    match collect_answer(rpc, &client).await {
        Ok(content) => Json(json!({
            "id": completion_id,
            "object": "chat.completion",
            "created": created,
            "model": active_model,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop"
            }]
        }))
        .into_response(),
        Err(error) => match serde_json::from_str::<Value>(&error) {
            Ok(pending) if pending["status"] == "waiting_approval" => {
                (StatusCode::ACCEPTED, Json(pending)).into_response()
            }
            _ => api_error(StatusCode::BAD_GATEWAY, "运行读回暂不可用"),
        },
    }
}

fn completion_stream(
    rpc: RpcStream,
    _client: DaemonClient,
    model: String,
    completion_id: String,
    created: u64,
) -> impl Stream<Item = Result<Event, Infallible>> {
    struct StreamState {
        rpc: RpcStream,
        model: String,
        completion_id: String,
        created: u64,
        done_pending: bool,
        finished: bool,
    }

    stream::unfold(
        StreamState {
            rpc,
            model,
            completion_id,
            created,
            done_pending: false,
            finished: false,
        },
        |mut state| async move {
            if state.finished {
                return None;
            }
            if state.done_pending {
                state.finished = true;
                return Some((Ok(Event::default().data("[DONE]")), state));
            }
            loop {
                let Some(frame) = state.rpc.next().await else {
                    let event = Event::default()
                        .data(json!({"error": {"message": "daemon 在终态响应前断开"}}).to_string());
                    state.done_pending = true;
                    return Some((Ok(event), state));
                };
                match frame {
                    ServerFrame::Event(event) if event.event == EventKind::TextDelta => {
                        let delta = event.data["delta"].as_str().unwrap_or_default();
                        let chunk = completion_chunk(
                            &state.completion_id,
                            state.created,
                            &state.model,
                            json!({"content": delta}),
                            Value::Null,
                        );
                        return Some((Ok(Event::default().data(chunk.to_string())), state));
                    }
                    ServerFrame::Event(event) if event.event == EventKind::ApprovalRequired => {
                        let event=Event::default().event("interaction.pending").data(json!({"status":"waiting_approval","interaction":event.data,"run_id":event.run_id,"cursor":event.seq}).to_string());
                        state.done_pending = true;
                        return Some((Ok(event), state));
                    }
                    ServerFrame::Event(_) => {}
                    ServerFrame::Response(response) => {
                        let chunk = if let Some(error) = response.error {
                            json!({"error": {"code": error.code, "message": error.message}})
                        } else {
                            completion_chunk(
                                &state.completion_id,
                                state.created,
                                &state.model,
                                json!({}),
                                json!("stop"),
                            )
                        };
                        state.done_pending = true;
                        return Some((Ok(Event::default().data(chunk.to_string())), state));
                    }
                }
            }
        },
    )
}

async fn collect_answer(mut rpc: RpcStream, _client: &DaemonClient) -> Result<String, String> {
    while let Some(frame) = rpc.next().await {
        match frame {
            ServerFrame::Event(event) if event.event == EventKind::ApprovalRequired => {
                return Err(json!({"status":"waiting_approval","interaction":event.data,"run_id":event.run_id,"cursor":event.seq}).to_string());
            }
            ServerFrame::Event(_) => {}
            ServerFrame::Response(response) => {
                if let Some(error) = response.error {
                    return Err(format!("daemon RPC {}: {}", error.code, error.message));
                }
                return response
                    .result
                    .and_then(|result| result["content"].as_str().map(str::to_owned))
                    .ok_or_else(|| "daemon 响应缺少 content".to_owned());
            }
        }
    }
    Err("daemon 在终态响应前断开".to_owned())
}

fn is_authorized(headers: &HeaderMap, required_token: Option<&str>) -> bool {
    let Some(required_token) = required_token else {
        return true;
    };
    let provided = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    token_matches(provided, Some(required_token))
}

fn token_matches(provided: Option<&str>, required: Option<&str>) -> bool {
    match required {
        Some(required) => provided == Some(required),
        None => true,
    }
}

fn extract_latest_user_prompt(messages: &[OpenAiMessage]) -> Option<String> {
    messages.iter().rev().find_map(|message| {
        if message.role != "user" {
            return None;
        }
        let content = match &message.content {
            Value::String(content) => content.clone(),
            Value::Array(parts) => parts
                .iter()
                .filter(|part| part["type"] == "text")
                .filter_map(|part| part["text"].as_str())
                .collect::<Vec<&str>>()
                .join("\n"),
            _ => String::new(),
        };
        (!content.trim().is_empty()).then_some(content)
    })
}

fn completion_chunk(
    id: &str,
    created: u64,
    model: &str,
    delta: Value,
    finish_reason: Value,
) -> Value {
    json!({
        "id": id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}]
    })
}

static NEXT_ERROR_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
fn api_error(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        Json(json!({"error": {"code":format!("HTTP_{}",status.as_u16()),"message":message.into(),"type":"my_agent_error","retryable":matches!(status.as_u16(),429|503|504),"safe_auto_retry":false,"request_id":format!("gateway-{}",NEXT_ERROR_ID.fetch_add(1,std::sync::atomic::Ordering::Relaxed)),"recovery_action":if status==StatusCode::UNAUTHORIZED{"refresh_credentials"}else{"read_durable_state_before_retry"}}})),
    )
        .into_response()
}

fn request_id_text(id: &RequestId) -> String {
    match id {
        RequestId::Number(value) => value.to_string(),
        RequestId::String(value) => value.clone(),
    }
}

fn unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

#[cfg(test)]
struct TestHost(DaemonClient);
#[cfg(test)]
#[async_trait::async_trait]
impl GatewayHost for TestHost {
    async fn connect_workspace(&self, _workspace: &Path) -> Result<DaemonClient> {
        Ok(self.0.clone())
    }
    fn active_model(&self, fallback: &str) -> String {
        fallback.to_owned()
    }
    fn config_path(&self) -> PathBuf {
        PathBuf::from("test-config.json")
    }
    fn list_models(&self) -> Result<Value> {
        anyhow::bail!("此 fixture 不提供配置")
    }
    fn save_configuration(&self, _value: Value, _activate: bool) -> Result<Value> {
        anyhow::bail!("此 fixture 不写配置")
    }
    fn activate_configuration(&self, _profile_id: &str) -> Result<Value> {
        anyhow::bail!("此 fixture 不写配置")
    }
}

async fn artifact_readback(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    Json(request): Json<HttpPlanRequest<agent_daemon_protocol::params::ArtifactReadParams>>,
) -> Response {
    let params = match serde_json::to_value(request.params) {
        Ok(params) => params,
        Err(_) => return api_error(StatusCode::BAD_REQUEST, "artifact 参数无效"),
    };
    plan_rpc(
        &state,
        &headers,
        request.workspace.as_deref(),
        "artifacts.read",
        params,
    )
    .await
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct OutboxQuery {
    workspace: Option<PathBuf>,
    #[serde(default)]
    after: u64,
    #[serde(default = "outbox_limit")]
    limit: u32,
}
fn outbox_limit() -> u32 {
    32
}
async fn run_fact_readback(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    Json(request): Json<HttpPlanRequest<agent_daemon_protocol::params::RunReadParams>>,
) -> Response {
    let params = match serde_json::to_value(request.params) {
        Ok(params) => params,
        Err(_) => return api_error(StatusCode::BAD_REQUEST, "run读回参数无效"),
    };
    plan_rpc(
        &state,
        &headers,
        request.workspace.as_deref(),
        "run.read",
        params,
    )
    .await
}
async fn run_tools_readback(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    Json(request): Json<HttpPlanRequest<agent_daemon_protocol::params::RunReadParams>>,
) -> Response {
    let params = match serde_json::to_value(request.params) {
        Ok(params) => params,
        Err(_) => return api_error(StatusCode::BAD_REQUEST, "run读回参数无效"),
    };
    plan_rpc(
        &state,
        &headers,
        request.workspace.as_deref(),
        "run.tools",
        params,
    )
    .await
}
async fn run_provider_readback(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    Json(request): Json<HttpPlanRequest<agent_daemon_protocol::params::RunReadParams>>,
) -> Response {
    let params = match serde_json::to_value(request.params) {
        Ok(params) => params,
        Err(_) => return api_error(StatusCode::BAD_REQUEST, "run读回参数无效"),
    };
    plan_rpc(
        &state,
        &headers,
        request.workspace.as_deref(),
        "run.provider_attempts",
        params,
    )
    .await
}
async fn run_events_readback(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    Json(request): Json<HttpPlanRequest<agent_daemon_protocol::params::RunEventsParams>>,
) -> Response {
    let params = match serde_json::to_value(request.params) {
        Ok(params) => params,
        Err(_) => return api_error(StatusCode::BAD_REQUEST, "run 事件参数无效"),
    };
    plan_rpc(
        &state,
        &headers,
        request.workspace.as_deref(),
        "run.events",
        params,
    )
    .await
}
async fn outbox_readback(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    Query(query): Query<OutboxQuery>,
) -> Response {
    plan_rpc(
        &state,
        &headers,
        query.workspace.as_deref(),
        "outbox.readback",
        serde_json::json!({"after":query.after,"limit":query.limit}),
    )
    .await
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityQuery {
    workspace: Option<PathBuf>,
}
async fn runtime_doctor(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    Query(query): Query<IdentityQuery>,
) -> Response {
    plan_rpc(
        &state,
        &headers,
        query.workspace.as_deref(),
        "runtime.doctor",
        json!({}),
    )
    .await
}

async fn identity_readback(
    ScopeState(state): ScopeState,
    headers: HeaderMap,
    Query(query): Query<IdentityQuery>,
) -> Response {
    plan_rpc(
        &state,
        &headers,
        query.workspace.as_deref(),
        "identity.readback",
        serde_json::json!({}),
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;
    use anyhow::Result;
    use async_trait::async_trait;
    use tokio_tungstenite::connect_async;
    use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;

    use agent_daemon::DaemonState;
    use agent_daemon::approval::ApprovalBroker;
    use agent_daemon::server::InMemoryServer;
    use agent_runtime::context::{ContextConfig, ContextManager};
    use agent_runtime::loop_engine::LoopEngine;
    use agent_runtime::plan::PlanStore;
    use agent_runtime::provider::{Message, Provider, Response, ToolCall, ToolSpec};
    use agent_runtime::safety::Approval;
    use agent_runtime::session::SessionStore;
    use agent_runtime::tools::{Tool, ToolRegistry};

    static NEXT_TEST: AtomicUsize = AtomicUsize::new(0);

    struct MockProvider {
        responses: StdMutex<VecDeque<Response>>,
    }

    #[async_trait]
    impl Provider for MockProvider {
        async fn chat(&self, _messages: &[Message], _tools: &[ToolSpec]) -> Result<Response> {
            self.responses
                .lock()
                .map_err(|_| anyhow::anyhow!("mock provider 锁已损坏"))?
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("mock 响应不足"))
        }
    }

    struct ApprovalTool {
        approvals: ApprovalBroker,
    }

    #[async_trait]
    impl Tool for ApprovalTool {
        fn name(&self) -> &str {
            "danger"
        }

        fn description(&self) -> &str {
            "测试 WebSocket 审批往返"
        }

        fn parameters(&self) -> Value {
            json!({"type": "object"})
        }

        async fn execute(&self, _args: Value) -> Result<String> {
            if self
                .approvals
                .request("执行高风险 WebSocket 测试动作")
                .await?
            {
                Ok("approved".to_owned())
            } else {
                anyhow::bail!("测试动作被拒绝")
            }
        }
    }

    async fn websocket_test_state() -> (ApiState, std::path::PathBuf) {
        let id = NEXT_TEST.fetch_add(1, Ordering::SeqCst);
        let session_path = std::env::temp_dir().join(format!(
            "my-agent-websocket-{}-{id}.jsonl",
            std::process::id()
        ));
        let provider: Arc<dyn Provider> = Arc::new(MockProvider {
            responses: StdMutex::new(VecDeque::from([
                Response::ToolCalls(vec![ToolCall {
                    id: "ws-danger-search".to_owned(),
                    name: "tool_search".to_owned(),
                    arguments: json!({"query":"select:danger","limit":1}),
                }]),
                Response::ToolCalls(vec![ToolCall {
                    id: "ws-danger".to_owned(),
                    name: "tool_search".to_owned(),
                    arguments: json!({"name":"danger","arguments":{}}),
                }]),
                Response::Text("WebSocket 审批后完成".to_owned()),
            ])),
        });
        let approvals = ApprovalBroker::new();
        let mut tools = ToolRegistry::new();
        tools.register(ApprovalTool {
            approvals: approvals.clone(),
        });
        let context = ContextManager::new(
            provider.clone(),
            std::env::current_dir().expect("测试工作区应存在"),
            ContextConfig {
                token_budget: 1_000_000,
                recent_messages: 100,
                mild_compression_percent: 60,
                strong_compression_percent: 85,
                summary_chunk_tokens: 100_000,
            },
            Arc::new(PlanStore::memory_only()),
        )
        .expect("测试上下文应可创建");
        let session = Arc::new(SessionStore::new(&session_path));
        let engine = Arc::new(LoopEngine::new(provider, tools, context, session.clone()));
        let daemon = Arc::new(DaemonState::new(engine, Vec::new(), session, approvals));
        let workspace = std::env::current_dir().expect("测试工作区应存在");
        let client = InMemoryServer::start(daemon);
        (
            ApiState {
                a2a_url:Some("http://127.0.0.1/a2a".into()),
                workspaces: WorkspaceRouter::new(workspace.clone(), client),
                model: "test-model".to_owned(),
                bearer_token: Some("test-token".to_owned()),
                oidc: None,
                connections: Arc::new(tokio::sync::Semaphore::new(256)),
                authentications: Arc::new(tokio::sync::Semaphore::new(32)),
                gateway_token_path: None,
                rate_limiter: Arc::new(Mutex::new(crate::rate_limit::RateLimiter::default())),
                runtime_routes: None,
                scope_routes:Arc::new(std::collections::BTreeMap::new()),
                selected_profile:None,
                workspace,
            },
            session_path,
        )
    }

    #[test]
    fn extracts_latest_string_or_content_parts() {
        let messages = vec![
            OpenAiMessage {
                role: "user".to_owned(),
                content: Value::String("旧问题".to_owned()),
            },
            OpenAiMessage {
                role: "assistant".to_owned(),
                content: Value::String("旧回答".to_owned()),
            },
            OpenAiMessage {
                role: "user".to_owned(),
                content: json!([
                    {"type": "text", "text": "第一段"},
                    {"type": "image_url", "image_url": {"url": "ignored"}},
                    {"type": "text", "text": "第二段"}
                ]),
            },
        ];

        assert_eq!(
            extract_latest_user_prompt(&messages).as_deref(),
            Some("第一段\n第二段")
        );
    }

    #[tokio::test]
    async fn embeds_the_web_console_assets() {
        let index = web_index().await;
        assert_eq!(index.status(), StatusCode::OK);
        assert_eq!(
            index.headers()[axum::http::header::CONTENT_TYPE],
            "text/html; charset=utf-8"
        );
        let body = axum::body::to_bytes(index.into_body(), usize::MAX)
            .await
            .expect("首页 body 应可读取");
        let html = String::from_utf8(body.to_vec()).expect("首页应为 UTF-8");
        assert!(html.contains("my-agent · 本地工作台"));
        assert!(html.contains("id=\"agent-view\""));
        assert!(html.contains("id=\"sessions-view\""));
        assert!(html.contains("id=\"workspace-dialog\""));
        assert!(html.contains("id=\"permissions-dialog\""));
        assert!(html.contains("id=\"session-list\""));
        assert!(WEB_APP.contains("session.trace"));
        assert!(WEB_APP.contains("session.load_page"));
        assert!(WEB_APP.contains("session.trace_page"));
        assert!(WEB_APP.contains("loadMoreMessages"));
        assert!(WEB_APP.contains("loadMoreTraces"));
        assert!(WEB_APP.contains("permissions.get"));
        assert!(WEB_APP.contains("permissions.set"));
        assert!(WEB_APP.contains("scheduleAgentTranscriptRender"));
        assert!(WEB_APP.contains("renderMarkdown"));
        assert!(WEB_APP.contains("isTableSeparator"));
        assert!(WEB_APP.contains("safeMarkdownUrl"));
        assert!(WEB_APP.contains("thinking_delta"));
        assert!(WEB_APP.contains("thinking_finished"));
        assert!(WEB_APP.contains("工具动态 · "));
        assert!(WEB_APP.contains("workspace:"));
        assert!(WEB_APP.contains("sessionDayKey"));
        assert!(WEB_APP.contains("toggleSessionDay"));
        assert!(WEB_STYLES.contains(".agent-layout"));
        assert!(WEB_STYLES.contains(".session-day.is-collapsed"));
        assert!(WEB_STYLES.contains(".streaming-cursor"));
        assert!(WEB_STYLES.contains(".tool-details"));
        assert!(WEB_STYLES.contains(".thinking-details"));
        assert!(WEB_STYLES.contains(".message-content table"));
    }

    #[test]
    fn bearer_auth_is_only_enforced_when_configured() {
        let mut headers = HeaderMap::new();
        assert!(is_authorized(&headers, None));
        assert!(!is_authorized(&headers, Some("secret")));
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer secret".parse().unwrap(),
        );
        assert!(is_authorized(&headers, Some("secret")));
    }

    #[test]
    fn websocket_origin_must_match_the_host_when_present() {
        let mut headers = HeaderMap::new();
        assert!(websocket_origin_allowed(&headers));
        headers.insert(axum::http::header::HOST, "127.0.0.1:8787".parse().unwrap());
        headers.insert(
            axum::http::header::ORIGIN,
            "http://127.0.0.1:8787".parse().unwrap(),
        );
        assert!(websocket_origin_allowed(&headers));
        headers.insert(
            axum::http::header::ORIGIN,
            "https://example.com".parse().unwrap(),
        );
        assert!(!websocket_origin_allowed(&headers));
    }

    #[tokio::test]
    async fn directory_listing_only_returns_directories() {
        let id = NEXT_TEST.fetch_add(1, Ordering::SeqCst);
        let root = std::env::temp_dir().join(format!(
            "my-agent-directory-listing-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(root.join("alpha")).unwrap();
        std::fs::create_dir_all(root.join("beta")).unwrap();
        std::fs::write(root.join("notes.txt"), b"not a directory").unwrap();

        let listing = directory_listing(Some(&root), &root)
            .await
            .expect("目录应可列出");
        let names = listing["directories"]
            .as_array()
            .expect("directories 应为数组")
            .iter()
            .filter_map(|entry| entry["name"].as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["alpha", "beta"]);

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn websocket_requires_token_and_supports_interactive_approval() {
        let (state, session_path) = websocket_test_state().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("测试端口应可监听");
        let address = listener.local_addr().expect("测试端口应有地址");
        let server = tokio::spawn(async move {
            axum::serve(listener, api_router(state))
                .await
                .expect("测试 Web 服务应正常运行");
        });
        let url = format!("ws://{address}/ws");

        let (mut rejected, _) = connect_async(&url).await.expect("WebSocket 应可升级");
        rejected
            .send(TungsteniteMessage::Text(
                json!({"type": "connect", "token": "wrong"})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("错误 token 帧应可发送");
        let rejected_message = rejected
            .next()
            .await
            .expect("应返回鉴权错误")
            .expect("鉴权错误帧应可读取")
            .into_text()
            .expect("鉴权错误应为文本");
        assert_eq!(
            serde_json::from_str::<Value>(&rejected_message).expect("错误帧应为 JSON")["type"],
            "error"
        );

        let (mut socket, _) = connect_async(&url).await.expect("WebSocket 应可再次升级");
        socket
            .send(TungsteniteMessage::Text(
                json!({"type": "connect", "token": "test-token"})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("connect 帧应可发送");
        let connected = socket
            .next()
            .await
            .expect("应返回 connected")
            .expect("connected 帧应可读取")
            .into_text()
            .expect("connected 应为文本");
        let connected = serde_json::from_str::<Value>(&connected).expect("connected 应为 JSON");
        assert_eq!(connected["type"], "connected");
        assert_eq!(
            connected["workspace"],
            std::env::current_dir()
                .expect("测试工作区应存在")
                .to_string_lossy()
                .as_ref()
        );
        let recovery = socket
            .next()
            .await
            .expect("应返回恢复快照")
            .expect("恢复快照应可读取")
            .into_text()
            .expect("恢复快照应为文本");
        assert_eq!(
            serde_json::from_str::<Value>(&recovery).expect("恢复快照应为 JSON")["type"],
            "recovery"
        );
        socket
            .send(TungsteniteMessage::Text(
                json!({
                    "jsonrpc": "2.0",
                    "id": "web-chat",
                    "method": "chat.send",
                    "params": {"message": "执行测试动作"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("chat 请求应可发送");

        let mut saw_approval = false;
        let mut saw_text = false;
        let mut completed = false;
        while !completed {
            let message = tokio::time::timeout(std::time::Duration::from_secs(2), socket.next())
                .await
                .expect("WebSocket 响应不应超时")
                .expect("WebSocket 不应提前关闭")
                .expect("WebSocket 帧应可读取")
                .into_text()
                .expect("daemon 帧应为文本");
            let frame: ServerFrame =
                serde_json::from_str(&message).expect("daemon 帧应符合 ServerFrame");
            match frame {
                ServerFrame::Event(event) if event.event == EventKind::ApprovalRequired => {
                    saw_approval = true;
                    let approval_id = event.data["approval"]["id"]
                        .as_str()
                        .expect("审批事件应有 id");
                    socket
                        .send(TungsteniteMessage::Text(
                            json!({
                                "jsonrpc": "2.0",
                                "id": "web-approval",
                                "method": "approval.respond",
                                "params": {"approval_id": approval_id, "approved": true}
                            })
                            .to_string()
                            .into(),
                        ))
                        .await
                        .expect("审批响应应可发送");
                }
                ServerFrame::Event(event) if event.event == EventKind::TextDelta => {
                    saw_text |= event.data["delta"] == "WebSocket 审批后完成";
                }
                ServerFrame::Response(response)
                    if response.id == RequestId::String("web-chat".to_owned()) =>
                {
                    assert!(response.error.is_none());
                    completed = true;
                }
                ServerFrame::Event(_) | ServerFrame::Response(_) => {}
            }
        }

        assert!(saw_approval);
        assert!(saw_text);
        server.abort();
        let _ = server.await;
        let _ = std::fs::remove_file(session_path);
    }

    #[tokio::test]
    async fn websocket_reconnect_recovers_pending_approval_and_active_output() {
        let (state, session_path) = websocket_test_state().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("测试端口应可监听");
        let address = listener.local_addr().expect("测试端口应有地址");
        let server = tokio::spawn(async move {
            axum::serve(listener, api_router(state))
                .await
                .expect("测试 Web 服务应正常运行");
        });
        let url = format!("ws://{address}/ws");

        let (mut first, _) = connect_async(&url).await.expect("首次连接应成功");
        first
            .send(TungsteniteMessage::Text(
                json!({"type": "connect", "token": "test-token"})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("首次 connect 应可发送");
        let _ = first.next().await.expect("应收到 connected");
        let _ = first.next().await.expect("应收到首次 recovery");
        first
            .send(TungsteniteMessage::Text(
                json!({
                    "jsonrpc": "2.0",
                    "id": "lost-ws-chat",
                    "method": "chat.send",
                    "params": {"message": "断线 WebSocket 恢复"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("chat 请求应可发送");
        loop {
            let message = tokio::time::timeout(Duration::from_secs(2), first.next())
                .await
                .expect("首次连接应收到审批")
                .expect("首次连接不应提前结束")
                .expect("首次 WebSocket 帧应可读取")
                .into_text()
                .expect("首次 daemon 帧应为文本");
            let frame: ServerFrame = serde_json::from_str(&message).expect("daemon 帧应为 JSON");
            if matches!(
                frame,
                ServerFrame::Event(event) if event.event == EventKind::ApprovalRequired
            ) {
                break;
            }
        }
        drop(first);

        let (mut second, _) = connect_async(&url).await.expect("重连应成功");
        second
            .send(TungsteniteMessage::Text(
                json!({"type": "connect", "token": "test-token"})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("重连 connect 应可发送");
        assert_eq!(
            serde_json::from_str::<Value>(
                &second
                    .next()
                    .await
                    .expect("应收到重连 connected")
                    .expect("connected 帧应可读取")
                    .into_text()
                    .expect("connected 应为文本"),
            )
            .expect("connected 应为 JSON")["type"],
            "connected"
        );
        let recovery = second
            .next()
            .await
            .expect("应收到重连 recovery")
            .expect("recovery 帧应可读取")
            .into_text()
            .expect("recovery 应为文本");
        let recovery_value: Value = serde_json::from_str(&recovery).expect("recovery 应为 JSON");
        assert_eq!(recovery_value["type"], "recovery");
        assert_eq!(
            recovery_value["snapshot"]["pending_approvals"]
                .as_array()
                .expect("恢复快照应包含审批")
                .len(),
            1
        );

        let mut saw_text = false;
        let mut completed = false;
        while !completed {
            let message = tokio::time::timeout(Duration::from_secs(2), second.next())
                .await
                .expect("重连订阅不应超时")
                .expect("重连 WebSocket 不应提前关闭")
                .expect("重连帧应可读取")
                .into_text()
                .expect("重连 daemon 帧应为文本");
            let frame: ServerFrame = serde_json::from_str(&message).expect("重连帧应为 JSON");
            match frame {
                ServerFrame::Event(event) if event.event == EventKind::ApprovalRequired => {
                    let approval_id = event.data["approval"]["id"]
                        .as_str()
                        .expect("恢复审批应包含 id");
                    second
                        .send(TungsteniteMessage::Text(
                            json!({
                                "jsonrpc": "2.0",
                                "id": "reconnect-approval",
                                "method": "approval.respond",
                                "params": {"approval_id": approval_id, "approved": true}
                            })
                            .to_string()
                            .into(),
                        ))
                        .await
                        .expect("恢复审批响应应可发送");
                }
                ServerFrame::Event(event) if event.event == EventKind::TextDelta => {
                    saw_text |= event.data["delta"] == "WebSocket 审批后完成";
                }
                ServerFrame::Response(response)
                    if response
                        .result
                        .as_ref()
                        .is_some_and(|result| result.get("content").is_some()) =>
                {
                    assert!(response.error.is_none());
                    completed = true;
                }
                ServerFrame::Event(_) | ServerFrame::Response(_) => {}
            }
        }
        assert!(saw_text);
        server.abort();
        let _ = server.await;
        let _ = std::fs::remove_file(session_path);
    }
}

#[derive(serde::Deserialize)]
#[serde(tag="command",content="value",rename_all="snake_case",deny_unknown_fields)]
enum WorkflowHttpCommand {Publish(agent_core::WorkflowPublish),Start(agent_core::WorkflowStart)}
async fn workflow_command(ScopeState(state):ScopeState,headers:HeaderMap,Json(c):Json<WorkflowHttpCommand>)->Response{match c{WorkflowHttpCommand::Publish(v)=>credential_command(&state,&headers,"workflow.publish",json!(v)).await,WorkflowHttpCommand::Start(v)=>credential_command(&state,&headers,"workflow.start",json!(v)).await}}
async fn workflow_read(ScopeState(state):ScopeState,headers:HeaderMap,Query(p):Query<agent_daemon_protocol::params::WorkflowReadParams>)->Response{credential_command(&state,&headers,"workflow.read",json!(p)).await}
async fn workflow_control(ScopeState(state):ScopeState,headers:HeaderMap,Json(c):Json<agent_core::WorkflowControl>)->Response{credential_command(&state,&headers,"workflow.control",json!(c)).await}
