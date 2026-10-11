use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::time::Instant;
use tokio::sync::{Mutex, mpsc};
use tracing::{Instrument, info, info_span};

use super::approval::PendingApprovalInfo;
use super::{ActiveKey, ActiveRequest, ActiveRequestUpdate, DaemonState, SessionRuntime};
use agent_core::{CoordinationError, TenantContext};
use agent_daemon_protocol::slash::{SlashAction, SlashParse, SlashRegistry, SlashResponse};
use agent_daemon_protocol::{
    EventKind, JsonRpcRequest, JsonRpcResponse, MAX_FRAME_BYTES, RequestId, ServerFrame,
};
use agent_runtime::config::ProfileSummary;
use agent_runtime::cron::ScheduleSpec;
use agent_runtime::loop_engine::{AgentEvent, CancellationToken};
use agent_runtime::provider::ProviderProfile;
use agent_runtime::safety::{SafetyMode, SafetyPolicy};
use agent_runtime::session::SessionTraceRecord;
use agent_runtime::tools::ReadFileTool;
use agent_storage::{
    Admission, AdmissionMode, DelegationRecord, DelegationRequest, EventSeq, InteractionId, RunId,
    RunStatus, RuntimeError, SessionId, StoredEvent,
};

const INVALID_PARAMS: i64 = -32602;
const METHOD_NOT_FOUND: i64 = -32601;
const INTERNAL_ERROR: i64 = -32603;

tokio::task_local! {
    pub(crate) static AUTHENTICATED_IDENTITY:agent_core::RuntimeIdentity;
    pub(crate) static WORKFLOW_LEASE:agent_core::WorkflowNodeLease;
    pub(crate) static WORKFLOW_DIRECTIVE:agent_core::WorkflowNativeDirective;
    static CREDENTIAL_EXPIRY:u64;
    static REQUEST_CHANNEL:agent_core::HookChannel;
}
const REQUEST_CANCELLED: i64 = -32800;
const REQUEST_CONFLICT: i64 = -32001;
const WEB_PAGE_MAX_BYTES: usize = MAX_FRAME_BYTES - 256 * 1024;
const WEB_PAGE_MAX_ITEM_BYTES: usize = 256 * 1024;
const WEB_PAGE_MAX_LIMIT: usize = 200;

struct AbortTimer(tokio::task::JoinHandle<()>);

impl Drop for AbortTimer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

use agent_daemon_protocol::params::*;
type ModelSaveParams = agent_daemon_protocol::params::ModelSaveParams<ProviderProfile>;

impl DaemonState {
    async fn run_fact_value(&self, run: &RunId, view: &str) -> Result<Value, (i64, String)> {
        let scope = self.scope();
        match view {
            "fact" => self
                .run_fact_store
                .read_run_fact(&scope, run)
                .await
                .map(|read| {
                    let mut fact = json!(read.run);
                    if let Some(compact) = read.compact {
                        fact["compact"] = json!(compact);
                    }
                    if let Some(stamp) = read.view_stamp {
                        fact["_my_agent_view"] = json!(stamp);
                    }
                    fact["snapshot"] = json!(read.snapshot);
                    fact["continuation_parent_run_id"] = json!(read.continuation_parent_run_id);
                    fact["continuation_run_id"] = json!(read.continuation_run_id);
                    fact
                })
                .map_err(|e| (e.rpc_code(), e.to_string())),
            "tools" => self
                .run_fact_store
                .read_tool_receipts(&scope, run)
                .await
                .map(|receipts| json!({"receipts":receipts}))
                .map_err(|e| (e.rpc_code(), e.to_string())),
            "provider" => self
                .run_fact_store
                .read_provider_history(&scope, run)
                .await
                .map(|value| json!(value))
                .map_err(|e| (e.rpc_code(), e.to_string())),
            _ => Err((INVALID_PARAMS, "run视图必须为fact/tools/provider".into())),
        }
    }
    async fn run_event_readback(
        &self,
        params: &RunEventsParams,
    ) -> Result<agent_core::RunEventPage, agent_core::CoordinationError> {
        let limit = u32::try_from(params.limit.unwrap_or(200))
            .map_err(|_| agent_core::CoordinationError::InvalidRequest)?;
        self.event_store
            .read_run_events(
                &self.scope(),
                &params.run_id,
                params.after_seq.unwrap_or(EventSeq(0)),
                limit,
            )
            .await
    }
    async fn commit_session_authorized(
        &self,
        command: &agent_core::SessionCommand,
        expected: Option<&agent_core::SessionLifetimeId>,
        operation: &str,
    ) -> Result<agent_core::SessionMetadata, agent_core::RepositoryError> {
        let authorization = self
            .authorize_local_request(
                RequestId::String(operation.into()),
                agent_core::session_command_action(command),
            )
            .await
            .map_err(RuntimeError::repository_error)?;
        self.lifecycle_store
            .commit_session_authorized(
                &authorization,
                command,
                expected,
                operation,
                Arc::new(agent_policy::PolicyEngine),
            )
            .await
    }
    async fn local_identity(&self) -> Result<agent_core::IdentityReadback, RuntimeError> {
        let subject = self.request_identity().principal.subject_id;
        self.identity_store
            .read_identity(&self.scope(), &subject)
            .await
            .map_err(Into::into)
    }
    pub(super) async fn authorize_local_request(
        &self,
        request_id: RequestId,
        action: agent_core::Action,
    ) -> Result<agent_core::AuthorizationRequest, RuntimeError> {
        use agent_core::AuthorizationVerifier;
        let identity = self.local_identity().await?;
        let context =
            agent_identity::bound_context(&self.request_identity(), &identity, request_id)?;
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| RuntimeError::Internal("系统时钟早于 Unix epoch".into()))?
            .as_millis()
            .min(i64::MAX as u128) as i64;
        let request = agent_core::AuthorizationRequest {
            context,
            expected_policy: identity.policy.version.clone(),
            action,
            attributes: agent_core::AuthorizationAttributes {
                workspace: self.safety.as_ref().map_or_else(
                    || ".".into(),
                    |s| s.workspace().to_string_lossy().into_owned(),
                ),
                entry_channel: REQUEST_CHANNEL
                    .try_with(|channel| *channel)
                    .unwrap_or(agent_core::HookChannel::DaemonRpc),
                tool_effect: None,
                timestamp_ms,
                network: agent_core::NetworkBoundary::None,
            },
        };
        agent_policy::PolicyEngine.verify(&request, &identity)?;
        Ok(request)
    }

    /// 网络凭据先验证，随后沿原 handler/准入事务执行；内部恢复仍使用固定 workload。
    pub(crate) async fn handle_transport_request(
        self: Arc<Self>,
        mut request: JsonRpcRequest,
        frames: super::frames::FrameSender,
    ) {
        let channel = if let Some(gateway) = request.gateway.take() {
            if self.verify_gateway(&gateway, &request.id).await.is_err() {
                send_authorization_error(
                    &frames,
                    request.id,
                    agent_core::AuthorizationError::Unauthenticated.into(),
                );
                return;
            }
            gateway.channel
        } else {
            agent_core::HookChannel::DaemonRpc
        };
        if matches!(
            request.method.as_str(),
            "chat.send" | "compact.start" | "slash.execute"
        ) {
            request.params["entry_channel"] = json!(channel);
        }
        let binding = match (&self.oidc, request.authorization.take()) {
            (Some(_), Some(token)) => match self.verify_transport_token(token.as_str()).await {
                Ok(binding) => Some(binding),
                Err(_) => {
                    send_authorization_error(
                        &frames,
                        request.id,
                        agent_core::AuthorizationError::Unauthenticated.into(),
                    );
                    return;
                }
            },
            (Some(_), None) | (None, Some(_)) => {
                send_authorization_error(
                    &frames,
                    request.id,
                    agent_core::AuthorizationError::Unauthenticated.into(),
                );
                return;
            }
            (None, None) => None,
        };
        let dispatch = async {
            match binding {
                Some(binding) => {
                    CREDENTIAL_EXPIRY
                        .scope(
                            binding.expires_at,
                            AUTHENTICATED_IDENTITY
                                .scope(binding.identity, self.handle_request(request, frames)),
                        )
                        .await
                }
                None => self.handle_request(request, frames).await,
            }
        };
        REQUEST_CHANNEL.scope(channel, dispatch).await;
    }
    async fn verify_transport_token(
        &self,
        token: &str,
    ) -> Result<agent_identity::VerifiedToken, RuntimeError> {
        let binding = if token.starts_with("ak.") {
            let meta = self
                .credential_store
                .authenticate_api_credential(
                    &self.scope(),
                    &agent_core::SecretToken::new(token.into())?,
                )
                .await?;
            agent_identity::VerifiedToken {
                identity: agent_core::RuntimeIdentity {
                    scope: meta.scope,
                    principal: meta.principal,
                },
                expires_at: meta.expires_at_ms.max(0) as u64 / 1000,
            }
        } else {
            self.oidc
                .as_ref()
                .ok_or(agent_core::AuthorizationError::Unauthenticated)?
                .verify(token)
                .await?
        };
        if binding.identity.scope != self.scope() {
            return Err(agent_core::AuthorizationError::Unauthenticated.into());
        }
        Ok(binding)
    }
    async fn verify_gateway(
        &self,
        gateway: &agent_daemon_protocol::GatewayCredential,
        id: &RequestId,
    ) -> Result<(), RuntimeError> {
        use agent_core::AuthorizationVerifier;
        let verifier = self
            .oidc
            .as_ref()
            .ok_or(agent_core::AuthorizationError::Unauthenticated)?;
        if !matches!(
            gateway.channel,
            agent_core::HookChannel::Http | agent_core::HookChannel::WebSocket
        ) {
            return Err(agent_core::AuthorizationError::Unauthenticated.into());
        }
        let primary = self
            .verify_transport_token(gateway.token.as_str())
            .await?
            .identity;
        if primary.principal.kind == agent_core::SubjectKind::Human
            || verifier.gateway_subject() != Some(&primary.principal.subject_id)
        {
            return Err(agent_core::AuthorizationError::Unauthenticated.into());
        }
        let identity = self
            .identity_store
            .read_identity(&primary.scope, &primary.principal.subject_id)
            .await?;
        let context = agent_identity::bound_context(&primary, &identity, id.clone())?;
        let authorization = agent_core::AuthorizationRequest {
            context,
            expected_policy: identity.policy.version.clone(),
            action: agent_core::Action::GatewayDelegate,
            attributes: agent_core::AuthorizationAttributes {
                workspace: self.safety.as_ref().map_or_else(
                    || ".".into(),
                    |s| s.workspace().to_string_lossy().into_owned(),
                ),
                entry_channel: gateway.channel,
                tool_effect: None,
                timestamp_ms: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |t| t.as_millis().min(i64::MAX as u128) as i64),
                network: agent_core::NetworkBoundary::None,
            },
        };
        agent_policy::PolicyEngine.verify(&authorization, &identity)?;
        Ok(())
    }

    pub(crate) async fn handle_request(
        self: Arc<Self>,
        request: JsonRpcRequest,
        frames: super::frames::FrameSender,
    ) {
        let channel = REQUEST_CHANNEL
            .try_with(|channel| *channel)
            .unwrap_or_else(|_| {
                request
                    .params
                    .get("entry_channel")
                    .and_then(|value| serde_json::from_value(value.clone()).ok())
                    .unwrap_or(agent_core::HookChannel::DaemonRpc)
            });
        REQUEST_CHANNEL
            .scope(channel, self.handle_request_inner(request, frames))
            .await;
    }
    async fn handle_request_inner(
        self: Arc<Self>,
        request: JsonRpcRequest,
        frames: super::frames::FrameSender,
    ) {
        let request = match agent_daemon_protocol::normalize_request(request.clone()) {
            Ok(request) => request,
            Err(error) => {
                send_result(
                    &frames,
                    request.id,
                    Err((INVALID_PARAMS, error.to_string())),
                );
                return;
            }
        };
        let Some(action) = agent_policy::action_for_rpc(&request.method) else {
            send_result(
                &frames,
                request.id,
                Err((
                    METHOD_NOT_FOUND,
                    format!("未知 RPC 方法: {}", request.method),
                )),
            );
            return;
        };
        if let Err(error) = self
            .authorize_local_request(request.id.clone(), action)
            .await
        {
            send_authorization_error(&frames, request.id, error);
            return;
        }
        match request.method.as_str() {
            "runtime.endpoint" => {
                send_result(&frames,request.id,Ok(json!({"scope":self.scope(),"runtime_endpoint":self.oidc.as_ref().map_or("runtime-default",|v|v.runtime_endpoint())})));
            }
            "credential.status" => {
                let result = async {
                let expires_at = CREDENTIAL_EXPIRY.try_with(|expiry| *expiry).unwrap_or(0);
                    Ok(json!(agent_identity::VerifiedToken {
                        identity: self.request_identity(),
                        expires_at
                    }))
                }.await;
                send_result(&frames,request.id,result);
            }
            "credential.issue" | "credential.revoke" => {
                let result = async {
                    let auth = self
                        .authorize_local_request(
                            request.id.clone(),
                            agent_core::Action::IdentityManage,
                        )
                        .await
                        .map_err(|e| {
                            let e = e.repository_error();
                            (e.rpc_code(), e.to_string())
                        })?;
                    let verifier: Arc<dyn agent_core::AuthorizationVerifier> =
                        Arc::new(agent_policy::PolicyEngine);
                    let result = if request.method == "credential.issue" {
                        let p: agent_daemon_protocol::params::CredentialIssueParams =
                            parse_params(&request.params).map_err(|e| (INVALID_PARAMS, e))?;
                        self.credential_store
                            .issue_api_credential(
                                &auth,
                                &p.subject_id,
                                p.ttl_secs,
                                &p.operation_id,
                                verifier,
                            )
                            .await
                            .map(|v| json!(v))
                    } else {
                        let p: agent_daemon_protocol::params::CredentialRevokeParams =
                            parse_params(&request.params).map_err(|e| (INVALID_PARAMS, e))?;
                        self.credential_store
                            .revoke_api_credential(&auth, &p.key_id, &p.operation_id, verifier)
                            .await
                            .map(|v| json!(v))
                    };
                    result.map_err(|e| (e.rpc_code(), e.to_string()))
                }
                .await;
                send_result(&frames, request.id, result);
            }
            "directory.find" => {
                let result=async {
                    let p:agent_daemon_protocol::params::DirectoryCreateParams=parse_params(&request.params).map_err(|e|(INVALID_PARAMS,e))?;
                    let auth=self.authorize_local_request(request.id.clone(),agent_core::Action::AgentRead).await.map_err(|e|{let e=e.repository_error();(e.rpc_code(),e.to_string())})?;
                    let result=self.access_store.find_directory_session(&auth,&p.operation_id,Arc::new(agent_policy::PolicyEngine)).await.map_err(|e|(e.rpc_code(),e.to_string()))?;
                    Ok(json!(result))
                }.await;
                send_result(&frames,request.id,result);
            }
            "oauth.begin" | "oauth.consume" => {
                let result = async {
                    let proof: agent_core::OAuthLoginProof = parse_params(&request.params).map_err(|e| (INVALID_PARAMS,e))?;
                    let auth = self.authorize_local_request(request.id.clone(),agent_core::Action::GatewayDelegate).await.map_err(|e| {let e=e.repository_error();(e.rpc_code(),e.to_string())})?;
                    self.access_store.oauth_login(&auth,&proof,request.method=="oauth.consume",Arc::new(agent_policy::PolicyEngine)).await.map_err(|e|(e.rpc_code(),e.to_string()))?;
                    Ok(json!({"accepted":true}))
                }.await;
                send_result(&frames,request.id,result);
            }
            "directory.create" | "directory.resolve" | "access.acquire" | "access.release" => {
                let result = async {
                    use agent_daemon_protocol::params::*;
                    let auth = self
                        .authorize_local_request(request.id.clone(), action)
                        .await
                        .map_err(|e| {
                            let error = e.repository_error();
                            (error.rpc_code(), error.to_string())
                        })?;
                    let verifier: Arc<dyn agent_core::AuthorizationVerifier> =
                        Arc::new(agent_policy::PolicyEngine);
                    let result: Result<Value, agent_core::RepositoryError> =
                        match request.method.as_str() {
                            "directory.create" => {
                                let p: DirectoryCreateParams = parse_params(&request.params)
                                    .map_err(|e| (INVALID_PARAMS, e))?;
                                let binding = serde_json::to_string(&(
                                    &auth.context.scope,
                                    &auth.context.principal.subject_id,
                                    &p.operation_id,
                                ))
                                .map_err(|_| (INVALID_PARAMS, "目录操作无效".into()))?;
                                let key = agent_core::SessionKey(
                                    self.session.session_key_for_operation(&binding),
                                );
                                self.access_store
                                    .create_directory_session(
                                        &auth,
                                        &key,
                                        self.oidc.as_ref().map_or("runtime-default",|v|v.runtime_endpoint()),
                                        &p.operation_id,
                                        verifier,
                                    )
                                    .await
                                    .map(|v| json!(v))
                            }
                            "directory.resolve" => {
                                let p: DirectoryResolveParams = parse_params(&request.params)
                                    .map_err(|e| (INVALID_PARAMS, e))?;
                                self.access_store
                                    .resolve_directory_session(&auth, &p.session_id, verifier)
                                    .await
                                    .map(|v| json!(v))
                            }
                            "access.acquire" => {
                                let p: AccessAcquireParams = parse_params(&request.params)
                                    .map_err(|e| (INVALID_PARAMS, e))?;
                                self.access_store
                                    .acquire_access_at(&auth, p.class,&p.endpoint, &p.operation_id, verifier)
                                    .await
                                    .map(|v| json!(v))
                            }
                            _ => {
                                let p: AccessReleaseParams = parse_params(&request.params)
                                    .map_err(|e| (INVALID_PARAMS, e))?;
                                self.access_store
                                    .release_access(&auth, &p.operation_id, verifier)
                                    .await
                                    .map(|_| json!({"released":true}))
                            }
                        };
                    result.map_err(|e| (e.rpc_code(), e.to_string()))
                }
                .await;
                send_result(&frames, request.id, result);
            }
            "identity.manage" => {
                let result = async {
                    let p: agent_daemon_protocol::params::IdentityManageParams =
                        parse_params(&request.params).map_err(|e| (INVALID_PARAMS, e))?;
                    let auth = self
                        .authorize_local_request(
                            request.id.clone(),
                            agent_core::Action::IdentityManage,
                        )
                        .await
                        .map_err(|e| {
                            let e = e.repository_error();
                            (e.rpc_code(), e.to_string())
                        })?;
                    let receipt = self
                        .identity_management
                        .manage_identity(
                            &auth,
                            &p.command,
                            &p.operation_id,
                            Arc::new(agent_policy::PolicyEngine),
                        )
                        .await
                        .map_err(|e| (e.rpc_code(), e.to_string()))?;
                    Ok(json!(receipt))
                }
                .await;
                send_result(&frames, request.id, result);
            }
            "audit.readback" => {
                let result = match parse_params::<AuditReadParams>(&request.params) {
                    Ok(p) => self
                        .audit_store
                        .read_audit_entries(&self.scope(), p.after, p.limit)
                        .await
                        .map(|entries| json!({"entries":entries}))
                        .map_err(|e| (e.rpc_code(), e.to_string())),
                    Err(e) => Err((INVALID_PARAMS, e)),
                };
                send_result(&frames, request.id, result);
            }
            "quota.readback" => {
                let result = match parse_params::<QuotaReadParams>(&request.params) {
                    Ok(p) => self
                        .quota_store
                        .read_quota_account(&self.scope(), &p.metric, p.window_start_ms)
                        .await
                        .map(|q| json!(q))
                        .map_err(|e| (e.rpc_code(), e.to_string())),
                    Err(e) => Err((INVALID_PARAMS, e)),
                };
                send_result(&frames, request.id, result);
            }
            "workflow.message" => {let result=async {let c:agent_core::WorkflowMessage=parse_params(&request.params).map_err(|e|(INVALID_PARAMS,e))?;let auth=self.authorize_local_request(request.id.clone(),agent_core::Action::SessionPrompt).await.map_err(|e|{let e=e.repository_error();(e.rpc_code(),e.to_string())})?;self.workflow_store.submit_workflow_message(&auth,&c,Arc::new(agent_policy::PolicyEngine)).await.map(|v|json!(v)).map_err(|e|(e.rpc_code(),e.to_string()))}.await;send_result(&frames,request.id,result);}
            "workflow.publish" => {let result=async {let command:agent_core::WorkflowPublish=parse_params(&request.params).map_err(|e|(INVALID_PARAMS,e))?;let auth=self.authorize_local_request(request.id.clone(),agent_core::Action::AgentConfigure).await.map_err(|e|{let e=e.repository_error();(e.rpc_code(),e.to_string())})?;self.workflow_store.publish_workflow(&auth,&command,Arc::new(agent_policy::PolicyEngine)).await.map(|v|json!(v)).map_err(|e|(e.rpc_code(),e.to_string()))}.await;send_result(&frames,request.id,result);}
            "workflow.start" => {let result=async {let command:agent_core::WorkflowStart=parse_params(&request.params).map_err(|e|(INVALID_PARAMS,e))?;let auth=self.authorize_local_request(request.id.clone(),agent_core::Action::SessionPrompt).await.map_err(|e|{let e=e.repository_error();(e.rpc_code(),e.to_string())})?;self.workflow_store.start_workflow(&auth,&command,Arc::new(agent_policy::PolicyEngine)).await.map(|v|json!(v)).map_err(|e|(e.rpc_code(),e.to_string()))}.await;send_result(&frames,request.id,result);}
            "workflow.control" => {let result=async {let command:agent_core::WorkflowControl=parse_params(&request.params).map_err(|e|(INVALID_PARAMS,e))?;let auth=self.authorize_local_request(request.id.clone(),agent_core::Action::SessionManage).await.map_err(|e|{let e=e.repository_error();(e.rpc_code(),e.to_string())})?;self.workflow_store.control_workflow(&auth,&command,Arc::new(agent_policy::PolicyEngine)).await.map(|v|json!(v)).map_err(|e|(e.rpc_code(),e.to_string()))}.await;send_result(&frames,request.id,result);}
            "workflow.read" => {let result=async {let p:agent_daemon_protocol::params::WorkflowReadParams=parse_params(&request.params).map_err(|e|(INVALID_PARAMS,e))?;self.workflow_store.read_workflow(&self.scope(),&p.id,p.after,p.limit).await.map(|v|json!(v)).map_err(|e|(e.rpc_code(),e.to_string()))}.await;send_result(&frames,request.id,result);}
            "registry.mutate" => {
                let result=async {
                    let mutation:agent_core::RegistryMutation=parse_params(&request.params).map_err(|e|(INVALID_PARAMS,e))?;
                    let action=match &mutation.command {agent_core::RegistryCommand::ConfigureTrust{..}=>agent_core::Action::IdentityManage,agent_core::RegistryCommand::Publish{..}=>agent_core::Action::PluginInstall,_=>agent_core::Action::AgentConfigure};
                    let auth=self.authorize_local_request(request.id.clone(),action).await.map_err(|e|{let e=e.repository_error();(e.rpc_code(),e.to_string())})?;
                    let receipt=self.registry_store.mutate_registry(&auth,&mutation,Arc::new(agent_policy::PolicyEngine)).await.map_err(|e|(e.rpc_code(),e.to_string()))?;
                    Ok(json!(receipt))
                }.await;send_result(&frames,request.id,result);
            }
            "registry.generation" => {
                let result=async {let params:agent_daemon_protocol::params::RegistryGenerationParams=parse_params(&request.params).map_err(|e|(INVALID_PARAMS,e))?;
                    if params.digest.len()!=64{return Err((INVALID_PARAMS,"发行代digest无效".into()))}
                    let read=self.registry_store.read_registry(&self.scope()).await.map_err(|e|(e.rpc_code(),e.to_string()))?;
                    let generation=read.generations.into_iter().find(|g|g.pack_digest==params.digest).ok_or((agent_core::AuthorizationError::Forbidden.rpc_code(),"资源不可访问".into()))?;Ok(json!(generation))
                }.await;send_result(&frames,request.id,result);
            }
            "registry.readback" => {
                let result=self.registry_store.read_registry(&self.scope()).await.map(|v|json!({"state":v.state,"generations":v.generations.into_iter().map(|g|json!({"digest":g.pack_digest,"version":g.manifest.definition.version,"effective_config_digest":g.effective.digest,"trust_digest":g.trust_digest,"scan_digest":g.scan_digest})).collect::<Vec<_>>()})).map_err(|e|(e.rpc_code(),e.to_string()));send_result(&frames,request.id,result);
            }
            "billing.configure" => {
                let result=async {
                    let p=parse_params::<BillingConfigureParams>(&request.params).map_err(|e|(INVALID_PARAMS,e))?;
                    let auth=self.authorize_local_request(request.id.clone(),agent_core::Action::IdentityManage).await.map_err(|e|(INTERNAL_ERROR,e.to_string()))?;
                    self.billing_store.configure_billing(&auth,&p.policy,&p.operation_id,Arc::new(agent_policy::PolicyEngine)).await.map(|p|json!(p)).map_err(|e|(e.rpc_code(),e.to_string()))
                }.await;send_result(&frames,request.id,result);
            }
            "billing.accounts" => {
                let result=self.billing_store.read_billing_accounts(&self.scope()).await.map(|p|json!({"accounts":p})).map_err(|e|(e.rpc_code(),e.to_string()));
                send_result(&frames,request.id,result);
            }
            "billing.readback" => {
                let result=self.billing_store.read_billing_policy(&self.scope()).await.map(|p|json!(p)).map_err(|e|(e.rpc_code(),e.to_string()));
                send_result(&frames,request.id,result);
            }
            "billing.usage" => {
                let result=async {let p=parse_params::<BillingUsageParams>(&request.params).map_err(|e|(INVALID_PARAMS,e))?;
                    self.billing_store.read_billing_usage(&self.scope(),p.after,p.limit).await.map(|p|json!(p)).map_err(|e|(e.rpc_code(),e.to_string()))
                }.await;send_result(&frames,request.id,result);
            }
            "quota.configure" => {
                let result = async {
                    let p = parse_params::<QuotaConfigureParams>(&request.params)
                        .map_err(|e| (INVALID_PARAMS, e))?;
                    let auth = self
                        .authorize_local_request(
                            request.id.clone(),
                            agent_core::Action::AgentConfigure,
                        )
                        .await
                        .map_err(|e| (INTERNAL_ERROR, e.to_string()))?;
                    self.quota_management
                        .configure_quota_window(
                            &auth,
                            &agent_core::QuotaAccount {
                                scope: auth.context.scope.clone(),
                                metric: p.metric,
                                window_start_ms: p.window_start_ms,
                                window_end_ms: p.window_end_ms,
                                budget: p.budget,
                                consumed: 0,
                                reserved: 0,
                                revision: 0,
                            },
                            Arc::new(agent_policy::PolicyEngine),
                        )
                        .await
                        .map_err(|e| (e.rpc_code(), e.to_string()))?;
                    Ok(json!({"configured":true}))
                }
                .await;
                send_result(&frames, request.id, result);
            }

            "outbox.readback" => {
                let result = match parse_params::<OutboxReadParams>(&request.params) {
                    Ok(params) if (1..=64).contains(&params.limit) => self
                        .publication_store
                        .read_publications(&self.scope(), params.after, params.limit + 1)
                        .await
                        .map(|mut records| {
                            let has_more = records.len() > params.limit as usize;
                            records.truncate(params.limit as usize);
                            let cursor = records.last().map_or(params.after, |r| r.cursor);
                            json!(agent_core::OutboxReadback {
                                records: records.into_iter().map(Into::into).collect(),
                                cursor,
                                has_more
                            })
                        }),
                    _ => Err(agent_core::CoordinationError::InvalidRequest),
                };
                let response = match result {
                    Ok(value) => JsonRpcResponse::success(request.id, value),
                    Err(error) => JsonRpcResponse::failure_data(
                        request.id,
                        error.rpc_code(),
                        error.to_string(),
                        json!({"kind":error,"retryable":error.retryable(),"safe_auto_retry":error.retryable(),"recovery_action":error.recovery_action()}),
                    ),
                };
                let _ = frames.send(ServerFrame::Response(response));
            }
            "identity.readback" => match self.local_identity().await {
                Ok(identity) => send_result(&frames, request.id, Ok(json!(identity))),
                Err(error) => send_authorization_error(&frames, request.id, error),
            },
            "run.authorization" => {
                let params = match parse_params::<AuthorizationReadParams>(&request.params) {
                    Ok(params) => params,
                    Err(error) => {
                        send_result(&frames, request.id, Err((INVALID_PARAMS, error)));
                        return;
                    }
                };
                match self
                    .identity_store
                    .read_authorization(&agent_core::EnterpriseOwnerEnvelope {
                        scope: self.scope(),
                        owner: params.owner,
                    })
                    .await
                {
                    Ok(snapshot) => {
                        send_result(&frames, request.id, Ok(json!({"snapshot":snapshot})))
                    }
                    Err(error) => send_authorization_error(&frames, request.id, error.into()),
                }
            }

            "context.readback" => {
                let result = parse_params::<ContextReadParams>(&request.params)
                    .map_err(|error| (INVALID_PARAMS, error));
                let result = match result {
                    Ok(params) => self.context_readback(params).await,
                    Err(error) => Err(error),
                };
                send_result(&frames, request.id, result);
            }
            "hooks.readback" => {
                let result = match parse_params::<HookReadParams>(&request.params) {
                    Ok(p) => self
                        .hook_observation_store
                        .read_hook_history(
                            &self.scope(),
                            &SessionId(p.session_id),
                            &p.expected_lifetime,
                            p.after_cursor,
                            p.limit,
                        )
                        .await
                        .map(|readback| json!(readback))
                        .map_err(|error| (error.rpc_code(), error.to_string())),
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "sessions.plan.readback" => {
                let result = match parse_params::<PlanReadParams>(&request.params) {
                    Ok(params) => self
                        .plan_control_store
                        .read_session_plan(&self.scope(), &SessionId(params.session_id))
                        .await
                        .map_err(|e| (e.rpc_code(), e.to_string()))
                        .and_then(|v| {
                            serde_json::to_value(v).map_err(|e| (INTERNAL_ERROR, e.to_string()))
                        }),
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "sessions.plan.discard" => {
                let result = match parse_params::<PlanDiscardParams>(&request.params) {
                    Ok(params) => self.discard_plan_command(params).await,
                    Err(e) => Err((INVALID_PARAMS, e)),
                };
                send_result(&frames, request.id, result);
            }
            "memory.store" | "memory.recall" | "memory.list" | "memory.forget" | "memory.scope"
            | "memory.feedback" | "memory.flywheel" | "memory.evidence" => {
                let result = self.memory_command(&request.method, &request.params).await;
                send_result(&frames, request.id, result);
            }
            "resources.reconcile" => {
                let result = match parse_params::<ResourceReconcileParams>(&request.params) {
                    Ok(params) => {
                        let scope = self.scope();
                        match self
                            .run_fact_store
                            .read_exact_run_owner(&scope, &params.owner_run_id)
                            .await
                        {
                            Ok(owner) if owner.session_key.0 == params.session_id => {
                                let envelope = agent_core::EnterpriseOwnerEnvelope { scope, owner };
                                self.resource_store
                                    .reconcile_run_resource(
                                        &envelope,
                                        params.resource_id,
                                        &params.terminal_state,
                                        &params.evidence,
                                    )
                                    .await
                                    .map(|r| json!({"resource":r,"kill_sent":false}))
                                    .map_err(|e| (e.rpc_code(), e.to_string()))
                            }
                            Ok(_) => {
                                Err((REQUEST_CONFLICT, "resource reconcile owner不匹配".into()))
                            }
                            Err(error) => Err(lifecycle_rpc_error(error)),
                        }
                    }
                    Err(e) => Err((INVALID_PARAMS, e)),
                };
                send_result(&frames, request.id, result);
            }
            "resources.list" | "resources.read" | "resources.logs" | "resources.wait"
            | "resources.stop" => {
                let result = match parse_params::<ResourceParams>(&request.params) {
                    Ok(params) => self.resource_command(&request.method, params).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "artifacts.read" => {
                let params = match parse_params::<ArtifactReadParams>(&request.params) {
                    Ok(params) => params,
                    Err(error) => {
                        send_result(&frames, request.id, Err((INVALID_PARAMS, error)));
                        return;
                    }
                };
                let result = self.artifact_readback(&params).await;
                match result {
                    Ok(value) => send_result(&frames, request.id, Ok(value)),
                    Err(RuntimeError::Artifact(reason)) => {
                        let response = JsonRpcResponse::failure_data(
                            request.id,
                            reason.rpc_code(),
                            reason.to_string(),
                            json!({"domain":"artifact","kind":reason,"retryable":matches!(reason,agent_core::ArtifactError::StorageUnavailable),"safe_auto_retry":matches!(reason,agent_core::ArtifactError::StorageUnavailable),"recovery_action":reason.recovery_action()}),
                        );
                        let _ = frames.send(ServerFrame::Response(response));
                    }
                    Err(error) => send_authorization_error(&frames, request.id, error),
                }
            }
            "runtime.doctor" => {
                let result=self.daemon_maintenance_store.read_storage_health(&self.scope()).await.map_err(|e|(e.rpc_code(),e.to_string())).map(|storage| {
                    let profiles=self.config_store.load().map(|c|c.profiles.iter().map(ProfileSummary::from_profile).collect::<Vec<_>>()).unwrap_or_default();
                    json!({"storage":storage,"profiles":profiles,"native_boundary":"soft","mcp_pending_limit":64,"mcp_response_bytes":262144,"remote_mcp":"HTTPS, DNS pinning, redirects disabled"})
                });
                send_result(&frames, request.id, result);
            }
            "session.compact" => {
                let result = match parse_params::<SessionCompactParams>(&request.params) {
                    Ok(params) => self.session_compact(params).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "compact.start" => {
                let result = match parse_params::<CompactStartParams>(&request.params) {
                    Ok(params) => {
                        self.start_compact_command(
                            agent_core::CompactRunRequest {
                                session_key: SessionId(params.session_id),
                                session_lifetime_id: params.expected_lifetime,
                                operation_id: params.operation_id,
                                expected_revision: params.expected_revision,
                                expected_projection_generation: params
                                    .expected_projection_generation,
                                compatibility_owner_run_id: None,
                            },
                            params.entry_channel,
                            false,
                        )
                        .await
                    }
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "chat.send" => self.handle_chat_send(request, frames).await,
            "session.load" => {
                let result = match parse_params::<SessionSelectorParams>(&request.params) {
                    Ok(params) => {
                        self.session_load_mode(params.session_id.as_deref(), params.history_mode)
                            .await
                    }
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "session.load_page" => {
                let result = match parse_params::<SessionPageParams>(&request.params) {
                    Ok(params) => self.session_load_page(params).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "session.list" => {
                let result = match parse_params::<SessionListParams>(&request.params) {
                    Ok(params) => self.session_list(params).await,
                    Err(e) => Err((INVALID_PARAMS, e)),
                };
                send_result(&frames, request.id, result);
            }
            "session.new" => {
                let result = match parse_params::<SessionCreateParams>(&request.params) {
                    Ok(params) => self.session_create(params).await,
                    Err(e) => Err((INVALID_PARAMS, e)),
                };
                send_result(&frames, request.id, result);
            }
            "session.close" => {
                let result = match parse_params::<SessionEndParams>(&request.params) {
                    Ok(params) => self.session_close(params).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "session.clear" | "session.delete" => {
                let result = match parse_params::<SessionEndParams>(&request.params) {
                    Ok(params) => {
                        self.session_end(params, request.method == "session.delete")
                            .await
                    }
                    Err(e) => Err((INVALID_PARAMS, e)),
                };
                send_result(&frames, request.id, result);
            }
            "session.fork" => {
                let result = match parse_params::<SessionForkParams>(&request.params) {
                    Ok(params) => self.session_fork(params).await,
                    Err(e) => Err((INVALID_PARAMS, e)),
                };
                send_result(&frames, request.id, result);
            }
            "session.resume" => {
                let result = match parse_params::<SessionResumeParams>(&request.params) {
                    Ok(params) => self.session_resume(&params.session_id).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "session.trace" => {
                let result = match parse_params::<SessionResumeParams>(&request.params) {
                    Ok(params) => self.session_trace(&params.session_id).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "session.trace_page" => {
                let result = match parse_params::<SessionPageParams>(&request.params) {
                    Ok(params) => self.session_trace_page(params).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "permissions.get" => {
                send_result(&frames, request.id, self.permissions_get());
            }
            "permissions.set" => {
                let result = match parse_params::<PermissionModeParams>(&request.params) {
                    Ok(params) => self.permissions_set(&params.mode),
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "models.list" => send_result(&frames, request.id, self.models_list()),
            "models.use" => {
                let result = match parse_params::<ModelUseParams>(&request.params) {
                    Ok(params) => self.models_use(&params.profile_id).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "models.save" => {
                let result = match parse_params::<ModelSaveParams>(&request.params) {
                    Ok(params) => self.models_save(params).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "approval.respond" | "interaction.respond" | "interaction.reject" => {
                let rejected = request.method == "interaction.reject";
                let result = match parse_params::<ApprovalRespondParams>(&request.params) {
                    Ok(params) => self.resolve_interaction(params, rejected).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "interaction.read" => {
                let result = match parse_params::<InteractionReadParams>(&request.params) {
                    Ok(params) => self
                        .interaction_store
                        .read_interaction_record(&self.scope(), &params.interaction_id)
                        .await
                        .map(|interaction| json!({"interaction":interaction}))
                        .map_err(|e| (e.rpc_code(), e.to_string())),
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "interaction.list" => {
                let result = match parse_params::<SessionResumeParams>(&request.params) {
                    Ok(params) => self
                        .interaction_store
                        .read_interaction_list(&self.scope(), &SessionId(params.session_id))
                        .await
                        .map(|interactions| json!({"interactions":interactions}))
                        .map_err(|e| (e.rpc_code(), e.to_string())),
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "agent.cancel" => {
                let result = match parse_params::<CancelParams>(&request.params) {
                    Ok(params) => self.cancel(params).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "spawn_subagent" => {
                let result = match parse_params::<SpawnSubagentParams>(&request.params) {
                    Ok(params) => self.spawn_subagent(params).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "list_subagents" => {
                let result = match parse_params::<SubagentListParams>(&request.params) {
                    Ok(params) => self
                        .delegation_store
                        .read_delegation_list(&self.scope(), &params.root_run_id)
                        .await
                        .map(|children| json!({"children":children}))
                        .map_err(|e| (e.rpc_code(), e.to_string())),
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "read_subagent" => {
                let result = match parse_params::<SubagentScopeParams>(&request.params) {
                    Ok(params) => self
                        .scoped_delegation(&params)
                        .await
                        .map(|child| json!({"child":child})),
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "wait_subagents" => {
                let result = match parse_params::<WaitSubagentsParams>(&request.params) {
                    Ok(params) => self.wait_subagents(params).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "cancel_subagent" => {
                let result = match parse_params::<SubagentScopeParams>(&request.params) {
                    Ok(params) => {
                        let child = self.scoped_delegation(&params).await;
                        match child {
                            Ok(child) => {
                                self.cancel(CancelParams {
                                    exact_owner: None,
                                    request_id: None,
                                    run_id: Some(child.child_run_id),
                                    session_id: Some(child.child_session_id.0),
                                })
                                .await
                            }
                            Err(error) => Err(error),
                        }
                    }
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "subagent.result.reserve" | "subagent.result.release" | "subagent.result.commit" => {
                let result = match parse_params::<SubagentResultParams>(&request.params) {
                    Ok(params) => {
                        let scope = self.scope();
                        match self
                            .run_fact_store
                            .read_exact_run_owner(&scope, &params.parent_run_id)
                            .await
                        {
                            Ok(owner) => {
                                let envelope = agent_core::EnterpriseOwnerEnvelope { scope, owner };
                                let child = match request.method.as_str() {
                                    "subagent.result.reserve" => {
                                        self.delegation_store
                                            .reserve_child_result(
                                                &envelope,
                                                &params.child_run_id,
                                                &params.owner,
                                                params.revision,
                                            )
                                            .await
                                    }
                                    "subagent.result.release" => {
                                        self.delegation_store
                                            .transition_child_result(
                                                &envelope,
                                                &params.child_run_id,
                                                &params.owner,
                                                params.revision,
                                                "unconsumed",
                                            )
                                            .await
                                    }
                                    _ => {
                                        self.delegation_store
                                            .transition_child_result(
                                                &envelope,
                                                &params.child_run_id,
                                                &params.owner,
                                                params.revision,
                                                "delivered",
                                            )
                                            .await
                                    }
                                };
                                child
                                    .map(|child| json!({"child":child}))
                                    .map_err(|e| (e.rpc_code(), e.to_string()))
                            }
                            Err(error) => Err(lifecycle_rpc_error(error)),
                        }
                    }
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "queue.list" => {
                let result = match parse_params::<SessionResumeParams>(&request.params) {
                    Ok(params) => self
                        .queue_store
                        .read_queued_messages(&self.scope(), &SessionId(params.session_id))
                        .await
                        .map(|items| json!({"items":items}))
                        .map_err(|e| (e.rpc_code(), e.to_string())),
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "queue.read" => {
                let result = match parse_params::<QueueItemParams>(&request.params) {
                    Ok(params) => self
                        .queue_store
                        .read_queued_message(
                            &self.scope(),
                            &SessionId(params.session_id),
                            &params.run_id,
                        )
                        .await
                        .map(|item| json!({"item":item}))
                        .map_err(|e| (e.rpc_code(), e.to_string())),
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "queue.remove" => {
                let result = match parse_params::<QueueItemParams>(&request.params) {
                    Ok(params) => {
                        self.cancel(CancelParams {
                            exact_owner: None,
                            request_id: None,
                            run_id: Some(params.run_id),
                            session_id: Some(params.session_id),
                        })
                        .await
                    }
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "agent.subscribe" => self.handle_subscribe(request, frames).await,
            "views.reduce" => {
                let result = parse_params::<agent_daemon_protocol::params::ViewReduceParams>(
                    &request.params,
                )
                .map_err(|error| (INVALID_PARAMS, error))
                .and_then(|params| {
                    params
                        .into_domain()
                        .map_err(|error| (INVALID_PARAMS, error.to_string()))
                })
                .map(|(state, input)| json!(agent_core::reduce_view(state, input)));
                send_result(&frames, request.id, result);
            }
            "run.read" => {
                let result = match parse_params::<RunReadParams>(&request.params) {
                    Ok(params) => self.run_fact_value(&params.run_id, "fact").await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "run.discovery" => {
                let result = match parse_params::<RunReadParams>(&request.params) {
                    Ok(params) => self
                        .run_discovery_store
                        .read_run_discoveries(&self.scope(), &params.run_id)
                        .await
                        .map(|receipts| json!({"receipts":receipts}))
                        .map_err(|error| (error.rpc_code(), error.to_string())),
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "run.tools" | "run.provider_attempts" => {
                let view = if request.method == "run.tools" {
                    "tools"
                } else {
                    "provider"
                };
                let result = match parse_params::<RunReadParams>(&request.params) {
                    Ok(params) => self.run_fact_value(&params.run_id, view).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "run.audit" => {
                let result = match parse_params::<RunReadParams>(&request.params) {
                    Ok(params) => self.audit_run(&params.run_id).await,
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "run.reconcile" => {
                let result = match parse_params::<RunReconcileParams>(&request.params) {
                    Ok(params) => {
                        let active = self
                            .run_coordinator
                            .active
                            .lock()
                            .await
                            .values()
                            .any(|item| item.run_id == params.run_id);
                        if active {
                            Err((REQUEST_CONFLICT, "run 仍有活动执行体".into()))
                        } else {
                            match self
                                .run_fact_store
                                .read_exact_run_owner(&self.scope(), &params.run_id)
                                .await
                            {
                                Ok(owner) if owner.session_key == params.session_id => {
                                    let envelope = agent_core::EnterpriseOwnerEnvelope {
                                        scope: self.scope(),
                                        owner,
                                    };
                                    self.daemon_maintenance_store
                                        .reconcile_run_outcome(
                                            &envelope,
                                            params.expected_last_seq,
                                            params.status,
                                            params.content.as_deref(),
                                            &params.evidence,
                                        )
                                        .await
                                        .map(|run| json!({"run":run}))
                                        .map_err(|e| (e.rpc_code(), e.to_string()))
                                }
                                Ok(_) => Err((REQUEST_CONFLICT, "run不属于指定session".into())),
                                Err(error) => Err((error.rpc_code(), error.to_string())),
                            }
                        }
                    }
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "run.events" => {
                let params = match parse_params::<RunEventsParams>(&request.params) {
                    Ok(params) => params,
                    Err(error) => {
                        send_result(&frames, request.id, Err((INVALID_PARAMS, error)));
                        return;
                    }
                };
                match self.run_event_readback(&params).await {
                    Ok(page) => send_result(&frames, request.id, Ok(json!(page))),
                    Err(error) => send_authorization_error(&frames, request.id, error.into()),
                }
            }
            "slash.execute" => {
                let result = match parse_params::<SlashExecuteParams>(&request.params) {
                    Ok(params) => {
                        self.execute_slash(
                            &params.line,
                            params.session_id.as_deref(),
                            params.entry_channel,
                        )
                        .await
                    }
                    Err(error) => Err((INVALID_PARAMS, error)),
                };
                send_result(&frames, request.id, result);
            }
            "daemon.stop" => {
                send_result(
                    &frames,
                    request.id,
                    Ok(
                        json!({"stopping": true, "active_turns_finish_gracefully": false,
                        "active_turns_cancelled": true}),
                    ),
                );
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                self.shutdown.cancel();
            }
            _ => {
                let _ = frames.send(ServerFrame::Response(JsonRpcResponse::failure(
                    request.id,
                    METHOD_NOT_FOUND,
                    format!("未知 RPC 方法: {}", request.method),
                )));
            }
        }
    }

    pub(crate) async fn spawn_subagent_for_tool(
        self: &Arc<Self>,
        parent_run_id: RunId,
        spawn_key: String,
        task: String,
        context_source_ids: Vec<String>,
        tools: Option<Vec<String>>,
    ) -> Result<Value, (i64, String)> {
        let parent = self
            .run_fact_store
            .read_run_fact(&self.scope(), &parent_run_id)
            .await
            .map_err(|e| (e.rpc_code(), e.to_string()))?
            .run;
        let response = self
            .spawn_subagent(SpawnSubagentParams {
                parent_session_id: parent.session_id,
                parent_run_id,
                spawn_key,
                task,
                context_source_ids,
                tools,
                max_rounds: None,
                max_tokens: None,
                max_tool_calls: None,
                timeout_ms: None,
            })
            .await?;
        Ok(response["child"].clone())
    }

    pub(crate) async fn wait_subagents_for_tool(
        &self,
        parent_run_id: RunId,
        child_run_ids: Vec<RunId>,
        timeout_ms: u64,
        after_seq: Option<EventSeq>,
    ) -> Result<Value, (i64, String)> {
        self.wait_subagents(WaitSubagentsParams {
            parent_run_id,
            child_run_ids,
            timeout_ms: Some(timeout_ms),
            after_seq,
        })
        .await
    }

    pub(crate) async fn cancel_subagent_for_tool(
        &self,
        parent_run_id: RunId,
        child_run_id: RunId,
    ) -> Result<Value, (i64, String)> {
        let child = self
            .scoped_delegation(&SubagentScopeParams {
                parent_run_id,
                child_run_id,
            })
            .await?;
        self.cancel(CancelParams {
            exact_owner: None,
            request_id: None,
            run_id: Some(child.child_run_id),
            session_id: Some(child.child_session_id.0),
        })
        .await
    }

    async fn spawn_subagent(
        self: &Arc<Self>,
        params: SpawnSubagentParams,
    ) -> Result<Value, (i64, String)> {
        let parent = self
            .run_fact_store
            .read_run_fact(&self.scope(), &params.parent_run_id)
            .await
            .map_err(|e| (e.rpc_code(), e.to_string()))?
            .run;
        if parent.session_id != params.parent_session_id {
            return Err((REQUEST_CONFLICT, "父 run 不属于指定 session".into()));
        }
        let scope = self.scope();
        let parent_owner =
            if let Some(envelope) = agent_runtime::loop_engine::current_execution_envelope() {
                if envelope.scope != scope
                    || envelope.owner.run_id != params.parent_run_id
                    || envelope.owner.session_key != params.parent_session_id
                {
                    return Err((REQUEST_CONFLICT, "委派工具的固定parent proof不匹配".into()));
                }
                envelope.owner
            } else {
                self.run_fact_store
                    .read_exact_run_owner(&scope, &params.parent_run_id)
                    .await
                    .map_err(lifecycle_rpc_error)?
            };
        let envelope = agent_core::EnterpriseOwnerEnvelope {
            scope: scope.clone(),
            owner: parent_owner,
        };
        let parent_delegation = self
            .delegation_store
            .read_delegation_record(&scope, &params.parent_run_id)
            .await
            .map_err(|e| (e.rpc_code(), e.to_string()))?;
        let requested_tools = params.tools.unwrap_or_else(|| vec!["read_file".into()]);
        // This first durable slice only grants the read-only capability. A child of a
        // child must also inherit it from its own immutable capability snapshot.
        if requested_tools.is_empty()
            || requested_tools.iter().any(|name| name != "read_file")
            || parent_delegation.as_ref().is_some_and(|record| {
                requested_tools
                    .iter()
                    .any(|name| !record.tools.contains(name))
            })
        {
            return Err((REQUEST_CONFLICT, "子 Agent 只能继承 read_file 能力".into()));
        }
        let max_rounds = params.max_rounds.unwrap_or(8);
        let max_tokens = params.max_tokens.unwrap_or(16_000);
        let max_tool_calls = params.max_tool_calls.unwrap_or(16);
        let timeout_ms = params.timeout_ms.unwrap_or(120_000);
        if !(1..=15).contains(&max_rounds)
            || !(512..=32_000).contains(&max_tokens)
            || !(1..=32).contains(&max_tool_calls)
            || !(1_000..=600_000).contains(&timeout_ms)
            || parent_delegation.as_ref().is_some_and(|record| {
                max_rounds > record.max_rounds
                    || max_tokens > record.max_tokens
                    || max_tool_calls > record.max_tool_calls
            })
        {
            return Err((INVALID_PARAMS, "子 Agent 预算无效或超过父预算".into()));
        }
        let parent_runtime = self
            .session_runtime(Some(&params.parent_session_id.0))
            .await?;
        if max_tokens as usize > parent_runtime.engine.token_budget() {
            return Err((INVALID_PARAMS, "子 Agent token 预算超过父会话".into()));
        }
        parent_runtime
            .engine
            .for_delegation(&requested_tools, max_rounds as usize, max_tokens as usize)
            .map_err(|error| (REQUEST_CONFLICT, error.to_string()))?;
        let (child_session_id, _) = self
            .session
            .create_isolated_session()
            .map_err(|error| (INTERNAL_ERROR, format!("{error:#}")))?;
        let cwd = parent_delegation
            .as_ref()
            .map(|record| record.cwd.clone())
            .or_else(|| {
                self.safety
                    .as_ref()
                    .map(|policy| policy.workspace().to_string_lossy().into_owned())
            })
            .unwrap_or_else(|| {
                self.session
                    .path_for_session(&parent.session_id.0)
                    .parent()
                    .unwrap_or(Path::new("."))
                    .to_string_lossy()
                    .into_owned()
            });
        // 初期只开放只读能力；即使父会话权限更宽，child 仍冻结为最窄模式。
        let permission_mode = SafetyMode::RequestApproval.key().to_owned();
        let deadline_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(i64::MAX as u128) as i64
            + timeout_ms;
        let deadline_ms = parent_delegation
            .as_ref()
            .map_or(deadline_ms, |record| deadline_ms.min(record.deadline_ms));
        let guard = self.session_supervisor.control_lock.lock().await;
        let duplicate = self
            .delegation_store
            .read_spawned_delegation(&scope, &params.parent_run_id, &params.spawn_key)
            .await
            .map_err(|e| (e.rpc_code(), e.to_string()))?
            .is_some();
        if !duplicate {
            let key = ActiveKey {
                session_id: parent.session_id.0.clone(),
                request_id: parent.request_id.clone(),
            };
            let active = self.run_coordinator.active.lock().await;
            if active.get(&key).is_none_or(|request| {
                request.run_id != parent.run_id || request.cancellation.is_cancelled()
            }) {
                return Err((REQUEST_CONFLICT, "父 run 已停止接纳子 Agent".into()));
            }
        }
        let child = self
            .delegation_store
            .admit_child_delegation(
                &envelope,
                &DelegationRequest {
                    parent_session_id: params.parent_session_id,
                    parent_run_id: params.parent_run_id,
                    child_session_id: SessionId(child_session_id),
                    spawn_key: params.spawn_key,
                    task: params.task.clone(),
                    context_source_ids: params.context_source_ids,
                    tools: requested_tools,
                    permission_mode,
                    cwd,
                    max_rounds,
                    max_tokens,
                    max_tool_calls,
                    deadline_ms,
                },
            )
            .await
            .map_err(|e| (e.rpc_code(), e.to_string()))?;
        drop(guard);
        self.notify_delegation_parent(&child, EventKind::DelegationSpawned)
            .await;
        if child.status == RunStatus::Queued {
            let request = JsonRpcRequest::new(
                RequestId::String(format!("delegation:{}", child.child_session_id.0)),
                "chat.send",
                json!({"session_id": child.child_session_id, "message": params.task,"entry_channel":"subagent"}),
            );
            let (frames, _receiver) = super::frames::frame_channel();
            let state = self.clone();
            let accepted = self
                .spawn_owned(async move {
                    state.handle_chat_send(request, frames).await;
                })
                .await;
            if !accepted {
                tracing::warn!(child_run_id = %child.child_run_id.0,
                    "daemon 关闭中；委派保留 queued，重启后恢复");
            }
        }
        Ok(json!({"child_session_id": child.child_session_id,
            "child_run_id": child.child_run_id, "status": child.status, "child": child}))
    }

    async fn scoped_delegation(
        &self,
        params: &SubagentScopeParams,
    ) -> Result<DelegationRecord, (i64, String)> {
        self.delegation_store
            .read_scoped_delegation(&self.scope(), &params.parent_run_id, &params.child_run_id)
            .await
            .map_err(|e| (e.rpc_code(), e.to_string()))?
            .ok_or((INVALID_PARAMS, "子Agent不存在".into()))
    }

    async fn wait_subagents(&self, params: WaitSubagentsParams) -> Result<Value, (i64, String)> {
        if params.child_run_ids.is_empty()
            || params.child_run_ids.len() > 8
            || params.timeout_ms.unwrap_or(30_000) > 60_000
        {
            return Err((INVALID_PARAMS, "等待对象或超时范围无效".into()));
        }
        let timeout = std::time::Duration::from_millis(params.timeout_ms.unwrap_or(30_000));
        let started = Instant::now();
        loop {
            let mut children = Vec::with_capacity(params.child_run_ids.len());
            let mut checkpoints = Vec::new();
            for child_run_id in &params.child_run_ids {
                let child = self
                    .scoped_delegation(&SubagentScopeParams {
                        parent_run_id: params.parent_run_id.clone(),
                        child_run_id: child_run_id.clone(),
                    })
                    .await?;
                if let Some(after_seq) = params.after_seq {
                    let run = self
                        .run_fact_store
                        .read_run_fact(&self.scope(), child_run_id)
                        .await
                        .map_err(|e| (e.rpc_code(), e.to_string()))?
                        .run;
                    if run.last_seq.0 > after_seq.0 {
                        checkpoints
                            .push(json!({"child_run_id": child_run_id, "last_seq": run.last_seq}));
                    }
                }
                children.push(child);
            }
            if !checkpoints.is_empty()
                || children.iter().any(|child| child.status.terminal())
                || started.elapsed() >= timeout
            {
                return Ok(json!({"timed_out": checkpoints.is_empty()
                        && !children.iter().any(|child| child.status.terminal()),
                    "children": children, "checkpoints": checkpoints}));
            }
            tokio::time::sleep(
                std::time::Duration::from_millis(50).min(timeout.saturating_sub(started.elapsed())),
            )
            .await;
        }
    }

    async fn notify_delegation_parent(&self, child: &DelegationRecord, kind: EventKind) {
        let Ok(fact) = self
            .run_fact_store
            .read_run_fact(&self.scope(), &child.parent_run_id)
            .await
        else {
            return;
        };
        let parent = fact.run;
        let key = ActiveKey {
            session_id: parent.session_id.0,
            request_id: parent.request_id.clone(),
        };
        let name = match serde_json::to_value(&kind)
            .ok()
            .and_then(|value| value.as_str().map(str::to_owned))
        {
            Some(name) => name,
            None => return,
        };
        let event = match self
            .event_store
            .read_run_view_event(
                &self.scope(),
                &child.parent_run_id,
                &name,
                Some(&child.child_run_id),
            )
            .await
        {
            Ok(Some(event)) => event,
            _ => return,
        };
        let Some(update) = stored_event_update(event) else {
            return;
        };
        let mut active = self.run_coordinator.active.lock().await;
        if let Some(active) = active.get_mut(&key) {
            active.publish_external(parent.request_id, update);
        }
    }

    fn frozen_child_read_tool(
        &self,
        child: &DelegationRecord,
    ) -> Result<ReadFileTool, (i64, String)> {
        let mode = SafetyMode::parse(&child.permission_mode)
            .ok_or((INTERNAL_ERROR, "委派权限快照无效".into()))?;
        let policy = Arc::new(
            SafetyPolicy::new(&child.cwd, Arc::new(self.approvals.clone()))
                .map_err(|error| (INTERNAL_ERROR, format!("子 Agent cwd 无法恢复: {error:#}")))?,
        );
        policy.set_mode(mode);
        Ok(ReadFileTool::new(policy))
    }

    pub(crate) fn handle_chat_send(
        self: Arc<Self>,
        request: JsonRpcRequest,
        frames: super::frames::FrameSender,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(self.handle_chat_send_inner(request, frames))
    }

    async fn handle_chat_send_inner(
        self: Arc<Self>,
        request: JsonRpcRequest,
        frames: super::frames::FrameSender,
    ) {
        if self.shutdown.is_cancelled() {
            send_result(
                &frames,
                request.id,
                Err((REQUEST_CONFLICT, "daemon 正在关闭，停止新准入".into())),
            );
            return;
        }
        let params = match parse_params::<ChatSendParams>(&request.params) {
            Ok(params) if !params.message.trim().is_empty() => params,
            Ok(_) => {
                send_result(
                    &frames,
                    request.id,
                    Err((INVALID_PARAMS, "message 不能为空".to_owned())),
                );
                return;
            }
            Err(error) => {
                send_result(&frames, request.id, Err((INVALID_PARAMS, error)));
                return;
            }
        };

        let admission_barrier = self.session_supervisor.control_lock.lock().await;
        if params.plan_execution.is_some() {
            let valid = match (
                params.session_id.as_ref(),
                params.expected_lifetime.as_ref(),
            ) {
                (Some(key), Some(lifetime)) => self
                    .session_control_store
                    .read_session_metadata(&self.scope(), &SessionId(key.clone()))
                    .await
                    .is_ok_and(|meta| meta.is_some_and(|m| !m.deleted && m.lifetime == *lifetime)),
                _ => false,
            };
            if !valid {
                send_result(
                    &frames,
                    request.id,
                    Err((
                        REQUEST_CONFLICT,
                        "plan execution 必须绑定存在的 session/lifetime".into(),
                    )),
                );
                return;
            }
        }
        let session = match self.session_runtime(params.session_id.as_deref()).await {
            Ok(session) => session,
            Err(error) => {
                send_result(&frames, request.id, Err(error));
                return;
            }
        };
        let session_id = session.id.clone();
        let delegation = match self
            .delegation_store
            .read_session_delegation(&self.scope(), &SessionId(session_id.clone()))
            .await
        {
            Ok(record) => record,
            Err(error) => {
                send_result(
                    &frames,
                    request.id,
                    Err((INTERNAL_ERROR, error.to_string())),
                );
                return;
            }
        };
        let _deadline_timer = if let Some(child) = &delegation {
            let expected = RequestId::String(format!("delegation:{session_id}"));
            if request.id != expected {
                send_result(
                    &frames,
                    request.id,
                    Err((
                        REQUEST_CONFLICT,
                        "委派 session 只能由已准入的 child run 驱动".into(),
                    )),
                );
                return;
            }
            let remaining = child.deadline_ms.saturating_sub(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
                    .min(i64::MAX as u128) as i64,
            );
            let state = self.clone();
            let child_run_id = child.child_run_id.clone();
            let child_session_id = child.child_session_id.0.clone();
            Some(AbortTimer(tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(remaining.max(0) as u64)).await;
                let _ = state
                    .cancel(CancelParams {
                        exact_owner: None,
                        request_id: None,
                        run_id: Some(child_run_id),
                        session_id: Some(child_session_id),
                    })
                    .await;
            })))
        } else {
            None
        };
        let mode = match params.admission_mode.as_deref().unwrap_or("queue") {
            "queue" => AdmissionMode::Queue,
            "reject_if_busy" => AdmissionMode::RejectIfBusy,
            _ => {
                send_result(
                    &frames,
                    request.id,
                    Err((INVALID_PARAMS, "不支持的 admission_mode".into())),
                );
                return;
            }
        };
        let installed_generation=match self.registry_store.read_session_generation(&self.scope(),&SessionId(session_id.clone()),&session.lifetime).await {
            Ok(g)=>g,Err(e)=>{send_result(&frames,request.id,Err((e.rpc_code(),e.to_string())));return;}
        };
        let frozen_route = match self.provider_manager.as_ref().map(|manager| manager.freeze(installed_generation.as_ref().map_or(session.engine.token_budget(),|g|session.engine.token_budget().min(g.effective.config.resources.context_tokens as usize)))).map(|r|installed_generation.as_ref().map_or(Ok(r.clone()),|g|r.restrict_models(&g.effective.config.models))).transpose() {
            Ok(route)=>route,Err(e)=>{send_result(&frames,request.id,Err((INVALID_PARAMS,e.to_string())));return;}
        };
        let workflow_engine=if WORKFLOW_DIRECTIVE.try_with(|d|matches!(d.definition.kind,agent_core::WorkflowNodeKind::Webhook{..})).unwrap_or(false){match self.safety.clone(){Some(safety)=>session.engine.with_tool(agent_runtime::tools::WorkflowWebhookTool{safety}),None=>{send_result(&frames,request.id,Err((INVALID_PARAMS,"Webhook缺少安全策略".into())));return;}}}else{(*session.engine).clone()};
        let frozen_engine = match workflow_engine.freeze_tools() {
            Ok(engine) => engine,
            Err(e) => {
                send_result(&frames, request.id, Err((INTERNAL_ERROR, e.to_string())));
                return;
            }
        };
        let frozen_engine=match installed_generation.as_ref().map(|g|frozen_engine.with_installed_generation(g)).transpose() {Ok(Some(e))=>e,Ok(None)=>frozen_engine,Err(e)=>{send_result(&frames,request.id,Err((INVALID_PARAMS,e.to_string())));return;}};
        let requested = params.sandbox.as_deref().unwrap_or("native");
        let (effective, notice) = match requested {
            "native" => ("native", None),
            "docker" | "auto" => {
                if agent_runtime::tools::NativeSandbox::docker_available().await {
                    ("docker", None)
                } else if requested == "auto" {
                    (
                        "native",
                        Some("Docker 不可用，auto 明确降级为 Native 软边界".to_owned()),
                    )
                } else {
                    send_result(
                        &frames,
                        request.id,
                        Err((
                            INVALID_PARAMS,
                            "Docker backend 或预装镜像不可用，强隔离请求被拒绝".into(),
                        )),
                    );
                    return;
                }
            }
            _ => {
                send_result(
                    &frames,
                    request.id,
                    Err((INVALID_PARAMS, "sandbox 必须为 native/docker/auto".into())),
                );
                return;
            }
        };
        let billing_policy=match self.billing_store.read_billing_policy(&self.scope()).await {
            Ok(policy)=>policy,
            Err(error)=>{send_result(&frames,request.id,Err((error.rpc_code(),error.to_string())));return;}
        };
        let mut max_tool_calls=match (delegation.as_ref().map(|d|d.max_tool_calls as u64),billing_policy.as_ref()) {
            (Some(a),Some(b))=>Some(a.min(b.policy.limits.max_tool_calls)),
            (None,Some(b))=>Some(b.policy.limits.max_tool_calls),
            (a,None)=>a,
        };
        if let Some(g)=&installed_generation {max_tool_calls=Some(max_tool_calls.map_or(g.effective.config.resources.max_tool_calls,|n|n.min(g.effective.config.resources.max_tool_calls)));}
        let snapshot = agent_core::RunSnapshot {
            workflow: WORKFLOW_DIRECTIVE.try_with(Clone::clone).ok(),
            installed_generation,
            billing_policy,
            artifact_store_id: frozen_engine.artifact_store_id(),
            entry_channel: params.entry_channel,
            route: frozen_route.as_ref().map(|r| r.snapshot.clone()),
            tools: frozen_engine.tool_specs(),
            cwd: self.safety.as_ref().map_or_else(
                || ".".into(),
                |s| s.workspace().to_string_lossy().into_owned(),
            ),
            permission_mode: self
                .safety
                .as_ref()
                .map_or("risk", |s| s.mode().key())
                .into(),
            sandbox_requested: requested.into(),
            sandbox_effective: effective.into(),
            sandbox_notice: notice,
            docker_image: (effective == "docker").then(|| {
                std::env::var("MY_AGENT_DOCKER_IMAGE").unwrap_or_else(|_| "alpine:3.21".into())
            }),
            delegation_context: None,
            context_read_only: params.context_read_only,
            context_token_budget: frozen_engine.token_budget(),
            context_policy_fingerprint: Some(frozen_engine.context_policy_fingerprint()),
            tool_catalog_digest: format!(
                "{:x}",
                sha2::Sha256::digest(
                    serde_json::to_vec(&frozen_engine.tool_specs()).unwrap_or_default()
                )
            ),
            memory_entry_budget: 8,
            memory_token_budget: 1024,
            max_tool_calls,
            config_generation: frozen_route
                .as_ref()
                .map_or(0, |r| r.snapshot.config_generation),
        };
        if let Some(g)=&snapshot.installed_generation {
            let c=&g.effective.config;
            if !c.secret_refs.is_empty() && !c.sandbox.secrets {send_result(&frames,request.id,Err((INVALID_PARAMS,"发行代secret策略拒绝".into())));return;}
        }
        let authorization = match self
            .authorize_local_request(request.id.clone(), agent_core::Action::SessionPrompt)
            .await
        {
            Ok(request) => request,
            Err(error) => {
                send_authorization_error(&frames, request.id, error);
                return;
            }
        };
        let (admitted, owner) = match self
            .run_execution_store
            .admit_run_authorized(
                &agent_core::RunAdmission {
                    workflow_node: WORKFLOW_LEASE.try_with(Clone::clone).ok(),
                    plan_execution: params.plan_execution.clone(),
                    session_key: SessionId(session_id.clone()),
                    expected_lifetime: Some(
                        params
                            .expected_lifetime
                            .clone()
                            .unwrap_or_else(|| session.lifetime.clone()),
                    ),
                    request_id: request.id.clone(),
                    input: params.message.clone(),
                    mode,
                },
                &snapshot,
                &authorization,
                std::sync::Arc::new(agent_policy::PolicyEngine),
            )
            .await
        {
            Ok(admitted) => admitted,
            Err(error) => {
                if error
                    == agent_core::RepositoryError::Coordination(
                        agent_core::CoordinationError::Conflict,
                    )
                {
                    send_result(
                        &frames,
                        request.id,
                        Err((REQUEST_CONFLICT, error.to_string())),
                    );
                } else {
                    send_authorization_error(&frames, request.id, error.into());
                }
                return;
            }
        };
        let execution_envelope = agent_core::EnterpriseOwnerEnvelope {
            scope: authorization.context.scope.clone(),
            owner: owner.clone(),
        };
        let run_record = match admitted {
            Admission::New(run) => {
                self.run_coordinator
                    .frozen_engines
                    .lock()
                    .await
                    .insert(run.run_id.clone(), frozen_engine);
                if let Some(route) = frozen_route {
                    self.run_coordinator
                        .frozen_routes
                        .lock()
                        .await
                        .insert(run.run_id.clone(), route);
                }
                run
            }
            Admission::Existing(run) => {
                if run.status == RunStatus::Completed {
                    let child = match self
                        .hook_continuation_store
                        .read_hook_child(&authorization.context.scope, &run.run_id)
                        .await
                    {
                        Ok(child) => child,
                        Err(error) => {
                            send_authorization_error(&frames, request.id, error.into());
                            return;
                        }
                    };
                    send_result(
                        &frames,
                        request.id,
                        Ok(
                            json!({"content": run.content, "run_id": run.run_id, "turn_id": run.turn_id,"continuation_run_id":child}),
                        ),
                    );
                    return;
                } else if run.status != RunStatus::Queued {
                    send_result(
                        &frames,
                        request.id,
                        Ok(json!({"run_id": run.run_id, "turn_id": run.turn_id,
                            "status": run.status, "duplicate": true,
                            "error_code": run.error_code, "error_message": run.error_message})),
                    );
                    return;
                } else {
                    run
                }
            }
        };
        let started_at = Instant::now();
        info!(
            session_id = %session_id,
            request_id = ?request.id,
            "chat 请求开始"
        );
        let active_key = ActiveKey {
            session_id: session_id.clone(),
            request_id: request.id.clone(),
        };
        let cancellation = CancellationToken::new();
        let mut active = self.run_coordinator.active.lock().await;
        if active.contains_key(&active_key)
            || active.values().any(|a| a.run_id == run_record.run_id)
        {
            drop(active);
            send_result(
                &frames,
                request.id,
                Ok(
                    json!({"run_id": run_record.run_id, "turn_id": run_record.turn_id, "status": "queued_or_running", "duplicate": true}),
                ),
            );
            return;
        }
        active.insert(
            active_key.clone(),
            ActiveRequest::new(cancellation.clone(), run_record.run_id.clone())
                .with_origin(frames.clone()),
        );
        drop(active);
        drop(admission_barrier);
        self.run_coordinator.queue_notify.notify_waiters();
        loop {
            let notified = self.run_coordinator.queue_notify.notified();
            if cancellation.is_cancelled() {
                self.run_coordinator.active.lock().await.remove(&active_key);
                self.run_coordinator
                    .frozen_routes
                    .lock()
                    .await
                    .remove(&run_record.run_id);
                self.run_coordinator
                    .frozen_engines
                    .lock()
                    .await
                    .remove(&run_record.run_id);
                send_result(
                    &frames,
                    request.id,
                    Err((REQUEST_CANCELLED, "请求已取消".into())),
                );
                return;
            }
            match self
                .run_execution_store
                .start_queued_run(&execution_envelope)
                .await
            {
                Ok(true) => break,
                Ok(false) => {}
                Err(error) => {
                    self.run_coordinator.active.lock().await.remove(&active_key);
                    send_authorization_error(&frames, request.id, error.into());
                    return;
                }
            }
            tokio::select! {
                _ = notified => {},
                _ = cancellation.cancelled() => {},
                _ = self.shutdown.cancelled() => {
                    self.run_coordinator.active.lock().await.remove(&active_key);
                    return;
                }
            }
        }

        let route = if let Some(route) = self
            .run_coordinator
            .frozen_routes
            .lock()
            .await
            .remove(&run_record.run_id)
        {
            Some(route)
        } else {
            match self
                .run_fact_store
                .read_provider_history(&self.scope(), &run_record.run_id)
                .await
                .map(|read| read.route)
            {
                Ok(Some(snapshot)) => {
                    let restore = self
                        .provider_manager
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("ProviderManager 不可用"))
                        .and_then(|manager| {
                            let mut profiles = self.config_store.load()?.profiles;
                            profiles.push(manager.profile());
                            manager.restore(snapshot, &profiles)
                        });
                    match restore {
                        Ok(route) => Some(route),
                        Err(error) => {
                            let _ = self
                                .finish_frozen_run(
                                    &execution_envelope,
                                    RunStatus::Failed,
                                    None,
                                    Some((-32002, "冻结 Provider 配置无法恢复")),
                                )
                                .await;
                            self.run_coordinator.active.lock().await.remove(&active_key);
                            send_result(
                                &frames,
                                request.id,
                                Err((
                                    INTERNAL_ERROR,
                                    format!("冻结 Provider 配置无法恢复: {error}"),
                                )),
                            );
                            return;
                        }
                    }
                }
                Ok(None) => None,
                Err(error) => {
                    self.run_coordinator.active.lock().await.remove(&active_key);
                    send_result(
                        &frames,
                        request.id,
                        Err((INTERNAL_ERROR, error.to_string())),
                    );
                    return;
                }
            }
        };
        let engine = self
            .run_coordinator
            .frozen_engines
            .lock()
            .await
            .remove(&run_record.run_id)
            .unwrap_or_else(|| (*session.engine).clone());
        let engine = match engine.freeze_tools() {
            Ok(engine) => engine,
            Err(error) => {
                let _ = self
                    .finish_frozen_run(
                        &execution_envelope,
                        RunStatus::Failed,
                        None,
                        Some((INTERNAL_ERROR, &error.to_string())),
                    )
                    .await;
                send_result(
                    &frames,
                    request.id,
                    Err((INTERNAL_ERROR, error.to_string())),
                );
                self.run_coordinator.active.lock().await.remove(&active_key);
                self.run_coordinator.queue_notify.notify_waiters();
                return;
            }
        };
        let mut run_engine = route
            .map(|route| engine.with_route(route))
            .unwrap_or(engine);
        let run_snapshot = match self
            .hook_execution_store
            .read_hook_snapshot(&execution_envelope)
            .await
            .map(Some)
        {
            Ok(value) => value,
            Err(e) => {
                send_result(&frames, request.id, Err((INTERNAL_ERROR, e.to_string())));
                self.run_coordinator.active.lock().await.remove(&active_key);
                return;
            }
        };
        if run_snapshot.as_ref().and_then(|s|s.workflow.as_ref()).is_some_and(|d|matches!(d.definition.kind,agent_core::WorkflowNodeKind::Webhook{..})){if let Some(safety)=self.safety.clone(){run_engine=run_engine.with_tool(agent_runtime::tools::WorkflowWebhookTool{safety});}}
        if let Some(g)=run_snapshot.as_ref().and_then(|s|s.installed_generation.as_ref()) {
            match run_engine.with_installed_generation(g) {Ok(engine)=>run_engine=engine,Err(error)=>{let _=self.finish_frozen_run(&execution_envelope,RunStatus::Failed,None,Some((INTERNAL_ERROR,&error.to_string()))).await;send_result(&frames,request.id,Err((INTERNAL_ERROR,error.to_string())));self.run_coordinator.active.lock().await.remove(&active_key);self.run_coordinator.queue_notify.notify_waiters();return;}}
        }
        if let Some(snapshot) = &run_snapshot
            && snapshot.tools != run_engine.tool_specs()
        {
            let _ = self
                .finish_frozen_run(
                    &execution_envelope,
                    RunStatus::Failed,
                    None,
                    Some((INTERNAL_ERROR, "冻结 tool catalog 无法恢复")),
                )
                .await;
            send_result(
                &frames,
                request.id,
                Err((INTERNAL_ERROR, "冻结 tool catalog 无法恢复".into())),
            );
            self.run_coordinator.active.lock().await.remove(&active_key);
            self.run_coordinator.queue_notify.notify_waiters();
            return;
        }
        if let Some(fingerprint) = run_snapshot
            .as_ref()
            .and_then(|s| s.context_policy_fingerprint.as_deref())
        {
            match run_engine.with_frozen_context(fingerprint) {
                Ok(engine) => run_engine = engine,
                Err(error) => {
                    let _ = self
                        .finish_frozen_run(
                            &execution_envelope,
                            RunStatus::Failed,
                            None,
                            Some((INTERNAL_ERROR, "冻结 context policy 无法恢复")),
                        )
                        .await;
                    self.run_coordinator.active.lock().await.remove(&active_key);
                    self.run_coordinator.queue_notify.notify_waiters();
                    send_result(
                        &frames,
                        request.id,
                        Err((INTERNAL_ERROR, error.to_string())),
                    );
                    return;
                }
            }
        }
        let frozen_mode = run_snapshot
            .as_ref()
            .and_then(|s| SafetyMode::parse(&s.permission_mode))
            .or_else(|| {
                delegation
                    .as_ref()
                    .and_then(|child| SafetyMode::parse(&child.permission_mode))
            })
            .unwrap_or(SafetyMode::RiskApproval);
        let continuation = match self
            .hook_continuation_store
            .read_hook_parent(&execution_envelope.scope, &run_record.run_id)
            .await
        {
            Ok(parent) => parent.is_some(),
            Err(error) => {
                let _ = self
                    .finish_frozen_run(
                        &execution_envelope,
                        RunStatus::Failed,
                        None,
                        Some((INTERNAL_ERROR, "continuation 身份无法恢复")),
                    )
                    .await;
                self.run_coordinator.active.lock().await.remove(&active_key);
                self.run_coordinator.queue_notify.notify_waiters();
                send_result(
                    &frames,
                    request.id,
                    Err((INTERNAL_ERROR, error.to_string())),
                );
                return;
            }
        };
        let _continuation_timer = continuation.then(|| {
            let cancellation = cancellation.clone();
            AbortTimer(tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                cancellation.cancel();
            }))
        });
        if continuation {
            run_engine = run_engine.for_hook_continuation();
        }
        if let Some(child) = &delegation {
            run_engine = match run_engine.for_delegation(
                &child.tools,
                child.max_rounds as usize,
                child.max_tokens as usize,
            ) {
                Ok(engine) => engine,
                Err(error) => {
                    let _ = self
                        .finish_frozen_run(
                            &execution_envelope,
                            RunStatus::Failed,
                            None,
                            Some((INTERNAL_ERROR, "子 Agent 能力无法恢复")),
                        )
                        .await;
                    self.run_coordinator.active.lock().await.remove(&active_key);
                    send_result(
                        &frames,
                        request.id,
                        Err((INTERNAL_ERROR, error.to_string())),
                    );
                    return;
                }
            };
        }
        let (agent_events, mut event_receiver) = mpsc::unbounded_channel();
        let (approval_events, mut approval_receiver) = super::frames::frame_channel();
        let trace_request_id = request_id_label(&request.id);
        let turn_span = info_span!(
            "agent_turn",
            session_id = %session_id,
            request_id = ?request.id,
        );
        let run = agent_runtime::safety::with_frozen_mode(
            frozen_mode,
            agent_runtime::loop_engine::with_native_execution(
                execution_envelope.clone(),
                agent_runtime::loop_engine::NativeExecutionPorts {
                    attempts: self.run_attempt_store.clone(),
                    tools: self.run_tool_store.clone(),
                    turns: self.run_turn_store.clone(),
                    requests: self.run_request_store.clone(),
                    discoveries: self.run_discovery_store.clone(),
                    memories: self.memory_store.clone(),
                },
                self.approvals.with_session_context(
                    session_id.clone(),
                    run_record.request_id.clone(),
                    approval_events,
                    async {
                        let _writer = session.writer.lock().await;
                        self.run_turn_store
                            .start_run_plan(&execution_envelope)
                            .await
                            .map_err(agent_storage::RuntimeError::from)?;
                        let mut history = self
                            .run_request_store
                            .read_execution_context(&execution_envelope)
                            .await
                            .map_err(agent_storage::RuntimeError::from)?
                            .snapshot
                            .messages;
                        agent_runtime::hooks::with_scope(
                            self.hook_scope(&execution_envelope),
                            async {
                                if delegation.is_some() {
                                    agent_runtime::hooks::current(
                                        agent_core::HookEvent::SubagentStart,
                                        "start",
                                        json!({"published":true}),
                                        &cancellation,
                                    )
                                    .await?;
                                }
                                if let Some(directive)=run_snapshot.as_ref().and_then(|s|s.workflow.as_ref()) {match &directive.definition.kind{
                                    agent_core::WorkflowNodeKind::ToolCall{call}=>return run_engine.run_direct_tool(&mut history,params.message,call.clone(),Some(agent_events),cancellation.clone(),Some(trace_request_id)).await,
                                    agent_core::WorkflowNodeKind::Approval{prompt}=>{use agent_runtime::safety::Approval;tokio::select!{_=cancellation.cancelled()=>return Err(agent_storage::RuntimeError::Cancelled.into()),result=self.approvals.request(prompt)=>{anyhow::ensure!(result?,"人工节点拒绝");return Ok("人工节点已批准".into())}}},
                                    agent_core::WorkflowNodeKind::Webhook{url,body,idempotency_key}=>return run_engine.run_direct_tool(&mut history,params.message,agent_core::ToolCall{id:format!("wf-webhook-{}",directive.key.occurrence),name:"workflow_webhook".into(),arguments:json!({"url":url,"body":body,"idempotency_key":idempotency_key})},Some(agent_events),cancellation.clone(),Some(trace_request_id)).await,
                                    agent_core::WorkflowNodeKind::AgentRun{..}=>{},_=>return Err(anyhow::anyhow!("纯节点不进入native执行"))
                                }}
                                run_engine
                                    .run_turn_with_events_for_request(
                                        &mut history,
                                        params.message,
                                        Some(agent_events),
                                        cancellation.clone(),
                                        Some(trace_request_id),
                                    )
                                    .await
                            },
                        )
                        .await
                    },
                ),
            ),
        )
        .instrument(turn_span);
        // 执行 future 与事件写入同时获得 poll；PG 事务不能因消费事件而暂停。
        // 完成信号只停止下一次接收，已接收的事件写入仍完成后才提交 run 终态。
        let producer_done = CancellationToken::new();
        let (result, ()) = tokio::join!(
            async {
                let result = run.await;
                producer_done.cancel();
                result
            },
            async {
                let mut events_open = true;
                let mut approvals_open = true;
                while !producer_done.is_cancelled() && (events_open || approvals_open) {
                    tokio::select! {
                        _ = producer_done.cancelled() => break,
                        event = event_receiver.recv(), if events_open => {
                            if let Some(event) = event {
                                self.publish_update(
                                    &frames,
                                    &active_key,
                                    &execution_envelope,
                                    agent_event_update(event),
                                ).await;
                            } else { events_open = false; }
                        }
                        approval = approval_receiver.recv(), if approvals_open => {
                            if let Some(ServerFrame::Event(event)) = approval {
                                self.publish_update(
                                    &frames,
                                    &active_key,
                                    &execution_envelope,
                                    ActiveRequestUpdate::Event {
                                        kind: event.event,
                                        data: event.data,
                                        run_id: None,
                                        seq: None,
                                    },
                                ).await;
                            } else if approval.is_none() { approvals_open = false; }
                        }
                    }
                }
            },
        );
        while let Ok(event) = event_receiver.try_recv() {
            self.publish_update(
                &frames,
                &active_key,
                &execution_envelope,
                agent_event_update(event),
            )
            .await;
        }
        while let Ok(ServerFrame::Event(event)) = approval_receiver.try_recv() {
            self.publish_update(
                &frames,
                &active_key,
                &execution_envelope,
                ActiveRequestUpdate::Event {
                    kind: event.event,
                    data: event.data,
                    run_id: None,
                    seq: None,
                },
            )
            .await;
        }

        let storage_error = self
            .run_coordinator
            .active
            .lock()
            .await
            .get(&active_key)
            .and_then(|active| active.storage_error.clone());
        let response = match (storage_error, result) {
            (Some(error), _) => Err((INTERNAL_ERROR, format!("持久化事件失败: {error}"))),
            (None, Ok(_)) if cancellation.is_cancelled() => {
                Err((REQUEST_CANCELLED, "请求已取消".to_owned()))
            }
            (None, Ok(content)) => Ok(
                json!({"content": content, "run_id": run_record.run_id, "turn_id": run_record.turn_id,"sandbox_requested":run_snapshot.as_ref().map(|s|&s.sandbox_requested),"sandbox_effective":run_snapshot.as_ref().map(|s|&s.sandbox_effective),"sandbox_notice":run_snapshot.as_ref().and_then(|s|s.sandbox_notice.as_ref())}),
            ),
            (None, Err(ref error))
                if cancellation.is_cancelled()
                    || matches!(
                        error.downcast_ref::<RuntimeError>(),
                        Some(RuntimeError::Cancelled)
                    ) =>
            {
                Err((REQUEST_CANCELLED, "请求已取消".to_owned()))
            }
            (None, Err(error)) => Err((INTERNAL_ERROR, format!("{error:#}"))),
        };
        let terminal_status = match &response {
            Ok(_) => RunStatus::Completed,
            Err((REQUEST_CANCELLED, _)) => RunStatus::Cancelled,
            Err(_) => RunStatus::Failed,
        };
        let content = response
            .as_ref()
            .ok()
            .and_then(|value| value["content"].as_str());
        let error = response
            .as_ref()
            .err()
            .map(|(code, message)| (*code, message.as_str()));
        let committed = match self
            .run_execution_store
            .commit_run_turn(
                &execution_envelope,
                &agent_core::TurnCommit {
                    owner: owner.clone(),
                    status: terminal_status,
                    content: content.map(str::to_owned),
                    error: error.map(|(code, msg)| (code, msg.to_owned())),
                },
            )
            .await
        {
            Ok(committed) => committed,
            Err(storage_error) => {
                tracing::error!(run_id = %run_record.run_id.0, error = %storage_error, "提交 run 终态失败");
                send_authorization_error(&frames, request.id, storage_error.into());
                self.run_coordinator.active.lock().await.remove(&active_key);
                return;
            }
        };
        if delegation.is_some()
            && let Ok(Some(child)) = self
                .delegation_store
                .read_delegation_record(&execution_envelope.scope, &run_record.run_id)
                .await
        {
            self.notify_delegation_parent(&child, EventKind::DelegationTerminal)
                .await;
        }
        let mut response = if committed.status == RunStatus::UnknownAfterRestart {
            Err((-32002, "工具执行结果未知，禁止自动重放".to_owned()))
        } else {
            response
        };
        let hook_result=agent_runtime::hooks::with_scope(self.hook_scope(&execution_envelope),async {
            if let Some(content)=committed.content.as_deref() {
                agent_runtime::hooks::current(agent_core::HookEvent::AssistantReply,"reply",json!({"content_bytes":content.len(),"content_sha256":format!("{:x}",Sha256::digest(content.as_bytes()))}),&cancellation).await?;
            }
            agent_runtime::hooks::current(agent_core::HookEvent::AfterTurn,"terminal",json!({"status":committed.status}),&cancellation).await?;
            if delegation.is_some() {agent_runtime::hooks::current(agent_core::HookEvent::SubagentEnd,"terminal",json!({"status":committed.status}),&cancellation).await?;}
            agent_runtime::hooks::current(agent_core::HookEvent::Notification,"terminal",json!({"kind":"run_terminal","status":committed.status}),&cancellation).await?;
            if committed.status==RunStatus::Completed {
                agent_runtime::hooks::current(agent_core::HookEvent::Stop,"stop",json!({"status":committed.status,"continuation_allowed":!continuation && delegation.is_none()}),&cancellation).await
            } else {Ok(agent_core::HookEffect::Observe {})}
        }).await;
        if let Ok(agent_core::HookEffect::Continue { prompt }) = hook_result {
            let operation = format!("hook:Stop:{}:stop", owner.run_id.0);
            let launch = match run_snapshot.as_ref() {
                Some(snapshot) => {
                    self.hook_continuation_store
                        .admit_stop_continuation(&execution_envelope, &operation, &prompt, snapshot)
                        .await
                }
                None => Err(agent_core::CoordinationError::Corrupt.into()),
            };
            match launch {
                Ok(Admission::New(child)) | Ok(Admission::Existing(child)) => {
                    if let Ok(value) = &mut response {
                        value["continuation_run_id"] = json!(child.run_id);
                    }
                    let state = self.clone();
                    let request = JsonRpcRequest::new(
                        child.request_id,
                        "chat.send",
                        json!({"session_id":child.session_id,"expected_lifetime":owner.session_lifetime_id,"message":prompt}),
                    );
                    let (frames, _receiver) = super::frames::frame_channel();
                    self.spawn_owned(async move {
                        Box::pin(state.handle_chat_send(request, frames)).await;
                    })
                    .await;
                }
                Err(error) => {
                    tracing::warn!(%error,"Stop continuation 准入拒绝；原 terminal 保留")
                }
            }
        } else if let Err(error) = hook_result {
            tracing::warn!(%error,"terminal hook 失败；原 terminal 保留");
        }
        if let Some(content) = committed.content.as_deref() {
            self.publish_committed_completion(&frames, &active_key, content)
                .await;
        }
        match &response {
            Ok(_) => info!(
                session_id = %session_id,
                request_id = ?active_key.request_id,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                outcome = "completed",
                "chat 请求结束"
            ),
            Err((code, error)) => info!(
                session_id = %session_id,
                request_id = ?active_key.request_id,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                outcome = "failed",
                error_code = *code,
                error,
                "chat 请求结束"
            ),
        }
        self.publish_update(
            &frames,
            &active_key,
            &execution_envelope,
            self.terminal_update(&run_record.run_id, response).await,
        )
        .await;
        self.run_coordinator.active.lock().await.remove(&active_key);
        self.run_coordinator.queue_notify.notify_waiters();
        // 维护不能改写已发布terminal，固定准入proof覆盖原摄入与诊断事务。
        if committed.status == RunStatus::Completed {
            let outcome = match self
                .memory_store
                .read_memory_settings(&execution_envelope)
                .await
            {
                Ok(snapshot) if snapshot.context_read_only => "skipped_read_only",
                Ok(_) => match self
                    .memory_store
                    .ingest_run_memory(&execution_envelope)
                    .await
                {
                    Ok(()) => "completed",
                    Err(error) => {
                        tracing::warn!(run_id=%committed.run_id.0,%error,"完成后memory维护未写入");
                        "failed"
                    }
                },
                Err(error) => {
                    tracing::warn!(%error,"memory维护无法确认冻结设置");
                    "failed"
                }
            };
            if let Err(error) = self
                .memory_store
                .record_memory_maintenance(&execution_envelope, outcome)
                .await
            {
                tracing::warn!(%error,"memory维护诊断被事务fence拒绝");
            }
        }
    }

    async fn resource_command(
        &self,
        method: &str,
        params: ResourceParams,
    ) -> Result<Value, (i64, String)> {
        let scope = self.scope();
        let key = SessionId(params.session_id.clone());
        let session = self
            .readback_store
            .read_session(&scope, &key, agent_core::HistoryReadMode::Omitted)
            .await
            .map_err(lifecycle_rpc_error)?;
        let life = &session.session_lifetime_id;
        let fail = |e: agent_core::RepositoryError| (e.rpc_code(), e.to_string());
        if method == "resources.list" {
            let entries = self
                .resource_store
                .read_resource_page(
                    &scope,
                    &key,
                    life,
                    agent_core::ResourceId(params.after_cursor),
                    params.limit,
                )
                .await
                .map_err(fail)?;
            return Ok(json!({"cursor":entries.last().map(|r|r.id),"resources":entries}));
        }
        let id = params
            .resource_id
            .ok_or_else(|| (INVALID_PARAMS, "缺少resource_id".into()))?;
        let record = self
            .resource_store
            .read_resource_record(&scope, &key, life, id)
            .await
            .map_err(fail)?;
        match method{
            "resources.read"=>Ok(json!({"resource":record})),
            "resources.logs"=>Ok(json!({"logs":self.resource_store.read_resource_log_page(&scope,&key,life,id,params.after_cursor,params.limit).await.map_err(fail)?,"cursor":record.log_cursor})),
            "resources.wait"=>self.wait_resource_scoped(&scope,&key,life,id,std::time::Duration::from_millis(params.timeout_ms.min(60000))).await.map(|r|json!({"resource":r,"timed_out":matches!(r.state.as_str(),"starting"|"running"|"stopping")})).map_err(|e|(INTERNAL_ERROR,e.to_string())),
            "resources.stop"=>{
                if params.owner_run_id.as_ref()!=Some(&record.owner.run_id){return Err((REQUEST_CONFLICT,"stop要求exact owner_run_id".into()));}
                let envelope=agent_core::EnterpriseOwnerEnvelope{scope,owner:record.owner};
                self.stop_resource_execution(&envelope,id).await.map(|r|json!({"resource":r})).map_err(|e|(INTERNAL_ERROR,e.to_string()))
            },
            _=>Err((METHOD_NOT_FOUND,"未知resources方法".into()))
        }
    }

    async fn memory_command(&self, method: &str, value: &Value) -> Result<Value, (i64, String)> {
        let session_id = value
            .get("session_id")
            .and_then(Value::as_str)
            .ok_or_else(|| (INVALID_PARAMS, "缺少 session_id".into()))?;
        let scope = self.scope();
        let key = SessionId(session_id.to_owned());
        let session = self
            .readback_store
            .read_session(&scope, &key, agent_core::HistoryReadMode::Omitted)
            .await
            .map_err(lifecycle_rpc_error)?;
        let project = self.safety.as_ref().map_or_else(
            || "".into(),
            |s| s.workspace().to_string_lossy().into_owned(),
        );
        let visibility = agent_core::MemoryVisibility {
            lifetime: session.session_lifetime_id.clone(),
            project: project.clone(),
            allow_confirmed_global: true,
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |time| time.as_secs());
        let fail = |e: agent_core::RepositoryError| (e.rpc_code(), e.to_string());
        match method {
            "memory.evidence" => {
                let params: MemoryEvidenceParams =
                    parse_params(value).map_err(|e| (INVALID_PARAMS, e))?;
                Ok(
                    json!({"evidence":self.memory_store.read_memory_evidence(&scope,&key,&visibility,&params.memory_id,params.after_source,params.limit).await.map_err(fail)?}),
                )
            }
            "memory.flywheel" => self
                .memory_store
                .read_memory_flywheel(&scope, &key, &visibility.lifetime)
                .await
                .map_err(fail),
            "memory.feedback" => {
                let params: MemoryFeedbackParams =
                    parse_params(value).map_err(|e| (INVALID_PARAMS, e))?;
                let owner = self
                    .run_fact_store
                    .read_exact_run_owner(&scope, &params.owner_run_id)
                    .await
                    .map_err(lifecycle_rpc_error)?;
                if owner.session_key.0 != session_id
                    || owner.session_lifetime_id != visibility.lifetime
                {
                    return Err((REQUEST_CONFLICT, "feedback owner session 不匹配".into()));
                }
                let envelope = agent_core::EnterpriseOwnerEnvelope {
                    scope: scope.clone(),
                    owner: owner.clone(),
                };
                self.memory_store
                    .record_run_memory_feedback(
                        &envelope,
                        &params.operation_id,
                        &params.memory_id,
                        params.feedback,
                    )
                    .await
                    .map_err(fail)?;
                Ok(
                    json!({"accepted":true,"policy":agent_memory::RECALL_POLICY,"scope":"session_lifetime"}),
                )
            }
            "memory.scope" => Ok(
                json!({"session_id":session_id,"scopes":["session","project","confirmed_global"],"legacy_visible":false}),
            ),
            "memory.recall" | "memory.list" => {
                let params: MemoryReadParams =
                    parse_params(value).map_err(|e| (INVALID_PARAMS, e))?;
                let mut entries = if method == "memory.list" {
                    self.memory_store
                        .read_memory_page(
                            &scope,
                            &key,
                            &visibility,
                            params.after_id.as_deref(),
                            params.limit.clamp(1, 100) + 1,
                        )
                        .await
                        .map_err(fail)?
                } else {
                    agent_memory::rank_with_feedback(
                        self.memory_store
                            .read_memory_candidates(&scope, &key, &visibility)
                            .await
                            .map_err(fail)?,
                        &visibility,
                        &params.query,
                        params.limit.clamp(1, 100) + 1,
                        now,
                        &self
                            .memory_store
                            .read_memory_assessments(&scope, &key, &visibility)
                            .await
                            .map_err(fail)?,
                    )
                };
                entries.retain(|e| visibility.allows(e, now));
                let total = entries.len();
                entries.truncate(params.limit.clamp(1, 100));
                let mut bytes = 0;
                let mut end = 0;
                for entry in &entries {
                    let size = serde_json::to_vec(entry)
                        .map_err(|e| (INTERNAL_ERROR, e.to_string()))?
                        .len();
                    if bytes + size > 256 * 1024 {
                        break;
                    }
                    bytes += size;
                    end += 1;
                }
                entries.truncate(end);
                let has_more = total > entries.len();
                let cursor = entries.last().map(|e| e.id.clone());
                Ok(
                    json!({"entries":entries,"cursor":cursor,"has_more":has_more,"session_id":session_id}),
                )
            }
            "memory.store" => {
                let params: MemoryStoreParams =
                    parse_params(value).map_err(|e| (INVALID_PARAMS, e))?;
                let owner = self
                    .run_fact_store
                    .read_exact_run_owner(&scope, &params.owner_run_id)
                    .await
                    .map_err(lifecycle_rpc_error)?;
                if owner.session_key.0 != session_id
                    || owner.session_lifetime_id != session.session_lifetime_id
                {
                    return Err((REQUEST_CONFLICT, "memory stale owner".into()));
                }
                let memory_scope = match params.scope.as_deref().unwrap_or("session") {
                    "session" => {
                        agent_core::MemoryScope::Session(session.session_lifetime_id.clone())
                    }
                    "project" if params.confirmed_by_user => {
                        agent_core::MemoryScope::Project(project)
                    }
                    "global" if params.confirmed_by_user => agent_core::MemoryScope::Global,
                    _ => return Err((INVALID_PARAMS, "跨会话 scope 要求用户显式确认".into())),
                };
                if params.operation_id.is_empty() || params.operation_id.len() > 256 {
                    return Err((INVALID_PARAMS, "memory operation_id 无效".into()));
                }
                let entry = agent_core::MemoryRecord {
                    id: format!("rpc:{}:{}", owner.run_id.0, params.operation_id),
                    layer: agent_core::MemoryLayer::Semantic,
                    scope: memory_scope,
                    kind: agent_core::MemoryKind::Explicit,
                    content_digest: format!("{:x}", Sha256::digest(params.content.as_bytes())),
                    content: params.content,
                    source: Some(owner.clone()),
                    source_message_ids: vec![],
                    event_time: now,
                    created_at: now,
                    updated_at: now,
                    expires_at: params
                        .ttl_days
                        .map(|days| now.saturating_add(days.saturating_mul(86400))),
                    confidence: 100,
                    confirmed_by_user: params.confirmed_by_user,
                    revision: 0,
                };
                let envelope = agent_core::EnterpriseOwnerEnvelope {
                    scope: scope.clone(),
                    owner: owner.clone(),
                };
                let saved = self
                    .memory_store
                    .store_run_memory(&envelope, &entry)
                    .await
                    .map_err(fail)?;
                Ok(json!({"entry":saved}))
            }
            "memory.forget" => {
                let params: MemoryForgetParams =
                    parse_params(value).map_err(|e| (INVALID_PARAMS, e))?;
                let owner = self
                    .run_fact_store
                    .read_exact_run_owner(&scope, &params.owner_run_id)
                    .await
                    .map_err(lifecycle_rpc_error)?;
                if owner.session_key.0 != session_id {
                    return Err((REQUEST_CONFLICT, "memory owner session 不匹配".into()));
                }
                let envelope = agent_core::EnterpriseOwnerEnvelope {
                    scope: scope.clone(),
                    owner: owner.clone(),
                };
                Ok(
                    json!({"forgotten":self.memory_store.forget_run_memory(&envelope,&visibility,&params.memory_id,params.revision).await.map_err(fail)?}),
                )
            }
            _ => Err((METHOD_NOT_FOUND, "未知 memory 方法".into())),
        }
    }

    async fn query_session_metadata(
        &self,
        session_id: &str,
    ) -> Result<agent_core::SessionMetadata, (i64, String)> {
        self.session_control_store
            .read_session_metadata(&self.scope(), &SessionId(session_id.to_owned()))
            .await
            .map_err(|e| (e.rpc_code(), e.to_string()))?
            .filter(|m| !m.deleted)
            .ok_or_else(|| (INVALID_PARAMS, "session 已删除或不存在".into()))
    }

    async fn query_session_trace_store(
        &self,
        session_id: &str,
    ) -> Result<agent_runtime::session::SessionStore, (i64, String)> {
        let metadata = self.query_session_metadata(session_id).await?;
        self.session
            .open_known_session(session_id)
            .and_then(|s| s.with_trace_lifetime(&metadata.lifetime))
            .map_err(|e| (INVALID_PARAMS, e.to_string()))
    }

    pub(super) async fn session_runtime(
        &self,
        requested_id: Option<&str>,
    ) -> Result<Arc<SessionRuntime>, (i64, String)> {
        let session_id = match requested_id {
            Some(session_id) => session_id.to_owned(),
            None => self
                .session_supervisor
                .legacy_session_id
                .lock()
                .await
                .clone(),
        };
        let metadata = self
            .session_control_store
            .read_session_metadata(&self.scope(), &SessionId(session_id.clone()))
            .await
            .map_err(|e| (e.rpc_code(), e.to_string()))?;
        if metadata.as_ref().is_some_and(|m| m.deleted) {
            return Err((INVALID_PARAMS, "session 已删除".into()));
        }
        if session_id == self.session_supervisor.default_session.id
            && metadata
                .as_ref()
                .is_some_and(|m| m.lifetime == self.session_supervisor.default_session.lifetime)
        {
            return Ok(self.session_supervisor.default_session.clone());
        }
        if let Some(runtime) = self
            .session_supervisor
            .sessions
            .lock()
            .await
            .get(&session_id)
            .cloned()
            && metadata
                .as_ref()
                .is_some_and(|m| m.lifetime == runtime.lifetime)
        {
            return Ok(runtime);
        }
        let store = match self.session.open_session(&session_id) {
            Ok(store) => store,
            Err(open_error) => {
                if metadata.is_none() {
                    return Err((INVALID_PARAMS, format!("{open_error:#}")));
                }
                self.session
                    .open_known_session(&session_id)
                    .map_err(|error| (INVALID_PARAMS, format!("{error:#}")))?
            }
        };
        let store = Arc::new(store);
        if self.scope() == TenantContext::local()
            && metadata.as_ref().is_none_or(|m| !m.legacy_imported)
        {
            let history = store
                .load()
                .await
                .map_err(|e| (INTERNAL_ERROR, format!("{e:#}")))?;
            self.session_control_store
                .import_legacy_session(
                    &self.scope(),
                    &SessionId(session_id.clone()),
                    metadata.as_ref().map(|m| &m.lifetime),
                    &history,
                )
                .await
                .map_err(|e| (e.rpc_code(), e.to_string()))?;
        }
        let snapshot = self
            .readback_store
            .read_session(
                &self.scope(),
                &SessionId(session_id.clone()),
                agent_core::HistoryReadMode::Omitted,
            )
            .await
            .map_err(|e| (e.rpc_code(), e.to_string()))?;
        let store = Arc::new(
            store
                .with_trace_lifetime(&snapshot.session_lifetime_id)
                .map_err(|e| (INTERNAL_ERROR, e.to_string()))?,
        );
        let mut engine = self
            .session_supervisor
            .default_session
            .engine
            .for_session(store.clone());
        if let Some(child) = self
            .delegation_store
            .read_session_delegation(&self.scope(), &SessionId(session_id.clone()))
            .await
            .map_err(|error| (INTERNAL_ERROR, error.to_string()))?
        {
            engine = engine
                .for_delegation(
                    &child.tools,
                    child.max_rounds as usize,
                    child.max_tokens as usize,
                )
                .map_err(|error| (INTERNAL_ERROR, error.to_string()))?;
            engine = engine.with_tool(self.frozen_child_read_tool(&child)?);
        }
        let runtime = Arc::new(SessionRuntime {
            id: session_id.clone(),
            engine: Arc::new(engine),
            lifetime: snapshot.session_lifetime_id,
            writer: Arc::new(Mutex::new(())),
            store,
        });
        let mut sessions = self.session_supervisor.sessions.lock().await;
        sessions.insert(session_id, runtime.clone());
        Ok(runtime)
    }

    async fn session_new(self: &Arc<Self>) -> Result<Value, (i64, String)> {
        self.session_create(SessionCreateParams::default()).await
    }

    async fn session_create(
        self: &Arc<Self>,
        params: SessionCreateParams,
    ) -> Result<Value, (i64, String)> {
        let _barrier = self.session_supervisor.control_lock.lock().await;
        let (session_id, _) = match params.session_id {
            Some(key) => (
                key.clone(),
                self.session
                    .open_known_session(&key)
                    .map_err(|e| (INVALID_PARAMS, e.to_string()))?,
            ),
            None => {
                if let Some(operation) = params.operation_id.as_deref() {
                    let key = self.session.session_key_for_operation(operation);
                    (
                        key.clone(),
                        self.session
                            .open_known_session(&key)
                            .map_err(|e| (INVALID_PARAMS, e.to_string()))?,
                    )
                } else {
                    self.session
                        .create_isolated_session()
                        .map_err(|e| (INTERNAL_ERROR, e.to_string()))?
                }
            }
        };
        let operation = params
            .operation_id
            .unwrap_or_else(|| format!("create:{session_id}"));
        let created = self
            .commit_session_authorized(
                &agent_core::SessionCommand::Create {
                    key: SessionId(session_id.clone()),
                },
                None,
                &operation,
            )
            .await
            .map_err(lifecycle_rpc_error)?;
        let current = self
            .session_control_store
            .read_session_metadata(&self.scope(), &SessionId(session_id.clone()))
            .await
            .map_err(|e| (e.rpc_code(), e.to_string()))?;
        if current.is_none_or(|m| m.deleted || m.lifetime != created.lifetime) {
            return Err((
                REQUEST_CONFLICT,
                "create receipt 属于旧 lifetime；请读回当前会话".into(),
            ));
        }
        self.session_control_store
            .select_session(
                &self.scope(),
                &SessionId(session_id.clone()),
                &created.lifetime,
            )
            .await
            .map_err(|e| (e.rpc_code(), e.to_string()))?;
        *self.session_supervisor.legacy_session_id.lock().await = session_id.clone();
        let mut snapshot = self
            .session_readback_value(&session_id, agent_core::HistoryReadMode::Canonical)
            .await?;
        snapshot["created"] = json!(true);
        self.dispatch_hook_publications()
            .await
            .map_err(|e| (INTERNAL_ERROR, e.to_string()))?;
        Ok(snapshot)
    }

    async fn session_close(
        self: &Arc<Self>,
        params: SessionEndParams,
    ) -> Result<Value, (i64, String)> {
        let key = SessionId(params.session_id.clone());
        let life = params
            .expected_lifetime
            .as_ref()
            .ok_or((INVALID_PARAMS, "close 缺少 expected lifetime".into()))?;

        let owners = {
            let _barrier = self.session_supervisor.control_lock.lock().await;
            let snapshot = self
                .readback_store
                .read_session(&self.scope(), &key, agent_core::HistoryReadMode::Omitted)
                .await
                .map_err(lifecycle_rpc_error)?;
            if &snapshot.session_lifetime_id != life {
                return Err((REQUEST_CONFLICT, "close stale lifetime".into()));
            }
            if let Some(receipt) = self
                .lifecycle_store
                .read_close_receipt(&self.scope(), &key, life, &params.operation_id)
                .await
                .map_err(lifecycle_rpc_error)?
            {
                return Ok(json!({"closed":true,"metadata":receipt,"duplicate":true}));
            }
            snapshot
                .run_owners
                .into_iter()
                .filter(|owner| {
                    snapshot
                        .active_runs
                        .iter()
                        .any(|run| run.run_id == owner.run_id)
                })
                .collect::<Vec<_>>()
        };
        for owner in &owners {
            self.cancel(CancelParams {
                exact_owner: Some(owner.clone()),
                request_id: None,
                run_id: Some(owner.run_id.clone()),
                session_id: Some(key.0.clone()),
            })
            .await?;
        }
        loop {
            let notified = self.run_coordinator.queue_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let barrier = self.session_supervisor.control_lock.lock().await;
            let mut settled = true;
            for owner in &owners {
                let run = self
                    .queue_store
                    .read_run_record(&self.scope(), &owner.run_id)
                    .await
                    .map_err(|e| (e.rpc_code(), e.to_string()))?
                    .ok_or((INTERNAL_ERROR, "关闭期间run事实丢失".into()))?;
                if !run.status.terminal() {
                    settled = false;
                    break;
                }
            }
            let attached = self
                .run_coordinator
                .active
                .lock()
                .await
                .values()
                .any(|active| owners.iter().any(|owner| owner.run_id == active.run_id));
            if settled && !attached {
                // 新准入不会被旧 close 取消；repository busy/lifetime CAS 决定是否关闭。
                let receipt = self
                    .commit_session_authorized(
                        &agent_core::SessionCommand::Close { key: key.clone() },
                        Some(life),
                        &params.operation_id,
                    )
                    .await
                    .map_err(lifecycle_rpc_error)?;
                if !self
                    .run_coordinator
                    .active
                    .lock()
                    .await
                    .keys()
                    .any(|active| active.session_id == key.0)
                {
                    self.session_supervisor.sessions.lock().await.remove(&key.0);
                }
                drop(barrier);
                self.dispatch_hook_publications()
                    .await
                    .map_err(|e| (INTERNAL_ERROR, e.to_string()))?;
                return Ok(json!({"closed":true,"metadata":receipt,"duplicate":false}));
            }
            drop(barrier);
            notified.await;
        }
    }

    async fn session_end(
        self: &Arc<Self>,
        params: SessionEndParams,
        delete: bool,
    ) -> Result<Value, (i64, String)> {
        let _barrier = self.session_supervisor.control_lock.lock().await;
        let key = SessionId(params.session_id.clone());
        if self
            .run_coordinator
            .active
            .lock()
            .await
            .keys()
            .any(|k| k.session_id == params.session_id)
        {
            return Err((
                REQUEST_CONFLICT,
                "session 正在执行，必须先精确 cancel 并等待结算".into(),
            ));
        }
        self.commit_session_authorized(
            &agent_core::SessionCommand::End {
                key: key.clone(),
                delete,
            },
            params.expected_lifetime.as_ref(),
            &params.operation_id,
        )
        .await
        .map_err(lifecycle_rpc_error)?;
        self.session_supervisor
            .sessions
            .lock()
            .await
            .remove(&params.session_id);
        self.dispatch_hook_publications()
            .await
            .map_err(|e| (INTERNAL_ERROR, e.to_string()))?;
        Ok(json!({"session_id":params.session_id,"deleted":delete,"cleared":!delete}))
    }

    async fn session_compact(
        self: &Arc<Self>,
        params: SessionCompactParams,
    ) -> Result<Value, (i64, String)> {
        let snapshot = self
            .readback_store
            .read_session(
                &self.scope(),
                &SessionId(params.session_id.clone()),
                agent_core::HistoryReadMode::Omitted,
            )
            .await
            .map_err(lifecycle_rpc_error)?;
        let owner = self
            .run_fact_store
            .read_exact_run_owner(&self.scope(), &params.owner_run_id)
            .await
            .map_err(lifecycle_rpc_error)?;
        if owner.session_key.0 != params.session_id
            || owner.session_lifetime_id != snapshot.session_lifetime_id
        {
            return Err((REQUEST_CONFLICT, "compact owner 不匹配".into()));
        }
        if params.operation_id.is_empty() || params.operation_id.len() > 256 {
            return Err((INVALID_PARAMS, "compact operation_id 无效".into()));
        }
        if let Some(stored) = self
            .run_request_store
            .read_compact_request(
                &self.scope(),
                &owner.session_key,
                &owner.session_lifetime_id,
                &params.operation_id,
            )
            .await
            .map_err(|e| (REQUEST_CONFLICT, e.to_string()))?
        {
            if stored.expected_revision != params.expected_revision
                || stored.compatibility_owner_run_id.as_ref() != Some(&params.owner_run_id)
            {
                return Err((REQUEST_CONFLICT, "compact operation 来源冲突".into()));
            }
            let mut response = self
                .start_compact_command(stored, agent_core::HookChannel::DaemonRpc, true)
                .await?;
            response["replayed"] = json!(true);
            return Ok(response);
        }
        if let Some(generation) = self
            .run_request_store
            .read_compaction_result(
                &agent_core::EnterpriseOwnerEnvelope {
                    scope: self.scope(),
                    owner: owner.clone(),
                },
                &params.operation_id,
                Some(params.expected_revision),
            )
            .await
            .map_err(|e| (REQUEST_CONFLICT, e.to_string()))?
        {
            return Ok(
                json!({"session_id":params.session_id,"projection_generation":generation,"transcript_revision":params.expected_revision,"replayed":true}),
            );
        }
        if snapshot.transcript_revision != params.expected_revision {
            return Err((REQUEST_CONFLICT, "compact source revision 已变化".into()));
        }
        self.start_compact_command(
            agent_core::CompactRunRequest {
                session_key: owner.session_key,
                session_lifetime_id: owner.session_lifetime_id,
                operation_id: params.operation_id,
                expected_revision: params.expected_revision,
                expected_projection_generation: snapshot.projection_generation,
                compatibility_owner_run_id: Some(params.owner_run_id),
            },
            agent_core::HookChannel::DaemonRpc,
            true,
        )
        .await
    }

    async fn session_fork(
        self: &Arc<Self>,
        params: SessionForkParams,
    ) -> Result<Value, (i64, String)> {
        let _barrier = self.session_supervisor.control_lock.lock().await;
        self.session
            .open_known_session(&params.target_session_id)
            .map_err(|e| (INVALID_PARAMS, e.to_string()))?;
        let source = SessionId(params.session_id);
        let target = SessionId(params.target_session_id);
        self.commit_session_authorized(
            &agent_core::SessionCommand::Fork {
                source,
                target: target.clone(),
                revision: params.expected_revision,
            },
            params.expected_lifetime.as_ref(),
            &params.operation_id,
        )
        .await
        .map_err(lifecycle_rpc_error)?;
        self.dispatch_hook_publications()
            .await
            .map_err(|e| (INTERNAL_ERROR, e.to_string()))?;
        Ok(json!({"forked":true,"session_id":target}))
    }

    async fn session_load_mode(
        &self,
        session_id: Option<&str>,
        mode: agent_core::HistoryReadMode,
    ) -> Result<Value, (i64, String)> {
        let key = match session_id {
            Some(key) => key.to_owned(),
            None => self
                .session_supervisor
                .legacy_session_id
                .lock()
                .await
                .clone(),
        };
        self.session_readback_value(&key, mode).await
    }

    async fn session_load_page(&self, params: SessionPageParams) -> Result<Value, (i64, String)> {
        let mut snapshot = self
            .readback_store
            .read_session(
                &self.scope(),
                &SessionId(params.session_id.clone()),
                agent_core::HistoryReadMode::Canonical,
            )
            .await
            .map_err(|e| (e.rpc_code(), e.to_string()))?;
        let history = std::mem::take(&mut snapshot.messages);
        let batch_ranges = std::mem::take(&mut snapshot.batch_ranges);
        let total_messages = history.len();
        let mut start = params.offset.min(total_messages);
        let limit = params.limit.clamp(1, WEB_PAGE_MAX_LIMIT);
        let mut end = start.saturating_add(limit).min(total_messages);
        for &(batch_start, batch_end) in &batch_ranges {
            if (batch_start as usize) < start && start < (batch_end as usize) {
                start = batch_start as usize;
            }
            if (batch_start as usize) < end && end < (batch_end as usize) {
                end = batch_end as usize;
            }
        }
        let page = history
            .into_iter()
            .skip(start)
            .take(end - start)
            .collect::<Vec<_>>();
        let paired = page
            .iter()
            .any(|m| !m.tool_calls.is_empty() || m.role == agent_runtime::provider::Role::Tool);
        if paired
            && serde_json::to_vec(&page)
                .map_err(|e| (INTERNAL_ERROR, e.to_string()))?
                .len()
                > MAX_FRAME_BYTES / 2
        {
            return Err((
                INVALID_PARAMS,
                "完整 tool batch 超过读取预算；需 artifact readback".into(),
            ));
        }
        let (messages, truncated_page) = if paired {
            (
                page.iter()
                    .map(serde_json::to_value)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| (INTERNAL_ERROR, e.to_string()))?,
                false,
            )
        } else {
            bounded_json_page(
                page.into_iter(),
                end - start,
                "消息过大，已截断；可缩小加载范围查看其余 Session 内容",
            )
        };
        let has_more = truncated_page || end < total_messages;
        let message_ids = snapshot
            .message_ids
            .iter()
            .skip(start)
            .take(messages.len())
            .cloned()
            .collect::<Vec<_>>();
        let mut readback = self.decorate_session_readback(snapshot)?;
        readback["messages"] = json!(messages);
        readback["message_ids"] = json!(message_ids);
        readback["history_mode"] = json!("canonical");
        readback["omitted"] = json!([
            "complete_messages",
            "batch_ranges",
            "plan_execution_identity"
        ]);
        readback["offset"] = json!(start);
        readback["cursor"] = json!(start + readback["messages"].as_array().map_or(0, Vec::len));
        readback["limit"] = json!(limit);
        readback["total_messages"] = json!(total_messages);
        readback["has_more"] = json!(has_more);
        Ok(readback)
    }

    async fn session_snapshot(&self, session_id: &str) -> Result<Value, (i64, String)> {
        self.session_readback_value(session_id, agent_core::HistoryReadMode::Canonical)
            .await
    }

    async fn session_readback_value(
        &self,
        session_id: &str,
        mode: agent_core::HistoryReadMode,
    ) -> Result<Value, (i64, String)> {
        let snapshot = self
            .readback_store
            .read_session(&self.scope(), &SessionId(session_id.to_owned()), mode)
            .await
            .map_err(|e| (e.rpc_code(), e.to_string()))?;
        self.decorate_session_readback(snapshot)
    }

    fn decorate_session_readback(
        &self,
        snapshot: agent_core::SessionReadback,
    ) -> Result<Value, (i64, String)> {
        let active_requests = snapshot
            .active_runs
            .iter()
            .map(|run| run.request_id.clone())
            .collect::<Vec<_>>();
        let pending_approvals = snapshot
            .pending_interactions
            .iter()
            .filter(|i| i.kind == "approval")
            .map(|i| {
                let run = snapshot
                    .active_runs
                    .iter()
                    .find(|run| run.run_id == i.owner_run_id)
                    .ok_or_else(|| {
                        (
                            INTERNAL_ERROR,
                            "pending interaction 缺少 active owner".into(),
                        )
                    })?;
                Ok(PendingApprovalInfo {
                    id: i.interaction_id.0.clone(),
                    request_id: run.request_id.clone(),
                    prompt: i.prompt.clone(),
                })
            })
            .collect::<Result<Vec<_>, (i64, String)>>()?;
        let status = if !pending_approvals.is_empty() {
            "waiting"
        } else if !active_requests.is_empty() {
            "running"
        } else {
            "idle"
        };
        let mut value =
            serde_json::to_value(snapshot).map_err(|e| (INTERNAL_ERROR, e.to_string()))?;
        value["active_requests"] = json!(active_requests);
        value["pending_approvals"] = json!(pending_approvals);
        value["status"] = json!(status);
        value["workspace"] = self
            .safety
            .as_ref()
            .map(|s| json!(s.workspace()))
            .unwrap_or(Value::Null);
        value["capability_generation"] = json!(1);
        Ok(value)
    }

    async fn session_list(&self, params: SessionListParams) -> Result<Value, (i64, String)> {
        let limit = params.limit.clamp(1, 1000);
        let mut metadata = self
            .readback_store
            .list_sessions(
                &self.scope(),
                params.after_id.as_deref(),
                (limit + 1) as u32,
            )
            .await
            .map_err(|e| (e.rpc_code(), e.to_string()))?;
        let has_more = metadata.len() > limit;
        metadata.truncate(limit);
        let cursor = metadata.last().map(|m| m.metadata.key.0.clone());
        Ok(
            json!({"sessions": self.session_infos_from(metadata).await?,"cursor":cursor,"has_more":has_more}),
        )
    }

    async fn session_trace(&self, session_id: &str) -> Result<Value, (i64, String)> {
        let store = self.query_session_trace_store(session_id).await?;
        let records = store
            .load_trace()
            .await
            .map_err(|error| (INTERNAL_ERROR, format!("{error:#}")))?;
        Ok(json!({
            "session_id": session_id,
            "records": records,
        }))
    }

    async fn session_trace_page(&self, params: SessionPageParams) -> Result<Value, (i64, String)> {
        let store = self.query_session_trace_store(&params.session_id).await?;
        let records = store
            .load_trace()
            .await
            .map_err(|error| (INTERNAL_ERROR, format!("{error:#}")))?;
        let total_records = records.len();
        let limit = params.limit.clamp(1, WEB_PAGE_MAX_LIMIT);
        let (records, has_more) = bounded_json_page(
            records.into_iter().skip(params.offset),
            limit,
            "链路记录过大，已截断；可继续加载其余记录",
        );
        Ok(json!({
            "session_id": params.session_id,
            "records": records,
            "offset": params.offset,
            "limit": limit,
            "total_records": total_records,
            "has_more": has_more,
        }))
    }

    fn permissions_get(&self) -> Result<Value, (i64, String)> {
        let Some(safety) = self.safety.as_ref() else {
            return Err((
                INTERNAL_ERROR,
                "当前 daemon 未暴露权限模式控制器".to_owned(),
            ));
        };
        let mode = safety.mode();
        Ok(json!({
            "mode": mode.key(),
            "label": mode.label(),
            "description": mode.description(),
            "options": SafetyMode::all().into_iter().map(|option| json!({
                "mode": option.key(),
                "label": option.label(),
                "description": option.description(),
            })).collect::<Vec<_>>(),
        }))
    }

    fn models_list(&self) -> Result<Value, (i64, String)> {
        let config = self
            .config_store
            .load()
            .map_err(|error| (INTERNAL_ERROR, format!("读取模型配置失败：{error:#}")))?;
        let active_id = config.active_profile.clone().or_else(|| {
            self.provider_manager
                .as_ref()
                .map(|manager| manager.profile().id)
        });
        let mut profiles = config
            .profiles
            .iter()
            .map(ProfileSummary::from_profile)
            .collect::<Vec<_>>();
        if profiles.is_empty()
            && let Some(manager) = &self.provider_manager
        {
            profiles.push(ProfileSummary::from_profile(&manager.profile()));
        }
        Ok(json!({
            "active_id": active_id,
            "profiles": profiles,
            "config_path": self.config_store.path(),
            "providers": [
                {"api_type": "openai-chat", "label": "OpenAI 兼容"},
                {"api_type": "anthropic-messages", "label": "Anthropic Messages"},
                {"api_type": "ollama", "label": "Ollama 本地模型"},
            ],
        }))
    }

    async fn models_use(&self, profile_id: &str) -> Result<Value, (i64, String)> {
        let Some(manager) = &self.provider_manager else {
            return Err((INTERNAL_ERROR, "当前 daemon 未启用模型配置管理".to_owned()));
        };
        let profile = self
            .config_store
            .activate(profile_id)
            .map_err(|error| (INVALID_PARAMS, format!("切换模型失败：{error:#}")))?;
        manager
            .switch(profile.clone())
            .map_err(|error| (INVALID_PARAMS, format!("加载模型失败：{error:#}")))?;
        let config = self
            .config_store
            .load()
            .map_err(|error| (INTERNAL_ERROR, format!("读取备用模型配置失败：{error:#}")))?;
        manager
            .set_fallbacks(
                config
                    .fallback_profile_ids
                    .iter()
                    .filter_map(|id| {
                        config
                            .profiles
                            .iter()
                            .find(|profile| &profile.id == id)
                            .cloned()
                    })
                    .collect(),
            )
            .map_err(|error| (INVALID_PARAMS, format!("加载备用模型失败：{error:#}")))?;
        Ok(json!({
            "changed": true,
            "active_id": profile.id,
            "profile": ProfileSummary::from_profile(&profile),
        }))
    }

    async fn models_save(&self, params: ModelSaveParams) -> Result<Value, (i64, String)> {
        let Some(manager) = &self.provider_manager else {
            return Err((INTERNAL_ERROR, "当前 daemon 未启用模型配置管理".to_owned()));
        };
        let config = self
            .config_store
            .upsert(params.profile, params.activate)
            .map_err(|error| (INVALID_PARAMS, format!("保存模型配置失败：{error:#}")))?;
        let active_id = config.active_profile.clone();
        let Some(active_id) = active_id else {
            return Err((INTERNAL_ERROR, "保存后没有活动模型配置".to_owned()));
        };
        let profile = config
            .profiles
            .iter()
            .find(|profile| profile.id == active_id)
            .cloned()
            .ok_or_else(|| (INTERNAL_ERROR, "活动模型配置不存在".to_owned()))?;
        manager
            .switch(profile.clone())
            .map_err(|error| (INVALID_PARAMS, format!("加载模型失败：{error:#}")))?;
        manager
            .set_fallbacks(
                config
                    .fallback_profile_ids
                    .iter()
                    .filter_map(|id| {
                        config
                            .profiles
                            .iter()
                            .find(|profile| &profile.id == id)
                            .cloned()
                    })
                    .collect(),
            )
            .map_err(|error| (INVALID_PARAMS, format!("加载备用模型失败：{error:#}")))?;
        Ok(json!({
            "changed": true,
            "active_id": profile.id,
            "profile": ProfileSummary::from_profile(&profile),
        }))
    }

    fn permissions_set(&self, value: &str) -> Result<Value, (i64, String)> {
        let Some(safety) = self.safety.as_ref() else {
            return Err((
                INTERNAL_ERROR,
                "当前 daemon 未暴露权限模式控制器".to_owned(),
            ));
        };
        let Some(mode) = SafetyMode::parse(value) else {
            return Err((
                INVALID_PARAMS,
                "权限模式无效，可选值：request、risk、full".to_owned(),
            ));
        };
        safety.set_mode(mode);
        Ok(json!({
            "changed": true,
            "mode": mode.key(),
            "label": mode.label(),
            "description": mode.description(),
        }))
    }

    async fn session_infos(
        &self,
    ) -> Result<Vec<agent_runtime::session::SessionInfo>, (i64, String)> {
        let metadata = self
            .readback_store
            .list_sessions(&self.scope(), None, 1000)
            .await
            .map_err(|e| (e.rpc_code(), e.to_string()))?;
        self.session_infos_from(metadata).await
    }
    async fn session_infos_from(
        &self,
        metadata: Vec<agent_core::SessionListEntry>,
    ) -> Result<Vec<agent_runtime::session::SessionInfo>, (i64, String)> {
        let mut sessions = metadata
            .into_iter()
            .map(|entry| {
                let meta = entry.metadata;
                agent_runtime::session::SessionInfo {
                    id: meta.key.0.clone(),
                    path: self.session.path_for_session(&meta.key.0),
                    active: false,
                    message_count: meta.revision.0 as usize,
                    modified_at: Some((meta.updated_at_ms / 1000).max(0) as u64),
                    preview: entry.preview,
                    status: entry.status,
                    active_requests: entry.active_requests,
                    updated_at: Some((meta.updated_at_ms / 1000).max(0) as u64),
                }
            })
            .collect::<Vec<_>>();
        let current_id = self
            .session_supervisor
            .legacy_session_id
            .lock()
            .await
            .clone();
        for session in &mut sessions {
            session.active = session.id == current_id;
        }
        sessions.sort_by(|left, right| {
            right
                .active
                .cmp(&left.active)
                .then_with(|| right.modified_at.cmp(&left.modified_at))
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(sessions)
    }

    async fn session_resume(&self, session_id: &str) -> Result<Value, (i64, String)> {
        let mut snapshot = self.session_snapshot(session_id).await?;
        snapshot["resumed"] = json!(true);
        Ok(snapshot)
    }

    async fn discard_plan_command(
        &self,
        params: PlanDiscardParams,
    ) -> Result<Value, (i64, String)> {
        let _barrier = self.session_supervisor.control_lock.lock().await;
        let receipt = self
            .plan_control_store
            .discard_session_plan(
                &self.scope(),
                &SessionId(params.session_id),
                &params.expected_lifetime,
                &params.identity,
            )
            .await
            .map_err(|e| (e.rpc_code(), e.to_string()))?;
        if let Some(run) = &receipt.run_id {
            for active in self
                .run_coordinator
                .active
                .lock()
                .await
                .values()
                .filter(|active| active.run_id == *run)
            {
                active.cancellation.cancel();
            }
            self.run_coordinator.queue_notify.notify_waiters();
        }
        serde_json::to_value(receipt).map_err(|e| (INTERNAL_ERROR, e.to_string()))
    }

    async fn artifact_readback(&self, params: &ArtifactReadParams) -> Result<Value, RuntimeError> {
        use base64::Engine as _;
        let owner = self
            .run_fact_store
            .read_exact_run_owner(&self.scope(), &params.run_id)
            .await
            .map_err(|error| RuntimeError::Coordination(error))?;
        if owner.session_key.0 != params.session_id {
            return Err(agent_core::ArtifactError::Forbidden.into());
        }
        let bytes = if params.artifact_ref.starts_with("artifact:v1:") {
            self.artifact_service
                .as_ref()
                .ok_or(agent_core::ArtifactError::StorageUnavailable)?
                .read(&owner, &params.artifact_ref, params.offset, params.limit)
                .await?
                .data
        } else {
            self.observation_store
                .read_legacy_artifact(
                    &self.scope(),
                    &owner,
                    &params.artifact_ref,
                    params.offset,
                    params.limit.clamp(1, 16384),
                )
                .await
                .map_err(RuntimeError::from)?
        };
        Ok(
            json!({"data_base64":base64::engine::general_purpose::STANDARD.encode(&bytes),"offset":params.offset,"cursor":params.offset+bytes.len() as u64,"has_more":bytes.len()==params.limit.clamp(1,16384)}),
        )
    }

    async fn execute_slash(
        self: &Arc<Self>,
        line: &str,
        session_id: Option<&str>,
        channel: agent_core::HookChannel,
    ) -> Result<Value, (i64, String)> {
        let registry = SlashRegistry::builtin();
        let response = match registry.parse(line) {
            SlashParse::NotCommand => SlashResponse::Text {
                content: "输入不是 slash 命令".to_owned(),
            },
            SlashParse::Error(content) => SlashResponse::Text { content },
            SlashParse::Command(invocation) => match invocation.action {
                SlashAction::Compact => {
                    let [key, lifetime, revision, generation, operation] =
                        invocation.args.as_slice()
                    else {
                        return Err((
                            INVALID_PARAMS,
                            "/compact <session> <lifetime> <revision> <generation> <operation_id>"
                                .into(),
                        ));
                    };
                    let request = agent_core::CompactRunRequest {
                        session_key: SessionId(key.clone()),
                        session_lifetime_id: agent_core::SessionLifetimeId(lifetime.clone()),
                        operation_id: operation.clone(),
                        expected_revision: agent_core::TranscriptSeq(
                            revision
                                .parse()
                                .map_err(|_| (INVALID_PARAMS, "revision 无效".into()))?,
                        ),
                        expected_projection_generation: agent_core::ProjectionGeneration(
                            generation
                                .parse()
                                .map_err(|_| (INVALID_PARAMS, "generation 无效".into()))?,
                        ),
                        compatibility_owner_run_id: None,
                    };
                    SlashResponse::CompactStarted {
                        run: self.start_compact_command(request, channel, false).await?,
                    }
                }
                SlashAction::Plan => {
                    let content = match invocation.args.as_slice() {
                        [action] if action == "read" => {
                            let key = session_id.ok_or_else(||(INVALID_PARAMS,"缺少 session".into()))?;
                            serde_json::to_string(&self.plan_control_store.read_session_plan(&self.scope(),&SessionId(key.into())).await.map_err(|e|(e.rpc_code(),e.to_string()))?).map_err(|e|(INTERNAL_ERROR,e.to_string()))?
                        },
                        [action,key] if action == "read" => serde_json::to_string(&self.plan_control_store.read_session_plan(&self.scope(),&SessionId(key.clone())).await.map_err(|e|(e.rpc_code(),e.to_string()))?).map_err(|e|(INTERNAL_ERROR,e.to_string()))?,
                        [action,key,lifetime,plan_id,revision,digest,operation] if action == "discard" => {
                            let identity=agent_core::PlanExecution { plan_id:plan_id.clone(), revision:revision.parse().map_err(|_|(INVALID_PARAMS,"revision 无效".into()))?, content_digest:digest.clone(), operation_id:operation.clone() };
                            serde_json::to_string(&self.discard_plan_command(PlanDiscardParams { session_id:key.clone(),expected_lifetime:agent_core::SessionLifetimeId(lifetime.clone()),identity }).await?).map_err(|e|(INTERNAL_ERROR,e.to_string()))?
                        },
                        _ => return Err((INVALID_PARAMS,"用法：/plan read [session]；/plan discard <session> <lifetime> <plan_id> <revision> <digest> <operation_id>；执行请发送完整 /plan execute 命令".into())),
                    };
                    SlashResponse::Text { content }
                }
                SlashAction::Help => SlashResponse::Text {
                    content: registry.help(),
                },
                SlashAction::Memory | SlashAction::Context | SlashAction::Resources => {
                    if invocation.args.len() != 1 {
                        return Err((INVALID_PARAMS, "需要一个 session ID".into()));
                    }
                    let key = &invocation.args[0];
                    let fact = match invocation.action {
                        SlashAction::Memory => {
                            self.memory_command(
                                "memory.list",
                                &json!({"session_id":key,"limit":100}),
                            )
                            .await?
                        }
                        SlashAction::Context => {
                            let summary = self
                                .observation_store
                                .read_context_summary(&self.scope(), &SessionId(key.clone()))
                                .await
                                .map_err(|e| (e.rpc_code(), e.to_string()))?;
                            let full = self
                                .context_readback(ContextReadParams {
                                    session_id: key.clone(),
                                    expected_lifetime: summary.lifetime.clone(),
                                    run_id: invocation.args.get(1).map(|id| RunId(id.clone())),
                                    capture_id: None,
                                    local_diagnostics: invocation
                                        .args
                                        .get(2)
                                        .is_some_and(|option| option == "--local"),
                                })
                                .await;
                            match full {
                                Ok(full) => full,
                                Err(error) if invocation.args.len() <= 1 => {
                                    json!({"session_id":key,"lifetime":summary.lifetime,"transcript_revision":summary.transcript_revision,"projection_generation":summary.projection_generation,"projected_messages":summary.projected_messages,"full_provider":{"availability":"unavailable","reason":error.1}})
                                }
                                Err(error) => return Err(error),
                            }
                        }
                        _ => {
                            let scope = self.scope();
                            let id = SessionId(key.clone());
                            let snapshot = self
                                .readback_store
                                .read_session(&scope, &id, agent_core::HistoryReadMode::Omitted)
                                .await
                                .map_err(lifecycle_rpc_error)?;
                            json!({"session_id":key,"resources":self.resource_store.read_resource_page(&scope,&id,&snapshot.session_lifetime_id,agent_core::ResourceId(0),100).await.map_err(|e|(e.rpc_code(),e.to_string()))?})
                        }
                    };
                    SlashResponse::Text {
                        content: serde_json::to_string(&fact)
                            .map_err(|e| (INTERNAL_ERROR, e.to_string()))?,
                    }
                }
                SlashAction::Snapshot => {
                    if invocation.args.len() != 1 {
                        return Err((INVALID_PARAMS, "需要一个 session ID".into()));
                    }
                    SlashResponse::Text {
                        content: self
                            .session_readback_value(
                                &invocation.args[0],
                                agent_core::HistoryReadMode::Omitted,
                            )
                            .await?
                            .to_string(),
                    }
                }
                SlashAction::Artifact => {
                    self.authorize_local_request(
                        RequestId::Number(0),
                        agent_core::Action::ArtifactRead,
                    )
                    .await
                    .map_err(|e| match e {
                        RuntimeError::Authorization(reason) => {
                            (reason.rpc_code(), reason.to_string())
                        }
                        RuntimeError::Coordination(reason) => {
                            (reason.rpc_code(), reason.to_string())
                        }
                        _ => (INTERNAL_ERROR, "artifact 授权失败".into()),
                    })?;
                    let args = &invocation.args;
                    if !(3..=5).contains(&args.len()) {
                        return Err((
                            INVALID_PARAMS,
                            "用法：/artifact <session> <run> <reference> [offset limit]".into(),
                        ));
                    }
                    let params = ArtifactReadParams {
                        session_id: args[0].clone(),
                        run_id: RunId(args[1].clone()),
                        artifact_ref: args[2].clone(),
                        offset: args
                            .get(3)
                            .map(|v| v.parse())
                            .transpose()
                            .map_err(|_| (INVALID_PARAMS, "offset 无效".into()))?
                            .unwrap_or(0),
                        limit: args
                            .get(4)
                            .map(|v| v.parse())
                            .transpose()
                            .map_err(|_| (INVALID_PARAMS, "limit 无效".into()))?
                            .unwrap_or(16384),
                    };
                    let result = self.artifact_readback(&params).await.map_err(|e| match e {
                        RuntimeError::Artifact(reason) => (reason.rpc_code(), reason.to_string()),
                        RuntimeError::Authorization(reason) => {
                            (reason.rpc_code(), reason.to_string())
                        }
                        _ => (INTERNAL_ERROR, "artifact 读回失败".into()),
                    })?;
                    SlashResponse::Text {
                        content: serde_json::to_string(&result)
                            .map_err(|_| (INTERNAL_ERROR, "artifact 编码失败".into()))?,
                    }
                }
                SlashAction::Events => {
                    if !(1..=3).contains(&invocation.args.len()) {
                        return Err((
                            INVALID_PARAMS,
                            "/events <run_id> [after_seq] [limit]".into(),
                        ));
                    }
                    let params = RunEventsParams {
                        run_id: RunId(invocation.args[0].clone()),
                        after_seq: invocation
                            .args
                            .get(1)
                            .map(|v| v.parse::<u64>().map(EventSeq))
                            .transpose()
                            .map_err(|_| (INVALID_PARAMS, "after_seq 无效".into()))?,
                        limit: invocation
                            .args
                            .get(2)
                            .map(|v| v.parse::<usize>())
                            .transpose()
                            .map_err(|_| (INVALID_PARAMS, "limit 无效".into()))?,
                    };
                    let page = self
                        .run_event_readback(&params)
                        .await
                        .map_err(|error| (error.rpc_code(), error.to_string()))?;
                    SlashResponse::Text {
                        content: serde_json::to_string(&page)
                            .map_err(|_| (INTERNAL_ERROR, "事件页编码失败".into()))?,
                    }
                }
                SlashAction::Outbox => {
                    let mut records = self
                        .publication_store
                        .read_publications(&self.scope(), 0, 33)
                        .await
                        .map_err(|e| (INTERNAL_ERROR, e.to_string()))?;
                    let has_more = records.len() > 32;
                    records.truncate(32);
                    let cursor = records.last().map_or(0, |r| r.cursor);
                    SlashResponse::Text {
                        content: serde_json::to_string(&agent_core::OutboxReadback {
                            records: records.into_iter().map(Into::into).collect(),
                            cursor,
                            has_more,
                        })
                        .map_err(|e| (INTERNAL_ERROR, e.to_string()))?,
                    }
                }
                SlashAction::Identity => SlashResponse::Text {
                    content: serde_json::to_string(&self.local_identity().await.map_err(
                        |e| match e {
                            RuntimeError::Authorization(error) => {
                                (error.rpc_code(), error.to_string())
                            }
                            RuntimeError::Coordination(error) => {
                                (error.rpc_code(), error.to_string())
                            }
                            _ => (INTERNAL_ERROR, "身份读取失败".into()),
                        },
                    )?)
                    .map_err(|e| (INTERNAL_ERROR, e.to_string()))?,
                },
                SlashAction::Doctor => SlashResponse::Text {
                    content: self
                        .daemon_maintenance_store
                        .read_storage_health(&self.scope())
                        .await
                        .map_err(|e| (e.rpc_code(), e.to_string()))?
                        .to_string(),
                },
                SlashAction::Status => {
                    let snapshot = self
                        .session_load_mode(session_id, agent_core::HistoryReadMode::Omitted)
                        .await?;
                    let model = self
                        .provider_manager
                        .as_ref()
                        .map(|manager| manager.profile().model)
                        .unwrap_or_else(|| "未配置".to_owned());
                    SlashResponse::Text {
                        content: format!(
                            "会话 {} · 模型 {} · 历史 {} 条 · 活动请求 {} 个 · 待审批 {} 个",
                            snapshot["session_id"].as_str().unwrap_or("unknown"),
                            model,
                            snapshot["transcript_revision"].as_u64().unwrap_or(0),
                            snapshot["active_requests"].as_array().map_or(0, Vec::len),
                            snapshot["pending_approvals"].as_array().map_or(0, Vec::len),
                        ),
                    }
                }
                SlashAction::Run => {
                    if !(1..=2).contains(&invocation.args.len()) {
                        return Err((INVALID_PARAMS, "/run <run_id> [fact|tools|provider]".into()));
                    }
                    let id = RunId(invocation.args[0].clone());
                    if let Some(view) = invocation.args.get(1) {
                        let value = self.run_fact_value(&id, view).await?;
                        SlashResponse::Text {
                            content: serde_json::to_string(&value)
                                .map_err(|_| (INTERNAL_ERROR, "run投影编码失败".into()))?,
                        }
                    } else {
                        let read = self
                            .run_fact_store
                            .read_run_fact(&self.scope(), &id)
                            .await
                            .map_err(|e| (e.rpc_code(), e.to_string()))?;
                        let run = read.run;
                        if let Some(compact) = read.compact {
                            SlashResponse::Text {
                                content: Self::compact_projection(&run, &compact, read.view_stamp)
                                    .to_string(),
                            }
                        } else {
                            SlashResponse::Text {
                                content: format!(
                                    "run_id={} · session_id={} · status={} · seq={} · terminal={}",
                                    run.run_id.0,
                                    run.session_id.0,
                                    run.status.as_str(),
                                    run.last_seq.0,
                                    run.content
                                        .as_deref()
                                        .or(run.error_message.as_deref())
                                        .unwrap_or("")
                                ),
                            }
                        }
                    }
                }
                SlashAction::Subagents => {
                    if invocation.args.len() != 1 {
                        return Err((INVALID_PARAMS, "用法：/subagents <root_run_id>".into()));
                    }
                    let children = self
                        .delegation_store
                        .read_delegation_list(&self.scope(), &RunId(invocation.args[0].clone()))
                        .await
                        .map_err(|error| (INTERNAL_ERROR, error.to_string()))?;
                    let content = if children.is_empty() {
                        "暂无子 Agent".to_owned()
                    } else {
                        children
                            .iter()
                            .map(|child| {
                                format!(
                                    "{} · {} · {}",
                                    child.child_run_id.0,
                                    child.child_session_id.0,
                                    serde_json::to_value(child.status)
                                        .unwrap_or_default()
                                        .as_str()
                                        .unwrap_or("unknown")
                                )
                            })
                            .collect::<Vec<_>>()
                            .join("\n")
                    };
                    SlashResponse::Text { content }
                }
                SlashAction::Subagent | SlashAction::SubagentWait | SlashAction::SubagentCancel => {
                    if invocation.args.len() < 2
                        || invocation.args.len() > 3
                        || invocation.args.len() == 3
                            && invocation.action != SlashAction::SubagentWait
                    {
                        return Err((INVALID_PARAMS, "用法：/subagent[-wait|-cancel] <parent_run_id> <child_run_id> [timeout_ms]".into()));
                    }
                    let scope = SubagentScopeParams {
                        parent_run_id: RunId(invocation.args[0].clone()),
                        child_run_id: RunId(invocation.args[1].clone()),
                    };
                    let child = self.scoped_delegation(&scope).await?;
                    match invocation.action {
                        SlashAction::Subagent => SlashResponse::Text {
                            content: format!(
                                "child_run_id={} · session_id={} · status={} · result_state={} · terminal={}",
                                child.child_run_id.0,
                                child.child_session_id.0,
                                serde_json::to_value(child.status)
                                    .unwrap_or_default()
                                    .as_str()
                                    .unwrap_or("unknown"),
                                child.result_state,
                                child
                                    .content
                                    .as_deref()
                                    .or(child.error_message.as_deref())
                                    .unwrap_or("")
                            ),
                        },
                        SlashAction::SubagentWait => {
                            let timeout_ms = invocation
                                .args
                                .get(2)
                                .map(|value| value.parse::<u64>())
                                .transpose()
                                .map_err(|_| (INVALID_PARAMS, "timeout_ms 需要非负整数".into()))?;
                            let result = self
                                .wait_subagents(WaitSubagentsParams {
                                    parent_run_id: scope.parent_run_id,
                                    child_run_ids: vec![scope.child_run_id],
                                    timeout_ms,
                                    after_seq: None,
                                })
                                .await?;
                            SlashResponse::Text {
                                content: result.to_string(),
                            }
                        }
                        SlashAction::SubagentCancel => {
                            let result = self
                                .cancel(CancelParams {
                                    exact_owner: None,
                                    request_id: None,
                                    run_id: Some(child.child_run_id),
                                    session_id: Some(child.child_session_id.0),
                                })
                                .await?;
                            SlashResponse::Text {
                                content: result.to_string(),
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                SlashAction::Tools => {
                    if invocation.args.len() != 1 {
                        return Err((INVALID_PARAMS, "/tools <run_id>".into()));
                    }
                    let receipts = self
                        .run_discovery_store
                        .read_run_discoveries(
                            &self.scope(),
                            &agent_core::RunId(invocation.args[0].clone()),
                        )
                        .await
                        .map_err(|error| (error.rpc_code(), error.to_string()))?;
                    SlashResponse::Text {
                        content: json!({"receipts":receipts}).to_string(),
                    }
                }
                SlashAction::Hooks => {
                    if !(2..=4).contains(&invocation.args.len()) {
                        return Err((
                            INVALID_PARAMS,
                            "/hooks <session> <lifetime> [cursor] [limit]".into(),
                        ));
                    }
                    let cursor = invocation
                        .args
                        .get(2)
                        .map(|raw| raw.parse::<u64>())
                        .transpose()
                        .map_err(|_| (INVALID_PARAMS, "hook cursor 非法".into()))?
                        .unwrap_or(0);
                    let limit = invocation
                        .args
                        .get(3)
                        .map(|raw| raw.parse::<usize>())
                        .transpose()
                        .map_err(|_| (INVALID_PARAMS, "hook limit 非法".into()))?
                        .unwrap_or(32);
                    let readback = self
                        .hook_observation_store
                        .read_hook_history(
                            &self.scope(),
                            &SessionId(invocation.args[0].clone()),
                            &agent_core::SessionLifetimeId(invocation.args[1].clone()),
                            cursor,
                            limit,
                        )
                        .await
                        .map_err(|error| (error.rpc_code(), error.to_string()))?;
                    SlashResponse::Text {
                        content: json!(readback).to_string(),
                    }
                }
                SlashAction::Sessions => SlashResponse::Sessions {
                    sessions: self.session_infos().await?,
                    select: false,
                },
                SlashAction::Resume if invocation.args.is_empty() => SlashResponse::Sessions {
                    sessions: self
                        .session_infos()
                        .await?
                        .into_iter()
                        .filter(|session| session.message_count > 0)
                        .collect(),
                    select: true,
                },
                SlashAction::Resume => {
                    let sessions = self
                        .session_infos()
                        .await?
                        .into_iter()
                        .filter(|session| session.message_count > 0)
                        .collect::<Vec<_>>();
                    let selection = &invocation.args[0];
                    let session_id = match selection.parse::<usize>() {
                        Ok(index) if index > 0 => sessions
                            .get(index - 1)
                            .map(|session| session.id.clone())
                            .ok_or_else(|| {
                                (INVALID_PARAMS, format!("会话编号超出范围：{index}"))
                            })?,
                        Ok(_) => return Err((INVALID_PARAMS, "会话编号从 1 开始".to_owned())),
                        Err(_) => selection.clone(),
                    };
                    let snapshot = self.session_resume(&session_id).await?;
                    let count = snapshot["messages"].as_array().map_or(0, Vec::len);
                    SlashResponse::SessionChanged {
                        message: format!("已恢复会话 {session_id}，共 {count} 条消息。"),
                        snapshot,
                    }
                }
                SlashAction::New => {
                    let snapshot = self.session_new().await?;
                    let session_id = snapshot["session_id"].as_str().unwrap_or("unknown");
                    SlashResponse::SessionChanged {
                        message: format!("已新建会话：{session_id}"),
                        snapshot,
                    }
                }
                SlashAction::Cancel => SlashResponse::Text {
                    content: "当前没有前台请求；运行中按 Ctrl-C 可取消本轮。".to_owned(),
                },
                SlashAction::Skill => self.execute_skill_command(&invocation.args).await,
                SlashAction::Cron => self.execute_cron_command(&invocation.args).await,
                SlashAction::Mcp => self.execute_mcp_command(&invocation.args).await,
                SlashAction::Permissions => self.execute_permissions_command(&invocation.args),
                SlashAction::Models => self.execute_models_command(&invocation.args).await,
                SlashAction::Ping => SlashResponse::Text {
                    content: "pong".to_owned(),
                },
                SlashAction::Dogfood => self.execute_dogfood(session_id).await?,
                SlashAction::Web => SlashResponse::Text {
                    content: "请在 TUI 中使用 /web，或运行 `my-agent serve`。".to_owned(),
                },
                SlashAction::Exit => SlashResponse::Exit,
            },
        };
        serde_json::to_value(response)
            .map_err(|error| (INTERNAL_ERROR, format!("序列化 slash 响应失败：{error}")))
    }

    async fn execute_dogfood(
        &self,
        session_id: Option<&str>,
    ) -> Result<SlashResponse, (i64, String)> {
        let runtime = self.session_runtime(session_id).await?;
        let session_path = runtime.store.path_for_session(&runtime.id);
        let dogfood_path = export_dogfood_file(&runtime.id, &session_path, &self.daemon_log_path)
            .await
            .map_err(|error| (INTERNAL_ERROR, format!("生成 dogfood 日志失败：{error:#}")))?;
        info!(
            session_id = %runtime.id,
            dogfood_path = %dogfood_path.display(),
            "已导出 dogfood 日志"
        );
        Ok(SlashResponse::Text {
            content: format!("dogfood 已生成：{}", dogfood_path.display()),
        })
    }

    fn execute_permissions_command(&self, args: &[String]) -> SlashResponse {
        let Some(safety) = self.safety.as_ref() else {
            return SlashResponse::Text {
                content: "当前 daemon 未暴露权限模式控制器。".to_owned(),
            };
        };
        if args.is_empty() {
            let mode = safety.mode();
            return SlashResponse::Text {
                content: format!(
                    "当前权限模式：{}（{}）。切换用法：/permissions request|risk|full",
                    mode.label(),
                    mode.description()
                ),
            };
        }
        let Some(mode) = SafetyMode::parse(&args[0]) else {
            return SlashResponse::Text {
                content: "权限模式无效，可选值：request（请求批准）、risk（帮我批准）、full（完全访问权限）。"
                    .to_owned(),
            };
        };
        safety.set_mode(mode);
        SlashResponse::Text {
            content: format!("已切换权限模式：{}（{}）", mode.label(), mode.description()),
        }
    }

    async fn execute_models_command(&self, args: &[String]) -> SlashResponse {
        let listed = match self.models_list() {
            Ok(value) => value,
            Err((_, message)) => return SlashResponse::Text { content: message },
        };
        let active_id = listed
            .get("active_id")
            .and_then(Value::as_str)
            .unwrap_or("未设置");
        let profiles = listed
            .get("profiles")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if args.is_empty() {
            if profiles.is_empty() {
                return SlashResponse::Text {
                    content: format!(
                        "暂无模型配置。请在 Web 设置中添加模型，或设置环境变量后重启 daemon。配置文件：{}",
                        self.config_store.path().display()
                    ),
                };
            }
            let lines = profiles
                .iter()
                .enumerate()
                .map(|(index, profile)| {
                    let id = profile["id"].as_str().unwrap_or("unknown");
                    let name = profile["name"].as_str().unwrap_or(id);
                    let provider = profile["api_type"].as_str().unwrap_or("unknown");
                    let model = profile["model"].as_str().unwrap_or("unknown");
                    let marker = if id == active_id { "*" } else { " " };
                    format!(
                        "{marker} {}. {name} · {provider} · {model} · id={id}",
                        index + 1
                    )
                })
                .collect::<Vec<_>>();
            return SlashResponse::Text {
                content: format!(
                    "当前模型：{active_id}\n{}\n切换用法：/models <编号或 ID>",
                    lines.join("\n")
                ),
            };
        }
        let selection = &args[0];
        let profile_id = match selection.parse::<usize>() {
            Ok(index) if index > 0 => profiles
                .get(index - 1)
                .and_then(|profile| profile["id"].as_str())
                .map(str::to_owned),
            _ => Some(selection.clone()),
        };
        let Some(profile_id) = profile_id else {
            return SlashResponse::Text {
                content: format!("模型编号超出范围：{selection}"),
            };
        };
        match self.models_use(&profile_id).await {
            Ok(value) => {
                let profile = value.get("profile").cloned().unwrap_or(Value::Null);
                SlashResponse::Text {
                    content: format!(
                        "已切换模型：{} · {}",
                        profile["name"].as_str().unwrap_or(&profile_id),
                        profile["model"].as_str().unwrap_or("unknown")
                    ),
                }
            }
            Err((_, message)) => SlashResponse::Text { content: message },
        }
    }

    async fn execute_skill_command(&self, args: &[String]) -> SlashResponse {
        let Some(skills) = self.skills.as_ref() else {
            return SlashResponse::Text {
                content: "当前 daemon 未启用 skill 管理器。".to_owned(),
            };
        };
        let result = match args.first().map(String::as_str) {
            Some("list") if args.len() == 1 => skills.list().await.map(|items| {
                if items.is_empty() {
                    "暂无已安装 skill。".to_owned()
                } else {
                    items
                        .iter()
                        .map(|item| format!("{}@{}：{}", item.name, item.version, item.description))
                        .collect::<Vec<_>>()
                        .join("\n")
                }
            }),
            Some("install") if args.len() >= 2 => {
                let force = args.last().is_some_and(|arg| arg == "--force");
                let end = args.len() - usize::from(force);
                let path = args[1..end].join(" ");
                if path.is_empty() {
                    Err(anyhow::anyhow!("用法：/skill install <路径> [--force]"))
                } else {
                    skills
                        .install(std::path::Path::new(&path), force)
                        .await
                        .map(|outcomes| {
                            outcomes
                                .iter()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                                .join("\n")
                        })
                }
            }
            Some("update") if args.len() >= 3 => {
                let path = args[2..].join(" ");
                skills
                    .update(&args[1], std::path::Path::new(&path))
                    .await
                    .map(|outcome| outcome.to_string())
            }
            Some("remove") if args.len() >= 2 => {
                let confirmed = args.get(2).is_some_and(|arg| arg == "--confirm");
                if args.len() > 3 {
                    Err(anyhow::anyhow!("用法：/skill remove <name> --confirm"))
                } else {
                    skills.remove(&args[1], confirmed).await
                }
            }
            _ => Err(anyhow::anyhow!(
                "用法：/skill list | install <路径> [--force] | update <name> <路径> | remove <name> --confirm"
            )),
        };
        SlashResponse::Text {
            content: match result {
                Ok(content) => content,
                Err(error) => format!("Skill 命令失败：{error:#}"),
            },
        }
    }

    async fn execute_cron_command(&self, args: &[String]) -> SlashResponse {
        let Some(cron) = self.cron.as_ref() else {
            return SlashResponse::Text {
                content: "当前 daemon 未启用 cron 管理器。".to_owned(),
            };
        };
        let result = match args.first().map(String::as_str) {
            Some("list") if args.len() == 1 => {
                let jobs = cron.store().list().await;
                Ok(if jobs.is_empty() {
                    "暂无 cron 任务。".to_owned()
                } else {
                    jobs.iter()
                        .map(|job| {
                            let last = job.history.last().map_or_else(
                                || "尚未运行".to_owned(),
                                |run| {
                                    format!(
                                        "上次={}，尝试={}，结果={}",
                                        run.finished_at,
                                        run.attempts,
                                        if run.success { "成功" } else { "失败" }
                                    )
                                },
                            );
                            format!(
                                "{} [{}] {} · {} · next={} · {}",
                                job.name,
                                job.id,
                                if job.enabled { "启用" } else { "停用" },
                                job.schedule.display(),
                                job.next_run_at,
                                last
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                })
            }
            Some("add") if args.len() >= 4 => match parse_cron_add(args) {
                Ok((name, schedule, prompt, retries, backoff)) => cron
                    .store()
                    .add(name, schedule, prompt, retries, backoff)
                    .await
                    .map(|job| {
                        format!(
                            "已添加 {} [{}]，下次运行 {}",
                            job.name, job.id, job.next_run_at
                        )
                    }),
                Err(error) => Err(error),
            },
            Some("enable") if args.len() == 2 => cron
                .store()
                .set_enabled(&args[1], true)
                .await
                .map(|job| format!("已启用 {}", job.name)),
            Some("disable") if args.len() == 2 => cron
                .store()
                .set_enabled(&args[1], false)
                .await
                .map(|job| format!("已停用 {}", job.name)),
            Some("run-now") if args.len() == 2 => cron.run_now(&args[1]).await.map(|run| {
                format!(
                    "立即运行{}（尝试 {} 次）：{}",
                    if run.success { "成功" } else { "失败" },
                    run.attempts,
                    run.result
                )
            }),
            Some("remove") if args.len() == 3 && args[2] == "--confirm" => cron
                .store()
                .remove(&args[1])
                .await
                .map(|job| format!("已删除 {}", job.name)),
            Some("remove") if args.len() == 2 => Err(anyhow::anyhow!(
                "删除需要显式确认：/cron remove {} --confirm",
                args[1]
            )),
            _ => Err(anyhow::anyhow!(
                "用法：/cron list | add <name> interval=<秒>|cron=<分,时,日,月,周> [--retries=N] [--backoff=N] <prompt> | enable|disable|run-now <ID或名称> | remove <ID或名称> --confirm"
            )),
        };
        SlashResponse::Text {
            content: match result {
                Ok(content) => content,
                Err(error) => format!("Cron 命令失败：{error:#}"),
            },
        }
    }

    async fn execute_mcp_command(&self, args: &[String]) -> SlashResponse {
        let Some(mcp) = self.mcp.as_ref() else {
            return SlashResponse::Text {
                content: "当前 daemon 未启用 MCP 管理器。".to_owned(),
            };
        };
        let content = match args {
            [command] if command == "reload" => {
                mcp.reload().await;
                format_mcp_status(&mcp.status(), true)
            }
            [command] if command == "status" => format_mcp_status(&mcp.status(), true),
            [command] if command == "list" => format_mcp_status(&mcp.status(), false),
            _ => "MCP 命令失败：用法：/mcp list | status | reload".to_owned(),
        };
        SlashResponse::Text { content }
    }

    async fn handle_subscribe(
        self: Arc<Self>,
        request: JsonRpcRequest,
        frames: super::frames::FrameSender,
    ) {
        let params = match parse_params::<SubscribeParams>(&request.params) {
            Ok(params) => params,
            Err(error) => {
                send_result(&frames, request.id, Err((INVALID_PARAMS, error)));
                return;
            }
        };
        let matches = self
            .run_coordinator
            .active
            .lock()
            .await
            .iter()
            .filter(|(key, _)| {
                key.request_id == params.request_id
                    && params
                        .session_id
                        .as_deref()
                        .is_none_or(|id| key.session_id == id)
            })
            .map(|(key, active)| (key.clone(), active.run_id.clone(), active.subscribe().1))
            .collect::<Vec<_>>();
        if matches.len() > 1 {
            send_result(
                &frames,
                request.id,
                Err((
                    REQUEST_CONFLICT,
                    "request_id 跨 session 不唯一；请指定 session_id".into(),
                )),
            );
            return;
        }
        let active_match = matches.into_iter().next();
        let run = if let Some((_, run_id, _)) = &active_match {
            self.queue_store
                .read_run_record(&self.scope(), run_id)
                .await
        } else if let Some(session_id) = params.session_id.as_deref() {
            self.queue_store
                .lookup_request_run(
                    &self.scope(),
                    Some(&SessionId(session_id.into())),
                    &params.request_id,
                )
                .await
        } else {
            Ok(None)
        };
        let run = match run {
            Ok(Some(run)) => run,
            Ok(None) => {
                send_result(
                    &frames,
                    request.id,
                    Ok(
                        json!({"subscribed": false, "request_id": params.request_id, "reason": "请求未在执行"}),
                    ),
                );
                return;
            }
            Err(error) => {
                send_result(
                    &frames,
                    request.id,
                    Err((INTERNAL_ERROR, error.to_string())),
                );
                return;
            }
        };
        let mut cursor = params.after_seq.unwrap_or(EventSeq(0));
        let pending_ids = self
            .approvals
            .pending()
            .await
            .into_iter()
            .map(|item| item.id)
            .collect::<HashSet<_>>();
        let page = match self
            .event_store
            .read_run_events(&self.scope(), &run.run_id, cursor, 1000)
            .await
        {
            Ok(page) => page,
            Err(error) => {
                send_authorization_error(&frames, request.id, error.into());
                return;
            }
        };
        if page.has_more {
            // 这一页尚未发送；cursor 只能指向已经交付的历史，避免重连跳过事实。
            let _ = frames.send(ServerFrame::Response(JsonRpcResponse::failure_data(
                request.id,
                REQUEST_CONFLICT,
                "resync_required",
                json!({"kind": "resync_required", "snapshot": run,
                    "last_seq": run.last_seq, "cursor": cursor, "next_method": "run.events"}),
            )));
            return;
        }
        let events = page.events;
        for event in events {
            cursor = event.seq;
            if let Some(update) = stored_event_update(event)
                && !is_resolved_approval(&update, &pending_ids)
                && frames.send(update.to_frame(request.id.clone())).is_err()
            {
                return;
            }
        }
        if run.kind == agent_core::RunKind::Compact && run.status.terminal() {
            let response = self
                .compact_response(&run)
                .await
                .map_err(|error| (INTERNAL_ERROR, error.to_string()));
            let _ = frames.send(
                self.terminal_update(&run.run_id, response)
                    .await
                    .to_frame(request.id),
            );
            return;
        }
        if run.status == RunStatus::Completed {
            let response =
                Ok(json!({"content":run.content,"run_id":run.run_id,"turn_id":run.turn_id}));
            let _ = frames.send(
                self.terminal_update(&run.run_id, response)
                    .await
                    .to_frame(request.id),
            );
            return;
        }
        if matches!(
            run.status,
            RunStatus::Failed | RunStatus::Cancelled | RunStatus::UnknownAfterRestart
        ) {
            let response = Err((
                run.error_code.unwrap_or(INTERNAL_ERROR),
                run.error_message
                    .clone()
                    .unwrap_or_else(|| format!("run 状态: {:?}", run.status)),
            ));
            let _ = frames.send(
                self.terminal_update(&run.run_id, response)
                    .await
                    .to_frame(request.id),
            );
            return;
        }
        let Some((_, _, mut receiver)) = active_match else {
            send_result(
                &frames,
                request.id,
                Ok(
                    json!({"subscribed": false, "request_id": params.request_id, "run_id": run.run_id, "reason": "请求没有活动执行体"}),
                ),
            );
            return;
        };
        loop {
            match receiver.recv().await {
                Ok(update) => {
                    if let ActiveRequestUpdate::Event { seq: Some(seq), .. } = &update {
                        if *seq <= cursor {
                            continue;
                        }
                        cursor = *seq;
                    }
                    let terminal = matches!(update, ActiveRequestUpdate::Terminal { .. });
                    if frames.send(update.to_frame(request.id.clone())).is_err() || terminal {
                        return;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    send_result(
                        &frames,
                        request.id,
                        Ok(
                            json!({"subscribed": false, "request_id": params.request_id, "reason": "请求已结束"}),
                        ),
                    );
                    return;
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    let snapshot = match self
                        .queue_store
                        .read_run_record(&self.scope(), &run.run_id)
                        .await
                    {
                        Ok(record) => record,
                        Err(error) => {
                            send_result(
                                &frames,
                                request.id,
                                Err((error.rpc_code(), error.to_string())),
                            );
                            return;
                        }
                    };
                    let last_seq = snapshot.as_ref().map(|item| item.last_seq);
                    let _ = frames.send(ServerFrame::Response(JsonRpcResponse::failure_data(
                        request.id,
                        REQUEST_CONFLICT,
                        "resync_required",
                        json!({"kind": "resync_required", "snapshot": snapshot,
                            "last_seq": last_seq, "cursor": cursor, "next_method": "run.events"}),
                    )));
                    return;
                }
            }
        }
    }

    pub(super) async fn terminal_update(
        &self,
        run: &RunId,
        response: Result<Value, (i64, String)>,
    ) -> ActiveRequestUpdate {
        match self
            .event_store
            .read_run_view_event(&self.scope(), run, "terminal", None)
            .await
        {
            Ok(Some(event)) if event.data.get("_my_agent_view").is_none() => {
                ActiveRequestUpdate::Terminal {
                    response,
                    view: None,
                }
            }
            Ok(Some(event)) => match serde_json::from_value::<agent_core::ViewStamp>(
                event.data["_my_agent_view"].clone(),
            ) {
                Ok(stamp) => ActiveRequestUpdate::Terminal {
                    response,
                    view: Some(Box::new(stamp)),
                },
                Err(error) => ActiveRequestUpdate::Terminal {
                    response: Err((INTERNAL_ERROR, error.to_string())),
                    view: None,
                },
            },
            Ok(None) => ActiveRequestUpdate::Terminal {
                response,
                view: None,
            },
            Err(error) => ActiveRequestUpdate::Terminal {
                response: Err((error.rpc_code(), error.to_string())),
                view: None,
            },
        }
    }

    async fn publish_update(
        self: &Arc<Self>,
        frames: &super::frames::FrameSender,
        active_key: &ActiveKey,
        execution: &agent_core::EnterpriseOwnerEnvelope,
        mut update: ActiveRequestUpdate,
    ) {
        if matches!(
            update,
            ActiveRequestUpdate::Event {
                kind: EventKind::TurnCompleted,
                ..
            }
        ) {
            return; // The final content and terminal become visible after one SQLite commit.
        }
        let Some(run_id) = self
            .run_coordinator
            .active
            .lock()
            .await
            .get(active_key)
            .filter(|active| active.run_id == execution.owner.run_id)
            .map(|active| active.run_id.clone())
        else {
            return;
        };
        if let ActiveRequestUpdate::Event {
            kind,
            data,
            run_id: field_id,
            seq,
        } = &mut update
        {
            let name = serde_json::to_value(&*kind)
                .ok()
                .and_then(|value| value.as_str().map(str::to_owned))
                .unwrap_or_default();
            if *kind == EventKind::ApprovalRequired
                && let Some(id) = data["approval"]["id"].as_str()
            {
                data["interaction"] = json!({"interaction_id": id, "session_id": active_key.session_id,
                    "owner_run_id": run_id, "kind": "approval", "status": "pending", "revision": 0,
                    "payload": data["approval"]});
            }
            match self
                .event_store
                .append_run_live_event(execution, &name, data)
                .await
            {
                Ok(next_seq) => {
                    *field_id = Some(run_id.clone());
                    *seq = Some(next_seq);
                    match self
                        .event_store
                        .read_run_events(
                            &execution.scope,
                            &run_id,
                            EventSeq(next_seq.0.saturating_sub(1)),
                            1,
                        )
                        .await
                        .and_then(|page| {
                            page.events
                                .into_iter()
                                .next()
                                .filter(|event| event.seq == next_seq)
                                .ok_or(CoordinationError::Corrupt)
                        }) {
                        Ok(event) => {
                            if let Some(stamp) = event.data.get("_my_agent_view") {
                                data["_my_agent_view"] = stamp.clone();
                            }
                        }
                        Err(error) => {
                            if let Some(active) = self
                                .run_coordinator
                                .active
                                .lock()
                                .await
                                .get_mut(active_key)
                                .filter(|active| active.run_id == execution.owner.run_id)
                            {
                                active.storage_error = Some(error.to_string());
                                active.cancellation.cancel();
                            }
                            return;
                        }
                    }
                    if *kind == EventKind::ApprovalRequired
                        && self
                            .hooks
                            .configured(agent_core::HookEvent::ApprovalRequested)
                        && let Some(id) = data["approval"]["id"].as_str()
                    {
                        let outcome = agent_runtime::hooks::with_scope(
                            self.hook_scope(execution),
                            agent_runtime::hooks::current(
                                agent_core::HookEvent::ApprovalRequested,
                                id,
                                json!({"interaction_id":id}),
                                &self.shutdown,
                            ),
                        )
                        .await;
                        if let Err(error) = outcome {
                            tracing::warn!(%error,"审批观察 hook 失败；interaction 保留");
                        }
                    }
                }
                Err(error) => {
                    tracing::error!(run_id = %run_id.0, %error, "持久化事件失败");
                    if let Some(active) = self
                        .run_coordinator
                        .active
                        .lock()
                        .await
                        .get_mut(active_key)
                        .filter(|active| active.run_id == execution.owner.run_id)
                    {
                        active.storage_error = Some(error.to_string());
                        active.cancellation.cancel();
                    }
                    return;
                }
            }
        }
        if let Some(active) = self
            .run_coordinator
            .active
            .lock()
            .await
            .get_mut(active_key)
            .filter(|active| active.run_id == execution.owner.run_id)
        {
            active.publish(update.clone());
        } else {
            return;
        }
        let _ = frames.send(update.to_frame(active_key.request_id.clone()));
    }

    async fn publish_committed_completion(
        &self,
        frames: &super::frames::FrameSender,
        active_key: &ActiveKey,
        content: &str,
    ) {
        let Some(active) = self
            .run_coordinator
            .active
            .lock()
            .await
            .get(active_key)
            .map(|active| active.run_id.clone())
        else {
            return;
        };
        let event = match self
            .event_store
            .read_run_view_event(&self.scope(), &active, "assistant_content", None)
            .await
        {
            Ok(Some(event)) => event,
            _ => return,
        };
        let mut data = event.data;
        data["content"] = json!(content);
        let update = ActiveRequestUpdate::Event {
            kind: EventKind::TurnCompleted,
            data,
            run_id: Some(active),
            seq: Some(event.seq),
        };
        if let Some(active) = self.run_coordinator.active.lock().await.get_mut(active_key) {
            active.publish(update.clone());
        }
        let _ = frames.send(update.to_frame(active_key.request_id.clone()));
    }

    async fn resolve_interaction(
        self: &Arc<Self>,
        params: ApprovalRespondParams,
        rejected: bool,
    ) -> Result<Value, (i64, String)> {
        // 仅交付句柄，持久回答仍由原interaction事务唯一发布；连接Drop不能detach已接受的claim。
        let (completed, result) = tokio::sync::oneshot::channel();
        let state = self.clone();
        if !self
            .spawn_effect_job(Box::pin(async move {
                let _ = completed.send(state.resolve_interaction_owned(params, rejected).await);
            }))
            .await
        {
            return Err((REQUEST_CONFLICT, "daemon已经停止审批回答准入".into()));
        }
        result
            .await
            .map_err(|_| (INTERNAL_ERROR, "审批回答任务未确认结果".into()))?
    }
    async fn resolve_interaction_owned(
        &self,
        params: ApprovalRespondParams,
        rejected: bool,
    ) -> Result<Value, (i64, String)> {
        let approved = if rejected {
            false
        } else {
            params
                .approved
                .ok_or((INVALID_PARAMS, "缺少approved".into()))?
        };
        let _guard = self.session_supervisor.control_lock.lock().await;
        let scope = self.scope();
        let id = InteractionId(params.approval_id);
        let fail = |error: agent_core::RepositoryError| (error.rpc_code(), error.to_string());
        let current = self
            .interaction_store
            .read_interaction_record(&scope, &id)
            .await
            .map_err(fail)?
            .ok_or((INVALID_PARAMS, "interaction不存在".into()))?;
        if params
            .session_id
            .as_deref()
            .is_some_and(|session| session != current.session_id.0)
            || params
                .owner_run_id
                .as_ref()
                .is_some_and(|run| run != &current.owner_run_id)
        {
            return Err((REQUEST_CONFLICT, "interaction owner不匹配".into()));
        }
        let owner = self
            .run_fact_store
            .read_exact_run_owner(&scope, &current.owner_run_id)
            .await
            .map_err(lifecycle_rpc_error)?;
        if params
            .exact_owner
            .as_ref()
            .is_some_and(|expected| expected != &owner)
        {
            return Err((REQUEST_CONFLICT, "interaction exact owner不匹配".into()));
        }
        let envelope = agent_core::EnterpriseOwnerEnvelope {
            scope: scope.clone(),
            owner,
        };
        let revision = params.revision.unwrap_or(current.revision);
        let authorization = self
            .authorize_local_request(
                RequestId::String(format!("interaction:{}:{}:{}", id.0, revision, approved)),
                agent_core::Action::InteractionRespond,
            )
            .await
            .map_err(|e| {
                let e = e.repository_error();
                (e.rpc_code(), e.to_string())
            })?;
        let claim = async {
            Ok(self
                .interaction_store
                .respond_interaction_authorized(
                    &authorization,
                    &envelope,
                    &id,
                    revision,
                    approved,
                    Arc::new(agent_policy::PolicyEngine),
                )
                .await?)
        };
        let record = if current.status == "pending" {
            match self
                .approvals
                .respond_claimed_async(&id.0, approved, claim)
                .await
            {
                Ok(record) => record,
                Err(error) => {
                    if let Ok(Some(after)) = self
                        .interaction_store
                        .read_interaction_record(&scope, &id)
                        .await
                    {
                        if matches!(after.status.as_str(), "answered" | "rejected") {
                            let commit = agent_core::TurnCommit {
                                owner: envelope.owner.clone(),
                                status: RunStatus::UnknownAfterRestart,
                                content: None,
                                error: Some((-32002, "审批执行体已消失".into())),
                            };
                            if let Err(failure) = self
                                .run_execution_store
                                .commit_run_turn(&envelope, &commit)
                                .await
                            {
                                tracing::warn!(%failure,"审批交付未确认，unknown结算被fence拒绝");
                            }
                        }
                    }
                    return Err((REQUEST_CONFLICT, format!("{error:#}")));
                }
            }
        } else {
            self.interaction_store
                .respond_interaction_authorized(
                    &authorization,
                    &envelope,
                    &id,
                    revision,
                    approved,
                    Arc::new(agent_policy::PolicyEngine),
                )
                .await
                .map_err(fail)?
        };
        let mut value = json!({"accepted":true,"interaction":record});
        if let Some(stamp) = self
            .interaction_store
            .read_interaction_stamp(&scope, &id)
            .await
            .map_err(fail)?
        {
            value["_my_agent_view"] = json!(stamp);
        }
        Ok(value)
    }

    async fn audit_run(&self, run_id: &RunId) -> Result<Value, (i64, String)> {
        let run = self
            .queue_store
            .read_run_record(&self.scope(), run_id)
            .await
            .map_err(|error| (error.rpc_code(), error.to_string()))?
            .ok_or((INVALID_PARAMS, "run 不存在".into()))?;
        let store = self
            .session
            .open_known_session(&run.session_id.0)
            .map_err(|error| (INTERNAL_ERROR, format!("JSONL 读取失败: {error:#}")))?;
        let owner = self
            .run_fact_store
            .read_exact_run_owner(&self.scope(), run_id)
            .await
            .map_err(|e| (e.rpc_code(), e.to_string()))?;
        let store = store
            .with_trace_lifetime(&owner.session_lifetime_id)
            .map_err(|e| (INTERNAL_ERROR, e.to_string()))?;
        let (messages, jsonl_error) = match self
            .observation_store
            .read_run_transcript(&self.scope(), run_id)
            .await
        {
            Ok(messages) => (messages, None),
            Err(e) => (Vec::new(), Some(e.to_string())),
        };
        let matching_assistant = run.content.as_ref().is_some_and(|content| {
            messages.iter().any(|message| {
                message.role == agent_runtime::provider::Role::Assistant
                    && message.content.as_ref() == Some(content)
            })
        });
        let trace_label = request_id_label(&run.request_id);
        let (trace_started, trace_completed, trace_error) = match store.load_trace().await {
            Ok(records) => {
                let started = records.iter().filter(|record| matches!(record,
                    SessionTraceRecord::TurnStarted { request_id, .. } if request_id == &trace_label)).count();
                let completed = records.iter().filter(|record| matches!(record,
                    SessionTraceRecord::TurnCompleted { request_id, .. } if request_id == &trace_label)).count();
                (started, completed, None)
            }
            Err(error) => (0, 0, Some(format!("{error:#}"))),
        };
        let receipts = self
            .run_fact_store
            .read_tool_receipts(&self.scope(), run_id)
            .await
            .map_err(|error| (error.rpc_code(), error.to_string()))?;
        let incomplete = receipts
            .iter()
            .filter(|receipt| receipt.status != "terminal")
            .count();
        let mut missing_artifacts = Vec::new();
        let mut corrupt_artifacts = Vec::new();
        for receipt in &receipts {
            let Some(path) = receipt.artifact_ref.as_deref() else {
                continue;
            };
            let expected = receipt
                .receipt
                .as_ref()
                .and_then(|value| value["output_sha256"].as_str());
            let actual = if path.starts_with("artifact:v1:") {
                match &self.artifact_service {
                    Some(service) => service
                        .read(&owner, path, 0, 1)
                        .await
                        .map(|page| page.identity.sha256)
                        .map_err(RuntimeError::from),
                    None => Err(agent_core::ArtifactError::StorageUnavailable.into()),
                }
            } else {
                self.observation_store
                    .read_legacy_artifact(&self.scope(), &owner, path, 0, 1)
                    .await
                    .map(|_| expected.unwrap_or_default().to_owned())
                    .map_err(RuntimeError::from)
            };
            match actual {
                Ok(actual) if expected.is_some_and(|expected| expected != actual) => {
                    corrupt_artifacts
                        .push(json!({"round": receipt.round, "call_id": receipt.call_id}))
                }
                Ok(_) => {}
                Err(_) => missing_artifacts
                    .push(json!({"round": receipt.round, "call_id": receipt.call_id})),
            }
        }
        let diagnostic = if !missing_artifacts.is_empty() || !corrupt_artifacts.is_empty() {
            "artifact_missing_or_corrupt"
        } else if run.status == RunStatus::Completed
            && (!matching_assistant || jsonl_error.is_some())
        {
            "transcript_missing_or_diverged"
        } else if run.status == RunStatus::UnknownAfterRestart {
            "control_state_unknown"
        } else {
            "no_detected_divergence"
        };
        Ok(
            json!({"run_id": run_id, "session_id": run.session_id, "control_status": run.status,
            "last_seq": run.last_seq, "diagnostic": diagnostic,
            "snapshot":self.run_fact_store.read_run_fact(&self.scope(),run_id).await.map_err(|e|(e.rpc_code(),e.to_string()))?.snapshot,
            "jsonl_matching_assistant": matching_assistant,
            "incomplete_tool_receipts": incomplete,
            "jsonl_is_authoritative": false,
            "jsonl": {"message_count": messages.len(), "matching_assistant_anywhere": matching_assistant,
                "error": jsonl_error, "authoritative": false},
            "trace": {"turn_started": trace_started, "turn_completed": trace_completed, "error": trace_error},
            "tools": {"receipt_count": receipts.len(), "incomplete": incomplete,
                "missing_artifacts": missing_artifacts, "corrupt_artifacts": corrupt_artifacts},
            "manual_resolution": if run.status == RunStatus::UnknownAfterRestart { "run.reconcile 需要人工证据和 expected_last_seq" } else { "不适用" }}),
        )
    }

    async fn cancel(&self, params: CancelParams) -> Result<Value, (i64, String)> {
        let result = self.cancel_one(params).await?;
        let Some(run_id) = result["run_id"].as_str() else {
            return Ok(result);
        };
        let run_id = RunId(run_id.to_owned());
        let scope = self.scope();
        let root = self
            .delegation_store
            .read_delegation_record(&scope, &run_id)
            .await
            .map_err(|e| (e.rpc_code(), e.to_string()))?
            .map(|record| record.root_run_id)
            .unwrap_or_else(|| run_id.clone());
        let descendants = self
            .delegation_store
            .read_delegation_list(&scope, &root)
            .await
            .map_err(|e| (e.rpc_code(), e.to_string()))?;
        let mut owned = HashSet::from([run_id]);
        let mut targets = Vec::new();
        loop {
            let before = owned.len();
            for child in &descendants {
                if owned.contains(&child.parent_run_id) && owned.insert(child.child_run_id.clone())
                {
                    targets.push((child.child_run_id.clone(), child.child_session_id.0.clone()));
                }
            }
            if owned.len() == before {
                break;
            }
        }
        for run in &owned {
            let scope = self.scope();
            if let Ok(owner) = self.run_fact_store.read_exact_run_owner(&scope, run).await {
                if let Ok(resources) = self
                    .resource_store
                    .read_resource_page(
                        &scope,
                        &owner.session_key,
                        &owner.session_lifetime_id,
                        agent_core::ResourceId(0),
                        100,
                    )
                    .await
                {
                    for resource in resources.into_iter().filter(|r| {
                        &r.owner.run_id == run
                            && matches!(r.state.as_str(), "starting" | "running" | "stopping")
                    }) {
                        let envelope = agent_core::EnterpriseOwnerEnvelope {
                            scope: scope.clone(),
                            owner: resource.owner,
                        };
                        let _ = self.stop_resource_execution(&envelope, resource.id).await;
                    }
                }
            }
        }
        for (child_run_id, child_session_id) in targets {
            let _ = self
                .cancel_one(CancelParams {
                    exact_owner: None,
                    request_id: None,
                    run_id: Some(child_run_id),
                    session_id: Some(child_session_id),
                })
                .await;
        }
        Ok(result)
    }

    async fn finish_frozen_run(
        &self,
        envelope: &agent_core::EnterpriseOwnerEnvelope,
        status: RunStatus,
        content: Option<&str>,
        error: Option<(i64, &str)>,
    ) -> Result<agent_core::RunRecord, RuntimeError> {
        self.run_execution_store
            .commit_run_turn(
                envelope,
                &agent_core::TurnCommit {
                    owner: envelope.owner.clone(),
                    status,
                    content: content.map(str::to_owned),
                    error: error.map(|(code, message)| (code, message.to_owned())),
                },
            )
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn cancel_one(&self, params: CancelParams) -> Result<Value, (i64, String)> {
        let _guard = self.session_supervisor.control_lock.lock().await;
        let mut run = if let Some(run_id) = params.run_id {
            let session_id = params
                .session_id
                .ok_or((INVALID_PARAMS, "exact cancel 需要 session_id".into()))?;
            let run = self
                .queue_store
                .read_run_record(&self.scope(), &run_id)
                .await
                .map_err(|error| (error.rpc_code(), error.to_string()))?
                .ok_or((INVALID_PARAMS, "run 不存在".into()))?;
            if run.session_id.0 != session_id {
                return Err((REQUEST_CONFLICT, "run 不属于指定 session".into()));
            }
            run
        } else if let Some(request_id) = params.request_id {
            let result = if let Some(session_id) = params.session_id {
                self.queue_store
                    .lookup_request_run(&self.scope(), Some(&SessionId(session_id)), &request_id)
                    .await
            } else {
                self.queue_store
                    .lookup_request_run(&self.scope(), None, &request_id)
                    .await
            };
            match result.map_err(|error| (REQUEST_CONFLICT, error.to_string()))? {
                Some(run) => run,
                None => return Ok(json!({"cancelled": false, "reason": "请求不存在"})),
            }
        } else {
            return Err((INVALID_PARAMS, "需要 run_id 或 request_id".into()));
        };
        let owner = self
            .run_fact_store
            .read_exact_run_owner(&self.scope(), &run.run_id)
            .await
            .map_err(|error| (error.rpc_code(), error.to_string()))?;
        if params
            .exact_owner
            .as_ref()
            .is_some_and(|expected| expected != &owner)
        {
            return Err((REQUEST_CONFLICT, "cancel exact owner 不匹配".into()));
        }
        let envelope = agent_core::EnterpriseOwnerEnvelope {
            scope: self.scope(),
            owner,
        };
        if run.status == RunStatus::Queued {
            let removed = self
                .run_execution_store
                .cancel_queued_run(&envelope)
                .await
                .map_err(|error| (error.rpc_code(), error.to_string()))?;
            if removed {
                let key = ActiveKey {
                    session_id: run.session_id.0.clone(),
                    request_id: run.request_id.clone(),
                };
                let terminal = self
                    .terminal_update(&run.run_id, Err((REQUEST_CANCELLED, "请求已取消".into())))
                    .await;
                if let Some(active) = self.run_coordinator.active.lock().await.get_mut(&key) {
                    active.publish(terminal);
                    active.cancellation.cancel();
                }
                self.run_coordinator.queue_notify.notify_waiters();
            }
            if removed {
                return Ok(json!({"cancelled": true, "run_id": run.run_id, "status": "queued"}));
            }
            run = self
                .queue_store
                .read_run_record(&self.scope(), &run.run_id)
                .await
                .map_err(|error| (error.rpc_code(), error.to_string()))?
                .ok_or((INVALID_PARAMS, "run 不存在".into()))?;
        }
        if !matches!(
            run.status,
            RunStatus::Running | RunStatus::WaitingInteraction
        ) {
            return Ok(json!({"cancelled": false, "run_id": run.run_id, "status": run.status}));
        }
        let key = ActiveKey {
            session_id: run.session_id.0.clone(),
            request_id: run.request_id.clone(),
        };
        let token = self
            .run_coordinator
            .active
            .lock()
            .await
            .get(&key)
            .filter(|active| active.run_id == run.run_id)
            .map(|active| active.cancellation.clone());
        let Some(token) = token else {
            return Ok(json!({"cancelled": false, "reason": "执行体已消失", "run_id": run.run_id}));
        };
        self.run_discovery_store
            .request_run_cancellation(&envelope)
            .await
            .map_err(|error| (error.rpc_code(), error.to_string()))?;
        token.cancel();
        self.approvals
            .cancel_request_in_session(&run.session_id.0, &run.request_id)
            .await;
        Ok(json!({"cancelled": true, "run_id": run.run_id, "status": "running"}))
    }
}

fn bounded_json_page<T: Serialize>(
    mut items: impl Iterator<Item = T>,
    limit: usize,
    oversized_message: &str,
) -> (Vec<Value>, bool) {
    let mut values = Vec::new();
    let mut used_bytes = 1024;
    let mut has_more = false;
    for _ in 0..limit {
        let Some(item) = items.next() else {
            return (values, false);
        };
        let value = serde_json::to_value(item).unwrap_or_else(|_| {
            json!({
                "truncated": true,
                "content": "该记录无法序列化，已隐藏"
            })
        });
        let value = compact_json_for_web(value, WEB_PAGE_MAX_ITEM_BYTES, oversized_message);
        let item_bytes = serde_json::to_vec(&value).map_or(0, |bytes| bytes.len());
        if !values.is_empty() && used_bytes + item_bytes + 1 > WEB_PAGE_MAX_BYTES {
            has_more = true;
            break;
        }
        values.push(value);
        used_bytes += item_bytes + 1;
    }
    if !has_more && items.next().is_some() {
        has_more = true;
    }
    (values, has_more)
}

fn compact_json_for_web(value: Value, max_bytes: usize, message: &str) -> Value {
    let original_bytes = serde_json::to_vec(&value).map_or(max_bytes + 1, |bytes| bytes.len());
    if original_bytes <= max_bytes {
        return value;
    }
    for max_chars in [65_536, 16_384, 4_096, 1_024, 256] {
        let mut candidate = value.clone();
        truncate_json_strings(&mut candidate, max_chars);
        let candidate_bytes =
            serde_json::to_vec(&candidate).map_or(max_bytes + 1, |bytes| bytes.len());
        if candidate_bytes <= max_bytes {
            return candidate;
        }
    }
    json!({
        "truncated": true,
        "content": format!("{message}（原始大小约 {original_bytes} 字节）")
    })
}

fn truncate_json_strings(value: &mut Value, max_chars: usize) {
    match value {
        Value::String(text) if text.chars().count() > max_chars => {
            let marker = "… [已截断]";
            let keep = max_chars.saturating_sub(marker.chars().count());
            let prefix = text.chars().take(keep).collect::<String>();
            *text = format!("{prefix}{marker}");
        }
        Value::Array(items) => {
            for item in items {
                truncate_json_strings(item, max_chars);
            }
        }
        Value::Object(fields) => {
            for field in fields.values_mut() {
                truncate_json_strings(field, max_chars);
            }
        }
        _ => {}
    }
}

fn parse_cron_add(args: &[String]) -> anyhow::Result<(String, ScheduleSpec, String, u32, u64)> {
    let name = args[1].clone();
    let schedule = if let Some(seconds) = args[2].strip_prefix("interval=") {
        ScheduleSpec::Interval {
            seconds: seconds.parse().context("interval 必须是正整数秒")?,
        }
    } else if let Some(expression) = args[2].strip_prefix("cron=") {
        ScheduleSpec::Cron {
            expression: expression.replace(',', " "),
        }
    } else {
        anyhow::bail!("schedule 必须是 interval=<秒> 或 cron=<分,时,日,月,周>");
    };
    let mut retries = 0_u32;
    let mut backoff = 5_u64;
    let mut prompt = Vec::new();
    for argument in &args[3..] {
        if let Some(value) = argument.strip_prefix("--retries=") {
            retries = value.parse().context("--retries 必须是非负整数")?;
        } else if let Some(value) = argument.strip_prefix("--backoff=") {
            backoff = value.parse().context("--backoff 必须是非负整数秒")?;
        } else {
            prompt.push(argument.as_str());
        }
    }
    if prompt.is_empty() {
        anyhow::bail!("cron prompt 不能为空");
    }
    Ok((name, schedule, prompt.join(" "), retries, backoff))
}

fn format_mcp_status(status: &agent_runtime::mcp::McpStatus, include_errors: bool) -> String {
    let mut lines = Vec::new();
    if let Some(error) = &status.file_error {
        lines.push(format!("配置错误：{error}"));
    }
    for server in &status.servers {
        let state = if server.connected {
            "已连接"
        } else {
            "不可用"
        };
        let tools = if server.tools.is_empty() {
            "无工具".to_owned()
        } else {
            server.tools.join("、")
        };
        let mut line = format!("{}：{} · {}", server.name, state, tools);
        if include_errors && let Some(error) = &server.error {
            line.push_str(&format!(" · {error}"));
        }
        lines.push(line);
    }
    if lines.is_empty() {
        "未配置 MCP server（期望 .my-agent/mcp.json）。".to_owned()
    } else {
        lines.join("\n")
    }
}

async fn export_dogfood_file(
    session_id: &str,
    session_path: &Path,
    daemon_log_path: &Path,
) -> anyhow::Result<PathBuf> {
    let session_bytes = match tokio::fs::read(session_path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("读取 session 文件失败: {}", session_path.display()));
        }
    };
    let daemon_bytes = match tokio::fs::read(daemon_log_path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("读取 daemon 日志失败: {}", daemon_log_path.display()));
        }
    };
    let session_content = String::from_utf8_lossy(&session_bytes);
    let daemon_content = String::from_utf8_lossy(&daemon_bytes);
    let session_marker = format!("session_id={session_id}");
    let quoted_session_marker = format!("session_id=\"{session_id}\"");
    let chain_logs = daemon_content
        .lines()
        // tracing-subscriber may decorate field names and `=` separately, for example
        // `\x1b[3msession_id\x1b[0m\x1b[2m=\x1b[0msession-...`. Match against a
        // plain-text copy so dogfood exports work whether ANSI colors are enabled or not.
        .map(strip_ansi_sequences)
        .filter(|line| line.contains(&session_marker) || line.contains(&quoted_session_marker))
        .collect::<Vec<_>>()
        .join("\n");
    let message_count = session_content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .count();
    let chain_log_count = chain_logs.lines().count();
    let generated_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("系统时间早于 UNIX_EPOCH")?
        .as_millis();
    let session_directory = session_path.parent().unwrap_or_else(|| Path::new("."));
    tokio::fs::create_dir_all(session_directory)
        .await
        .with_context(|| format!("创建 session 目录失败: {}", session_directory.display()))?;
    let session_stem = session_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("session");
    let dogfood_path = session_directory.join(format!("dogfood-{session_stem}.log"));
    let mut output = format!(
        "# my-agent dogfood export\n\
schema_version=1\n\
generated_at_unix_ms={generated_at_ms}\n\
session_id={session_id}\n\
session_file={}\n\
daemon_log_file={}\n\
conversation_messages={message_count}\n\
chain_log_lines={chain_log_count}\n\n\
===== LLM / REACT CONVERSATION (RAW SESSION JSONL) =====\n",
        session_path.display(),
        daemon_log_path.display(),
    );
    if session_content.is_empty() {
        output.push_str("(当前 session 尚无持久化消息)\n");
    } else {
        output.push_str(&session_content);
        if !session_content.ends_with('\n') {
            output.push('\n');
        }
    }
    output.push_str("\n===== DAEMON CHAIN LOG (CURRENT SESSION ONLY) =====\n");
    if chain_logs.is_empty() {
        output.push_str("(未找到带当前 session_id 的 daemon 链路日志)\n");
    } else {
        output.push_str(&chain_logs);
        output.push('\n');
    }
    tokio::fs::write(&dogfood_path, output)
        .await
        .with_context(|| format!("写入 dogfood 文件失败: {}", dogfood_path.display()))?;
    tokio::fs::canonicalize(&dogfood_path)
        .await
        .with_context(|| format!("解析 dogfood 文件路径失败: {}", dogfood_path.display()))
}

fn strip_ansi_sequences(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0;
    let mut plain_start = 0;

    while cursor < bytes.len() {
        if bytes[cursor] == 0x1b && bytes.get(cursor + 1) == Some(&b'[') {
            output.push_str(&input[plain_start..cursor]);
            cursor += 2;
            while cursor < bytes.len() {
                let byte = bytes[cursor];
                cursor += 1;
                if (0x40..=0x7e).contains(&byte) {
                    break;
                }
            }
            plain_start = cursor;
        } else {
            cursor += 1;
        }
    }
    output.push_str(&input[plain_start..]);
    output
}

fn parse_params<T: for<'de> Deserialize<'de>>(params: &Value) -> Result<T, String> {
    serde_json::from_value(params.clone()).map_err(|error| format!("参数无效: {error}"))
}

fn request_id_label(request_id: &RequestId) -> String {
    match request_id {
        RequestId::Number(value) => value.to_string(),
        RequestId::String(value) => value.clone(),
    }
}

fn lifecycle_rpc_error(error: impl Into<agent_core::RepositoryError>) -> (i64, String) {
    let error = error.into();
    let code = if error
        == agent_core::RepositoryError::Coordination(agent_core::CoordinationError::Conflict)
    {
        REQUEST_CONFLICT
    } else {
        error.rpc_code()
    };
    (code, error.to_string())
}

fn send_result(
    frames: &super::frames::FrameSender,
    id: RequestId,
    result: Result<Value, (i64, String)>,
) {
    let response = match result {
        Ok(value) => JsonRpcResponse::success(id, value),
        Err((code, message)) => match agent_core::CoordinationError::from_rpc_code(code) {
            Some(reason) => JsonRpcResponse::failure_data(
                id,
                code,
                reason.to_string(),
                json!({"domain":"coordination","kind":reason,"retryable":reason.retryable(),"safe_auto_retry":false,"recovery_action":reason.recovery_action()}),
            ),
            None => JsonRpcResponse::failure(id, code, message),
        },
    };
    let _ = frames.send(ServerFrame::Response(response));
}

fn agent_event_update(event: AgentEvent) -> ActiveRequestUpdate {
    let (kind, data) = match event {
        AgentEvent::TurnStarted => (EventKind::TurnStarted, json!({})),
        AgentEvent::ThinkingDelta(delta) => (EventKind::ThinkingDelta, json!({"delta": delta})),
        AgentEvent::ThinkingFinished => (EventKind::ThinkingFinished, json!({})),
        AgentEvent::TextDelta(delta) => (EventKind::TextDelta, json!({"delta": delta})),
        AgentEvent::ToolStarted {
            call_id,
            name,
            round,
        } => (
            EventKind::ToolStarted,
            json!({"tool_call_id": call_id, "name": name, "round": round}),
        ),
        AgentEvent::ToolFinished {
            call_id,
            name,
            output,
            round,
            duration_ms,
            success,
            error,
        } => (
            EventKind::ToolFinished,
            json!({
                "tool_call_id": call_id,
                "name": name,
                "output": output,
                "round": round,
                "duration_ms": duration_ms,
                "success": success,
                "error": error,
            }),
        ),
        AgentEvent::TurnCompleted { content } => {
            (EventKind::TurnCompleted, json!({"content": content}))
        }
    };
    ActiveRequestUpdate::Event {
        kind,
        data,
        run_id: None,
        seq: None,
    }
}

pub(super) fn stored_event_update(mut event: StoredEvent) -> Option<ActiveRequestUpdate> {
    if event.event == "run_started" && event.data["kind"] != "compact" {
        return None;
    }
    event.data["_my_agent_replay"] = json!(true);
    let kind = if event.event == "assistant_content" {
        EventKind::TurnCompleted
    } else {
        serde_json::from_value::<EventKind>(Value::String(event.event)).ok()?
    };
    Some(ActiveRequestUpdate::Event {
        kind,
        data: event.data,
        run_id: Some(event.run_id),
        seq: Some(event.seq),
    })
}

fn is_resolved_approval(update: &ActiveRequestUpdate, pending_ids: &HashSet<String>) -> bool {
    let ActiveRequestUpdate::Event {
        kind: EventKind::ApprovalRequired,
        data,
        ..
    } = update
    else {
        return false;
    };
    data["approval"]["id"]
        .as_str()
        .is_some_and(|id| !pending_ids.contains(id))
}

fn send_authorization_error(
    frames: &super::frames::FrameSender,
    id: RequestId,
    error: RuntimeError,
) {
    let response = match error {
        RuntimeError::Ledger(reason) => JsonRpcResponse::failure_data(
            id,
            reason.rpc_code(),
            reason.to_string(),
            json!({"kind":reason,"retryable":false,"safe_auto_retry":false,"recovery_action":reason.recovery_action()}),
        ),
        RuntimeError::Authorization(reason) => JsonRpcResponse::failure_data(
            id,
            reason.rpc_code(),
            reason.to_string(),
            json!({"kind":reason,"retryable":false,"safe_auto_retry":false,"recovery_action":reason.recovery_action()}),
        ),
        RuntimeError::Coordination(reason) => JsonRpcResponse::failure_data(
            id,
            reason.rpc_code(),
            reason.to_string(),
            json!({"domain":"coordination","kind":reason,"retryable":reason.retryable(),"safe_auto_retry":false,"recovery_action":reason.recovery_action()}),
        ),
        error => JsonRpcResponse::failure(id, INTERNAL_ERROR, error.to_string()),
    };
    let _ = frames.send(ServerFrame::Response(response));
}

#[cfg(test)]
mod dogfood_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{WEB_PAGE_MAX_BYTES, bounded_json_page, export_dogfood_file};
    use agent_daemon_protocol::{JsonRpcResponse, RequestId, encode_frame};
    use agent_runtime::provider::{Message, Role};

    static NEXT_TEST: AtomicUsize = AtomicUsize::new(0);

    #[tokio::test]
    async fn exports_full_conversation_and_only_current_session_chain_logs() {
        let id = NEXT_TEST.fetch_add(1, Ordering::SeqCst);
        let directory =
            std::env::temp_dir().join(format!("my-agent-dogfood-{}-{id}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let session_id = "session-42.jsonl";
        let session_path = directory.join(session_id);
        let daemon_log_path = directory.join("daemon.log");
        let conversation = concat!(
            "{\"role\":\"user\",\"content\":\"检查项目\"}\n",
            "{\"role\":\"assistant\",\"tool_calls\":[{\"name\":\"exec\",\"arguments\":{\"command\":\"pwd\"}}]}\n",
            "{\"role\":\"tool\",\"content\":\"/workspace\",\"name\":\"exec\"}\n",
            "{\"role\":\"assistant\",\"content\":\"完成\"}\n",
        );
        std::fs::write(&session_path, conversation).unwrap();
        std::fs::write(
            &daemon_log_path,
            concat!(
                "\x1b[32m INFO\x1b[0m agent_turn{\x1b[3msession_id\x1b[0m\x1b[2m=\x1b[0msession-42.jsonl request_id=Number(7)}: 开始 ReAct 轮次\n",
                "INFO agent_turn{session_id=other.jsonl request_id=Number(8)}: 其他会话\n",
                "INFO session_id=session-42.jsonl request_id=Number(7): chat 请求结束\n",
            ),
        )
        .unwrap();

        let exported = export_dogfood_file(session_id, &session_path, &daemon_log_path)
            .await
            .unwrap();
        let content = std::fs::read_to_string(&exported).unwrap();

        assert_eq!(
            exported.file_name().and_then(|value| value.to_str()),
            Some("dogfood-session-42.log")
        );
        assert!(content.contains("conversation_messages=4"));
        assert!(content.contains("chain_log_lines=2"));
        assert!(content.contains(conversation));
        assert!(content.contains("request_id=Number(7)"));
        assert!(!content.contains("其他会话"));
        assert!(!content.contains('\x1b'));

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn bounds_large_web_pages_and_marks_oversized_content() {
        let messages = (0..80)
            .map(|_| Message::text(Role::Tool, "x".repeat(300 * 1024)))
            .collect::<Vec<_>>();
        let (items, has_more) = bounded_json_page(messages.into_iter(), 80, "消息过大，已截断");
        let encoded = serde_json::to_vec(&items).unwrap();
        assert!(encoded.len() <= WEB_PAGE_MAX_BYTES);
        let frame = encode_frame(&JsonRpcResponse::success(
            RequestId::Number(1),
            serde_json::json!({"messages": items.clone()}),
        ))
        .unwrap();
        assert!(frame.len() <= agent_daemon_protocol::MAX_FRAME_BYTES + 1);
        assert!(has_more);
        assert!(items.len() < 80);
        assert_eq!(items[0]["role"], "tool");
        assert!(items[0]["content"].as_str().unwrap().contains("已截断"));
    }
}
