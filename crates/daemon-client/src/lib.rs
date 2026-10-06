//! 统一 daemon 客户端；传输失败不改变业务终态，不重发 mutation。
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::print_stdout)
)]
use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use std::path::PathBuf;
use thiserror::Error;
type Result<T> = std::result::Result<T, ClientError>;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::{Mutex, OnceCell, mpsc};

use agent_daemon_protocol::{
    EventSeq, JsonRpcRequest, JsonRpcResponse, MAX_FRAME_BYTES, RequestId, RunId, RunRecord,
    ServerFrame, StoredEvent, decode_request, decode_server_frame_for_version, encode_frame,
    server_frame_request_id,
};
#[cfg(feature = "test-transport")]
pub struct InMemoryEnvelope {
    pub request: JsonRpcRequest,
    pub frames: mpsc::UnboundedSender<ServerFrame>,
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("daemon readback 响应格式错误：{0}")]
    InvalidReadback(#[from] serde_json::Error),
    #[error("daemon 要求从持久 cursor 重新同步：{cursor}")]
    ResyncRequired { cursor: u64, snapshot: Value },
    #[error("连接 daemon 失败：{socket}: {source}")]
    Connect {
        socket: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("daemon 协议错误：{0}")]
    Protocol(#[from] agent_daemon_protocol::ProtocolError),
    #[error("请求 id 已在等待响应")]
    DuplicateRequest,
    #[error("daemon 传输已关闭")]
    Disconnected,
    #[error("daemon 响应缺少 result")]
    MissingResult,
    #[error("旧版本展示已忽略；请从持久会话快照恢复")]
    StaleView,
    #[error("daemon RPC {0:?}")]
    Rpc(agent_daemon_protocol::RpcError),
}

#[derive(Clone)]
pub struct DaemonClient {
    inner: Arc<DaemonClientInner>,
    views: Arc<Mutex<HashMap<String, agent_daemon_protocol::ViewState>>>,
    project_views: bool,
}

struct DaemonClientInner {
    negotiated: OnceCell<agent_daemon_protocol::NegotiatedCapabilities>,
    transport: ClientTransport,
    next_id: Arc<AtomicU64>,
    socket: Option<PathBuf>,
}

enum ClientTransport {
    #[cfg(feature = "test-transport")]
    InMemory(mpsc::Sender<InMemoryEnvelope>),
    Unix {
        alive: Arc<AtomicBool>,
        requests: mpsc::Sender<JsonRpcRequest>,
        pending: Arc<Mutex<HashMap<RequestId, mpsc::Sender<ServerFrame>>>>,
    },
}

impl DaemonClient {
    #[cfg(feature = "test-transport")]
    pub fn in_memory(requests: mpsc::Sender<InMemoryEnvelope>) -> Self {
        Self {
            inner: Arc::new(DaemonClientInner {
                negotiated: OnceCell::new(),
                transport: ClientTransport::InMemory(requests),
                next_id: Arc::new(AtomicU64::new(1)),
                socket: None,
            }),
            views: Arc::new(Mutex::new(HashMap::new())),
            project_views: true,
        }
    }

    pub async fn connect_unix(socket: &Path) -> Result<Self> {
        Self::connect_with_counter(
            socket,
            Arc::new(AtomicU64::new(1)),
            Arc::new(Mutex::new(HashMap::new())),
        )
        .await
    }

    async fn connect_with_counter(
        socket: &Path,
        next_id: Arc<AtomicU64>,
        views: Arc<Mutex<HashMap<String, agent_daemon_protocol::ViewState>>>,
    ) -> Result<Self> {
        let stream = UnixStream::connect(socket)
            .await
            .map_err(|source| ClientError::Connect {
                socket: socket.to_path_buf(),
                source,
            })?;
        let (reader, mut writer) = stream.into_split();
        let (requests, mut request_receiver) = mpsc::channel::<JsonRpcRequest>(64);
        let pending = Arc::new(Mutex::new(
            HashMap::<RequestId, mpsc::Sender<ServerFrame>>::new(),
        ));
        let alive = Arc::new(AtomicBool::new(true));
        let reader_alive = alive.clone();
        let writer_alive = alive.clone();
        let reader_pending = pending.clone();
        let writer_pending = pending.clone();

        tokio::spawn(async move {
            while let Some(request) = request_receiver.recv().await {
                if !writer_alive.load(Ordering::Acquire) {
                    return;
                }
                let encoded = match encode_frame(&request) {
                    Ok(encoded) => encoded,
                    Err(error) => {
                        fail_pending(&writer_pending, &request.id, -32002, error.to_string()).await;
                        continue;
                    }
                };
                if let Err(error) = writer.write_all(&encoded).await {
                    writer_alive.store(false, Ordering::Release);
                    fail_all_pending(&writer_pending, format!("写入 daemon 请求失败: {error}"))
                        .await;
                    return;
                }
                if let Err(error) = writer.flush().await {
                    writer_alive.store(false, Ordering::Release);
                    fail_all_pending(&writer_pending, format!("刷新 daemon 请求失败: {error}"))
                        .await;
                    return;
                }
            }
        });

        tokio::spawn(async move {
            let mut reader = BufReader::new(reader);
            loop {
                let mut line = Vec::new();
                match (&mut reader)
                    .take((MAX_FRAME_BYTES + 2) as u64)
                    .read_until(b'\n', &mut line)
                    .await
                {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(error) => {
                        reader_alive.store(false, Ordering::Release);
                        fail_all_pending(&reader_pending, format!("读取 daemon 响应失败: {error}"))
                            .await;
                        return;
                    }
                }
                let frame = match decode_server_frame_for_version(&line, 1) {
                    Ok(frame) => frame,
                    Err(error) => {
                        reader_alive.store(false, Ordering::Release);
                        fail_all_pending(&reader_pending, error.to_string()).await;
                        return;
                    }
                };
                let id = server_frame_request_id(&frame).clone();
                let is_terminal = matches!(frame, ServerFrame::Response(_));
                let destination = reader_pending.lock().await.get(&id).cloned();
                if let Some(destination) = destination {
                    if destination.try_send(frame).is_err() {
                        // 慢消费者只 detach 此请求；业务继续，EOF 要求从 durable cursor 恢复。
                        reader_pending.lock().await.remove(&id);
                    }
                }
                if is_terminal {
                    reader_pending.lock().await.remove(&id);
                }
            }
            reader_alive.store(false, Ordering::Release);
            fail_all_pending(&reader_pending, "daemon 连接已关闭".to_owned()).await;
        });

        Ok(Self {
            inner: Arc::new(DaemonClientInner {
                negotiated: OnceCell::new(),
                transport: ClientTransport::Unix {
                    requests,
                    pending,
                    alive,
                },
                next_id,
                socket: Some(socket.to_path_buf()),
            }),
            views,
            project_views: true,
        })
    }

    /// 显式重连只建立传输；mutation 必须先读回，不自动重复。
    pub async fn reconnect(&self) -> Result<Self> {
        let socket = self
            .inner
            .socket
            .as_ref()
            .ok_or(ClientError::Disconnected)?;
        let mut client =
            Self::connect_with_counter(socket, self.inner.next_id.clone(), self.views.clone())
                .await?;
        client.project_views = self.project_views;
        Ok(client)
    }

    pub async fn request_result(&self, method: &str, params: Value) -> Result<Value> {
        let mut stream = self.request(method, params).await?;
        while let Some(frame) = stream.next().await {
            if let ServerFrame::Response(response) = frame {
                if response_view_ignored(&response)
                    && [
                        "session.load",
                        "session.resume",
                        "session.new",
                        "session.load_page",
                    ]
                    .contains(&agent_daemon_protocol::canonical_method(method))
                {
                    return Err(ClientError::StaleView);
                }
                if let Some(error) = response.error {
                    if error
                        .data
                        .as_ref()
                        .and_then(|data| data.get("kind"))
                        .and_then(Value::as_str)
                        == Some("transport_disconnected")
                    {
                        return Err(ClientError::Disconnected);
                    }
                    if error
                        .data
                        .as_ref()
                        .and_then(|data| data.get("kind"))
                        .and_then(Value::as_str)
                        == Some("resync_required")
                    {
                        let data = error.data.as_ref().ok_or(ClientError::MissingResult)?;
                        return Err(ClientError::ResyncRequired {
                            cursor: data.get("cursor").and_then(Value::as_u64).unwrap_or(0),
                            snapshot: data.get("snapshot").cloned().unwrap_or(Value::Null),
                        });
                    }
                    return Err(ClientError::Rpc(error));
                }
                let value = response.result.ok_or(ClientError::MissingResult)?;
                if value.get("_my_agent_view_decision").and_then(Value::as_str) == Some("ignored")
                    && ["compact_started", "session_changed"]
                        .contains(&value.get("kind").and_then(Value::as_str).unwrap_or(""))
                {
                    return Err(ClientError::StaleView);
                }

                if ["session.load", "session.resume", "session.new"]
                    .contains(&agent_daemon_protocol::canonical_method(method))
                {
                    agent_daemon_protocol::decode_session_readback(value.clone())?;
                }
                return Ok(value);
            }
        }
        Err(ClientError::Disconnected)
    }

    pub async fn read_session(
        &self,
        key: &agent_daemon_protocol::SessionKey,
        mode: agent_daemon_protocol::HistoryReadMode,
    ) -> Result<agent_daemon_protocol::SessionReadback> {
        let value = self
            .request_result(
                "sessions.read",
                serde_json::json!({"session_id":key,"history_mode":mode}),
            )
            .await?;
        let snapshot = agent_daemon_protocol::decode_session_readback(value)?;
        if snapshot.session_id != *key || snapshot.history_mode != mode {
            return Err(ClientError::Protocol(
                agent_daemon_protocol::ProtocolError::InvalidParams(
                    "session readback owner/mode 不匹配".into(),
                ),
            ));
        }
        Ok(snapshot)
    }

    pub async fn read_provider_request(
        &self,
        params: agent_daemon_protocol::params::ContextReadParams,
    ) -> Result<agent_daemon_protocol::ProviderRequestReadback> {
        let value = self
            .request_result("context.readback", serde_json::to_value(&params)?)
            .await?;
        let result = agent_daemon_protocol::decode_provider_request(value)?;
        if result.owner.session_key.0 != params.session_id
            || result.owner.session_lifetime_id != params.expected_lifetime
            || params
                .run_id
                .as_ref()
                .is_some_and(|run| run != &result.owner.run_id)
            || params
                .capture_id
                .as_ref()
                .is_some_and(|id| id != &result.capture_id)
        {
            return Err(ClientError::Protocol(
                agent_daemon_protocol::ProtocolError::InvalidParams("请求读回 owner 不匹配".into()),
            ));
        }
        Ok(result)
    }

    pub async fn read_run(&self, run_id: &RunId) -> Result<RunRecord> {
        let value = self
            .request_result("runs.read", serde_json::json!({"run_id":run_id}))
            .await?;
        let ignored =
            value.get("_my_agent_view_decision").and_then(Value::as_str) == Some("ignored");
        let record = agent_daemon_protocol::decode_run_readback(value)?;
        if record.run_id != *run_id {
            return Err(ClientError::Protocol(
                agent_daemon_protocol::ProtocolError::InvalidParams(
                    "readback run owner 不匹配".into(),
                ),
            ));
        }
        if ignored {
            return Err(ClientError::StaleView);
        }
        Ok(record)
    }

    pub async fn read_events(
        &self,
        run_id: &RunId,
        after: EventSeq,
        limit: usize,
    ) -> Result<EventPage> {
        let limit = limit.clamp(1, 1000);
        let value = self
            .request_result(
                "runs.events",
                serde_json::json!({"run_id":run_id, "after_seq":after, "limit":limit}),
            )
            .await?;
        let events: Vec<StoredEvent> = serde_json::from_value(
            value
                .get("events")
                .cloned()
                .ok_or(ClientError::MissingResult)?,
        )?;
        let mut cursor = after;
        for event in &events {
            if event.run_id != *run_id
                || cursor.0.checked_add(1) != Some(event.seq.0)
                || events.len() > limit
            {
                return Err(ClientError::Protocol(
                    agent_daemon_protocol::ProtocolError::InvalidParams(
                        "readback owner/cursor/page 违反合同".into(),
                    ),
                ));
            }
            cursor = event.seq;
        }
        Ok(EventPage {
            has_more: events.len() == limit,
            events,
            cursor,
        })
    }

    pub async fn request(&self, method: &str, params: Value) -> Result<RpcStream> {
        let id = RequestId::Number(self.inner.next_id.fetch_add(1, Ordering::Relaxed));
        self.request_with_id(id, method, params).await
    }

    pub async fn negotiated_capabilities(
        &self,
    ) -> Result<&agent_daemon_protocol::NegotiatedCapabilities> {
        self.inner
            .negotiated
            .get_or_try_init(|| async {
                let offer = agent_daemon_protocol::InitializeParams::default();
                let id = RequestId::Number(self.inner.next_id.fetch_add(1, Ordering::Relaxed));
                let mut stream = self
                    .request_raw_with_id(id, "connection.initialize", serde_json::to_value(&offer)?)
                    .await?;
                let frame = tokio::time::timeout(std::time::Duration::from_secs(10), stream.next())
                    .await
                    .map_err(|_| ClientError::Disconnected)?
                    .ok_or(ClientError::Disconnected)?;
                let ServerFrame::Response(response) = frame else {
                    return Err(ClientError::MissingResult);
                };
                if let Some(error) = response.error {
                    return Err(ClientError::Rpc(error));
                }
                let result: agent_daemon_protocol::NegotiatedCapabilities =
                    serde_json::from_value(response.result.ok_or(ClientError::MissingResult)?)?;
                result.validate_offer(&offer)?;
                Ok(result)
            })
            .await
    }

    pub async fn request_with_id(
        &self,
        id: RequestId,
        method: &str,
        params: Value,
    ) -> Result<RpcStream> {
        let mut candidate = JsonRpcRequest::new(id.clone(), method, params.clone());
        candidate.protocol_version = Some(1);
        agent_daemon_protocol::normalize_request(candidate).map_err(|error| {
            ClientError::Rpc(agent_daemon_protocol::RpcError {
                code: -32602,
                message: error.to_string(),
                data: None,
            })
        })?;
        let negotiated = self.negotiated_capabilities().await?;
        if !negotiated.permits(agent_daemon_protocol::canonical_method(method)) {
            return Err(ClientError::Protocol(
                agent_daemon_protocol::ProtocolError::InvalidParams("方法未协商".into()),
            ));
        }
        self.request_raw_with_id(id, method, params).await
    }

    async fn request_raw_with_id(
        &self,
        id: RequestId,
        method: &str,
        params: Value,
    ) -> Result<RpcStream> {
        let (frames, incoming) = mpsc::channel(256);
        let after_seq = EventSeq(params.get("after_seq").and_then(Value::as_u64).unwrap_or(0));
        let mut request = JsonRpcRequest::new(id.clone(), method, params);
        request.protocol_version = Some(1);
        let encoded = encode_frame(&request)?;
        let request = decode_request(&encoded)?;
        match &self.inner.transport {
            #[cfg(feature = "test-transport")]
            ClientTransport::InMemory(requests) => {
                let (legacy_frames, mut legacy_receiver) = mpsc::unbounded_channel();
                tokio::spawn(async move {
                    while let Some(frame) = legacy_receiver.recv().await {
                        if frames.send(frame).await.is_err() {
                            break;
                        }
                    }
                });
                requests
                    .send(InMemoryEnvelope {
                        request,
                        frames: legacy_frames,
                    })
                    .await
                    .map_err(|_| ClientError::Disconnected)?;
            }
            ClientTransport::Unix {
                requests,
                pending,
                alive,
            } => {
                let mut destinations = pending.lock().await;
                if !alive.load(Ordering::Acquire) {
                    return Err(ClientError::Disconnected);
                }
                if destinations.contains_key(&id) {
                    return Err(ClientError::DuplicateRequest);
                }
                destinations.insert(id.clone(), frames);
                drop(destinations);
                if requests.send(request).await.is_err() {
                    pending.lock().await.remove(&id);
                    return Err(ClientError::Disconnected);
                }
            }
        }
        let (output, receiver) = mpsc::channel(256);
        let projector = ViewProjector {
            id: id.clone(),
            receiver: incoming,
            method: agent_daemon_protocol::canonical_method(method).into(),
            client: self.clone(),
            after_seq,
            ready: VecDeque::new(),
            recovery: None,
        };
        let pump = tokio::spawn(project_frames(projector, output));
        Ok(RpcStream { id, receiver, pump })
    }

    /// 新消费者共享连接而拥有独立展示游标。
    pub fn fork_view(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            views: Arc::new(Mutex::new(HashMap::new())),
            project_views: self.project_views,
        }
    }

    /// Web 的归约通过 views.reduce 调用同一 core 函数；Gateway 只转发持久帧。
    pub fn protocol_adapter(&self) -> Self {
        let mut client = self.fork_view();
        client.project_views = false;
        client
    }

    /// 共享展示游标；不授予 owner、不更改持久状态，重连保留。
    pub async fn reduce_view(
        &self,
        key: &str,
        input: agent_daemon_protocol::ViewInput,
    ) -> agent_daemon_protocol::ViewReduction {
        let mut views = self.views.lock().await;
        let reduction = agent_daemon_protocol::reduce_view(views.get(key).cloned(), input);
        if let Some(state) = &reduction.state {
            views.insert(key.into(), state.clone());
        }
        reduction
    }
}

async fn fail_pending(
    pending: &Mutex<HashMap<RequestId, mpsc::Sender<ServerFrame>>>,
    id: &RequestId,
    code: i64,
    message: String,
) {
    if let Some(destination) = pending.lock().await.remove(id) {
        let _ = destination.try_send(ServerFrame::Response(JsonRpcResponse::failure(
            id.clone(),
            code,
            message,
        )));
    }
}

async fn fail_all_pending(
    pending: &Mutex<HashMap<RequestId, mpsc::Sender<ServerFrame>>>,
    message: String,
) {
    let destinations = std::mem::take(&mut *pending.lock().await);
    for (id, destination) in destinations {
        let _ = destination.try_send(ServerFrame::Response(JsonRpcResponse::failure_data(
            id,
            -32000,
            message.clone(),
            serde_json::json!({"kind":"transport_disconnected"}),
        )));
    }
}

pub struct RpcStream {
    id: RequestId,
    receiver: mpsc::Receiver<ServerFrame>,
    pump: tokio::task::JoinHandle<()>,
}
impl Drop for RpcStream {
    fn drop(&mut self) {
        self.pump.abort();
    }
}
impl RpcStream {
    pub fn request_id(&self) -> &RequestId {
        &self.id
    }
    /// 可取消安全：归约等待由独立 pump 完成，TUI poll/ctrl-C 不会消费后丢帧。
    pub async fn next(&mut self) -> Option<ServerFrame> {
        self.receiver.recv().await
    }
}
struct RecoveryCursor {
    run: RunId,
    cursor: EventSeq,
    through: EventSeq,
    base: EventSeq,
}
fn project_frames(
    mut projector: ViewProjector,
    output: mpsc::Sender<ServerFrame>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
    Box::pin(async move {
        while let Some(frame) = projector.next().await {
            let terminal = matches!(frame, ServerFrame::Response(_));
            if output.send(frame).await.is_err() || terminal {
                break;
            }
        }
    })
}
struct ViewProjector {
    id: RequestId,
    receiver: mpsc::Receiver<ServerFrame>,
    method: String,
    client: DaemonClient,
    after_seq: EventSeq,
    ready: VecDeque<ServerFrame>,
    recovery: Option<RecoveryCursor>,
}
impl ViewProjector {
    async fn next(&mut self) -> Option<ServerFrame> {
        use agent_daemon_protocol::ViewDecision;
        loop {
            if let Some(frame) = self.ready.pop_front() {
                return Some(frame);
            }
            if self.recovery.is_some() {
                if let Err(error) = self.replay_page().await {
                    self.recovery = None;
                    return Some(self.resync_error(error.to_string()));
                }
                continue;
            }
            let mut frame = self.receiver.recv().await?;
            if !self.client.project_views {
                return Some(frame);
            }
            let (key, input) = match frame_view_input(&self.method, &frame, self.after_seq) {
                Ok(Some(input)) => input,
                Ok(None) => return Some(frame),
                Err(error) => {
                    return Some(ServerFrame::Response(JsonRpcResponse::failure(
                        self.id.clone(),
                        -32602,
                        error.to_string(),
                    )));
                }
            };
            let previous = self
                .client
                .views
                .lock()
                .await
                .get(&key)
                .and_then(|state| match &frame {
                    ServerFrame::Event(event) => event
                        .run_id
                        .as_ref()
                        .and_then(|run| state.runs.get(&run.0))
                        .map(|run| run.event_seq),
                    _ => None,
                })
                .unwrap_or(self.after_seq);
            let reduced = self.client.reduce_view(&key, input).await;
            match &mut frame {
                ServerFrame::Event(_) if reduced.decision != ViewDecision::Accepted => {
                    if reduced.decision == ViewDecision::Resync {
                        let run = match &frame {
                            ServerFrame::Event(event) => event.run_id.clone(),
                            _ => None,
                        };
                        if let Err(error) = self.resync(&key, run, previous).await {
                            return Some(self.resync_error(error.to_string()));
                        }
                    }
                    continue;
                }
                ServerFrame::Response(response)
                    if reduced.decision != ViewDecision::Accepted && !reduced.duplicate =>
                {
                    if let Some(value) = &mut response.result {
                        value["_my_agent_view_decision"] = serde_json::json!("ignored");
                    }
                    if let Some(error) = &mut response.error {
                        let data = error.data.get_or_insert_with(|| serde_json::json!({}));
                        data["_my_agent_view_decision"] = serde_json::json!("ignored");
                    }
                }
                _ => {}
            }
            return Some(frame);
        }
    }
    fn resync_error(&self, message: String) -> ServerFrame {
        ServerFrame::Response(JsonRpcResponse::failure_data(
            self.id.clone(),
            -32001,
            message,
            serde_json::json!({"kind":"resync_required","cursor":self.after_seq.0,"snapshot":null}),
        ))
    }
    async fn resync(&mut self, key: &str, run: Option<RunId>, previous: EventSeq) -> Result<()> {
        let reader = self.client.protocol_adapter();
        let first = reader
            .request_result(
                "session.load_page",
                serde_json::json!({"session_id":key,"offset":0,"limit":60}),
            )
            .await?;
        let total = first
            .get("total_messages")
            .and_then(Value::as_u64)
            .ok_or(ClientError::MissingResult)?;
        let value = if total > 60 {
            reader
                .request_result(
                    "session.load_page",
                    serde_json::json!({"session_id":key,"offset":total-60,"limit":60}),
                )
                .await?
        } else {
            first
        };
        let (snapshot, page_key) = agent_daemon_protocol::decode_session_page(value.clone())?;
        let active = run
            .as_ref()
            .and_then(|id| snapshot.active_runs.iter().find(|run| run.run_id == *id));
        let recovery = active
            .filter(|run| run.last_seq > previous)
            .map(|run| RecoveryCursor {
                run: run.run_id.clone(),
                cursor: previous,
                through: run.last_seq,
                base: previous,
            });
        let terminal = run
            .as_ref()
            .and_then(|id| {
                snapshot
                    .last_durable_terminal
                    .as_ref()
                    .filter(|run| run.run_id == *id)
            })
            .cloned();
        let reduced = self
            .client
            .reduce_view(
                key,
                agent_daemon_protocol::ViewInput::Page {
                    snapshot: Box::new(snapshot),
                    page_key,
                },
            )
            .await;
        if reduced.decision != agent_daemon_protocol::ViewDecision::Accepted && !reduced.duplicate {
            return Err(ClientError::StaleView);
        }
        self.ready
            .push_back(ServerFrame::Event(agent_daemon_protocol::EventFrame::new(
                self.id.clone(),
                agent_daemon_protocol::EventKind::ViewResynced,
                serde_json::json!({"snapshot":value,"run":terminal}),
            )));
        self.recovery = recovery;
        Ok(())
    }
    async fn replay_page(&mut self) -> Result<()> {
        let Some(mut recovery) = self.recovery.take() else {
            return Ok(());
        };
        let reader = self.client.protocol_adapter();
        let page = reader
            .read_events(&recovery.run, recovery.cursor, 32)
            .await?;
        if page.events.is_empty() && recovery.cursor < recovery.through {
            return Err(ClientError::MissingResult);
        }
        for stored in page.events {
            if stored.seq > recovery.through {
                break;
            }
            recovery.cursor = stored.seq;
            if stored.event == "run_started" && stored.data["kind"] != "compact" {
                continue;
            }
            let event = if stored.event == "assistant_content" {
                agent_daemon_protocol::EventKind::TurnCompleted
            } else {
                match serde_json::from_value::<agent_daemon_protocol::EventKind>(Value::String(
                    stored.event,
                )) {
                    Ok(event) => event,
                    Err(_) => continue,
                }
            };
            let mut frame =
                agent_daemon_protocol::EventFrame::new(self.id.clone(), event, stored.data);
            frame.run_id = Some(stored.run_id);
            frame.seq = Some(stored.seq);
            frame.data["_my_agent_replay"] = serde_json::json!(true);
            if let Some((key, input)) = frame_view_input(
                "agent.subscribe",
                &ServerFrame::Event(frame.clone()),
                recovery.base,
            )? {
                let reduced = self.client.reduce_view(&key, input).await;
                if reduced.decision == agent_daemon_protocol::ViewDecision::Resync {
                    return Err(ClientError::ResyncRequired {
                        cursor: recovery.cursor.0,
                        snapshot: Value::Null,
                    });
                }
                if reduced.decision == agent_daemon_protocol::ViewDecision::Accepted {
                    self.ready.push_back(ServerFrame::Event(frame));
                }
            } else {
                return Err(ClientError::MissingResult);
            }
        }
        if recovery.cursor < recovery.through {
            self.recovery = Some(recovery);
        }
        Ok(())
    }
}

/// cursor 仅是读回位置，不授予 run ownership。
pub struct EventPage {
    pub events: Vec<StoredEvent>,
    pub cursor: EventSeq,
    pub has_more: bool,
}

fn frame_view_input(
    method: &str,
    frame: &ServerFrame,
    after_seq: EventSeq,
) -> Result<Option<(String, agent_daemon_protocol::ViewInput)>> {
    use agent_daemon_protocol::{ViewInput, ViewStamp};
    let value = match frame {
        ServerFrame::Event(event) => {
            let Some(value) = event.data.get("_my_agent_view") else {
                return Ok(None);
            };
            let stamp: ViewStamp = serde_json::from_value(value.clone())?;
            if stamp.owner.as_ref().map(|owner| &owner.run_id) != event.run_id.as_ref()
                || stamp.event_seq != event.seq
            {
                return Err(ClientError::Protocol(
                    agent_daemon_protocol::ProtocolError::InvalidParams(
                        "事件展示 owner/cursor 冲突".into(),
                    ),
                ));
            }
            return Ok(Some((
                stamp.session_key.0.clone(),
                if event.data.get("_my_agent_replay").and_then(Value::as_bool) == Some(true) {
                    ViewInput::Replay {
                        stamp: Some(Box::new(stamp)),
                        after_seq,
                    }
                } else {
                    ViewInput::Event {
                        stamp: Some(Box::new(stamp)),
                    }
                },
            )));
        }
        ServerFrame::Response(response) => match response.result.as_ref().or_else(|| {
            response
                .error
                .as_ref()
                .and_then(|error| error.data.as_ref())
        }) {
            Some(value) => value,
            None => return Ok(None),
        },
    };
    if value.get("kind").and_then(Value::as_str) == Some("session_changed") {
        let snapshot = agent_daemon_protocol::decode_session_readback(
            value
                .get("snapshot")
                .cloned()
                .ok_or(ClientError::MissingResult)?,
        )?;
        return Ok(Some((
            snapshot.session_id.0.clone(),
            ViewInput::Readback {
                snapshot: Box::new(snapshot),
            },
        )));
    }
    if value.get("kind").and_then(Value::as_str) == Some("compact_started") {
        let value = value
            .get("run")
            .cloned()
            .ok_or(ClientError::MissingResult)?;
        let run = agent_daemon_protocol::decode_run_readback(value.clone())?;
        let stamp = value
            .get("_my_agent_view")
            .cloned()
            .map(serde_json::from_value)
            .transpose()?;
        return Ok(Some((
            run.session_id.0.clone(),
            ViewInput::Run {
                stamp,
                run: Box::new(run),
            },
        )));
    }
    if ["session.load", "session.resume", "session.new"].contains(&method) {
        let snapshot = agent_daemon_protocol::decode_session_readback(value.clone())?;
        Ok(Some((
            snapshot.session_id.0.clone(),
            ViewInput::Readback {
                snapshot: Box::new(snapshot),
            },
        )))
    } else if method == "session.load_page" {
        let (snapshot, page_key) = agent_daemon_protocol::decode_session_page(value.clone())?;
        Ok(Some((
            snapshot.session_id.0.clone(),
            ViewInput::Page {
                snapshot: Box::new(snapshot),
                page_key,
            },
        )))
    } else if method == "run.read"
        || (value.get("kind").and_then(Value::as_str) == Some("compact")
            && value.get("compact").is_some())
    {
        let run = agent_daemon_protocol::decode_run_readback(value.clone())?;
        let stamp = value
            .get("_my_agent_view")
            .cloned()
            .map(serde_json::from_value)
            .transpose()?;
        Ok(Some((
            run.session_id.0.clone(),
            ViewInput::Run {
                stamp,
                run: Box::new(run),
            },
        )))
    } else if let Some(value) = value.get("_my_agent_view") {
        let stamp: ViewStamp = serde_json::from_value(value.clone())?;
        Ok(Some((
            stamp.session_key.0.clone(),
            ViewInput::Event {
                stamp: Some(Box::new(stamp)),
            },
        )))
    } else {
        Ok(None)
    }
}

/// 传输结束不必对应当前展示的新终态；各适配层仅处理归约器已接纳的内容。
pub fn response_view_ignored(response: &JsonRpcResponse) -> bool {
    response
        .result
        .as_ref()
        .or_else(|| {
            response
                .error
                .as_ref()
                .and_then(|error| error.data.as_ref())
        })
        .is_some_and(|value| {
            value.get("_my_agent_view_decision").and_then(Value::as_str) == Some("ignored")
        })
}
