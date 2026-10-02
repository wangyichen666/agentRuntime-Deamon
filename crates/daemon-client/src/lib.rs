//! 统一 daemon 客户端；传输失败不改变业务终态，不重发 mutation。
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::print_stdout)
)]
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use std::path::PathBuf;
use thiserror::Error;
type Result<T> = std::result::Result<T, ClientError>;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::{Mutex, mpsc};

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
    #[error("daemon RPC {0:?}")]
    Rpc(agent_daemon_protocol::RpcError),
}

#[derive(Clone)]
pub struct DaemonClient {
    inner: Arc<DaemonClientInner>,
}

struct DaemonClientInner {
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
        pending: Arc<Mutex<HashMap<RequestId, mpsc::UnboundedSender<ServerFrame>>>>,
    },
}

impl DaemonClient {
    #[cfg(feature = "test-transport")]
    pub fn in_memory(requests: mpsc::Sender<InMemoryEnvelope>) -> Self {
        Self {
            inner: Arc::new(DaemonClientInner {
                transport: ClientTransport::InMemory(requests),
                next_id: Arc::new(AtomicU64::new(1)),
                socket: None,
            }),
        }
    }

    pub async fn connect_unix(socket: &Path) -> Result<Self> {
        Self::connect_with_counter(socket, Arc::new(AtomicU64::new(1))).await
    }

    async fn connect_with_counter(socket: &Path, next_id: Arc<AtomicU64>) -> Result<Self> {
        let stream = UnixStream::connect(socket)
            .await
            .map_err(|source| ClientError::Connect {
                socket: socket.to_path_buf(),
                source,
            })?;
        let (reader, mut writer) = stream.into_split();
        let (requests, mut request_receiver) = mpsc::channel::<JsonRpcRequest>(64);
        let pending = Arc::new(Mutex::new(HashMap::<
            RequestId,
            mpsc::UnboundedSender<ServerFrame>,
        >::new()));
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
                    let _ = destination.send(frame);
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
                transport: ClientTransport::Unix {
                    requests,
                    pending,
                    alive,
                },
                next_id,
                socket: Some(socket.to_path_buf()),
            }),
        })
    }

    /// 显式重连只建立传输；mutation 必须先读回，不自动重复。
    pub async fn reconnect(&self) -> Result<Self> {
        let socket = self
            .inner
            .socket
            .as_ref()
            .ok_or(ClientError::Disconnected)?;
        Self::connect_with_counter(socket, self.inner.next_id.clone()).await
    }

    pub async fn request_result(&self, method: &str, params: Value) -> Result<Value> {
        let mut stream = self.request(method, params).await?;
        while let Some(frame) = stream.next().await {
            if let ServerFrame::Response(response) = frame {
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
                return response.result.ok_or(ClientError::MissingResult);
            }
        }
        Err(ClientError::Disconnected)
    }

    pub async fn read_run(&self, run_id: &RunId) -> Result<RunRecord> {
        let value = self
            .request_result("runs.read", serde_json::json!({"run_id":run_id}))
            .await?;
        let record: RunRecord = serde_json::from_value(value)?;
        if record.run_id != *run_id {
            return Err(ClientError::Protocol(
                agent_daemon_protocol::ProtocolError::InvalidParams(
                    "readback run owner 不匹配".into(),
                ),
            ));
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

    pub async fn request_with_id(
        &self,
        id: RequestId,
        method: &str,
        params: Value,
    ) -> Result<RpcStream> {
        let (frames, receiver) = mpsc::unbounded_channel();
        let mut request = JsonRpcRequest::new(id.clone(), method, params);
        request.protocol_version = Some(1);
        let encoded = encode_frame(&request)?;
        let request = decode_request(&encoded)?;
        match &self.inner.transport {
            #[cfg(feature = "test-transport")]
            ClientTransport::InMemory(requests) => {
                requests
                    .send(InMemoryEnvelope { request, frames })
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
        Ok(RpcStream { id, receiver })
    }
}

async fn fail_pending(
    pending: &Mutex<HashMap<RequestId, mpsc::UnboundedSender<ServerFrame>>>,
    id: &RequestId,
    code: i64,
    message: String,
) {
    if let Some(destination) = pending.lock().await.remove(id) {
        let _ = destination.send(ServerFrame::Response(JsonRpcResponse::failure(
            id.clone(),
            code,
            message,
        )));
    }
}

async fn fail_all_pending(
    pending: &Mutex<HashMap<RequestId, mpsc::UnboundedSender<ServerFrame>>>,
    message: String,
) {
    let destinations = std::mem::take(&mut *pending.lock().await);
    for (id, destination) in destinations {
        let _ = destination.send(ServerFrame::Response(JsonRpcResponse::failure_data(
            id,
            -32000,
            message.clone(),
            serde_json::json!({"kind":"transport_disconnected"}),
        )));
    }
}

pub struct RpcStream {
    id: RequestId,
    receiver: mpsc::UnboundedReceiver<ServerFrame>,
}

impl RpcStream {
    pub fn request_id(&self) -> &RequestId {
        &self.id
    }

    pub async fn next(&mut self) -> Option<ServerFrame> {
        self.receiver.recv().await
    }
}

/// cursor 仅是读回位置，不授予 run ownership。
pub struct EventPage {
    pub events: Vec<StoredEvent>,
    pub cursor: EventSeq,
    pub has_more: bool,
}
