//! ACP 物理传输预算；溢出只 detach，不批准/取消/结算任何业务。
use agent_client_protocol::{Agent, ConnectTo, Error, Lines};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::{BufRead, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

const MAX_QUEUED_FRAMES: usize = 256;
const MAX_QUEUED_BYTES: usize = 8 * 1024 * 1024;
#[derive(Default)]
struct Accounting {
    requests: BTreeMap<String, usize>,
    outputs: BTreeMap<String, (usize, usize)>,
    frames: usize,
    bytes: usize,
}
pub struct WireBudget {
    accounting: Mutex<Accounting>,
    detached: AtomicBool,
    changed: Notify,
    tasks: Arc<Semaphore>,
}
impl Default for WireBudget {
    fn default() -> Self {
        Self {
            accounting: Mutex::default(),
            detached: AtomicBool::default(),
            changed: Notify::default(),
            tasks: Arc::new(Semaphore::new(128)),
        }
    }
}
impl WireBudget {
    pub fn task(&self) -> Result<OwnedSemaphorePermit, Error> {
        self.tasks.clone().try_acquire_owned().map_err(|_| {
            self.detach();
            Error::internal_error().data("ACP 连接后台任务超过128，需 durable 恢复")
        })
    }
    pub(super) fn detach(&self) {
        self.detached.store(true, Ordering::Release);
        self.changed.notify_one();
    }
    pub async fn detached(&self) {
        if !self.detached.load(Ordering::Acquire) {
            self.changed.notified().await;
        }
    }
    fn reserve(&self, value: &Value, incoming: bool) -> Result<(), Error> {
        let bytes = serde_json::to_vec(value)
            .map_err(Error::into_internal_error)?
            .len();
        if bytes > agent_daemon_protocol::MAX_FRAME_BYTES {
            self.detach();
            return Err(Error::invalid_params().data("ACP 帧超过预算"));
        }
        let mut state = self
            .accounting
            .lock()
            .map_err(|_| Error::internal_error().data("ACP accounting lock 损坏"))?;
        if self.detached.load(Ordering::Acquire)
            || state.frames >= MAX_QUEUED_FRAMES
            || state.bytes.saturating_add(bytes) > MAX_QUEUED_BYTES
        {
            drop(state);
            self.detach();
            return Err(Error::internal_error().data("ACP 消费者溢出，需 durable 恢复"));
        }
        if incoming {
            let id = signature(&value["id"])?;
            if state.requests.contains_key(&id) {
                drop(state);
                self.detach();
                return Err(Error::invalid_params().data("ACP pending 请求 ID 重复"));
            }
            state.requests.insert(id, bytes);
        } else {
            let key = signature(&outgoing_payload(value))?;
            let row = state.outputs.entry(key).or_insert((0, 0));
            row.0 += 1;
            row.1 += bytes;
        }
        state.frames += 1;
        state.bytes += bytes;
        Ok(())
    }
    pub fn output(&self, value: &Value) -> Result<(), Error> {
        self.reserve(value, false)
    }
    fn incoming(&self, value: &Value) -> Result<(), Error> {
        if value.is_array() {
            self.detach();
            return Err(Error::invalid_params().data("ACP stdio 不接受 batch"));
        }
        if value.get("id").is_some() && value.get("method").is_some() {
            self.reserve(value, true)
        } else {
            Ok(())
        }
    }
    fn written(&self, value: &Value) -> Result<(), Error> {
        let mut state = self
            .accounting
            .lock()
            .map_err(|_| Error::internal_error().data("ACP accounting lock 损坏"))?;
        if value.get("method").is_none()
            && let Some(id) = value.get("id")
            && let Some(bytes) = state.requests.remove(&signature(id)?)
        {
            state.frames = state.frames.saturating_sub(1);
            state.bytes = state.bytes.saturating_sub(bytes);
        }
        let key = signature(&outgoing_payload(value))?;
        if let Some((count, total)) = state.outputs.get(&key).copied() {
            let bytes = total / count;
            state.frames = state.frames.saturating_sub(1);
            state.bytes = state.bytes.saturating_sub(bytes);
            if count == 1 {
                state.outputs.remove(&key);
            } else {
                state.outputs.insert(key, (count - 1, total - bytes));
            }
        }
        Ok(())
    }
}
fn outgoing_payload(value: &Value) -> Value {
    if value.get("method").is_some() {
        json!({"method":value["method"],"params":value["params"]})
    } else if value.get("error").is_some() {
        json!({"error":value["error"]})
    } else {
        json!({"result":value["result"]})
    }
}
fn signature(value: &Value) -> Result<String, Error> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(value).map_err(Error::into_internal_error)?)
    ))
}

pub fn stdio(budget: Arc<WireBudget>) -> impl ConnectTo<Agent> {
    // stdio 的阻塞线程不加入 Tokio runtime；连接 detach 后不能卡住 runtime shutdown。
    // 桥接队列固定容量，完成回执只表示物理写入，不结算业务。
    let (incoming_tx, incoming_rx) = tokio::sync::mpsc::channel(8);
    let input_budget = budget.clone();
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        let mut reader = stdin.lock();
        loop {
            let mut bytes = Vec::new();
            let result = (&mut reader)
                .take(agent_daemon_protocol::MAX_FRAME_BYTES as u64 + 1)
                .read_until(b'\n', &mut bytes);
            if matches!(result, Ok(0)) {
                break;
            }
            let line = match result {
                Ok(_) if bytes.len() > agent_daemon_protocol::MAX_FRAME_BYTES => {
                    input_budget.detach();
                    Err(std::io::Error::other("ACP 帧超过预算"))
                }
                Ok(_) => String::from_utf8(bytes).map_err(std::io::Error::other),
                Err(error) => Err(error),
            }
            .and_then(|line| {
                let value: Value = serde_json::from_str(&line).map_err(std::io::Error::other)?;
                input_budget
                    .incoming(&value)
                    .map_err(std::io::Error::other)?;
                Ok(line)
            });
            let failed = line.is_err();
            if incoming_tx.blocking_send(line).is_err() || failed {
                break;
            }
        }
    });
    type WriteFrame = (String, tokio::sync::oneshot::Sender<std::io::Result<()>>);
    let (outgoing_tx, outgoing_rx) = std::sync::mpsc::sync_channel::<WriteFrame>(1);
    std::thread::spawn(move || {
        let stdout = std::io::stdout();
        let mut writer = stdout.lock();
        for (line, done) in outgoing_rx {
            let result = (|| {
                let value: Value = serde_json::from_str(&line).map_err(std::io::Error::other)?;
                budget.written(&value).map_err(std::io::Error::other)?;
                writer.write_all(line.as_bytes())?;
                writer.write_all(b"\n")?;
                writer.flush()
            })();
            let failed = result.is_err();
            let _ = done.send(result);
            if failed {
                break;
            }
        }
    });
    let incoming = futures_util::stream::unfold(incoming_rx, |mut receiver| async move {
        receiver.recv().await.map(|line| (line, receiver))
    });
    let outgoing = futures_util::sink::unfold(outgoing_tx, |sender, line: String| async move {
        let (done, receipt) = tokio::sync::oneshot::channel();
        sender
            .try_send((line, done))
            .map_err(std::io::Error::other)?;
        receipt.await.map_err(std::io::Error::other)??;
        Ok::<_, std::io::Error>(sender)
    });
    Lines::new(Box::pin(outgoing), Box::pin(incoming))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn slow_consumer_budget_is_bounded_and_draining_only_changes_transport_accounting() {
        let budget = WireBudget::default();
        let event = json!({"method":"session/update","params":{"sessionId":"s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"x"},"messageId":"native"}}});
        for _ in 0..MAX_QUEUED_FRAMES {
            budget.output(&event).unwrap();
        }
        assert!(budget.output(&event).is_err());
        budget.detached().await;
        let fresh = WireBudget::default();
        fresh
            .incoming(&json!({"id":1,"method":"session/list","params":{}}))
            .unwrap();
        let reply = json!({"result":{}});
        fresh.output(&reply).unwrap();
        fresh
            .written(&json!({"id":1,"jsonrpc":"2.0","result":{}}))
            .unwrap();
        let state = fresh.accounting.lock().unwrap();
        assert_eq!((state.frames, state.bytes), (0, 0));
        assert!(state.requests.is_empty());
        assert!(state.outputs.is_empty());
    }
}
