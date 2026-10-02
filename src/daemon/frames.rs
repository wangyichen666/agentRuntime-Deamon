//! 慢消费者只能断开并从 durable cursor 重订阅，不能无限累积内存帧。
use super::protocol::ServerFrame;
use tokio::sync::{mpsc, watch};
#[derive(Debug, thiserror::Error)]
pub(crate) enum FrameSendError {
    #[error("连接帧预算已满")]
    Full,
    #[error("连接已关闭")]
    Closed,
}
#[derive(Clone)]
pub(crate) enum FrameSender {
    Bounded {
        sender: mpsc::Sender<ServerFrame>,
        overflow: watch::Sender<bool>,
    },
    #[cfg(test)]
    Test(mpsc::UnboundedSender<ServerFrame>),
}
impl FrameSender {
    pub(crate) fn send(&self, frame: ServerFrame) -> Result<(), FrameSendError> {
        match self {
            Self::Bounded { sender, overflow } => {
                if *overflow.borrow() {
                    return Err(FrameSendError::Closed);
                }
                let result = sender.try_send(frame);
                if matches!(result, Err(mpsc::error::TrySendError::Full(_))) {
                    overflow.send_replace(true);
                }
                result.map_err(|e| match e {
                    mpsc::error::TrySendError::Full(_) => FrameSendError::Full,
                    mpsc::error::TrySendError::Closed(_) => FrameSendError::Closed,
                })
            }
            #[cfg(test)]
            Self::Test(sender) => sender.send(frame).map_err(|_| FrameSendError::Closed),
        }
    }
    pub(crate) fn overflow_receiver(&self) -> watch::Receiver<bool> {
        match self {
            Self::Bounded { overflow, .. } => overflow.subscribe(),
            #[cfg(test)]
            Self::Test(_) => {
                let (_, rx) = watch::channel(false);
                rx
            }
        }
    }
}
#[cfg(test)]
impl From<mpsc::UnboundedSender<ServerFrame>> for FrameSender {
    fn from(sender: mpsc::UnboundedSender<ServerFrame>) -> Self {
        Self::Test(sender)
    }
}
pub(crate) fn frame_channel() -> (FrameSender, mpsc::Receiver<ServerFrame>) {
    let (sender, receiver) = mpsc::channel(256);
    let (overflow, _) = watch::channel(false);
    (FrameSender::Bounded { sender, overflow }, receiver)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn slow_consumer_disconnects_instead_of_buffering_without_limit() {
        let (sender, _receiver) = frame_channel();
        let status = sender.overflow_receiver();
        for id in 0..256 {
            sender
                .send(ServerFrame::Response(
                    super::super::protocol::JsonRpcResponse::success(
                        super::super::protocol::RequestId::Number(id),
                        serde_json::json!({}),
                    ),
                ))
                .unwrap();
        }
        assert!(
            sender
                .send(ServerFrame::Response(
                    super::super::protocol::JsonRpcResponse::success(
                        super::super::protocol::RequestId::Number(256),
                        serde_json::json!({})
                    )
                ))
                .is_err()
        );
        assert!(*status.borrow());
    }
}
