//! 慢消费者只能断开并从 durable cursor 重订阅，不能无限累积内存帧。
use agent_daemon_protocol::ServerFrame;
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
    #[cfg(any(test, feature = "test-support"))]
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
            #[cfg(any(test, feature = "test-support"))]
            Self::Test(sender) => sender.send(frame).map_err(|_| FrameSendError::Closed),
        }
    }
    pub(crate) fn overflow_receiver(&self) -> watch::Receiver<bool> {
        match self {
            Self::Bounded { overflow, .. } => overflow.subscribe(),
            #[cfg(any(test, feature = "test-support"))]
            Self::Test(_) => {
                let (_, rx) = watch::channel(false);
                rx
            }
        }
    }
}
#[cfg(any(test, feature = "test-support"))]
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

/// watch 的正常关闭不等于 overflow；最后的持久 response 必须继续排空。
pub(crate) async fn next_frame(
    receiver: &mut mpsc::Receiver<ServerFrame>,
    overflow: &mut watch::Receiver<bool>,
) -> Result<Option<ServerFrame>, FrameSendError> {
    loop {
        if *overflow.borrow_and_update() {
            return Err(FrameSendError::Full);
        }
        tokio::select! {
            changed=overflow.changed()=> {
                if *overflow.borrow() {return Err(FrameSendError::Full);}
                if changed.is_err() {return Ok(receiver.recv().await);}
            },
            frame=receiver.recv()=>return Ok(frame),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn producer_detach_drains_committed_response_but_real_overflow_still_disconnects() {
        let (sender, mut receiver) = frame_channel();
        let mut status = sender.overflow_receiver();
        sender
            .send(ServerFrame::Response(
                agent_daemon_protocol::JsonRpcResponse::success(
                    agent_daemon_protocol::RequestId::Number(1),
                    serde_json::json!({"run_id":"native","compact":{"outcome":"cancelled"}}),
                ),
            ))
            .unwrap();
        drop(sender);
        assert!(status.changed().await.is_err());
        assert!(
            matches!(next_frame(&mut receiver,&mut status).await.unwrap(),Some(ServerFrame::Response(response)) if response.result.as_ref().unwrap()["run_id"]=="native")
        );
        assert!(
            next_frame(&mut receiver, &mut status)
                .await
                .unwrap()
                .is_none()
        );
        let (sender, mut receiver) = frame_channel();
        let mut status = sender.overflow_receiver();
        for id in 0..257 {
            let _ = sender.send(ServerFrame::Response(
                agent_daemon_protocol::JsonRpcResponse::success(
                    agent_daemon_protocol::RequestId::Number(id),
                    serde_json::json!({}),
                ),
            ));
        }
        drop(sender);
        assert!(matches!(
            next_frame(&mut receiver, &mut status).await,
            Err(FrameSendError::Full)
        ));
    }
    #[test]
    fn slow_consumer_disconnects_instead_of_buffering_without_limit() {
        let (sender, _receiver) = frame_channel();
        let status = sender.overflow_receiver();
        for id in 0..256 {
            sender
                .send(ServerFrame::Response(
                    agent_daemon_protocol::JsonRpcResponse::success(
                        agent_daemon_protocol::RequestId::Number(id),
                        serde_json::json!({}),
                    ),
                ))
                .unwrap();
        }
        assert!(
            sender
                .send(ServerFrame::Response(
                    agent_daemon_protocol::JsonRpcResponse::success(
                        agent_daemon_protocol::RequestId::Number(256),
                        serde_json::json!({})
                    )
                ))
                .is_err()
        );
        assert!(*status.borrow());
    }
}
