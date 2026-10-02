use anyhow::{Context, Result};
use serde_json::json;

use agent_daemon_client::{DaemonClient, RpcStream};
use agent_daemon_protocol::RequestId;

pub use agent_daemon_protocol::RecoverySnapshot;

pub async fn load_snapshot(client: &DaemonClient) -> Result<RecoverySnapshot> {
    let value = crate::entry::cli::request_result(client, "session.load", json!({})).await?;
    parse_snapshot(value, "session.load")
}

pub async fn start_new_session(client: &DaemonClient) -> Result<RecoverySnapshot> {
    let value = crate::entry::cli::request_result(client, "session.new", json!({})).await?;
    parse_snapshot(value, "session.new")
}

pub async fn resume_session(client: &DaemonClient, session_id: &str) -> Result<RecoverySnapshot> {
    let value = crate::entry::cli::request_result(
        client,
        "session.resume",
        json!({"session_id": session_id}),
    )
    .await?;
    parse_snapshot(value, "session.resume")
}

pub(crate) fn parse_snapshot(value: serde_json::Value, method: &str) -> Result<RecoverySnapshot> {
    serde_json::from_value(value).with_context(|| format!("daemon {method} 会话快照格式无效"))
}

pub async fn respond_to_approval(
    client: &DaemonClient,
    approval_id: &str,
    approved: bool,
) -> Result<()> {
    crate::entry::cli::request_result(
        client,
        "approval.respond",
        json!({"approval_id": approval_id, "approved": approved}),
    )
    .await
    .map(|_| ())
}

pub async fn subscribe(client: &DaemonClient, request_id: &RequestId) -> Result<RpcStream> {
    client
        .request("agent.subscribe", json!({"request_id": request_id}))
        .await
        .map_err(Into::into)
}

pub async fn subscribe_for_session(
    client: &DaemonClient,
    request_id: &RequestId,
    session_id: &str,
) -> Result<RpcStream> {
    client
        .request(
            "agent.subscribe",
            json!({"request_id": request_id, "session_id": session_id}),
        )
        .await
        .map_err(Into::into)
}
