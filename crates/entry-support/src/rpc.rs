use agent_daemon_client::DaemonClient;
use anyhow::Result;
use serde_json::Value;

pub async fn request_result(client: &DaemonClient, method: &str, params: Value) -> Result<Value> {
    client
        .request_result(method, params)
        .await
        .map_err(Into::into)
}
