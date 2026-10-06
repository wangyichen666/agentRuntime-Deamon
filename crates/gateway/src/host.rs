use agent_daemon_client::DaemonClient;
use anyhow::Result;
use serde_json::Value;
use std::path::{Path, PathBuf};

/// binary 组合端口；会话事实仍只能通过 daemon-client 获取。
#[async_trait::async_trait]
pub trait GatewayHost: Send + Sync {
    async fn connect_workspace(&self, workspace: &Path) -> Result<DaemonClient>;
    fn active_model(&self, fallback: &str) -> String;
    fn config_path(&self) -> PathBuf;
    fn list_models(&self) -> Result<Value>;
    fn save_configuration(&self, value: Value, activate: bool) -> Result<Value>;
    fn activate_configuration(&self, profile_id: &str) -> Result<Value>;
}
