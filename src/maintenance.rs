//! 显式离线兼容读取，不提供在线运行 mutation 能力。
use std::path::Path;

use agent_core::SessionInfo;
use anyhow::{Context, Result};

pub async fn legacy_session_listing(workspace: &Path) -> Result<Vec<SessionInfo>> {
    crate::session::SessionStore::from_env(workspace)
        .list_sessions()
        .await
        .context("读取离线旧会话清单失败")
}
