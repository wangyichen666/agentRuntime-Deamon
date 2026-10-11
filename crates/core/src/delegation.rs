//! 原持久委派的领域值，存储后端共享同一模型。
use crate::{RouteSnapshot, RunId, RunStatus, SessionKey as SessionId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DelegationRequest {
    pub parent_session_id: SessionId,
    pub parent_run_id: RunId,
    pub child_session_id: SessionId,
    pub spawn_key: String,
    pub task: String,
    pub context_source_ids: Vec<String>,
    pub tools: Vec<String>,
    pub permission_mode: String,
    pub cwd: String,
    pub max_rounds: i64,
    pub max_tokens: i64,
    pub max_tool_calls: i64,
    pub deadline_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DelegationRecord {
    pub root_session_id: SessionId,
    pub root_run_id: RunId,
    pub parent_session_id: SessionId,
    pub parent_run_id: RunId,
    pub child_session_id: SessionId,
    pub child_run_id: RunId,
    pub spawn_key: String,
    pub depth: i64,
    pub status: RunStatus,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    pub tools: Vec<String>,
    pub permission_mode: String,
    pub route: Option<RouteSnapshot>,
    pub cwd: String,
    pub max_rounds: i64,
    pub max_tokens: i64,
    pub max_tool_calls: i64,
    pub deadline_ms: i64,
    pub terminal: Option<Value>,
    pub result_state: String,
    pub reservation_owner: Option<String>,
    pub revision: i64,
    pub reservation_released_at_ms: Option<i64>,
    pub reservation_expires_at_ms: Option<i64>,
    pub content: Option<String>,
    pub error_code: Option<i64>,
    pub error_message: Option<String>,
}
