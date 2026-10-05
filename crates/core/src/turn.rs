use crate::{ExactOwner, Message, RouteSnapshot, RunStatus, ToolSpec};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunSnapshot {
    pub route: Option<RouteSnapshot>,
    pub tools: Vec<ToolSpec>,
    pub cwd: String,
    pub permission_mode: String,
    pub sandbox_requested: String,
    pub sandbox_effective: String,
    #[serde(default)]
    pub sandbox_notice: Option<String>,
    #[serde(default)]
    pub docker_image: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_context: Option<crate::DelegationContext>,
    pub context_read_only: bool,
    #[serde(default)]
    pub context_token_budget: usize,
    #[serde(default)]
    pub context_policy_fingerprint: Option<String>,
    #[serde(default)]
    pub tool_catalog_digest: String,
    #[serde(default)]
    pub memory_entry_budget: usize,
    #[serde(default)]
    pub memory_token_budget: usize,
    pub max_tool_calls: Option<u64>,
    pub config_generation: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TurnCommit {
    pub owner: ExactOwner,
    pub status: RunStatus,
    pub content: Option<String>,
    pub error: Option<(i64, String)>,
}
#[derive(Default)]
pub struct TurnState {
    pub round: usize,
    pub consecutive_tool_failures: usize,
    pub estimated_total_tokens: u64,
}
#[derive(Default)]
pub struct RoundState {
    pub tool_calls: Vec<crate::ToolCall>,
    pub closed_exchange: Vec<Message>,
}

#[derive(Clone, Debug)]
pub struct RunAdmission {
    pub session_key: crate::SessionKey,
    pub expected_lifetime: Option<crate::SessionLifetimeId>,
    pub request_id: crate::RequestId,
    pub input: String,
    pub mode: crate::AdmissionMode,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlanSnapshot {
    pub revision: u64,
    pub value: serde_json::Value,
}
