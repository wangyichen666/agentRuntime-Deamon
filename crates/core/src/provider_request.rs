//! 可重建的发送前材料；身份与授权由 repository 验证。
use crate::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderRequestPurpose {
    #[default]
    Model,
    CompactSummary,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRequestInput {
    #[serde(default)]
    pub purpose: ProviderRequestPurpose,
    pub source: ContextSource,
    pub messages: Vec<Message>,
    pub tools: Vec<ToolSpec>,
    pub catalog: Vec<ToolSpec>,
    pub route: String,
    pub provider_identity: String,
    pub images: bool,
    pub tool_calls: bool,
    pub budget: u64,
    pub calibration: Option<(u64, u64)>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRequestCapture {
    pub schema_version: u16,
    pub capture_id: String,
    pub owner: ExactOwner,
    pub round: u32,
    pub candidate_index: u32,
    pub snapshot_revision: SnapshotRevision,
    pub input: ProviderRequestInput,
    pub policy_fingerprint: String,
    pub request_digest: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRequestReadback {
    #[serde(default)]
    pub purpose: ProviderRequestPurpose,
    pub schema_version: u16,
    pub capture_id: String,
    pub owner: ExactOwner,
    pub round: u32,
    pub candidate_index: u32,
    pub snapshot_revision: SnapshotRevision,
    pub envelope: ContextEnvelope,
    pub policy_fingerprint: String,
    pub request_digest: String,
    pub message_digest: String,
    pub provider_tools_digest: String,
    pub images: bool,
    pub tool_calls: bool,
    pub replayability: String,
    pub omissions: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub messages: Option<Vec<Message>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<ToolSpec>>,
}

/// 一次纯函数的返回值，不拥有 session 或长期历史。
#[derive(Clone, Debug)]
pub struct AssembledProviderRequest {
    pub messages: Vec<Message>,
    pub tools: Vec<ToolSpec>,
    pub envelope: ContextEnvelope,
    pub request_digest: String,
    pub message_digest: String,
    pub provider_tools_digest: String,
    pub omissions: Vec<String>,
}
