use crate::{
    EventSeq, InteractionId, RequestId, RunId, RunStatus, SessionKey as SessionId, TurnId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunRecord {
    #[serde(default)]
    pub kind: RunKind,
    pub run_id: RunId,
    pub turn_id: TurnId,
    pub session_id: SessionId,
    pub request_id: RequestId,
    pub status: RunStatus,
    pub last_seq: EventSeq,
    pub content: Option<String>,
    pub error_code: Option<i64>,
    pub error_message: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunKind {
    #[default]
    Chat,
    Compact,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredEvent {
    pub run_id: RunId,
    pub seq: EventSeq,
    pub event: String,
    pub data: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolReceipt {
    pub run_id: RunId,
    pub round: i64,
    pub call_id: String,
    pub name: String,
    pub status: String,
    pub effect: String,
    pub argument_digest: String,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    pub outcome: Option<String>,
    pub artifact_ref: Option<String>,
    pub safe_to_replay: bool,
    pub receipt: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueuedMessage {
    pub id: i64,
    pub position: i64,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub message: String,
    pub status: String,
}

/// 原持久队列的稳定恢复游标，只引用既有行，不保存恢复状态副本。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueRecoveryCursor {
    pub created_at_ms: i64,
    pub row_id: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueRecoveryPage {
    pub runs: Vec<RunId>,
    pub cursor: Option<QueueRecoveryCursor>,
    pub has_more: bool,
}

/// 原投影的诊断摘要，只返回已校验canonical材料的计数。
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionContextSummary {
    pub session_key: SessionId,
    pub lifetime: crate::SessionLifetimeId,
    pub transcript_revision: crate::TranscriptSeq,
    pub projection_generation: crate::ProjectionGeneration,
    pub projected_messages: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InteractionRecord {
    pub interaction_id: InteractionId,
    pub session_id: SessionId,
    pub owner_run_id: RunId,
    pub kind: String,
    pub status: String,
    pub revision: i64,
    pub prompt: String,
    pub payload: Value,
    pub response: Option<Value>,
}

impl InteractionRecord {
    /// 正文只在 SQL owner 及字段预算已经判定后校验。
    pub fn validate_saved(&self) -> Result<(), crate::CoordinationError> {
        if self.revision < 0
            || !matches!(
                self.status.as_str(),
                "pending" | "answered" | "rejected" | "orphaned"
            )
            || !self.payload.is_object()
            || self
                .payload
                .get("id")
                .is_some_and(|value| value.as_str() != Some(&self.interaction_id.0))
            || self
                .payload
                .get("prompt")
                .is_some_and(|value| value.as_str() != Some(&self.prompt))
            || (self.status == "pending" && self.response.is_some())
            || (matches!(self.status.as_str(), "answered" | "rejected")
                && self
                    .response
                    .as_ref()
                    .and_then(|value| value["approved"].as_bool())
                    != Some(self.status == "answered"))
        {
            return Err(crate::CoordinationError::Corrupt);
        }
        Ok(())
    }
}

pub fn is_run_live_event(event: &str) -> bool {
    matches!(
        event,
        "turn_started"
            | "thinking_delta"
            | "thinking_finished"
            | "text_delta"
            | "tool_started"
            | "tool_finished"
            | "approval_required"
    )
}

#[derive(Clone, Debug)]
pub enum Admission {
    New(RunRecord),
    Existing(RunRecord),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionMode {
    Queue,
    RejectIfBusy,
}

/// canonical 事件的稳定游标页；cursor 仅推进到实际返回的最后一条事件。
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunEventPage {
    pub events: Vec<StoredEvent>,
    pub cursor: EventSeq,
    pub has_more: bool,
}

/// 同事务读取原 run、冻结 snapshot、compact linkage 和 Hook continuation。
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunFactReadback {
    pub run: RunRecord,
    pub snapshot: Option<crate::RunSnapshot>,
    pub view_stamp: Option<crate::ViewStamp>,
    pub compact: Option<crate::CompactRunReadback>,
    pub continuation_parent_run_id: Option<RunId>,
    pub continuation_run_id: Option<RunId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunProviderReadback {
    pub route: Option<crate::RouteSnapshot>,
    pub attempts: Vec<crate::ProviderAttempt>,
    pub usage: crate::ProviderUsage,
    pub final_candidate_index: Option<u32>,
}
