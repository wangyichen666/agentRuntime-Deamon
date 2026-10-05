use crate::{
    EventSeq, InteractionId, RequestId, RunId, RunStatus, SessionKey as SessionId, TurnId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunRecord {
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

#[derive(Clone, Debug, Serialize, Deserialize)]
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
