use agent_core::{EventSeq, InteractionId, RequestId, RunId, RunStatus, SessionKey as SessionId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextReadParams {
    pub session_id: String,
    pub expected_lifetime: agent_core::SessionLifetimeId,
    #[serde(default)]
    pub run_id: Option<agent_core::RunId>,
    #[serde(default)]
    pub capture_id: Option<String>,
    #[serde(default)]
    pub local_diagnostics: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ViewReduceParams {
    pub state: Option<agent_core::ViewState>,
    pub input: ViewInputParams,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ViewInputParams {
    Readback {
        snapshot: serde_json::Value,
    },
    Run {
        stamp: Option<Box<agent_core::ViewStamp>>,
        run: serde_json::Value,
    },
    Page {
        snapshot: serde_json::Value,
    },
    Replay {
        stamp: Option<Box<agent_core::ViewStamp>>,
        after_seq: EventSeq,
    },
    Event {
        stamp: Option<Box<agent_core::ViewStamp>>,
    },
}
impl ViewReduceParams {
    pub fn into_domain(
        self,
    ) -> Result<(Option<agent_core::ViewState>, agent_core::ViewInput), crate::ProtocolError> {
        let input = match self.input {
            ViewInputParams::Run { stamp, run } => agent_core::ViewInput::Run {
                stamp,
                run: Box::new(crate::decode_run_readback(run)?),
            },
            ViewInputParams::Readback { snapshot } => agent_core::ViewInput::Readback {
                snapshot: Box::new(crate::decode_session_readback(snapshot)?),
            },
            ViewInputParams::Page { snapshot } => {
                let (snapshot, page_key) = crate::decode_session_page(snapshot)?;
                agent_core::ViewInput::Page {
                    snapshot: Box::new(snapshot),
                    page_key,
                }
            }
            ViewInputParams::Replay { stamp, after_seq } => {
                agent_core::ViewInput::Replay { stamp, after_seq }
            }
            ViewInputParams::Event { stamp } => agent_core::ViewInput::Event { stamp },
        };
        Ok((self.state, input))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HookReadParams {
    pub session_id: String,
    pub expected_lifetime: agent_core::SessionLifetimeId,
    #[serde(default)]
    pub after_cursor: u64,
    #[serde(default = "hook_read_limit")]
    pub limit: usize,
}
fn hook_read_limit() -> usize {
    32
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryFeedbackParams {
    pub session_id: String,
    pub owner_run_id: RunId,
    pub operation_id: String,
    pub memory_id: String,
    pub feedback: agent_core::MemoryFeedback,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryEvidenceParams {
    pub session_id: String,
    pub memory_id: String,
    #[serde(default)]
    pub after_source: usize,
    #[serde(default = "evidence_limit")]
    pub limit: usize,
}
fn evidence_limit() -> usize {
    4
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryReadParams {
    pub session_id: String,
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub after_id: Option<String>,
    #[serde(default = "memory_limit")]
    pub limit: usize,
}
fn memory_limit() -> usize {
    20
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceParams {
    pub session_id: String,
    #[serde(default)]
    pub resource_id: Option<agent_core::ResourceId>,
    #[serde(default)]
    pub owner_run_id: Option<RunId>,
    #[serde(default)]
    pub after_cursor: u64,
    #[serde(default = "memory_limit")]
    pub limit: usize,
    #[serde(default)]
    pub timeout_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactReadParams {
    pub session_id: String,
    pub run_id: RunId,
    pub artifact_ref: String,
    #[serde(default)]
    pub offset: u64,
    #[serde(default = "memory_limit")]
    pub limit: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SessionCompactParams {
    pub session_id: String,
    pub owner_run_id: RunId,
    pub operation_id: String,
    pub expected_revision: agent_core::TranscriptSeq,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompactStartParams {
    pub session_id: String,
    pub expected_lifetime: agent_core::SessionLifetimeId,
    pub operation_id: String,
    pub expected_revision: agent_core::TranscriptSeq,
    pub expected_projection_generation: agent_core::ProjectionGeneration,
    #[serde(default)]
    pub entry_channel: agent_core::HookChannel,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryStoreParams {
    pub session_id: String,
    pub owner_run_id: RunId,
    pub operation_id: String,
    pub content: String,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub confirmed_by_user: bool,
    #[serde(default)]
    pub ttl_days: Option<u64>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryForgetParams {
    pub session_id: String,
    pub owner_run_id: RunId,
    pub memory_id: String,
    pub revision: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChatSendParams {
    #[serde(default)]
    pub entry_channel: agent_core::HookChannel,
    #[serde(default)]
    pub plan_execution: Option<agent_core::PlanExecution>,
    #[serde(default)]
    pub expected_lifetime: Option<agent_core::SessionLifetimeId>,
    #[serde(default)]
    pub sandbox: Option<String>,
    #[serde(default)]
    pub context_read_only: bool,
    pub message: String,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub admission_mode: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalRespondParams {
    #[serde(default)]
    pub exact_owner: Option<agent_core::ExactOwner>,
    #[serde(alias = "interaction_id")]
    pub approval_id: String,
    #[serde(default)]
    pub approved: Option<bool>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub owner_run_id: Option<RunId>,
    #[serde(default)]
    pub revision: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InteractionReadParams {
    pub interaction_id: InteractionId,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CancelParams {
    #[serde(default)]
    pub exact_owner: Option<agent_core::ExactOwner>,
    #[serde(default)]
    pub request_id: Option<RequestId>,
    #[serde(default)]
    pub run_id: Option<RunId>,
    #[serde(default)]
    pub session_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SubscribeParams {
    pub request_id: RequestId,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub after_seq: Option<EventSeq>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunReadParams {
    pub run_id: RunId,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SpawnSubagentParams {
    pub parent_session_id: SessionId,
    pub parent_run_id: RunId,
    pub spawn_key: String,
    pub task: String,
    #[serde(default)]
    pub context_source_ids: Vec<String>,
    #[serde(default)]
    pub tools: Option<Vec<String>>,
    #[serde(default)]
    pub max_rounds: Option<i64>,
    #[serde(default)]
    pub max_tokens: Option<i64>,
    #[serde(default)]
    pub max_tool_calls: Option<i64>,
    #[serde(default)]
    pub timeout_ms: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SubagentScopeParams {
    pub parent_run_id: RunId,
    pub child_run_id: RunId,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SubagentListParams {
    pub root_run_id: RunId,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WaitSubagentsParams {
    pub parent_run_id: RunId,
    pub child_run_ids: Vec<RunId>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub after_seq: Option<EventSeq>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SubagentResultParams {
    pub parent_run_id: RunId,
    pub child_run_id: RunId,
    pub owner: String,
    pub revision: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunReconcileParams {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub expected_last_seq: EventSeq,
    pub status: RunStatus,
    #[serde(default)]
    pub content: Option<String>,
    pub evidence: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QueueItemParams {
    pub session_id: String,
    pub run_id: RunId,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunEventsParams {
    pub run_id: RunId,
    #[serde(default)]
    pub after_seq: Option<EventSeq>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SessionResumeParams {
    pub session_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SessionPageParams {
    pub session_id: String,
    #[serde(default)]
    pub offset: usize,
    #[serde(default = "default_web_page_limit")]
    pub limit: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PermissionModeParams {
    pub mode: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelUseParams {
    pub profile_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelSaveParams<P> {
    pub profile: P,
    #[serde(default)]
    pub activate: bool,
}

/// v1 配置 wire 值；不构造 Provider，也不拥有配置存储。
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelProfileParams {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
    pub api_type: agent_core::ApiType,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub base_url: String,
    pub model: String,
}

fn default_web_page_limit() -> usize {
    80
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct SessionSelectorParams {
    #[serde(default)]
    pub read_model_only: Option<bool>,
    #[serde(default)]
    pub history_mode: agent_core::HistoryReadMode,
    #[serde(default)]
    pub session_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SlashExecuteParams {
    pub line: String,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub entry_channel: agent_core::HookChannel,
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct SessionCreateParams {
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub operation_id: Option<String>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SessionEndParams {
    #[serde(default)]
    pub expected_lifetime: Option<agent_core::SessionLifetimeId>,
    pub session_id: String,
    pub operation_id: String,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SessionForkParams {
    #[serde(default)]
    pub expected_lifetime: Option<agent_core::SessionLifetimeId>,
    pub session_id: String,
    pub target_session_id: String,
    pub operation_id: String,
    pub expected_revision: agent_core::TranscriptSeq,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SessionListParams {
    #[serde(default)]
    pub after_id: Option<String>,
    #[serde(default = "session_list_limit")]
    pub limit: usize,
}
fn session_list_limit() -> usize {
    1000
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceReconcileParams {
    pub session_id: String,
    pub resource_id: agent_core::ResourceId,
    pub owner_run_id: RunId,
    pub terminal_state: String,
    pub evidence: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PlanReadParams {
    pub session_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PlanDiscardParams {
    pub session_id: String,
    pub expected_lifetime: agent_core::SessionLifetimeId,
    pub identity: agent_core::PlanExecution,
}
