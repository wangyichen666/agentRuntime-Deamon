use crate::{ExactOwner, Message, ProjectionGeneration, Role, SessionLifetimeId, TranscriptSeq};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ContextSource {
    pub lifetime: SessionLifetimeId,
    #[serde(default = "source_start")]
    pub source_start: TranscriptSeq,
    pub source_end: TranscriptSeq,
    pub prefix_digest: String,
    pub generation: ProjectionGeneration,
    pub policy_fingerprint: String,
    #[serde(default)]
    pub pressure_route: String,
    #[serde(default)]
    pub summary_route: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactIntent {
    pub operation: String,
    pub owner: ExactOwner,
    pub source: ContextSource,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactRunRequest {
    pub session_key: crate::SessionKey,
    pub session_lifetime_id: SessionLifetimeId,
    pub operation_id: String,
    pub expected_revision: TranscriptSeq,
    pub expected_projection_generation: ProjectionGeneration,
    #[serde(default)]
    pub compatibility_owner_run_id: Option<crate::RunId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactRunOutcome {
    Started,
    Committed,
    NoGain,
    Rejected,
    Failed,
    Cancelled,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactRunReadback {
    pub schema_version: u16,
    pub owner: ExactOwner,
    pub operation_id: String,
    pub source: ContextSource,
    pub outcome: CompactRunOutcome,
    pub result_generation: Option<ProjectionGeneration>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContextProjection {
    pub source_end: TranscriptSeq,
    pub generation: ProjectionGeneration,
    pub messages: Vec<Message>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextEnvelope {
    pub source: ContextSource,
    pub route: String,
    #[serde(default)]
    pub provider_identity: String,
    pub tool_catalog_digest: String,
    pub stable_tokens: u64,
    pub history_tokens: u64,
    pub retrieved_tokens: u64,
    pub overlay_tokens: u64,
    #[serde(default)]
    pub calibrated_input_tokens: u64,
    pub output_reserve: u64,
    pub budget: u64,
}

fn source_start() -> TranscriptSeq {
    TranscriptSeq(0)
}

/// 压缩不得删除或改写最新用户输入及其后的完整轮次。
pub fn current_turn_start(messages: &[Message]) -> usize {
    messages
        .iter()
        .rposition(|m| m.role == Role::User)
        .unwrap_or(messages.len())
}
pub fn preserves_current_turn(source: &[Message], replacement: &[Message]) -> bool {
    replacement.ends_with(&source[current_turn_start(source)..])
}

/// 两种事实源共用 compact 候选规则，保留当前轮次及完整工具配对。
pub fn validate_compaction_candidate(
    source: &[Message],
    candidate: &[Message],
) -> Result<(), crate::CoordinationError> {
    if !preserves_current_turn(source, candidate)
        || candidate.is_empty()
        || candidate.iter().all(|message| {
            message
                .content
                .as_deref()
                .is_none_or(|content| content.trim().is_empty())
        })
    {
        return Err(crate::CoordinationError::InvalidRequest);
    }
    let mut pending = std::collections::VecDeque::new();
    for message in candidate {
        if message.role == Role::Tool {
            if pending.pop_front() != message.tool_call_id.as_deref() {
                return Err(crate::CoordinationError::InvalidRequest);
            }
        } else {
            if !pending.is_empty() {
                return Err(crate::CoordinationError::InvalidRequest);
            }
            pending.extend(message.tool_calls.iter().map(|call| call.id.as_str()));
        }
    }
    if !pending.is_empty() {
        return Err(crate::CoordinationError::InvalidRequest);
    }
    Ok(())
}

/// 显式选取的父 run 材料；来源已由宿主核实，文本可能截断或过滤。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DelegationContext {
    pub version: u8,
    pub parent: ExactOwner,
    pub sources: Vec<crate::MemorySourceEvidence>,
    pub digest: String,
}

/// 同事务的原运行上下文，不是入口DTO或缓存事实。
#[derive(Clone, Debug)]
pub struct RunContextReadback {
    pub snapshot: crate::SessionSnapshot,
    pub projection: ContextProjection,
    pub frozen: crate::RunSnapshot,
}
/// 原compact运行、回执和最后事件stamp的同事务投影。
#[derive(Clone, Debug)]
pub struct RunCompactReadback {
    pub run: crate::RunRecord,
    pub receipt: crate::CompactRunReadback,
    pub view_stamp: Option<crate::ViewStamp>,
}
