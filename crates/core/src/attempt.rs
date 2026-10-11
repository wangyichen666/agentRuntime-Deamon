use serde::{Deserialize, Serialize};

use crate::{ApiType, ProviderDiagnostic, ProviderErrorKind, RunId};

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProviderUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cache_read_tokens: Option<u64>,
    pub cache_creation_tokens: Option<u64>,
}

impl ProviderUsage {
    pub fn merge_partial(&mut self, other: &Self) {
        for (target, source) in [
            (&mut self.input_tokens, other.input_tokens),
            (&mut self.output_tokens, other.output_tokens),
            (&mut self.cache_read_tokens, other.cache_read_tokens),
            (&mut self.cache_creation_tokens, other.cache_creation_tokens),
        ] {
            if source.is_some() {
                *target = source;
            }
        }
    }

    pub fn add_attempt(&mut self, other: &Self) {
        for (target, source) in [
            (&mut self.input_tokens, other.input_tokens),
            (&mut self.output_tokens, other.output_tokens),
            (&mut self.cache_read_tokens, other.cache_read_tokens),
            (&mut self.cache_creation_tokens, other.cache_creation_tokens),
        ] {
            if let Some(value) = source {
                *target = Some(target.unwrap_or(0).saturating_add(value));
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStatus {
    Started,
    Succeeded,
    Failed,
    Cancelled,
    OutcomeUnknown,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProviderAttempt {
    pub attempt_id: String,
    pub run_id: RunId,
    pub round: u32,
    pub candidate_index: u32,
    pub provider_profile_id: String,
    pub api_type: ApiType,
    pub model: String,
    pub status: AttemptStatus,
    pub error_kind: Option<ProviderErrorKind>,
    pub diagnostic: Option<ProviderDiagnostic>,
    pub retry_after_ms: Option<u64>,
    pub stream_committed: bool,
    pub started_at_ms: i64,
    pub first_event_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    pub usage: Option<ProviderUsage>,
}

impl ProviderAttempt {
    /// 两种存储后端共用原调用身份；终态不得改写开始时已冻结的路由。
    pub fn same_invocation(&self, other: &Self) -> bool {
        self.attempt_id == other.attempt_id
            && self.run_id == other.run_id
            && self.round == other.round
            && self.candidate_index == other.candidate_index
            && self.provider_profile_id == other.provider_profile_id
            && self.api_type == other.api_type
            && self.model == other.model
            && self.started_at_ms == other.started_at_ms
    }
    pub fn status_name(&self) -> &'static str {
        match self.status {
            AttemptStatus::Started => "started",
            AttemptStatus::Succeeded => "succeeded",
            AttemptStatus::Failed => "failed",
            AttemptStatus::Cancelled => "cancelled",
            AttemptStatus::OutcomeUnknown => "outcome_unknown",
        }
    }
}

/// 保存正文必须与先查询的原 SQL 身份绑定；仅接受确定的旧 unknown 拼写。
pub fn decode_provider_attempt_fact(
    run: &RunId,
    id: &str,
    round: u32,
    candidate: u32,
    status: &str,
    raw: &str,
) -> Result<ProviderAttempt, crate::CoordinationError> {
    let attempt: ProviderAttempt =
        serde_json::from_str(raw).map_err(|_| crate::CoordinationError::Corrupt)?;
    let legacy_unknown =
        status == "outcomeunknown" && attempt.status == AttemptStatus::OutcomeUnknown;
    if attempt.run_id != *run
        || attempt.attempt_id != id
        || attempt.round != round
        || attempt.candidate_index != candidate
        || (status != attempt.status_name() && !legacy_unknown)
    {
        return Err(crate::CoordinationError::Corrupt);
    }
    Ok(attempt)
}
