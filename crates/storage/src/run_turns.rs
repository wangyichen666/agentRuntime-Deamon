//! 原 turn/transcript 事务的异步适配，复用统一 SQLite worker 预算。
use crate::{RunStore, execution};
use agent_core::*;

const TURN_BYTES: usize = 4 * 1024 * 1024 - 8192;
fn valid(envelope: &EnterpriseOwnerEnvelope) -> bool {
    execution::valid_owner(&envelope.owner)
}
fn round_valid(round: usize) -> bool {
    round > 0 && round <= i64::MAX as usize
}
#[async_trait::async_trait]
impl agent_storage_ports::RunTurnRepository for RunStore {
    async fn read_run_plan(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<RunPlanReadback, RepositoryError> {
        if !valid(envelope) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let envelope = envelope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .read_plan_scoped(&envelope.owner, Some(&envelope))
                .map_err(crate::RuntimeError::repository_error)
        })
        .await
    }
    async fn stage_run_plan(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        expected_revision: u64,
        value: &serde_json::Value,
    ) -> Result<PlanSnapshot, RepositoryError> {
        if !valid(envelope)
            || expected_revision > i64::MAX as u64
            || !value.is_object()
            || !execution::within_json_budget(value, 512 * 1024)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let definition: PlanDefinition =
            serde_json::from_value(value.clone()).map_err(|_| CoordinationError::InvalidRequest)?;
        definition
            .canonical_bytes()
            .map_err(|_| CoordinationError::InvalidRequest)?;
        let (envelope, value) = (envelope.clone(), value.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .stage_plan_scoped(&envelope.owner, expected_revision, &value, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn start_run_plan(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<(), RepositoryError> {
        if !valid(envelope) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let envelope = envelope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .begin_plan_execution_scoped(&envelope.owner, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn reject_run_tool_batch(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        round: usize,
        calls: &[ToolCall],
        reason: &str,
    ) -> Result<Vec<Message>, RepositoryError> {
        // 原 reason 被嵌入 JSON 结果后再次编码；预先以最坏转义预算拒绝过大的整批。
        if !valid(envelope)
            || !round_valid(round)
            || calls.is_empty()
            || calls.len() > 256
            || reason.is_empty()
            || reason.len() > 64 * 1024
            || reason.len().saturating_mul(calls.len()).saturating_mul(12) > 2 * 1024 * 1024
            || !execution::within_json_budget(&calls, 1024 * 1024)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let mut ids = std::collections::BTreeSet::new();
        if calls.iter().any(|call| {
            call.id.is_empty()
                || call.id.len() > 1024
                || call.name.is_empty()
                || call.name.len() > 1024
                || !ids.insert(&call.id)
        }) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, calls, reason) = (envelope.clone(), calls.to_vec(), reason.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .reject_tool_batch_scoped(&envelope.owner, round, &calls, &reason, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn stage_run_assistant(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        message: &Message,
    ) -> Result<(), RepositoryError> {
        if !valid(envelope)
            || message.role != Role::Assistant
            || !message.tool_calls.is_empty()
            || message.tool_call_id.is_some()
            || !execution::within_json_budget(message, TURN_BYTES)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, message) = (envelope.clone(), message.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .stage_assistant_scoped(&envelope.owner, &message, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn append_run_transcript(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        operation: &str,
        messages: &[Message],
    ) -> Result<TranscriptSeq, RepositoryError> {
        if !valid(envelope)
            || operation.is_empty()
            || operation.len() > 1024
            || messages.is_empty()
            || messages.len() > 257
            || !execution::within_json_budget(&messages, TURN_BYTES)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, operation, messages) =
            (envelope.clone(), operation.to_owned(), messages.to_vec());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .append_transcript_scoped(&envelope.owner, &operation, &messages, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
}
