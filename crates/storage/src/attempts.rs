//! 原 Provider 事务的异步适配，共用 canonical Connection/owner/budget。
use crate::{RunStore, RuntimeError, execution};
use agent_core::*;
impl RunStore {
    fn attempt_usage_scoped(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<ProviderUsage, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        execution::check_execution_in(&tx, envelope)?;
        let (count,bytes):(u64,u64)=tx.query_row("SELECT count(*),COALESCE(sum(length(CAST(data_json AS BLOB))),0) FROM provider_attempts WHERE run_id=?1",[&envelope.owner.run_id.0],|r|Ok((r.get(0)?,r.get(1)?)))?;
        if count > 1000 || bytes > 1024 * 1024 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let mut total = ProviderUsage::default();
        for attempt in crate::provider_attempts_in(&tx, &envelope.owner.run_id)? {
            if let Some(usage) = attempt.usage {
                total.add_attempt(&usage);
            }
        }
        tx.commit()?;
        Ok(total)
    }
}
fn valid_attempt(envelope: &EnterpriseOwnerEnvelope, attempt: &ProviderAttempt) -> bool {
    execution::valid_owner(&envelope.owner)
        && attempt.run_id == envelope.owner.run_id
        && !attempt.attempt_id.is_empty()
        && attempt.attempt_id.len() <= 256
        && attempt.round > 0
        && attempt.provider_profile_id.len() <= 1024
        && !attempt.provider_profile_id.is_empty()
        && !attempt.model.is_empty()
        && attempt.model.len() <= 1024
        && execution::within_json_budget(attempt, 1024 * 1024 - 1024)
}
#[async_trait::async_trait]
impl agent_storage_ports::RunAttemptRepository for RunStore {
    async fn start_run_attempt(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        attempt: &ProviderAttempt,
    ) -> Result<(), RepositoryError> {
        if !valid_attempt(envelope, attempt)
            || attempt.status != AttemptStatus::Started
            || attempt.finished_at_ms.is_some()
            || attempt.first_event_at_ms.is_some()
            || attempt.usage.is_some()
            || attempt.stream_committed
            || attempt.error_kind.is_some()
            || attempt.diagnostic.is_some()
            || attempt.retry_after_ms.is_some()
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, attempt) = (envelope.clone(), attempt.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .start_attempt_scoped(&attempt, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn finish_run_attempt(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        attempt: &ProviderAttempt,
    ) -> Result<(), RepositoryError> {
        if !valid_attempt(envelope, attempt)
            || attempt.status == AttemptStatus::Started
            || attempt.finished_at_ms.is_none()
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, attempt) = (envelope.clone(), attempt.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .finish_attempt_scoped(&attempt, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn read_attempt_usage(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<ProviderUsage, RepositoryError> {
        if !execution::valid_owner(&envelope.owner) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let envelope = envelope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .attempt_usage_scoped(&envelope)
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn append_provider_event(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        event: &str,
        data: &serde_json::Value,
    ) -> Result<EventSeq, RepositoryError> {
        if !execution::valid_owner(&envelope.owner)
            || !matches!(
                event,
                "provider_circuit" | "provider_retry" | "provider_fallback"
            )
            || !data.is_object()
            || !execution::within_json_budget(data, 64 * 1024)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, event, data) = (envelope.clone(), event.to_owned(), data.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .append_event_scoped(&envelope.owner.run_id, &event, &data, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
}
