//! 原 compact 事务的异步适配；同一连接、共享预算、固定准入身份。
use crate::{RunStore, execution};
use agent_core::*;
use std::sync::Arc;
fn read_error(error: crate::RuntimeError) -> RepositoryError {
    match error {
        crate::RuntimeError::Protocol(_) => CoordinationError::Corrupt.into(),
        error => error.repository_error(),
    }
}
fn valid_operation(operation: &str) -> bool {
    !operation.is_empty() && operation.len() <= 256
}
fn valid_key(key: &SessionKey, lifetime: &SessionLifetimeId) -> bool {
    !key.0.is_empty() && key.0.len() <= 1024 && !lifetime.0.is_empty() && lifetime.0.len() <= 1024
}
impl RunStore {
    pub(crate) fn execution_settings_scoped(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<RunSnapshot, crate::RuntimeError> {
        let mut connection = self.lock_connection()?;
        let db = connection.transaction()?;
        execution::check_execution_in(&db, envelope)?;
        let snapshot = crate::requests::frozen_in(&db, &envelope.owner.run_id)?;
        db.commit()?;
        Ok(snapshot)
    }
}
#[async_trait::async_trait]
impl agent_storage_ports::RunContextRepository for RunStore {
    async fn read_execution_settings(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<RunSnapshot, RepositoryError> {
        if !execution::valid_owner(&envelope.owner) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let envelope = envelope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .execution_settings_scoped(&envelope)
                .map_err(read_error)
        })
        .await
    }

    async fn read_execution_context(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<RunContextReadback, RepositoryError> {
        if !execution::valid_owner(&envelope.owner) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let envelope = envelope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store.context_source_scoped(&envelope).map_err(read_error)
        })
        .await
    }
    async fn read_compaction_result(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        operation: &str,
        expected_revision: Option<TranscriptSeq>,
    ) -> Result<Option<ProjectionGeneration>, RepositoryError> {
        if !execution::valid_owner(&envelope.owner)
            || !valid_operation(operation)
            || expected_revision.is_some_and(|r| r.0 > i64::MAX as u64)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, operation) = (envelope.clone(), operation.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .compact_result_scoped(
                    &envelope.owner,
                    &operation,
                    expected_revision,
                    Some(&envelope),
                )
                .map_err(read_error)
        })
        .await
    }
    async fn begin_compaction(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        intent: &CompactIntent,
    ) -> Result<(), RepositoryError> {
        if !execution::valid_owner(&envelope.owner)
            || !valid_operation(&intent.operation)
            || !execution::within_json_budget(intent, 16384)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, intent) = (envelope.clone(), intent.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .begin_compact_scoped(&intent, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn settle_compaction(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        intent: &CompactIntent,
        replacement: Option<&[Message]>,
        reason: &str,
    ) -> Result<ProjectionGeneration, RepositoryError> {
        if !execution::valid_owner(&envelope.owner)
            || !valid_operation(&intent.operation)
            || !execution::within_json_budget(intent, 16384)
            || reason.is_empty()
            || reason.len() > 1024
            || replacement.is_some_and(|r| {
                r.len() > 32768 || !execution::within_json_budget(&r, 4 * 1024 * 1024)
            })
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, intent, replacement, reason) = (
            envelope.clone(),
            intent.clone(),
            replacement.map(<[Message]>::to_vec),
            reason.to_owned(),
        );
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .settle_compact_scoped(&intent, replacement.as_deref(), &reason, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn admit_compaction(
        &self,
        request: &CompactRunRequest,
        snapshot: &RunSnapshot,
        authorization: &AuthorizationRequest,
        verifier: Arc<dyn AuthorizationVerifier>,
    ) -> Result<(Admission, ExactOwner), RepositoryError> {
        if !valid_key(&request.session_key, &request.session_lifetime_id)
            || !valid_operation(&request.operation_id)
            || !execution::within_json_budget(request, 16384)
            || !execution::within_json_budget(snapshot, 1024 * 1024)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (request, snapshot, authorization) =
            (request.clone(), snapshot.clone(), authorization.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .admit_compact_owned(&request, &snapshot, &authorization, verifier.as_ref())
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn read_compact_fact(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Option<RunCompactReadback>, RepositoryError> {
        if run.0.is_empty() || run.0.len() > 1024 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, run) = (scope.clone(), run.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store.compact_fact_scoped(&scope, &run).map_err(read_error)
        })
        .await
    }
    async fn read_compact_request(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        operation: &str,
    ) -> Result<Option<CompactRunRequest>, RepositoryError> {
        if !valid_key(key, lifetime) || !valid_operation(operation) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, key, lifetime, operation) = (
            scope.clone(),
            key.clone(),
            lifetime.clone(),
            operation.to_owned(),
        );
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .compact_request_scoped(&scope, &key, &lifetime, &operation)
                .map_err(read_error)
        })
        .await
    }
    async fn read_compact_operation(
        &self,
        scope: &TenantContext,
        request: &CompactRunRequest,
    ) -> Result<Option<RunRecord>, RepositoryError> {
        if !valid_key(&request.session_key, &request.session_lifetime_id)
            || !valid_operation(&request.operation_id)
            || !execution::within_json_budget(request, 16384)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, request) = (scope.clone(), request.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .compact_operation_scoped(&scope, &request)
                .map_err(read_error)
        })
        .await
    }
}
