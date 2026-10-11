//! 原发送前捕获事务的 async 适配，使用唯一连接及共享四 worker 预算。
use crate::{RunStore, execution};
use agent_core::*;
const CAPTURE_BYTES: usize = 16 * 1024 * 1024;
fn read_error(error: crate::RuntimeError) -> RepositoryError {
    match error {
        crate::RuntimeError::Protocol(_) => CoordinationError::Corrupt.into(),
        error => error.repository_error(),
    }
}
#[async_trait::async_trait]
impl agent_storage_ports::RunRequestRepository for RunStore {
    async fn read_run_request_source(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        route: &str,
        identity: &str,
    ) -> Result<RunRequestSource, RepositoryError> {
        if !execution::valid_owner(&envelope.owner)
            || route.is_empty()
            || route.len() > 1024
            || identity.is_empty()
            || identity.len() > 1024
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, route, identity) = (envelope.clone(), route.to_owned(), identity.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .request_source_scoped(&envelope.owner, &route, &identity, Some(&envelope))
                .map_err(read_error)
        })
        .await
    }
    async fn capture_run_request(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        capture: &ProviderRequestCapture,
        context: &ContextEnvelope,
    ) -> Result<(), RepositoryError> {
        if !execution::valid_owner(&envelope.owner)
            || !execution::within_json_budget(capture, CAPTURE_BYTES)
            || !execution::within_json_budget(context, 16384)
            || capture.schema_version != 1
            || capture.round == 0
            || capture.capture_id.is_empty()
            || capture.capture_id.len() > 256
            || capture.policy_fingerprint.len() != 64
            || capture.request_digest.len() != 64
            || context.source != capture.input.source
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        // 身份差异必须在原事务内核对，不能因缺少请求捕获而放过终态/旧 generation。
        let (envelope, capture, context) = (envelope.clone(), capture.clone(), context.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .record_request_scoped(&capture, &context, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn read_provider_request(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        run: Option<&RunId>,
        capture: Option<&str>,
    ) -> Result<(ProviderRequestCapture, RunSnapshot), RepositoryError> {
        if key.0.is_empty()
            || key.0.len() > 1024
            || lifetime.0.is_empty()
            || lifetime.0.len() > 1024
            || run.is_some_and(|r| r.0.is_empty() || r.0.len() > 1024)
            || capture.is_some_and(|c| c.is_empty() || c.len() > 256)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, key, lifetime, run, capture) = (
            scope.clone(),
            key.clone(),
            lifetime.clone(),
            run.cloned(),
            capture.map(str::to_owned),
        );
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .provider_request_scoped(&scope, &key, &lifetime, run.as_ref(), capture.as_deref())
                .map_err(read_error)
        })
        .await
    }
}
