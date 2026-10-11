//! 原发现事务的异步适配，同原Connection与四permit。
use crate::{RunStore, RuntimeError, execution};
use agent_core::*;
fn read_error(error: RuntimeError) -> RepositoryError {
    match error {
        RuntimeError::Protocol(_) => CoordinationError::Corrupt.into(),
        error => error.repository_error(),
    }
}
#[async_trait::async_trait]
impl agent_storage_ports::RunDiscoveryRepository for RunStore {
    async fn publish_run_discovery(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        receipt: &ToolDiscoveryReceipt,
    ) -> Result<ToolDiscoveryReceipt, RepositoryError> {
        if !execution::valid_owner(&envelope.owner)
            || !execution::within_json_budget(receipt, 32 * 1024)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, receipt) = (envelope.clone(), receipt.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .publish_discovery_scoped(&receipt, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn read_discovered_tool(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        generation: &str,
        name: &str,
    ) -> Result<bool, RepositoryError> {
        if !execution::valid_owner(&envelope.owner)
            || generation.len() != 64
            || !generation.bytes().all(|b| b.is_ascii_hexdigit())
            || name.is_empty()
            || name.len() > 256
            || name.chars().any(char::is_whitespace)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, generation, name) =
            (envelope.clone(), generation.to_owned(), name.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .discovered_tool_scoped(&envelope.owner, &generation, &name, Some(&envelope))
                .map_err(read_error)
        })
        .await
    }
    async fn read_run_discoveries(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Vec<ToolDiscoveryReceipt>, RepositoryError> {
        if run.0.is_empty() || run.0.len() > 1024 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, run) = (scope.clone(), run.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store.discoveries_scoped(&scope, &run).map_err(read_error)
        })
        .await
    }
    async fn request_run_cancellation(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<(), RepositoryError> {
        if !execution::valid_owner(&envelope.owner) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let envelope = envelope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .request_cancellation_scoped(&envelope.owner, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
}
