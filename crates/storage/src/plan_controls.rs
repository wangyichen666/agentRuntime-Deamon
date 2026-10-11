//! 原计划管理端口，所有正文和receipt仍来自原表。
use crate::{RunStore, RuntimeError, enterprise, execution, sessions};
use agent_core::*;
use rusqlite::{Connection, OptionalExtension, params};
pub(crate) fn plan_control_budget_in(
    db: &Connection,
    life: &SessionLifetimeId,
) -> Result<(), RuntimeError> {
    let invalid:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM session_plans WHERE lifetime=?1 AND (typeof(data_json)<>'text' OR length(CAST(data_json AS BLOB))>1048576)) OR EXISTS(SELECT 1 FROM plan_versions WHERE lifetime=?1 AND (length(CAST(markdown AS BLOB))>1048576 OR length(CAST(document_json AS BLOB))>1048576)) OR EXISTS(SELECT 1 FROM plan_decisions WHERE lifetime=?1 AND (length(CAST(identity_json AS BLOB))>16384 OR length(CAST(receipt_json AS BLOB))>65536))",params![life.0],|r|r.get(0))?;
    if invalid {
        return Err(CoordinationError::Corrupt.into());
    }
    Ok(())
}
impl RunStore {
    pub(crate) fn legacy_plan_import_scoped(
        &self,
        scope: &TenantContext,
        source: &str,
    ) -> Result<bool, RuntimeError> {
        if scope != &TenantContext::local() {
            return Err(AuthorizationError::Forbidden.into());
        }
        let mut connection = self.lock_connection()?;
        let db = connection.transaction()?;
        let life:Option<Option<String>>=db.query_row("SELECT CASE WHEN length(CAST(lifetime AS BLOB)) BETWEEN 1 AND 1024 THEN lifetime END FROM legacy_plan_imports WHERE source=?1",params![source],|r|r.get(0)).optional()?;
        if let Some(life) = life {
            let life = SessionLifetimeId(life.ok_or(CoordinationError::Corrupt)?);
            let key:Option<Option<String>>=db.query_row("SELECT CASE WHEN length(CAST(session_id AS BLOB)) BETWEEN 1 AND 1024 THEN session_id END FROM session_lifetime_owners WHERE lifetime=?1 AND provenance='canonical'",params![life.0],|r|r.get(0)).optional()?;
            let key = SessionKey(key.flatten().ok_or(CoordinationError::Corrupt)?);
            enterprise::check_session_scope_in(&db, scope, &key)?;
            sessions::lifetime_owner_in(&db, &key, &life)?;
            db.commit()?;
            Ok(true)
        } else {
            db.commit()?;
            Ok(false)
        }
    }
}
fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 1024
}
#[async_trait::async_trait]
impl agent_storage_ports::PlanControlRepository for RunStore {
    async fn read_session_plan(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
    ) -> Result<PlanReadback, RepositoryError> {
        if !valid_id(&key.0) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, key) = (scope.clone(), key.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            let read = store
                .read_plan_document_scoped(&scope, &key)
                .map_err(RuntimeError::repository_error)?;
            if !execution::within_json_budget(&read, 1024 * 1024 - 16384) {
                return Err(CoordinationError::InvalidRequest.into());
            }
            Ok(read)
        })
        .await
    }
    async fn discard_session_plan(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        identity: &PlanExecution,
    ) -> Result<PlanDecisionReceipt, RepositoryError> {
        if !valid_id(&key.0)
            || !valid_id(&lifetime.0)
            || !execution::within_json_budget(identity, 16384)
            || identity.revision > i64::MAX as u64
            || identity.operation_id.is_empty()
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, key, lifetime, identity) = (
            scope.clone(),
            key.clone(),
            lifetime.clone(),
            identity.clone(),
        );
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .discard_plan_document_scoped(&scope, &key, &lifetime, &identity)
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn read_legacy_plan_import(
        &self,
        scope: &TenantContext,
        source: &str,
    ) -> Result<bool, RepositoryError> {
        if source.is_empty() || source.len() > 16384 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, source) = (scope.clone(), source.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .legacy_plan_import_scoped(&scope, &source)
                .map_err(RuntimeError::repository_error)
        })
        .await
    }
    async fn import_legacy_plan_file(
        &self,
        scope: &TenantContext,
        source: &str,
        digest: &str,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        value: &serde_json::Value,
    ) -> Result<(), RepositoryError> {
        if !valid_id(&key.0)
            || !valid_id(&lifetime.0)
            || source.is_empty()
            || source.len() > 16384
            || digest.len() != 64
            || !digest.bytes().all(|b| b.is_ascii_hexdigit())
            || !execution::within_json_budget(value, 1024 * 1024)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, source, digest, key, lifetime, value) = (
            scope.clone(),
            source.to_owned(),
            digest.to_owned(),
            key.clone(),
            lifetime.clone(),
            value.clone(),
        );
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .import_plan_file_scoped(&scope, &source, &digest, &key, Some(&lifetime), &value)
                .map_err(execution::execution_error)
        })
        .await
    }
}
