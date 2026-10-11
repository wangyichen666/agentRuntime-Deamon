//! 原会话装配端口；旧文件只是导入材料，canonical head和transcript不变。
use crate::{RunStore, RuntimeError, enterprise, execution, sessions};
use agent_core::*;
use rusqlite::params;
impl RunStore {
    pub(crate) fn selected_session_scoped(
        &self,
        scope: &TenantContext,
    ) -> Result<Option<SessionKey>, RuntimeError> {
        use rusqlite::OptionalExtension;
        if scope != &TenantContext::local() {
            return Err(AuthorizationError::Forbidden.into());
        }
        let mut connection = self.lock_connection()?;
        let db = connection.transaction()?;
        let raw:Option<Option<String>>=db.query_row("SELECT CASE WHEN length(CAST(value AS BLOB)) BETWEEN 1 AND 1024 THEN value END FROM runtime_settings WHERE name='active_session'",[],|r|r.get(0)).optional()?;
        let selected = if let Some(raw) = raw {
            let key = SessionKey(raw.ok_or(CoordinationError::Corrupt)?);
            enterprise::check_session_scope_in(&db, scope, &key)?;
            let meta = sessions::metadata_in(&db, &key)?.ok_or(CoordinationError::Corrupt)?;
            sessions::lifetime_owner_in(&db, &key, &meta.lifetime)?;
            if meta.deleted { None } else { Some(key) }
        } else {
            None
        };
        db.commit()?;
        Ok(selected)
    }

    fn session_metadata_scoped(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
    ) -> Result<Option<SessionMetadata>, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let db = connection.transaction()?;
        let exists: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sessions WHERE id=?1)",
            params![key.0],
            |r| r.get(0),
        )?;
        if !exists {
            return Ok(None);
        }
        enterprise::check_session_scope_in(&db, scope, key)?;
        let valid:bool=db.query_row("SELECT length(CAST(lifetime AS BLOB)) BETWEEN 1 AND 1024 AND revision>=0 AND typeof(revision)='integer' FROM session_heads WHERE session_id=?1",params![key.0],|r|r.get(0))?;
        if !valid {
            return Err(CoordinationError::Corrupt.into());
        }
        let meta = sessions::metadata_in(&db, key)?.ok_or(CoordinationError::Corrupt)?;
        sessions::lifetime_owner_in(&db, key, &meta.lifetime)?;
        db.commit()?;
        Ok(Some(meta))
    }
}
fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 1024
}
#[async_trait::async_trait]
impl agent_storage_ports::SessionControlRepository for RunStore {
    async fn read_selected_session(
        &self,
        scope: &TenantContext,
    ) -> Result<Option<SessionKey>, RepositoryError> {
        let scope = scope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .selected_session_scoped(&scope)
                .map_err(RuntimeError::repository_error)
        })
        .await
    }

    async fn read_session_metadata(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
    ) -> Result<Option<SessionMetadata>, RepositoryError> {
        if !valid_id(&key.0) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, key) = (scope.clone(), key.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .session_metadata_scoped(&scope, &key)
                .map_err(RuntimeError::repository_error)
        })
        .await
    }
    async fn select_session(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
    ) -> Result<(), RepositoryError> {
        if !valid_id(&key.0) || !valid_id(&lifetime.0) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, key, lifetime) = (scope.clone(), key.clone(), lifetime.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .select_session_scoped(&scope, &key, Some(&lifetime))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn import_legacy_session(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        expected: Option<&SessionLifetimeId>,
        messages: &[Message],
    ) -> Result<(), RepositoryError> {
        if !valid_id(&key.0)
            || expected.is_some_and(|id| !valid_id(&id.0))
            || messages.len() > 32768
            || !execution::within_json_budget(&messages, 16 * 1024 * 1024)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, key, expected, messages) = (
            scope.clone(),
            key.clone(),
            expected.cloned(),
            messages.to_vec(),
        );
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .import_legacy_scoped(&scope, &key, expected.as_ref(), &messages, true)
                .map_err(execution::execution_error)
        })
        .await
    }
}
