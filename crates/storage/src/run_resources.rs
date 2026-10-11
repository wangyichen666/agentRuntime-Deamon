//! 原资源表与日志游标的同事务异步适配。
use crate::{RunStore, RuntimeError, enterprise, execution, resources, sessions};
use agent_core::*;
use rusqlite::{Connection, OptionalExtension, params};

pub(crate) fn check_resource_execution_in(
    db: &Connection,
    owner: &ExactOwner,
    envelope: Option<&EnterpriseOwnerEnvelope>,
) -> Result<(), RuntimeError> {
    if let Some(envelope) = envelope {
        envelope
            .owner
            .fence(owner)
            .map_err(|_| AuthorizationError::StaleOwner)?;
        execution::check_execution_in(db, envelope)?;
        let _ = crate::requests::frozen_in(db, &owner.run_id)?;
    } else {
        enterprise::check_run_scope_in(db, &TenantContext::local(), &owner.run_id)?;
        sessions::fence_in(db, owner)?;
    }
    Ok(())
}
pub(crate) fn check_resource_identity_in(
    db: &Connection,
    scope: &TenantContext,
    key: &SessionKey,
    life: &SessionLifetimeId,
    id: ResourceId,
) -> Result<(), RuntimeError> {
    enterprise::check_session_scope_in(db, scope, key)?;
    sessions::lifetime_owner_in(db, key, life)?;
    let facts:Option<(bool,bool)>=db.query_row("SELECT r.session_id=?2 AND r.lifetime=?3,p.lifetime=r.lifetime FROM resources p JOIN runs r ON r.id=p.run_id WHERE p.id=?1",params![id.0,key.0,life.0],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    let Some((owned, bound)) = facts else {
        return Err(CoordinationError::Conflict.into());
    };
    if !owned {
        return Err(AuthorizationError::Forbidden.into());
    }
    if !bound {
        return Err(CoordinationError::Corrupt.into());
    }
    Ok(())
}
fn anchor_in(
    db: &Connection,
    id: ResourceId,
) -> Result<(SessionKey, SessionLifetimeId), RuntimeError> {
    let row:Option<(Option<String>,Option<String>)>=db.query_row("SELECT CASE WHEN length(CAST(r.session_id AS BLOB)) BETWEEN 1 AND 1024 THEN r.session_id END,CASE WHEN length(CAST(r.lifetime AS BLOB)) BETWEEN 1 AND 1024 THEN r.lifetime END FROM resources p JOIN runs r ON r.id=p.run_id WHERE p.id=?1",params![id.0],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    let row = row.ok_or(CoordinationError::Conflict)?;
    Ok((
        SessionKey(row.0.ok_or(CoordinationError::Corrupt)?),
        SessionLifetimeId(row.1.ok_or(CoordinationError::Corrupt)?),
    ))
}
fn valid_id(id: ResourceId) -> bool {
    id.0 > 0 && id.0 <= i64::MAX as u64
}
fn valid_identity(key: &SessionKey, life: &SessionLifetimeId) -> bool {
    !key.0.is_empty() && key.0.len() <= 1024 && !life.0.is_empty() && life.0.len() <= 1024
}
impl RunStore {
    pub(crate) fn read_resource_scoped(
        &self,
        scope: &TenantContext,
        context: Option<(&SessionKey, &SessionLifetimeId)>,
        id: ResourceId,
    ) -> Result<ResourceRecord, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let db = connection.transaction()?;
        let anchor;
        if let Some((key, life)) = context {
            check_resource_identity_in(&db, scope, key, life, id)?;
        } else {
            anchor = anchor_in(&db, id)?;
            check_resource_identity_in(&db, scope, &anchor.0, &anchor.1, id)?;
        }
        let record = resources::read_in(&db, id)?;
        db.commit()?;
        Ok(record)
    }
    pub(crate) fn list_resources_scoped(
        &self,
        scope: &TenantContext,
        key: Option<&SessionKey>,
        life: &SessionLifetimeId,
        after: ResourceId,
        limit: usize,
    ) -> Result<Vec<ResourceRecord>, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let db = connection.transaction()?;
        let saved;
        let key = if let Some(key) = key {
            key
        } else {
            let raw:Option<String>=db.query_row("SELECT CASE WHEN length(CAST(session_id AS BLOB)) BETWEEN 1 AND 1024 THEN session_id END FROM session_lifetime_owners WHERE lifetime=?1 AND provenance='canonical'",params![life.0],|r|r.get(0))?;
            saved = SessionKey(raw.ok_or(CoordinationError::Corrupt)?);
            &saved
        };
        enterprise::check_session_scope_in(&db, scope, key)?;
        sessions::lifetime_owner_in(&db, key, life)?;
        let mut query=db.prepare("SELECT p.id,length(CAST(p.owner_json AS BLOB))+length(CAST(p.state AS BLOB))+length(CAST(p.cwd AS BLOB))+length(CAST(p.requested AS BLOB))+length(CAST(p.effective AS BLOB))+COALESCE(length(CAST(p.process_identity AS BLOB)),0)+COALESCE(length(CAST(p.terminal_reason AS BLOB)),0) FROM resources p JOIN runs r ON r.id=p.run_id WHERE r.session_id=?1 AND r.lifetime=?2 AND p.id>?3 ORDER BY p.id LIMIT ?4")?;
        let mut rows = query.query(params![key.0, life.0, after.0, limit.clamp(1, 100)])?;
        let mut bytes = 0u64;
        let mut records = Vec::new();
        while let Some(row) = rows.next()? {
            let id = ResourceId(row.get(0)?);
            bytes = bytes.saturating_add(row.get::<_, u64>(1)?);
            if bytes > 1024 * 1024 - 16384 {
                return Err(CoordinationError::Corrupt.into());
            }
            check_resource_identity_in(&db, scope, key, life, id)?;
            records.push(resources::read_in(&db, id)?);
        }
        drop(rows);
        drop(query);
        db.commit()?;
        Ok(records)
    }
    pub(crate) fn resource_logs_scoped(
        &self,
        scope: &TenantContext,
        context: Option<(&SessionKey, &SessionLifetimeId)>,
        id: ResourceId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<(u64, String)>, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let db = connection.transaction()?;
        let anchor;
        if let Some((key, life)) = context {
            check_resource_identity_in(&db, scope, key, life, id)?;
        } else {
            anchor = anchor_in(&db, id)?;
            check_resource_identity_in(&db, scope, &anchor.0, &anchor.1, id)?;
        }
        let record = resources::read_in(&db, id)?;
        let mut query=db.prepare("SELECT cursor,CASE WHEN typeof(content)='text' AND length(CAST(content AS BLOB))<=65536 THEN content END FROM resource_logs WHERE resource_id=?1 AND cursor>?2 ORDER BY cursor LIMIT ?3")?;
        let mut rows = query.query(params![id.0, after, limit.clamp(1, 100)])?;
        let mut page = Vec::new();
        let mut cursor = after;
        let mut bytes = 0usize;
        while let Some(row) = rows.next()? {
            let next: u64 = row.get(0)?;
            if next != cursor.saturating_add(1) || next > record.log_cursor {
                return Err(CoordinationError::Corrupt.into());
            }
            let content = row
                .get::<_, Option<String>>(1)?
                .ok_or(CoordinationError::Corrupt)?;
            bytes = bytes.saturating_add(content.len());
            if bytes > 65536 {
                return Err(CoordinationError::Corrupt.into());
            }
            page.push((next, content));
            cursor = next;
        }
        drop(rows);
        drop(query);
        db.commit()?;
        Ok(page)
    }
}
#[async_trait::async_trait]
impl agent_storage_ports::RunResourceRepository for RunStore {
    async fn read_resource_settings(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<RunSnapshot, RepositoryError> {
        if !execution::valid_owner(&envelope.owner) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let envelope = envelope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            let read = || -> Result<RunSnapshot, RuntimeError> {
                let mut connection = store.lock_connection()?;
                let db = connection.transaction()?;
                execution::check_execution_in(&db, &envelope)?;
                let frozen = crate::requests::frozen_in(&db, &envelope.owner.run_id)?;
                db.commit()?;
                Ok(frozen)
            };
            read().map_err(execution::execution_error)
        })
        .await
    }
    async fn create_run_resource(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        cwd: &str,
        requested: &str,
        effective: &str,
    ) -> Result<ResourceRecord, RepositoryError> {
        if !execution::valid_owner(&envelope.owner)
            || cwd.len() > 16384
            || requested.len() > 64
            || effective.len() > 64
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, cwd, requested, effective) = (
            envelope.clone(),
            cwd.to_owned(),
            requested.to_owned(),
            effective.to_owned(),
        );
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .create_resource_scoped(
                    &envelope.owner,
                    &cwd,
                    &requested,
                    &effective,
                    Some(&envelope),
                )
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn update_run_resource(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        id: ResourceId,
        state: &str,
        identity: Option<&str>,
        reason: Option<&str>,
    ) -> Result<ResourceRecord, RepositoryError> {
        if !execution::valid_owner(&envelope.owner)
            || !valid_id(id)
            || state.len() > 32
            || identity.is_some_and(|s| s.len() > 4096)
            || reason.is_some_and(|s| s.len() > 4096)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, state, identity, reason) = (
            envelope.clone(),
            state.to_owned(),
            identity.map(str::to_owned),
            reason.map(str::to_owned),
        );
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .update_resource_scoped(
                    &envelope.owner,
                    id,
                    &state,
                    identity.as_deref(),
                    reason.as_deref(),
                    Some(&envelope),
                )
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn append_run_resource_log(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        id: ResourceId,
        content: &str,
    ) -> Result<u64, RepositoryError> {
        if !execution::valid_owner(&envelope.owner) || !valid_id(id) || content.len() > 16384 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, content) = (envelope.clone(), content.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .append_resource_log_scoped(&envelope.owner, id, &content, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn reconcile_run_resource(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        id: ResourceId,
        state: &str,
        evidence: &str,
    ) -> Result<ResourceRecord, RepositoryError> {
        if !execution::valid_owner(&envelope.owner)
            || !valid_id(id)
            || state.len() > 32
            || evidence.trim().is_empty()
            || evidence.len() > 4096
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, state, evidence) = (envelope.clone(), state.to_owned(), evidence.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .reconcile_resource_scoped(&envelope.owner, id, &state, &evidence, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn read_resource_record(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        life: &SessionLifetimeId,
        id: ResourceId,
    ) -> Result<ResourceRecord, RepositoryError> {
        if !valid_identity(key, life) || !valid_id(id) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, key, life) = (scope.clone(), key.clone(), life.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .read_resource_scoped(&scope, Some((&key, &life)), id)
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn read_resource_page(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        life: &SessionLifetimeId,
        after: ResourceId,
        limit: usize,
    ) -> Result<Vec<ResourceRecord>, RepositoryError> {
        if !valid_identity(key, life) || after.0 > i64::MAX as u64 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, key, life) = (scope.clone(), key.clone(), life.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .list_resources_scoped(&scope, Some(&key), &life, after, limit)
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn read_resource_log_page(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        life: &SessionLifetimeId,
        id: ResourceId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<(u64, String)>, RepositoryError> {
        if !valid_identity(key, life) || !valid_id(id) || after > i64::MAX as u64 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, key, life) = (scope.clone(), key.clone(), life.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .resource_logs_scoped(&scope, Some((&key, &life)), id, after, limit)
                .map_err(execution::execution_error)
        })
        .await
    }
}
