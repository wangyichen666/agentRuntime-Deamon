//! 原持久队列和请求定位的异步端口；范围和永久归属在正文加载前核对。
use crate::{RunStore, RuntimeError, enterprise, execution, sessions};
use agent_core::*;
use rusqlite::{Connection, OptionalExtension, params};
const PAGE_BYTES: usize = 1024 * 1024 - 16384;

fn bounded_record_in(
    db: &Connection,
    scope: &TenantContext,
    run: &RunId,
) -> Result<RunRecord, RuntimeError> {
    enterprise::check_run_scope_in(db, scope, run)?;
    let owner = sessions::owner_in(db, run)?;
    sessions::lifetime_owner_in(db, &owner.session_key, &owner.session_lifetime_id)?;
    let valid: bool = db.query_row("SELECT length(CAST(session_id AS BLOB)) BETWEEN 1 AND 1024 AND length(CAST(request_id_wire_json AS BLOB))<=8192 AND length(CAST(status AS BLOB))<=32 AND COALESCE(length(CAST(content AS BLOB)),0)+COALESCE(length(CAST(error_message AS BLOB)),0)<=1000000 AND last_seq>=0 FROM runs WHERE id=?1", params![run.0], |r|r.get(0))?;
    if !valid {
        return Err(CoordinationError::Corrupt.into());
    }
    let record = crate::read_run_in(db, &run.0)?.ok_or(CoordinationError::Corrupt)?;
    if !execution::within_json_budget(&record, PAGE_BYTES) {
        return Err(CoordinationError::InvalidRequest.into());
    }
    Ok(record)
}
fn queue_record_in(
    db: &Connection,
    scope: &TenantContext,
    key: &SessionKey,
    run: &RunId,
    queued_only: bool,
) -> Result<Option<QueuedMessage>, RuntimeError> {
    enterprise::check_run_scope_in(db, scope, run)?;
    let owner = sessions::owner_in(db, run)?;
    if owner.session_key != *key {
        return Err(AuthorizationError::Forbidden.into());
    }
    sessions::lifetime_owner_in(db, key, &owner.session_lifetime_id)?;
    type Raw = (i64, Option<String>, Option<String>, bool);
    let raw:Option<Raw>=db.query_row("SELECT q.id,CASE WHEN typeof(q.message)='text' AND length(CAST(q.message AS BLOB))<=65536 THEN q.message END,CASE WHEN length(CAST(q.status AS BLOB))<=32 THEN q.status END,q.session_id=r.session_id AND (q.status=r.status OR q.status='running' AND r.status='waiting_interaction') FROM queued_messages q JOIN runs r ON r.id=q.run_id JOIN session_heads h ON h.session_id=r.session_id AND h.lifetime=r.lifetime AND h.deleted=0 WHERE q.session_id=?1 AND q.run_id=?2 AND (?3=0 OR q.status='queued')",params![key.0,run.0,queued_only],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
    let Some((id, message, status, bound)) = raw else {
        return Ok(None);
    };
    let status = status.ok_or(CoordinationError::Corrupt)?;
    if id <= 0
        || !bound
        || !matches!(
            status.as_str(),
            "queued"
                | "running"
                | "waiting_interaction"
                | "completed"
                | "failed"
                | "cancelled"
                | "unknown_after_restart"
        )
    {
        return Err(CoordinationError::Corrupt.into());
    }
    let position = if status == "queued" {
        db.query_row("SELECT count(*) FROM queued_messages q JOIN runs r ON r.id=q.run_id JOIN session_heads h ON h.session_id=r.session_id AND h.lifetime=r.lifetime AND h.deleted=0 WHERE q.session_id=?1 AND q.status='queued' AND q.id<=?2",params![key.0,id],|r|r.get::<_,i64>(0))?
    } else {
        0
    };
    if !(0..=64).contains(&position) {
        return Err(CoordinationError::Corrupt.into());
    }
    Ok(Some(QueuedMessage {
        id,
        position,
        session_id: key.clone(),
        run_id: run.clone(),
        message: message.ok_or(CoordinationError::Corrupt)?,
        status,
    }))
}
impl RunStore {
    pub(crate) fn queue_messages_scoped(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        run: Option<&RunId>,
    ) -> Result<Vec<QueuedMessage>, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let db = connection.transaction()?;
        enterprise::check_session_scope_in(&db, scope, key)?;
        let records = if let Some(run) = run {
            queue_record_in(&db, scope, key, run, false)?
                .into_iter()
                .collect::<Vec<_>>()
        } else {
            let mut query=db.prepare("SELECT CASE WHEN length(CAST(q.run_id AS BLOB)) BETWEEN 1 AND 1024 THEN q.run_id END FROM queued_messages q JOIN runs r ON r.id=q.run_id JOIN session_heads h ON h.session_id=r.session_id AND h.lifetime=r.lifetime AND h.deleted=0 WHERE q.session_id=?1 AND q.status='queued' ORDER BY q.id LIMIT 65")?;
            let ids = query
                .query_map(params![key.0], |r| r.get::<_, Option<String>>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            drop(query);
            if ids.len() > 64 {
                return Err(CoordinationError::Corrupt.into());
            }
            ids.into_iter()
                .map(|id| {
                    let run = RunId(id.ok_or(CoordinationError::Corrupt)?);
                    queue_record_in(&db, scope, key, &run, true)?
                        .ok_or_else(|| RuntimeError::Coordination(CoordinationError::Corrupt))
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        if !execution::within_json_budget(&records, PAGE_BYTES) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        db.commit()?;
        Ok(records)
    }
    pub(crate) fn lookup_request_scoped(
        &self,
        scope: &TenantContext,
        key: Option<&SessionKey>,
        request: &RequestId,
    ) -> Result<Option<RunRecord>, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let db = connection.transaction()?;
        if let Some(key) = key {
            enterprise::check_session_scope_in(&db, scope, key)?;
        }
        let request_json =
            serde_json::to_string(request).map_err(|_| CoordinationError::InvalidRequest)?;
        // 唯一性只在可见范围内判定；另一租户的重复request_id不会泄露或抢占本域结果。
        let mut query=db.prepare("SELECT CASE WHEN length(CAST(r.id AS BLOB)) BETWEEN 1 AND 1024 THEN r.id END FROM runs r JOIN session_heads h ON h.session_id=r.session_id AND h.lifetime=r.lifetime AND h.deleted=0 JOIN session_scopes s ON s.session_id=r.session_id JOIN tenants t ON t.id=s.tenant_id WHERE t.organization_id=?1 AND s.tenant_id=?2 AND s.project_id=?3 AND s.agent_uid=?4 AND (?5 IS NULL OR r.session_id=?5) AND r.request_id_wire_json=?6 ORDER BY r.rowid LIMIT 2")?;
        let ids = query
            .query_map(
                params![
                    scope.organization_id.as_str(),
                    scope.tenant_id.as_str(),
                    scope.project_id.as_str(),
                    scope.agent_uid.as_str(),
                    key.map(|k| k.0.as_str()),
                    request_json
                ],
                |r| r.get::<_, Option<String>>(0),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        drop(query);
        if ids.len() > 1 {
            return Err(CoordinationError::Conflict.into());
        }
        let record = ids
            .into_iter()
            .next()
            .map(|id| bounded_record_in(&db, scope, &RunId(id.ok_or(CoordinationError::Corrupt)?)))
            .transpose()?;
        db.commit()?;
        Ok(record)
    }
    pub(crate) fn run_record_scoped(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Option<RunRecord>, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let db = connection.transaction()?;
        let record = bounded_record_in(&db, scope, run)?;
        db.commit()?;
        Ok(Some(record))
    }
    pub(crate) fn recoverable_queue_scoped(
        &self,
        scope: &TenantContext,
        after: Option<QueueRecoveryCursor>,
        limit: usize,
    ) -> Result<QueueRecoveryPage, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let db = connection.transaction()?;
        let mut query=db.prepare("SELECT CASE WHEN length(CAST(r.id AS BLOB)) BETWEEN 1 AND 1024 THEN r.id END,r.created_at_ms,r.rowid FROM runs r JOIN session_heads h ON h.session_id=r.session_id AND h.lifetime=r.lifetime AND h.deleted=0 JOIN session_scopes s ON s.session_id=r.session_id JOIN tenants t ON t.id=s.tenant_id WHERE t.organization_id=?1 AND s.tenant_id=?2 AND s.project_id=?3 AND s.agent_uid=?4 AND r.status='queued' AND NOT EXISTS(SELECT 1 FROM plan_decisions p WHERE p.run_id=r.id AND p.decision='execute') AND (?5 IS NULL OR r.created_at_ms>?5 OR r.created_at_ms=?5 AND r.rowid>?6) ORDER BY r.created_at_ms,r.rowid LIMIT ?7")?;
        let raw = query
            .query_map(
                params![
                    scope.organization_id.as_str(),
                    scope.tenant_id.as_str(),
                    scope.project_id.as_str(),
                    scope.agent_uid.as_str(),
                    after.map(|c| c.created_at_ms),
                    after.map(|c| c.row_id),
                    limit as i64 + 1
                ],
                |r| {
                    Ok((
                        r.get::<_, Option<String>>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                    ))
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;
        drop(query);
        let has_more = raw.len() > limit;
        let mut runs = Vec::new();
        let mut cursor = after;
        for (id, created_at_ms, row_id) in raw.into_iter().take(limit) {
            if row_id <= 0 {
                return Err(CoordinationError::Corrupt.into());
            }
            let run = RunId(id.ok_or(CoordinationError::Corrupt)?);
            enterprise::check_run_scope_in(&db, scope, &run)?;
            let owner = sessions::owner_in(&db, &run)?;
            sessions::lifetime_owner_in(&db, &owner.session_key, &owner.session_lifetime_id)?;
            cursor = Some(QueueRecoveryCursor {
                created_at_ms,
                row_id,
            });
            runs.push(run);
        }
        let page = QueueRecoveryPage {
            runs,
            cursor,
            has_more,
        };
        if !execution::within_json_budget(&page, 65536) {
            return Err(CoordinationError::Corrupt.into());
        }
        db.commit()?;
        Ok(page)
    }
}
fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 1024
}
#[async_trait::async_trait]
impl agent_storage_ports::RunQueueRepository for RunStore {
    async fn read_queued_messages(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
    ) -> Result<Vec<QueuedMessage>, RepositoryError> {
        if !valid_id(&key.0) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, key) = (scope.clone(), key.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .queue_messages_scoped(&scope, &key, None)
                .map_err(RuntimeError::repository_error)
        })
        .await
    }
    async fn read_queued_message(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        run: &RunId,
    ) -> Result<Option<QueuedMessage>, RepositoryError> {
        if !valid_id(&key.0) || !valid_id(&run.0) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, key, run) = (scope.clone(), key.clone(), run.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .queue_messages_scoped(&scope, &key, Some(&run))
                .map(|mut rows| rows.pop())
                .map_err(RuntimeError::repository_error)
        })
        .await
    }
    async fn read_recoverable_queue(
        &self,
        scope: &TenantContext,
        after: Option<QueueRecoveryCursor>,
        limit: usize,
    ) -> Result<QueueRecoveryPage, RepositoryError> {
        if !(1..=32).contains(&limit) || after.is_some_and(|c| c.row_id <= 0) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let scope = scope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .recoverable_queue_scoped(&scope, after, limit)
                .map_err(RuntimeError::repository_error)
        })
        .await
    }
    async fn lookup_request_run(
        &self,
        scope: &TenantContext,
        key: Option<&SessionKey>,
        request: &RequestId,
    ) -> Result<Option<RunRecord>, RepositoryError> {
        if key.is_some_and(|k| !valid_id(&k.0)) || !execution::within_json_budget(request, 8192) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, key, request) = (scope.clone(), key.cloned(), request.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .lookup_request_scoped(&scope, key.as_ref(), &request)
                .map_err(RuntimeError::repository_error)
        })
        .await
    }
    async fn read_run_record(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Option<RunRecord>, RepositoryError> {
        if !valid_id(&run.0) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, run) = (scope.clone(), run.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .run_record_scoped(&scope, &run)
                .map_err(RuntimeError::repository_error)
        })
        .await
    }
}
