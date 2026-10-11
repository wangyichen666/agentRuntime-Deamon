//! 原委派SQL的异步适配，原child run和reservation仍是唯一事实。
use crate::{RunStore, RuntimeError, delegation, enterprise, execution, sessions};
use agent_core::*;
use rusqlite::{Connection, OptionalExtension, params};
const DELEGATION_BYTES: u64 = 4 * 1024 * 1024;

pub(crate) fn delegation_budget_in(db: &Connection, child: &str) -> Result<(), RuntimeError> {
    let row:Option<(u64,bool)>=db.query_row("SELECT length(CAST(d.root_session_id AS BLOB))+length(CAST(d.root_run_id AS BLOB))+length(CAST(d.parent_session_id AS BLOB))+length(CAST(d.parent_run_id AS BLOB))+length(CAST(d.child_session_id AS BLOB))+length(CAST(d.spawn_key AS BLOB))+length(CAST(d.tools_json AS BLOB))+length(CAST(d.permission_mode AS BLOB))+COALESCE(length(CAST(d.route_json AS BLOB)),0)+length(CAST(d.cwd AS BLOB))+COALESCE(length(CAST(d.terminal_json AS BLOB)),0)+COALESCE(length(CAST(d.reservation_owner AS BLOB)),0)+COALESCE(length(CAST(r.content AS BLOB)),0)+COALESCE(length(CAST(r.error_message AS BLOB)),0),length(CAST(d.root_session_id AS BLOB)) BETWEEN 1 AND 1024 AND length(CAST(d.root_run_id AS BLOB)) BETWEEN 1 AND 1024 AND length(CAST(d.parent_session_id AS BLOB)) BETWEEN 1 AND 1024 AND length(CAST(d.parent_run_id AS BLOB)) BETWEEN 1 AND 1024 AND length(CAST(d.child_session_id AS BLOB)) BETWEEN 1 AND 1024 AND length(CAST(d.spawn_key AS BLOB)) BETWEEN 1 AND 128 AND length(CAST(d.permission_mode AS BLOB))<=64 AND length(CAST(d.cwd AS BLOB))<=16384 AND length(CAST(d.tools_json AS BLOB))<=65536 AND (d.route_json IS NULL OR length(CAST(d.route_json AS BLOB))<=1048576) AND (d.reservation_owner IS NULL OR length(CAST(d.reservation_owner AS BLOB))<=256) AND d.depth BETWEEN 1 AND 2 AND d.revision>=0 AND d.max_rounds>0 AND d.max_tokens>0 AND d.max_tool_calls>0 AND d.status=r.status AND d.child_session_id=r.session_id AND d.result_state IN ('unconsumed','reserved','delivered') FROM delegations d JOIN runs r ON r.id=d.child_run_id WHERE d.child_run_id=?1",params![child],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    if row.is_some_and(|(bytes, valid)| bytes > DELEGATION_BYTES || !valid) {
        return Err(CoordinationError::Corrupt.into());
    }
    Ok(())
}
fn relation_in(
    db: &Connection,
    scope: &TenantContext,
    child: &RunId,
    parent: Option<&RunId>,
) -> Result<bool, RuntimeError> {
    let exists: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM delegations WHERE child_run_id=?1)",
        params![child.0],
        |r| r.get(0),
    )?;
    if !exists {
        return Ok(false);
    }
    enterprise::check_run_scope_in(db, scope, child)?;
    let row:(Option<String>,Option<String>,bool)=db.query_row("SELECT CASE WHEN length(CAST(parent_run_id AS BLOB)) BETWEEN 1 AND 1024 THEN parent_run_id END,CASE WHEN length(CAST(root_run_id AS BLOB)) BETWEEN 1 AND 1024 THEN root_run_id END,EXISTS(SELECT 1 FROM runs p JOIN runs c ON c.id=d.child_run_id JOIN runs root ON root.id=d.root_run_id WHERE p.id=d.parent_run_id AND p.session_id=d.parent_session_id AND c.session_id=d.child_session_id AND root.session_id=d.root_session_id AND p.id<>c.id AND root.id<>c.id) FROM delegations d WHERE child_run_id=?1",params![child.0],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    let (parent_run, root_run) = (
        RunId(row.0.ok_or(CoordinationError::Corrupt)?),
        RunId(row.1.ok_or(CoordinationError::Corrupt)?),
    );
    if !row.2 {
        return Err(CoordinationError::Corrupt.into());
    }
    if parent.is_some_and(|requested| requested != &parent_run && requested != &root_run) {
        return Err(AuthorizationError::Forbidden.into());
    }
    for run in [&parent_run, &root_run] {
        enterprise::check_run_scope_in(db, scope, run).map_err(|_| CoordinationError::Corrupt)?;
        let owner = sessions::owner_in(db, run)?;
        sessions::lifetime_owner_in(db, &owner.session_key, &owner.session_lifetime_id)
            .map_err(|_| CoordinationError::Corrupt)?;
    }
    let owner = sessions::owner_in(db, child)?;
    sessions::lifetime_owner_in(db, &owner.session_key, &owner.session_lifetime_id)?;
    Ok(true)
}
pub(crate) fn check_parent_execution_in(
    db: &Connection,
    key: &SessionKey,
    run: &RunId,
    envelope: Option<&EnterpriseOwnerEnvelope>,
) -> Result<(), RuntimeError> {
    if let Some(envelope) = envelope {
        if envelope.owner.session_key != *key || envelope.owner.run_id != *run {
            return Err(AuthorizationError::StaleOwner.into());
        }
        execution::check_execution_in(db, envelope)?;
        let _ = crate::requests::frozen_in(db, run)?;
        // 原child身份及继承helper当前仅支持Local。全量租户key/PG尚未实施，明确拒绝跨域准入。
        if envelope.scope != TenantContext::local() {
            return Err(AuthorizationError::Forbidden.into());
        }
    } else {
        enterprise::check_run_scope_in(db, &TenantContext::local(), run)?;
        let owner = sessions::owner_in(db, run)?;
        if owner.session_key != *key {
            return Err(AuthorizationError::Forbidden.into());
        }
        sessions::fence_in(db, &owner)?;
    }
    Ok(())
}
pub(crate) fn check_result_execution_in(
    db: &Connection,
    child: &RunId,
    envelope: Option<&EnterpriseOwnerEnvelope>,
) -> Result<(), RuntimeError> {
    if let Some(envelope) = envelope {
        execution::check_execution_in(db, envelope)?;
        let _ = crate::requests::frozen_in(db, &envelope.owner.run_id)?;
        if !relation_in(db, &envelope.scope, child, Some(&envelope.owner.run_id))? {
            return Err(CoordinationError::Conflict.into());
        }
    } else {
        if !relation_in(db, &TenantContext::local(), child, None)? {
            return Err(CoordinationError::Conflict.into());
        }
        let parent: String = db.query_row(
            "SELECT parent_run_id FROM delegations WHERE child_run_id=?1",
            params![child.0],
            |r| r.get(0),
        )?;
        let owner = sessions::owner_in(db, &RunId(parent))?;
        sessions::fence_in(db, &owner)?;
    }
    Ok(())
}
fn valid_run(run: &RunId) -> bool {
    !run.0.is_empty() && run.0.len() <= 1024
}
impl RunStore {
    pub(crate) fn delegation_scoped(
        &self,
        scope: &TenantContext,
        child: &RunId,
        parent: Option<&RunId>,
    ) -> Result<Option<DelegationRecord>, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let db = connection.transaction()?;
        if !relation_in(&db, scope, child, parent)? {
            return Ok(None);
        }
        let record = delegation::read_delegation_in(&db, &child.0)?;
        db.commit()?;
        Ok(record)
    }
    pub(crate) fn delegation_lookup_scoped(
        &self,
        scope: &TenantContext,
        key: Option<&SessionKey>,
        parent: Option<(&RunId, &str)>,
    ) -> Result<Option<DelegationRecord>, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let db = connection.transaction()?;
        let raw: Option<Option<String>> = if let Some(key) = key {
            enterprise::check_session_scope_in(&db, scope, key)?;
            db.query_row("SELECT CASE WHEN length(CAST(child_run_id AS BLOB)) BETWEEN 1 AND 1024 THEN child_run_id END FROM delegations WHERE child_session_id=?1",params![key.0],|r|r.get(0)).optional()?
        } else if let Some((run, spawn)) = parent {
            enterprise::check_run_scope_in(&db, scope, run)?;
            db.query_row("SELECT CASE WHEN length(CAST(child_run_id AS BLOB)) BETWEEN 1 AND 1024 THEN child_run_id END FROM delegations WHERE parent_run_id=?1 AND spawn_key=?2",params![run.0,spawn],|r|r.get(0)).optional()?
        } else {
            return Err(CoordinationError::InvalidRequest.into());
        };
        let record = if let Some(raw) = raw {
            let child = RunId(raw.ok_or(CoordinationError::Corrupt)?);
            if !relation_in(&db, scope, &child, parent.map(|(run, _)| run))? {
                return Err(CoordinationError::Corrupt.into());
            }
            delegation::read_delegation_in(&db, &child.0)?
        } else {
            None
        };
        db.commit()?;
        Ok(record)
    }
    pub(crate) fn delegation_list_scoped(
        &self,
        scope: &TenantContext,
        root: &RunId,
    ) -> Result<Vec<DelegationRecord>, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let db = connection.transaction()?;
        enterprise::check_run_scope_in(&db, scope, root)?;
        let count: u64 = db.query_row(
            "SELECT count(*) FROM (SELECT id FROM delegations WHERE root_run_id=?1 LIMIT 9)",
            params![root.0],
            |r| r.get(0),
        )?;
        if count > 8 {
            return Err(CoordinationError::Corrupt.into());
        }
        let mut query=db.prepare("SELECT CASE WHEN length(CAST(child_run_id AS BLOB)) BETWEEN 1 AND 1024 THEN child_run_id END FROM delegations WHERE root_run_id=?1 ORDER BY id LIMIT 8")?;
        let mut rows = query.query(params![root.0])?;
        let mut records = Vec::new();
        let mut bytes = 0usize;
        while let Some(row) = rows.next()? {
            let child = RunId(
                row.get::<_, Option<String>>(0)?
                    .ok_or(CoordinationError::Corrupt)?,
            );
            if !relation_in(&db, scope, &child, Some(root))? {
                return Err(CoordinationError::Corrupt.into());
            }
            let record =
                delegation::read_delegation_in(&db, &child.0)?.ok_or(CoordinationError::Corrupt)?;
            let size = serde_json::to_vec(&record)
                .map_err(|_| CoordinationError::Corrupt)?
                .len();
            bytes = bytes.saturating_add(size);
            if bytes > DELEGATION_BYTES as usize {
                return Err(CoordinationError::Corrupt.into());
            }
            records.push(record);
        }
        drop(rows);
        drop(query);
        db.commit()?;
        Ok(records)
    }
}
#[async_trait::async_trait]
impl agent_storage_ports::RunDelegationRepository for RunStore {
    async fn read_delegation_record(
        &self,
        scope: &TenantContext,
        child: &RunId,
    ) -> Result<Option<DelegationRecord>, RepositoryError> {
        if !valid_run(child) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, child) = (scope.clone(), child.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .delegation_scoped(&scope, &child, None)
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn read_scoped_delegation(
        &self,
        scope: &TenantContext,
        parent: &RunId,
        child: &RunId,
    ) -> Result<Option<DelegationRecord>, RepositoryError> {
        if !valid_run(parent) || !valid_run(child) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, parent, child) = (scope.clone(), parent.clone(), child.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .delegation_scoped(&scope, &child, Some(&parent))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn read_delegation_list(
        &self,
        scope: &TenantContext,
        root: &RunId,
    ) -> Result<Vec<DelegationRecord>, RepositoryError> {
        if !valid_run(root) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, root) = (scope.clone(), root.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .delegation_list_scoped(&scope, &root)
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn read_spawned_delegation(
        &self,
        scope: &TenantContext,
        parent: &RunId,
        spawn: &str,
    ) -> Result<Option<DelegationRecord>, RepositoryError> {
        if !valid_run(parent) || spawn.is_empty() || spawn.len() > 128 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, parent, spawn) = (scope.clone(), parent.clone(), spawn.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .delegation_lookup_scoped(&scope, None, Some((&parent, &spawn)))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn read_session_delegation(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
    ) -> Result<Option<DelegationRecord>, RepositoryError> {
        if key.0.is_empty() || key.0.len() > 1024 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, key) = (scope.clone(), key.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .delegation_lookup_scoped(&scope, Some(&key), None)
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn admit_child_delegation(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        request: &DelegationRequest,
    ) -> Result<DelegationRecord, RepositoryError> {
        if !execution::valid_owner(&envelope.owner)
            || request.parent_session_id != envelope.owner.session_key
            || request.parent_run_id != envelope.owner.run_id
            || request.child_session_id.0.is_empty()
            || request.child_session_id.0.len() > 1024
            || request.context_source_ids.len() > 4
            || request.tools.len() > 128
            || request.tools.iter().any(|name| name != "read_file")
            || request.cwd.len() > 16384
            || request.permission_mode.len() > 64
            || request.max_rounds > 15
            || request.max_tokens > 32000
            || request.max_tool_calls > 32
            || !execution::within_json_budget(request, 128 * 1024)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, request) = (envelope.clone(), request.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .admit_delegation_scoped(request, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn reserve_child_result(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        child: &RunId,
        owner: &str,
        revision: i64,
    ) -> Result<DelegationRecord, RepositoryError> {
        if !execution::valid_owner(&envelope.owner)
            || !valid_run(child)
            || owner.is_empty()
            || owner.len() > 256
            || revision < 0
            || revision > i64::MAX - 2
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, child, owner) = (envelope.clone(), child.clone(), owner.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .reserve_delegation_result_scoped(&child, &owner, revision, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn transition_child_result(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        child: &RunId,
        owner: &str,
        revision: i64,
        target: &str,
    ) -> Result<DelegationRecord, RepositoryError> {
        if !execution::valid_owner(&envelope.owner)
            || !valid_run(child)
            || owner.is_empty()
            || owner.len() > 256
            || revision < 0
            || revision == i64::MAX
            || !matches!(target, "unconsumed" | "delivered")
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, child, owner, target) = (
            envelope.clone(),
            child.clone(),
            owner.to_owned(),
            target.to_owned(),
        );
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .transition_result_scoped(&child, &owner, revision, &target, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
}
