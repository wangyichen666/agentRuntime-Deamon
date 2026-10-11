use super::{RunStore, RuntimeError, sessions::fence_in};
use agent_core::*;
use rusqlite::{Connection, params};
pub trait ResourceRepository: Send + Sync {
    fn reconcile_resource(
        &self,
        owner: &ExactOwner,
        id: ResourceId,
        state: &str,
        evidence: &str,
    ) -> Result<ResourceRecord, RuntimeError>;
    fn create_resource(
        &self,
        owner: &ExactOwner,
        cwd: &str,
        requested: &str,
        effective: &str,
    ) -> Result<ResourceRecord, RuntimeError>;
    fn read_resource(&self, id: ResourceId) -> Result<ResourceRecord, RuntimeError>;
    fn list_resources(
        &self,
        lifetime: &SessionLifetimeId,
        after: ResourceId,
        limit: usize,
    ) -> Result<Vec<ResourceRecord>, RuntimeError>;
    fn update_resource(
        &self,
        owner: &ExactOwner,
        id: ResourceId,
        state: &str,
        identity: Option<&str>,
        reason: Option<&str>,
    ) -> Result<ResourceRecord, RuntimeError>;
    fn append_resource_log(
        &self,
        owner: &ExactOwner,
        id: ResourceId,
        content: &str,
    ) -> Result<u64, RuntimeError>;
    fn resource_logs(
        &self,
        id: ResourceId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<(u64, String)>, RuntimeError>;
}
pub(crate) fn read_in(db: &Connection, id: ResourceId) -> Result<ResourceRecord, RuntimeError> {
    type Raw = (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        u64,
        Option<String>,
        bool,
    );
    let raw:Raw=db.query_row("SELECT CASE WHEN typeof(owner_json)='text' AND length(CAST(owner_json AS BLOB)) BETWEEN 1 AND 8192 THEN owner_json END,CASE WHEN length(CAST(state AS BLOB))<=32 THEN state END,CASE WHEN typeof(cwd)='text' AND length(CAST(cwd AS BLOB))<=16384 THEN cwd END,CASE WHEN length(CAST(requested AS BLOB))<=64 THEN requested END,CASE WHEN length(CAST(effective AS BLOB))<=64 THEN effective END,CASE WHEN length(CAST(process_identity AS BLOB))<=4096 THEN process_identity END,log_cursor,CASE WHEN length(CAST(terminal_reason AS BLOB))<=4096 THEN terminal_reason END,(process_identity IS NULL OR length(CAST(process_identity AS BLOB))<=4096) AND (terminal_reason IS NULL OR length(CAST(terminal_reason AS BLOB))<=4096) FROM resources WHERE id=?1",params![id.0],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?)))?;
    let owner: ExactOwner = serde_json::from_str(&raw.0.ok_or(CoordinationError::Corrupt)?)
        .map_err(|_| CoordinationError::Corrupt)?;
    if !raw.8 || !crate::execution::valid_owner(&owner) {
        return Err(CoordinationError::Corrupt.into());
    }
    let bound:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM resources p JOIN runs r ON r.id=p.run_id JOIN turns t ON t.run_id=r.id JOIN session_lifetime_owners o ON o.session_id=r.session_id AND o.lifetime=r.lifetime AND o.provenance='canonical' WHERE p.id=?1 AND r.id=?2 AND r.session_id=?3 AND r.lifetime=?4 AND p.lifetime=r.lifetime AND r.generation=?5 AND t.id=?6)",params![id.0,owner.run_id.0,owner.session_key.0,owner.session_lifetime_id.0,owner.run_generation.0,owner.turn_id.0],|r|r.get(0))?;
    if !bound {
        return Err(CoordinationError::Corrupt.into());
    }
    let state = raw.1.ok_or(CoordinationError::Corrupt)?;
    if !valid_resource_state(&state) {
        return Err(CoordinationError::Corrupt.into());
    }
    Ok(ResourceRecord {
        id,
        owner,
        state,
        cwd: raw.2.ok_or(CoordinationError::Corrupt)?,
        sandbox_requested: raw.3.ok_or(CoordinationError::Corrupt)?,
        sandbox_effective: raw.4.ok_or(CoordinationError::Corrupt)?,
        process_identity: raw.5,
        log_cursor: raw.6,
        terminal_reason: raw.7,
    })
}

impl ResourceRepository for RunStore {
    fn reconcile_resource(
        &self,
        owner: &ExactOwner,
        id: ResourceId,
        state: &str,
        evidence: &str,
    ) -> Result<ResourceRecord, RuntimeError> {
        self.reconcile_resource_scoped(owner, id, state, evidence, None)
    }
    fn create_resource(
        &self,
        owner: &ExactOwner,
        cwd: &str,
        requested: &str,
        effective: &str,
    ) -> Result<ResourceRecord, RuntimeError> {
        self.create_resource_scoped(owner, cwd, requested, effective, None)
    }
    fn read_resource(&self, id: ResourceId) -> Result<ResourceRecord, RuntimeError> {
        self.read_resource_scoped(&TenantContext::local(), None, id)
    }
    fn list_resources(
        &self,
        lifetime: &SessionLifetimeId,
        after: ResourceId,
        limit: usize,
    ) -> Result<Vec<ResourceRecord>, RuntimeError> {
        self.list_resources_scoped(&TenantContext::local(), None, lifetime, after, limit)
    }
    fn update_resource(
        &self,
        owner: &ExactOwner,
        id: ResourceId,
        state: &str,
        identity: Option<&str>,
        reason: Option<&str>,
    ) -> Result<ResourceRecord, RuntimeError> {
        self.update_resource_scoped(owner, id, state, identity, reason, None)
    }
    fn append_resource_log(
        &self,
        owner: &ExactOwner,
        id: ResourceId,
        content: &str,
    ) -> Result<u64, RuntimeError> {
        self.append_resource_log_scoped(owner, id, content, None)
    }
    fn resource_logs(
        &self,
        id: ResourceId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<(u64, String)>, RuntimeError> {
        self.resource_logs_scoped(&TenantContext::local(), None, id, after, limit)
    }
}

impl RunStore {
    pub(crate) fn create_resource_scoped(
        &self,
        owner: &ExactOwner,
        cwd: &str,
        requested: &str,
        effective: &str,
        envelope: Option<&EnterpriseOwnerEnvelope>,
    ) -> Result<ResourceRecord, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        crate::run_resources::check_resource_execution_in(&tx, owner, envelope)?;
        fence_in(&tx, owner)?;
        if let Some(envelope) = envelope {
            let frozen = crate::requests::frozen_in(&tx, &envelope.owner.run_id)?;
            if frozen.cwd != cwd
                || frozen.sandbox_requested != requested
                || frozen.sandbox_effective != effective
                || effective != "native"
            {
                return Err(CoordinationError::Conflict.into());
            }
        }
        let scope = envelope
            .map(|e| e.scope.clone())
            .unwrap_or_else(TenantContext::local);
        let state: String = tx.query_row(
            "SELECT status FROM runs WHERE id=?1",
            params![owner.run_id.0],
            |r| r.get(0),
        )?;
        if !matches!(state.as_str(), "running" | "waiting_interaction") {
            return Err(RuntimeError::Protocol("资源启动要求 active owner".into()));
        }
        let count: i64 = tx.query_row(
            "SELECT count(*) FROM resources p JOIN runs r ON r.id=p.run_id JOIN session_scopes s ON s.session_id=r.session_id JOIN tenants t ON t.id=s.tenant_id WHERE p.state IN ('starting','running','stopping') AND t.organization_id=?1 AND s.tenant_id=?2 AND s.project_id=?3 AND s.agent_uid=?4",
            params![scope.organization_id.as_str(),scope.tenant_id.as_str(),scope.project_id.as_str(),scope.agent_uid.as_str()],
            |r| r.get(0),
        )?;
        if count >= 16 {
            return Err(RuntimeError::Protocol("后台资源并发预算已满".into()));
        }
        tx.execute("INSERT INTO resources(run_id,lifetime,state,owner_json,cwd,requested,effective,updated_at_ms) VALUES(?1,?2,'starting',?3,?4,?5,?6,?7)",params![owner.run_id.0,owner.session_lifetime_id.0,serde_json::to_string(owner).map_err(|e|RuntimeError::Protocol(e.to_string()))?,cwd,requested,effective,super::now_ms()])?;
        let record = read_in(&tx, ResourceId(tx.last_insert_rowid() as u64))?;
        let env=crate::billing::envelope_in(&tx,&owner.run_id)?;
        crate::billing::allocate_sandbox_in(&tx,&env,&format!("resource:{}",record.id.0))?;
        super::views::commit(tx)?;
        Ok(record)
    }
    pub(crate) fn update_resource_scoped(
        &self,
        owner: &ExactOwner,
        id: ResourceId,
        state: &str,
        identity: Option<&str>,
        reason: Option<&str>,
        envelope: Option<&EnterpriseOwnerEnvelope>,
    ) -> Result<ResourceRecord, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        crate::run_resources::check_resource_execution_in(&tx, owner, envelope)?;
        fence_in(&tx, owner)?;
        crate::run_resources::check_resource_identity_in(
            &tx,
            envelope
                .map(|e| &e.scope)
                .unwrap_or(&TenantContext::local()),
            &owner.session_key,
            &owner.session_lifetime_id,
            id,
        )?;
        let record = read_in(&tx, id)?;
        record
            .owner
            .fence(owner)
            .map_err(|e| RuntimeError::Protocol(e.to_string()))?;
        if !resource_transition_allowed(&record.state, state) {
            return Err(RuntimeError::Protocol("resource state CAS 冲突".into()));
        }
        tx.execute("UPDATE resources SET state=?2,process_identity=COALESCE(?3,process_identity),terminal_reason=?4,updated_at_ms=?5 WHERE id=?1",params![id.0,state,identity,reason,super::now_ms()])?;
        let record = read_in(&tx, id)?;
        if matches!(state,"completed"|"failed"|"stopped"|"orphaned") {
            let env=crate::billing::envelope_in(&tx,&owner.run_id)?;
            crate::billing::finish_sandbox_in(&tx,&env,&format!("resource:{}",id.0),state=="orphaned")?;
            if let Some(run)=crate::read_run_in(&tx,&owner.run_id.0)? {if run.status.terminal(){crate::billing::terminal_in(&tx,&env,run.status)?;}}
        }
        super::insert_event(
            &tx,
            &owner.run_id,
            "resource_updated",
            &serde_json::json!({"resource_id":id,"state":state}),
        )?;
        super::views::commit(tx)?;
        Ok(record)
    }
    pub(crate) fn append_resource_log_scoped(
        &self,
        owner: &ExactOwner,
        id: ResourceId,
        content: &str,
        envelope: Option<&EnterpriseOwnerEnvelope>,
    ) -> Result<u64, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        crate::run_resources::check_resource_execution_in(&tx, owner, envelope)?;
        fence_in(&tx, owner)?;
        crate::run_resources::check_resource_identity_in(
            &tx,
            envelope
                .map(|e| &e.scope)
                .unwrap_or(&TenantContext::local()),
            &owner.session_key,
            &owner.session_lifetime_id,
            id,
        )?;
        let record = read_in(&tx, id)?;
        record
            .owner
            .fence(owner)
            .map_err(|e| RuntimeError::Protocol(e.to_string()))?;
        if !matches!(record.state.as_str(), "running" | "stopping") {
            return Err(RuntimeError::Protocol("resource 已结算".into()));
        }
        let size:u64=tx.query_row("SELECT COALESCE(sum(length(CAST(content AS BLOB))),0) FROM resource_logs WHERE resource_id=?1",params![id.0],|r|r.get(0))?;
        let Some(kept) = bounded_resource_log(content, size)? else {
            return Ok(record.log_cursor);
        };
        let next = record
            .log_cursor
            .checked_add(1)
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or_else(|| RuntimeError::Protocol("log cursor 耗尽".into()))?;
        tx.execute(
            "INSERT INTO resource_logs VALUES(?1,?2,?3)",
            params![id.0, next, kept],
        )?;
        tx.execute(
            "UPDATE resources SET log_cursor=?2 WHERE id=?1",
            params![id.0, next],
        )?;
        super::views::commit(tx)?;
        Ok(next)
    }
    pub(crate) fn reconcile_resource_scoped(
        &self,
        owner: &ExactOwner,
        id: ResourceId,
        state: &str,
        evidence: &str,
        envelope: Option<&EnterpriseOwnerEnvelope>,
    ) -> Result<ResourceRecord, RuntimeError> {
        let reason = resource_reconcile_reason(state, evidence)?;
        let mut db = self.lock_connection()?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        crate::run_resources::check_resource_execution_in(&tx, owner, envelope)?;
        fence_in(&tx, owner)?;
        crate::run_resources::check_resource_identity_in(
            &tx,
            envelope
                .map(|e| &e.scope)
                .unwrap_or(&TenantContext::local()),
            &owner.session_key,
            &owner.session_lifetime_id,
            id,
        )?;
        let record = read_in(&tx, id)?;
        record
            .owner
            .fence(owner)
            .map_err(|e| RuntimeError::Protocol(e.to_string()))?;
        if record.state == state && record.terminal_reason.as_deref() == Some(&reason) {
            return Ok(record);
        }
        if record.state != "orphaned" {
            return Err(RuntimeError::Protocol(
                "只能核对 orphaned resource；存活资源需要 exact stop".into(),
            ));
        }
        tx.execute(
            "UPDATE resources SET state=?2,terminal_reason=?3,updated_at_ms=?4 WHERE id=?1",
            params![id.0, state, reason, super::now_ms()],
        )?;
        super::insert_event(
            &tx,
            &owner.run_id,
            "resource_reconciled",
            &serde_json::json!({"resource_id":id,"state":state,"evidence":evidence,"kill_sent":false}),
        )?;
        let record = read_in(&tx, id)?;
        super::views::commit(tx)?;
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SessionLifecycle, SessionQuery};
    #[test]
    fn resource_log_empty_or_partial_character_does_not_allocate_cursor() {
        let root = std::env::temp_dir().join(format!(
            "resource-log-budget-{}-{}",
            std::process::id(),
            super::super::now_ms()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let store = RunStore::open(&root.join("runtime.sqlite3")).unwrap();
        let key = SessionKey("resource-budget".into());
        store.create_session(&key).unwrap();
        let Admission::New(run) = store
            .admit_with_route(
                key,
                RequestId::Number(1),
                "work",
                AdmissionMode::Queue,
                None,
            )
            .unwrap()
        else {
            panic!("new");
        };
        store.try_start_queued(&run.run_id).unwrap();
        let owner = store.run_owner(&run.run_id).unwrap();
        let resource = store
            .create_resource(&owner, ".", "native", "native")
            .unwrap();
        store
            .update_resource(&owner, resource.id, "running", Some("fixed-workload"), None)
            .unwrap();
        assert_eq!(
            store.append_resource_log(&owner, resource.id, "").unwrap(),
            0
        );
        for _ in 0..3 {
            store
                .append_resource_log(&owner, resource.id, &"x".repeat(16384))
                .unwrap();
        }
        store
            .append_resource_log(&owner, resource.id, &"x".repeat(16383))
            .unwrap();
        assert_eq!(
            store
                .append_resource_log(&owner, resource.id, "你")
                .unwrap(),
            4
        );
        assert_eq!(
            store.append_resource_log(&owner, resource.id, "z").unwrap(),
            5
        );
        assert_eq!(
            store
                .append_resource_log(&owner, resource.id, "满额")
                .unwrap(),
            5
        );
        let logs = store.resource_logs(resource.id, 0, 100).unwrap();
        assert_eq!(logs.len(), 5);
        assert_eq!(
            logs.iter().map(|(_, content)| content.len()).sum::<usize>(),
            65536
        );
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn resources_reconcile_as_orphans_and_late_callbacks_cannot_write_new_lifetime() {
        let dir = std::env::temp_dir().join(format!(
            "resource-restart-{}-{}",
            std::process::id(),
            super::super::now_ms()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("runtime.sqlite3");
        let store = RunStore::open(&path).unwrap();
        let key = SessionKey("resource".into());
        store.create_session(&key).unwrap();
        let Admission::New(run) = store
            .admit_with_route(
                key.clone(),
                RequestId::Number(1),
                "work",
                AdmissionMode::Queue,
                None,
            )
            .unwrap()
        else {
            panic!("new")
        };
        store.try_start_queued(&run.run_id).unwrap();
        let owner = store.run_owner(&run.run_id).unwrap();
        let resource = store
            .create_resource(&owner, "/workspace", "native", "native")
            .unwrap();
        store
            .update_resource(
                &owner,
                resource.id,
                "running",
                Some("unverifiable:pid-1"),
                None,
            )
            .unwrap();
        store
            .append_resource_log(&owner, resource.id, "ready")
            .unwrap();
        store
            .finish(&run.run_id, RunStatus::Completed, Some("done"), None)
            .unwrap();
        assert!(
            store
                .end_session(&key, &owner.session_lifetime_id, true)
                .is_err()
        );
        // 打开额外 repository 不等于 daemon 重启，不得抢先结算活动资源。
        let extra = RunStore::open(&path).unwrap();
        assert_eq!(extra.read_resource(resource.id).unwrap().state, "running");
        drop(extra);
        drop(store);
        let store = RunStore::open(&path).unwrap();
        store.recover().unwrap();
        assert_eq!(store.read_resource(resource.id).unwrap().state, "orphaned");
        assert!(
            store
                .reconcile_resource(&owner, resource.id, "completed", "")
                .is_err()
        );
        let reconciled = store
            .reconcile_resource(&owner, resource.id, "stopped", "人工确认原进程已经退出")
            .unwrap();
        assert_eq!(reconciled.state, "stopped");
        assert_eq!(
            store
                .reconcile_resource(&owner, resource.id, "stopped", "人工确认原进程已经退出")
                .unwrap()
                .state,
            "stopped"
        );
        store
            .end_session(&key, &owner.session_lifetime_id, true)
            .unwrap();
        store.create_session(&key).unwrap();
        assert!(
            store
                .append_resource_log(&owner, resource.id, "late")
                .is_err()
        );
        assert!(
            store
                .update_resource(&owner, resource.id, "completed", None, None)
                .is_err()
        );
        assert!(
            store
                .reconcile_resource(&owner, resource.id, "stopped", "人工确认原进程已经退出")
                .is_err()
        );
        assert!(store.session_snapshot(&key).unwrap().messages.is_empty());
        assert_eq!(
            store.resource_logs(resource.id, 0, 20).unwrap(),
            vec![(1, "ready".into())]
        );
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
