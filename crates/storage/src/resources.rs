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
fn read_in(db: &Connection, id: ResourceId) -> Result<ResourceRecord, RuntimeError> {
    let raw:(String,String,String,String,String,Option<String>,u64,Option<String>)=db.query_row("SELECT owner_json,state,cwd,requested,effective,process_identity,log_cursor,terminal_reason FROM resources WHERE id=?1",params![id.0],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?)))?;
    Ok(ResourceRecord {
        id,
        owner: serde_json::from_str(&raw.0).map_err(|e| RuntimeError::Protocol(e.to_string()))?,
        state: raw.1,
        cwd: raw.2,
        sandbox_requested: raw.3,
        sandbox_effective: raw.4,
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
        if !matches!(state, "completed" | "failed" | "stopped")
            || evidence.trim().is_empty()
            || evidence.len() > 4096
        {
            return Err(RuntimeError::Protocol(
                "resource reconcile 要求终态和有界人工证据".into(),
            ));
        }
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        fence_in(&tx, owner)?;
        let record = read_in(&tx, id)?;
        record
            .owner
            .fence(owner)
            .map_err(|e| RuntimeError::Protocol(e.to_string()))?;
        let reason = format!("manual reconcile: {evidence}");
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
        tx.commit()?;
        Ok(record)
    }
    fn create_resource(
        &self,
        owner: &ExactOwner,
        cwd: &str,
        requested: &str,
        effective: &str,
    ) -> Result<ResourceRecord, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        fence_in(&tx, owner)?;
        let state: String = tx.query_row(
            "SELECT status FROM runs WHERE id=?1",
            params![owner.run_id.0],
            |r| r.get(0),
        )?;
        if !matches!(state.as_str(), "running" | "waiting_interaction") {
            return Err(RuntimeError::Protocol("资源启动要求 active owner".into()));
        }
        let count: i64 = tx.query_row(
            "SELECT count(*) FROM resources WHERE state IN ('starting','running','stopping')",
            [],
            |r| r.get(0),
        )?;
        if count >= 16 {
            return Err(RuntimeError::Protocol("后台资源并发预算已满".into()));
        }
        tx.execute("INSERT INTO resources(run_id,lifetime,state,owner_json,cwd,requested,effective,updated_at_ms) VALUES(?1,?2,'starting',?3,?4,?5,?6,?7)",params![owner.run_id.0,owner.session_lifetime_id.0,serde_json::to_string(owner).map_err(|e|RuntimeError::Protocol(e.to_string()))?,cwd,requested,effective,super::now_ms()])?;
        let record = read_in(&tx, ResourceId(tx.last_insert_rowid() as u64))?;
        tx.commit()?;
        Ok(record)
    }
    fn read_resource(&self, id: ResourceId) -> Result<ResourceRecord, RuntimeError> {
        read_in(&*self.lock_connection()?, id)
    }
    fn list_resources(
        &self,
        lifetime: &SessionLifetimeId,
        after: ResourceId,
        limit: usize,
    ) -> Result<Vec<ResourceRecord>, RuntimeError> {
        let db = self.lock_connection()?;
        let mut q = db
            .prepare("SELECT id FROM resources WHERE lifetime=?1 AND id>?2 ORDER BY id LIMIT ?3")?;
        let ids = q
            .query_map(params![lifetime.0, after.0, limit.clamp(1, 100)], |r| {
                r.get::<_, u64>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|id| read_in(&db, ResourceId(id)))
            .collect()
    }
    fn update_resource(
        &self,
        owner: &ExactOwner,
        id: ResourceId,
        state: &str,
        identity: Option<&str>,
        reason: Option<&str>,
    ) -> Result<ResourceRecord, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        fence_in(&tx, owner)?;
        let record = read_in(&tx, id)?;
        record
            .owner
            .fence(owner)
            .map_err(|e| RuntimeError::Protocol(e.to_string()))?;
        let allowed = match record.state.as_str() {
            "starting" => matches!(state, "running" | "failed" | "orphaned"),
            "running" => matches!(
                state,
                "stopping" | "completed" | "failed" | "stopped" | "orphaned"
            ),
            "stopping" => matches!(state, "stopped" | "completed" | "failed" | "orphaned"),
            _ => state == record.state,
        };
        if !allowed {
            return Err(RuntimeError::Protocol("resource state CAS 冲突".into()));
        }
        tx.execute("UPDATE resources SET state=?2,process_identity=COALESCE(?3,process_identity),terminal_reason=?4,updated_at_ms=?5 WHERE id=?1",params![id.0,state,identity,reason,super::now_ms()])?;
        let record = read_in(&tx, id)?;
        super::insert_event(
            &tx,
            &owner.run_id,
            "resource_updated",
            &serde_json::json!({"resource_id":id,"state":state}),
        )?;
        tx.commit()?;
        Ok(record)
    }
    fn append_resource_log(
        &self,
        owner: &ExactOwner,
        id: ResourceId,
        content: &str,
    ) -> Result<u64, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        fence_in(&tx, owner)?;
        let record = read_in(&tx, id)?;
        record
            .owner
            .fence(owner)
            .map_err(|e| RuntimeError::Protocol(e.to_string()))?;
        if !matches!(record.state.as_str(), "running" | "stopping") {
            return Err(RuntimeError::Protocol("resource 已结算".into()));
        }
        let size:u64=tx.query_row("SELECT COALESCE(sum(length(CAST(content AS BLOB))),0) FROM resource_logs WHERE resource_id=?1",params![id.0],|r|r.get(0))?;
        if size >= 65536 {
            return Ok(record.log_cursor);
        }
        let mut kept = String::new();
        for c in content.chars() {
            if kept.len() + c.len_utf8() > (65536 - size) as usize {
                break;
            }
            kept.push(c);
        }
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
        tx.commit()?;
        Ok(next)
    }
    fn resource_logs(
        &self,
        id: ResourceId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<(u64, String)>, RuntimeError> {
        let db = self.lock_connection()?;
        let mut q=db.prepare("SELECT cursor,content FROM resource_logs WHERE resource_id=?1 AND cursor>?2 ORDER BY cursor LIMIT ?3")?;
        Ok(q.query_map(params![id.0, after, limit.clamp(1, 100)], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SessionLifecycle, SessionQuery};
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
