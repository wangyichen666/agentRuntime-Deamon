//! 原 Hook 观察读取与 ACK 的同连接适配，不另存发布状态。
use crate::{RunStore, RuntimeError, enterprise, hooks, sessions};
use agent_core::*;
use rusqlite::{OptionalExtension, params};
const OUTCOME_BYTES: usize = 48 * 1024;
const PAGE_BYTES: usize = 1024 * 1024 - 16 * 1024;
// JSON转义可比原1024字节session身份大六倍，保留原合法键。
const PUBLICATION_BYTES: usize = 16 * 1024;
fn valid_identity(key: &SessionKey, life: &SessionLifetimeId) -> bool {
    !key.0.is_empty() && key.0.len() <= 1024 && !life.0.is_empty() && life.0.len() <= 128
}
fn corrupt(_: impl std::fmt::Display) -> RuntimeError {
    CoordinationError::Corrupt.into()
}
impl RunStore {
    pub(crate) fn hook_readback_scoped(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        life: &SessionLifetimeId,
        after: u64,
        limit: usize,
    ) -> Result<HookReadback, RuntimeError> {
        if !valid_identity(key, life) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let after_sql = i64::try_from(after).map_err(|_| CoordinationError::InvalidRequest)?;
        let limit = limit.clamp(1, 32);
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        enterprise::check_session_scope_in(&tx, scope, key)?;
        sessions::lifetime_owner_in(&tx, key, life)?;
        let revision = SnapshotRevision(tx.query_row(
            "SELECT revision FROM snapshot_clock WHERE id=1",
            [],
            |r| r.get(0),
        )?);
        // 额外一行只读 cursor，不先载入下一页的私密 JSON。
        let count: usize = tx.query_row("SELECT count(*) FROM (SELECT rowid FROM hook_outcomes WHERE session_id=?1 AND lifetime=?2 AND rowid>?3 ORDER BY rowid LIMIT ?4)",params![key.0,life.0,after_sql,limit+1],|r|r.get(0))?;
        let bytes: u64 = tx.query_row("SELECT COALESCE(SUM(length(CAST(operation_id AS BLOB))+length(CAST(outcome_json AS BLOB))),0) FROM (SELECT operation_id,outcome_json FROM hook_outcomes WHERE session_id=?1 AND lifetime=?2 AND rowid>?3 ORDER BY rowid LIMIT ?4)",params![key.0,life.0,after_sql,limit],|r|r.get(0))?;
        if bytes > PAGE_BYTES as u64 {
            return Err(CoordinationError::Corrupt.into());
        }
        let mut q = tx.prepare("SELECT rowid,CASE WHEN typeof(operation_id)='text' AND length(CAST(operation_id AS BLOB)) BETWEEN 1 AND 256 THEN operation_id END,CASE WHEN typeof(outcome_json)='text' AND length(CAST(outcome_json AS BLOB)) BETWEEN 1 AND ?5 THEN outcome_json END FROM hook_outcomes WHERE session_id=?1 AND lifetime=?2 AND rowid>?3 ORDER BY rowid LIMIT ?4")?;
        let mut rows = q.query(params![key.0, life.0, after_sql, limit, OUTCOME_BYTES])?;
        let mut outcomes = Vec::with_capacity(limit);
        let mut cursor = after;
        while let Some(row) = rows.next()? {
            cursor = row.get(0)?;
            let operation: String = row
                .get::<_, Option<String>>(1)?
                .ok_or(CoordinationError::Corrupt)?;
            let raw: String = row
                .get::<_, Option<String>>(2)?
                .ok_or(CoordinationError::Corrupt)?;
            let outcome: HookOutcome = serde_json::from_str(&raw).map_err(corrupt)?;
            hooks::validate_outcome(&outcome).map_err(corrupt)?;
            if outcome.payload.session_key != *key
                || outcome.payload.session_lifetime_id != *life
                || outcome.payload.operation_id != operation
            {
                return Err(CoordinationError::Corrupt.into());
            }
            if let Some(owner) = &outcome.payload.owner {
                let bound = tx.query_row("SELECT 1 FROM runs r JOIN turns t ON t.run_id=r.id WHERE r.id=?1 AND r.session_id=?2 AND r.lifetime=?3 AND r.generation=?4 AND t.id=?5",params![owner.run_id.0,key.0,life.0,owner.run_generation.0,owner.turn_id.0],|r|r.get::<_,u64>(0)).optional()?;
                if bound.is_none() {
                    return Err(CoordinationError::Corrupt.into());
                }
            }
            outcomes.push(outcome);
        }
        drop(rows);
        drop(q);
        tx.commit()?;
        Ok(HookReadback {
            schema_version: 1,
            session_key: key.clone(),
            session_lifetime_id: life.clone(),
            snapshot_revision: revision,
            outcomes,
            cursor,
            has_more: count > limit,
        })
    }
    pub(crate) fn hook_publications_scoped(
        &self,
        scope: &TenantContext,
    ) -> Result<Vec<HookPublication>, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        // 原 publication JSON 是唯一身份来源。SQL 先做字节/语法预算，
        // 再以其有界身份 JOIN 原 scope；外域行不载入 Rust，LIMIT 在范围过滤后。
        let mut q = tx.prepare("WITH bounded AS (SELECT rowid,operation_id,CASE WHEN typeof(payload_json)='text' AND length(CAST(payload_json AS BLOB)) BETWEEN 1 AND ?5 THEN CASE WHEN json_valid(payload_json) THEN payload_json END END AS payload FROM hook_publications WHERE dispatched=0), visible AS (SELECT b.rowid,b.operation_id,b.payload,s.session_id,t.organization_id,s.tenant_id,s.project_id,s.agent_uid FROM bounded b LEFT JOIN session_scopes s ON s.session_id=json_extract(b.payload,'$.session_key') LEFT JOIN tenants t ON t.id=s.tenant_id) SELECT CASE WHEN typeof(operation_id)='text' AND length(CAST(operation_id AS BLOB)) BETWEEN 1 AND 256 THEN operation_id END,CASE WHEN organization_id=?1 AND tenant_id=?2 AND project_id=?3 AND agent_uid=?4 THEN payload END FROM visible WHERE session_id IS NULL OR (organization_id=?1 AND tenant_id=?2 AND project_id=?3 AND agent_uid=?4) ORDER BY rowid LIMIT 64")?;
        let mut rows = q.query(params![
            scope.organization_id.as_str(),
            scope.tenant_id.as_str(),
            scope.project_id.as_str(),
            scope.agent_uid.as_str(),
            PUBLICATION_BYTES
        ])?;
        let mut publications = Vec::new();
        while let Some(row) = rows.next()? {
            let operation: String = row
                .get::<_, Option<String>>(0)?
                .ok_or(CoordinationError::Corrupt)?;
            let raw: String = row
                .get::<_, Option<String>>(1)?
                .ok_or(CoordinationError::Corrupt)?;
            let publication: HookPublication = serde_json::from_str(&raw).map_err(corrupt)?;
            if !valid_identity(&publication.session_key, &publication.session_lifetime_id)
                || publication.operation_id != operation
            {
                return Err(CoordinationError::Corrupt.into());
            }
            sessions::lifetime_owner_in(
                &tx,
                &publication.session_key,
                &publication.session_lifetime_id,
            )?;
            publications.push(publication);
        }
        drop(rows);
        drop(q);
        tx.commit()?;
        Ok(publications)
    }
    pub(crate) fn acknowledge_hook_publication_scoped(
        &self,
        scope: &TenantContext,
        operation: &str,
        expected: Option<&HookPublication>,
    ) -> Result<(), RuntimeError> {
        if operation.is_empty()
            || operation.len() > 256
            || expected.is_some_and(|p| !valid_identity(&p.session_key, &p.session_lifetime_id))
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let mut db = self.lock_connection()?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        if let Some(p) = expected {
            enterprise::check_session_scope_in(&tx, scope, &p.session_key)?;
            sessions::lifetime_owner_in(&tx, &p.session_key, &p.session_lifetime_id)?;
        }
        // 兼容接口只有 operation；只抽取原有界身份，再核范围，不先 decode publication。
        let header: Option<(Option<String>,Option<String>)>=tx.query_row("WITH bounded AS (SELECT CASE WHEN typeof(payload_json)='text' AND length(CAST(payload_json AS BLOB)) BETWEEN 1 AND ?2 THEN CASE WHEN json_valid(payload_json) THEN payload_json END END AS payload FROM hook_publications WHERE operation_id=?1) SELECT CASE WHEN json_type(payload,'$.session_key')='text' THEN json_extract(payload,'$.session_key') END,CASE WHEN json_type(payload,'$.session_lifetime_id')='text' THEN json_extract(payload,'$.session_lifetime_id') END FROM bounded",params![operation,PUBLICATION_BYTES],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        let (key, life) = header.ok_or(CoordinationError::Conflict)?;
        let key = SessionKey(key.ok_or(CoordinationError::Corrupt)?);
        let life = SessionLifetimeId(life.ok_or(CoordinationError::Corrupt)?);
        if !valid_identity(&key, &life) {
            return Err(CoordinationError::Corrupt.into());
        }
        if expected.is_some_and(|p| p.session_key != key || p.session_lifetime_id != life) {
            return Err(CoordinationError::Conflict.into());
        }
        enterprise::check_session_scope_in(&tx, scope, &key)?;
        sessions::lifetime_owner_in(&tx, &key, &life)?;
        let raw: String=tx.query_row("SELECT payload_json FROM hook_publications WHERE operation_id=?1 AND json_extract(payload_json,'$.session_key')=?2 AND json_extract(payload_json,'$.session_lifetime_id')=?3",params![operation,key.0,life.0],|r|r.get(0))?;
        let publication: HookPublication = serde_json::from_str(&raw).map_err(corrupt)?;
        if publication.operation_id != operation {
            return Err(CoordinationError::Corrupt.into());
        }
        if expected.is_some_and(|p| p.event != publication.event) {
            return Err(CoordinationError::Conflict.into());
        }
        crate::writer::fence_current_in(&tx, &key)?;
        tx.execute("UPDATE hook_publications SET dispatched=1 WHERE operation_id=?1 AND json_extract(payload_json,'$.session_key')=?2 AND json_extract(payload_json,'$.session_lifetime_id')=?3",params![operation,key.0,life.0])?;
        crate::writer::before_commit(&tx)?;
        tx.commit()?;
        Ok(())
    }
}
#[async_trait::async_trait]
impl agent_storage_ports::HookObservationRepository for RunStore {
    async fn read_hook_history(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        life: &SessionLifetimeId,
        after: u64,
        limit: usize,
    ) -> Result<HookReadback, RepositoryError> {
        if !valid_identity(key, life) || after > i64::MAX as u64 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, key, life) = (scope.clone(), key.clone(), life.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .hook_readback_scoped(&scope, &key, &life, after, limit)
                .map_err(RuntimeError::repository_error)
        })
        .await
    }
    async fn read_pending_hooks(
        &self,
        scope: &TenantContext,
    ) -> Result<Vec<HookPublication>, RepositoryError> {
        let scope = scope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .hook_publications_scoped(&scope)
                .map_err(RuntimeError::repository_error)
        })
        .await
    }
    async fn acknowledge_pending_hook(
        &self,
        scope: &TenantContext,
        publi: &HookPublication,
    ) -> Result<(), RepositoryError> {
        if !valid_identity(&publi.session_key, &publi.session_lifetime_id)
            || publi.operation_id.is_empty()
            || publi.operation_id.len() > 256
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, publi) = (scope.clone(), publi.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .acknowledge_hook_publication_scoped(&scope, &publi.operation_id, Some(&publi))
                .map_err(RuntimeError::repository_error)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HookRepository, SessionLifecycle};
    use agent_storage_ports::HookObservationRepository;
    #[tokio::test(flavor = "current_thread")]
    async fn hook_observation_ports_yield_on_real_connection_lock_and_cancelled_waiter_keeps_budget()
     {
        for operation in ["history", "pending", "ack"] {
            let store = RunStore::open(std::path::Path::new(":memory:")).unwrap();
            let meta = store
                .create_session(&SessionKey("hook-lock".into()))
                .unwrap();
            let publication = store.hook_publications().unwrap().remove(0);
            let occupied = store.clone();
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
            let holder = std::thread::spawn(move || {
                let db = occupied.lock_connection().unwrap();
                ready_tx.send(()).unwrap();
                let _ = release_rx.recv_timeout(std::time::Duration::from_secs(5));
                drop(db);
            });
            ready_rx.await.unwrap();
            let started = std::time::Instant::now();
            let result = tokio::time::timeout(std::time::Duration::from_millis(25), async {
                let scope = TenantContext::local();
                match operation {
                    "history" => store
                        .read_hook_history(&scope, &meta.key, &meta.lifetime, 0, 32)
                        .await
                        .map(|_| ()),
                    "pending" => store.read_pending_hooks(&scope).await.map(|_| ()),
                    _ => store.acknowledge_pending_hook(&scope, &publication).await,
                }
            })
            .await;
            let elapsed = started.elapsed();
            let permits = store.blocking_budget.available_permits();
            let _ = release_tx.send(());
            holder.join().unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while store.blocking_budget.available_permits() != 4 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(result.is_err(), "仍占用真实Connection {operation}");
            assert!(
                elapsed < std::time::Duration::from_millis(150),
                "SQL阻塞Tokio timer {operation}: {elapsed:?}"
            );
            assert_eq!(
                permits, 3,
                "awaiter取消后worker持permit至真实完成 {operation}"
            );
            let db = store.lock_connection().unwrap();
            let dispatched: u64 = db
                .query_row("SELECT dispatched FROM hook_publications", [], |r| r.get(0))
                .unwrap();
            assert_eq!(dispatched, u64::from(operation == "ack"));
            let outcomes: u64 = db
                .query_row("SELECT count(*) FROM hook_outcomes", [], |r| r.get(0))
                .unwrap();
            assert_eq!(outcomes, 0, "只测观察/ACK不启动外部Hook");
        }
    }
}
