//! canonical session writer lease；TEMP 只缓存本连接取得的 proof，不保存业务状态。
use crate::{RunStore, RuntimeError, enterprise, sessions};
use agent_core::*;
use agent_storage_ports::SessionWriterRepository;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
const TTL: i64 = 15_000;
fn unavailable(_: rusqlite::Error) -> CoordinationError {
    CoordinationError::StorageUnavailable
}
fn mapped(error: RuntimeError) -> CoordinationError {
    match error {
        RuntimeError::Coordination(e) => e,
        RuntimeError::Authorization(_) | RuntimeError::Protocol(_) => CoordinationError::Forbidden,
        _ => CoordinationError::StorageUnavailable,
    }
}
fn now(db: &Connection) -> Result<i64, CoordinationError> {
    db.query_row(
        "SELECT CAST((julianday('now')-2440587.5)*86400000 AS INTEGER)",
        [],
        |r| r.get(0),
    )
    .map_err(unavailable)
}
pub(crate) fn initialize(db: &Connection) -> Result<(), RuntimeError> {
    db.execute_batch("CREATE TEMP TABLE writer_context(owner_id TEXT,ttl_ms INTEGER NOT NULL);INSERT INTO writer_context VALUES(NULL,15000);CREATE TEMP TABLE writer_grants(session_key TEXT NOT NULL,lifetime TEXT NOT NULL,lease_json TEXT NOT NULL,PRIMARY KEY(session_key,lifetime));")?;
    Ok(())
}
fn key(
    db: &Connection,
    session: &SessionKey,
    lifetime: &SessionLifetimeId,
) -> Result<SessionWriterLeaseKey, CoordinationError> {
    let row:(String,String,String,String)=db.query_row("SELECT t.organization_id,s.tenant_id,s.project_id,s.agent_uid FROM session_scopes s JOIN tenants t ON t.id=s.tenant_id WHERE s.session_id=?1",[&session.0],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).map_err(unavailable)?;
    let scope = TenantContext {
        organization_id: row.0.try_into().map_err(|_| CoordinationError::Corrupt)?,
        tenant_id: row.1.try_into().map_err(|_| CoordinationError::Corrupt)?,
        project_id: row.2.try_into().map_err(|_| CoordinationError::Corrupt)?,
        agent_uid: row.3.try_into().map_err(|_| CoordinationError::Corrupt)?,
    };
    Ok(SessionWriterLeaseKey {
        scope,
        session_key: session.clone(),
        session_lifetime_id: lifetime.clone(),
    })
}
fn domain(db: &Connection, key: &SessionWriterLeaseKey) -> Result<(), CoordinationError> {
    enterprise::check_session_scope_in(db, &key.scope, &key.session_key).map_err(mapped)?;
    let meta = sessions::metadata_in(db, &key.session_key)
        .map_err(mapped)?
        .ok_or(CoordinationError::Forbidden)?;
    if meta.lifetime != key.session_lifetime_id {
        return Err(CoordinationError::StaleLease);
    }
    Ok(())
}
fn current(
    db: &Connection,
    key: &SessionWriterLeaseKey,
) -> Result<Option<SessionWriterLease>, CoordinationError> {
    let scope = &key.scope;
    let row:Option<(String,u64,i64)>=db.query_row("SELECT owner_id,epoch,expires_at_ms FROM session_writer_leases WHERE organization_id=?1 AND tenant_id=?2 AND project_id=?3 AND agent_uid=?4 AND session_key=?5 AND lifetime=?6",params![scope.organization_id.as_str(),scope.tenant_id.as_str(),scope.project_id.as_str(),scope.agent_uid.as_str(),key.session_key.0,key.session_lifetime_id.0],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional().map_err(unavailable)?;
    Ok(row.map(|row| SessionWriterLease {
        key: key.clone(),
        owner_id: row.0,
        epoch: row.1,
        expires_at_ms: row.2,
    }))
}
fn save(db: &Connection, lease: &SessionWriterLease) -> Result<(), CoordinationError> {
    let scope = &lease.key.scope;
    db.execute("INSERT INTO session_writer_leases(organization_id,tenant_id,project_id,agent_uid,session_key,lifetime,owner_id,epoch,expires_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9) ON CONFLICT(organization_id,tenant_id,project_id,agent_uid,session_key,lifetime) DO UPDATE SET owner_id=excluded.owner_id,epoch=excluded.epoch,expires_at_ms=excluded.expires_at_ms",params![scope.organization_id.as_str(),scope.tenant_id.as_str(),scope.project_id.as_str(),scope.agent_uid.as_str(),lease.key.session_key.0,lease.key.session_lifetime_id.0,lease.owner_id,lease.epoch as i64,lease.expires_at_ms]).map_err(unavailable)?;
    Ok(())
}
fn claim(
    db: &Connection,
    key: &SessionWriterLeaseKey,
    owner: &str,
    ttl: i64,
) -> Result<SessionWriterLease, CoordinationError> {
    domain(db, key)?;
    let prior = current(db, key)?;
    let lease = SessionWriterLease::acquire(prior.as_ref(), key, owner, now(db)?, ttl)?;
    save(db, &lease)?;
    Ok(lease)
}
fn renew(
    db: &Connection,
    lease: &SessionWriterLease,
    ttl: i64,
) -> Result<SessionWriterLease, CoordinationError> {
    domain(db, &lease.key)?;
    let prior = current(db, &lease.key)?.ok_or(CoordinationError::StaleLease)?;
    let time = now(db)?;
    prior.fence(lease, time)?;
    let next = SessionWriterLease::acquire(Some(&prior), &lease.key, &lease.owner_id, time, ttl)?;
    save(db, &next)?;
    Ok(next)
}
fn grant(
    db: &Connection,
    key: &SessionWriterLeaseKey,
) -> Result<Option<SessionWriterLease>, CoordinationError> {
    let raw: Option<String> = db
        .query_row(
            "SELECT lease_json FROM temp.writer_grants WHERE session_key=?1 AND lifetime=?2",
            params![key.session_key.0, key.session_lifetime_id.0],
            |r| r.get(0),
        )
        .optional()
        .map_err(unavailable)?;
    raw.map(|raw| serde_json::from_str(&raw).map_err(|_| CoordinationError::Corrupt))
        .transpose()
}
fn remember(db: &Connection, lease: &SessionWriterLease) -> Result<(), CoordinationError> {
    db.execute(
        "DELETE FROM temp.writer_grants WHERE session_key=?1 AND lifetime<>?2",
        params![lease.key.session_key.0, lease.key.session_lifetime_id.0],
    )
    .map_err(unavailable)?;
    let count: usize = db
        .query_row("SELECT count(*) FROM temp.writer_grants", [], |r| r.get(0))
        .map_err(unavailable)?;
    if count >= 1024 && grant(db, &lease.key)?.is_none() {
        return Err(CoordinationError::InvalidRequest);
    }
    let raw = serde_json::to_string(lease).map_err(|_| CoordinationError::Corrupt)?;
    db.execute("INSERT INTO temp.writer_grants VALUES(?1,?2,?3) ON CONFLICT(session_key,lifetime) DO UPDATE SET lease_json=excluded.lease_json",params![lease.key.session_key.0,lease.key.session_lifetime_id.0,raw]).map_err(unavailable)?;
    Ok(())
}
/// 已取得的过期 proof 不会在原进程中自动 acquire 新 epoch。
pub(crate) fn acquire_in(
    db: &Connection,
    session: &SessionKey,
    lifetime: &SessionLifetimeId,
) -> Result<(), RuntimeError> {
    let key = key(db, session, lifetime)?;
    if let Some(proof) = grant(db, &key)? {
        current(db, &key)?
            .ok_or(CoordinationError::StaleLease)?
            .fence(&proof, now(db)?)?;
        return Ok(());
    }
    let (owner, ttl): (Option<String>, i64) =
        db.query_row("SELECT owner_id,ttl_ms FROM temp.writer_context", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })?;
    match owner {
        Some(owner) => {
            let proof = claim(db, &key, &owner, ttl)?;
            remember(db, &proof)?;
        }
        None => {
            let has_lease: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM session_writer_leases)",
                [],
                |r| r.get(0),
            )?;
            if has_lease {
                return Err(CoordinationError::StaleLease.into());
            }
        }
    }
    Ok(())
}
/// 只核验，不 acquire/renew；供已有 ExactOwner 的原读写 fence 使用。
pub(crate) fn fence_in(
    db: &Connection,
    session: &SessionKey,
    lifetime: &SessionLifetimeId,
) -> Result<(), RuntimeError> {
    let key = key(db, session, lifetime)?;
    let current = current(db, &key)?;
    if let Some(current) = current {
        let proof = grant(db, &key)?.ok_or(CoordinationError::StaleLease)?;
        current.fence(&proof, now(db)?)?;
    } else {
        let enabled: bool = db.query_row(
            "SELECT owner_id IS NOT NULL FROM temp.writer_context",
            [],
            |r| r.get(0),
        )?;
        let protected: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM session_writer_leases)",
            [],
            |r| r.get(0),
        )?;
        if enabled || protected {
            return Err(CoordinationError::StaleLease.into());
        }
    }
    Ok(())
}
pub(crate) fn fence_current_in(db: &Connection, session: &SessionKey) -> Result<(), RuntimeError> {
    let meta = sessions::metadata_in(db, session)?.ok_or(CoordinationError::Forbidden)?;
    fence_in(db, session, &meta.lifetime)
}
pub(crate) fn health_scoped_in(
    db: &Connection,
    scope: &TenantContext,
) -> Result<serde_json::Value, RuntimeError> {
    let enabled: bool = db.query_row(
        "SELECT owner_id IS NOT NULL FROM temp.writer_context",
        [],
        |r| r.get(0),
    )?;
    let time = now(db)?;
    let (persisted, expired, max_epoch): (u64,u64,u64) = db.query_row("SELECT count(*),COALESCE(sum(l.expires_at_ms<=?1),0),COALESCE(max(l.epoch),0) FROM session_writer_leases l JOIN tenants t ON t.id=l.tenant_id WHERE t.organization_id=?2 AND l.tenant_id=?3 AND l.project_id=?4 AND l.agent_uid=?5", params![time,scope.organization_id.as_str(),scope.tenant_id.as_str(),scope.project_id.as_str(),scope.agent_uid.as_str()], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    let mut q=db.prepare("SELECT CASE WHEN length(CAST(g.lease_json AS BLOB))<=16384 THEN g.lease_json END FROM temp.writer_grants g JOIN session_heads h ON h.session_id=g.session_key AND h.lifetime=g.lifetime JOIN session_scopes s ON s.session_id=h.session_id JOIN tenants t ON t.id=s.tenant_id WHERE t.organization_id=?1 AND s.tenant_id=?2 AND s.project_id=?3 AND s.agent_uid=?4 LIMIT 1025")?;
    let raws = q
        .query_map(
            params![
                scope.organization_id.as_str(),
                scope.tenant_id.as_str(),
                scope.project_id.as_str(),
                scope.agent_uid.as_str()
            ],
            |r| r.get::<_, Option<String>>(0),
        )?
        .collect::<Result<Vec<_>, _>>()?;
    drop(q);
    if raws.len() > 1024 {
        return Err(CoordinationError::InvalidRequest.into());
    }
    let mut held = 0;
    for raw in &raws {
        let raw = raw.as_ref().ok_or(CoordinationError::Corrupt)?;
        let proof: SessionWriterLease =
            serde_json::from_str(raw).map_err(|_| CoordinationError::Corrupt)?;
        if &proof.key.scope != scope {
            return Err(CoordinationError::Corrupt.into());
        }
        if current(db, &proof.key)?.is_some_and(|lease| lease.fence(&proof, time).is_ok()) {
            held += 1;
        }
    }
    Ok(
        serde_json::json!({"enabled":enabled,"persisted":persisted,"expired":expired,"max_epoch":max_epoch,"held":held,"stale_bound":raws.len()-held,"database_time_ms":time,"ttl_ms":TTL,"renew_interval_ms":3000,"bound_limit":1024,"backend":"sqlite","recovery_scope":"local_workspace"}),
    )
}
pub(crate) fn before_commit(db: &Connection) -> Result<(), RuntimeError> {
    let enabled: bool = db.query_row(
        "SELECT owner_id IS NOT NULL FROM temp.writer_context",
        [],
        |r| r.get(0),
    )?;
    let protected: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_writer_leases)",
        [],
        |r| r.get(0),
    )?;
    if !enabled && protected {
        return Err(CoordinationError::StaleLease.into());
    }
    let mut query=db.prepare("SELECT g.lease_json FROM temp.writer_grants g JOIN session_heads s ON s.session_id=g.session_key AND s.lifetime=g.lifetime")?;
    let rows = query
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    drop(query);
    let mut earliest = i64::MAX;
    for raw in rows {
        let proof: SessionWriterLease =
            serde_json::from_str(&raw).map_err(|_| CoordinationError::Corrupt)?;
        domain(db, &proof.key)?;
        let lease = current(db, &proof.key)?.ok_or(CoordinationError::StaleLease)?;
        lease.fence(&proof, now(db)?)?;
        earliest = earliest.min(lease.expires_at_ms);
    }
    if earliest <= now(db)? {
        return Err(CoordinationError::StaleLease.into());
    }
    Ok(())
}
fn verify_activation_in(db: &Connection, scope: &TenantContext) -> Result<(), RuntimeError> {
    // 未受租约保护的活执行体不能靠打开开关被当成崩溃恢复。
    let unsafe_activation:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM session_heads h JOIN session_scopes s ON s.session_id=h.session_id JOIN tenants t ON t.id=s.tenant_id WHERE t.organization_id=?1 AND s.tenant_id=?2 AND s.project_id=?3 AND s.agent_uid=?4 AND (EXISTS(SELECT 1 FROM runs r WHERE r.session_id=h.session_id AND r.status IN ('running','waiting_interaction')) OR EXISTS(SELECT 1 FROM resources x JOIN runs r ON r.id=x.run_id WHERE r.session_id=h.session_id AND x.state IN ('starting','running','stopping')) OR EXISTS(SELECT 1 FROM hook_outcomes o WHERE o.session_id=h.session_id AND CASE WHEN json_valid(o.outcome_json) AND length(CAST(o.outcome_json AS BLOB))<=49152 THEN json_extract(o.outcome_json,'$.status') ELSE 'running' END='running')) AND NOT EXISTS(SELECT 1 FROM session_writer_leases l JOIN session_scopes s ON s.session_id=l.session_key AND s.tenant_id=l.tenant_id AND s.project_id=l.project_id AND s.agent_uid=l.agent_uid WHERE l.session_key=h.session_id AND l.lifetime=h.lifetime))",params![scope.organization_id.as_str(),scope.tenant_id.as_str(),scope.project_id.as_str(),scope.agent_uid.as_str()],|r|r.get(0))?;
    if unsafe_activation {
        return Err(CoordinationError::Conflict.into());
    }
    Ok(())
}
pub(crate) fn protect_recovery(db: &Connection, scope: &TenantContext) -> Result<(), RuntimeError> {
    let enabled: bool = db.query_row(
        "SELECT owner_id IS NOT NULL FROM temp.writer_context",
        [],
        |r| r.get(0),
    )?;
    if !enabled {
        let has_lease: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM session_writer_leases l JOIN tenants t ON t.id=l.tenant_id WHERE t.organization_id=?1 AND l.tenant_id=?2 AND l.project_id=?3 AND l.agent_uid=?4)",
            params![scope.organization_id.as_str(),scope.tenant_id.as_str(),scope.project_id.as_str(),scope.agent_uid.as_str()],
            |r| r.get(0),
        )?;
        if has_lease {
            return Err(CoordinationError::StaleLease.into());
        }
        return Ok(());
    }
    verify_activation_in(db, scope)?;
    let mut query =
        db.prepare("SELECT h.session_id,h.lifetime FROM session_heads h JOIN session_scopes s ON s.session_id=h.session_id JOIN tenants t ON t.id=s.tenant_id WHERE t.organization_id=?1 AND s.tenant_id=?2 AND s.project_id=?3 AND s.agent_uid=?4 ORDER BY h.session_id LIMIT 1025")?;
    let keys = query
        .query_map(
            params![
                scope.organization_id.as_str(),
                scope.tenant_id.as_str(),
                scope.project_id.as_str(),
                scope.agent_uid.as_str()
            ],
            |r| Ok((SessionKey(r.get(0)?), SessionLifetimeId(r.get(1)?))),
        )?
        .collect::<Result<Vec<_>, _>>()?;
    drop(query);
    if enabled && keys.len() > 1024 {
        return Err(CoordinationError::InvalidRequest.into());
    }
    // 未启用时也检查持久租约，不能绕过另一个实例已建立的 writer。
    for (session, lifetime) in keys {
        acquire_in(db, &session, &lifetime)?;
    }
    Ok(())
}
impl RunStore {
    pub fn enable_session_writer(self) -> Result<Self, RuntimeError> {
        {
            let db = self.lock_connection()?;
            verify_activation_in(&db, &TenantContext::local())?;
            let owner: String =
                db.query_row("SELECT 'daemon-'||lower(hex(randomblob(16)))", [], |r| {
                    r.get(0)
                })?;
            db.execute(
                "UPDATE temp.writer_context SET owner_id=?1,ttl_ms=?2",
                params![owner, TTL],
            )?;
        }
        Ok(self)
    }
    fn acquire_writer_sync(
        &self,
        key: &SessionWriterLeaseKey,
        owner: &str,
        ttl: i64,
    ) -> Result<SessionWriterLease, CoordinationError> {
        let mut db = self.lock_connection().map_err(mapped)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(unavailable)?;
        let lease = claim(&tx, key, owner, ttl)?;
        tx.commit().map_err(unavailable)?;
        Ok(lease)
    }
    fn renew_writer_sync(
        &self,
        lease: &SessionWriterLease,
        ttl: i64,
    ) -> Result<SessionWriterLease, CoordinationError> {
        let mut db = self.lock_connection().map_err(mapped)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(unavailable)?;
        let lease = renew(&tx, lease, ttl)?;
        tx.commit().map_err(unavailable)?;
        Ok(lease)
    }
    fn release_writer_sync(&self, lease: &SessionWriterLease) -> Result<(), CoordinationError> {
        let mut db = self.lock_connection().map_err(mapped)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(unavailable)?;
        let prior = current(&tx, &lease.key)?.ok_or(CoordinationError::StaleLease)?;
        let time = now(&tx)?;
        prior.fence(lease, time)?;
        let mut released = prior;
        released.expires_at_ms = time;
        save(&tx, &released)?;
        tx.commit().map_err(unavailable)?;
        Ok(())
    }
    fn renew_bound_sync(&self) -> Result<usize, CoordinationError> {
        let mut db = self.lock_connection().map_err(mapped)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(unavailable)?;
        let mut query=tx.prepare("SELECT g.lease_json FROM temp.writer_grants g JOIN session_heads s ON s.session_id=g.session_key AND s.lifetime=g.lifetime LIMIT 1025").map_err(unavailable)?;
        let rows = query
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(unavailable)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(unavailable)?;
        drop(query);
        if rows.len() > 1024 {
            return Err(CoordinationError::InvalidRequest);
        }
        for raw in &rows {
            let lease: SessionWriterLease =
                serde_json::from_str(raw).map_err(|_| CoordinationError::Corrupt)?;
            let lease = renew(&tx, &lease, TTL)?;
            remember(&tx, &lease)?;
        }
        tx.commit().map_err(unavailable)?;
        Ok(rows.len())
    }
    fn release_bound_sync(&self) -> Result<usize, CoordinationError> {
        let mut db = self.lock_connection().map_err(mapped)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(unavailable)?;
        let mut q=tx.prepare("SELECT g.lease_json FROM temp.writer_grants g JOIN session_heads s ON s.session_id=g.session_key AND s.lifetime=g.lifetime LIMIT 1025").map_err(unavailable)?;
        let raws = q
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(unavailable)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(unavailable)?;
        drop(q);
        if raws.len() > 1024 {
            return Err(CoordinationError::InvalidRequest);
        }
        let time = now(&tx)?;
        for raw in &raws {
            let proof: SessionWriterLease =
                serde_json::from_str(raw).map_err(|_| CoordinationError::Corrupt)?;
            let mut lease = current(&tx, &proof.key)?.ok_or(CoordinationError::StaleLease)?;
            lease.fence(&proof, time)?;
            lease.expires_at_ms = time;
            save(&tx, &lease)?;
        }
        // 不移除 proof：退出阶段的迟到 callback 必须得到 Stale，而不能重新 acquire。
        tx.commit().map_err(unavailable)?;
        Ok(raws.len())
    }
    async fn writer_blocking<T: Send + 'static>(
        &self,
        operation: impl FnOnce(Self) -> Result<T, CoordinationError> + Send + 'static,
    ) -> Result<T, CoordinationError> {
        self.sqlite_blocking(CoordinationError::StorageUnavailable, operation)
            .await
    }
}
#[async_trait::async_trait]
impl SessionWriterRepository for RunStore {
    async fn acquire_writer_lease(
        &self,
        key: &SessionWriterLeaseKey,
        owner: &str,
        ttl: i64,
    ) -> Result<SessionWriterLease, CoordinationError> {
        let key = key.clone();
        let owner = owner.to_owned();
        self.writer_blocking(move |store| store.acquire_writer_sync(&key, &owner, ttl))
            .await
    }
    async fn renew_writer_lease(
        &self,
        lease: &SessionWriterLease,
        ttl: i64,
    ) -> Result<SessionWriterLease, CoordinationError> {
        let lease = lease.clone();
        self.writer_blocking(move |store| store.renew_writer_sync(&lease, ttl))
            .await
    }
    async fn release_writer_lease(
        &self,
        lease: &SessionWriterLease,
    ) -> Result<(), CoordinationError> {
        let lease = lease.clone();
        self.writer_blocking(move |store| store.release_writer_sync(&lease))
            .await
    }
    async fn read_writer_lease(
        &self,
        key: &SessionWriterLeaseKey,
    ) -> Result<Option<SessionWriterLease>, CoordinationError> {
        let key = key.clone();
        self.writer_blocking(move |store| {
            let db = store.lock_connection().map_err(mapped)?;
            domain(&db, &key)?;
            current(&db, &key)
        })
        .await
    }
    async fn renew_bound_writers(&self) -> Result<usize, CoordinationError> {
        self.writer_blocking(|store| store.renew_bound_sync()).await
    }
    async fn release_bound_writers(&self) -> Result<usize, CoordinationError> {
        self.writer_blocking(|store| store.release_bound_sync())
            .await
    }
}
