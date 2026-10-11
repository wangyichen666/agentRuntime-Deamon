//! canonical PostgreSQL事实源与native writer；租约proof只在内存持有句柄，不保存业务状态。
use crate::{
    KEY, PgPublicationStore, SCOPE, identifier, now, parameters, scope_values, unavailable,
};
use agent_core::*;
use agent_storage_ports::{SessionWriterRepository, StorageWorkDrainRepository};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use tokio::sync::Mutex;
use tokio_postgres::{GenericClient, Row, Transaction};

const SCHEMA: &str = include_str!("canonical_schema.sql");
const MIGRATION_V2: &str = include_str!("canonical_v2.sql");
const MIGRATION_V3: &str = include_str!("canonical_v3.sql");
const MIGRATION_V4: &str = include_str!("canonical_v4.sql");
const MIGRATION_V5: &str = include_str!("canonical_v5.sql");
const MIGRATION_V6: &str = include_str!("canonical_v6.sql");
const MIGRATION_V7: &str = include_str!("canonical_v7.sql");
const MIGRATION_V8:&str=include_str!("canonical_v8.sql");
const MIGRATION_V9:&str=include_str!("canonical_v9.sql");
const MIGRATION_V10:&str=include_str!("canonical_v10.sql");
const VERSION: i64 = 10;
pub struct PgCanonicalStore {
    pub(crate) inner: std::sync::Arc<PgPublicationStore>,
    pub(crate) maintenance_tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    pub(crate) proofs: Mutex<BTreeMap<String, SessionWriterLease>>,
    pub(crate) writer_owner: String,
    pub(crate) local_files: Option<std::sync::Arc<agent_artifact::FileArtifactDirectory>>,
    pub(crate) credential_budget: std::sync::Arc<tokio::sync::Semaphore>,
    pub(crate) file_budget: std::sync::Arc<tokio::sync::Semaphore>,
}

pub(crate) async fn set_scope(
    db: &Transaction<'_>,
    scope: &TenantContext,
) -> Result<(), CoordinationError> {
    // 离线回滚与所有native领域事务共用schema栅栏；回滚持独占锁直到marker提交。
    db.query_one(
        "SELECT pg_advisory_xact_lock_shared(hashtextextended(current_schema(),55))",
        &[],
    )
    .await
    .map_err(unavailable)?;
    let table: Option<String> = db
        .query_one(
            "SELECT to_regclass('offline_migration_receipts')::text",
            &[],
        )
        .await
        .map_err(unavailable)?
        .try_get(0)
        .map_err(unavailable)?;
    if table.is_some() {
        let closed: bool = db
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM offline_migration_receipts WHERE state='rolled_back')",
                &[],
            )
            .await
            .map_err(unavailable)?
            .try_get(0)
            .map_err(unavailable)?;
        if closed {
            return Err(CoordinationError::Conflict);
        }
    }
    let values = scope_values(scope);
    db.query_one("SELECT set_config('agent.organization_id',$1,true),set_config('agent.tenant_id',$2,true),set_config('agent.project_id',$3,true),set_config('agent.agent_uid',$4,true)", &parameters(&values)).await.map_err(unavailable)?;
    db.batch_execute("SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='1s'; SET LOCAL idle_in_transaction_session_timeout='10s'").await.map_err(unavailable)
}
pub(crate) fn key_values(key: &SessionWriterLeaseKey) -> Vec<String> {
    let mut values = scope_values(&key.scope);
    values.extend([key.session_key.0.clone(), key.session_lifetime_id.0.clone()]);
    values
}
pub(crate) fn proof_key(key: &SessionWriterLeaseKey) -> Result<String, CoordinationError> {
    serde_json::to_string(key).map_err(|_| CoordinationError::InvalidRequest)
}
pub(crate) fn integer(value: u64) -> Result<i64, CoordinationError> {
    i64::try_from(value).map_err(|_| CoordinationError::InvalidRequest)
}
pub(crate) fn unsigned(value: i64) -> Result<u64, CoordinationError> {
    u64::try_from(value).map_err(|_| CoordinationError::Corrupt)
}
pub(crate) fn field(row: &Row, index: usize) -> Result<String, CoordinationError> {
    row.try_get::<_, Option<String>>(index)
        .map_err(unavailable)?
        .ok_or(CoordinationError::Corrupt)
}
pub(crate) async fn permanent_life<C: GenericClient + Sync>(
    db: &C,
    key: &SessionWriterLeaseKey,
) -> Result<(), CoordinationError> {
    let found: bool = db.query_one(&format!("SELECT EXISTS(SELECT 1 FROM session_lifetime_owners WHERE {SCOPE} AND session_id=$5 AND lifetime=$6 AND provenance='canonical')"),&parameters(&key_values(key))).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
    if !found {
        return Err(CoordinationError::Forbidden);
    }
    Ok(())
}
pub(crate) async fn current_head(
    db: &Transaction<'_>,
    key: &SessionWriterLeaseKey,
    require_live: bool,
) -> Result<(), CoordinationError> {
    permanent_life(db, key).await?;
    let exists = db.query_opt(&format!("SELECT 1 FROM session_heads WHERE {SCOPE} AND session_id=$5 AND lifetime=$6 AND ($7::text::boolean=FALSE OR NOT deleted) FOR UPDATE"),&parameters(&{let mut values=key_values(key); values.push(require_live.to_string()); values})).await.map_err(unavailable)?;
    if exists.is_none() {
        return Err(CoordinationError::StaleLease);
    }
    Ok(())
}
pub(crate) async fn live_head(
    db: &Transaction<'_>,
    key: &SessionWriterLeaseKey,
) -> Result<(), CoordinationError> {
    current_head(db, key, true).await
}

pub(crate) async fn current<C: GenericClient + Sync>(
    db: &C,
    key: &SessionWriterLeaseKey,
    lock: bool,
) -> Result<Option<SessionWriterLease>, CoordinationError> {
    let suffix = if lock { " FOR UPDATE" } else { "" };
    db.query_opt(&format!("SELECT CASE WHEN octet_length(owner_id) BETWEEN 1 AND 128 THEN owner_id END,epoch,expires_at_ms FROM session_writer_leases WHERE {KEY}{suffix}"),&parameters(&key_values(key))).await.map_err(unavailable)?.map(|row|Ok(SessionWriterLease{key:key.clone(),owner_id:field(&row,0)?,epoch:unsigned(row.try_get(1).map_err(unavailable)?)?,expires_at_ms:row.try_get(2).map_err(unavailable)?})).transpose()
}
pub(crate) async fn save(
    db: &Transaction<'_>,
    lease: &SessionWriterLease,
) -> Result<(), CoordinationError> {
    let mut values = key_values(&lease.key);
    values.extend([
        lease.owner_id.clone(),
        integer(lease.epoch)?.to_string(),
        lease.expires_at_ms.to_string(),
    ]);
    let count=db.execute(&format!("UPDATE session_writer_leases SET owner_id=$7,epoch=$8::text::bigint,expires_at_ms=$9::text::bigint WHERE {KEY}"),&parameters(&values)).await.map_err(unavailable)?;
    if count != 1 {
        return Err(CoordinationError::Corrupt);
    }
    Ok(())
}
impl PgCanonicalStore {
    /// 显式本机Unix socket开发接入；远程连接使用独立严格TLS配置。
    pub async fn connect_local(
        database: &str,
        user: &str,
        schema: &str,
    ) -> Result<Self, CoordinationError> {
        Self::from_publication(
            PgPublicationStore::connect_local(database, user, schema).await?,
            schema,
        )
        .await
    }
    pub async fn connect_remote(
        config: &crate::PgConnectionConfig,
    ) -> Result<Self, CoordinationError> {
        Self::from_publication(
            PgPublicationStore::connect_remote(config).await?,
            &config.schema,
        )
        .await
    }
    async fn from_publication(
        publication: PgPublicationStore,
        schema: &str,
    ) -> Result<Self, CoordinationError> {
        if !identifier(schema) {
            return Err(CoordinationError::InvalidRequest);
        }
        let inner = std::sync::Arc::new(publication);
        {
            let mut client = inner.client.lock().await;
            let tx = client.transaction().await.map_err(unavailable)?;
            tx.batch_execute("SET LOCAL statement_timeout='30s'; SET LOCAL lock_timeout='5s'")
                .await
                .map_err(unavailable)?;
            let server: i32 = tx
                .query_one("SELECT current_setting('server_version_num')::integer", &[])
                .await
                .map_err(unavailable)?
                .try_get(0)
                .map_err(unavailable)?;
            if server < 160000 {
                return Err(CoordinationError::InvalidRequest);
            }
            tx.query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 39))",
                &[&schema],
            )
            .await
            .map_err(unavailable)?;
            let table: Option<String> = tx
                .query_one("SELECT to_regclass('canonical_migrations')::text", &[])
                .await
                .map_err(unavailable)?
                .try_get(0)
                .map_err(unavailable)?;
            let checksum = format!("{:x}", Sha256::digest(SCHEMA.as_bytes()));
            if table.is_some() {
                let row=tx.query_opt("SELECT version,checksum FROM canonical_migrations ORDER BY version DESC LIMIT 1",&[]).await.map_err(unavailable)?.ok_or(CoordinationError::Corrupt)?;
                let version: i64 = row.try_get(0).map_err(unavailable)?;
                if version > VERSION {
                    return Err(CoordinationError::SchemaTooNew);
                }
                let base = tx
                    .query_opt(
                        "SELECT checksum FROM canonical_migrations WHERE version=1",
                        &[],
                    )
                    .await
                    .map_err(unavailable)?
                    .ok_or(CoordinationError::Corrupt)?;
                if !(1..=VERSION).contains(&version)
                    || base.try_get::<_, String>(0).map_err(unavailable)? != checksum
                {
                    return Err(CoordinationError::Corrupt);
                }
                for (required, migration) in [
                    (2_i64, MIGRATION_V2),
                    (3_i64, MIGRATION_V3),
                    (4_i64, MIGRATION_V4),
                    (5_i64, MIGRATION_V5),
                    (6_i64, MIGRATION_V6),
                    (7_i64, MIGRATION_V7),
                    (8_i64,MIGRATION_V8),
                    (9_i64,MIGRATION_V9),
                    (10_i64,MIGRATION_V10),
                ] {
                    if version >= required {
                        let saved = tx
                            .query_opt(
                                "SELECT checksum FROM canonical_migrations WHERE version=$1",
                                &[&required],
                            )
                            .await
                            .map_err(unavailable)?
                            .ok_or(CoordinationError::Corrupt)?;
                        if saved.try_get::<_, String>(0).map_err(unavailable)?
                            != format!("{:x}", Sha256::digest(migration.as_bytes()))
                        {
                            return Err(CoordinationError::Corrupt);
                        }
                    }
                }
            } else {
                let existing:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM event_outbox) OR EXISTS(SELECT 1 FROM delivery_leases) OR EXISTS(SELECT 1 FROM publication_inbox)",&[]).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
                if existing {
                    return Err(CoordinationError::Conflict);
                }
                tx.batch_execute(SCHEMA).await.map_err(unavailable)?;
                let at = now(&tx).await?;
                tx.execute(
                    "INSERT INTO canonical_migrations VALUES($1,$2,$3)",
                    &[&1_i64, &checksum, &at],
                )
                .await
                .map_err(unavailable)?;
            }
            let upgraded: bool = tx
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM canonical_migrations WHERE version=2)",
                    &[],
                )
                .await
                .map_err(unavailable)?
                .try_get(0)
                .map_err(unavailable)?;
            if !upgraded {
                tx.batch_execute(MIGRATION_V2).await.map_err(unavailable)?;
                let checksum = format!("{:x}", Sha256::digest(MIGRATION_V2.as_bytes()));
                let at = now(&tx).await?;
                tx.execute(
                    "INSERT INTO canonical_migrations VALUES($1,$2,$3)",
                    &[&2_i64, &checksum, &at],
                )
                .await
                .map_err(unavailable)?;
            }
            let upgraded: bool = tx
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM canonical_migrations WHERE version=3)",
                    &[],
                )
                .await
                .map_err(unavailable)?
                .try_get(0)
                .map_err(unavailable)?;
            if !upgraded {
                tx.batch_execute(MIGRATION_V3).await.map_err(unavailable)?;
                tx.execute(
                    "INSERT INTO canonical_migrations VALUES($1,$2,$3)",
                    &[
                        &3_i64,
                        &format!("{:x}", Sha256::digest(MIGRATION_V3.as_bytes())),
                        &now(&tx).await?,
                    ],
                )
                .await
                .map_err(unavailable)?;
            }
            let upgraded: bool = tx
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM canonical_migrations WHERE version=4)",
                    &[],
                )
                .await
                .map_err(unavailable)?
                .try_get(0)
                .map_err(unavailable)?;
            if !upgraded {
                tx.batch_execute(MIGRATION_V4).await.map_err(unavailable)?;
                tx.execute(
                    "INSERT INTO canonical_migrations VALUES($1,$2,$3)",
                    &[
                        &4_i64,
                        &format!("{:x}", Sha256::digest(MIGRATION_V4.as_bytes())),
                        &now(&tx).await?,
                    ],
                )
                .await
                .map_err(unavailable)?;
            }
            let upgraded: bool = tx
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM canonical_migrations WHERE version=5)",
                    &[],
                )
                .await
                .map_err(unavailable)?
                .try_get(0)
                .map_err(unavailable)?;
            if !upgraded {
                tx.batch_execute(MIGRATION_V5).await.map_err(unavailable)?;
                tx.execute(
                    "INSERT INTO canonical_migrations VALUES($1,$2,$3)",
                    &[
                        &5_i64,
                        &format!("{:x}", Sha256::digest(MIGRATION_V5.as_bytes())),
                        &now(&tx).await?,
                    ],
                )
                .await
                .map_err(unavailable)?;
            }
            let upgraded: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM canonical_migrations WHERE version=6)", &[]).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
            if !upgraded {
                tx.batch_execute(MIGRATION_V6).await.map_err(unavailable)?;
                tx.execute("INSERT INTO canonical_migrations VALUES($1,$2,$3)", &[&6_i64,&format!("{:x}",Sha256::digest(MIGRATION_V6.as_bytes())),&now(&tx).await?]).await.map_err(unavailable)?;
            }
            let upgraded:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM canonical_migrations WHERE version=7)",&[]).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
            if !upgraded {tx.batch_execute(MIGRATION_V7).await.map_err(unavailable)?;tx.execute("INSERT INTO canonical_migrations VALUES($1,$2,$3)",&[&7_i64,&format!("{:x}",Sha256::digest(MIGRATION_V7.as_bytes())),&now(&tx).await?]).await.map_err(unavailable)?;}
            let upgraded:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM canonical_migrations WHERE version=8)",&[]).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
            if !upgraded {tx.batch_execute(MIGRATION_V8).await.map_err(unavailable)?;tx.execute("INSERT INTO canonical_migrations VALUES($1,$2,$3)",&[&8_i64,&format!("{:x}",Sha256::digest(MIGRATION_V8.as_bytes())),&now(&tx).await?]).await.map_err(unavailable)?;}
            let upgraded:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM canonical_migrations WHERE version=9)",&[]).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
            if !upgraded {tx.batch_execute(MIGRATION_V9).await.map_err(unavailable)?;tx.execute("INSERT INTO canonical_migrations VALUES($1,$2,$3)",&[&9_i64,&format!("{:x}",Sha256::digest(MIGRATION_V9.as_bytes())),&now(&tx).await?]).await.map_err(unavailable)?;}
            let upgraded=tx.query_opt("SELECT 1 FROM canonical_migrations WHERE version=10",&[]).await.map_err(unavailable)?.is_some();
            if !upgraded {tx.batch_execute(MIGRATION_V10).await.map_err(unavailable)?;tx.execute("INSERT INTO canonical_migrations VALUES($1,$2,$3)",&[&10_i64,&format!("{:x}",Sha256::digest(MIGRATION_V10.as_bytes())),&now(&tx).await?]).await.map_err(unavailable)?;}
            tx.commit().await.map_err(unavailable)?;
        }
        let writer_owner = {
            let client = inner.client.lock().await;
            client
                .query_one("SELECT gen_random_uuid()::text", &[])
                .await
                .map_err(unavailable)?
                .try_get(0)
                .map_err(unavailable)?
        };
        Ok(Self {
            inner,
            maintenance_tasks: Mutex::new(Vec::new()),
            proofs: Mutex::new(BTreeMap::new()),
            writer_owner,
            local_files: None,
            file_budget: std::sync::Arc::new(tokio::sync::Semaphore::new(4)),
            credential_budget: std::sync::Arc::new(tokio::sync::Semaphore::new(4)),
        })
    }
    /// 受信任启动装配显式开启 Local 文件目录；远程租户不能使用宿主路径。
    pub async fn with_local_file_artifacts(
        mut self,
        path: std::path::PathBuf,
    ) -> Result<Self, ArtifactError> {
        let directory =
            tokio::task::spawn_blocking(move || agent_artifact::FileArtifactDirectory::new(path))
                .await
                .map_err(|_| ArtifactError::StorageUnavailable)??;
        self.local_files = Some(std::sync::Arc::new(directory));
        Ok(self)
    }
    pub async fn close(self) {
        let _ = self.drain_admitted_work().await;
        if let Ok(inner) = std::sync::Arc::try_unwrap(self.inner) {
            inner.close().await;
        }
    }
    pub(crate) async fn bound_proof(
        &self,
        key: &SessionWriterLeaseKey,
    ) -> Result<SessionWriterLease, CoordinationError> {
        self.proofs
            .lock()
            .await
            .get(&proof_key(key)?)
            .cloned()
            .ok_or(CoordinationError::StaleLease)
    }
    /// 每个业务mutation先取固定proof，再在原事务锁head/lease并校验数据库时钟。
    pub(crate) async fn fence_in(
        db: &Transaction<'_>,
        lease: &SessionWriterLease,
    ) -> Result<(), CoordinationError> {
        live_head(db, &lease.key).await?;
        current(db, &lease.key, true)
            .await?
            .ok_or(CoordinationError::StaleLease)?
            .fence(lease, now(db).await?)
    }
    /// 提交前再核时钟，不把开始事务时有效的短lease延伸到超时提交。
    pub(crate) async fn before_commit(
        db: &Transaction<'_>,
        lease: &SessionWriterLease,
    ) -> Result<(), CoordinationError> {
        current(db, &lease.key, true)
            .await?
            .ok_or(CoordinationError::StaleLease)?
            .fence(lease, now(db).await?)
    }
}
#[async_trait::async_trait]
impl SessionWriterRepository for PgCanonicalStore {
    async fn acquire_writer_lease(
        &self,
        key: &SessionWriterLeaseKey,
        owner: &str,
        ttl_ms: i64,
    ) -> Result<SessionWriterLease, CoordinationError> {
        SessionWriterLease::acquire(None, key, owner, 0, ttl_ms)?;
        let proof_id = proof_key(key)?;
        // 只限制本进程持有的句柄数，不另建业务ledger。
        let mut proofs = self.proofs.lock().await;
        if !proofs.contains_key(&proof_id) && proofs.len() >= 1024 {
            return Err(CoordinationError::Conflict);
        }
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &key.scope).await?;
        let lease = grant_in(&tx, key, owner, ttl_ms).await?;
        tx.commit().await.map_err(unavailable)?;
        proofs.insert(proof_id, lease.clone());
        Ok(lease)
    }
    async fn renew_writer_lease(
        &self,
        lease: &SessionWriterLease,
        ttl_ms: i64,
    ) -> Result<SessionWriterLease, CoordinationError> {
        SessionWriterLease::acquire(None, &lease.key, &lease.owner_id, 0, ttl_ms)?;
        let mut proofs = self.proofs.lock().await;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &lease.key.scope).await?;
        current_head(&tx, &lease.key, false).await?;
        Self::before_commit(&tx, lease).await?;
        let saved = current(&tx, &lease.key, true)
            .await?
            .ok_or(CoordinationError::StaleLease)?;
        let renewed = SessionWriterLease::acquire(
            Some(&saved),
            &lease.key,
            &lease.owner_id,
            now(&tx).await?,
            ttl_ms,
        )?;
        save(&tx, &renewed).await?;
        tx.commit().await.map_err(unavailable)?;
        let id = proof_key(&lease.key)?;
        if proofs
            .get(&id)
            .is_some_and(|bound| bound.owner_id == lease.owner_id && bound.epoch == lease.epoch)
        {
            proofs.insert(id, renewed.clone());
        }
        Ok(renewed)
    }
    async fn release_writer_lease(
        &self,
        lease: &SessionWriterLease,
    ) -> Result<(), CoordinationError> {
        let mut proofs = self.proofs.lock().await;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &lease.key.scope).await?;
        permanent_life(&tx, &lease.key).await?;
        let previous = current(&tx, &lease.key, true)
            .await?
            .ok_or(CoordinationError::StaleLease)?;
        previous.fence(lease, now(&tx).await?)?;
        save(
            &tx,
            &SessionWriterLease {
                expires_at_ms: now(&tx).await?,
                ..previous
            },
        )
        .await?;
        tx.commit().await.map_err(unavailable)?;
        let id = proof_key(&lease.key)?;
        if proofs
            .get(&id)
            .is_some_and(|bound| bound.owner_id == lease.owner_id && bound.epoch == lease.epoch)
        {
            proofs.remove(&id);
        }
        Ok(())
    }
    async fn read_writer_lease(
        &self,
        key: &SessionWriterLeaseKey,
    ) -> Result<Option<SessionWriterLease>, CoordinationError> {
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &key.scope).await?;
        permanent_life(&tx, key).await?;
        let result = current(&tx, key, false).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
    async fn renew_bound_writers(&self) -> Result<usize, CoordinationError> {
        let leases: Vec<_> = self.proofs.lock().await.values().cloned().collect();
        for lease in &leases {
            self.renew_writer_lease(lease, 30_000).await?;
        }
        Ok(leases.len())
    }
    async fn release_bound_writers(&self) -> Result<usize, CoordinationError> {
        let leases: Vec<_> = self.proofs.lock().await.values().cloned().collect();
        let mut released = 0;
        for lease in &leases {
            match self.release_writer_lease(lease).await {
                Ok(()) => released += 1,
                Err(CoordinationError::StaleLease) => {
                    self.proofs.lock().await.remove(&proof_key(&lease.key)?);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(released)
    }
}
#[async_trait::async_trait]
impl StorageWorkDrainRepository for PgCanonicalStore {
    async fn drain_admitted_work(&self) -> Result<(), CoordinationError> {
        let mut tasks = self.maintenance_tasks.lock().await;
        while let Some(task) = tasks.pop() {
            task.await
                .map_err(|_| CoordinationError::StorageUnavailable)?;
        }
        // 取消后的文件 worker 仍持有 permit，关闭等待它们退出。
        let _files = self
            .file_budget
            .acquire_many(4)
            .await
            .map_err(|_| CoordinationError::StorageUnavailable)?;
        let _guard = self.inner.client.lock().await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl agent_storage_ports::PublicationRepository for PgCanonicalStore {
    async fn append_publication(
        &self,
        event: &OutboxEvent,
    ) -> Result<OutboxRecord, CoordinationError> {
        self.inner.append_publication(event).await
    }
    async fn acquire_delivery_lease(
        &self,
        key: &DeliveryLeaseKey,
        owner: &str,
        ttl_ms: i64,
    ) -> Result<DeliveryLease, CoordinationError> {
        self.inner.acquire_delivery_lease(key, owner, ttl_ms).await
    }
    async fn release_delivery_lease(&self, lease: &DeliveryLease) -> Result<(), CoordinationError> {
        self.inner.release_delivery_lease(lease).await
    }
    async fn pending_delivery_keys(
        &self,
        scope: &TenantContext,
        count: u32,
    ) -> Result<Vec<DeliveryLeaseKey>, CoordinationError> {
        self.inner.pending_delivery_keys(scope, count).await
    }
    async fn read_publications(
        &self,
        scope: &TenantContext,
        after: u64,
        count: u32,
    ) -> Result<Vec<OutboxRecord>, CoordinationError> {
        self.inner.read_publications(scope, after, count).await
    }
    async fn claim_publications(
        &self,
        lease: &DeliveryLease,
        count: u32,
    ) -> Result<Vec<OutboxRecord>, CoordinationError> {
        self.inner.claim_publications(lease, count).await
    }
    async fn deliver_to_inbox(
        &self,
        lease: &DeliveryLease,
        consumer: &str,
        item: &OutboxRecord,
    ) -> Result<bool, CoordinationError> {
        self.inner.deliver_to_inbox(lease, consumer, item).await
    }
    async fn acknowledge_publication(
        &self,
        lease: &DeliveryLease,
        id: &str,
        digest: &str,
    ) -> Result<(), CoordinationError> {
        self.inner.acknowledge_publication(lease, id, digest).await
    }
    async fn inbox_count(
        &self,
        scope: &TenantContext,
        consumer: &str,
    ) -> Result<u64, CoordinationError> {
        self.inner.inbox_count(scope, consumer).await
    }
}

pub(crate) async fn grant_in(
    db: &Transaction<'_>,
    key: &SessionWriterLeaseKey,
    owner: &str,
    ttl_ms: i64,
) -> Result<SessionWriterLease, CoordinationError> {
    current_head(db, key, false).await?;
    let initial = SessionWriterLease::acquire(None, key, owner, now(db).await?, ttl_ms)?;
    let mut values = key_values(key);
    values.extend([owner.into(), initial.expires_at_ms.to_string()]);
    db.execute("INSERT INTO session_writer_leases VALUES($1,$2,$3,$4,$5,$6,$7,1,$8::text::bigint) ON CONFLICT DO NOTHING",&parameters(&values)).await.map_err(unavailable)?;
    let previous = current(db, key, true)
        .await?
        .ok_or(CoordinationError::Corrupt)?;
    let lease = SessionWriterLease::acquire(Some(&previous), key, owner, now(db).await?, ttl_ms)?;
    save(db, &lease).await?;
    Ok(lease)
}
