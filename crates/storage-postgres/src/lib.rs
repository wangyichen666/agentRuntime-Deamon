//! PostgreSQL发布协调与开发中的canonical事实源。提供本机 Unix socket 的 Local runtime 装配；远程企业运行仍待实现。
#![forbid(unsafe_code)]
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::print_stdout)
)]
mod access;
mod artifacts;
mod attempts;
mod canonical;
mod connection;
mod credentials;
mod billing;
mod registry;
mod workflow;
pub use connection::PgConnectionConfig;
mod compacts;
mod continuations;
mod delegations;
mod discovery;
mod events;
mod executions;
mod facts;
mod hooks;
mod identities;
mod identity_management;
mod interactions;
mod ledgers;
mod maintenance;
mod memories;
mod observations;
mod offline;
mod plan_controls;
mod plans;
mod queues;
mod readback;
mod requests;
mod resources;
mod sessions;
mod tools;
mod turns;
pub use canonical::PgCanonicalStore;

use agent_core::*;
use agent_storage_ports::{PublicationRepository, decode_record, seal_event};
use tokio::sync::Mutex;
use tokio_postgres::{Client, GenericClient, NoTls, Row, Transaction, types::ToSql};

const SCOPE: &str = "organization_id=$1 AND tenant_id=$2 AND project_id=$3 AND agent_uid=$4";
const KEY: &str = "organization_id=$1 AND tenant_id=$2 AND project_id=$3 AND agent_uid=$4 AND session_key=$5 AND lifetime=$6";
// SQL先抑制超大保存字段；PG不会把超预算payload传给Rust。
const RECORD: &str = "cursor,CASE WHEN octet_length(event_id)=64 THEN event_id END,CASE WHEN octet_length(payload_json) BETWEEN 1 AND 4194304 THEN payload_json END,CASE WHEN octet_length(payload_digest)=64 THEN payload_digest END,delivered,CASE WHEN octet_length(organization_id) BETWEEN 1 AND 128 THEN organization_id END,CASE WHEN octet_length(tenant_id) BETWEEN 1 AND 128 THEN tenant_id END,CASE WHEN octet_length(project_id) BETWEEN 1 AND 128 THEN project_id END,CASE WHEN octet_length(agent_uid) BETWEEN 1 AND 128 THEN agent_uid END,CASE WHEN octet_length(session_key) BETWEEN 1 AND 1024 THEN session_key END,CASE WHEN octet_length(lifetime) BETWEEN 1 AND 128 THEN lifetime END,CASE WHEN octet_length(run_id) BETWEEN 1 AND 1024 THEN run_id END,event_seq";
pub struct PgPublicationStore {
    client: Mutex<Client>,
    driver: tokio::task::JoinHandle<()>,
}
impl Drop for PgPublicationStore {
    fn drop(&mut self) {
        self.driver.abort();
    }
}
fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
}
#[track_caller]
fn unavailable(error: tokio_postgres::Error) -> CoordinationError {
    if error.code() == Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) {
        return CoordinationError::Conflict;
    }
    let location = std::panic::Location::caller();
    tracing::warn!(
        sqlstate = error
            .code()
            .map_or("transport_or_codec", |code| code.code()),
        source_line = location.line(),
        "PostgreSQL 发布协调操作失败"
    );
    CoordinationError::StorageUnavailable
}
fn scope_values(scope: &TenantContext) -> Vec<String> {
    vec![
        scope.organization_id.as_str().into(),
        scope.tenant_id.as_str().into(),
        scope.project_id.as_str().into(),
        scope.agent_uid.as_str().into(),
    ]
}
fn key_values(key: &DeliveryLeaseKey) -> Vec<String> {
    let mut values = scope_values(&key.scope);
    values.extend([key.session_key.0.clone(), key.session_lifetime_id.0.clone()]);
    values
}
fn parameters(values: &[String]) -> Vec<&(dyn ToSql + Sync)> {
    values.iter().map(|v| v as &(dyn ToSql + Sync)).collect()
}
fn limit(value: u32, max: u32) -> Result<(), CoordinationError> {
    if value == 0 || value > max {
        Err(CoordinationError::InvalidRequest)
    } else {
        Ok(())
    }
}
fn decode(row: Row) -> Result<OutboxRecord, CoordinationError> {
    let column = |index| -> Result<String, CoordinationError> {
        row.try_get::<_, Option<String>>(index)
            .map_err(unavailable)?
            .ok_or(CoordinationError::Corrupt)
    };
    let cursor: i64 = row.try_get(0).map_err(unavailable)?;
    let seq: i64 = row.try_get(12).map_err(unavailable)?;
    let binding = OutboxRowBinding {
        lease_key: DeliveryLeaseKey {
            scope: TenantContext {
                organization_id: column(5)?
                    .try_into()
                    .map_err(|_| CoordinationError::Corrupt)?,
                tenant_id: column(6)?
                    .try_into()
                    .map_err(|_| CoordinationError::Corrupt)?,
                project_id: column(7)?
                    .try_into()
                    .map_err(|_| CoordinationError::Corrupt)?,
                agent_uid: column(8)?
                    .try_into()
                    .map_err(|_| CoordinationError::Corrupt)?,
            },
            session_key: SessionKey(column(9)?),
            session_lifetime_id: SessionLifetimeId(column(10)?),
        },
        run_id: RunId(column(11)?),
        event_seq: EventSeq(u64::try_from(seq).map_err(|_| CoordinationError::Corrupt)?),
    };
    decode_record(
        u64::try_from(cursor).map_err(|_| CoordinationError::Corrupt)?,
        column(1)?,
        column(2)?,
        column(3)?,
        row.try_get(4).map_err(unavailable)?,
        binding,
    )
}

async fn now<C: GenericClient + Sync>(db: &C) -> Result<i64, CoordinationError> {
    db.query_one(
        "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",
        &[],
    )
    .await
    .map_err(unavailable)?
    .try_get(0)
    .map_err(unavailable)
}
async fn current(
    db: &Transaction<'_>,
    key: &DeliveryLeaseKey,
) -> Result<Option<DeliveryLease>, CoordinationError> {
    let values = key_values(key);
    let row = db
        .query_opt(
            &format!(
                "SELECT owner_id,epoch,expires_at_ms FROM delivery_leases WHERE {KEY} FOR UPDATE"
            ),
            &parameters(&values),
        )
        .await
        .map_err(unavailable)?;
    row.map(|r| {
        let epoch: i64 = r.try_get(1).map_err(unavailable)?;
        Ok(DeliveryLease {
            key: key.clone(),
            owner_id: r.try_get(0).map_err(unavailable)?,
            epoch: u64::try_from(epoch).map_err(|_| CoordinationError::Corrupt)?,
            expires_at_ms: r.try_get(2).map_err(unavailable)?,
        })
    })
    .transpose()
}
async fn fence(db: &Transaction<'_>, lease: &DeliveryLease) -> Result<(), CoordinationError> {
    current(db, &lease.key)
        .await?
        .ok_or(CoordinationError::StaleLease)?
        .fence(lease, now(db).await?)
}
async fn record<C: GenericClient + Sync>(
    db: &C,
    key: &DeliveryLeaseKey,
    id: &str,
) -> Result<OutboxRecord, CoordinationError> {
    let mut values = key_values(key);
    values.push(id.into());
    decode(
        db.query_opt(
            &format!("SELECT {RECORD} FROM event_outbox WHERE {KEY} AND event_id=$7"),
            &parameters(&values),
        )
        .await
        .map_err(unavailable)?
        .ok_or(CoordinationError::Forbidden)?,
    )
}
async fn check_claim(
    db: &Transaction<'_>,
    lease: &DeliveryLease,
    id: &str,
    digest: &str,
) -> Result<OutboxRecord, CoordinationError> {
    let item = record(db, &lease.key, id).await?;
    if item.event.lease_key() != lease.key {
        return Err(CoordinationError::Forbidden);
    }
    if item.payload_digest != digest {
        return Err(CoordinationError::Conflict);
    }
    let mut values = key_values(&lease.key);
    values.push(id.into());
    let row = db
        .query_one(
            &format!(
                "SELECT claim_owner,claim_epoch FROM event_outbox WHERE {KEY} AND event_id=$7"
            ),
            &parameters(&values),
        )
        .await
        .map_err(unavailable)?;
    let owner: Option<String> = row.try_get(0).map_err(unavailable)?;
    let epoch: Option<i64> = row.try_get(1).map_err(unavailable)?;
    if owner.as_deref() != Some(&lease.owner_id)
        || epoch != Some(i64::try_from(lease.epoch).map_err(|_| CoordinationError::StaleLease)?)
    {
        return Err(CoordinationError::StaleLease);
    }
    Ok(item)
}
impl PgPublicationStore {
    /// 受管 driver 必须先结束再交接给另一进程；Drop 负责异常路径取消。
    pub async fn close(mut self) {
        self.driver.abort();
        let _ = (&mut self.driver).await;
    }
    /// database/user/schema 都为显式配置；固定 Unix socket，不接收/记录含密码 DSN。
    pub async fn connect_local(
        database: &str,
        user: &str,
        schema: &str,
    ) -> Result<Self, CoordinationError> {
        if ![database, user, schema].into_iter().all(identifier)
            || schema == "public"
            || schema.starts_with("pg_")
        {
            return Err(CoordinationError::InvalidRequest);
        }
        let mut config = tokio_postgres::Config::new();
        config
            .application_name(format!("agent-publication-{schema}"))
            .host("/tmp")
            .dbname(database)
            .user(user)
            .connect_timeout(std::time::Duration::from_secs(5));
        let (client, connection) = config.connect(NoTls).await.map_err(unavailable)?;
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });
        Self::initialize(client, driver, schema).await
    }
    pub(crate) async fn initialize(
        mut client: Client,
        driver: tokio::task::JoinHandle<()>,
        schema: &str,
    ) -> Result<Self, CoordinationError> {
        let migration=async {
            let tx=client.transaction().await.map_err(unavailable)?;
            tx.execute("SELECT pg_advisory_xact_lock(hashtextextended($1,0))",&[&schema]).await.map_err(unavailable)?;
            tx.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\";SET search_path TO \"{schema}\";CREATE TABLE IF NOT EXISTS coordination_migrations(version BIGINT PRIMARY KEY); SET statement_timeout=5000; SET lock_timeout=1000;")).await.map_err(unavailable)?;
            let version:i64=tx.query_one("SELECT COALESCE(MAX(version),0) FROM coordination_migrations",&[]).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
            if version>1 {return Err(CoordinationError::SchemaTooNew);}
            if version==0 {tx.batch_execute(include_str!("schema.sql")).await.map_err(unavailable)?;}
            tx.commit().await.map_err(unavailable)?;Ok(())
        }.await;
        if let Err(error) = migration {
            driver.abort();
            return Err(error);
        }
        Ok(Self {
            client: Mutex::new(client),
            driver,
        })
    }
}
#[async_trait::async_trait]
impl PublicationRepository for PgPublicationStore {
    async fn append_publication(
        &self,
        event: &OutboxEvent,
    ) -> Result<OutboxRecord, CoordinationError> {
        let (id, digest, payload) = seal_event(event)?;
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        canonical::set_scope(&tx, &event.envelope.scope).await?;
        let mut values = vec![id.clone()];
        values.extend(key_values(&event.lease_key()));
        values.extend([
            event.envelope.owner.run_id.0.clone(),
            event.event_seq.0.to_string(),
            payload,
            digest.clone(),
        ]);
        tx.execute("INSERT INTO event_outbox(event_id,organization_id,tenant_id,project_id,agent_uid,session_key,lifetime,run_id,event_seq,payload_json,payload_digest) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::text::bigint,$10,$11) ON CONFLICT DO NOTHING",&parameters(&values)).await.map_err(unavailable)?;
        let found = record(&tx, &event.lease_key(), &id).await?;
        if found.payload_digest != digest {
            return Err(CoordinationError::Conflict);
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(found)
    }
    async fn acquire_delivery_lease(
        &self,
        key: &DeliveryLeaseKey,
        owner: &str,
        ttl_ms: i64,
    ) -> Result<DeliveryLease, CoordinationError> {
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        canonical::set_scope(&tx, &key.scope).await?;
        let first = DeliveryLease::acquire(None, key, owner, now(&tx).await?, ttl_ms)?;
        let mut values = key_values(key);
        values.extend([
            owner.into(),
            first.epoch.to_string(),
            first.expires_at_ms.to_string(),
        ]);
        let inserted=tx.execute("INSERT INTO delivery_leases VALUES($1,$2,$3,$4,$5,$6,$7,$8::text::bigint,$9::text::bigint) ON CONFLICT DO NOTHING",&parameters(&values)).await.map_err(unavailable)?;
        let found = current(&tx, key).await?.ok_or(CoordinationError::Corrupt)?;
        // 新行也使用锁后的数据库时间；过期时同 owner 必须增加 epoch。
        let lease = if inserted == 1 {
            DeliveryLease {
                expires_at_ms: now(&tx)
                    .await?
                    .checked_add(ttl_ms)
                    .ok_or(CoordinationError::InvalidRequest)?,
                ..first
            }
        } else {
            DeliveryLease::acquire(Some(&found), key, owner, now(&tx).await?, ttl_ms)?
        };
        let mut values = key_values(key);
        values.extend([
            lease.owner_id.clone(),
            lease.epoch.to_string(),
            lease.expires_at_ms.to_string(),
        ]);
        tx.execute(&format!("UPDATE delivery_leases SET owner_id=$7,epoch=$8::text::bigint,expires_at_ms=$9::text::bigint WHERE {KEY}"),&parameters(&values)).await.map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(lease)
    }
    async fn release_delivery_lease(&self, lease: &DeliveryLease) -> Result<(), CoordinationError> {
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        canonical::set_scope(&tx, &lease.key.scope).await?;
        fence(&tx, lease).await?;
        let mut values = key_values(&lease.key);
        values.push(now(&tx).await?.to_string());
        tx.execute(
            &format!("UPDATE delivery_leases SET expires_at_ms=$7::text::bigint WHERE {KEY}"),
            &parameters(&values),
        )
        .await
        .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)
    }
    async fn pending_delivery_keys(
        &self,
        scope: &TenantContext,
        count: u32,
    ) -> Result<Vec<DeliveryLeaseKey>, CoordinationError> {
        limit(count, 16)?;
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        canonical::set_scope(&tx, scope).await?;
        let mut values = scope_values(scope);
        values.push(count.to_string());
        let result = tx.query(&format!("SELECT CASE WHEN octet_length(session_key) BETWEEN 1 AND 1024 THEN session_key END,CASE WHEN octet_length(lifetime) BETWEEN 1 AND 128 THEN lifetime END FROM event_outbox WHERE {SCOPE} AND NOT delivered GROUP BY session_key,lifetime ORDER BY MIN(cursor) LIMIT $5::text::bigint"),&parameters(&values)).await.map_err(unavailable)?.into_iter().map(|r|Ok(DeliveryLeaseKey{scope:scope.clone(),session_key:SessionKey(r.try_get::<_,Option<String>>(0).map_err(unavailable)?.ok_or(CoordinationError::Corrupt)?),session_lifetime_id:SessionLifetimeId(r.try_get::<_,Option<String>>(1).map_err(unavailable)?.ok_or(CoordinationError::Corrupt)?)})).collect::<Result<Vec<_>, CoordinationError>>()?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
    async fn read_publications(
        &self,
        scope: &TenantContext,
        after: u64,
        count: u32,
    ) -> Result<Vec<OutboxRecord>, CoordinationError> {
        limit(count, 65)?;
        if after > i64::MAX as u64 {
            return Err(CoordinationError::InvalidRequest);
        }
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        canonical::set_scope(&tx, scope).await?;
        let mut values = scope_values(scope);
        values.extend([after.to_string(), count.to_string()]);
        let result = tx.query(&format!("SELECT {RECORD} FROM event_outbox WHERE {SCOPE} AND cursor>$5::text::bigint ORDER BY cursor LIMIT $6::text::bigint"),&parameters(&values)).await.map_err(unavailable)?.into_iter().map(decode).collect::<Result<Vec<_>, CoordinationError>>()?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
    async fn claim_publications(
        &self,
        lease: &DeliveryLease,
        count: u32,
    ) -> Result<Vec<OutboxRecord>, CoordinationError> {
        limit(count, 64)?;
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        canonical::set_scope(&tx, &lease.key.scope).await?;
        fence(&tx, lease).await?;
        let mut values = key_values(&lease.key);
        values.push(count.to_string());
        let found:Vec<_>=tx.query(&format!("SELECT {RECORD} FROM event_outbox WHERE {KEY} AND NOT delivered ORDER BY cursor LIMIT $7::text::bigint FOR UPDATE"),&parameters(&values)).await.map_err(unavailable)?.into_iter().map(decode).collect::<Result<_,_>>()?;
        let epoch = i64::try_from(lease.epoch).map_err(|_| CoordinationError::StaleLease)?;
        for item in &found {
            let mut claim = key_values(&lease.key);
            claim.extend([
                item.event_id.clone(),
                lease.owner_id.clone(),
                epoch.to_string(),
            ]);
            tx.execute(&format!("UPDATE event_outbox SET claim_owner=$8,claim_epoch=$9::text::bigint WHERE {KEY} AND event_id=$7 AND NOT delivered"),&parameters(&claim)).await.map_err(unavailable)?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(found)
    }
    async fn deliver_to_inbox(
        &self,
        lease: &DeliveryLease,
        consumer: &str,
        item: &OutboxRecord,
    ) -> Result<bool, CoordinationError> {
        if consumer.is_empty() || consumer.len() > 128 {
            return Err(CoordinationError::InvalidRequest);
        }
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        canonical::set_scope(&tx, &lease.key.scope).await?;
        fence(&tx, lease).await?;
        if check_claim(&tx, lease, &item.event_id, &item.payload_digest).await? != *item {
            return Err(CoordinationError::Conflict);
        }
        let mut values = scope_values(&lease.key.scope);
        values.extend([
            consumer.into(),
            item.event_id.clone(),
            item.payload_digest.clone(),
        ]);
        let changed = tx
            .execute(
                "INSERT INTO publication_inbox VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING",
                &parameters(&values),
            )
            .await
            .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(changed == 1)
    }
    async fn acknowledge_publication(
        &self,
        lease: &DeliveryLease,
        id: &str,
        digest: &str,
    ) -> Result<(), CoordinationError> {
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        canonical::set_scope(&tx, &lease.key.scope).await?;
        fence(&tx, lease).await?;
        check_claim(&tx, lease, id, digest).await?;
        let mut receipt_values = scope_values(&lease.key.scope);
        receipt_values.extend([id.into(), digest.into()]);
        let receipt:bool=tx.query_one(&format!("SELECT EXISTS(SELECT 1 FROM publication_inbox WHERE {SCOPE} AND event_id=$5 AND payload_digest=$6)"),&parameters(&receipt_values)).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
        if !receipt {
            return Err(CoordinationError::Conflict);
        }
        let mut ack = key_values(&lease.key);
        ack.push(id.into());
        tx.execute(
            &format!("UPDATE event_outbox SET delivered=TRUE WHERE {KEY} AND event_id=$7"),
            &parameters(&ack),
        )
        .await
        .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)
    }
    async fn inbox_count(
        &self,
        scope: &TenantContext,
        consumer: &str,
    ) -> Result<u64, CoordinationError> {
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        canonical::set_scope(&tx, scope).await?;
        let mut values = scope_values(scope);
        values.push(consumer.into());
        let count: i64 = tx
            .query_one(
                &format!("SELECT COUNT(*) FROM publication_inbox WHERE {SCOPE} AND consumer=$5"),
                &parameters(&values),
            )
            .await
            .map_err(unavailable)?
            .try_get(0)
            .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        u64::try_from(count).map_err(|_| CoordinationError::Corrupt)
    }
}

#[cfg(test)]
mod native_contract;
