//! 与 canonical 事件同事务的 outbox；发布租约不能充当 run writer 授权。
use crate::{RunStore, RuntimeError};
use agent_core::*;
use agent_storage_ports::{PublicationRepository, decode_record, seal_event};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params, params_from_iter};

const SCOPE: &str = "organization_id=?1 AND tenant_id=?2 AND project_id=?3 AND agent_uid=?4";
const KEY: &str = "organization_id=?1 AND tenant_id=?2 AND project_id=?3 AND agent_uid=?4 AND session_key=?5 AND lifetime=?6";
// SQL先抑制超大保存字段；PG不会把超预算payload传给Rust。
const RECORD: &str = "cursor,CASE WHEN typeof(event_id)='text' AND length(CAST(event_id AS BLOB))=64 THEN event_id END,CASE WHEN typeof(payload_json)='text' AND length(CAST(payload_json AS BLOB)) BETWEEN 1 AND 4194304 THEN payload_json END,CASE WHEN typeof(payload_digest)='text' AND length(CAST(payload_digest AS BLOB))=64 THEN payload_digest END,delivered,CASE WHEN typeof(organization_id)='text' AND length(CAST(organization_id AS BLOB)) BETWEEN 1 AND 128 THEN organization_id END,CASE WHEN typeof(tenant_id)='text' AND length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 128 THEN tenant_id END,CASE WHEN typeof(project_id)='text' AND length(CAST(project_id AS BLOB)) BETWEEN 1 AND 128 THEN project_id END,CASE WHEN typeof(agent_uid)='text' AND length(CAST(agent_uid AS BLOB)) BETWEEN 1 AND 128 THEN agent_uid END,CASE WHEN typeof(session_key)='text' AND length(CAST(session_key AS BLOB)) BETWEEN 1 AND 1024 THEN session_key END,CASE WHEN typeof(lifetime)='text' AND length(CAST(lifetime AS BLOB)) BETWEEN 1 AND 128 THEN lifetime END,CASE WHEN typeof(run_id)='text' AND length(CAST(run_id AS BLOB)) BETWEEN 1 AND 1024 THEN run_id END,event_seq";
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
fn limit(value: u32, max: u32) -> Result<(), CoordinationError> {
    if value == 0 || value > max {
        Err(CoordinationError::InvalidRequest)
    } else {
        Ok(())
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
fn unavailable<T>(_: T) -> CoordinationError {
    CoordinationError::StorageUnavailable
}
fn current(
    db: &Connection,
    key: &DeliveryLeaseKey,
) -> Result<Option<DeliveryLease>, CoordinationError> {
    db.query_row(
        &format!("SELECT owner_id,epoch,expires_at_ms FROM delivery_leases WHERE {KEY}"),
        params_from_iter(key_values(key)),
        |r| {
            Ok(DeliveryLease {
                key: key.clone(),
                owner_id: r.get(0)?,
                epoch: r.get(1)?,
                expires_at_ms: r.get(2)?,
            })
        },
    )
    .optional()
    .map_err(unavailable)
}
fn fence(db: &Connection, lease: &DeliveryLease) -> Result<(), CoordinationError> {
    current(db, &lease.key)?
        .ok_or(CoordinationError::StaleLease)?
        .fence(lease, now(db)?)
}
fn records(
    db: &Connection,
    sql: &str,
    values: Vec<String>,
) -> Result<Vec<OutboxRecord>, CoordinationError> {
    let mut query = db.prepare(sql).map_err(unavailable)?;
    let mut rows = query.query(params_from_iter(values)).map_err(unavailable)?;
    let mut found = Vec::new();
    while let Some(row) = rows.next().map_err(unavailable)? {
        let column = |index| -> Result<String, CoordinationError> {
            row.get::<_, Option<String>>(index)
                .map_err(unavailable)?
                .ok_or(CoordinationError::Corrupt)
        };
        let cursor: i64 = row.get(0).map_err(unavailable)?;
        let seq: i64 = row.get(12).map_err(unavailable)?;
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
        found.push(decode_record(
            u64::try_from(cursor).map_err(|_| CoordinationError::Corrupt)?,
            column(1)?,
            column(2)?,
            column(3)?,
            row.get(4).map_err(unavailable)?,
            binding,
        )?);
    }
    Ok(found)
}
fn record(
    db: &Connection,
    key: &DeliveryLeaseKey,
    id: &str,
) -> Result<OutboxRecord, CoordinationError> {
    let mut values = key_values(key);
    values.push(id.into());
    records(
        db,
        &format!("SELECT {RECORD} FROM event_outbox WHERE {KEY} AND event_id=?7"),
        values,
    )?
    .pop()
    .ok_or(CoordinationError::Forbidden)
}

fn insert(db: &Connection, event: &OutboxEvent) -> Result<OutboxRecord, CoordinationError> {
    let (id, digest, payload) = seal_event(event)?;
    let key = event.lease_key();
    let mut values = key_values(&key);
    values.push(id.clone());
    if let Some(existing) = records(
        db,
        &format!("SELECT {RECORD} FROM event_outbox WHERE {KEY} AND event_id=?7"),
        values,
    )?
    .pop()
    {
        return if existing.payload_digest == digest {
            Ok(existing)
        } else {
            Err(CoordinationError::Conflict)
        };
    }
    db.execute("INSERT INTO event_outbox(event_id,organization_id,tenant_id,project_id,agent_uid,session_key,lifetime,run_id,event_seq,payload_json,payload_digest) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",params![id,event.envelope.scope.organization_id.as_str(),event.envelope.scope.tenant_id.as_str(),event.envelope.scope.project_id.as_str(),event.envelope.scope.agent_uid.as_str(),event.envelope.owner.session_key.0,event.envelope.owner.session_lifetime_id.0,event.envelope.owner.run_id.0,event.event_seq.0,payload,digest]).map_err(unavailable)?;
    record(db, &key, &id)
}
/// 只接收原事件封存事务中的 canonical owner/data/version；旧历史不回填。
pub(crate) fn seal_in(
    db: &Connection,
    run: &RunId,
    seq: EventSeq,
    stamp: &ViewStamp,
) -> Result<(), RuntimeError> {
    let (org,tenant,project,agent,event,data):(String,String,String,String,String,String)=db.query_row("SELECT t.organization_id,s.tenant_id,s.project_id,s.agent_uid,e.event,e.data_json FROM events e JOIN runs r ON r.id=e.run_id JOIN session_scopes s ON s.session_id=r.session_id JOIN tenants t ON t.id=s.tenant_id WHERE e.run_id=?1 AND e.seq=?2",params![run.0,seq.0],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)))?;
    let envelope = EnterpriseOwnerEnvelope {
        scope: TenantContext {
            organization_id: OrganizationId::try_from(org)
                .map_err(|_| CoordinationError::Corrupt)?,
            tenant_id: TenantId::try_from(tenant).map_err(|_| CoordinationError::Corrupt)?,
            project_id: ProjectId::try_from(project).map_err(|_| CoordinationError::Corrupt)?,
            agent_uid: AgentUid::try_from(agent).map_err(|_| CoordinationError::Corrupt)?,
        },
        owner: stamp.owner.clone().ok_or(CoordinationError::Corrupt)?,
    };
    let event = OutboxEvent {
        envelope,
        event_seq: seq,
        event,
        data: serde_json::from_str(&data).map_err(|_| CoordinationError::Corrupt)?,
        stamp: stamp.clone(),
    };
    insert(db, &event)?;
    Ok(())
}
impl RunStore {
    fn publication_write<T>(
        &self,
        operation: impl FnOnce(&Connection) -> Result<T, CoordinationError>,
    ) -> Result<T, CoordinationError> {
        let mut db = self.lock_connection().map_err(unavailable)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(unavailable)?;
        let result = operation(&tx)?;
        tx.commit().map_err(unavailable)?;
        Ok(result)
    }
}
impl RunStore {
    fn append_publication_sync(
        &self,
        event: &OutboxEvent,
    ) -> Result<OutboxRecord, CoordinationError> {
        self.publication_write(|db| {
            let (id, digest, _) = seal_event(event)?;
            let existing = record(db, &event.lease_key(), &id)?;
            if existing.payload_digest != digest {
                return Err(CoordinationError::Conflict);
            }
            // 不通过端口补造业务事件；只有 views::commit 能新增。
            Ok(existing)
        })
    }
    fn acquire_delivery_lease_sync(
        &self,
        key: &DeliveryLeaseKey,
        owner: &str,
        ttl_ms: i64,
    ) -> Result<DeliveryLease, CoordinationError> {
        self.publication_write(|db| {
            let lease=DeliveryLease::acquire(current(db,key)?.as_ref(),key,owner,now(db)?,ttl_ms)?;
            let mut values=key_values(key); values.extend([lease.owner_id.clone(),lease.epoch.to_string(),lease.expires_at_ms.to_string()]);
            db.execute("INSERT INTO delivery_leases VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9) ON CONFLICT(organization_id,tenant_id,project_id,agent_uid,session_key,lifetime) DO UPDATE SET owner_id=excluded.owner_id,epoch=excluded.epoch,expires_at_ms=excluded.expires_at_ms",params_from_iter(values)).map_err(unavailable)?;
            Ok(lease)
        })
    }
    fn release_delivery_lease_sync(&self, lease: &DeliveryLease) -> Result<(), CoordinationError> {
        self.publication_write(|db| {
            fence(db, lease)?;
            let mut values = key_values(&lease.key);
            values.push(now(db)?.to_string());
            db.execute(
                &format!("UPDATE delivery_leases SET expires_at_ms=?7 WHERE {KEY}"),
                params_from_iter(values),
            )
            .map_err(unavailable)?;
            Ok(())
        })
    }
    fn pending_delivery_keys_sync(
        &self,
        scope: &TenantContext,
        count: u32,
    ) -> Result<Vec<DeliveryLeaseKey>, CoordinationError> {
        limit(count, 16)?;
        let db = self.lock_connection().map_err(unavailable)?;
        let mut values = scope_values(scope);
        values.push(count.to_string());
        let mut query=db.prepare(&format!("SELECT CASE WHEN typeof(session_key)='text' AND length(CAST(session_key AS BLOB)) BETWEEN 1 AND 1024 THEN session_key END,CASE WHEN typeof(lifetime)='text' AND length(CAST(lifetime AS BLOB)) BETWEEN 1 AND 128 THEN lifetime END FROM event_outbox WHERE {SCOPE} AND delivered=0 GROUP BY session_key,lifetime ORDER BY MIN(cursor) LIMIT ?5")).map_err(unavailable)?;
        query
            .query_map(params_from_iter(values), |r| {
                Ok((
                    r.get::<_, Option<String>>(0)?,
                    r.get::<_, Option<String>>(1)?,
                ))
            })
            .map_err(unavailable)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(unavailable)?
            .into_iter()
            .map(|(session, lifetime)| {
                Ok(DeliveryLeaseKey {
                    scope: scope.clone(),
                    session_key: SessionKey(session.ok_or(CoordinationError::Corrupt)?),
                    session_lifetime_id: SessionLifetimeId(
                        lifetime.ok_or(CoordinationError::Corrupt)?,
                    ),
                })
            })
            .collect()
    }

    fn read_publications_sync(
        &self,
        scope: &TenantContext,
        after: u64,
        count: u32,
    ) -> Result<Vec<OutboxRecord>, CoordinationError> {
        limit(count, 65)?;
        if after > i64::MAX as u64 {
            return Err(CoordinationError::InvalidRequest);
        }
        let db = self.lock_connection().map_err(unavailable)?;
        let mut values = scope_values(scope);
        values.extend([after.to_string(), count.to_string()]);
        records(
            &db,
            &format!(
                "SELECT {RECORD} FROM event_outbox WHERE {SCOPE} AND cursor>?5 ORDER BY cursor LIMIT ?6"
            ),
            values,
        )
    }
    fn claim_publications_sync(
        &self,
        lease: &DeliveryLease,
        count: u32,
    ) -> Result<Vec<OutboxRecord>, CoordinationError> {
        limit(count, 64)?;
        self.publication_write(|db| {
            fence(db,lease)?;let mut values=key_values(&lease.key);values.push(count.to_string());
            let found=records(db,&format!("SELECT {RECORD} FROM event_outbox WHERE {KEY} AND delivered=0 ORDER BY cursor LIMIT ?7"),values)?;
            for item in &found {
                let mut claim=key_values(&lease.key);claim.extend([item.event_id.clone(),lease.owner_id.clone(),lease.epoch.to_string()]);
                db.execute(&format!("UPDATE event_outbox SET claim_owner=?8,claim_epoch=?9 WHERE {KEY} AND event_id=?7 AND delivered=0"),params_from_iter(claim)).map_err(unavailable)?;
            }
            Ok(found)
        })
    }
    fn deliver_to_inbox_sync(
        &self,
        lease: &DeliveryLease,
        consumer: &str,
        item: &OutboxRecord,
    ) -> Result<bool, CoordinationError> {
        if consumer.is_empty() || consumer.len() > 128 {
            return Err(CoordinationError::InvalidRequest);
        }
        self.publication_write(|db| {
            fence(db,lease)?;check_claim(db,lease,&item.event_id,&item.payload_digest)?;
            if record(db,&lease.key,&item.event_id)?!=*item {return Err(CoordinationError::Conflict);}
            let mut values=scope_values(&lease.key.scope);values.extend([consumer.into(),item.event_id.clone(),item.payload_digest.clone()]);
            let changed=db.execute("INSERT INTO publication_inbox VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT DO NOTHING",params_from_iter(values)).map_err(unavailable)?;
            Ok(changed==1)
        })
    }
    fn acknowledge_publication_sync(
        &self,
        lease: &DeliveryLease,
        id: &str,
        digest: &str,
    ) -> Result<(), CoordinationError> {
        self.publication_write(|db| {
            fence(db, lease)?;
            check_claim(db, lease, id, digest)?;
            let mut receipt_values=scope_values(&lease.key.scope);receipt_values.extend([id.into(),digest.into()]);
            let receipt:bool=db.query_row(&format!("SELECT EXISTS(SELECT 1 FROM publication_inbox WHERE {SCOPE} AND event_id=?5 AND payload_digest=?6)"),params_from_iter(receipt_values),|r|r.get(0)).map_err(unavailable)?;
            if !receipt {return Err(CoordinationError::Conflict);}
            let mut ack=key_values(&lease.key);ack.push(id.into());
            db.execute(
                &format!("UPDATE event_outbox SET delivered=1 WHERE {KEY} AND event_id=?7"),
                params_from_iter(ack),
            )
            .map_err(unavailable)?;
            Ok(())
        })
    }
    fn inbox_count_sync(
        &self,
        scope: &TenantContext,
        consumer: &str,
    ) -> Result<u64, CoordinationError> {
        let db = self.lock_connection().map_err(unavailable)?;
        let mut values = scope_values(scope);
        values.push(consumer.into());
        db.query_row(
            &format!("SELECT COUNT(*) FROM publication_inbox WHERE {SCOPE} AND consumer=?5"),
            params_from_iter(values),
            |r| r.get(0),
        )
        .map_err(unavailable)
    }
}
fn check_claim(
    db: &Connection,
    lease: &DeliveryLease,
    id: &str,
    digest: &str,
) -> Result<(), CoordinationError> {
    let item = record(db, &lease.key, id)?;
    if item.event.lease_key() != lease.key {
        return Err(CoordinationError::Forbidden);
    }
    if item.payload_digest != digest {
        return Err(CoordinationError::Conflict);
    }
    let mut values = key_values(&lease.key);
    values.push(id.into());
    let (owner, epoch): (Option<String>, Option<u64>) = db
        .query_row(
            &format!(
                "SELECT claim_owner,claim_epoch FROM event_outbox WHERE {KEY} AND event_id=?7"
            ),
            params_from_iter(values),
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(unavailable)?;
    if owner.as_deref() != Some(&lease.owner_id) || epoch != Some(lease.epoch) {
        return Err(CoordinationError::StaleLease);
    }
    Ok(())
}

#[async_trait::async_trait]
impl PublicationRepository for RunStore {
    async fn append_publication(
        &self,
        event: &OutboxEvent,
    ) -> Result<OutboxRecord, CoordinationError> {
        let event = event.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable, move |s| {
            s.append_publication_sync(&event)
        })
        .await
    }
    async fn acquire_delivery_lease(
        &self,
        key: &DeliveryLeaseKey,
        owner: &str,
        ttl_ms: i64,
    ) -> Result<DeliveryLease, CoordinationError> {
        let key = key.clone();
        let owner = owner.to_owned();
        self.sqlite_blocking(CoordinationError::StorageUnavailable, move |s| {
            s.acquire_delivery_lease_sync(&key, &owner, ttl_ms)
        })
        .await
    }
    async fn release_delivery_lease(&self, lease: &DeliveryLease) -> Result<(), CoordinationError> {
        let lease = lease.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable, move |s| {
            s.release_delivery_lease_sync(&lease)
        })
        .await
    }
    async fn pending_delivery_keys(
        &self,
        scope: &TenantContext,
        count: u32,
    ) -> Result<Vec<DeliveryLeaseKey>, CoordinationError> {
        let scope = scope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable, move |s| {
            s.pending_delivery_keys_sync(&scope, count)
        })
        .await
    }
    async fn read_publications(
        &self,
        scope: &TenantContext,
        after: u64,
        count: u32,
    ) -> Result<Vec<OutboxRecord>, CoordinationError> {
        let scope = scope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable, move |s| {
            s.read_publications_sync(&scope, after, count)
        })
        .await
    }
    async fn claim_publications(
        &self,
        lease: &DeliveryLease,
        count: u32,
    ) -> Result<Vec<OutboxRecord>, CoordinationError> {
        let lease = lease.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable, move |s| {
            s.claim_publications_sync(&lease, count)
        })
        .await
    }
    async fn deliver_to_inbox(
        &self,
        lease: &DeliveryLease,
        consumer: &str,
        item: &OutboxRecord,
    ) -> Result<bool, CoordinationError> {
        let lease = lease.clone();
        let consumer = consumer.to_owned();
        let item = item.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable, move |s| {
            s.deliver_to_inbox_sync(&lease, &consumer, &item)
        })
        .await
    }
    async fn acknowledge_publication(
        &self,
        lease: &DeliveryLease,
        id: &str,
        digest: &str,
    ) -> Result<(), CoordinationError> {
        let lease = lease.clone();
        let id = id.to_owned();
        let digest = digest.to_owned();
        self.sqlite_blocking(CoordinationError::StorageUnavailable, move |s| {
            s.acknowledge_publication_sync(&lease, &id, &digest)
        })
        .await
    }
    async fn inbox_count(
        &self,
        scope: &TenantContext,
        consumer: &str,
    ) -> Result<u64, CoordinationError> {
        let scope = scope.clone();
        let consumer = consumer.to_owned();
        self.sqlite_blocking(CoordinationError::StorageUnavailable, move |s| {
            s.inbox_count_sync(&scope, &consumer)
        })
        .await
    }
}
