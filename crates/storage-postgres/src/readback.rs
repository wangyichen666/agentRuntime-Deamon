//! PostgreSQL会话读回以同一REPEATABLE READ事务构建，不拼接缓存事实。
use crate::canonical::{PgCanonicalStore, field, set_scope, unsigned};
use crate::executions::record_in;
use crate::identities::exact_owner_in;
use crate::sessions::{encode, metadata_in, transcript_in};
use crate::{SCOPE, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::SessionReadRepository;
use sha2::{Digest, Sha256};
use tokio_postgres::Transaction;
pub(crate) async fn snapshot_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    meta: &SessionMetadata,
) -> Result<SessionSnapshot, CoordinationError> {
    if meta.deleted {
        return Err(CoordinationError::Forbidden);
    }
    let (messages, batch_ranges) = transcript_in(db, scope, meta).await?;
    let mut values = scope_values(scope);
    values.push(meta.key.0.clone());
    let row = db
        .query_one(
            &format!(
                "SELECT projection_generation FROM session_heads WHERE {SCOPE} AND session_id=$5"
            ),
            &parameters(&values),
        )
        .await
        .map_err(unavailable)?;
    let mut batches = scope_values(scope);
    batches.push(meta.lifetime.0.clone());
    let rows=db.query(&format!("SELECT CASE WHEN octet_length(messages_json)<=16777216 THEN messages_json END FROM transcript_batches WHERE {SCOPE} AND lifetime=$5 ORDER BY start_seq,storage_row_id LIMIT 32768"), &parameters(&batches)).await.map_err(unavailable)?;
    let mut digest = Sha256::new();
    for row in rows {
        digest.update(field(&row, 0)?.as_bytes());
    }
    let prefix_digest = format!("{:x}", digest.finalize());
    Ok(SessionSnapshot {
        session_key: meta.key.clone(),
        lifetime: meta.lifetime.clone(),
        revision: meta.revision,
        projection_generation: ProjectionGeneration(unsigned(
            row.try_get(0).map_err(unavailable)?,
        )?),
        messages,
        batch_ranges,
        prefix_digest,
    })
}
pub(crate) async fn projection_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    snapshot: &SessionSnapshot,
) -> Result<ContextProjection, CoordinationError> {
    let mut values = scope_values(scope);
    values.push(snapshot.lifetime.0.clone());
    let row=db.query_opt(&format!("SELECT source_end,generation,CASE WHEN octet_length(messages_json)<=16777216 THEN messages_json END FROM context_heads WHERE {SCOPE} AND lifetime=$5"),&parameters(&values)).await.map_err(unavailable)?;
    let messages = if let Some(row) = row {
        let end = unsigned(row.try_get(0).map_err(unavailable)?)?;
        let generation = unsigned(row.try_get(1).map_err(unavailable)?)?;
        if end > snapshot.revision.0 || generation != snapshot.projection_generation.0 {
            return Err(CoordinationError::Corrupt);
        }
        let mut messages: Vec<Message> =
            serde_json::from_str(&field(&row, 2)?).map_err(|_| CoordinationError::Corrupt)?;
        messages.extend_from_slice(
            snapshot
                .messages
                .get(end as usize..)
                .ok_or(CoordinationError::Corrupt)?,
        );
        messages
    } else {
        snapshot.messages.clone()
    };
    if messages.len() > 32768 {
        return Err(CoordinationError::Corrupt);
    }
    encode(&messages, 16 * 1024 * 1024)?;
    Ok(ContextProjection {
        source_end: snapshot.revision,
        generation: snapshot.projection_generation,
        messages,
    })
}
async fn message_ids(
    db: &Transaction<'_>,
    scope: &TenantContext,
    snapshot: &SessionSnapshot,
) -> Result<Vec<String>, CoordinationError> {
    let mut values = scope_values(scope);
    values.push(snapshot.lifetime.0.clone());
    let rows=db.query("SELECT b.start_seq,b.end_seq,b.run_id,CASE WHEN octet_length(t.id)<=1024 THEN t.id END,r.session_id,r.lifetime FROM transcript_batches b LEFT JOIN runs r ON (r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id)=(b.organization_id,b.tenant_id,b.project_id,b.agent_uid,b.run_id) LEFT JOIN turns t ON (t.organization_id,t.tenant_id,t.project_id,t.agent_uid,t.run_id)=(r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id) WHERE b.organization_id=$1 AND b.tenant_id=$2 AND b.project_id=$3 AND b.agent_uid=$4 AND b.lifetime=$5 ORDER BY b.start_seq,b.storage_row_id LIMIT 32768",&parameters(&values)).await.map_err(unavailable)?;
    let mut turns = Vec::with_capacity(snapshot.messages.len());
    for row in rows {
        let start = unsigned(row.try_get(0).map_err(unavailable)?)?;
        let end = unsigned(row.try_get(1).map_err(unavailable)?)?;
        if end < start || end > snapshot.revision.0 {
            return Err(CoordinationError::Corrupt);
        }
        let run: Option<String> = row.try_get(2).map_err(unavailable)?;
        let turn = if run.is_some() {
            if row
                .try_get::<_, Option<String>>(4)
                .map_err(unavailable)?
                .as_deref()
                != Some(&snapshot.session_key.0)
                || row
                    .try_get::<_, Option<String>>(5)
                    .map_err(unavailable)?
                    .as_deref()
                    != Some(&snapshot.lifetime.0)
            {
                return Err(CoordinationError::Corrupt);
            }
            Some(TurnId(field(&row, 3)?))
        } else {
            None
        };
        for _ in start..end {
            turns.push(turn.clone());
        }
    }
    canonical_message_identities(&snapshot.lifetime, &snapshot.messages, &turns)
}
async fn pending_interactions(
    db: &Transaction<'_>,
    scope: &TenantContext,
    meta: &SessionMetadata,
) -> Result<Vec<InteractionRecord>, CoordinationError> {
    crate::interactions::list_in(db, scope, meta, true).await
}
#[async_trait::async_trait]
impl SessionReadRepository for PgCanonicalStore {
    async fn read_session(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        mode: HistoryReadMode,
    ) -> Result<SessionReadback, CoordinationError> {
        if key.0.is_empty() || key.0.len() > 1024 {
            return Err(CoordinationError::InvalidRequest);
        }
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let meta = metadata_in(&tx, scope, key, false)
            .await?
            .filter(|meta| !meta.deleted)
            .ok_or(CoordinationError::Forbidden)?;
        let mut values = scope_values(scope);
        values.extend([key.0.clone(), meta.lifetime.0.clone()]);
        let head=tx.query_one("SELECT h.metadata_revision,h.projection_generation,c.revision FROM session_heads h JOIN snapshot_clock c USING(organization_id,tenant_id,project_id,agent_uid) WHERE h.organization_id=$1 AND h.tenant_id=$2 AND h.project_id=$3 AND h.agent_uid=$4 AND h.session_id=$5 AND h.lifetime=$6 AND c.slot=1",&parameters(&values)).await.map_err(unavailable)?;
        let generation = ProjectionGeneration(unsigned(head.try_get(1).map_err(unavailable)?)?);
        let mut omitted = Vec::new();
        let (messages, batch_ranges, message_ids) = match mode {
            HistoryReadMode::Omitted => {
                omitted.extend(["messages".into(), "batch_ranges".into()]);
                (Vec::new(), Vec::new(), Vec::new())
            }
            HistoryReadMode::Canonical => {
                let snapshot = snapshot_in(&tx, scope, &meta).await?;
                let ids = message_ids(&tx, scope, &snapshot).await?;
                (snapshot.messages, snapshot.batch_ranges, ids)
            }
            HistoryReadMode::Model => {
                let snapshot = snapshot_in(&tx, scope, &meta).await?;
                let projection = projection_in(&tx, scope, &snapshot).await?;
                omitted.extend(
                    [
                        "stable_prefix",
                        "retrieved_context",
                        "turn_overlay",
                        "canonical_messages",
                        "batch_ranges",
                    ]
                    .map(String::from),
                );
                (projection.messages, Vec::new(), Vec::new())
            }
        };
        let rows=tx.query(&format!("SELECT id FROM runs WHERE {SCOPE} AND session_id=$5 AND lifetime=$6 AND status IN ('queued','running','waiting_interaction') ORDER BY generation LIMIT 66"),&parameters(&values)).await.map_err(unavailable)?;
        if rows.len() > 65 {
            return Err(CoordinationError::InvalidRequest);
        }
        let mut active_runs = Vec::new();
        let mut run_owners = Vec::new();
        let mut active_owner = None;
        for row in rows {
            let run = RunId(row.try_get(0).map_err(unavailable)?);
            let record = record_in(&tx, scope, &run).await?;
            let owner = exact_owner_in(&tx, scope, &run).await?;
            if matches!(
                record.status,
                RunStatus::Running | RunStatus::WaitingInteraction
            ) {
                if active_owner.is_some() {
                    return Err(CoordinationError::Corrupt);
                }
                active_owner = Some(owner.clone());
            }
            active_runs.push(record);
            run_owners.push(owner);
        }
        let queue_rows = crate::queues::queues_in(&tx, scope, key, None).await?;
        let pending_interactions = pending_interactions(&tx, scope, &meta).await?;
        let last=tx.query_opt("SELECT r.id FROM runs r JOIN events e ON (e.organization_id,e.tenant_id,e.project_id,e.agent_uid,e.run_id)=(r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id) WHERE r.organization_id=$1 AND r.tenant_id=$2 AND r.project_id=$3 AND r.agent_uid=$4 AND r.session_id=$5 AND r.lifetime=$6 AND r.status IN ('completed','failed','cancelled','unknown_after_restart') AND e.event='terminal' ORDER BY e.created_at_ms DESC,r.generation DESC,e.seq DESC LIMIT 1",&parameters(&values)).await.map_err(unavailable)?;
        let last_durable_terminal = if let Some(row) = last {
            let run = RunId(row.try_get(0).map_err(unavailable)?);
            let record = record_in(&tx, scope, &run).await?;
            run_owners.push(exact_owner_in(&tx, scope, &run).await?);
            Some(record)
        } else {
            None
        };
        let mut plan_values = scope_values(scope);
        plan_values.push(meta.lifetime.0.clone());
        let plan=tx.query_opt(&format!("SELECT revision,CASE WHEN octet_length(data_json)<=1048576 THEN data_json END FROM session_plans WHERE {SCOPE} AND lifetime=$5"),&parameters(&plan_values)).await.map_err(unavailable)?;
        let current_plan = plan
            .map(|row| {
                Ok(PlanSnapshot {
                    revision: unsigned(row.try_get(0).map_err(unavailable)?)?,
                    value: serde_json::from_str(&field(&row, 1)?)
                        .map_err(|_| CoordinationError::Corrupt)?,
                })
            })
            .transpose()?;
        let plan_digest = if let Some(plan) = &current_plan {
            if plan.value.get("plan_id").is_some() {
                let doc: PlanDocument = serde_json::from_value(plan.value.clone())
                    .map_err(|_| CoordinationError::Corrupt)?;
                crate::plans::validate_document(&doc)?;
                if doc.revision != plan.revision {
                    return Err(CoordinationError::Corrupt);
                }
                Some(doc.content_digest)
            } else {
                let definition: PlanDefinition = serde_json::from_value(plan.value.clone())
                    .map_err(|_| CoordinationError::Corrupt)?;
                definition
                    .canonical_bytes()
                    .map_err(|_| CoordinationError::Corrupt)?;
                omitted.push("plan_execution_identity".into());
                Some(format!(
                    "{:x}",
                    Sha256::digest(encode(&plan.value, 1048576)?.as_bytes())
                ))
            }
        } else {
            omitted.push("plan_execution_identity".into());
            None
        };
        let mut usage_values = values.clone();
        usage_values.push(generation.0.to_string());
        let ledger=tx.query_opt("SELECT CASE WHEN octet_length(l.envelope_json)<=1048576 THEN l.envelope_json END,r.id FROM context_ledgers l JOIN runs r ON (r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id)=(l.organization_id,l.tenant_id,l.project_id,l.agent_uid,l.run_id) WHERE r.organization_id=$1 AND r.tenant_id=$2 AND r.project_id=$3 AND r.agent_uid=$4 AND r.session_id=$5 AND r.lifetime=$6 AND (l.envelope_json::jsonb->'source'->>'generation')::bigint=$7::text::bigint ORDER BY r.generation DESC,l.round DESC LIMIT 1",&parameters(&usage_values)).await.map_err(unavailable)?;
        let context_usage = if let Some(row) = ledger {
            let context: ContextEnvelope =
                serde_json::from_str(&field(&row, 0)?).map_err(|_| CoordinationError::Corrupt)?;
            if context.source.lifetime != meta.lifetime
                || context.source.generation != generation
                || context.source.source_start.0 != 0
                || context.source.source_end.0 > meta.revision.0
            {
                return Err(CoordinationError::Corrupt);
            }
            exact_owner_in(&tx, scope, &RunId(row.try_get(1).map_err(unavailable)?)).await?;
            Some(context)
        } else {
            None
        };
        let result = SessionReadback {
            schema_version: 1,
            session_id: key.clone(),
            session_lifetime_id: meta.lifetime.clone(),
            snapshot_revision: SnapshotRevision(unsigned(head.try_get(2).map_err(unavailable)?)?),
            metadata_revision: SnapshotRevision(unsigned(head.try_get(0).map_err(unavailable)?)?),
            transcript_revision: meta.revision,
            projection_generation: generation,
            history_mode: mode,
            omitted,
            messages,
            message_ids,
            batch_ranges,
            active_owner,
            run_owners,
            active_runs,
            queue_cursor: queue_rows.last().map(|row| row.id),
            queue_rows,
            pending_interactions,
            last_durable_terminal,
            current_plan,
            plan_digest,
            context_usage,
            metadata: meta,
        };
        encode(&result, 16 * 1024 * 1024)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
    async fn list_sessions(
        &self,
        scope: &TenantContext,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<SessionListEntry>, CoordinationError> {
        if limit == 0 || limit > 1000 || after.is_some_and(|after| after.len() > 1024) {
            return Err(CoordinationError::InvalidRequest);
        }
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let mut values = scope_values(scope);
        values.extend([after.unwrap_or("").into(), limit.to_string()]);
        let rows=tx.query(&format!("SELECT CASE WHEN octet_length(session_id) BETWEEN 1 AND 1024 THEN session_id END FROM session_heads WHERE {SCOPE} AND NOT deleted AND session_id>$5 ORDER BY session_id LIMIT $6::text::bigint"),&parameters(&values)).await.map_err(unavailable)?;
        let mut entries = Vec::new();
        for row in rows {
            let key = SessionKey(field(&row, 0)?);
            let meta = metadata_in(&tx, scope, &key, false)
                .await?
                .ok_or(CoordinationError::Corrupt)?;
            let mut values = scope_values(scope);
            values.extend([key.0.clone(), meta.lifetime.0.clone()]);
            let state=tx.query_one(&format!("SELECT count(*) FILTER(WHERE status IN ('queued','running','waiting_interaction')),bool_or(status='waiting_interaction'),bool_or(status='running') FROM runs WHERE {SCOPE} AND session_id=$5 AND lifetime=$6"),&parameters(&values)).await.map_err(unavailable)?;
            let active_requests = unsigned(state.try_get(0).map_err(unavailable)?)? as usize;
            let waiting = state
                .try_get::<_, Option<bool>>(1)
                .map_err(unavailable)?
                .unwrap_or(false);
            let running = state
                .try_get::<_, Option<bool>>(2)
                .map_err(unavailable)?
                .unwrap_or(false);
            let status = if waiting {
                SessionStatus::Waiting
            } else if running {
                SessionStatus::Running
            } else {
                SessionStatus::Idle
            };
            let preview=tx.query_opt(&format!("SELECT substring(message->>'content' FROM 1 FOR 160) FROM transcript_batches CROSS JOIN LATERAL jsonb_array_elements(messages_json::jsonb) WITH ORDINALITY item(message,ordinal) WHERE {SCOPE} AND lifetime=$5 AND message->>'role'='user' AND message->>'content' IS NOT NULL ORDER BY start_seq,storage_row_id,ordinal LIMIT 1"),&parameters(&{let mut v=scope_values(scope);v.push(meta.lifetime.0.clone());v})).await.map_err(unavailable)?.map(|row|row.try_get::<_,Option<String>>(0).map_err(unavailable)).transpose()?.flatten();
            entries.push(SessionListEntry {
                metadata: meta,
                preview,
                status,
                active_requests,
            });
        }
        encode(&entries, 1024 * 1024)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(entries)
    }
}
