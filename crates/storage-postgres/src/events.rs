//! canonical events、提交版本和outbox同事务封存；NOTIFY仅唤醒，不承担持久事实。
use crate::canonical::{PgCanonicalStore, field, integer, set_scope, unsigned};
use crate::identities::exact_owner_in;
use crate::sessions::encode;
use crate::{SCOPE, now, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::{RunEventRepository, seal_event};
use sha2::{Digest, Sha256};
use tokio_postgres::Transaction;
pub(crate) async fn append_in(
    db: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    event: &str,
    data: &serde_json::Value,
) -> Result<EventSeq, CoordinationError> {
    if event.is_empty() || event.len() > 128 || !data.is_object() {
        return Err(CoordinationError::InvalidRequest);
    }
    encode(data, 4 * 1024 * 1024)?;
    let mut data = data.clone();
    if let Some(object) = data.as_object_mut() {
        for field in [
            "_my_agent_view",
            "_my_agent_replay",
            "_my_agent_view_decision",
        ] {
            object.remove(field);
        }
    }
    let raw = encode(&data, 4 * 1024 * 1024)?;
    let mut values = scope_values(&env.scope);
    values.extend([env.owner.run_id.0.clone(), now(db).await?.to_string()]);
    let row=db.query_opt(&format!("UPDATE runs SET last_seq=last_seq+1,updated_at_ms=$6::text::bigint WHERE {SCOPE} AND id=$5 RETURNING last_seq"),&parameters(&values)).await.map_err(unavailable)?.ok_or(CoordinationError::Forbidden)?;
    let seq = EventSeq(unsigned(row.try_get(0).map_err(unavailable)?)?);
    let mut insert = scope_values(&env.scope);
    insert.extend([
        env.owner.run_id.0.clone(),
        seq.0.to_string(),
        event.into(),
        raw,
        now(db).await?.to_string(),
    ]);
    db.execute("INSERT INTO events(organization_id,tenant_id,project_id,agent_uid,run_id,seq,event,data_json,created_at_ms) VALUES($1,$2,$3,$4,$5,$6::text::bigint,$7,$8,$9::text::bigint)",&parameters(&insert)).await.map_err(unavailable)?;
    let mut head = scope_values(&env.scope);
    head.push(env.owner.session_key.0.clone());
    let row=db.query_one(&format!("SELECT h.metadata_revision,h.revision,h.projection_generation,c.revision FROM session_heads h JOIN snapshot_clock c USING(organization_id,tenant_id,project_id,agent_uid) WHERE h.organization_id=$1 AND h.tenant_id=$2 AND h.project_id=$3 AND h.agent_uid=$4 AND h.session_id=$5 AND c.slot=1"),&parameters(&head)).await.map_err(unavailable)?;
    let visible = encode(VISIBLE_VIEW_EVENTS, 4096)?;
    let mut previous = scope_values(&env.scope);
    previous.extend([env.owner.run_id.0.clone(), seq.0.to_string(), visible]);
    let prev:i64=db.query_one(&format!("SELECT COALESCE(max(seq),0) FROM events WHERE {SCOPE} AND run_id=$5 AND seq<$6::text::bigint AND event IN (SELECT jsonb_array_elements_text($7::text::jsonb)) AND (event<>'run_started' OR data_json::jsonb->>'kind'='compact')"),&parameters(&previous)).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
    let interaction_id = data
        .get("interaction")
        .and_then(|v| v.get("interaction_id"))
        .or_else(|| data.get("interaction_id"))
        .or_else(|| data.get("approval").and_then(|v| v.get("id")))
        .and_then(serde_json::Value::as_str);
    let interaction = if let Some(id) = interaction_id {
        let mut values = scope_values(&env.scope);
        values.extend([env.owner.run_id.0.clone(), id.into()]);
        db.query_opt(
            &format!(
                "SELECT revision,status FROM interactions WHERE {SCOPE} AND run_id=$5 AND id=$6"
            ),
            &parameters(&values),
        )
        .await
        .map_err(unavailable)?
        .map(|row| {
            Ok(ViewInteraction {
                interaction_id: InteractionId(id.into()),
                revision: row.try_get(0).map_err(unavailable)?,
                pending: row.try_get::<_, String>(1).map_err(unavailable)? == "pending",
            })
        })
        .transpose()?
    } else {
        None
    };
    let terminal = event == "compact_terminal"
        || (event == "terminal"
            && data
                .get("status")
                .and_then(serde_json::Value::as_str)
                .map(RunStatus::parse)
                .transpose()
                .map_err(|_| CoordinationError::InvalidRequest)?
                .is_some_and(|s| s.terminal()));
    let stamp = ViewStamp {
        schema_version: 1,
        session_key: env.owner.session_key.clone(),
        session_lifetime_id: env.owner.session_lifetime_id.clone(),
        snapshot_revision: SnapshotRevision(unsigned(row.try_get(3).map_err(unavailable)?)?),
        metadata_revision: SnapshotRevision(unsigned(row.try_get(0).map_err(unavailable)?)?),
        transcript_revision: TranscriptSeq(unsigned(row.try_get(1).map_err(unavailable)?)?),
        projection_generation: ProjectionGeneration(unsigned(
            row.try_get(2).map_err(unavailable)?,
        )?),
        owner: Some(env.owner.clone()),
        event_seq: Some(seq),
        previous_visible_seq: Some(EventSeq(unsigned(prev)?)),
        terminal,
        interaction,
    };
    if !stamp.valid() {
        return Err(CoordinationError::Corrupt);
    }
    let raw = encode(&stamp, 16384)?;
    let hash = format!("{:x}", Sha256::digest(raw.as_bytes()));
    let mut values = scope_values(&env.scope);
    values.extend([env.owner.run_id.0.clone(), seq.0.to_string(), raw, hash]);
    db.execute("INSERT INTO event_view_stamps(organization_id,tenant_id,project_id,agent_uid,run_id,seq,stamp_json,stamp_digest,published) VALUES($1,$2,$3,$4,$5,$6::text::bigint,$7,$8,FALSE)",&parameters(&values)).await.map_err(unavailable)?;
    Ok(seq)
}
pub(crate) async fn seal_in(
    db: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
) -> Result<(), CoordinationError> {
    let mut values = scope_values(&env.scope);
    values.push(env.owner.run_id.0.clone());
    let rows=db.query(&format!("SELECT seq,CASE WHEN octet_length(stamp_json)<=16384 THEN stamp_json END,stamp_digest FROM event_view_stamps WHERE {SCOPE} AND run_id=$5 AND NOT published ORDER BY seq LIMIT 65"),&parameters(&values)).await.map_err(unavailable)?;
    if rows.len() > 64 {
        return Err(CoordinationError::InvalidRequest);
    }
    let head=db.query_one("SELECT h.lifetime,h.metadata_revision,h.revision,h.projection_generation,c.revision FROM session_heads h JOIN snapshot_clock c USING(organization_id,tenant_id,project_id,agent_uid) WHERE h.organization_id=$1 AND h.tenant_id=$2 AND h.project_id=$3 AND h.agent_uid=$4 AND h.session_id=$5 AND c.slot=1",&parameters(&{let mut v=scope_values(&env.scope);v.push(env.owner.session_key.0.clone());v})).await.map_err(unavailable)?;
    if head.try_get::<_, String>(0).map_err(unavailable)? != env.owner.session_lifetime_id.0 {
        return Err(CoordinationError::StaleLease);
    }
    let owner = exact_owner_in(db, &env.scope, &env.owner.run_id).await?;
    if owner != env.owner {
        return Err(CoordinationError::Corrupt);
    }
    for row in rows {
        let seq = EventSeq(unsigned(row.try_get(0).map_err(unavailable)?)?);
        let raw = field(&row, 1)?;
        if format!("{:x}", Sha256::digest(raw.as_bytes()))
            != row.try_get::<_, String>(2).map_err(unavailable)?
        {
            return Err(CoordinationError::Corrupt);
        }
        let mut stamp: ViewStamp =
            serde_json::from_str(&raw).map_err(|_| CoordinationError::Corrupt)?;
        if !stamp.valid()
            || stamp.owner.as_ref() != Some(&env.owner)
            || stamp.event_seq != Some(seq)
        {
            return Err(CoordinationError::Corrupt);
        }
        let mut event_query = values.clone();
        event_query.push(seq.0.to_string());
        let event=db.query_one(&format!("SELECT event,CASE WHEN octet_length(data_json)<=4194304 THEN data_json END,(SELECT status FROM runs WHERE {SCOPE} AND id=$5) FROM events WHERE {SCOPE} AND run_id=$5 AND seq=$6::text::bigint"),&parameters(&event_query)).await.map_err(unavailable)?;
        if stamp.terminal
            && !RunStatus::parse(&event.try_get::<_, String>(2).map_err(unavailable)?)
                .map_err(|_| CoordinationError::Corrupt)?
                .terminal()
        {
            return Err(CoordinationError::Corrupt);
        }
        stamp.metadata_revision =
            SnapshotRevision(unsigned(head.try_get(1).map_err(unavailable)?)?);
        stamp.transcript_revision = TranscriptSeq(unsigned(head.try_get(2).map_err(unavailable)?)?);
        stamp.projection_generation =
            ProjectionGeneration(unsigned(head.try_get(3).map_err(unavailable)?)?);
        stamp.snapshot_revision =
            SnapshotRevision(unsigned(head.try_get(4).map_err(unavailable)?)?);
        let stamp_raw = encode(&stamp, 16384)?;
        let stamp_hash = format!("{:x}", Sha256::digest(stamp_raw.as_bytes()));
        let mut stamp_values = event_query;
        stamp_values.extend([stamp_raw, stamp_hash]);
        db.execute(&format!("UPDATE event_view_stamps SET stamp_json=$7,stamp_digest=$8,published=TRUE WHERE {SCOPE} AND run_id=$5 AND seq=$6::text::bigint AND NOT published"),&parameters(&stamp_values)).await.map_err(unavailable)?;
        let data =
            serde_json::from_str(&field(&event, 1)?).map_err(|_| CoordinationError::Corrupt)?;
        let publication = OutboxEvent {
            envelope: env.clone(),
            event_seq: seq,
            event: event.try_get(0).map_err(unavailable)?,
            data,
            stamp,
        };
        let (id, digest, payload) = seal_event(&publication)?;
        let mut outbox = scope_values(&env.scope);
        outbox.extend([
            env.owner.session_key.0.clone(),
            env.owner.session_lifetime_id.0.clone(),
            env.owner.run_id.0.clone(),
            seq.0.to_string(),
            id,
            payload,
            digest,
        ]);
        db.execute("INSERT INTO event_outbox(organization_id,tenant_id,project_id,agent_uid,session_key,lifetime,run_id,event_seq,event_id,payload_json,payload_digest) VALUES($1,$2,$3,$4,$5,$6,$7,$8::text::bigint,$9,$10,$11)",&parameters(&outbox)).await.map_err(unavailable)?;
    }
    let payload = format!("{:x}", Sha256::digest(encode(&env.scope, 4096)?.as_bytes()));
    db.query_one("SELECT pg_notify('agent_event_outbox',$1)", &[&payload])
        .await
        .map_err(unavailable)?;
    Ok(())
}
async fn decode_event(
    db: &Transaction<'_>,
    scope: &TenantContext,
    row: tokio_postgres::Row,
) -> Result<StoredEvent, CoordinationError> {
    let run = RunId(field(&row, 0)?);
    let seq = EventSeq(unsigned(row.try_get(1).map_err(unavailable)?)?);
    let event = field(&row, 2)?;
    let mut data: serde_json::Value =
        serde_json::from_str(&field(&row, 3)?).map_err(|_| CoordinationError::Corrupt)?;
    if !data.is_object() {
        return Err(CoordinationError::Corrupt);
    }
    let provenance: String = row.try_get(4).map_err(unavailable)?;
    if let Some(stamp) =
        crate::facts::stamp_in(db, scope, &exact_owner_in(db, scope, &run).await?, seq).await?
    {
        data.as_object_mut()
            .ok_or(CoordinationError::Corrupt)?
            .insert(
                "_my_agent_view".into(),
                serde_json::to_value(stamp).map_err(|_| CoordinationError::Corrupt)?,
            );
    } else if provenance != "legacy" {
        return Err(CoordinationError::Corrupt);
    }
    Ok(StoredEvent {
        run_id: run,
        seq,
        event,
        data,
    })
}
const EVENT: &str = "CASE WHEN octet_length(run_id) BETWEEN 1 AND 1024 THEN run_id END,seq,CASE WHEN octet_length(event) BETWEEN 1 AND 128 THEN event END,CASE WHEN octet_length(data_json)<=4194304 THEN data_json END,view_provenance";
#[async_trait::async_trait]
impl RunEventRepository for PgCanonicalStore {
    async fn append_run_live_event(
        &self,
        env: &EnterpriseOwnerEnvelope,
        event: &str,
        data: &serde_json::Value,
    ) -> Result<EventSeq, RepositoryError> {
        if !is_run_live_event(event) || !data.is_object() {
            return Err(CoordinationError::InvalidRequest.into());
        }
        encode(data, 1024 * 1024 - 17408)?;
        let proof = self
            .bound_proof(&SessionWriterLeaseKey {
                scope: env.scope.clone(),
                session_key: env.owner.session_key.clone(),
                session_lifetime_id: env.owner.session_lifetime_id.clone(),
            })
            .await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        Self::fence_envelope_in(&tx, &proof, env).await?;
        let mut values = scope_values(&env.scope);
        values.push(env.owner.run_id.0.clone());
        let state: String = tx
            .query_one(
                &format!("SELECT status FROM runs WHERE {SCOPE} AND id=$5 FOR UPDATE"),
                &parameters(&values),
            )
            .await
            .map_err(unavailable)?
            .try_get(0)
            .map_err(unavailable)?;
        if state != "running" && state != "waiting_interaction" {
            return Err(CoordinationError::Conflict.into());
        }
        if event == "approval_required" {
            crate::interactions::publish_approval_in(&tx, env, data).await?;
        }
        let seq = append_in(&tx, env, event, data).await?;
        seal_in(&tx, env).await?;
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(seq)
    }
    async fn read_run_view_event(
        &self,
        scope: &TenantContext,
        run: &RunId,
        event: &str,
        child: Option<&RunId>,
    ) -> Result<Option<StoredEvent>, CoordinationError> {
        if event.is_empty() || event.len() > 128 {
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
        exact_owner_in(&tx, scope, run).await?;
        if let Some(child) = child {
            exact_owner_in(&tx, scope, child).await?;
        }
        let mut values = scope_values(scope);
        values.extend([run.0.clone(), event.into()]);
        let child_value = child.map(|run| run.0.as_str());
        let mut params = parameters(&values);
        params.push(&child_value);
        let row=tx.query_opt(&format!("SELECT {EVENT} FROM events WHERE {SCOPE} AND run_id=$5 AND event=$6 AND ($7::text IS NULL OR data_json::jsonb->>'child_run_id'=$7::text) ORDER BY seq DESC LIMIT 1"),&params).await.map_err(unavailable)?;
        let result = if let Some(row) = row {
            Some(decode_event(&tx, scope, row).await?)
        } else {
            None
        };
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
    async fn read_run_events(
        &self,
        scope: &TenantContext,
        run: &RunId,
        after: EventSeq,
        limit: u32,
    ) -> Result<RunEventPage, CoordinationError> {
        if limit == 0 || limit > 256 {
            return Err(CoordinationError::InvalidRequest);
        }
        integer(after.0)?;
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        exact_owner_in(&tx, scope, run).await?;
        let mut values = scope_values(scope);
        values.extend([run.0.clone(), after.0.to_string(), (limit + 1).to_string()]);
        let rows=tx.query(&format!("SELECT {EVENT} FROM events WHERE {SCOPE} AND run_id=$5 AND seq>$6::text::bigint ORDER BY seq LIMIT $7::text::bigint"),&parameters(&values)).await.map_err(unavailable)?;
        let has_more = rows.len() > limit as usize;
        let mut events = Vec::new();
        let mut bytes = 0;
        for row in rows.into_iter().take(limit as usize) {
            let event = decode_event(&tx, scope, row).await?;
            bytes += encode(&event, 4 * 1024 * 1024)?.len();
            if bytes > 4 * 1024 * 1024 {
                return Err(CoordinationError::InvalidRequest);
            }
            events.push(event);
        }
        let cursor = events.last().map_or(after, |event| event.seq);
        tx.commit().await.map_err(unavailable)?;
        Ok(RunEventPage {
            events,
            cursor,
            has_more,
        })
    }
}
