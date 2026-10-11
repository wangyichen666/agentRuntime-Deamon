//! canonical 记忆：作用域、来源、曝光、人工反馈和一次性摄入均复用原表。
use crate::canonical::{
    PgCanonicalStore, field, grant_in, integer, permanent_life, proof_key, set_scope, unsigned,
};
use crate::executions::record_in;
use crate::facts::frozen_in;
use crate::identities::exact_owner_in;
use crate::sessions::{encode, metadata_in};
use crate::{SCOPE, now, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::{
    LegacyMemoryImportRepository, MemoryEpisodeExcerpt, MemoryMaintenanceRepository,
    ScopedMemoryRepository, decode_transcript_batch, redact_memory_evidence,
};
use sha2::{Digest, Sha256};
use tokio_postgres::Transaction;
const RECORD_BYTES: usize = 262144;
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn id_input(id: &str) -> Result<(), CoordinationError> {
    if id.is_empty() || id.len() > 512 {
        Err(CoordinationError::InvalidRequest)
    } else {
        Ok(())
    }
}
fn read_input(key: &SessionKey, visible: &MemoryVisibility) -> Result<(), CoordinationError> {
    if key.0.is_empty()
        || key.0.len() > 1024
        || visible.lifetime.0.is_empty()
        || visible.lifetime.0.len() > 128
        || visible.project.len() > 16384
    {
        Err(CoordinationError::InvalidRequest)
    } else {
        Ok(())
    }
}
fn validate_entry(entry: &MemoryRecord) -> Result<(), CoordinationError> {
    id_input(&entry.id)?;
    integer(entry.revision)?;
    if entry.content.is_empty()
        || entry.content.len() > 32768
        || entry.confidence > 100
        || entry.content_digest != digest(entry.content.as_bytes())
        || entry.source_message_ids.len() > 2048
        || entry
            .source_message_ids
            .iter()
            .any(|id| id.is_empty() || id.len() > 512)
    {
        return Err(CoordinationError::InvalidRequest);
    }
    encode(entry, RECORD_BYTES)?;
    Ok(())
}
async fn visibility_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    key: &SessionKey,
    visible: &MemoryVisibility,
) -> Result<(), RepositoryError> {
    read_input(key, visible)?;
    let meta = metadata_in(db, scope, key, false)
        .await?
        .filter(|meta| !meta.deleted && meta.lifetime == visible.lifetime)
        .ok_or(AuthorizationError::StaleOwner)?;
    permanent_life(
        db,
        &SessionWriterLeaseKey {
            scope: scope.clone(),
            session_key: meta.key,
            session_lifetime_id: meta.lifetime,
        },
    )
    .await?;
    Ok(())
}
pub(crate) async fn read_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    id: &str,
) -> Result<Option<MemoryRecord>, CoordinationError> {
    let mut values = scope_values(scope);
    values.push(id.into());
    let row=db.query_opt(&format!("SELECT CASE WHEN octet_length(data_json) BETWEEN 1 AND 262144 THEN data_json END,lifetime IS NULL,CASE WHEN octet_length(lifetime)<=128 THEN lifetime END,project IS NULL,CASE WHEN octet_length(project)<=16384 THEN project END,scope,CASE WHEN octet_length(source_run) BETWEEN 1 AND 1024 THEN source_run END,revision FROM memories WHERE {SCOPE} AND id=$5"),&parameters(&values)).await.map_err(unavailable)?;
    let Some(row) = row else { return Ok(None) };
    let entry: MemoryRecord =
        serde_json::from_str(&field(&row, 0)?).map_err(|_| CoordinationError::Corrupt)?;
    validate_entry(&entry).map_err(|_| CoordinationError::Corrupt)?;
    let source = entry.source.as_ref().ok_or(CoordinationError::Corrupt)?;
    let life_null: bool = row.try_get(1).map_err(unavailable)?;
    let project_null: bool = row.try_get(3).map_err(unavailable)?;
    let bound = match &entry.scope {
        MemoryScope::Session(life) => {
            field(&row, 5)? == "session" && !life_null && field(&row, 2)? == life.0 && project_null
        }
        MemoryScope::Project(project) => {
            field(&row, 5)? == "project"
                && !project_null
                && field(&row, 4)? == *project
                && life_null
                && entry.confirmed_by_user
        }
        MemoryScope::Global => {
            field(&row, 5)? == "global"
                && life_null
                && project_null
                && entry.confirmed_by_user
                && entry.layer == MemoryLayer::Semantic
        }
        MemoryScope::Legacy(_) => false,
    };
    if entry.id != id
        || !bound
        || source.run_id.0 != field(&row, 6)?
        || entry.revision != unsigned(row.try_get(7).map_err(unavailable)?)?
    {
        return Err(CoordinationError::Corrupt);
    }
    let actual = exact_owner_in(db, scope, &source.run_id)
        .await
        .map_err(|error| {
            if error == CoordinationError::Forbidden {
                CoordinationError::Corrupt
            } else {
                error
            }
        })?;
    if actual != *source {
        return Err(CoordinationError::Corrupt);
    }
    Ok(Some(entry))
}
async fn write_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    entry: &MemoryRecord,
) -> Result<(), CoordinationError> {
    validate_entry(entry)?;
    let (kind, life, project) = match &entry.scope {
        MemoryScope::Session(life) => ("session", Some(life.0.as_str()), None),
        MemoryScope::Project(project) => ("project", None, Some(project.as_str())),
        MemoryScope::Global => ("global", None, None),
        MemoryScope::Legacy(_) => ("legacy", None, None),
    };
    let raw = encode(entry, RECORD_BYTES)?;
    let source = entry.source.as_ref().map(|owner| owner.run_id.0.as_str());
    let revision = integer(entry.revision)?;
    let values = scope_values(scope);
    let mut params = parameters(&values);
    params.extend([
        &entry.id as &(dyn tokio_postgres::types::ToSql + Sync),
        &life,
        &project,
        &kind,
        &source,
        &revision,
        &raw,
    ]);
    db.execute("INSERT INTO memories(organization_id,tenant_id,project_id,agent_uid,id,lifetime,project,scope,source_run,revision,data_json) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",&params).await.map_err(unavailable)?;
    Ok(())
}
async fn list_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    key: &SessionKey,
    visible: &MemoryVisibility,
    after: Option<&str>,
    limit: usize,
    paged: bool,
) -> Result<Vec<MemoryRecord>, RepositoryError> {
    visibility_in(db, scope, key, visible).await?;
    if after.is_some_and(|id| id.len() > 512) {
        return Err(CoordinationError::InvalidRequest.into());
    }
    let at = unsigned(now(db).await?)? / 1000;
    let mut values = scope_values(scope);
    values.extend([
        visible.lifetime.0.clone(),
        visible.project.clone(),
        visible.allow_confirmed_global.to_string(),
        after.unwrap_or("").into(),
        paged.to_string(),
        if paged { limit.clamp(1, 101) } else { 1000 }.to_string(),
        at.to_string(),
    ]);
    let order = if paged { "id" } else { "storage_row_id DESC" };
    // 无效保存 JSON/expiry 留给唯一 decoder 返回 Corrupt，合法过期项在 LIMIT 之前过滤。
    let rows=db.query(&format!("SELECT CASE WHEN octet_length(id) BETWEEN 1 AND 512 THEN id END,octet_length(data_json) FROM memories WHERE {SCOPE} AND (NOT $9::text::boolean OR id>$8) AND ((scope='session' AND lifetime=$5) OR (scope='project' AND project=$6) OR (scope='global' AND $7::text::boolean)) AND CASE WHEN octet_length(data_json)<=262144 AND pg_input_is_valid(data_json,'jsonb') THEN CASE WHEN data_json::jsonb->>'expires_at' IS NULL THEN TRUE WHEN pg_input_is_valid(data_json::jsonb->>'expires_at','numeric') THEN (data_json::jsonb->>'expires_at')::numeric>$11::text::numeric ELSE TRUE END ELSE TRUE END ORDER BY {order} LIMIT $10::text::bigint"),&parameters(&values)).await.map_err(unavailable)?;
    let mut bytes = 0usize;
    let mut output = Vec::new();
    for row in rows {
        let size: i32 = row.try_get(1).map_err(unavailable)?;
        if size < 0 || size as usize > RECORD_BYTES {
            return Err(CoordinationError::Corrupt.into());
        }
        bytes += size as usize;
        if bytes > 4 * 1024 * 1024 {
            return Err(CoordinationError::Corrupt.into());
        }
        let entry = read_in(db, scope, &field(&row, 0)?)
            .await?
            .ok_or(CoordinationError::Corrupt)?;
        if !visible.allows(&entry, at) {
            return Err(CoordinationError::Corrupt.into());
        }
        output.push(entry);
    }
    Ok(output)
}
pub(crate) async fn source_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    owner: &ExactOwner,
    id: &str,
    char_limit: usize,
) -> Result<MemorySourceEvidence, CoordinationError> {
    id_input(id)?;
    let (life, seq) = id
        .rsplit_once(':')
        .ok_or(CoordinationError::InvalidRequest)?;
    let seq = seq
        .parse::<u64>()
        .map_err(|_| CoordinationError::InvalidRequest)?;
    integer(seq)?;
    if life != owner.session_lifetime_id.0 {
        return Err(CoordinationError::Forbidden);
    }
    let mut values = scope_values(scope);
    values.extend([life.into(), owner.run_id.0.clone(), seq.to_string()]);
    let row=db.query_opt(&format!("SELECT CASE WHEN octet_length(messages_json)<=4194304 THEN messages_json END,start_seq,end_seq,CASE WHEN octet_length(digest)=64 THEN digest END FROM transcript_batches WHERE {SCOPE} AND lifetime=$5 AND run_id=$6 AND start_seq<=$7::text::bigint AND end_seq>$7::text::bigint ORDER BY start_seq DESC LIMIT 1"),&parameters(&values)).await.map_err(unavailable)?.ok_or(CoordinationError::Conflict)?;
    let start = unsigned(row.try_get(1).map_err(unavailable)?)?;
    let end = unsigned(row.try_get(2).map_err(unavailable)?)?;
    let batch = decode_transcript_batch(&field(&row, 0)?, &field(&row, 3)?, start, end)?;
    let message = batch
        .get((seq - start) as usize)
        .ok_or(CoordinationError::Corrupt)?;
    let safe = redact_memory_evidence(
        message.content.as_deref().unwrap_or_default(),
        char_limit + 1,
    );
    Ok(MemorySourceEvidence {
        source_id: id.into(),
        role: message.role.clone(),
        truncated: safe.chars().count() > char_limit,
        text: safe.chars().take(char_limit).collect(),
    })
}
#[derive(Default)]
struct MemoryResult {
    record: Option<MemoryRecord>,
    forgotten: bool,
}
enum Mutation<'a> {
    Store(&'a MemoryRecord),
    Forget(&'a MemoryVisibility, &'a str, u64),
    Exposure(&'a [MemoryExposure], &'a str, &'a str),
    Feedback(&'a str, &'a str, MemoryFeedback),
    Ingest,
    Maintenance(&'a str),
}
impl PgCanonicalStore {
    async fn memory_mutation(
        &self,
        env: &EnterpriseOwnerEnvelope,
        command: Mutation<'_>,
    ) -> Result<MemoryResult, RepositoryError> {
        let key = SessionWriterLeaseKey {
            scope: env.scope.clone(),
            session_key: env.owner.session_key.clone(),
            session_lifetime_id: env.owner.session_lifetime_id.clone(),
        };
        let id = proof_key(&key)?;
        let mut proofs = self.proofs.lock().await;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        let proof = if let Some(proof) = proofs.get(&id) {
            Self::fence_in(&tx, proof).await?;
            proof.clone()
        } else {
            if proofs.len() >= 1024 {
                return Err(CoordinationError::Conflict.into());
            }
            grant_in(&tx, &key, &self.writer_owner, 30000).await?
        };
        Self::fence_envelope_in(&tx, &proof, env).await?;
        let frozen = frozen_in(&tx, &env.scope, &env.owner.run_id)
            .await?
            .ok_or(CoordinationError::Corrupt)?;
        let mut output = MemoryResult::default();
        if frozen.context_read_only
            && !matches!(command, Mutation::Exposure(..) | Mutation::Maintenance(..))
        {
            return Err(AuthorizationError::Forbidden.into());
        }
        let at = now(&tx).await?;
        let scope = &env.scope;
        let owner = &env.owner;
        match command {
            Mutation::Store(entry) => {
                validate_entry(entry)?;
                if entry.source.as_ref() != Some(owner) {
                    return Err(AuthorizationError::StaleOwner.into());
                }
                match &entry.scope {
                    MemoryScope::Session(life) if life == &owner.session_lifetime_id => {}
                    MemoryScope::Project(project)
                        if entry.confirmed_by_user && project == &frozen.cwd => {}
                    MemoryScope::Global
                        if entry.confirmed_by_user && entry.layer == MemoryLayer::Semantic => {}
                    _ => return Err(AuthorizationError::Forbidden.into()),
                }
                if let Some(previous) = read_in(&tx, scope, &entry.id).await? {
                    if previous.content_digest != entry.content_digest
                        || previous.scope != entry.scope
                        || previous.source != entry.source
                        || previous.kind != entry.kind
                        || previous.layer != entry.layer
                        || previous.confirmed_by_user != entry.confirmed_by_user
                        || previous
                            .expires_at
                            .map(|n| n.saturating_sub(previous.created_at))
                            != entry.expires_at.map(|n| n.saturating_sub(entry.created_at))
                    {
                        return Err(CoordinationError::Conflict.into());
                    }
                    output.record = Some(previous);
                } else {
                    let mut values = scope_values(scope);
                    values.push(entry.id.clone());
                    let forgotten:bool=tx.query_one(&format!("SELECT EXISTS(SELECT 1 FROM memory_forget_receipts WHERE {SCOPE} AND memory_id=$5 AND result)"),&parameters(&values)).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
                    if forgotten {
                        return Err(CoordinationError::Conflict.into());
                    }
                    write_in(&tx, scope, entry).await?;
                    output.record = Some(entry.clone());
                }
            }
            Mutation::Forget(visible, id, revision) => {
                id_input(id)?;
                read_input(&owner.session_key, visible)?;
                integer(revision)?;
                if visible.lifetime != owner.session_lifetime_id || visible.project != frozen.cwd {
                    return Err(AuthorizationError::Forbidden.into());
                }
                let mut values = scope_values(scope);
                values.extend([owner.run_id.0.clone(), id.into(), revision.to_string()]);
                if let Some(row)=tx.query_opt(&format!("SELECT result FROM memory_forget_receipts WHERE {SCOPE} AND run_id=$5 AND memory_id=$6 AND revision=$7::text::bigint"),&parameters(&values)).await.map_err(unavailable)?{output.forgotten=row.try_get(0).map_err(unavailable)?;}else{
                    if let Some(entry)=read_in(&tx,scope,id).await?{if !visible.allows(&entry,unsigned(at)?/1000)||entry.revision!=revision{return Err(CoordinationError::Conflict.into());}
                        tx.execute(&format!("DELETE FROM memories WHERE {SCOPE} AND id=$5 AND revision=$6::text::bigint"),&parameters(&values[..4].iter().cloned().chain(values[5..].iter().cloned()).collect::<Vec<_>>())).await.map_err(unavailable)?;
                        output.forgotten=true;
                    }
                    values.push(output.forgotten.to_string());tx.execute("INSERT INTO memory_forget_receipts VALUES($1,$2,$3,$4,$5,$6,$7::text::bigint,$8::text::boolean)",&parameters(&values)).await.map_err(unavailable)?;
                }
            }
            Mutation::Exposure(entries, channel, policy) => {
                if entries.len() > 20
                    || !matches!(channel, "context_prepared" | "tool_result_prepared")
                    || policy.len() > 80
                {
                    return Err(CoordinationError::InvalidRequest.into());
                }
                encode(entries, 16384)?;
                if !frozen.context_read_only {
                    let visible = MemoryVisibility {
                        lifetime: owner.session_lifetime_id.clone(),
                        project: frozen.cwd.clone(),
                        allow_confirmed_global: true,
                    };
                    for exposure in entries {
                        id_input(&exposure.id)?;
                        integer(exposure.revision)?;
                        let Some(saved) = read_in(&tx, scope, &exposure.id).await? else {
                            continue;
                        };
                        if !visible.allows(&saved, unsigned(at)? / 1000)
                            || saved.content_digest != exposure.content_digest
                            || saved.revision != exposure.revision
                        {
                            return Err(CoordinationError::Conflict.into());
                        }
                        let mut values = scope_values(scope);
                        values.extend([
                            owner.run_id.0.clone(),
                            exposure.id.clone(),
                            owner.session_lifetime_id.0.clone(),
                            exposure.content_digest.clone(),
                            exposure.revision.to_string(),
                            channel.into(),
                            policy.into(),
                            at.to_string(),
                        ]);
                        tx.execute("INSERT INTO memory_exposures VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::text::bigint,$10,$11,$12::text::bigint) ON CONFLICT DO NOTHING",&parameters(&values)).await.map_err(unavailable)?;
                    }
                }
            }
            Mutation::Feedback(operation, id, feedback) => {
                id_input(id)?;
                if operation.is_empty() || operation.len() > 128 {
                    return Err(CoordinationError::InvalidRequest.into());
                }
                let command = encode(&(id, feedback), 4096)?;
                let mut values = scope_values(scope);
                values.extend([owner.run_id.0.clone(), operation.into()]);
                if let Some(row)=tx.query_opt(&format!("SELECT CASE WHEN octet_length(command_json)<=4096 THEN command_json END,memory_id,lifetime FROM memory_feedback_receipts WHERE {SCOPE} AND run_id=$5 AND operation_id=$6"),&parameters(&values)).await.map_err(unavailable)?{
                    if field(&row,0)?!=command{return Err(CoordinationError::Conflict.into());}if field(&row,1)?!=id||field(&row,2)?!=owner.session_lifetime_id.0{return Err(CoordinationError::Corrupt.into());}
                }else{
                    if !record_in(&tx,scope,&owner.run_id).await?.status.terminal(){return Err(CoordinationError::Conflict.into());}
                    let saved=read_in(&tx,scope,id).await?.ok_or(CoordinationError::Conflict)?;
                    let mut exposure=scope_values(scope);exposure.extend([owner.run_id.0.clone(),id.into(),owner.session_lifetime_id.0.clone()]);
                    let row=tx.query_opt(&format!("SELECT CASE WHEN octet_length(content_digest)=64 THEN content_digest END,revision FROM memory_exposures WHERE {SCOPE} AND run_id=$5 AND memory_id=$6 AND lifetime=$7"),&parameters(&exposure)).await.map_err(unavailable)?.ok_or(CoordinationError::Conflict)?;
                    if field(&row,0)?!=saved.content_digest||unsigned(row.try_get(1).map_err(unavailable)?)?!=saved.revision{return Err(CoordinationError::Conflict.into());}
                    let mut assessment=scope_values(scope);assessment.extend([owner.session_lifetime_id.0.clone(),id.into(),saved.content_digest,encode(&feedback,64)?,at.to_string()]);
                    tx.execute("INSERT INTO memory_assessments VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::text::bigint) ON CONFLICT(organization_id,tenant_id,project_id,agent_uid,lifetime,memory_id) DO UPDATE SET content_digest=excluded.content_digest,feedback=excluded.feedback,updated_at_ms=excluded.updated_at_ms",&parameters(&assessment)).await.map_err(unavailable)?;
                    values.extend([id.into(),owner.session_lifetime_id.0.clone(),command]);tx.execute("INSERT INTO memory_feedback_receipts VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",&parameters(&values)).await.map_err(unavailable)?;
                }
            }
            Mutation::Ingest => {
                ingest_in(&tx, env, at).await?;
            }
            Mutation::Maintenance(outcome) => {
                if !matches!(outcome, "completed" | "failed" | "skipped_read_only") {
                    return Err(CoordinationError::InvalidRequest.into());
                }
                diagnostic_in(&tx, env, outcome).await?;
            }
        }
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        proofs.insert(id, proof);
        Ok(output)
    }
}
pub(crate) async fn diagnostic_in(
    db: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    outcome: &str,
) -> Result<(), CoordinationError> {
    let mut values = scope_values(&env.scope);
    values.extend([env.owner.run_id.0.clone(), outcome.into()]);
    db.execute("INSERT INTO maintenance_diagnostics VALUES($1,$2,$3,$4,$5,'memory_ingest',$6) ON CONFLICT(organization_id,tenant_id,project_id,agent_uid,run_id,kind) DO UPDATE SET diagnostic=excluded.diagnostic",&parameters(&values)).await.map_err(unavailable)?;
    Ok(())
}
async fn ingest_in(
    db: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    at: i64,
) -> Result<(), RepositoryError> {
    let owner = &env.owner;
    let scope = &env.scope;
    let record = record_in(db, scope, &owner.run_id).await?;
    if record.kind == RunKind::Compact || record.status != RunStatus::Completed {
        return Err(CoordinationError::Conflict.into());
    }
    let mut values = scope_values(scope);
    values.push(owner.run_id.0.clone());
    let row=db.query_one(&format!("SELECT EXISTS(SELECT 1 FROM turn_commits WHERE {SCOPE} AND run_id=$5 AND status='completed'),EXISTS(SELECT 1 FROM memory_ingests WHERE {SCOPE} AND run_id=$5)"),&parameters(&values)).await.map_err(unavailable)?;
    if !row.try_get::<_, bool>(0).map_err(unavailable)? {
        return Err(CoordinationError::Conflict.into());
    }
    if row.try_get::<_, bool>(1).map_err(unavailable)? {
        return Ok(());
    }
    let budget=db.query_one(&format!("SELECT count(*)::bigint,COALESCE(sum(octet_length(messages_json)),0)::bigint,COALESCE(sum(end_seq-start_seq),0)::bigint FROM (SELECT messages_json,start_seq,end_seq FROM transcript_batches WHERE {SCOPE} AND run_id=$5 LIMIT 32769) b"),&parameters(&values)).await.map_err(unavailable)?;
    if budget.try_get::<_, i64>(0).map_err(unavailable)? > 32768
        || budget.try_get::<_, i64>(1).map_err(unavailable)? > 16777216
        || !(0..=32768).contains(&budget.try_get::<_, i64>(2).map_err(unavailable)?)
    {
        return Err(CoordinationError::Corrupt.into());
    }
    let rows=db.query(&format!("SELECT CASE WHEN octet_length(messages_json)<=4194304 THEN messages_json END,CASE WHEN octet_length(digest)=64 THEN digest END,start_seq,end_seq,CASE WHEN octet_length(lifetime)<=128 THEN lifetime END FROM transcript_batches WHERE {SCOPE} AND run_id=$5 ORDER BY start_seq,storage_row_id LIMIT 32768"),&parameters(&values)).await.map_err(unavailable)?;
    let mut excerpt = MemoryEpisodeExcerpt::default();
    let mut previous = 0;
    for row in rows {
        let start = unsigned(row.try_get(2).map_err(unavailable)?)?;
        let end = unsigned(row.try_get(3).map_err(unavailable)?)?;
        if start < previous || field(&row, 4)? != owner.session_lifetime_id.0 {
            return Err(CoordinationError::Corrupt.into());
        }
        previous = end;
        let batch = decode_transcript_batch(&field(&row, 0)?, &field(&row, 1)?, start, end)?;
        for (index, message) in batch.iter().enumerate() {
            excerpt.push(
                message,
                format!("{}:{}", owner.session_lifetime_id.0, start + index as u64),
            );
        }
    }
    let time = unsigned(at)? / 1000;
    let entry = MemoryRecord {
        id: format!("turn:{}", owner.run_id.0),
        layer: MemoryLayer::Episode,
        scope: MemoryScope::Session(owner.session_lifetime_id.clone()),
        kind: MemoryKind::TurnSummary,
        content_digest: digest(excerpt.text.as_bytes()),
        content: excerpt.text,
        source: Some(owner.clone()),
        source_message_ids: excerpt.source_message_ids,
        event_time: time,
        created_at: time,
        updated_at: time,
        expires_at: None,
        confidence: 70,
        confirmed_by_user: false,
        revision: 0,
    };
    let mut lookup = scope_values(scope);
    lookup.push(entry.id.clone());
    let forgotten:bool=db.query_one(&format!("SELECT EXISTS(SELECT 1 FROM memory_forget_receipts WHERE {SCOPE} AND memory_id=$5 AND result)"),&parameters(&lookup)).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
    if !entry.content.is_empty() && !forgotten {
        if read_in(db, scope, &entry.id).await?.is_some() {
            return Err(CoordinationError::Conflict.into());
        }
        write_in(db, scope, &entry).await?;
    }
    values.push(owner.session_lifetime_id.0.clone());
    db.execute(
        "INSERT INTO memory_ingests VALUES($1,$2,$3,$4,$5,$6,'completed',NULL)",
        &parameters(&values),
    )
    .await
    .map_err(unavailable)?;
    diagnostic_in(db, env, "completed").await?;
    Ok(())
}
#[async_trait::async_trait]
impl ScopedMemoryRepository for PgCanonicalStore {
    async fn read_memory_settings(
        &self,
        env: &EnterpriseOwnerEnvelope,
    ) -> Result<RunSnapshot, RepositoryError> {
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
        let result = frozen_in(&tx, &env.scope, &env.owner.run_id)
            .await?
            .ok_or(CoordinationError::Corrupt)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
    async fn read_memory_candidates(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        visible: &MemoryVisibility,
    ) -> Result<Vec<MemoryRecord>, RepositoryError> {
        self.memory_page(scope, key, visible, None, 1000, false)
            .await
    }
    async fn read_memory_page(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        visible: &MemoryVisibility,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MemoryRecord>, RepositoryError> {
        self.memory_page(scope, key, visible, after, limit, true)
            .await
    }
    async fn read_memory_evidence(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        visible: &MemoryVisibility,
        id: &str,
        after: usize,
        limit: usize,
    ) -> Result<MemoryEvidencePage, RepositoryError> {
        id_input(id)?;
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        visibility_in(&tx, scope, key, visible).await?;
        let entry = read_in(&tx, scope, id)
            .await?
            .ok_or(CoordinationError::Conflict)?;
        if !visible.allows(&entry, unsigned(now(&tx).await?)? / 1000) {
            return Err(AuthorizationError::Forbidden.into());
        }
        let source = entry.source.as_ref().ok_or(CoordinationError::Corrupt)?;
        let restricted =
            source.session_lifetime_id != visible.lifetime || source.session_key != *key;
        let mut page = MemoryEvidencePage {
            memory_id: entry.id.clone(),
            content_digest: entry.content_digest.clone(),
            sources: Vec::new(),
            next_source: after,
            has_more: false,
            source_scope_restricted: restricted,
        };
        if !restricted {
            let mut bytes = encode(&page, 32768)?.len() + 128;
            for (index, id) in entry
                .source_message_ids
                .iter()
                .enumerate()
                .skip(after)
                .take(limit.clamp(1, 16))
            {
                let evidence = source_in(&tx, scope, source, id, 4096).await?;
                let size = encode(&evidence, 32768)?.len();
                if bytes + size + 1 > 32768 {
                    break;
                }
                bytes += size + 1;
                page.sources.push(evidence);
                page.next_source = index + 1;
            }
            page.has_more = page.next_source < entry.source_message_ids.len();
        }
        encode(&page, 32768)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(page)
    }
    async fn read_memory_assessments(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        visible: &MemoryVisibility,
    ) -> Result<Vec<MemoryAssessment>, RepositoryError> {
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let entries = list_in(&tx, scope, key, visible, None, 1000, false).await?;
        let mut output = Vec::new();
        for entry in entries {
            let mut values = scope_values(scope);
            values.extend([visible.lifetime.0.clone(), entry.id.clone()]);
            if let Some(row)=tx.query_opt(&format!("SELECT CASE WHEN octet_length(content_digest)=64 THEN content_digest END,CASE WHEN octet_length(feedback)<=64 THEN feedback END FROM memory_assessments WHERE {SCOPE} AND lifetime=$5 AND memory_id=$6"),&parameters(&values)).await.map_err(unavailable)?{
                output.push(MemoryAssessment{memory_id:entry.id,content_digest:field(&row,0)?,feedback:serde_json::from_str(&field(&row,1)?).map_err(|_|CoordinationError::Corrupt)?});
            }
        }
        encode(&output, 1048576)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(output)
    }
    async fn read_memory_flywheel(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        life: &SessionLifetimeId,
    ) -> Result<serde_json::Value, RepositoryError> {
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        visibility_in(
            &tx,
            scope,
            key,
            &MemoryVisibility {
                lifetime: life.clone(),
                project: String::new(),
                allow_confirmed_global: false,
            },
        )
        .await?;
        let mut values = scope_values(scope);
        values.push(life.0.clone());
        let exposure_count: i64 = tx
            .query_one(
                &format!(
                    "SELECT count(*)::bigint FROM memory_exposures WHERE {SCOPE} AND lifetime=$5"
                ),
                &parameters(&values),
            )
            .await
            .map_err(unavailable)?
            .try_get(0)
            .map_err(unavailable)?;
        let assessment_count: i64 = tx
            .query_one(
                &format!(
                    "SELECT count(*)::bigint FROM memory_assessments WHERE {SCOPE} AND lifetime=$5"
                ),
                &parameters(&values),
            )
            .await
            .map_err(unavailable)?
            .try_get(0)
            .map_err(unavailable)?;
        let mut feedback = serde_json::Map::new();
        for label in [
            MemoryFeedback::Helpful,
            MemoryFeedback::Irrelevant,
            MemoryFeedback::Incorrect,
            MemoryFeedback::Outdated,
        ] {
            let raw = encode(&label, 64)?;
            let mut parameters_owned = values.clone();
            parameters_owned.push(raw.clone());
            let count:i64=tx.query_one(&format!("SELECT count(*)::bigint FROM memory_assessments WHERE {SCOPE} AND lifetime=$5 AND feedback=$6"),&parameters(&parameters_owned)).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
            let label: String =
                serde_json::from_str(&raw).map_err(|_| CoordinationError::Corrupt)?;
            feedback.insert(label, count.into());
        }
        let rows=tx.query("SELECT CASE WHEN octet_length(e.run_id)<=1024 THEN e.run_id END,CASE WHEN octet_length(e.memory_id)<=512 THEN e.memory_id END,CASE WHEN octet_length(e.content_digest)=64 THEN e.content_digest END,CASE WHEN octet_length(e.channel)<=80 THEN e.channel END,CASE WHEN octet_length(e.policy)<=80 THEN e.policy END,r.status,a.feedback IS NULL,CASE WHEN octet_length(a.feedback)<=64 THEN a.feedback END FROM memory_exposures e JOIN runs r ON (r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id)=(e.organization_id,e.tenant_id,e.project_id,e.agent_uid,e.run_id) LEFT JOIN memory_assessments a ON (a.organization_id,a.tenant_id,a.project_id,a.agent_uid,a.lifetime,a.memory_id,a.content_digest)=(e.organization_id,e.tenant_id,e.project_id,e.agent_uid,e.lifetime,e.memory_id,e.content_digest) WHERE e.organization_id=$1 AND e.tenant_id=$2 AND e.project_id=$3 AND e.agent_uid=$4 AND e.lifetime=$5 ORDER BY e.created_at_ms DESC,e.run_id,e.memory_id LIMIT 100",&parameters(&values)).await.map_err(unavailable)?;
        let mut exposures = Vec::new();
        for row in rows {
            let outcome =
                RunStatus::parse(&field(&row, 5)?).map_err(|_| CoordinationError::Corrupt)?;
            let feedback = if row.try_get::<_, bool>(6).map_err(unavailable)? {
                None
            } else {
                Some(
                    serde_json::from_str::<MemoryFeedback>(&field(&row, 7)?)
                        .map_err(|_| CoordinationError::Corrupt)?,
                )
            };
            exposures.push(serde_json::json!({"run_id":field(&row,0)?,"memory_id":field(&row,1)?,"content_digest":field(&row,2)?,"channel":field(&row,3)?,"policy":field(&row,4)?,"run_status":outcome,"feedback":feedback}));
        }
        let pending:i64=tx.query_one("SELECT count(*)::bigint FROM turn_commits t JOIN runs r ON (r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id)=(t.organization_id,t.tenant_id,t.project_id,t.agent_uid,t.run_id) WHERE r.organization_id=$1 AND r.tenant_id=$2 AND r.project_id=$3 AND r.agent_uid=$4 AND r.lifetime=$5 AND t.status='completed' AND NOT EXISTS(SELECT 1 FROM compact_run_links c WHERE (c.organization_id,c.tenant_id,c.project_id,c.agent_uid,c.run_id)=(r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id)) AND NOT EXISTS(SELECT 1 FROM memory_ingests i WHERE (i.organization_id,i.tenant_id,i.project_id,i.agent_uid,i.run_id)=(r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id))",&parameters(&values)).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
        let exhausted:i64=tx.query_one("SELECT count(*)::bigint FROM memory_ingest_retries b JOIN runs r ON (r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id)=(b.organization_id,b.tenant_id,b.project_id,b.agent_uid,b.run_id) WHERE r.organization_id=$1 AND r.tenant_id=$2 AND r.project_id=$3 AND r.agent_uid=$4 AND r.lifetime=$5 AND b.attempts>=3 AND NOT EXISTS(SELECT 1 FROM memory_ingests i WHERE (i.organization_id,i.tenant_id,i.project_id,i.agent_uid,i.run_id)=(r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id))",&parameters(&values)).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
        let result = serde_json::json!({"lifetime":life,"exposure_count":exposure_count,"assessed_memory_count":assessment_count,"feedback":feedback,"pending_ingests":pending,"retry_exhausted":exhausted,"exposures":exposures,"has_more":exposure_count>100,"sample_limit":100,"signal_note":"曝光是请求材料准备记录，不证明模型采用；运行成功不等于记忆有用。评价仅用于本会话 lifetime，未改变事实或作用域。"});
        encode(&result, 1048576)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
    async fn store_run_memory(
        &self,
        env: &EnterpriseOwnerEnvelope,
        entry: &MemoryRecord,
    ) -> Result<MemoryRecord, RepositoryError> {
        self.memory_mutation(env, Mutation::Store(entry))
            .await?
            .record
            .ok_or_else(|| CoordinationError::Corrupt.into())
    }
    async fn forget_run_memory(
        &self,
        env: &EnterpriseOwnerEnvelope,
        visible: &MemoryVisibility,
        id: &str,
        revision: u64,
    ) -> Result<bool, RepositoryError> {
        self.memory_mutation(env, Mutation::Forget(visible, id, revision))
            .await
            .map(|result| result.forgotten)
    }
    async fn record_run_memory_exposures(
        &self,
        env: &EnterpriseOwnerEnvelope,
        entries: &[MemoryExposure],
        channel: &str,
        policy: &str,
    ) -> Result<(), RepositoryError> {
        self.memory_mutation(env, Mutation::Exposure(entries, channel, policy))
            .await
            .map(|_| ())
    }
    async fn record_run_memory_feedback(
        &self,
        env: &EnterpriseOwnerEnvelope,
        operation: &str,
        id: &str,
        feedback: MemoryFeedback,
    ) -> Result<(), RepositoryError> {
        self.memory_mutation(env, Mutation::Feedback(operation, id, feedback))
            .await
            .map(|_| ())
    }
    async fn ingest_run_memory(
        &self,
        env: &EnterpriseOwnerEnvelope,
    ) -> Result<(), RepositoryError> {
        self.memory_mutation(env, Mutation::Ingest)
            .await
            .map(|_| ())
    }
    async fn record_memory_maintenance(
        &self,
        env: &EnterpriseOwnerEnvelope,
        outcome: &str,
    ) -> Result<(), RepositoryError> {
        self.memory_mutation(env, Mutation::Maintenance(outcome))
            .await
            .map(|_| ())
    }
}
impl PgCanonicalStore {
    async fn memory_page(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        visible: &MemoryVisibility,
        after: Option<&str>,
        limit: usize,
        paged: bool,
    ) -> Result<Vec<MemoryRecord>, RepositoryError> {
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let result = list_in(&tx, scope, key, visible, after, limit, paged).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
}
#[async_trait::async_trait]
impl LegacyMemoryImportRepository for PgCanonicalStore {
    async fn import_legacy_memory_file(
        &self,
        scope: &TenantContext,
        source: &str,
        bytes: &[u8],
    ) -> Result<(), RepositoryError> {
        if scope != &TenantContext::local() {
            return Err(AuthorizationError::Forbidden.into());
        }
        if source.is_empty()
            || source.len() > 16384
            || bytes.len() > 16777216
            || bytes.iter().filter(|byte| **byte == b'\n').count() > 32768
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let hash = digest(bytes);
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let mut values = scope_values(scope);
        values.push(source.into());
        let lock = encode(&(scope, source), 32768)?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,47))",
            &[&lock],
        )
        .await
        .map_err(unavailable)?;
        if let Some(row)=tx.query_opt(&format!("SELECT CASE WHEN octet_length(digest)=64 THEN digest END FROM memory_legacy_imports WHERE {SCOPE} AND source=$5"),&parameters(&values)).await.map_err(unavailable)?{if field(&row,0)?!=hash{return Err(CoordinationError::Conflict.into());}}
        else{for(index,line)in String::from_utf8_lossy(bytes).lines().enumerate(){if line.trim().is_empty(){continue;}let content=serde_json::from_str::<serde_json::Value>(line).ok().and_then(|value|value.get("content").and_then(|value|value.as_str()).map(str::to_owned)).unwrap_or_else(||line.into());let content:String=content.chars().take(8000).collect();
            if content.is_empty(){continue;}let entry=MemoryRecord{id:format!("legacy:{hash}:{index}"),layer:MemoryLayer::Episode,scope:MemoryScope::Legacy(source.into()),kind:MemoryKind::Explicit,content_digest:digest(content.as_bytes()),content,source:None,source_message_ids:Vec::new(),event_time:0,created_at:0,updated_at:0,expires_at:None,confidence:0,confirmed_by_user:false,revision:0};write_in(&tx,scope,&entry).await?;
        }values.push(hash);tx.execute("INSERT INTO memory_legacy_imports VALUES($1,$2,$3,$4,$5,$6)",&parameters(&values)).await.map_err(unavailable)?;}
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }
}
const PENDING: &str = "FROM turn_commits t JOIN runs r ON (r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id)=(t.organization_id,t.tenant_id,t.project_id,t.agent_uid,t.run_id) JOIN session_heads h ON (h.organization_id,h.tenant_id,h.project_id,h.agent_uid,h.session_id,h.lifetime)=(r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.session_id,r.lifetime) LEFT JOIN memory_ingest_retries b ON (b.organization_id,b.tenant_id,b.project_id,b.agent_uid,b.run_id)=(r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id) WHERE r.organization_id=$1 AND r.tenant_id=$2 AND r.project_id=$3 AND r.agent_uid=$4 AND NOT h.deleted AND t.status='completed' AND NOT EXISTS(SELECT 1 FROM compact_run_links c WHERE (c.organization_id,c.tenant_id,c.project_id,c.agent_uid,c.run_id)=(r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id)) AND NOT EXISTS(SELECT 1 FROM memory_ingests i WHERE (i.organization_id,i.tenant_id,i.project_id,i.agent_uid,i.run_id)=(r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id)) AND COALESCE(b.attempts,0)<3";
#[async_trait::async_trait]
impl MemoryMaintenanceRepository for PgCanonicalStore {
    async fn maintain_memory_turns(
        &self,
        scope: &TenantContext,
        limit: usize,
    ) -> Result<(usize, Option<i64>), RepositoryError> {
        if limit == 0 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let candidates = {
            let mut client = self.inner.client.lock().await;
            let tx = client.transaction().await.map_err(unavailable)?;
            set_scope(&tx, scope).await?;
            let mut values = scope_values(scope);
            values.extend([now(&tx).await?.to_string(), limit.min(32).to_string()]);
            let rows=tx.query(&format!("SELECT CASE WHEN octet_length(r.id) BETWEEN 1 AND 1024 THEN r.id END {PENDING} AND COALESCE(b.retry_at_ms,0)<=$5::text::bigint ORDER BY r.storage_row_id LIMIT $6::text::bigint"),&parameters(&values)).await.map_err(unavailable)?;
            let mut candidates = Vec::new();
            for row in rows {
                let run = RunId(field(&row, 0)?);
                candidates.push(EnterpriseOwnerEnvelope {
                    scope: scope.clone(),
                    owner: exact_owner_in(&tx, scope, &run).await?,
                });
            }
            tx.commit().await.map_err(unavailable)?;
            candidates
        };
        let mut completed = 0;
        for env in candidates {
            // 摄入与诊断各有原幂等键；失败仅保存有界退避，不重放任何外部效果。
            let result = self.ingest_run_memory(&env).await;
            if result.is_ok() {
                completed += 1;
                continue;
            }
            let key = SessionWriterLeaseKey {
                scope: scope.clone(),
                session_key: env.owner.session_key.clone(),
                session_lifetime_id: env.owner.session_lifetime_id.clone(),
            };
            let proof_id = proof_key(&key)?;
            let mut proofs = self.proofs.lock().await;
            let mut client = self.inner.client.lock().await;
            let tx = client.transaction().await.map_err(unavailable)?;
            set_scope(&tx, scope).await?;
            let proof = if let Some(proof) = proofs.get(&proof_id) {
                proof.clone()
            } else {
                if proofs.len() >= 1024 {
                    continue;
                }
                match grant_in(&tx, &key, &self.writer_owner, 30000).await {
                    Ok(proof) => proof,
                    Err(CoordinationError::LeaseBusy | CoordinationError::StaleLease) => continue,
                    Err(error) => return Err(error.into()),
                }
            };
            match Self::fence_envelope_in(&tx, &proof, &env).await {
                Ok(_) => {}
                Err(RepositoryError::Authorization(_)) => continue,
                Err(error) => return Err(error),
            }
            let frozen = frozen_in(&tx, scope, &env.owner.run_id)
                .await?
                .ok_or(CoordinationError::Corrupt)?;
            let mut values = scope_values(scope);
            values.push(env.owner.run_id.0.clone());
            if frozen.context_read_only {
                values.push(env.owner.session_lifetime_id.0.clone());
                tx.execute("INSERT INTO memory_ingests VALUES($1,$2,$3,$4,$5,$6,'skipped_read_only',NULL) ON CONFLICT DO NOTHING",&parameters(&values)).await.map_err(unavailable)?;
                diagnostic_in(&tx, &env, "skipped_read_only").await?;
            } else {
                values.push(now(&tx).await?.saturating_add(60000).to_string());
                tx.execute("INSERT INTO memory_ingest_retries VALUES($1,$2,$3,$4,$5,1,$6::text::bigint) ON CONFLICT(organization_id,tenant_id,project_id,agent_uid,run_id) DO UPDATE SET attempts=LEAST(memory_ingest_retries.attempts+1,3),retry_at_ms=excluded.retry_at_ms",&parameters(&values)).await.map_err(unavailable)?;
                diagnostic_in(&tx, &env, "failed").await?;
            }
            Self::before_commit(&tx, &proof).await?;
            tx.commit().await.map_err(unavailable)?;
            proofs.insert(proof_id, proof);
        }
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let due: Option<i64> = tx
            .query_one(
                &format!("SELECT MIN(COALESCE(b.retry_at_ms,0)) {PENDING}"),
                &parameters(&scope_values(scope)),
            )
            .await
            .map_err(unavailable)?
            .try_get(0)
            .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok((completed, due))
    }
}
