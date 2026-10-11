//! 原发送前捕获与上下文来源；读回不会创建运行或执行 Provider。
use crate::canonical::{PgCanonicalStore, field, set_scope, unsigned};
use crate::facts::frozen_in;
use crate::identities::exact_owner_in;
use crate::readback::{projection_in, snapshot_in};
use crate::sessions::{encode, metadata_in};
use crate::{SCOPE, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::{RunRequestRepository, validate_frozen_request};
use sha2::{Digest, Sha256};
use tokio_postgres::Transaction;
fn values(scope: &TenantContext, run: &RunId) -> Vec<String> {
    let mut values = scope_values(scope);
    values.push(run.0.clone());
    values
}
pub(crate) async fn context_source_in(
    db: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
) -> Result<RunContextReadback, CoordinationError> {
    let actual = exact_owner_in(db, &env.scope, &env.owner.run_id).await?;
    if actual != env.owner {
        return Err(CoordinationError::Forbidden);
    }
    let meta = metadata_in(db, &env.scope, &env.owner.session_key, false)
        .await?
        .filter(|meta| !meta.deleted && meta.lifetime == env.owner.session_lifetime_id)
        .ok_or(CoordinationError::StaleLease)?;
    let snapshot = snapshot_in(db, &env.scope, &meta).await?;
    let projection = projection_in(db, &env.scope, &snapshot).await?;
    let frozen = frozen_in(db, &env.scope, &env.owner.run_id)
        .await?
        .ok_or(CoordinationError::Corrupt)?;
    Ok(RunContextReadback {
        snapshot,
        projection,
        frozen,
    })
}
pub(crate) fn source_check(
    snapshot: &SessionSnapshot,
    source: &ContextSource,
    historical: bool,
) -> Result<(), CoordinationError> {
    if snapshot.lifetime != source.lifetime
        || source.source_start != TranscriptSeq(0)
        || source.source_end > snapshot.revision
        || if historical {
            source.generation > snapshot.projection_generation
        } else {
            source.generation != snapshot.projection_generation
        }
    {
        return Err(CoordinationError::Conflict);
    }
    if snapshot
        .batch_ranges
        .iter()
        .any(|batch| batch.0 < source.source_end.0 && batch.1 > source.source_end.0)
    {
        return Err(CoordinationError::Conflict);
    }
    let end = usize::try_from(source.source_end.0).map_err(|_| CoordinationError::Corrupt)?;
    let prefix = snapshot
        .messages
        .get(..end)
        .ok_or(CoordinationError::Corrupt)?;
    if format!(
        "{:x}",
        Sha256::digest(encode(prefix, 16 * 1024 * 1024)?.as_bytes())
    ) != source.prefix_digest
    {
        return Err(CoordinationError::Corrupt);
    }
    Ok(())
}
async fn running(
    db: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    proof: &SessionWriterLease,
) -> Result<(), RepositoryError> {
    PgCanonicalStore::fence_envelope_in(db, proof, env).await?;
    let state: String = db
        .query_one(
            &format!("SELECT status FROM runs WHERE {SCOPE} AND id=$5 FOR UPDATE"),
            &parameters(&values(&env.scope, &env.owner.run_id)),
        )
        .await
        .map_err(unavailable)?
        .try_get(0)
        .map_err(unavailable)?;
    if state != "running" {
        return Err(CoordinationError::Conflict.into());
    }
    Ok(())
}
async fn anchor_in(
    db: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    generation: ProjectionGeneration,
    route: &str,
    identity: &str,
) -> Result<Option<(u64, u64)>, CoordinationError> {
    let mut values = scope_values(&env.scope);
    values.extend([
        env.owner.session_lifetime_id.0.clone(),
        route.into(),
        identity.into(),
        generation.0.to_string(),
    ]);
    let row=db.query_opt("SELECT CASE WHEN octet_length(a.data_json)<=1048576 THEN a.data_json END,CASE WHEN octet_length(l.envelope_json)<=16384 THEN l.envelope_json END,CASE WHEN octet_length(a.run_id)<=1024 THEN a.run_id END,CASE WHEN octet_length(a.attempt_id)<=256 THEN a.attempt_id END,a.round,a.candidate_index FROM provider_attempts a JOIN runs r ON (r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id)=(a.organization_id,a.tenant_id,a.project_id,a.agent_uid,a.run_id) JOIN context_ledgers l ON (l.organization_id,l.tenant_id,l.project_id,l.agent_uid,l.run_id,l.round)=(a.organization_id,a.tenant_id,a.project_id,a.agent_uid,a.run_id,a.round) WHERE r.organization_id=$1 AND r.tenant_id=$2 AND r.project_id=$3 AND r.agent_uid=$4 AND r.lifetime=$5 AND r.status='completed' AND a.status='succeeded' AND CASE WHEN octet_length(a.data_json)<=1048576 AND octet_length(l.envelope_json)<=16384 THEN l.envelope_json::jsonb->>'route'=$6 AND l.envelope_json::jsonb->>'provider_identity'=$7 AND l.envelope_json::jsonb->'source'->>'generation'=$8 AND a.data_json::jsonb->>'model'=$6 AND a.data_json::jsonb->'usage'->>'input_tokens' IS NOT NULL AND a.data_json::jsonb->'usage'->>'input_tokens'<>'0' ELSE FALSE END ORDER BY r.storage_row_id DESC,a.round DESC,a.storage_row_id DESC LIMIT 1",&parameters(&values)).await.map_err(unavailable)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let run = RunId(field(&row, 2)?);
    exact_owner_in(db, &env.scope, &run).await?;
    let attempt = decode_provider_attempt_fact(
        &run,
        &field(&row, 3)?,
        u32::try_from(unsigned(row.try_get(4).map_err(unavailable)?)?)
            .map_err(|_| CoordinationError::Corrupt)?,
        u32::try_from(unsigned(row.try_get(5).map_err(unavailable)?)?)
            .map_err(|_| CoordinationError::Corrupt)?,
        "succeeded",
        &field(&row, 0)?,
    )?;
    let context: ContextEnvelope =
        serde_json::from_str(&field(&row, 1)?).map_err(|_| CoordinationError::Corrupt)?;
    if context.source.lifetime != env.owner.session_lifetime_id
        || context.source.generation != generation
        || context.route != route
        || context.provider_identity != identity
        || attempt.model != route
    {
        return Err(CoordinationError::Corrupt);
    }
    let input = attempt
        .usage
        .and_then(|usage| usage.input_tokens)
        .filter(|value| *value > 0)
        .ok_or(CoordinationError::Corrupt)?;
    let estimated = context
        .stable_tokens
        .checked_add(context.history_tokens)
        .and_then(|sum| sum.checked_add(context.retrieved_tokens))
        .and_then(|sum| sum.checked_add(context.overlay_tokens))
        .ok_or(CoordinationError::Corrupt)?;
    Ok(Some((input, estimated)))
}
#[async_trait::async_trait]
impl RunRequestRepository for PgCanonicalStore {
    async fn read_run_request_source(
        &self,
        env: &EnterpriseOwnerEnvelope,
        route: &str,
        identity: &str,
    ) -> Result<RunRequestSource, RepositoryError> {
        if route.is_empty() || route.len() > 1024 || identity.is_empty() || identity.len() > 1024 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        running(&tx, env, &proof).await?;
        let source = context_source_in(&tx, env).await?;
        let row = tx
            .query_one(
                &format!("SELECT revision FROM snapshot_clock WHERE {SCOPE} AND slot=1"),
                &parameters(&scope_values(&env.scope)),
            )
            .await
            .map_err(unavailable)?;
        let revision = SnapshotRevision(unsigned(row.try_get(0).map_err(unavailable)?)?);
        let calibration = anchor_in(
            &tx,
            env,
            source.snapshot.projection_generation,
            route,
            identity,
        )
        .await?;
        let result = RunRequestSource {
            snapshot: source.snapshot,
            revision,
            calibration,
            frozen: source.frozen,
        };
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
    async fn capture_run_request(
        &self,
        env: &EnterpriseOwnerEnvelope,
        capture: &ProviderRequestCapture,
        context: &ContextEnvelope,
    ) -> Result<(), RepositoryError> {
        let raw = encode(capture, 16 * 1024 * 1024)?;
        let context_raw = encode(context, 16384)?;
        if capture.owner != env.owner {
            return Err(AuthorizationError::StaleOwner.into());
        }
        if capture.schema_version != 1
            || capture.round == 0
            || capture.capture_id.is_empty()
            || capture.capture_id.len() > 256
            || capture.policy_fingerprint.len() != 64
            || capture.request_digest.len() != 64
            || context.source != capture.input.source
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        running(&tx, env, &proof).await?;
        let source = context_source_in(&tx, env).await?;
        source_check(&source.snapshot, &capture.input.source, false)?;
        validate_frozen_request(&source.frozen, capture, Some(context))
            .map_err(|_| CoordinationError::InvalidRequest)?;
        let digest = format!("{:x}", Sha256::digest(raw.as_bytes()));
        let mut values = values(&env.scope, &env.owner.run_id);
        values.push(capture.capture_id.clone());
        if let Some(row)=tx.query_opt(&format!("SELECT CASE WHEN octet_length(capture_json)<=16777216 THEN capture_json END,CASE WHEN octet_length(capture_digest)=64 THEN capture_digest END,round,candidate_index FROM provider_requests WHERE {SCOPE} AND run_id=$5 AND capture_id=$6"),&parameters(&values)).await.map_err(unavailable)?{
            let saved=field(&row,0)?;if format!("{:x}",Sha256::digest(saved.as_bytes()))!=field(&row,1)?{return Err(CoordinationError::Corrupt.into());}if field(&row,1)?!=digest{return Err(CoordinationError::Conflict.into());}if saved!=raw||row.try_get::<_,i64>(2).map_err(unavailable)?!=i64::from(capture.round)||row.try_get::<_,i64>(3).map_err(unavailable)?!=i64::from(capture.candidate_index){return Err(CoordinationError::Corrupt.into());}Self::before_commit(&tx,&proof).await?;tx.commit().await.map_err(unavailable)?;return Ok(());
        }
        let count: i64 = tx
            .query_one(
                &format!(
                    "SELECT count(*)::bigint FROM provider_requests WHERE {SCOPE} AND run_id=$5"
                ),
                &parameters(&values[..5]),
            )
            .await
            .map_err(unavailable)?
            .try_get(0)
            .map_err(unavailable)?;
        if count >= 1024 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        values.extend([
            capture.round.to_string(),
            capture.candidate_index.to_string(),
            raw,
            digest,
        ]);
        tx.execute("INSERT INTO provider_requests(organization_id,tenant_id,project_id,agent_uid,run_id,capture_id,round,candidate_index,capture_json,capture_digest) VALUES($1,$2,$3,$4,$5,$6,$7::text::bigint,$8::text::bigint,$9,$10)",&parameters(&values)).await.map_err(unavailable)?;
        let mut values = values[..5].to_vec();
        values.extend([capture.round.to_string(), context_raw]);
        tx.execute("INSERT INTO context_ledgers VALUES($1,$2,$3,$4,$5,$6::text::bigint,$7) ON CONFLICT(organization_id,tenant_id,project_id,agent_uid,run_id,round) DO UPDATE SET envelope_json=excluded.envelope_json",&parameters(&values)).await.map_err(unavailable)?;
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }
    async fn read_provider_request(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        life: &SessionLifetimeId,
        run: Option<&RunId>,
        capture: Option<&str>,
    ) -> Result<(ProviderRequestCapture, RunSnapshot), RepositoryError> {
        if key.0.is_empty()
            || key.0.len() > 1024
            || life.0.is_empty()
            || life.0.len() > 128
            || run.is_some_and(|run| run.0.is_empty() || run.0.len() > 1024)
            || capture.is_some_and(|id| id.is_empty() || id.len() > 256)
        {
            return Err(CoordinationError::InvalidRequest.into());
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
            .filter(|meta| !meta.deleted && meta.lifetime == *life)
            .ok_or(CoordinationError::StaleLease)?;
        let mut values = scope_values(scope);
        values.extend([key.0.clone(), life.0.clone()]);
        let run_text = run.map(|run| run.0.as_str());
        let mut params = parameters(&values);
        params.extend([
            &run_text as &(dyn tokio_postgres::types::ToSql + Sync),
            &capture,
        ]);
        let row=tx.query_opt("SELECT CASE WHEN octet_length(p.run_id) BETWEEN 1 AND 1024 THEN p.run_id END,CASE WHEN octet_length(p.capture_id) BETWEEN 1 AND 256 THEN p.capture_id END,p.round,p.candidate_index FROM provider_requests p JOIN runs r ON (r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id)=(p.organization_id,p.tenant_id,p.project_id,p.agent_uid,p.run_id) WHERE r.organization_id=$1 AND r.tenant_id=$2 AND r.project_id=$3 AND r.agent_uid=$4 AND r.session_id=$5 AND r.lifetime=$6 AND ($7::text IS NULL OR p.run_id=$7) AND ($8::text IS NULL OR p.capture_id=$8) ORDER BY p.storage_row_id DESC LIMIT 1",&params).await.map_err(unavailable)?.ok_or(CoordinationError::Conflict)?;
        let sql_run = RunId(field(&row, 0)?);
        let sql_capture = field(&row, 1)?;
        let owner = exact_owner_in(&tx, scope, &sql_run).await?;
        if owner.session_key != *key || owner.session_lifetime_id != *life {
            return Err(CoordinationError::Corrupt.into());
        }
        let mut values = values[..4].to_vec();
        values.extend([sql_run.0.clone(), sql_capture.clone()]);
        let body=tx.query_one(&format!("SELECT CASE WHEN octet_length(capture_json)<=16777216 THEN capture_json END,CASE WHEN octet_length(capture_digest)=64 THEN capture_digest END FROM provider_requests WHERE {SCOPE} AND run_id=$5 AND capture_id=$6"),&parameters(&values)).await.map_err(unavailable)?;
        let raw = field(&body, 0)?;
        if format!("{:x}", Sha256::digest(raw.as_bytes())) != field(&body, 1)? {
            return Err(CoordinationError::Corrupt.into());
        }
        let captured: ProviderRequestCapture =
            serde_json::from_str(&raw).map_err(|_| CoordinationError::Corrupt)?;
        if captured.owner != owner
            || captured.capture_id != sql_capture
            || i64::from(captured.round) != row.try_get::<_, i64>(2).map_err(unavailable)?
            || i64::from(captured.candidate_index)
                != row.try_get::<_, i64>(3).map_err(unavailable)?
        {
            return Err(CoordinationError::Corrupt.into());
        }
        let frozen = frozen_in(&tx, scope, &sql_run)
            .await?
            .ok_or(CoordinationError::Corrupt)?;
        validate_frozen_request(&frozen, &captured, None)?;
        let snapshot = snapshot_in(&tx, scope, &meta).await?;
        projection_in(&tx, scope, &snapshot).await?;
        source_check(&snapshot, &captured.input.source, true)
            .map_err(|_| CoordinationError::Corrupt)?;
        tx.commit().await.map_err(unavailable)?;
        Ok((captured, frozen))
    }
}
