//! 原运行事实的 PostgreSQL 读回；先固定完整域与永久 owner，再读取有界正文。
use crate::attempts::{attempts_in, route_in};
use crate::canonical::{PgCanonicalStore, field, set_scope, unsigned};
use crate::executions::record_in;
use crate::identities::exact_owner_in;
use crate::sessions::encode;
use crate::{SCOPE, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::RunFactRepository;
use sha2::{Digest, Sha256};
use tokio_postgres::{GenericClient, Transaction};
const BUDGET: usize = 1024 * 1024 - 1024;
fn valid_run(run: &RunId) -> Result<(), CoordinationError> {
    if run.0.is_empty() || run.0.len() > 1024 {
        Err(CoordinationError::InvalidRequest)
    } else {
        Ok(())
    }
}
fn values(scope: &TenantContext, run: &RunId) -> Vec<String> {
    let mut values = scope_values(scope);
    values.push(run.0.clone());
    values
}
pub(crate) async fn frozen_in<C: GenericClient + Sync>(
    db: &C,
    scope: &TenantContext,
    run: &RunId,
) -> Result<Option<RunSnapshot>, CoordinationError> {
    db.query_opt(&format!("SELECT CASE WHEN octet_length(snapshot_json)<=1048576 THEN snapshot_json END FROM run_snapshots WHERE {SCOPE} AND run_id=$5"),&parameters(&values(scope,run))).await.map_err(unavailable)?.map(|row|serde_json::from_str(&field(&row,0)?).map_err(|_|CoordinationError::Corrupt)).transpose()
}
pub(crate) async fn stamp_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    owner: &ExactOwner,
    seq: EventSeq,
) -> Result<Option<ViewStamp>, CoordinationError> {
    if seq.0 == 0 {
        return Ok(None);
    }
    let mut values = values(scope, &owner.run_id);
    values.push(seq.0.to_string());
    let row=db.query_opt(&format!("SELECT e.view_provenance,CASE WHEN octet_length(s.stamp_json)<=16384 THEN s.stamp_json END,CASE WHEN octet_length(s.stamp_digest)=64 THEN s.stamp_digest END,s.published,s.seq IS NOT NULL FROM events e LEFT JOIN event_view_stamps s USING(organization_id,tenant_id,project_id,agent_uid,run_id,seq) WHERE e.organization_id=$1 AND e.tenant_id=$2 AND e.project_id=$3 AND e.agent_uid=$4 AND e.run_id=$5 AND e.seq=$6::text::bigint"),&parameters(&values)).await.map_err(unavailable)?.ok_or(CoordinationError::Corrupt)?;
    let provenance: String = row.try_get(0).map_err(unavailable)?;
    let raw: Option<String> = row.try_get(1).map_err(unavailable)?;
    let Some(raw) = raw else {
        if provenance == "legacy" && !row.try_get::<_, bool>(4).map_err(unavailable)? {
            return Ok(None);
        }
        return Err(CoordinationError::Corrupt);
    };
    if provenance != "native"
        || row.try_get::<_, Option<bool>>(3).map_err(unavailable)? != Some(true)
        || format!("{:x}", Sha256::digest(raw.as_bytes())) != field(&row, 2)?
    {
        return Err(CoordinationError::Corrupt);
    }
    let stamp: ViewStamp = serde_json::from_str(&raw).map_err(|_| CoordinationError::Corrupt)?;
    if !stamp.valid()
        || stamp.owner.as_ref() != Some(owner)
        || stamp.event_seq != Some(seq)
        || stamp.session_key != owner.session_key
        || stamp.session_lifetime_id != owner.session_lifetime_id
    {
        return Err(CoordinationError::Corrupt);
    }
    Ok(Some(stamp))
}
pub(crate) async fn compact_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    owner: &ExactOwner,
    record: &RunRecord,
) -> Result<Option<CompactRunReadback>, CoordinationError> {
    let row=db.query_opt("SELECT CASE WHEN octet_length(l.operation_id) BETWEEN 1 AND 256 THEN l.operation_id END,CASE WHEN octet_length(l.source_json)<=65536 THEN l.source_json END,CASE WHEN octet_length(c.run_id)<=1024 THEN c.run_id END,CASE WHEN octet_length(c.state)<=32 THEN c.state END,c.reason IS NULL,CASE WHEN octet_length(c.reason)<=1024 THEN c.reason END,c.result_generation,CASE WHEN octet_length(l.request_json)<=16384 THEN l.request_json END,CASE WHEN octet_length(c.source_json)<=65536 THEN c.source_json END,CASE WHEN octet_length(c.lifetime)<=128 THEN c.lifetime END,CASE WHEN octet_length(r.input)<=16384 THEN r.input END FROM compact_run_links l LEFT JOIN compact_operations c ON (c.organization_id,c.tenant_id,c.project_id,c.agent_uid,c.operation)=(l.organization_id,l.tenant_id,l.project_id,l.agent_uid,l.operation_id) JOIN runs r ON (r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id)=(l.organization_id,l.tenant_id,l.project_id,l.agent_uid,l.run_id) WHERE l.organization_id=$1 AND l.tenant_id=$2 AND l.project_id=$3 AND l.agent_uid=$4 AND l.run_id=$5",&parameters(&values(scope,&owner.run_id))).await.map_err(unavailable)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let operation = field(&row, 0)?;
    let raw = field(&row, 1)?;
    let source: ContextSource =
        serde_json::from_str(&raw).map_err(|_| CoordinationError::Corrupt)?;
    let request: CompactRunRequest =
        serde_json::from_str(&field(&row, 7)?).map_err(|_| CoordinationError::Corrupt)?;
    let original: CompactRunRequest =
        serde_json::from_str(&field(&row, 10)?).map_err(|_| CoordinationError::Corrupt)?;
    let state = field(&row, 3)?;
    let generation = row
        .try_get::<_, Option<i64>>(6)
        .map_err(unavailable)?
        .map(unsigned)
        .transpose()?;
    let reason = if row.try_get::<_, bool>(4).map_err(unavailable)? {
        None
    } else {
        Some(field(&row, 5)?)
    };
    if field(&row, 2)? != owner.run_id.0
        || field(&row, 9)? != owner.session_lifetime_id.0
        || field(&row, 8)? != raw
        || source.lifetime != owner.session_lifetime_id
        || request.session_key != owner.session_key
        || request.session_lifetime_id != source.lifetime
        || request.operation_id != operation
        || request.expected_revision != source.source_end
        || request.expected_projection_generation != source.generation
        || source.source_start != TranscriptSeq(0)
        || source.policy_fingerprint.is_empty()
        || source.prefix_digest.len() != 64
        || request != original
        || record.request_id != RequestId::String(format!("compact:{operation}"))
        || generation.is_some_and(|generation| {
            generation
                != if state == "committed" {
                    source.generation.0.saturating_add(1)
                } else {
                    source.generation.0
                }
        })
    {
        return Err(CoordinationError::Corrupt);
    }
    let outcome = match record.status {
        RunStatus::Running if state == "pending" => CompactRunOutcome::Started,
        RunStatus::Completed if state == "committed" && generation.is_some() => {
            CompactRunOutcome::Committed
        }
        RunStatus::Completed if state == "rejected" && reason.as_deref() == Some("no_gain") => {
            CompactRunOutcome::NoGain
        }
        RunStatus::Failed if state == "rejected" && reason.as_deref() == Some("rejected") => {
            CompactRunOutcome::Rejected
        }
        RunStatus::Failed if state == "rejected" => CompactRunOutcome::Failed,
        RunStatus::Cancelled if state == "rejected" => CompactRunOutcome::Cancelled,
        RunStatus::UnknownAfterRestart => CompactRunOutcome::Unknown,
        _ => return Err(CoordinationError::Corrupt),
    };
    Ok(Some(CompactRunReadback {
        schema_version: 1,
        owner: owner.clone(),
        operation_id: operation,
        source,
        outcome,
        result_generation: generation.map(ProjectionGeneration),
    }))
}
pub(crate) async fn receipts_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    owner: &ExactOwner,
) -> Result<Vec<ToolReceipt>, CoordinationError> {
    let values = values(scope, &owner.run_id);
    let size=db.query_one(&format!("SELECT count(*)::bigint,COALESCE(sum(octet_length(call_id)+octet_length(name)+octet_length(status)+octet_length(effect)+octet_length(argument_digest)+COALESCE(octet_length(outcome),0)+COALESCE(octet_length(artifact_ref),0)+COALESCE(octet_length(receipt_json),0)),0)::bigint FROM tool_executions WHERE {SCOPE} AND run_id=$5"),&parameters(&values)).await.map_err(unavailable)?;
    if size.try_get::<_, i64>(0).map_err(unavailable)? > 1000
        || size.try_get::<_, i64>(1).map_err(unavailable)? > BUDGET as i64
    {
        return Err(CoordinationError::InvalidRequest);
    }
    let rows=db.query(&format!("SELECT round,CASE WHEN octet_length(call_id) BETWEEN 1 AND 1024 THEN call_id END,CASE WHEN octet_length(name) BETWEEN 1 AND 1024 THEN name END,CASE WHEN octet_length(status)<=32 THEN status END,CASE WHEN octet_length(effect)<=32 THEN effect END,CASE WHEN octet_length(argument_digest)=64 OR (effect='not_executed' AND argument_digest='') THEN argument_digest END,started_at_ms,finished_at_ms,outcome IS NULL,CASE WHEN octet_length(outcome)<=128 THEN outcome END,artifact_ref IS NULL,CASE WHEN octet_length(artifact_ref)<=4096 THEN artifact_ref END,safe_to_replay,receipt_json IS NULL,CASE WHEN octet_length(receipt_json)<=1048576 THEN receipt_json END,CASE WHEN octet_length(lifetime)<=128 THEN lifetime END,generation FROM tool_executions WHERE {SCOPE} AND run_id=$5 ORDER BY round,id LIMIT 1000"),&parameters(&values)).await.map_err(unavailable)?;
    let mut result = Vec::new();
    for row in rows {
        if field(&row, 15)? != owner.session_lifetime_id.0
            || unsigned(row.try_get(16).map_err(unavailable)?)? != owner.run_generation.0
        {
            return Err(CoordinationError::Corrupt);
        }
        let outcome = if row.try_get::<_, bool>(8).map_err(unavailable)? {
            None
        } else {
            Some(field(&row, 9)?)
        };
        let artifact_ref = if row.try_get::<_, bool>(10).map_err(unavailable)? {
            None
        } else {
            Some(field(&row, 11)?)
        };
        let receipt = if row.try_get::<_, bool>(13).map_err(unavailable)? {
            None
        } else {
            Some(serde_json::from_str(&field(&row, 14)?).map_err(|_| CoordinationError::Corrupt)?)
        };
        result.push(ToolReceipt {
            run_id: owner.run_id.clone(),
            round: row.try_get(0).map_err(unavailable)?,
            call_id: field(&row, 1)?,
            name: field(&row, 2)?,
            status: field(&row, 3)?,
            effect: field(&row, 4)?,
            argument_digest: field(&row, 5)?,
            started_at_ms: row.try_get(6).map_err(unavailable)?,
            finished_at_ms: row.try_get(7).map_err(unavailable)?,
            outcome,
            artifact_ref,
            safe_to_replay: row.try_get(12).map_err(unavailable)?,
            receipt,
        });
    }
    encode(&result, BUDGET)?;
    Ok(result)
}
#[async_trait::async_trait]
impl RunFactRepository for PgCanonicalStore {
    async fn read_exact_run_owner(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<ExactOwner, CoordinationError> {
        valid_run(run)?;
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let owner = exact_owner_in(&tx, scope, run).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(owner)
    }
    async fn read_run_fact(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<RunFactReadback, CoordinationError> {
        valid_run(run)?;
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let owner = exact_owner_in(&tx, scope, run).await?;
        let record = record_in(&tx, scope, run).await?;
        let compact = compact_in(&tx, scope, &owner, &record).await?;
        if record.kind == RunKind::Compact && compact.is_none() {
            return Err(CoordinationError::Corrupt);
        }
        let value = RunFactReadback {
            view_stamp: stamp_in(&tx, scope, &owner, record.last_seq).await?,
            snapshot: frozen_in(&tx, scope, run).await?,
            compact,
            continuation_parent_run_id: crate::continuations::relation_in(&tx, scope, run, true)
                .await?,
            continuation_run_id: crate::continuations::relation_in(&tx, scope, run, false).await?,
            run: record,
        };
        encode(&value, BUDGET)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(value)
    }
    async fn read_tool_receipts(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Vec<ToolReceipt>, CoordinationError> {
        valid_run(run)?;
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let owner = exact_owner_in(&tx, scope, run).await?;
        let receipts = receipts_in(&tx, scope, &owner).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(receipts)
    }
    async fn read_provider_history(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<RunProviderReadback, CoordinationError> {
        valid_run(run)?;
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
        let attempts = attempts_in(&tx, scope, run).await?;
        let mut usage = ProviderUsage::default();
        for attempt in &attempts {
            if let Some(value) = &attempt.usage {
                usage.add_attempt(value);
            }
        }
        let value = RunProviderReadback {
            route: route_in(&tx, scope, run).await?,
            final_candidate_index: attempts.last().map(|attempt| attempt.candidate_index),
            attempts,
            usage,
        };
        encode(&value, BUDGET)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(value)
    }
}
