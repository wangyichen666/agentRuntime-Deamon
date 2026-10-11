//! Provider 调用原事实；开始、终态、事件与 outbox 在同一事务提交。
use crate::canonical::{PgCanonicalStore, field, set_scope, unsigned};
use crate::events::{append_in, seal_in};
use crate::executions::record_in;
use crate::sessions::encode;
use crate::{SCOPE, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::RunAttemptRepository;
use tokio_postgres::{GenericClient, Row, Transaction};

fn values(scope: &TenantContext, run: &RunId) -> Vec<String> {
    let mut values = scope_values(scope);
    values.push(run.0.clone());
    values
}
fn decode(row: &Row, run: &RunId) -> Result<ProviderAttempt, CoordinationError> {
    let round = u32::try_from(unsigned(row.try_get(1).map_err(unavailable)?)?)
        .map_err(|_| CoordinationError::Corrupt)?;
    let candidate = u32::try_from(unsigned(row.try_get(2).map_err(unavailable)?)?)
        .map_err(|_| CoordinationError::Corrupt)?;
    decode_provider_attempt_fact(
        run,
        &field(row, 0)?,
        round,
        candidate,
        &field(row, 3)?,
        &field(row, 4)?,
    )
}
const FIELDS: &str = "CASE WHEN octet_length(attempt_id) BETWEEN 1 AND 256 THEN attempt_id END,round,candidate_index,CASE WHEN octet_length(status)<=32 THEN status END,CASE WHEN octet_length(data_json)<=1048576 THEN data_json END";
pub(crate) async fn route_in<C: GenericClient + Sync>(
    db: &C,
    scope: &TenantContext,
    run: &RunId,
) -> Result<Option<RouteSnapshot>, CoordinationError> {
    db.query_opt(&format!("SELECT CASE WHEN octet_length(snapshot_json)<=1048576 THEN snapshot_json END FROM run_routes WHERE {SCOPE} AND run_id=$5"), &parameters(&values(scope,run))).await.map_err(unavailable)?
        .map(|row| serde_json::from_str(&field(&row,0)?).map_err(|_|CoordinationError::Corrupt)).transpose()
}
pub(crate) async fn attempts_in<C: GenericClient + Sync>(
    db: &C,
    scope: &TenantContext,
    run: &RunId,
) -> Result<Vec<ProviderAttempt>, CoordinationError> {
    let values = values(scope, run);
    let size=db.query_one(&format!("SELECT count(*)::bigint,COALESCE(sum(octet_length(data_json)+octet_length(attempt_id)+octet_length(status)),0)::bigint FROM provider_attempts WHERE {SCOPE} AND run_id=$5"),&parameters(&values)).await.map_err(unavailable)?;
    if size.try_get::<_, i64>(0).map_err(unavailable)? > 1000
        || size.try_get::<_, i64>(1).map_err(unavailable)? > 1024 * 1024 - 1024
    {
        return Err(CoordinationError::InvalidRequest);
    }
    let rows=db.query(&format!("SELECT {FIELDS} FROM provider_attempts WHERE {SCOPE} AND run_id=$5 ORDER BY storage_row_id LIMIT 1000"),&parameters(&values)).await.map_err(unavailable)?;
    rows.iter().map(|row| decode(row, run)).collect()
}
async fn attempt_in(
    db: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    id: &str,
) -> Result<Option<ProviderAttempt>, CoordinationError> {
    let mut values = values(&env.scope, &env.owner.run_id);
    values.push(id.into());
    db.query_opt(&format!("SELECT {FIELDS} FROM provider_attempts WHERE {SCOPE} AND run_id=$5 AND attempt_id=$6 FOR UPDATE"),&parameters(&values)).await.map_err(unavailable)?.map(|row|decode(&row,&env.owner.run_id)).transpose()
}
fn validate(
    env: &EnterpriseOwnerEnvelope,
    attempt: &ProviderAttempt,
) -> Result<String, CoordinationError> {
    if attempt.run_id != env.owner.run_id
        || attempt.attempt_id.is_empty()
        || attempt.attempt_id.len() > 256
        || attempt.round == 0
        || attempt.provider_profile_id.is_empty()
        || attempt.provider_profile_id.len() > 1024
        || attempt.model.is_empty()
        || attempt.model.len() > 1024
    {
        return Err(CoordinationError::InvalidRequest);
    }
    encode(attempt, 1024 * 1024 - 1024)
}
fn identical(a: &ProviderAttempt, b: &ProviderAttempt) -> Result<bool, CoordinationError> {
    Ok(
        serde_json::to_value(a).map_err(|_| CoordinationError::Corrupt)?
            == serde_json::to_value(b).map_err(|_| CoordinationError::InvalidRequest)?,
    )
}
impl PgCanonicalStore {
    async fn write_attempt(
        &self,
        env: &EnterpriseOwnerEnvelope,
        attempt: &ProviderAttempt,
        start: bool,
    ) -> Result<(), RepositoryError> {
        let raw = validate(env, attempt)?;
        if start {
            if attempt.status != AttemptStatus::Started
                || attempt.finished_at_ms.is_some()
                || attempt.first_event_at_ms.is_some()
                || attempt.usage.is_some()
                || attempt.stream_committed
                || attempt.error_kind.is_some()
                || attempt.diagnostic.is_some()
                || attempt.retry_after_ms.is_some()
            {
                return Err(CoordinationError::InvalidRequest.into());
            }
        } else if attempt.status == AttemptStatus::Started || attempt.finished_at_ms.is_none() {
            return Err(CoordinationError::InvalidRequest.into());
        }
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
        if start {
            if record_in(&tx, &env.scope, &env.owner.run_id).await?.status != RunStatus::Running {
                return Err(CoordinationError::Conflict.into());
            }
            if let Some(route) = route_in(&tx, &env.scope, &env.owner.run_id).await? {
                let candidate = route
                    .candidates
                    .get(attempt.candidate_index as usize)
                    .ok_or(CoordinationError::Conflict)?;
                if candidate.profile_id != attempt.provider_profile_id
                    || candidate.api_type != attempt.api_type
                    || candidate.model != attempt.model
                {
                    return Err(CoordinationError::Conflict.into());
                }
            }
        }
        let previous = attempt_in(&tx, env, &attempt.attempt_id).await?;
        if let Some(previous) = &previous {
            if !previous.same_invocation(attempt) {
                return Err(CoordinationError::Conflict.into());
            }
            if start || previous.status != AttemptStatus::Started {
                if !identical(previous, attempt)? {
                    return Err(CoordinationError::Conflict.into());
                }
                Self::before_commit(&tx, &proof).await?;
                tx.commit().await.map_err(unavailable)?;
                return Ok(());
            }
        } else if !start {
            return Err(CoordinationError::Conflict.into());
        }
        let mut values = scope_values(&env.scope);
        if start {
            let prior = attempts_in(&tx, &env.scope, &env.owner.run_id).await?;
            let prior_bytes = encode(&prior, 1024 * 1024)?.len();
            if prior.len() >= 1000 || prior_bytes.saturating_add(raw.len()) > 1024 * 1024 - 1024 {
                return Err(CoordinationError::InvalidRequest.into());
            }
            crate::billing::start_provider_in(&tx,env,attempt).await?;
            values.extend([
                attempt.attempt_id.clone(),
                attempt.run_id.0.clone(),
                attempt.round.to_string(),
                attempt.candidate_index.to_string(),
                raw,
            ]);
            tx.execute("INSERT INTO provider_attempts(organization_id,tenant_id,project_id,agent_uid,attempt_id,run_id,round,candidate_index,status,data_json) VALUES($1,$2,$3,$4,$5,$6,$7::text::bigint,$8::text::bigint,'started',$9)",&parameters(&values)).await.map_err(unavailable)?;
            append_in(&tx,env,"provider_attempt_started",&serde_json::json!({"attempt_id":attempt.attempt_id,"round":attempt.round,"candidate_index":attempt.candidate_index,"profile_id":attempt.provider_profile_id,"model":attempt.model})).await?;
        } else {
            let prior = attempts_in(&tx, &env.scope, &env.owner.run_id).await?;
            let mut updated = prior;
            let item = updated
                .iter_mut()
                .find(|item| item.attempt_id == attempt.attempt_id)
                .ok_or(CoordinationError::Corrupt)?;
            *item = attempt.clone();
            encode(&updated, 1024 * 1024 - 1024)?;
            values.extend([
                attempt.attempt_id.clone(),
                attempt.run_id.0.clone(),
                attempt.status_name().into(),
                raw,
            ]);
            let changed=tx.execute(&format!("UPDATE provider_attempts SET status=$7,data_json=$8 WHERE {SCOPE} AND attempt_id=$5 AND run_id=$6 AND status='started'"),&parameters(&values)).await.map_err(unavailable)?;
            if changed != 1 {
                return Err(CoordinationError::Conflict.into());
            }
            crate::billing::provider_in(&tx,env,attempt).await?;
            append_in(&tx,env,"provider_attempt_terminal",&serde_json::json!({"attempt_id":attempt.attempt_id,"status":attempt.status,"error_kind":attempt.error_kind,"retry_after_ms":attempt.retry_after_ms,"stream_committed":attempt.stream_committed,"first_event_at_ms":attempt.first_event_at_ms,"finished_at_ms":attempt.finished_at_ms,"usage":attempt.usage})).await?;
        }
        seal_in(&tx, env).await?;
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }
}
#[async_trait::async_trait]
impl RunAttemptRepository for PgCanonicalStore {
    async fn start_run_attempt(
        &self,
        env: &EnterpriseOwnerEnvelope,
        attempt: &ProviderAttempt,
    ) -> Result<(), RepositoryError> {
        self.write_attempt(env, attempt, true).await
    }
    async fn finish_run_attempt(
        &self,
        env: &EnterpriseOwnerEnvelope,
        attempt: &ProviderAttempt,
    ) -> Result<(), RepositoryError> {
        self.write_attempt(env, attempt, false).await
    }
    async fn read_attempt_usage(
        &self,
        env: &EnterpriseOwnerEnvelope,
    ) -> Result<ProviderUsage, RepositoryError> {
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
        let mut total = ProviderUsage::default();
        for attempt in attempts_in(&tx, &env.scope, &env.owner.run_id).await? {
            if let Some(usage) = attempt.usage {
                total.add_attempt(&usage);
            }
        }
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(total)
    }
    async fn append_provider_event(
        &self,
        env: &EnterpriseOwnerEnvelope,
        event: &str,
        data: &serde_json::Value,
    ) -> Result<EventSeq, RepositoryError> {
        if !matches!(
            event,
            "provider_circuit" | "provider_retry" | "provider_fallback"
        ) || !data.is_object()
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        encode(data, 65536)?;
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
        let seq = append_in(&tx, env, event, data).await?;
        seal_in(&tx, env).await?;
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(seq)
    }
}
