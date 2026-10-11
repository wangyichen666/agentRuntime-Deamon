//! 后台资源的唯一持久状态与日志游标；数据库回执不执行或猜测进程身份。
use crate::canonical::{PgCanonicalStore, field, integer, permanent_life, set_scope, unsigned};
use crate::events::{append_in, seal_in};
use crate::facts::frozen_in;
use crate::identities::exact_owner_in;
use crate::sessions::encode;
use crate::{SCOPE, now, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::RunResourceRepository;
use tokio_postgres::Transaction;

#[cfg(test)]
#[path = "resource_contract.rs"]
mod tests;

fn id_check(id: ResourceId) -> Result<(), CoordinationError> {
    if id.0 == 0 || id.0 > i64::MAX as u64 {
        return Err(CoordinationError::InvalidRequest);
    }
    Ok(())
}
fn key_check(key: &SessionKey, life: &SessionLifetimeId) -> Result<(), CoordinationError> {
    if key.0.is_empty() || key.0.len() > 1024 || life.0.is_empty() || life.0.len() > 128 {
        return Err(CoordinationError::InvalidRequest);
    }
    Ok(())
}
fn values(scope: &TenantContext, id: ResourceId) -> Vec<String> {
    let mut values = scope_values(scope);
    values.push(id.0.to_string());
    values
}
async fn life_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    key: &SessionKey,
    life: &SessionLifetimeId,
) -> Result<(), CoordinationError> {
    key_check(key, life)?;
    permanent_life(
        db,
        &SessionWriterLeaseKey {
            scope: scope.clone(),
            session_key: key.clone(),
            session_lifetime_id: life.clone(),
        },
    )
    .await
}

/// 首先在 SQL 按完整域筛选元数据，校验永久 owner 后才读有界正文。
async fn record_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    key: &SessionKey,
    life: &SessionLifetimeId,
    id: ResourceId,
    lock: bool,
) -> Result<ResourceRecord, CoordinationError> {
    let suffix = if lock { " FOR UPDATE" } else { "" };
    let params = values(scope, id);
    let row = db.query_opt(&format!("SELECT CASE WHEN octet_length(run_id) BETWEEN 1 AND 1024 THEN run_id END,CASE WHEN octet_length(lifetime) BETWEEN 1 AND 128 THEN lifetime END FROM resources WHERE {SCOPE} AND id=$5::text::bigint{suffix}"), &parameters(&params)).await.map_err(unavailable)?.ok_or(CoordinationError::Conflict)?;
    let run = RunId(field(&row, 0)?);
    let owner = exact_owner_in(db, scope, &run).await?;
    if owner.session_key != *key || owner.session_lifetime_id != *life {
        return Err(CoordinationError::Forbidden);
    }
    if field(&row, 1)? != life.0 {
        return Err(CoordinationError::Corrupt);
    }
    let row = db.query_one(&format!("SELECT CASE WHEN octet_length(owner_json) BETWEEN 1 AND 8192 THEN owner_json END,CASE WHEN octet_length(state)<=32 THEN state END,CASE WHEN octet_length(cwd)<=16384 THEN cwd END,CASE WHEN octet_length(requested)<=64 THEN requested END,CASE WHEN octet_length(effective)<=64 THEN effective END,CASE WHEN octet_length(process_identity)<=4096 THEN process_identity END,log_cursor,CASE WHEN octet_length(terminal_reason)<=4096 THEN terminal_reason END,(process_identity IS NULL OR octet_length(process_identity)<=4096) AND (terminal_reason IS NULL OR octet_length(terminal_reason)<=4096) FROM resources WHERE {SCOPE} AND id=$5::text::bigint"), &parameters(&params)).await.map_err(unavailable)?;
    let saved: ExactOwner =
        serde_json::from_str(&field(&row, 0)?).map_err(|_| CoordinationError::Corrupt)?;
    let state = field(&row, 1)?;
    if saved != owner
        || !valid_resource_state(&state)
        || !row.try_get::<_, bool>(8).map_err(unavailable)?
    {
        return Err(CoordinationError::Corrupt);
    }
    Ok(ResourceRecord {
        id,
        owner,
        state,
        cwd: field(&row, 2)?,
        sandbox_requested: field(&row, 3)?,
        sandbox_effective: field(&row, 4)?,
        process_identity: row.try_get(5).map_err(unavailable)?,
        log_cursor: unsigned(row.try_get(6).map_err(unavailable)?)?,
        terminal_reason: row.try_get(7).map_err(unavailable)?,
    })
}

async fn execution_in(
    db: &Transaction<'_>,
    proof: &SessionWriterLease,
    env: &EnterpriseOwnerEnvelope,
) -> Result<RunSnapshot, RepositoryError> {
    PgCanonicalStore::fence_envelope_in(db, proof, env).await?;
    let mut values = scope_values(&env.scope);
    values.push(env.owner.session_key.0.clone());
    let generation: i64 = db
        .query_one(
            &format!("SELECT run_generation FROM session_heads WHERE {SCOPE} AND session_id=$5"),
            &parameters(&values),
        )
        .await
        .map_err(unavailable)?
        .try_get(0)
        .map_err(unavailable)?;
    if unsigned(generation)? != env.owner.run_generation.0 {
        return Err(AuthorizationError::StaleOwner.into());
    }
    frozen_in(db, &env.scope, &env.owner.run_id)
        .await?
        .ok_or_else(|| CoordinationError::Corrupt.into())
}

async fn owned_in(
    db: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    id: ResourceId,
) -> Result<ResourceRecord, RepositoryError> {
    let record = record_in(
        db,
        &env.scope,
        &env.owner.session_key,
        &env.owner.session_lifetime_id,
        id,
        true,
    )
    .await?;
    if record.owner != env.owner {
        return Err(AuthorizationError::StaleOwner.into());
    }
    Ok(record)
}

#[async_trait::async_trait]
impl RunResourceRepository for PgCanonicalStore {
    async fn read_resource_settings(
        &self,
        env: &EnterpriseOwnerEnvelope,
    ) -> Result<RunSnapshot, RepositoryError> {
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        let result = execution_in(&tx, &proof, env).await?;
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }

    async fn create_run_resource(
        &self,
        env: &EnterpriseOwnerEnvelope,
        cwd: &str,
        requested: &str,
        effective: &str,
    ) -> Result<ResourceRecord, RepositoryError> {
        if cwd.len() > 16384 || requested.len() > 64 || effective.len() > 64 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        // 不同 session 的启动必须共享并发预算锁；先域锁、后 head/lease/resource。
        let scope_lock = encode(&env.scope, 4096)?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,44))",
            &[&scope_lock],
        )
        .await
        .map_err(unavailable)?;
        let frozen = execution_in(&tx, &proof, env).await?;
        // 原 Native 后台进程只向 Local 开放，不声称已有远程 worker 强隔离。
        if env.scope != TenantContext::local() {
            return Err(AuthorizationError::Forbidden.into());
        }
        if frozen.cwd != cwd
            || frozen.sandbox_requested != requested
            || frozen.sandbox_effective != effective
            || effective != "native"
        {
            return Err(CoordinationError::Conflict.into());
        }
        let mut params = scope_values(&env.scope);
        params.push(env.owner.run_id.0.clone());
        let state: String = tx
            .query_one(
                &format!("SELECT status FROM runs WHERE {SCOPE} AND id=$5 FOR UPDATE"),
                &parameters(&params),
            )
            .await
            .map_err(unavailable)?
            .try_get(0)
            .map_err(unavailable)?;
        if !matches!(state.as_str(), "running" | "waiting_interaction") {
            return Err(CoordinationError::Conflict.into());
        }
        let count: i64 = tx.query_one(&format!("SELECT count(*)::bigint FROM resources WHERE {SCOPE} AND state IN ('starting','running','stopping')"), &parameters(&params[..4])).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
        if count >= 16 {
            return Err(CoordinationError::Conflict.into());
        }
        params.extend([
            env.owner.session_lifetime_id.0.clone(),
            encode(&env.owner, 8192)?,
            cwd.into(),
            requested.into(),
            effective.into(),
            now(&tx).await?.to_string(),
        ]);
        let row = tx.query_one("INSERT INTO resources(organization_id,tenant_id,project_id,agent_uid,run_id,lifetime,state,owner_json,cwd,requested,effective,updated_at_ms) VALUES($1,$2,$3,$4,$5,$6,'starting',$7,$8,$9,$10,$11::text::bigint) RETURNING id", &parameters(&params)).await.map_err(unavailable)?;
        let id = ResourceId(unsigned(row.try_get(0).map_err(unavailable)?)?);
        crate::billing::allocate_sandbox_in(&tx,env,&format!("resource:{}",id.0)).await?;
        let result = owned_in(&tx, env, id).await?;
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }

    async fn update_run_resource(
        &self,
        env: &EnterpriseOwnerEnvelope,
        id: ResourceId,
        state: &str,
        identity: Option<&str>,
        reason: Option<&str>,
    ) -> Result<ResourceRecord, RepositoryError> {
        id_check(id)?;
        if state.len() > 32
            || identity.is_some_and(|s| s.len() > 4096)
            || reason.is_some_and(|s| s.len() > 4096)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        execution_in(&tx, &proof, env).await?;
        let old = owned_in(&tx, env, id).await?;
        if !resource_transition_allowed(&old.state, state) {
            return Err(CoordinationError::Conflict.into());
        }
        let mut params = values(&env.scope, id);
        params.extend([state.into(), now(&tx).await?.to_string()]);
        let mut bindings = parameters(&params);
        bindings.extend([
            &identity as &(dyn tokio_postgres::types::ToSql + Sync),
            &reason,
        ]);
        tx.execute(&format!("UPDATE resources SET state=$6,updated_at_ms=$7::text::bigint,process_identity=COALESCE($8::text,process_identity),terminal_reason=$9::text WHERE {SCOPE} AND id=$5::text::bigint"), &bindings).await.map_err(unavailable)?;
        if matches!(state,"completed"|"failed"|"stopped"|"orphaned") {
            crate::billing::finish_sandbox_in(&tx,env,&format!("resource:{}",id.0),state=="orphaned").await?;
            let run=crate::executions::record_in(&tx,&env.scope,&env.owner.run_id).await?;
            if run.status.terminal(){crate::billing::terminal_in(&tx,env,run.status).await?;}
        }
        append_in(
            &tx,
            env,
            "resource_updated",
            &serde_json::json!({"resource_id":id,"state":state}),
        )
        .await?;
        seal_in(&tx, env).await?;
        let result = owned_in(&tx, env, id).await?;
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }

    async fn append_run_resource_log(
        &self,
        env: &EnterpriseOwnerEnvelope,
        id: ResourceId,
        content: &str,
    ) -> Result<u64, RepositoryError> {
        id_check(id)?;
        if content.len() > 16384 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        execution_in(&tx, &proof, env).await?;
        let record = owned_in(&tx, env, id).await?;
        if !matches!(record.state.as_str(), "running" | "stopping") {
            return Err(CoordinationError::Conflict.into());
        }
        let params = values(&env.scope, id);
        let row = tx.query_one(&format!("SELECT COALESCE(sum(octet_length(content)),0)::bigint,count(*)::bigint,COALESCE(max(cursor),0)::bigint FROM resource_logs WHERE {SCOPE} AND resource_id=$5::text::bigint"), &parameters(&params)).await.map_err(unavailable)?;
        let size = unsigned(row.try_get(0).map_err(unavailable)?)?;
        if unsigned(row.try_get(1).map_err(unavailable)?)? != record.log_cursor
            || unsigned(row.try_get(2).map_err(unavailable)?)? != record.log_cursor
        {
            return Err(CoordinationError::Corrupt.into());
        }
        let next = if let Some(kept) = bounded_resource_log(content, size)? {
            let next = record
                .log_cursor
                .checked_add(1)
                .ok_or(CoordinationError::Corrupt)?;
            integer(next)?;
            let mut params = params;
            params.extend([next.to_string(), kept.into()]);
            tx.execute("INSERT INTO resource_logs VALUES($1,$2,$3,$4,$5::text::bigint,$6::text::bigint,$7)", &parameters(&params)).await.map_err(unavailable)?;
            tx.execute(&format!("UPDATE resources SET log_cursor=$6::text::bigint WHERE {SCOPE} AND id=$5::text::bigint"), &parameters(&params[..6])).await.map_err(unavailable)?;
            next
        } else {
            record.log_cursor
        };
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(next)
    }

    async fn reconcile_run_resource(
        &self,
        env: &EnterpriseOwnerEnvelope,
        id: ResourceId,
        state: &str,
        evidence: &str,
    ) -> Result<ResourceRecord, RepositoryError> {
        id_check(id)?;
        let reason = resource_reconcile_reason(state, evidence)?;
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        execution_in(&tx, &proof, env).await?;
        let old = owned_in(&tx, env, id).await?;
        if old.state == state && old.terminal_reason.as_deref() == Some(&reason) {
            Self::before_commit(&tx, &proof).await?;
            tx.commit().await.map_err(unavailable)?;
            return Ok(old);
        }
        if old.state != "orphaned" {
            return Err(CoordinationError::Conflict.into());
        }
        let mut params = values(&env.scope, id);
        params.extend([state.into(), reason, now(&tx).await?.to_string()]);
        tx.execute(&format!("UPDATE resources SET state=$6,terminal_reason=$7,updated_at_ms=$8::text::bigint WHERE {SCOPE} AND id=$5::text::bigint"), &parameters(&params)).await.map_err(unavailable)?;
        append_in(&tx, env, "resource_reconciled", &serde_json::json!({"resource_id":id,"state":state,"evidence":evidence,"kill_sent":false})).await?;
        seal_in(&tx, env).await?;
        let result = owned_in(&tx, env, id).await?;
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }

    async fn read_resource_record(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        life: &SessionLifetimeId,
        id: ResourceId,
    ) -> Result<ResourceRecord, RepositoryError> {
        id_check(id)?;
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        life_in(&tx, scope, key, life).await?;
        let result = record_in(&tx, scope, key, life, id, false).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }

    async fn read_resource_page(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        life: &SessionLifetimeId,
        after: ResourceId,
        limit: usize,
    ) -> Result<Vec<ResourceRecord>, RepositoryError> {
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
        life_in(&tx, scope, key, life).await?;
        let mut params = scope_values(scope);
        params.extend([
            key.0.clone(),
            life.0.clone(),
            after.0.to_string(),
            limit.clamp(1, 100).to_string(),
        ]);
        let rows = tx.query("SELECT p.id,(octet_length(p.owner_json)::bigint+octet_length(p.state)+octet_length(p.cwd)+octet_length(p.requested)+octet_length(p.effective)+COALESCE(octet_length(p.process_identity),0)+COALESCE(octet_length(p.terminal_reason),0)) FROM resources p JOIN runs r ON (r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id)=(p.organization_id,p.tenant_id,p.project_id,p.agent_uid,p.run_id) WHERE r.organization_id=$1 AND r.tenant_id=$2 AND r.project_id=$3 AND r.agent_uid=$4 AND r.session_id=$5 AND r.lifetime=$6 AND p.id>$7::text::bigint ORDER BY p.id LIMIT $8::text::bigint", &parameters(&params)).await.map_err(unavailable)?;
        let mut bytes = 0u64;
        let mut result = Vec::new();
        for row in rows {
            bytes = bytes.saturating_add(unsigned(row.try_get(1).map_err(unavailable)?)?);
            if bytes > 1024 * 1024 - 16384 {
                return Err(CoordinationError::Corrupt.into());
            }
            result.push(
                record_in(
                    &tx,
                    scope,
                    key,
                    life,
                    ResourceId(unsigned(row.try_get(0).map_err(unavailable)?)?),
                    false,
                )
                .await?,
            );
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }

    async fn read_resource_log_page(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        life: &SessionLifetimeId,
        id: ResourceId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<(u64, String)>, RepositoryError> {
        id_check(id)?;
        integer(after)?;
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        life_in(&tx, scope, key, life).await?;
        let record = record_in(&tx, scope, key, life, id, false).await?;
        let mut params = values(scope, id);
        params.extend([after.to_string(), limit.clamp(1, 100).to_string()]);
        let rows = tx.query(&format!("SELECT cursor,CASE WHEN octet_length(content)<=65536 THEN content END FROM resource_logs WHERE {SCOPE} AND resource_id=$5::text::bigint AND cursor>$6::text::bigint ORDER BY cursor LIMIT $7::text::bigint"), &parameters(&params)).await.map_err(unavailable)?;
        let mut cursor = after;
        let mut bytes = 0usize;
        let mut result = Vec::new();
        for row in rows {
            let next = unsigned(row.try_get(0).map_err(unavailable)?)?;
            if Some(next) != cursor.checked_add(1) || next > record.log_cursor {
                return Err(CoordinationError::Corrupt.into());
            }
            let content = field(&row, 1)?;
            bytes = bytes.saturating_add(content.len());
            if bytes > 65536 {
                return Err(CoordinationError::Corrupt.into());
            }
            result.push((next, content));
            cursor = next;
        }
        if result.len() < limit.clamp(1, 100) && cursor < record.log_cursor {
            return Err(CoordinationError::Corrupt.into());
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
}
