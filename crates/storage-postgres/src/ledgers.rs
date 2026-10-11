//! 同事务审计链和配额账本；不依赖缓存计数，不把已预占额度重复消耗。
use crate::canonical::{PgCanonicalStore, field, integer, set_scope, unsigned};
use crate::{SCOPE, now, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::{
    AuditReadRepository, QuotaRepository, decode_audit_entry, seal_audit_entry,
};
use tokio_postgres::{Row, Transaction};
const GENESIS: &str = "0000000000000000000000000000000000000000000000000000000000000000";
fn ledger(error: RepositoryError) -> LedgerError {
    match error {
        RepositoryError::Ledger(e) => e,
        RepositoryError::Coordination(e) => e.into(),
        _ => CoordinationError::Forbidden.into(),
    }
}
fn metric_valid(metric: &str) -> bool {
    !metric.is_empty()
        && metric.len() <= 64
        && metric
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}
fn operation_valid(operation: &str) -> bool {
    !operation.is_empty() && operation.len() <= 1000
}
pub(crate) async fn append_audit_in(
    db: &Transaction<'_>,
    command: &AuditCommand,
    at: i64,
) -> Result<AuditEntry, CoordinationError> {
    // 验证预算先于clone与SQL；相同operation必须对应完全相同命令。
    crate::sessions::encode(command, 60 * 1024)?;
    seal_audit_entry(&AuditEntry {
        seq: 1,
        command: command.clone(),
        previous_digest: GENESIS.into(),
        digest: String::new(),
        created_at_ms: at,
    })?;
    let values = scope_values(&command.scope);
    db.execute(
        "INSERT INTO audit_heads VALUES($1,$2,$3,$4,1,0,$5) ON CONFLICT DO NOTHING",
        &parameters(&{
            let mut params = values.clone();
            params.push(GENESIS.into());
            params
        }),
    )
    .await
    .map_err(unavailable)?;
    let head=db.query_one(&format!("SELECT seq,CASE WHEN octet_length(head_digest)=64 THEN head_digest END FROM audit_heads WHERE {SCOPE} AND slot=1 FOR UPDATE"),&parameters(&values)).await.map_err(unavailable)?;
    let mut lookup = values.clone();
    lookup.push(command.operation_id.clone());
    if let Some(row)=db.query_opt(&format!("SELECT CASE WHEN octet_length(entry_json)<=65536 THEN entry_json END,CASE WHEN octet_length(entry_digest)=64 THEN entry_digest END FROM audit_ledger WHERE {SCOPE} AND operation_id=$5"),&parameters(&lookup)).await.map_err(unavailable)?{
        let entry=decode_audit_entry(&field(&row,0)?,&field(&row,1)?)?;
        if entry.command!=*command{return Err(CoordinationError::Conflict);}
        return Ok(entry);
    }
    let seq = unsigned(head.try_get(0).map_err(unavailable)?)?
        .checked_add(1)
        .filter(|n| *n <= i64::MAX as u64)
        .ok_or(CoordinationError::Conflict)?;
    let previous = field(&head, 1)?;
    let mut entry = AuditEntry {
        seq,
        command: command.clone(),
        previous_digest: previous,
        digest: String::new(),
        created_at_ms: at,
    };
    let (digest, raw) = seal_audit_entry(&entry)?;
    entry.digest = digest;
    let resource =
        serde_json::to_string(&command.resource).map_err(|_| CoordinationError::InvalidRequest)?;
    let mut values = values;
    values.extend([
        seq.to_string(),
        command.operation_id.clone(),
        command.subject_id.as_str().into(),
        command.action.clone(),
        resource,
        command.outcome.clone(),
        entry.previous_digest.clone(),
        entry.digest.clone(),
        raw,
        at.to_string(),
    ]);
    db.execute("INSERT INTO audit_ledger VALUES($1,$2,$3,$4,$5::text::bigint,$6,$7,$8,$9,$10,$11,$12,$13,$14::text::bigint)",&parameters(&values)).await.map_err(unavailable)?;
    let mut params = scope_values(&command.scope);
    params.extend([seq.to_string(), entry.digest.clone()]);
    db.execute(
        &format!(
            "UPDATE audit_heads SET seq=$5::text::bigint,head_digest=$6 WHERE {SCOPE} AND slot=1"
        ),
        &parameters(&params),
    )
    .await
    .map_err(unavailable)?;
    Ok(entry)
}
#[async_trait::async_trait]
impl AuditReadRepository for PgCanonicalStore {
    async fn read_audit_entries(
        &self,
        scope: &TenantContext,
        after: u64,
        limit: u32,
    ) -> Result<Vec<AuditEntry>, CoordinationError> {
        if limit == 0 || limit > 64 {
            return Err(CoordinationError::InvalidRequest);
        }
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
        let mut values = scope_values(scope);
        values.push(after.to_string());
        let mut previous = if after == 0 {
            GENESIS.into()
        } else {
            field(&tx.query_opt(&format!("SELECT CASE WHEN octet_length(entry_digest)=64 THEN entry_digest END FROM audit_ledger WHERE {SCOPE} AND seq=$5::text::bigint"),&parameters(&values)).await.map_err(unavailable)?.ok_or(CoordinationError::InvalidRequest)?,0)?
        };
        values.push(limit.to_string());
        let rows=tx.query(&format!("SELECT seq,CASE WHEN octet_length(entry_json)<=65536 THEN entry_json END,CASE WHEN octet_length(entry_digest)=64 THEN entry_digest END,operation_id,subject_id,action,outcome,created_at_ms,previous_digest,CASE WHEN octet_length(resource_json)<=65536 THEN resource_json END FROM audit_ledger WHERE {SCOPE} AND seq>$5::text::bigint ORDER BY seq LIMIT $6::text::bigint"),&parameters(&values)).await.map_err(unavailable)?;
        let mut entries = Vec::new();
        let mut bytes = 0;
        let mut expected = after;
        for row in rows {
            let raw = field(&row, 1)?;
            bytes += raw.len();
            if bytes > 1024 * 1024 {
                return Err(CoordinationError::InvalidRequest);
            }
            let entry = decode_audit_entry(&raw, &field(&row, 2)?)?;
            expected = expected.checked_add(1).ok_or(CoordinationError::Corrupt)?;
            if entry.seq != expected
                || entry.seq != unsigned(row.try_get(0).map_err(unavailable)?)?
                || entry.command.scope != *scope
                || entry.previous_digest != previous
                || entry.command.operation_id != row.try_get::<_, String>(3).map_err(unavailable)?
                || entry.command.subject_id.as_str()
                    != row.try_get::<_, String>(4).map_err(unavailable)?
                || entry.command.action != row.try_get::<_, String>(5).map_err(unavailable)?
                || entry.command.outcome != row.try_get::<_, String>(6).map_err(unavailable)?
                || entry.created_at_ms != row.try_get::<_, i64>(7).map_err(unavailable)?
                || entry.previous_digest != row.try_get::<_, String>(8).map_err(unavailable)?
                || serde_json::from_str::<serde_json::Value>(&field(&row, 9)?)
                    .map_err(|_| CoordinationError::Corrupt)?
                    != entry.command.resource
            {
                return Err(CoordinationError::Corrupt);
            }
            previous = entry.digest.clone();
            entries.push(entry);
        }
        if let Some(head) = tx
            .query_opt(
                &format!("SELECT seq,head_digest FROM audit_heads WHERE {SCOPE} AND slot=1"),
                &parameters(&scope_values(scope)),
            )
            .await
            .map_err(unavailable)?
        {
            let head_seq = unsigned(head.try_get(0).map_err(unavailable)?)?;
            if expected > head_seq
                || (expected == head_seq
                    && previous != head.try_get::<_, String>(1).map_err(unavailable)?)
                || (entries.len() < (limit as usize) && expected < head_seq)
            {
                return Err(CoordinationError::Corrupt);
            }
        } else if after != 0 || !entries.is_empty() {
            return Err(CoordinationError::Corrupt);
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(entries)
    }
}
fn account(row: &Row, scope: &TenantContext) -> Result<QuotaAccount, CoordinationError> {
    let value = QuotaAccount {
        scope: scope.clone(),
        metric: row.try_get(0).map_err(unavailable)?,
        window_start_ms: row.try_get(1).map_err(unavailable)?,
        window_end_ms: row.try_get(2).map_err(unavailable)?,
        budget: unsigned(row.try_get(3).map_err(unavailable)?)?,
        consumed: unsigned(row.try_get(4).map_err(unavailable)?)?,
        reserved: unsigned(row.try_get(5).map_err(unavailable)?)?,
        revision: unsigned(row.try_get(6).map_err(unavailable)?)?,
    };
    if !metric_valid(&value.metric)
        || value.window_end_ms <= value.window_start_ms
        || value
            .consumed
            .checked_add(value.reserved)
            .is_none_or(|n| n > value.budget)
    {
        return Err(CoordinationError::Corrupt);
    }
    Ok(value)
}
fn reservation(row: &Row, scope: &TenantContext) -> Result<QuotaReservation, CoordinationError> {
    let state: QuotaState = serde_json::from_value(serde_json::Value::String(
        row.try_get(5).map_err(unavailable)?,
    ))
    .map_err(|_| CoordinationError::Corrupt)?;
    Ok(QuotaReservation {
        scope: scope.clone(),
        operation_id: row.try_get(0).map_err(unavailable)?,
        metric: row.try_get(1).map_err(unavailable)?,
        window_start_ms: row.try_get(2).map_err(unavailable)?,
        amount: unsigned(row.try_get(3).map_err(unavailable)?)?,
        run_id: RunId(row.try_get(4).map_err(unavailable)?),
        state,
        created_at_ms: row.try_get(6).map_err(unavailable)?,
        updated_at_ms: row.try_get(7).map_err(unavailable)?,
    })
}
const ACCOUNT: &str = "metric,window_start_ms,window_end_ms,budget,consumed,reserved,revision";
const RESERVATION: &str =
    "operation_id,metric,window_start_ms,amount,run_id,state,created_at_ms,updated_at_ms";
impl PgCanonicalStore {
    /// 配置在原身份事务纯验证AgentConfigure；窗口创建后不可覆盖消耗历史或重叠开放窗口。
    pub async fn configure_quota_window(
        &self,
        request: &AuthorizationRequest,
        value: &QuotaAccount,
        verifier: &dyn AuthorizationVerifier,
    ) -> Result<(), RepositoryError> {
        if request.action != Action::AgentConfigure
            || request.context.scope != value.scope
            || !metric_valid(&value.metric)
            || value.consumed != 0
            || value.reserved != 0
            || value.revision != 0
            || value.window_end_ms <= value.window_start_ms
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        integer(value.budget)?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &value.scope).await?;
        let identity = crate::identities::identity_in(
            &tx,
            &value.scope,
            &request.context.principal.subject_id,
        )
        .await?;
        verifier.verify(request, &identity)?;
        let mut values = scope_values(&value.scope);
        values.push(value.metric.clone());
        let lock = serde_json::to_string(&(&value.scope, &value.metric))
            .map_err(|_| CoordinationError::InvalidRequest)?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,41))",
            &[&lock],
        )
        .await
        .map_err(unavailable)?;
        let mut overlap = values.clone();
        overlap.extend([
            value.window_start_ms.to_string(),
            value.window_end_ms.to_string(),
        ]);
        let exists:bool=tx.query_one(&format!("SELECT EXISTS(SELECT 1 FROM quota_accounts WHERE {SCOPE} AND metric=$5 AND window_start_ms<$7::text::bigint AND window_end_ms>$6::text::bigint)"),&parameters(&overlap)).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
        if exists {
            return Err(CoordinationError::Conflict.into());
        }
        values.extend([
            value.budget.to_string(),
            value.window_start_ms.to_string(),
            value.window_end_ms.to_string(),
        ]);
        tx.execute("INSERT INTO quota_accounts(organization_id,tenant_id,project_id,agent_uid,metric,budget,consumed,reserved,revision,window_start_ms,window_end_ms) VALUES($1,$2,$3,$4,$5,$6::text::bigint,0,0,0,$7::text::bigint,$8::text::bigint)",&parameters(&values)).await.map_err(unavailable)?;
        append_audit_in(&tx,&AuditCommand{scope:value.scope.clone(),operation_id:format!("quota-window:{}:{}",value.metric,value.window_start_ms),subject_id:request.context.principal.subject_id.clone(),action:"quota.configure".into(),resource:serde_json::json!({"metric":value.metric,"start_ms":value.window_start_ms,"end_ms":value.window_end_ms,"budget":value.budget}),outcome:"created".into()},now(&tx).await?).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }
}
#[async_trait::async_trait]
impl QuotaRepository for PgCanonicalStore {
    async fn read_quota_account(
        &self,
        scope: &TenantContext,
        metric: &str,
        window_start_ms: i64,
    ) -> Result<QuotaAccount, LedgerError> {
        if !metric_valid(metric) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let mut values = scope_values(scope);
        values.extend([metric.into(), window_start_ms.to_string()]);
        let row=tx.query_opt(&format!("SELECT {ACCOUNT} FROM quota_accounts WHERE {SCOPE} AND metric=$5 AND window_start_ms=$6::text::bigint"),&parameters(&values)).await.map_err(unavailable)?.ok_or(CoordinationError::Forbidden)?;
        let result = account(&row, scope)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
    async fn reserve_run_quota(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        operation: &str,
        metric: &str,
        amount: u64,
    ) -> Result<QuotaReservation, LedgerError> {
        let proof = self
            .bound_proof(&SessionWriterLeaseKey {
                scope: envelope.scope.clone(),
                session_key: envelope.owner.session_key.clone(),
                session_lifetime_id: envelope.owner.session_lifetime_id.clone(),
            })
            .await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &envelope.scope).await?;
        Self::fence_envelope_in(&tx, &proof, envelope)
            .await
            .map_err(ledger)?;
        let r = reserve_in(
            &tx,
            &envelope.scope,
            &envelope.owner.run_id,
            operation,
            metric,
            amount,
            false,
        )
        .await
        .map_err(ledger)?
        .ok_or(CoordinationError::Corrupt)?;
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(r)
    }
    async fn settle_run_quota(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        candidate: &QuotaReservation,
        state: QuotaState,
    ) -> Result<QuotaReservation, LedgerError> {
        if candidate.scope != envelope.scope || candidate.run_id != envelope.owner.run_id {
            return Err(CoordinationError::Forbidden.into());
        }
        let proof = self
            .bound_proof(&SessionWriterLeaseKey {
                scope: envelope.scope.clone(),
                session_key: envelope.owner.session_key.clone(),
                session_lifetime_id: envelope.owner.session_lifetime_id.clone(),
            })
            .await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &envelope.scope).await?;
        Self::fence_envelope_in(&tx, &proof, envelope)
            .await
            .map_err(ledger)?;
        let r = settle_in(&tx, &envelope.scope, candidate, state)
            .await
            .map_err(ledger)?;
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(r)
    }
}
pub(crate) async fn reserve_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    run: &RunId,
    operation: &str,
    metric: &str,
    amount: u64,
    optional: bool,
) -> Result<Option<QuotaReservation>, RepositoryError> {
    if !metric_valid(metric) || !operation_valid(operation) || amount == 0 {
        return Err(CoordinationError::InvalidRequest.into());
    }
    integer(amount)?;
    let at = now(db).await?;
    let mut values = scope_values(scope);
    values.extend([metric.into(), at.to_string()]);
    // 锁顺序account→ledger；同一个run准入由canonical writer/head先序列化。
    let row=db.query_opt(&format!("SELECT {ACCOUNT} FROM quota_accounts WHERE {SCOPE} AND metric=$5 AND window_start_ms<=$6::text::bigint AND window_end_ms>$6::text::bigint ORDER BY window_start_ms DESC LIMIT 1 FOR UPDATE"),&parameters(&values)).await.map_err(unavailable)?;
    let mut lookup = scope_values(scope);
    lookup.push(operation.into());
    if let Some(row) = db
        .query_opt(
            &format!("SELECT {RESERVATION} FROM quota_ledger WHERE {SCOPE} AND operation_id=$5"),
            &parameters(&lookup),
        )
        .await
        .map_err(unavailable)?
    {
        let old = reservation(&row, scope)?;
        if old.metric != metric || old.amount != amount || old.run_id != *run {
            return Err(CoordinationError::Conflict.into());
        }
        return Ok(Some(old));
    }
    let Some(row) = row else {
        let configured: bool = db
            .query_one(
                &format!("SELECT EXISTS(SELECT 1 FROM quota_accounts WHERE {SCOPE} AND metric=$5)"),
                &parameters(&values[..5]),
            )
            .await
            .map_err(unavailable)?
            .try_get(0)
            .map_err(unavailable)?;
        if optional && !configured {
            return Ok(None);
        }
        return Err(LedgerError::WindowClosed.into());
    };
    let q = account(&row, scope)?;
    check_quota_reservation(&q, amount)?;
    let mut update = scope_values(scope);
    update.extend([
        metric.into(),
        q.window_start_ms.to_string(),
        amount.to_string(),
    ]);
    db.execute(&format!("UPDATE quota_accounts SET reserved=reserved+$7::text::bigint,revision=revision+1 WHERE {SCOPE} AND metric=$5 AND window_start_ms=$6::text::bigint"),&parameters(&update)).await.map_err(unavailable)?;
    let mut insert = scope_values(scope);
    insert.extend([
        operation.into(),
        metric.into(),
        q.window_start_ms.to_string(),
        amount.to_string(),
        run.0.clone(),
        at.to_string(),
    ]);
    db.execute("INSERT INTO quota_ledger(organization_id,tenant_id,project_id,agent_uid,operation_id,metric,window_start_ms,amount,run_id,state,created_at_ms,updated_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7::text::bigint,$8::text::bigint,$9,'reserved',$10::text::bigint,$10::text::bigint)",&parameters(&insert)).await.map_err(unavailable)?;
    Ok(Some(QuotaReservation {
        scope: scope.clone(),
        operation_id: operation.into(),
        metric: metric.into(),
        window_start_ms: q.window_start_ms,
        amount,
        run_id: run.clone(),
        state: QuotaState::Reserved,
        created_at_ms: at,
        updated_at_ms: at,
    }))
}
async fn settle_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    c: &QuotaReservation,
    state: QuotaState,
) -> Result<QuotaReservation, RepositoryError> {
    if state == QuotaState::Reserved
        || c.scope != *scope
        || !metric_valid(&c.metric)
        || !operation_valid(&c.operation_id)
    {
        return Err(CoordinationError::InvalidRequest.into());
    }
    let mut values = scope_values(scope);
    values.extend([c.metric.clone(), c.window_start_ms.to_string()]);
    let q=account(&db.query_opt(&format!("SELECT {ACCOUNT} FROM quota_accounts WHERE {SCOPE} AND metric=$5 AND window_start_ms=$6::text::bigint FOR UPDATE"),&parameters(&values)).await.map_err(unavailable)?.ok_or(CoordinationError::Forbidden)?,scope)?;
    let mut lookup = scope_values(scope);
    lookup.push(c.operation_id.clone());
    let mut old=reservation(&db.query_opt(&format!("SELECT {RESERVATION} FROM quota_ledger WHERE {SCOPE} AND operation_id=$5 FOR UPDATE"),&parameters(&lookup)).await.map_err(unavailable)?.ok_or(CoordinationError::Forbidden)?,scope)?;
    if old.metric != c.metric
        || old.run_id != c.run_id
        || old.amount != c.amount
        || old.window_start_ms != c.window_start_ms
        || old.created_at_ms != c.created_at_ms
    {
        return Err(CoordinationError::Conflict.into());
    }
    if old.state != QuotaState::Reserved {
        if old.state != state {
            return Err(CoordinationError::Conflict.into());
        }
        return Ok(old);
    }
    if q.reserved < old.amount {
        return Err(CoordinationError::Corrupt.into());
    }
    values.extend([
        old.amount.to_string(),
        if state == QuotaState::Consumed {
            old.amount
        } else {
            0
        }
        .to_string(),
    ]);
    db.execute(&format!("UPDATE quota_accounts SET reserved=reserved-$7::text::bigint,consumed=consumed+$8::text::bigint,revision=revision+1 WHERE {SCOPE} AND metric=$5 AND window_start_ms=$6::text::bigint"),&parameters(&values)).await.map_err(unavailable)?;
    let at = now(db).await?;
    lookup.extend([state.as_str().into(), at.to_string()]);
    db.execute(&format!("UPDATE quota_ledger SET state=$6,updated_at_ms=$7::text::bigint WHERE {SCOPE} AND operation_id=$5 AND state='reserved'"),&parameters(&lookup)).await.map_err(unavailable)?;
    old.state = state;
    old.updated_at_ms = at;
    Ok(old)
}
pub(crate) async fn terminal_in(
    db: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    status: RunStatus,
) -> Result<(), RepositoryError> {
    crate::billing::terminal_in(db,env,status).await?;
    let mut values = scope_values(&env.scope);
    values.push(format!("admission:{}", env.owner.run_id.0));
    if let Some(row) = db
        .query_opt(
            &format!("SELECT {RESERVATION} FROM quota_ledger WHERE {SCOPE} AND operation_id=$5"),
            &parameters(&values),
        )
        .await
        .map_err(unavailable)?
    {
        let r = reservation(&row, &env.scope)?;
        values[4] = env.owner.run_id.0.clone();
        let started:bool=db.query_one(&format!("SELECT EXISTS(SELECT 1 FROM events WHERE {SCOPE} AND run_id=$5 AND event='run_started')"),&parameters(&values)).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
        settle_in(
            db,
            &env.scope,
            &r,
            if status == RunStatus::Cancelled && !started {
                QuotaState::Released
            } else {
                QuotaState::Consumed
            },
        )
        .await?;
    }
    Ok(())
}

#[async_trait::async_trait]
impl agent_storage_ports::QuotaManagementRepository for PgCanonicalStore {
    async fn configure_quota_window(
        &self,
        r: &AuthorizationRequest,
        q: &QuotaAccount,
        v: std::sync::Arc<dyn AuthorizationVerifier>,
    ) -> Result<(), RepositoryError> {
        PgCanonicalStore::configure_quota_window(self, r, q, v.as_ref()).await
    }
}
