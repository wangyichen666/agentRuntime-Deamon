//! 原事务中的费用 policy、分层预占及不可变 Provider 用量。
use crate::canonical::{field, set_scope, unsigned};
use crate::{PgCanonicalStore, SCOPE, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::{BillingRepository, billing_budget_key, freeze_billing};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio_postgres::Transaction;
fn decode(raw: &str, digest: &str) -> Result<FrozenBillingPolicy, RepositoryError> {
    if raw.len() > 65536 {
        return Err(CoordinationError::Corrupt.into());
    }
    let p: BillingPolicy = serde_json::from_str(raw).map_err(|_| CoordinationError::Corrupt)?;
    let f = freeze_billing(&p)?;
    if f.digest != digest {
        return Err(CoordinationError::Corrupt.into());
    }
    Ok(f)
}
pub(crate) async fn lock_in(
    tx: &Transaction<'_>,
    s: &TenantContext,
) -> Result<(), RepositoryError> {
    let key = serde_json::to_string(&(&s.organization_id, &s.tenant_id))
        .map_err(|_| CoordinationError::Corrupt)?;
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,63))",
        &[&key],
    )
    .await
    .map_err(unavailable)?;
    Ok(())
}
pub(crate) async fn current_in(
    tx: &Transaction<'_>,
    s: &TenantContext,
) -> Result<Option<FrozenBillingPolicy>, RepositoryError> {
    tx.query_opt(&format!("SELECT CASE WHEN octet_length(policy_json)<=65536 THEN policy_json END,p.digest FROM billing_policies p JOIN billing_current c USING(organization_id,tenant_id,project_id,agent_uid,digest) WHERE {SCOPE}"),&parameters(&scope_values(s))).await.map_err(unavailable)?.map(|r|decode(&field(&r,0)?,&field(&r,1)?)).transpose()
}
#[async_trait::async_trait]
impl BillingRepository for PgCanonicalStore {
    async fn configure_billing(
        &self,
        request: &AuthorizationRequest,
        policy: &BillingPolicy,
        operation: &str,
        verifier: Arc<dyn AuthorizationVerifier>,
    ) -> Result<FrozenBillingPolicy, RepositoryError> {
        if request.action != Action::IdentityManage || operation.is_empty() || operation.len() > 256
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let frozen = freeze_billing(policy)?;
        let p = &frozen.policy;
        let s = &request.context.scope;
        let actor = &request.context.principal.subject_id;
        let mut db = self.inner.client.lock().await;
        let tx = db.transaction().await.map_err(unavailable)?;
        set_scope(&tx, s).await?;
        let identity = crate::identities::identity_in(&tx, s, actor).await?;
        let mut checked = request.clone();
        checked.attributes.timestamp_ms = crate::now(&tx).await?;
        verifier.verify(&checked, &identity)?;
        lock_in(&tx, s).await?;
        let mut v = scope_values(s);
        v.extend([actor.as_str().into(), operation.into()]);
        if let Some(r)=tx.query_opt(&format!("SELECT policy_json,digest FROM billing_policies WHERE {SCOPE} AND actor_id=$5 AND operation_id=$6"),&parameters(&v)).await.map_err(unavailable)? {
            let old=decode(&field(&r,0)?,&field(&r,1)?)?;if old!=frozen{return Err(CoordinationError::Conflict.into())}return Ok(old)
        }
        let at = crate::now(&tx).await?;
        if at >= p.window_end_ms {
            return Err(CoordinationError::InvalidRequest.into());
        }
        for b in &p.budgets {
            let key = billing_budget_key(s, &b.dimension)?;
            let start = p.window_start_ms;
            let prior=tx.query_opt("SELECT window_end_ms,currency,reserved_micros,consumed_micros FROM billing_accounts WHERE organization_id=$1 AND tenant_id=$2 AND budget_key=$3 AND window_start_ms=$4 FOR UPDATE",&[&s.organization_id.as_str(),&s.tenant_id.as_str(),&key,&start]).await.map_err(unavailable)?;
            if let Some(r) = prior {
                let end: i64 = r.try_get(0).map_err(unavailable)?;
                let reserved = unsigned(r.try_get(2).map_err(unavailable)?)?;
                let consumed = unsigned(r.try_get(3).map_err(unavailable)?)?;
                if end != p.window_end_ms
                    || field(&r, 1)? != p.currency
                    || reserved
                        .checked_add(consumed)
                        .is_none_or(|n| n > b.hard_micros)
                {
                    return Err(CoordinationError::Conflict.into());
                }
            }
            tx.execute("INSERT INTO billing_accounts VALUES($1,$2,$3,$4,$5,$6,$7,$8,0,0,0) ON CONFLICT(organization_id,tenant_id,budget_key,window_start_ms) DO UPDATE SET hard_micros=excluded.hard_micros,soft_micros=excluded.soft_micros,revision=billing_accounts.revision+1",&[&s.organization_id.as_str(),&s.tenant_id.as_str(),&key,&start,&p.window_end_ms,&p.currency,&(b.hard_micros as i64),&(b.soft_micros as i64)]).await.map_err(unavailable)?;
        }
        let mut v = scope_values(s);
        v.extend([
            frozen.digest.clone(),
            p.version.clone(),
            serde_json::to_string(p).map_err(|_| CoordinationError::Corrupt)?,
            actor.as_str().into(),
            operation.into(),
            at.to_string(),
        ]);
        tx.execute(
            "INSERT INTO billing_policies VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::text::bigint)",
            &parameters(&v),
        )
        .await
        .map_err(unavailable)?;
        v.truncate(5);
        tx.execute("INSERT INTO billing_current VALUES($1,$2,$3,$4,$5) ON CONFLICT(organization_id,tenant_id,project_id,agent_uid) DO UPDATE SET digest=excluded.digest",&parameters(&v)).await.map_err(unavailable)?;
        crate::ledgers::append_audit_in(
            &tx,
            &AuditCommand {
                scope: s.clone(),
                operation_id: format!(
                    "billing.configure:{}",
                    format!(
                        "{:x}",
                        Sha256::digest(format!("{}:{operation}", actor.as_str()).as_bytes())
                    )
                ),
                subject_id: actor.clone(),
                action: "billing.configure".into(),
                resource: serde_json::json!({"digest":frozen.digest,"version":p.version}),
                outcome: "committed".into(),
            },
            at,
        )
        .await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(frozen)
    }
    async fn read_billing_policy(
        &self,
        s: &TenantContext,
    ) -> Result<Option<FrozenBillingPolicy>, RepositoryError> {
        let mut db = self.inner.client.lock().await;
        let tx = db.transaction().await.map_err(unavailable)?;
        set_scope(&tx, s).await?;
        let f = current_in(&tx, s).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(f)
    }
    async fn read_billing_accounts(
        &self,
        s: &TenantContext,
    ) -> Result<Vec<BillingAccount>, RepositoryError> {
        let mut db = self.inner.client.lock().await;
        let tx = db.transaction().await.map_err(unavailable)?;
        set_scope(&tx, s).await?;
        let Some(f) = current_in(&tx, s).await? else {
            return Ok(Vec::new());
        };
        let mut accounts = Vec::new();
        for b in f.policy.budgets {
            let key = billing_budget_key(s, &b.dimension)?;
            let r=tx.query_one("SELECT currency,window_start_ms,window_end_ms,hard_micros,soft_micros,reserved_micros,consumed_micros,revision FROM billing_accounts WHERE organization_id=$1 AND tenant_id=$2 AND budget_key=$3 AND window_start_ms=$4",&[&s.organization_id.as_str(),&s.tenant_id.as_str(),&key,&f.policy.window_start_ms]).await.map_err(unavailable)?;
            accounts.push(BillingAccount {
                dimension: b.dimension,
                currency: field(&r, 0)?,
                window_start_ms: r.try_get(1).map_err(unavailable)?,
                window_end_ms: r.try_get(2).map_err(unavailable)?,
                hard_micros: unsigned(r.try_get(3).map_err(unavailable)?)?,
                soft_micros: unsigned(r.try_get(4).map_err(unavailable)?)?,
                reserved_micros: unsigned(r.try_get(5).map_err(unavailable)?)?,
                consumed_micros: unsigned(r.try_get(6).map_err(unavailable)?)?,
                revision: unsigned(r.try_get(7).map_err(unavailable)?)?,
            });
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(accounts)
    }
    async fn read_billing_usage(
        &self,
        s: &TenantContext,
        after: u64,
        limit: u32,
    ) -> Result<UsagePage, RepositoryError> {
        if after > i64::MAX as u64 || !(1..=64).contains(&limit) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let mut db = self.inner.client.lock().await;
        let tx = db.transaction().await.map_err(unavailable)?;
        set_scope(&tx, s).await?;
        let mut v = scope_values(s);
        v.extend([after.to_string(), (limit + 1).to_string()]);
        let rows=tx.query(&format!("SELECT seq,CASE WHEN octet_length(owner_json)<=4096 THEN owner_json END,operation_id,model,kind,CASE WHEN octet_length(units_json)<=16384 THEN units_json END,policy_digest,currency,cost_micros,outcome_unknown,created_at_ms FROM usage_charges WHERE {SCOPE} AND seq>$5::text::bigint ORDER BY seq LIMIT $6::text::bigint"),&parameters(&v)).await.map_err(unavailable)?;
        let mut entries = Vec::new();
        let mut bytes = 0;
        for r in &rows {
            let owner = field(r, 1)?;
            let units = field(r, 5)?;
            bytes += owner.len() + units.len();
            if bytes > 1024 * 1024 {
                return Err(CoordinationError::Corrupt.into());
            }
            entries.push(UsageCharge {
                seq: unsigned(r.try_get(0).map_err(unavailable)?)?,
                scope: s.clone(),
                owner: serde_json::from_str(&owner).map_err(|_| CoordinationError::Corrupt)?,
                operation_id: field(r, 2)?,
                model: r.try_get(3).map_err(unavailable)?,
                kind: field(r, 4)?,
                units: serde_json::from_str(&units).map_err(|_| CoordinationError::Corrupt)?,
                policy_digest: r.try_get(6).map_err(unavailable)?,
                currency: r.try_get(7).map_err(unavailable)?,
                cost_micros: r
                    .try_get::<_, Option<i64>>(8)
                    .map_err(unavailable)?
                    .map(unsigned)
                    .transpose()?,
                outcome_unknown: r.try_get(9).map_err(unavailable)?,
                created_at_ms: r.try_get(10).map_err(unavailable)?,
            });
        }
        let has_more = entries.len() > limit as usize;
        entries.truncate(limit as usize);
        let next_seq = entries.last().map_or(after, |e| e.seq);
        tx.commit().await.map_err(unavailable)?;
        Ok(UsagePage {
            entries,
            next_seq,
            has_more,
        })
    }
}
pub(crate) async fn reserve_in(
    tx: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    snapshot: &RunSnapshot,
    enforce_current: bool,
) -> Result<(), RepositoryError> {
    lock_in(tx, &env.scope).await?;
    if !enforce_current && snapshot.billing_policy.is_none() && current_in(tx,&env.scope).await?.is_some() {return Err(CoordinationError::Conflict.into())}
    if enforce_current
        && current_in(tx, &env.scope).await?.as_ref() != snapshot.billing_policy.as_ref()
    {
        return Err(CoordinationError::Conflict.into());
    }
    let Some(frozen) = &snapshot.billing_policy else {
        return Ok(());
    };
    if freeze_billing(&frozen.policy)? != *frozen {
        return Err(CoordinationError::Corrupt.into());
    }
    let p = &frozen.policy;
    let at = crate::now(tx).await?;
    let amount = p.maximum_cost()? as i64;
    if at < p.window_start_ms
        || at >= p.window_end_ms
        || snapshot.route.as_ref().is_none_or(|r| {
            r.candidates
                .iter()
                .any(|c| !p.models.contains_key(&c.model))
        })
    {
        return Err(CoordinationError::Forbidden.into());
    }
    let mut binding = scope_values(&env.scope);
    binding.push(env.owner.run_id.0.clone());
    tx.execute(
        "INSERT INTO billing_run_limits VALUES($1,$2,$3,$4,$5,0,0,0)",
        &parameters(&binding),
    )
    .await
    .map_err(unavailable)?;
    for b in &p.budgets {
        let key = billing_budget_key(&env.scope, &b.dimension)?;
        let changed=tx.execute("UPDATE billing_accounts SET reserved_micros=reserved_micros+$5,revision=revision+1 WHERE organization_id=$1 AND tenant_id=$2 AND budget_key=$3 AND window_start_ms=$4 AND window_end_ms=$6 AND currency=$7 AND hard_micros-consumed_micros-reserved_micros>=$5",&[&env.scope.organization_id.as_str(),&env.scope.tenant_id.as_str(),&key,&p.window_start_ms,&amount,&p.window_end_ms,&p.currency]).await.map_err(unavailable)?;
        if changed != 1 {
            return Err(LedgerError::QuotaExceeded.into());
        }
        let mut v = scope_values(&env.scope);
        v.extend([
            env.owner.run_id.0.clone(),
            key,
            p.window_start_ms.to_string(),
            amount.to_string(),
            frozen.digest.clone(),
        ]);
        tx.execute("INSERT INTO billing_run_reservations VALUES($1,$2,$3,$4,$5,$6,$7::text::bigint,$8::text::bigint,$9,'reserved',NULL)",&parameters(&v)).await.map_err(unavailable)?;
    }
    Ok(())
}
pub(crate) async fn provider_in(
    tx: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    attempt: &ProviderAttempt,
) -> Result<(), RepositoryError> {
    let snapshot = crate::facts::frozen_in(tx, &env.scope, &env.owner.run_id).await?;
    let frozen = snapshot.as_ref().and_then(|s| s.billing_policy.as_ref());
    let cost = match (frozen, attempt.usage.as_ref()) {
        (Some(f), Some(u)) => f
            .policy
            .provider_cost(&attempt.model, attempt.api_type, u)
            .unwrap_or(None),
        _ => None,
    };
    let unknown = attempt.status == AttemptStatus::OutcomeUnknown || cost.is_none();
    append_in(tx,env,&format!("provider:{}",attempt.attempt_id),Some(&attempt.model),"provider",&serde_json::json!({"usage":attempt.usage,"api_type":attempt.api_type,"status":attempt.status,"pricing_rule":"missing_cache_fields_assumed_zero;missing_or_invalid_required_usage_unknown"}),frozen,cost,unknown).await
}
#[allow(clippy::too_many_arguments)]
pub(crate) async fn append_in(
    tx: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    operation: &str,
    model: Option<&str>,
    kind: &str,
    units: &serde_json::Value,
    frozen: Option<&FrozenBillingPolicy>,
    cost: Option<u64>,
    unknown: bool,
) -> Result<(), RepositoryError> {
    let operation = agent_storage_ports::billing_charge_operation(&env.owner.run_id, operation);
    let owner = crate::sessions::encode(&env.owner, 4096)?;
    let units = crate::sessions::encode(units, 16384)?;
    let at = crate::now(tx).await?;
    let amount = cost
        .map(|c| i64::try_from(c).map_err(|_| CoordinationError::Corrupt))
        .transpose()?;
    tx.execute("INSERT INTO usage_charges(organization_id,tenant_id,project_id,agent_uid,run_id,operation_id,model,kind,owner_json,units_json,policy_digest,currency,cost_micros,outcome_unknown,created_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",&[&env.scope.organization_id.as_str(),&env.scope.tenant_id.as_str(),&env.scope.project_id.as_str(),&env.scope.agent_uid.as_str(),&env.owner.run_id.0,&operation,&model,&kind,&owner,&units,&frozen.map(|f|f.digest.as_str()),&frozen.map(|f|f.policy.currency.as_str()),&amount,&unknown,&at]).await.map_err(unavailable)?;
    Ok(())
}

pub(crate) async fn start_provider_in(
    tx: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    attempt: &ProviderAttempt,
) -> Result<(), RepositoryError> {
    let snapshot = crate::facts::frozen_in(tx, &env.scope, &env.owner.run_id).await?;
    let Some(f) = snapshot.and_then(|s| s.billing_policy) else {
        return Ok(());
    };
    let l = &f.policy.limits;
    if attempt.round > l.max_rounds || !f.policy.models.contains_key(&attempt.model) {
        return Err(LedgerError::QuotaExceeded.into());
    }
    let mut v = scope_values(&env.scope);
    v.extend([
        env.owner.run_id.0.clone(),
        attempt.round.to_string(),
        attempt.candidate_index.to_string(),
    ]);
    let row=tx.query_opt(&format!("SELECT CASE WHEN octet_length(capture_json)<=2097152 THEN capture_json END FROM provider_requests WHERE {SCOPE} AND run_id=$5 AND round=$6::text::bigint AND candidate_index=$7::text::bigint ORDER BY storage_row_id DESC LIMIT 1"),&parameters(&v)).await.map_err(unavailable)?.ok_or(CoordinationError::Conflict)?;
    let capture: ProviderRequestCapture =
        serde_json::from_str(&field(&row, 0)?).map_err(|_| CoordinationError::Corrupt)?;
    let upper = agent_storage_ports::billing_input_upper_bound(&capture)?;
    v.truncate(5);
    v.extend([
        upper.to_string(),
        l.max_provider_attempts.to_string(),
        l.max_input_tokens.to_string(),
    ]);
    let changed=tx.execute(&format!("UPDATE billing_run_limits SET provider_attempts=provider_attempts+1,input_upper_bound=input_upper_bound+$6::text::bigint WHERE {SCOPE} AND run_id=$5 AND provider_attempts<$7::text::bigint AND input_upper_bound<=$8::text::bigint-$6::text::bigint"),&parameters(&v)).await.map_err(unavailable)?;
    if changed != 1 {
        return Err(LedgerError::QuotaExceeded.into());
    }
    Ok(())
}

pub(crate) async fn terminal_in(
    tx: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    status: RunStatus,
) -> Result<(), RepositoryError> {
    if matches!(status, RunStatus::UnknownAfterRestart) {
        return Ok(());
    }
    let mut v = scope_values(&env.scope);
    v.push(env.owner.run_id.0.clone());
    let unsafe_facts:bool=tx.query_one(&format!("SELECT EXISTS(SELECT 1 FROM provider_attempts WHERE {SCOPE} AND run_id=$5 AND status IN ('started','outcome_unknown','outcomeunknown')) OR EXISTS(SELECT 1 FROM tool_executions WHERE {SCOPE} AND run_id=$5 AND (status='running' OR outcome='outcome_unknown')) OR EXISTS(SELECT 1 FROM resources WHERE {SCOPE} AND run_id=$5 AND state IN ('starting','running','stopping','orphaned')) OR EXISTS(SELECT 1 FROM usage_charges WHERE {SCOPE} AND run_id=$5 AND (outcome_unknown OR cost_micros IS NULL))"),&parameters(&v)).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
    if unsafe_facts {
        return Ok(());
    }
    let snapshot = crate::facts::frozen_in(tx, &env.scope, &env.owner.run_id).await?;
    let Some(frozen) = snapshot.and_then(|s| s.billing_policy) else {
        return Ok(());
    };
    lock_in(tx, &env.scope).await?;
    let rows=tx.query(&format!("SELECT budget_key,window_start_ms,reserved_micros FROM billing_run_reservations WHERE {SCOPE} AND run_id=$5 AND state='reserved' ORDER BY budget_key FOR UPDATE"),&parameters(&v)).await.map_err(unavailable)?;
    for row in rows {
        let key = field(&row, 0)?;
        let start: i64 = row.try_get(1).map_err(unavailable)?;
        let reserved: i64 = row.try_get(2).map_err(unavailable)?;
        let dim = frozen
            .policy
            .budgets
            .iter()
            .find(|b| billing_budget_key(&env.scope, &b.dimension).is_ok_and(|k| k == key))
            .ok_or(CoordinationError::Corrupt)?;
        let model = match &dim.dimension {
            BillingDimension::Model(model) => Some(model.as_str()),
            _ => None,
        };
        let mut binding = v.clone();
        binding.push(model.unwrap_or_default().into());
        let costs=tx.query(&format!("SELECT cost_micros FROM usage_charges WHERE {SCOPE} AND run_id=$5 AND ($6='' OR model=$6) LIMIT 2049"),&parameters(&binding)).await.map_err(unavailable)?;
        if costs.len() > 2048 {
            return Err(CoordinationError::Corrupt.into());
        }
        let mut actual = 0i64;
        for c in costs {
            let n: i64 = c.try_get(0).map_err(unavailable)?;
            actual = actual.checked_add(n).ok_or(CoordinationError::Corrupt)?;
        }
        let changed=tx.execute("UPDATE billing_accounts SET reserved_micros=reserved_micros-$5,consumed_micros=consumed_micros+$6,revision=revision+1 WHERE organization_id=$1 AND tenant_id=$2 AND budget_key=$3 AND window_start_ms=$4 AND reserved_micros>=$5 AND consumed_micros<=9223372036854775807-$6",&[&env.scope.organization_id.as_str(),&env.scope.tenant_id.as_str(),&key,&start,&reserved,&actual]).await.map_err(unavailable)?;
        if changed != 1 {
            return Err(CoordinationError::Corrupt.into());
        }
        let mut binding = v.clone();
        binding.extend([key.clone(), actual.to_string()]);
        tx.execute(&format!("UPDATE billing_run_reservations SET state='settled',charged_micros=$7::text::bigint WHERE {SCOPE} AND run_id=$5 AND budget_key=$6 AND state='reserved'"),&parameters(&binding)).await.map_err(unavailable)?;
        let account=tx.query_one("SELECT hard_micros,soft_micros,consumed_micros,reserved_micros FROM billing_accounts WHERE organization_id=$1 AND tenant_id=$2 AND budget_key=$3 AND window_start_ms=$4",&[&env.scope.organization_id.as_str(),&env.scope.tenant_id.as_str(),&key,&start]).await.map_err(unavailable)?;
        let hard: i64 = account.try_get(0).map_err(unavailable)?;
        let soft: i64 = account.try_get(1).map_err(unavailable)?;
        let consumed: i64 = account.try_get(2).map_err(unavailable)?;
        let reserved_now: i64 = account.try_get(3).map_err(unavailable)?;
        if actual > reserved || consumed > hard || consumed.saturating_add(reserved_now) >= soft {
            let auth = crate::identities::saved_authorization_in(tx, env)
                .await?
                .ok_or(CoordinationError::Corrupt)?;
            crate::ledgers::append_audit_in(tx,&AuditCommand{scope:env.scope.clone(),operation_id:format!("billing.alert:{:x}",Sha256::digest(serde_json::json!([env.owner.run_id,key,start]).to_string().as_bytes())),subject_id:auth.context.principal.subject_id,action:"billing.budget_alert".into(),resource:serde_json::json!({"run_id":env.owner.run_id,"budget_key":key,"actual_micros":actual,"consumed_micros":consumed,"reserved_micros":reserved_now,"hard_micros":hard,"soft_micros":soft,"currency":frozen.policy.currency}),outcome:"warning".into()},crate::now(tx).await?).await?;
        }
    }
    Ok(())
}

pub(crate) async fn allocate_sandbox_in(
    tx: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    operation: &str,
) -> Result<(), RepositoryError> {
    let snapshot = crate::facts::frozen_in(tx, &env.scope, &env.owner.run_id).await?;
    let Some(f) = snapshot.and_then(|s| s.billing_policy) else {
        return Ok(());
    };
    let l = &f.policy.limits;
    if l.max_sandbox_wall_ms_per_resource == 0 {
        return Err(LedgerError::QuotaExceeded.into());
    }
    let mut v = scope_values(&env.scope);
    v.extend([
        env.owner.run_id.0.clone(),
        l.max_sandbox_wall_ms_per_resource.to_string(),
        l.max_sandbox_wall_ms.to_string(),
    ]);
    let changed=tx.execute(&format!("UPDATE billing_run_limits SET sandbox_allocated_ms=sandbox_allocated_ms+$6::text::bigint WHERE {SCOPE} AND run_id=$5 AND sandbox_allocated_ms<=$7::text::bigint-$6::text::bigint"),&parameters(&v)).await.map_err(unavailable)?;
    if changed != 1 {
        return Err(LedgerError::QuotaExceeded.into());
    }
    v.truncate(5);
    v.extend([
        operation.into(),
        l.max_sandbox_wall_ms_per_resource.to_string(),
        crate::now(tx).await?.to_string(),
    ]);
    tx.execute("INSERT INTO billing_sandbox_allocations VALUES($1,$2,$3,$4,$5,$6,$7::text::bigint,$8::text::bigint)",&parameters(&v)).await.map_err(unavailable)?;
    Ok(())
}
pub(crate) async fn start_tool_in(
    tx: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    round: usize,
    id: &str,
) -> Result<(), RepositoryError> {
    let mut v = scope_values(&env.scope);
    v.extend([env.owner.run_id.0.clone(), round.to_string(), id.into()]);
    let row=tx.query_one(&format!("SELECT name FROM tool_executions WHERE {SCOPE} AND run_id=$5 AND round=$6::text::bigint AND call_id=$7"),&parameters(&v)).await.map_err(unavailable)?;
    let name = field(&row, 0)?;
    let op = format!("tool:{round}:{id}");
    if name == "exec" {
        allocate_sandbox_in(tx, env, &op).await?
    }
    let snapshot = crate::facts::frozen_in(tx, &env.scope, &env.owner.run_id).await?;
    let f = snapshot.as_ref().and_then(|s| s.billing_policy.as_ref());
    append_in(
        tx,
        env,
        &op,
        None,
        "tool",
        &serde_json::json!({"calls":1,"name":name,"round":round,"call_id":id}),
        f,
        f.map(|f| f.policy.tool_call_micros),
        false,
    )
    .await
}
pub(crate) async fn finish_sandbox_in(
    tx: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    operation: &str,
    unknown: bool,
) -> Result<(), RepositoryError> {
    let mut v = scope_values(&env.scope);
    v.extend([env.owner.run_id.0.clone(), operation.into()]);
    let row=tx.query_opt(&format!("SELECT started_at_ms FROM billing_sandbox_allocations WHERE {SCOPE} AND run_id=$5 AND operation_id=$6"),&parameters(&v)).await.map_err(unavailable)?;
    let Some(row) = row else { return Ok(()) };
    let start: i64 = row.try_get(0).map_err(unavailable)?;
    let charge = format!("sandbox:{operation}");
    v[5] = agent_storage_ports::billing_charge_operation(&env.owner.run_id, &charge);
    let exists:bool=tx.query_one(&format!("SELECT EXISTS(SELECT 1 FROM usage_charges WHERE {SCOPE} AND run_id=$5 AND operation_id=$6)"),&parameters(&v)).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
    if exists {
        return Ok(());
    }
    let wall = u64::try_from(crate::now(tx).await?.saturating_sub(start))
        .map_err(|_| CoordinationError::Corrupt)?;
    let snapshot = crate::facts::frozen_in(tx, &env.scope, &env.owner.run_id).await?;
    let f = snapshot.as_ref().and_then(|s| s.billing_policy.as_ref());
    let cost = if unknown {
        None
    } else {
        f.map(|f| micros(wall, f.policy.sandbox_per_million_ms))
            .transpose()?
    };
    append_in(
        tx,
        env,
        &charge,
        None,
        "sandbox",
        &serde_json::json!({"wall_ms":wall,"allocation":operation}),
        f,
        cost,
        unknown,
    )
    .await
}
