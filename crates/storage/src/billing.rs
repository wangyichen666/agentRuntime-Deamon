//! 费用账本复用原运行事务；unknown 保留财务预占，不改变运行事实。
use crate::{RunStore, RuntimeError, enterprise, now_ms};
use agent_core::*;
use agent_storage_ports::{BillingRepository, billing_budget_key, freeze_billing};
use rusqlite::{Connection, OptionalExtension, params};
use std::sync::Arc;
const SCOPE: &str = "organization_id=?1 AND tenant_id=?2 AND project_id=?3 AND agent_uid=?4";
fn values(s: &TenantContext) -> [&str; 4] {
    [
        s.organization_id.as_str(),
        s.tenant_id.as_str(),
        s.project_id.as_str(),
        s.agent_uid.as_str(),
    ]
}
fn decode(raw: &str, digest: &str) -> Result<FrozenBillingPolicy, RuntimeError> {
    if raw.len() > 65536 {
        return Err(CoordinationError::Corrupt.into());
    }
    let policy: BillingPolicy =
        serde_json::from_str(raw).map_err(|_| CoordinationError::Corrupt)?;
    let frozen = freeze_billing(&policy)?;
    if frozen.digest != digest {
        return Err(CoordinationError::Corrupt.into());
    }
    Ok(frozen)
}
pub(crate) fn current_in(
    db: &Connection,
    s: &TenantContext,
) -> Result<Option<FrozenBillingPolicy>, RuntimeError> {
    let v = values(s);
    db.query_row(&format!("SELECT CASE WHEN length(CAST(policy_json AS BLOB))<=65536 THEN policy_json END,p.digest FROM billing_policies p JOIN billing_current c USING(organization_id,tenant_id,project_id,agent_uid,digest) WHERE {SCOPE}"),params![v[0],v[1],v[2],v[3]],|r|Ok((r.get::<_,Option<String>>(0)?,r.get::<_,String>(1)?))).optional()?.map(|(raw,digest)|decode(&raw.ok_or(CoordinationError::Corrupt)?,&digest)).transpose()
}
#[async_trait::async_trait]
impl BillingRepository for RunStore {
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
        let (request, frozen, operation) = (
            request.clone(),
            freeze_billing(policy)?,
            operation.to_owned(),
        );
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(),move|store| {
            let execute=||->Result<FrozenBillingPolicy,RuntimeError> {
                let mut db=store.lock_connection()?;let tx=db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let s=&request.context.scope;let v=values(s);let actor=&request.context.principal.subject_id;
                let identity=enterprise::identity_in(&tx,s,actor)?;let mut checked=request.clone();checked.attributes.timestamp_ms=now_ms();verifier.verify(&checked,&identity)?;
                let prior=tx.query_row(&format!("SELECT policy_json,digest FROM billing_policies WHERE {SCOPE} AND actor_id=?5 AND operation_id=?6"),params![v[0],v[1],v[2],v[3],actor.as_str(),operation],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?))).optional()?;
                if let Some((raw,digest))=prior {let old=decode(&raw,&digest)?;if old!=frozen{return Err(CoordinationError::Conflict.into())}return Ok(old)}
                let p=&frozen.policy;let at=now_ms();if at>=p.window_end_ms {return Err(CoordinationError::InvalidRequest.into())}
                for b in &p.budgets {
                    let key=billing_budget_key(s,&b.dimension)?;
                    let prior=tx.query_row("SELECT window_end_ms,currency,reserved_micros,consumed_micros FROM billing_accounts WHERE organization_id=?1 AND tenant_id=?2 AND budget_key=?3 AND window_start_ms=?4",params![v[0],v[1],key,p.window_start_ms],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?,r.get::<_,u64>(2)?,r.get::<_,u64>(3)?))).optional()?;
                    if let Some((end,currency,reserved,consumed))=prior {if end!=p.window_end_ms || currency!=p.currency || reserved.checked_add(consumed).is_none_or(|n|n>b.hard_micros){return Err(CoordinationError::Conflict.into())}}
                    tx.execute("INSERT INTO billing_accounts VALUES(?1,?2,?3,?4,?5,?6,?7,?8,0,0,0) ON CONFLICT(organization_id,tenant_id,budget_key,window_start_ms) DO UPDATE SET hard_micros=excluded.hard_micros,soft_micros=excluded.soft_micros,revision=billing_accounts.revision+1",params![v[0],v[1],key,p.window_start_ms,p.window_end_ms,p.currency,b.hard_micros,b.soft_micros])?;
                }
                let raw=serde_json::to_string(p).map_err(|_|CoordinationError::Corrupt)?;
                tx.execute("INSERT INTO billing_policies VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",params![v[0],v[1],v[2],v[3],frozen.digest,p.version,raw,actor.as_str(),operation,at])?;
                tx.execute("INSERT INTO billing_current VALUES(?1,?2,?3,?4,?5) ON CONFLICT(organization_id,tenant_id,project_id,agent_uid) DO UPDATE SET digest=excluded.digest",params![v[0],v[1],v[2],v[3],frozen.digest])?;
                crate::ledgers::append_audit_in(&tx,&AuditCommand {scope:s.clone(),operation_id:format!("billing.configure:{}",enterprise::hash(format!("{}:{operation}",actor.as_str()).as_bytes())),subject_id:actor.clone(),action:"billing.configure".into(),resource:serde_json::json!({"digest":frozen.digest,"version":p.version}),outcome:"committed".into()},at)?;
                tx.commit()?;Ok(frozen)
            };execute().map_err(RuntimeError::repository_error)
        }).await
    }
    async fn read_billing_policy(
        &self,
        scope: &TenantContext,
    ) -> Result<Option<FrozenBillingPolicy>, RepositoryError> {
        let s = scope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            let db = store
                .lock_connection()
                .map_err(RuntimeError::repository_error)?;
            current_in(&db, &s).map_err(RuntimeError::repository_error)
        })
        .await
    }
    async fn read_billing_accounts(
        &self,
        scope: &TenantContext,
    ) -> Result<Vec<BillingAccount>, RepositoryError> {
        let s = scope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(),move|store| {
            let execute=||->Result<Vec<BillingAccount>,RuntimeError> {
                let mut db=store.lock_connection()?;let tx=db.transaction()?;
                let Some(f)=current_in(&tx,&s)? else{return Ok(Vec::new())};let mut accounts=Vec::new();
                for b in f.policy.budgets {let key=billing_budget_key(&s,&b.dimension)?;
                    accounts.push(tx.query_row("SELECT currency,window_start_ms,window_end_ms,hard_micros,soft_micros,reserved_micros,consumed_micros,revision FROM billing_accounts WHERE organization_id=?1 AND tenant_id=?2 AND budget_key=?3 AND window_start_ms=?4",params![s.organization_id.as_str(),s.tenant_id.as_str(),key,f.policy.window_start_ms],|r|Ok(BillingAccount{dimension:b.dimension.clone(),currency:r.get(0)?,window_start_ms:r.get(1)?,window_end_ms:r.get(2)?,hard_micros:r.get(3)?,soft_micros:r.get(4)?,reserved_micros:r.get(5)?,consumed_micros:r.get(6)?,revision:r.get(7)?}))?);
                }tx.commit()?;Ok(accounts)
            };execute().map_err(RuntimeError::repository_error)
        }).await
    }
    async fn read_billing_usage(
        &self,
        scope: &TenantContext,
        after: u64,
        limit: u32,
    ) -> Result<UsagePage, RepositoryError> {
        if after > i64::MAX as u64 || !(1..=64).contains(&limit) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let s = scope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(),move|store| {
            let execute=||->Result<UsagePage,RuntimeError> {
                let db=store.lock_connection()?;let v=values(&s);
                let mut stmt=db.prepare(&format!("SELECT seq,owner_json,operation_id,model,kind,units_json,policy_digest,currency,cost_micros,outcome_unknown,created_at_ms FROM usage_charges WHERE {SCOPE} AND seq>?5 ORDER BY seq LIMIT ?6"))?;
                let mut rows=stmt.query(params![v[0],v[1],v[2],v[3],after,limit+1])?;let mut entries=Vec::new();let mut bytes=0;
                while let Some(r)=rows.next()? {
                    let owner:String=r.get(1)?;let units:String=r.get(5)?;bytes+=owner.len()+units.len();if owner.len()>4096 || units.len()>16384 || bytes>1024*1024{return Err(CoordinationError::Corrupt.into())}
                    entries.push(UsageCharge{seq:r.get(0)?,scope:s.clone(),owner:serde_json::from_str(&owner).map_err(|_|CoordinationError::Corrupt)?,operation_id:r.get(2)?,model:r.get(3)?,kind:r.get(4)?,units:serde_json::from_str(&units).map_err(|_|CoordinationError::Corrupt)?,policy_digest:r.get(6)?,currency:r.get(7)?,cost_micros:r.get(8)?,outcome_unknown:r.get(9)?,created_at_ms:r.get(10)?});
                }
                let has_more=entries.len()>limit as usize;entries.truncate(limit as usize);let next_seq=entries.last().map_or(after,|r|r.seq);Ok(UsagePage{entries,next_seq,has_more})
            };execute().map_err(RuntimeError::repository_error)
        }).await
    }
}
pub(crate) fn reserve_in(
    db: &Connection,
    env: &EnterpriseOwnerEnvelope,
    snapshot: Option<&RunSnapshot>,
    enforce_current: bool,
) -> Result<(), RuntimeError> {
    let frozen = snapshot.and_then(|s| s.billing_policy.as_ref());
    if !enforce_current && frozen.is_none() && current_in(db,&env.scope)?.is_some() {return Err(CoordinationError::Conflict.into())}
    if enforce_current && current_in(db, &env.scope)?.as_ref() != frozen {
        return Err(CoordinationError::Conflict.into());
    }
    let Some(frozen) = frozen else { return Ok(()) };
    if freeze_billing(&frozen.policy)? != *frozen {
        return Err(CoordinationError::Corrupt.into());
    }
    let p = &frozen.policy;
    let at = now_ms();
    let amount = p.maximum_cost()?;
    if at < p.window_start_ms
        || at >= p.window_end_ms
        || snapshot.and_then(|s| s.route.as_ref()).is_none_or(|r| {
            r.candidates
                .iter()
                .any(|c| !p.models.contains_key(&c.model))
        })
    {
        return Err(CoordinationError::Forbidden.into());
    }
    let v = values(&env.scope);
    db.execute(
        "INSERT INTO billing_run_limits VALUES(?1,0,0,0)",
        [&env.owner.run_id.0],
    )?;
    for b in &p.budgets {
        let key = billing_budget_key(&env.scope, &b.dimension)?;
        let changed=db.execute("UPDATE billing_accounts SET reserved_micros=reserved_micros+?5,revision=revision+1 WHERE organization_id=?1 AND tenant_id=?2 AND budget_key=?3 AND window_start_ms=?4 AND window_end_ms=?6 AND currency=?7 AND hard_micros-consumed_micros-reserved_micros>=?5",params![v[0],v[1],key,p.window_start_ms,amount,p.window_end_ms,p.currency])?;
        if changed != 1 {
            return Err(LedgerError::QuotaExceeded.into());
        }
        db.execute("INSERT INTO billing_run_reservations VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,'reserved',NULL)",params![v[0],v[1],v[2],v[3],env.owner.run_id.0,key,p.window_start_ms,amount,frozen.digest])?;
    }
    Ok(())
}
pub(crate) fn provider_in(
    db: &Connection,
    env: &EnterpriseOwnerEnvelope,
    attempt: &ProviderAttempt,
) -> Result<(), RuntimeError> {
    let snapshot = crate::turns::run_snapshot_in(db, &env.owner.run_id)?;
    let frozen = snapshot.as_ref().and_then(|s| s.billing_policy.as_ref());
    let cost = match (frozen, attempt.usage.as_ref()) {
        (Some(f), Some(u)) => f
            .policy
            .provider_cost(&attempt.model, attempt.api_type, u)
            .unwrap_or(None),
        _ => None,
    };
    let unknown = attempt.status == AttemptStatus::OutcomeUnknown || cost.is_none();
    append_in(
        db,
        env,
        &format!("provider:{}", attempt.attempt_id),
        Some(&attempt.model),
        "provider",
        &serde_json::json!({"usage":attempt.usage,"api_type":attempt.api_type,"status":attempt.status,"pricing_rule":"missing_cache_fields_assumed_zero;missing_or_invalid_required_usage_unknown"}),
        frozen,
        cost,
        unknown,
    )
}
#[allow(clippy::too_many_arguments)]
pub(crate) fn append_in(
    db: &Connection,
    env: &EnterpriseOwnerEnvelope,
    operation: &str,
    model: Option<&str>,
    kind: &str,
    units: &serde_json::Value,
    frozen: Option<&FrozenBillingPolicy>,
    cost: Option<u64>,
    unknown: bool,
) -> Result<(), RuntimeError> {
    let operation = agent_storage_ports::billing_charge_operation(&env.owner.run_id, operation);
    let v = values(&env.scope);
    let owner = serde_json::to_string(&env.owner).map_err(|_| CoordinationError::Corrupt)?;
    let units = serde_json::to_string(units).map_err(|_| CoordinationError::Corrupt)?;
    if owner.len() > 4096 || units.len() > 16384 {
        return Err(CoordinationError::InvalidRequest.into());
    }
    db.execute("INSERT INTO usage_charges(organization_id,tenant_id,project_id,agent_uid,run_id,operation_id,model,kind,owner_json,units_json,policy_digest,currency,cost_micros,outcome_unknown,created_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",params![v[0],v[1],v[2],v[3],env.owner.run_id.0,operation,model,kind,owner,units,frozen.map(|f|f.digest.as_str()),frozen.map(|f|f.policy.currency.as_str()),cost,unknown,now_ms()])?;
    Ok(())
}

pub(crate) fn start_provider_in(
    db: &Connection,
    run: &RunId,
    attempt: &ProviderAttempt,
) -> Result<(), RuntimeError> {
    let snapshot = crate::turns::run_snapshot_in(db, run)?;
    let Some(f) = snapshot.and_then(|s| s.billing_policy) else {
        return Ok(());
    };
    let l = &f.policy.limits;
    if attempt.round > l.max_rounds || !f.policy.models.contains_key(&attempt.model) {
        return Err(LedgerError::QuotaExceeded.into());
    }
    let raw:Option<String>=db.query_row("SELECT CASE WHEN length(CAST(capture_json AS BLOB))<=2097152 THEN capture_json END FROM provider_requests WHERE run_id=?1 AND round=?2 AND candidate_index=?3 ORDER BY rowid DESC LIMIT 1",params![run.0,attempt.round,attempt.candidate_index],|r|r.get(0)).optional()?.flatten();
    let capture: ProviderRequestCapture =
        serde_json::from_str(&raw.ok_or(CoordinationError::Conflict)?)
            .map_err(|_| CoordinationError::Corrupt)?;
    let upper = agent_storage_ports::billing_input_upper_bound(&capture)?;
    let changed=db.execute("UPDATE billing_run_limits SET provider_attempts=provider_attempts+1,input_upper_bound=input_upper_bound+?2 WHERE run_id=?1 AND provider_attempts<?3 AND input_upper_bound<=?4-?2",params![run.0,upper,l.max_provider_attempts,l.max_input_tokens])?;
    if changed != 1 {
        return Err(LedgerError::QuotaExceeded.into());
    }
    Ok(())
}

pub(crate) fn terminal_in(
    db: &Connection,
    env: &EnterpriseOwnerEnvelope,
    status: RunStatus,
) -> Result<(), RuntimeError> {
    if matches!(status, RunStatus::UnknownAfterRestart) {
        return Ok(());
    }
    let unsafe_facts:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM provider_attempts WHERE run_id=?1 AND status IN ('started','outcome_unknown','outcomeunknown')) OR EXISTS(SELECT 1 FROM tool_executions WHERE run_id=?1 AND (status='running' OR outcome='outcome_unknown')) OR EXISTS(SELECT 1 FROM resources WHERE run_id=?1 AND state IN ('starting','running','stopping','orphaned')) OR EXISTS(SELECT 1 FROM usage_charges WHERE run_id=?1 AND (outcome_unknown=1 OR cost_micros IS NULL))",[&env.owner.run_id.0],|r|r.get(0))?;
    if unsafe_facts {
        return Ok(());
    }
    let snapshot = crate::turns::run_snapshot_in(db, &env.owner.run_id)?;
    let Some(frozen) = snapshot.and_then(|s| s.billing_policy) else {
        return Ok(());
    };
    let v = values(&env.scope);
    let mut stmt=db.prepare(&format!("SELECT budget_key,window_start_ms,reserved_micros FROM billing_run_reservations WHERE {SCOPE} AND run_id=?5 AND state='reserved' ORDER BY budget_key"))?;
    let reservations = stmt
        .query_map(params![v[0], v[1], v[2], v[3], env.owner.run_id.0], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, u64>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    for (key, start, reserved) in reservations {
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
        let mut stmt=db.prepare("SELECT cost_micros FROM usage_charges WHERE run_id=?1 AND (?2 IS NULL OR model=?2) LIMIT 2049")?;
        let costs = stmt
            .query_map(params![env.owner.run_id.0, model], |r| r.get::<_, u64>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        if costs.len() > 2048 {
            return Err(CoordinationError::Corrupt.into());
        }
        let actual = costs
            .into_iter()
            .try_fold(0u64, |sum, c| sum.checked_add(c))
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or(CoordinationError::Corrupt)?;
        let changed=db.execute("UPDATE billing_accounts SET reserved_micros=reserved_micros-?5,consumed_micros=consumed_micros+?6,revision=revision+1 WHERE organization_id=?1 AND tenant_id=?2 AND budget_key=?3 AND window_start_ms=?4 AND reserved_micros>=?5 AND consumed_micros<=9223372036854775807-?6",params![v[0],v[1],key,start,reserved,actual])?;
        if changed != 1 {
            return Err(CoordinationError::Corrupt.into());
        }
        db.execute(&format!("UPDATE billing_run_reservations SET state='settled',charged_micros=?7 WHERE {SCOPE} AND run_id=?5 AND budget_key=?6 AND state='reserved'"),params![v[0],v[1],v[2],v[3],env.owner.run_id.0,key,actual])?;
        let (hard,soft,consumed,reserved_now):(u64,u64,u64,u64)=db.query_row("SELECT hard_micros,soft_micros,consumed_micros,reserved_micros FROM billing_accounts WHERE organization_id=?1 AND tenant_id=?2 AND budget_key=?3 AND window_start_ms=?4",params![v[0],v[1],key,start],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
        if actual > reserved || consumed > hard || consumed.saturating_add(reserved_now) >= soft {
            let raw: String = db.query_row(
                "SELECT snapshot_json FROM run_authorizations WHERE run_id=?1",
                [&env.owner.run_id.0],
                |r| r.get(0),
            )?;
            let auth: AuthorizationSnapshot =
                serde_json::from_str(&raw).map_err(|_| CoordinationError::Corrupt)?;
            crate::ledgers::append_audit_in(
                db,
                &AuditCommand {
                    scope: env.scope.clone(),
                    operation_id: format!(
                        "billing.alert:{}",
                        enterprise::hash(
                            serde_json::json!([env.owner.run_id, key, start])
                                .to_string()
                                .as_bytes()
                        )
                    ),
                    subject_id: auth.context.principal.subject_id,
                    action: "billing.budget_alert".into(),
                    resource: serde_json::json!({"run_id":env.owner.run_id,"budget_key":key,"actual_micros":actual,"consumed_micros":consumed,"reserved_micros":reserved_now,"hard_micros":hard,"soft_micros":soft,"currency":frozen.policy.currency}),
                    outcome: "warning".into(),
                },
                now_ms(),
            )?;
        }
    }
    Ok(())
}

pub(crate) fn envelope_in(
    db: &Connection,
    run: &RunId,
) -> Result<EnterpriseOwnerEnvelope, RuntimeError> {
    let scope: Option<String> = db
        .query_row(
            "SELECT snapshot_json FROM run_authorizations WHERE run_id=?1",
            [&run.0],
            |r| r.get(0),
        )
        .optional()?;
    let scope = scope
        .map(|s| {
            serde_json::from_str::<AuthorizationSnapshot>(&s)
                .map(|a| a.envelope.scope)
                .map_err(|_| CoordinationError::Corrupt)
        })
        .transpose()?
        .unwrap_or_else(TenantContext::local);
    Ok(EnterpriseOwnerEnvelope {
        scope,
        owner: crate::sessions::owner_in(db, run)?,
    })
}
pub(crate) fn allocate_sandbox_in(
    db: &Connection,
    env: &EnterpriseOwnerEnvelope,
    operation: &str,
) -> Result<(), RuntimeError> {
    let snapshot = crate::turns::run_snapshot_in(db, &env.owner.run_id)?;
    let Some(f) = snapshot.and_then(|s| s.billing_policy) else {
        return Ok(());
    };
    let l = &f.policy.limits;
    if l.max_sandbox_wall_ms_per_resource == 0 {
        return Err(LedgerError::QuotaExceeded.into());
    }
    let changed=db.execute("UPDATE billing_run_limits SET sandbox_allocated_ms=sandbox_allocated_ms+?2 WHERE run_id=?1 AND sandbox_allocated_ms<=?3-?2",params![env.owner.run_id.0,l.max_sandbox_wall_ms_per_resource,l.max_sandbox_wall_ms])?;
    if changed != 1 {
        return Err(LedgerError::QuotaExceeded.into());
    }
    db.execute(
        "INSERT INTO billing_sandbox_allocations VALUES(?1,?2,?3,?4)",
        params![
            env.owner.run_id.0,
            operation,
            l.max_sandbox_wall_ms_per_resource,
            now_ms()
        ],
    )?;
    Ok(())
}
pub(crate) fn start_tool_in(
    db: &Connection,
    env: &EnterpriseOwnerEnvelope,
    round: usize,
    id: &str,
) -> Result<(), RuntimeError> {
    let name: String = db.query_row(
        "SELECT name FROM tool_executions WHERE run_id=?1 AND round=?2 AND call_id=?3",
        params![env.owner.run_id.0, round as i64, id],
        |r| r.get(0),
    )?;
    let op = format!("tool:{round}:{id}");
    if name == "exec" {
        allocate_sandbox_in(db, env, &op)?
    }
    let snapshot = crate::turns::run_snapshot_in(db, &env.owner.run_id)?;
    let f = snapshot.as_ref().and_then(|s| s.billing_policy.as_ref());
    append_in(
        db,
        env,
        &op,
        None,
        "tool",
        &serde_json::json!({"calls":1,"name":name,"round":round,"call_id":id}),
        f,
        f.map(|f| f.policy.tool_call_micros),
        false,
    )
}
pub(crate) fn finish_sandbox_in(
    db: &Connection,
    env: &EnterpriseOwnerEnvelope,
    operation: &str,
    unknown: bool,
) -> Result<(), RuntimeError> {
    let row=db.query_row("SELECT started_at_ms FROM billing_sandbox_allocations WHERE run_id=?1 AND operation_id=?2",params![env.owner.run_id.0,operation],|r|r.get::<_,i64>(0)).optional()?;
    let Some(start) = row else { return Ok(()) };
    let charge = format!("sandbox:{operation}");
    let exists: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM usage_charges WHERE run_id=?1 AND operation_id=?2)",
        params![
            env.owner.run_id.0,
            agent_storage_ports::billing_charge_operation(&env.owner.run_id, &charge)
        ],
        |r| r.get(0),
    )?;
    if exists {
        return Ok(());
    }
    let wall =
        u64::try_from(now_ms().saturating_sub(start)).map_err(|_| CoordinationError::Corrupt)?;
    let snapshot = crate::turns::run_snapshot_in(db, &env.owner.run_id)?;
    let f = snapshot.as_ref().and_then(|s| s.billing_policy.as_ref());
    let cost = if unknown {
        None
    } else {
        f.map(|f| micros(wall, f.policy.sandbox_per_million_ms))
            .transpose()?
    };
    append_in(
        db,
        env,
        &charge,
        None,
        "sandbox",
        &serde_json::json!({"wall_ms":wall,"allocation":operation}),
        f,
        cost,
        unknown,
    )
}
