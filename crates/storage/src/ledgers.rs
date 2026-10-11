//! SQLite审计与配额复用canonical连接；预占、运行和终态一起提交。
use crate::{RunStore, RuntimeError, enterprise, execution, now_ms};
use agent_core::*;
use agent_storage_ports::{
    AuditReadRepository, QuotaRepository, decode_audit_entry, seal_audit_entry,
};
use rusqlite::{Connection, OptionalExtension, params};
const SCOPE: &str = "organization_id=?1 AND tenant_id=?2 AND project_id=?3 AND agent_uid=?4";
const GENESIS: &str = "0000000000000000000000000000000000000000000000000000000000000000";
fn values(s: &TenantContext) -> [&str; 4] {
    [
        s.organization_id.as_str(),
        s.tenant_id.as_str(),
        s.project_id.as_str(),
        s.agent_uid.as_str(),
    ]
}
fn coordination(e: RuntimeError) -> CoordinationError {
    match e.repository_error() {
        RepositoryError::Coordination(e) => e,
        _ => CoordinationError::Forbidden,
    }
}
fn ledger(e: RuntimeError) -> LedgerError {
    match e {
        RuntimeError::Ledger(e) => e,
        e => coordination(e).into(),
    }
}
fn read_entry(db: &Connection, s: &TenantContext, seq: u64) -> Result<AuditEntry, RuntimeError> {
    let v = values(s);
    let row=db.query_row(&format!("SELECT CASE WHEN length(CAST(entry_json AS BLOB))<=65536 THEN entry_json END,entry_digest,operation_id,subject_id,action,outcome,previous_digest,created_at_ms,resource_json FROM audit_ledger WHERE {SCOPE} AND seq=?5"),params![v[0],v[1],v[2],v[3],seq],|r|Ok((r.get::<_,Option<String>>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?,r.get::<_,String>(4)?,r.get::<_,String>(5)?,r.get::<_,String>(6)?,r.get::<_,i64>(7)?,r.get::<_,String>(8)?))).optional()?.ok_or(CoordinationError::Corrupt)?;
    let entry = decode_audit_entry(row.0.as_deref().ok_or(CoordinationError::Corrupt)?, &row.1)?;
    if entry.seq != seq
        || entry.command.scope != *s
        || entry.command.operation_id != row.2
        || entry.command.subject_id.as_str() != row.3
        || entry.command.action != row.4
        || entry.command.outcome != row.5
        || entry.previous_digest != row.6
        || entry.created_at_ms != row.7
        || serde_json::from_str::<serde_json::Value>(&row.8)
            .map_err(|_| CoordinationError::Corrupt)?
            != entry.command.resource
    {
        return Err(CoordinationError::Corrupt.into());
    }
    Ok(entry)
}
pub(crate) fn append_audit_in(
    db: &Connection,
    cmd: &AuditCommand,
    at: i64,
) -> Result<AuditEntry, RuntimeError> {
    seal_audit_entry(&AuditEntry {
        seq: 1,
        command: cmd.clone(),
        previous_digest: GENESIS.into(),
        digest: String::new(),
        created_at_ms: at,
    })?;
    let v = values(&cmd.scope);
    db.execute(
        "INSERT OR IGNORE INTO audit_heads VALUES(?1,?2,?3,?4,1,0,?5)",
        params![v[0], v[1], v[2], v[3], GENESIS],
    )?;
    if let Some(seq) = db
        .query_row(
            &format!("SELECT seq FROM audit_ledger WHERE {SCOPE} AND operation_id=?5"),
            params![v[0], v[1], v[2], v[3], cmd.operation_id],
            |r| r.get::<_, u64>(0),
        )
        .optional()?
    {
        let old = read_entry(db, &cmd.scope, seq)?;
        if old.command != *cmd {
            return Err(CoordinationError::Conflict.into());
        }
        return Ok(old);
    }
    let (seq, previous) = db.query_row(
        &format!("SELECT seq,head_digest FROM audit_heads WHERE {SCOPE} AND slot=1"),
        v,
        |r| Ok((r.get::<_, u64>(0)?, r.get::<_, String>(1)?)),
    )?;
    let mut entry = AuditEntry {
        seq: seq
            .checked_add(1)
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or(CoordinationError::Corrupt)?,
        command: cmd.clone(),
        previous_digest: previous,
        digest: String::new(),
        created_at_ms: at,
    };
    let (digest, raw) = seal_audit_entry(&entry)?;
    entry.digest = digest;
    db.execute(
        "INSERT INTO audit_ledger VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
        params![
            v[0],
            v[1],
            v[2],
            v[3],
            entry.seq,
            cmd.operation_id,
            cmd.subject_id.as_str(),
            cmd.action,
            serde_json::to_string(&cmd.resource).map_err(|_| CoordinationError::InvalidRequest)?,
            cmd.outcome,
            entry.previous_digest,
            entry.digest,
            raw,
            at
        ],
    )?;
    db.execute(
        &format!("UPDATE audit_heads SET seq=?5,head_digest=?6 WHERE {SCOPE} AND slot=1"),
        params![v[0], v[1], v[2], v[3], entry.seq, entry.digest],
    )?;
    Ok(entry)
}
fn account_in(
    db: &Connection,
    s: &TenantContext,
    metric: &str,
    start: i64,
) -> Result<QuotaAccount, RuntimeError> {
    let v = values(s);
    let q=db.query_row(&format!("SELECT budget,consumed,reserved,revision,window_end_ms FROM quota_accounts WHERE {SCOPE} AND metric=?5 AND window_start_ms=?6"),params![v[0],v[1],v[2],v[3],metric,start],|r|Ok(QuotaAccount{scope:s.clone(),metric:metric.into(),window_start_ms:start,budget:r.get(0)?,consumed:r.get(1)?,reserved:r.get(2)?,revision:r.get(3)?,window_end_ms:r.get(4)?})).optional()?.ok_or(CoordinationError::Forbidden)?;
    if !quota_metric_valid(metric)
        || q.window_start_ms >= q.window_end_ms
        || q.consumed
            .checked_add(q.reserved)
            .is_none_or(|n| n > q.budget)
    {
        return Err(CoordinationError::Corrupt.into());
    }
    Ok(q)
}
fn reservation_in(
    db: &Connection,
    s: &TenantContext,
    operation: &str,
) -> Result<Option<QuotaReservation>, RuntimeError> {
    let v = values(s);
    db.query_row(&format!("SELECT metric,window_start_ms,amount,run_id,state,created_at_ms,updated_at_ms FROM quota_ledger WHERE {SCOPE} AND operation_id=?5"),params![v[0],v[1],v[2],v[3],operation],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?,r.get::<_,u64>(2)?,r.get::<_,String>(3)?,r.get::<_,String>(4)?,r.get::<_,i64>(5)?,r.get::<_,i64>(6)?))).optional()?.map(|r|Ok(QuotaReservation{scope:s.clone(),operation_id:operation.into(),metric:r.0,window_start_ms:r.1,amount:r.2,run_id:RunId(r.3),state:serde_json::from_value(serde_json::Value::String(r.4)).map_err(|_|CoordinationError::Corrupt)?,created_at_ms:r.5,updated_at_ms:r.6})).transpose()
}
fn reserve_in(
    db: &Connection,
    s: &TenantContext,
    run: &RunId,
    operation: &str,
    metric: &str,
    amount: u64,
    optional: bool,
) -> Result<Option<QuotaReservation>, RuntimeError> {
    if !quota_metric_valid(metric) || operation.is_empty() || operation.len() > 1000 || amount == 0
    {
        return Err(CoordinationError::InvalidRequest.into());
    }
    if let Some(old) = reservation_in(db, s, operation)? {
        if old.metric != metric || old.amount != amount || old.run_id != *run {
            return Err(CoordinationError::Conflict.into());
        }
        return Ok(Some(old));
    }
    let v = values(s);
    let at = now_ms();
    let start=db.query_row(&format!("SELECT window_start_ms FROM quota_accounts WHERE {SCOPE} AND metric=?5 AND window_start_ms<=?6 AND window_end_ms>?6 ORDER BY window_start_ms DESC LIMIT 1"),params![v[0],v[1],v[2],v[3],metric,at],|r|r.get::<_,i64>(0)).optional()?;
    let Some(start) = start else {
        let configured: bool = db.query_row(
            &format!("SELECT EXISTS(SELECT 1 FROM quota_accounts WHERE {SCOPE} AND metric=?5)"),
            params![v[0], v[1], v[2], v[3], metric],
            |r| r.get(0),
        )?;
        if optional && !configured {
            return Ok(None);
        }
        return Err(LedgerError::WindowClosed.into());
    };
    let q = account_in(db, s, metric, start)?;
    check_quota_reservation(&q, amount)?;
    db.execute(&format!("UPDATE quota_accounts SET reserved=reserved+?7,revision=revision+1 WHERE {SCOPE} AND metric=?5 AND window_start_ms=?6"),params![v[0],v[1],v[2],v[3],metric,start,amount])?;
    db.execute(
        "INSERT INTO quota_ledger VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'reserved',?9,?10,?10)",
        params![
            v[0], v[1], v[2], v[3], operation, metric, start, amount, run.0, at
        ],
    )?;
    reservation_in(db, s, operation)
}
fn settle_in(
    db: &Connection,
    s: &TenantContext,
    c: &QuotaReservation,
    state: QuotaState,
) -> Result<QuotaReservation, RuntimeError> {
    if state == QuotaState::Reserved || c.scope != *s {
        return Err(CoordinationError::InvalidRequest.into());
    }
    let mut old = reservation_in(db, s, &c.operation_id)?.ok_or(CoordinationError::Forbidden)?;
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
    let q = account_in(db, s, &old.metric, old.window_start_ms)?;
    if q.reserved < old.amount {
        return Err(CoordinationError::Corrupt.into());
    }
    let v = values(s);
    let at = now_ms();
    db.execute(&format!("UPDATE quota_accounts SET reserved=reserved-?7,consumed=consumed+?8,revision=revision+1 WHERE {SCOPE} AND metric=?5 AND window_start_ms=?6"),params![v[0],v[1],v[2],v[3],old.metric,old.window_start_ms,old.amount,if state==QuotaState::Consumed{old.amount}else{0}])?;
    db.execute(&format!("UPDATE quota_ledger SET state=?6,updated_at_ms=?7 WHERE {SCOPE} AND operation_id=?5 AND state='reserved'"),params![v[0],v[1],v[2],v[3],old.operation_id,state.as_str(),at])?;
    old.state = state;
    old.updated_at_ms = at;
    Ok(old)
}
fn frozen_in(db: &Connection, run: &RunId) -> Result<Option<AuthorizationSnapshot>, RuntimeError> {
    db.query_row(
        "SELECT snapshot_json,digest FROM run_authorizations WHERE run_id=?1",
        params![run.0],
        |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
    )
    .optional()?
    .map(|(raw, digest)| {
        if raw.len() > 1048576 || enterprise::hash(raw.as_bytes()) != digest {
            return Err(CoordinationError::Corrupt.into());
        }
        serde_json::from_str(&raw).map_err(|_| CoordinationError::Corrupt.into())
    })
    .transpose()
}
pub(crate) fn admit_in(db: &Connection, run: &RunId) -> Result<(), RuntimeError> {
    if let Some(auth) = frozen_in(db, run)? {
        reserve_in(
            db,
            &auth.envelope.scope,
            run,
            &format!("admission:{}", run.0),
            "runs",
            1,
            auth.envelope.scope == TenantContext::local(),
        )?;
        append_audit_in(
            db,
            &AuditCommand {
                scope: auth.envelope.scope,
                operation_id: format!("run-admit:{}", run.0),
                subject_id: auth.context.principal.subject_id,
                action: "run.admit".into(),
                resource: serde_json::json!({"owner":auth.envelope.owner,"policy_digest":auth.policy_digest}),
                outcome: "queued".into(),
            },
            now_ms(),
        )?;
    }
    Ok(())
}
pub(crate) fn terminal_in(
    db: &Connection,
    run: &RunId,
    status: RunStatus,
) -> Result<(), RuntimeError> {
    if let Some(auth) = frozen_in(db, run)? {
        crate::billing::terminal_in(db,&auth.envelope,status)?;
        if let Some(r) = reservation_in(db, &auth.envelope.scope, &format!("admission:{}", run.0))?
        {
            // 真正执行过或结果未知均消费额度；仅排队中取消可释放。
            let started: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM events WHERE run_id=?1 AND event='run_started')",
                params![run.0],
                |r| r.get(0),
            )?;
            settle_in(
                db,
                &auth.envelope.scope,
                &r,
                if status == RunStatus::Cancelled && !started {
                    QuotaState::Released
                } else {
                    QuotaState::Consumed
                },
            )?;
        }
        append_audit_in(
            db,
            &AuditCommand {
                scope: auth.envelope.scope,
                operation_id: format!("run-terminal:{}", run.0),
                subject_id: auth.context.principal.subject_id,
                action: "run.terminal".into(),
                resource: serde_json::json!({"owner":auth.envelope.owner}),
                outcome: status.as_str().into(),
            },
            now_ms(),
        )?;
    }
    Ok(())
}
impl RunStore {
    pub fn configure_quota_window(
        &self,
        request: &AuthorizationRequest,
        value: &QuotaAccount,
        verifier: &dyn AuthorizationVerifier,
    ) -> Result<(), RuntimeError> {
        if request.action != Action::AgentConfigure
            || request.context.scope != value.scope
            || !quota_metric_valid(&value.metric)
            || value.window_start_ms >= value.window_end_ms
            || value.budget > i64::MAX as u64
            || value.consumed != 0
            || value.reserved != 0
            || value.revision != 0
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let mut connection = self.lock_connection()?;
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let mut checked = request.clone();
        checked.attributes.timestamp_ms = now_ms();
        let identity = enterprise::identity_in(
            &tx,
            &checked.context.scope,
            &checked.context.principal.subject_id,
        )?;
        verifier.verify(&checked, &identity)?;
        let v = values(&value.scope);
        let overlap:bool=tx.query_row(&format!("SELECT EXISTS(SELECT 1 FROM quota_accounts WHERE {SCOPE} AND metric=?5 AND window_start_ms<?7 AND window_end_ms>?6)"),params![v[0],v[1],v[2],v[3],value.metric,value.window_start_ms,value.window_end_ms],|r|r.get(0))?;
        if overlap {
            return Err(CoordinationError::Conflict.into());
        }
        tx.execute(
            "INSERT INTO quota_accounts VALUES(?1,?2,?3,?4,?5,?6,0,0,0,?7,?8)",
            params![
                v[0],
                v[1],
                v[2],
                v[3],
                value.metric,
                value.budget,
                value.window_start_ms,
                value.window_end_ms
            ],
        )?;
        append_audit_in(
            &tx,
            &AuditCommand {
                scope: value.scope.clone(),
                operation_id: format!("quota-window:{}:{}", value.metric, value.window_start_ms),
                subject_id: request.context.principal.subject_id.clone(),
                action: "quota.configure".into(),
                resource: serde_json::json!({"metric":value.metric,"start_ms":value.window_start_ms,"end_ms":value.window_end_ms,"budget":value.budget}),
                outcome: "created".into(),
            },
            now_ms(),
        )?;
        tx.commit()?;
        Ok(())
    }
}
#[async_trait::async_trait]
impl AuditReadRepository for RunStore {
    async fn read_audit_entries(
        &self,
        s: &TenantContext,
        after: u64,
        limit: u32,
    ) -> Result<Vec<AuditEntry>, CoordinationError> {
        if limit == 0 || limit > 64 || after > i64::MAX as u64 {
            return Err(CoordinationError::InvalidRequest);
        }
        let s = s.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable, move |store| {
            let mut connection = store.lock_connection().map_err(coordination)?;
            let tx = connection
                .transaction()
                .map_err(|_| CoordinationError::StorageUnavailable)?;
            let v = values(&s);
            let head = tx
                .query_row(
                    &format!("SELECT seq,head_digest FROM audit_heads WHERE {SCOPE} AND slot=1"),
                    v,
                    |r| Ok((r.get::<_, u64>(0)?, r.get::<_, String>(1)?)),
                )
                .optional()
                .map_err(|_| CoordinationError::StorageUnavailable)?;
            let Some((seq, digest)) = head else {
                if after != 0 {
                    return Err(CoordinationError::InvalidRequest);
                }
                return Ok(Vec::new());
            };
            if after > seq {
                return Err(CoordinationError::InvalidRequest);
            }
            let mut previous = if after == 0 {
                GENESIS.into()
            } else {
                read_entry(&tx, &s, after).map_err(coordination)?.digest
            };
            let mut result = Vec::new();
            let mut bytes = 0;
            for id in after + 1..=seq.min(after.saturating_add(limit.into())) {
                let entry = read_entry(&tx, &s, id).map_err(coordination)?;
                bytes += serde_json::to_vec(&entry)
                    .map_err(|_| CoordinationError::Corrupt)?
                    .len();
                if entry.previous_digest != previous || bytes > 1024 * 1024 {
                    return Err(CoordinationError::Corrupt);
                }
                previous = entry.digest.clone();
                result.push(entry);
            }
            if after + result.len() as u64 == seq && previous != digest {
                return Err(CoordinationError::Corrupt);
            }
            Ok(result)
        })
        .await
    }
}
#[async_trait::async_trait]
impl QuotaRepository for RunStore {
    async fn read_quota_account(
        &self,
        s: &TenantContext,
        metric: &str,
        start: i64,
    ) -> Result<QuotaAccount, LedgerError> {
        let (s, m) = (s.clone(), metric.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            let connection = store.lock_connection().map_err(ledger)?;
            account_in(&connection, &s, &m, start).map_err(ledger)
        })
        .await
    }
    async fn reserve_run_quota(
        &self,
        e: &EnterpriseOwnerEnvelope,
        operation: &str,
        metric: &str,
        amount: u64,
    ) -> Result<QuotaReservation, LedgerError> {
        let (e, o, m) = (e.clone(), operation.to_owned(), metric.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            let mut connection = store.lock_connection().map_err(ledger)?;
            let tx = connection
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(|_| CoordinationError::StorageUnavailable)?;
            execution::check_execution_in(&tx, &e).map_err(ledger)?;
            let r = reserve_in(&tx, &e.scope, &e.owner.run_id, &o, &m, amount, false)
                .map_err(ledger)?
                .ok_or(CoordinationError::Corrupt)?;
            tx.commit()
                .map_err(|_| CoordinationError::StorageUnavailable)?;
            Ok(r)
        })
        .await
    }
    async fn settle_run_quota(
        &self,
        e: &EnterpriseOwnerEnvelope,
        c: &QuotaReservation,
        state: QuotaState,
    ) -> Result<QuotaReservation, LedgerError> {
        if e.scope != c.scope || e.owner.run_id != c.run_id {
            return Err(CoordinationError::Forbidden.into());
        }
        let (e, c) = (e.clone(), c.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            let mut connection = store.lock_connection().map_err(ledger)?;
            let tx = connection
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(|_| CoordinationError::StorageUnavailable)?;
            execution::check_execution_in(&tx, &e).map_err(ledger)?;
            let r = settle_in(&tx, &e.scope, &c, state).map_err(ledger)?;
            tx.commit()
                .map_err(|_| CoordinationError::StorageUnavailable)?;
            Ok(r)
        })
        .await
    }
}

#[async_trait::async_trait]
impl agent_storage_ports::QuotaManagementRepository for RunStore {
    async fn configure_quota_window(
        &self,
        r: &AuthorizationRequest,
        q: &QuotaAccount,
        v: std::sync::Arc<dyn AuthorizationVerifier>,
    ) -> Result<(), RepositoryError> {
        let (r, q) = (r.clone(), q.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            RunStore::configure_quota_window(&store, &r, &q, v.as_ref())
                .map_err(RuntimeError::repository_error)
        })
        .await
    }
}
