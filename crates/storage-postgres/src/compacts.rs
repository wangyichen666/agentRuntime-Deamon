//! 原独立 compact 准入、intent/source CAS、投影与终态回执同事务。
use crate::attempts::attempts_in;
use crate::canonical::{PgCanonicalStore, field, permanent_life, set_scope, unsigned};
use crate::events::{append_in, seal_in};
use crate::executions::record_in;
use crate::facts::{compact_in, frozen_in, stamp_in};
use crate::identities::{authorization_in, exact_owner_in};
use crate::readback::snapshot_in;
use crate::requests::{context_source_in, source_check};
use crate::sessions::{encode, metadata_in};
use crate::{SCOPE, now, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::RunContextRepository;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio_postgres::Transaction;
fn operation(operation: &str) -> Result<(), CoordinationError> {
    if operation.is_empty() || operation.len() > 256 {
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
pub(crate) async fn admission_source_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    request: &CompactRunRequest,
    frozen: &RunSnapshot,
    meta: &SessionMetadata,
) -> Result<(), RepositoryError> {
    operation(&request.operation_id)?;
    encode(request, 16384)?;
    if meta.key != request.session_key
        || meta.lifetime != request.session_lifetime_id
        || frozen.context_read_only
        || frozen.context_policy_fingerprint.is_none()
    {
        return Err(CoordinationError::InvalidRequest.into());
    }
    let source = snapshot_in(db, scope, meta).await?;
    if source.revision != request.expected_revision
        || source.projection_generation != request.expected_projection_generation
    {
        return Err(CoordinationError::Conflict.into());
    }
    if let Some(run) = &request.compatibility_owner_run_id {
        let owner = exact_owner_in(db, scope, run).await?;
        if owner.session_key != meta.key || owner.session_lifetime_id != meta.lifetime {
            return Err(AuthorizationError::Forbidden.into());
        }
        if frozen_in(db, scope, run)
            .await?
            .is_some_and(|snapshot| snapshot.context_read_only)
        {
            return Err(AuthorizationError::Forbidden.into());
        }
    }
    let mut values = scope_values(scope);
    values.push(request.operation_id.clone());
    let exists: bool = db
        .query_one(
            &format!(
                "SELECT EXISTS(SELECT 1 FROM compact_operations WHERE {SCOPE} AND operation=$5)"
            ),
            &parameters(&values),
        )
        .await
        .map_err(unavailable)?
        .try_get(0)
        .map_err(unavailable)?;
    if exists {
        return Err(CoordinationError::Conflict.into());
    }
    Ok(())
}
pub(crate) async fn start_in(
    db: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    request: &CompactRunRequest,
    frozen: &RunSnapshot,
    meta: &SessionMetadata,
) -> Result<(), CoordinationError> {
    let snapshot = snapshot_in(db, &env.scope, meta).await?;
    let route = frozen
        .route
        .as_ref()
        .and_then(|route| route.candidates.first())
        .map_or("default", |candidate| candidate.model.as_str());
    let source = ContextSource {
        lifetime: snapshot.lifetime,
        source_start: TranscriptSeq(0),
        source_end: snapshot.revision,
        prefix_digest: format!(
            "{:x}",
            Sha256::digest(encode(&snapshot.messages, 16 * 1024 * 1024)?.as_bytes())
        ),
        generation: snapshot.projection_generation,
        policy_fingerprint: frozen
            .context_policy_fingerprint
            .clone()
            .ok_or(CoordinationError::InvalidRequest)?,
        pressure_route: route.into(),
        summary_route: route.into(),
    };
    let raw = encode(&source, 16384)?;
    let mut values = scope_values(&env.scope);
    values.extend([
        env.owner.run_id.0.clone(),
        request.operation_id.clone(),
        encode(request, 16384)?,
        raw.clone(),
    ]);
    db.execute(
        "INSERT INTO compact_run_links VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
        &parameters(&values),
    )
    .await
    .map_err(unavailable)?;
    let mut values = scope_values(&env.scope);
    values.extend([
        request.operation_id.clone(),
        env.owner.run_id.0.clone(),
        env.owner.session_lifetime_id.0.clone(),
        raw,
    ]);
    db.execute("INSERT INTO compact_operations(organization_id,tenant_id,project_id,agent_uid,operation,run_id,lifetime,source_json,state) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'pending')",&parameters(&values)).await.map_err(unavailable)?;
    let values = crate::compacts::values(&env.scope, &env.owner.run_id);
    db.execute(
        &format!("UPDATE runs SET status='running' WHERE {SCOPE} AND id=$5"),
        &parameters(&values),
    )
    .await
    .map_err(unavailable)?;
    db.execute(
        &format!("UPDATE turns SET status='running' WHERE {SCOPE} AND run_id=$5"),
        &parameters(&values),
    )
    .await
    .map_err(unavailable)?;
    append_in(db,env,"run_started",&serde_json::json!({"kind":"compact","operation_id":request.operation_id,"source_revision":source.source_end,"projection_generation":source.generation})).await?;
    Ok(())
}
struct StoredIntent {
    source: ContextSource,
    state: String,
    reason: Option<String>,
    generation: Option<ProjectionGeneration>,
}
async fn stored_in(
    db: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    operation: &str,
) -> Result<Option<StoredIntent>, CoordinationError> {
    let mut values = values(&env.scope, &env.owner.run_id);
    values.extend([operation.into(), env.owner.session_lifetime_id.0.clone()]);
    let row=db.query_opt(&format!("SELECT CASE WHEN octet_length(source_json)<=16384 THEN source_json END,CASE WHEN octet_length(state)<=32 THEN state END,reason IS NULL,CASE WHEN octet_length(reason)<=1024 THEN reason END,result_generation FROM compact_operations WHERE {SCOPE} AND run_id=$5 AND operation=$6 AND lifetime=$7 FOR UPDATE"),&parameters(&values)).await.map_err(unavailable)?;
    row.map(|row| {
        let source: ContextSource =
            serde_json::from_str(&field(&row, 0)?).map_err(|_| CoordinationError::Corrupt)?;
        if source.lifetime != env.owner.session_lifetime_id
            || source.source_start != TranscriptSeq(0)
            || source.source_end.0 > i64::MAX as u64
            || source.generation.0 > i64::MAX as u64
        {
            return Err(CoordinationError::Corrupt);
        }
        Ok(StoredIntent {
            source,
            state: field(&row, 1)?,
            reason: if row.try_get::<_, bool>(2).map_err(unavailable)? {
                None
            } else {
                Some(field(&row, 3)?)
            },
            generation: row
                .try_get::<_, Option<i64>>(4)
                .map_err(unavailable)?
                .map(unsigned)
                .transpose()?
                .map(ProjectionGeneration),
        })
    })
    .transpose()
}
async fn check_link(
    db: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    operation: &str,
) -> Result<(), CoordinationError> {
    let row=db.query_opt(&format!("SELECT CASE WHEN octet_length(operation_id)<=256 THEN operation_id END FROM compact_run_links WHERE {SCOPE} AND run_id=$5"),&parameters(&values(&env.scope,&env.owner.run_id))).await.map_err(unavailable)?;
    if let Some(row) = row {
        if field(&row, 0)? != operation {
            return Err(CoordinationError::Conflict);
        }
    }
    Ok(())
}
pub(crate) async fn terminal_in(
    db: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    state: &str,
    reason: &str,
    generation: ProjectionGeneration,
) -> Result<(), RepositoryError> {
    let record = record_in(db, &env.scope, &env.owner.run_id).await?;
    if record.kind != RunKind::Compact {
        return Ok(());
    }
    let (requested, outcome, error) = if state == "committed" {
        (RunStatus::Completed, CompactRunOutcome::Committed, None)
    } else {
        match reason {
            "restart" => (
                RunStatus::UnknownAfterRestart,
                CompactRunOutcome::Unknown,
                Some((-32002, "compact 执行结果未知，禁止自动重放")),
            ),
            "no_gain" => (RunStatus::Completed, CompactRunOutcome::NoGain, None),
            "cancelled" => (
                RunStatus::Cancelled,
                CompactRunOutcome::Cancelled,
                Some((-32800, "compact 已取消")),
            ),
            "rejected" => (
                RunStatus::Failed,
                CompactRunOutcome::Rejected,
                Some((-32603, "compact 已拒绝")),
            ),
            _ => (
                RunStatus::Failed,
                CompactRunOutcome::Failed,
                Some((-32603, "compact 摘要或 source 失败")),
            ),
        }
    };
    let attempts = attempts_in(db, &env.scope, &env.owner.run_id).await?;
    let unknown = attempts.iter().any(|attempt| {
        matches!(
            attempt.status,
            AttemptStatus::Started | AttemptStatus::OutcomeUnknown
        )
    });
    let status = normalize_run_terminal_status(requested, false, unknown, false);
    let outcome = if status == RunStatus::UnknownAfterRestart {
        CompactRunOutcome::Unknown
    } else {
        outcome
    };
    let mut usage = ProviderUsage::default();
    let at = now(db).await?;
    for mut attempt in attempts {
        if let Some(value) = &attempt.usage {
            usage.add_attempt(value);
        }
        if attempt.status == AttemptStatus::Started {
            attempt.status = AttemptStatus::OutcomeUnknown;
            attempt.finished_at_ms = Some(at);
            let mut values = values(&env.scope, &env.owner.run_id);
            values.extend([attempt.attempt_id.clone(), encode(&attempt, 1048576)?]);
            db.execute(&format!("UPDATE provider_attempts SET status='outcome_unknown',data_json=$7 WHERE {SCOPE} AND run_id=$5 AND attempt_id=$6 AND status='started'"),&parameters(&values)).await.map_err(unavailable)?;
            append_in(db,env,"provider_attempt_unknown",&serde_json::json!({"attempt_id":attempt.attempt_id,"status":attempt.status,"recovery_action":"reconcile_original_run"})).await?;
        }
    }
    let mut values = values(&env.scope, &env.owner.run_id);
    values.extend([
        encode(&env.owner, 16384)?,
        encode(&usage, 4096)?,
        status.as_str().into(),
    ]);
    db.execute("INSERT INTO turn_commits(organization_id,tenant_id,project_id,agent_uid,run_id,owner_json,usage_json,status) VALUES($1,$2,$3,$4,$5,$6,$7,$8)",&parameters(&values)).await.map_err(unavailable)?;
    append_in(
        db,
        env,
        "compact_terminal",
        &serde_json::json!({"outcome":outcome,"projection_generation":if outcome==CompactRunOutcome::Unknown{None}else{Some(generation)}}),
    )
    .await?;
    append_in(db,env,"terminal",&serde_json::json!({"status":status,"error_code":error.map(|error|error.0),"error_message":error.map(|error|error.1)})).await?;
    let mut values = values[..5].to_vec();
    values.extend([status.as_str().into(), at.to_string()]);
    let error_code = error.map(|error| error.0);
    let error_message = error.map(|error| error.1);
    let mut params = parameters(&values);
    params.extend([
        &error_code as &(dyn tokio_postgres::types::ToSql + Sync),
        &error_message,
    ]);
    db.execute(&format!("UPDATE runs SET status=$6,updated_at_ms=$7::text::bigint,error_code=$8,error_message=$9 WHERE {SCOPE} AND id=$5"),&params).await.map_err(unavailable)?;
    db.execute(
        &format!("UPDATE turns SET status=$6 WHERE {SCOPE} AND run_id=$5"),
        &parameters(&values[..6]),
    )
    .await
    .map_err(unavailable)?;
    db.execute(
        &format!("UPDATE queued_messages SET status=$6 WHERE {SCOPE} AND run_id=$5"),
        &parameters(&values[..6]),
    )
    .await
    .map_err(unavailable)?;
    let authorization = if reason == "restart" {
        crate::identities::saved_authorization_in(db, env).await?
    } else {
        authorization_in(db, env).await?
    }
    .ok_or(AuthorizationError::Forbidden)?;
    crate::ledgers::terminal_in(db, env, status).await?;
    crate::ledgers::append_audit_in(
        db,
        &AuditCommand {
            scope: env.scope.clone(),
            operation_id: format!("compact-terminal:{}", env.owner.run_id.0),
            subject_id: authorization.context.principal.subject_id,
            action: "compact.terminal".into(),
            resource: serde_json::json!({"owner":env.owner,"generation":generation,"state":state}),
            outcome: status.as_str().into(),
        },
        at,
    )
    .await?;
    Ok(())
}
#[async_trait::async_trait]
impl RunContextRepository for PgCanonicalStore {
    async fn read_execution_settings(
        &self,
        env: &EnterpriseOwnerEnvelope,
    ) -> Result<RunSnapshot, RepositoryError> {
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        Self::fence_envelope_in(&tx, &proof, env).await?;
        let snapshot = frozen_in(&tx, &env.scope, &env.owner.run_id)
            .await?
            .ok_or(CoordinationError::Corrupt)?;
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(snapshot)
    }
    async fn read_execution_context(
        &self,
        env: &EnterpriseOwnerEnvelope,
    ) -> Result<RunContextReadback, RepositoryError> {
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        Self::fence_envelope_in(&tx, &proof, env).await?;
        let source = context_source_in(&tx, env).await?;
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(source)
    }
    async fn read_compaction_result(
        &self,
        env: &EnterpriseOwnerEnvelope,
        id: &str,
        revision: Option<TranscriptSeq>,
    ) -> Result<Option<ProjectionGeneration>, RepositoryError> {
        operation(id)?;
        if revision.is_some_and(|revision| revision.0 > i64::MAX as u64) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        Self::fence_envelope_in(&tx, &proof, env).await?;
        check_link(&tx, env, id).await?;
        let result = if let Some(stored) = stored_in(&tx, env, id).await? {
            if !matches!(stored.state.as_str(), "committed" | "rejected") {
                None
            } else {
                if revision.is_some_and(|revision| revision != stored.source.source_end)
                    || stored.state == "rejected" && stored.reason.as_deref() != Some("no_gain")
                {
                    return Err(CoordinationError::Conflict.into());
                }
                let expected = if stored.state == "committed" {
                    ProjectionGeneration(
                        stored
                            .source
                            .generation
                            .0
                            .checked_add(1)
                            .filter(|value| *value <= i64::MAX as u64)
                            .ok_or(CoordinationError::Corrupt)?,
                    )
                } else {
                    stored.source.generation
                };
                if stored.generation != Some(expected) {
                    return Err(CoordinationError::Corrupt.into());
                }
                Some(expected)
            }
        } else {
            None
        };
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
    async fn begin_compaction(
        &self,
        env: &EnterpriseOwnerEnvelope,
        intent: &CompactIntent,
    ) -> Result<(), RepositoryError> {
        operation(&intent.operation)?;
        encode(intent, 16384)?;
        if intent.owner != env.owner {
            return Err(AuthorizationError::StaleOwner.into());
        }
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        crate::turns::execution_in(&tx, env, &proof).await?;
        let record = record_in(&tx, &env.scope, &env.owner.run_id).await?;
        if record.status != RunStatus::Running {
            return Err(CoordinationError::Conflict.into());
        }
        let source = context_source_in(&tx, env).await?;
        source_check(&source.snapshot, &intent.source, false)?;
        if source.frozen.context_read_only
            || source
                .frozen
                .context_policy_fingerprint
                .as_deref()
                .is_some_and(|policy| policy != intent.source.policy_fingerprint)
        {
            return Err(CoordinationError::Conflict.into());
        }
        check_link(&tx, env, &intent.operation).await?;
        if let Some(old) = stored_in(&tx, env, &intent.operation).await? {
            if old.source != intent.source || old.state != "pending" {
                return Err(CoordinationError::Conflict.into());
            }
        } else {
            let mut values = values(&env.scope, &env.owner.run_id);
            values.extend([
                intent.operation.clone(),
                env.owner.session_lifetime_id.0.clone(),
                encode(&intent.source, 16384)?,
            ]);
            tx.execute("INSERT INTO compact_operations(organization_id,tenant_id,project_id,agent_uid,run_id,operation,lifetime,source_json,state) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'pending')",&parameters(&values)).await.map_err(unavailable)?;
        }
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }
    async fn settle_compaction(
        &self,
        env: &EnterpriseOwnerEnvelope,
        intent: &CompactIntent,
        replacement: Option<&[Message]>,
        reason: &str,
    ) -> Result<ProjectionGeneration, RepositoryError> {
        operation(&intent.operation)?;
        encode(intent, 16384)?;
        if intent.owner != env.owner {
            return Err(AuthorizationError::StaleOwner.into());
        }
        if reason.is_empty()
            || reason.len() > 1024
            || replacement.is_some_and(|messages| messages.len() > 32768)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let candidate = replacement
            .map(|messages| encode(messages, 4 * 1024 * 1024))
            .transpose()?;
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        crate::turns::execution_in(&tx, env, &proof).await?;
        if record_in(&tx, &env.scope, &env.owner.run_id).await?.status != RunStatus::Running {
            return Err(CoordinationError::Conflict.into());
        }
        check_link(&tx, env, &intent.operation).await?;
        let old = stored_in(&tx, env, &intent.operation)
            .await?
            .ok_or(CoordinationError::Conflict)?;
        if old.source != intent.source || old.state != "pending" {
            return Err(CoordinationError::Conflict.into());
        }
        let source = context_source_in(&tx, env).await?;
        source_check(&source.snapshot, &intent.source, false)?;
        if source.frozen.context_read_only {
            return Err(AuthorizationError::Forbidden.into());
        }
        let generation = if let (Some(messages), Some(raw)) = (replacement, candidate) {
            let prefix = source
                .snapshot
                .messages
                .get(
                    ..usize::try_from(intent.source.source_end.0)
                        .map_err(|_| CoordinationError::Corrupt)?,
                )
                .ok_or(CoordinationError::Corrupt)?;
            validate_compaction_candidate(prefix, messages)?;
            let next = ProjectionGeneration(
                intent
                    .source
                    .generation
                    .0
                    .checked_add(1)
                    .filter(|value| *value <= i64::MAX as u64)
                    .ok_or(CoordinationError::Conflict)?,
            );
            let mut values = scope_values(&env.scope);
            values.extend([
                env.owner.session_lifetime_id.0.clone(),
                intent.source.source_end.0.to_string(),
                next.0.to_string(),
                raw.clone(),
            ]);
            tx.execute("INSERT INTO context_heads VALUES($1,$2,$3,$4,$5,$6::text::bigint,$7::text::bigint,$8) ON CONFLICT(organization_id,tenant_id,project_id,agent_uid,lifetime) DO UPDATE SET source_end=excluded.source_end,generation=excluded.generation,messages_json=excluded.messages_json",&parameters(&values)).await.map_err(unavailable)?;
            let mut values = scope_values(&env.scope);
            values.extend([
                env.owner.session_key.0.clone(),
                env.owner.session_lifetime_id.0.clone(),
                next.0.to_string(),
                intent.source.generation.0.to_string(),
            ]);
            let changed=tx.execute(&format!("UPDATE session_heads SET projection_generation=$7::text::bigint WHERE {SCOPE} AND session_id=$5 AND lifetime=$6 AND projection_generation=$8::text::bigint"),&parameters(&values)).await.map_err(unavailable)?;
            if changed != 1 {
                return Err(CoordinationError::Conflict.into());
            }
            let mut values = values[..4].to_vec();
            values.extend([
                intent.operation.clone(),
                reason.into(),
                next.0.to_string(),
                format!("{:x}", Sha256::digest(raw.as_bytes())),
            ]);
            tx.execute(&format!("UPDATE compact_operations SET state='committed',reason=$6,result_generation=$7::text::bigint,candidate_digest=$8 WHERE {SCOPE} AND operation=$5 AND state='pending'"),&parameters(&values)).await.map_err(unavailable)?;
            append_in(
                &tx,
                env,
                "context_compacted",
                &serde_json::json!({"generation":next,"operation":intent.operation,"mode":reason}),
            )
            .await?;
            terminal_in(&tx, env, "committed", reason, next).await?;
            next
        } else {
            let mut values = scope_values(&env.scope);
            values.extend([
                intent.operation.clone(),
                reason.into(),
                intent.source.generation.0.to_string(),
            ]);
            tx.execute(&format!("UPDATE compact_operations SET state='rejected',reason=$6,result_generation=$7::text::bigint WHERE {SCOPE} AND operation=$5 AND state='pending'"),&parameters(&values)).await.map_err(unavailable)?;
            terminal_in(&tx, env, "rejected", reason, intent.source.generation).await?;
            intent.source.generation
        };
        seal_in(&tx, env).await?;
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(generation)
    }
    async fn admit_compaction(
        &self,
        request: &CompactRunRequest,
        snapshot: &RunSnapshot,
        authorization: &AuthorizationRequest,
        verifier: Arc<dyn AuthorizationVerifier>,
    ) -> Result<(Admission, ExactOwner), RepositoryError> {
        operation(&request.operation_id)?;
        encode(request, 16384)?;
        let admission = RunAdmission {
            workflow_node: None,
            session_key: request.session_key.clone(),
            expected_lifetime: Some(request.session_lifetime_id.clone()),
            request_id: RequestId::String(format!("compact:{}", request.operation_id)),
            input: encode(request, 16384)?,
            mode: AdmissionMode::RejectIfBusy,
            plan_execution: None,
        };
        self.admit_in(
            &admission,
            snapshot,
            authorization,
            verifier,
            crate::executions::AdmissionPurpose::Compact(request),
        )
        .await
    }
    async fn read_compact_fact(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Option<RunCompactReadback>, RepositoryError> {
        if run.0.is_empty() || run.0.len() > 1024 {
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
        let owner = exact_owner_in(&tx, scope, run).await?;
        let record = record_in(&tx, scope, run).await?;
        let result = if let Some(receipt) = compact_in(&tx, scope, &owner, &record).await? {
            let stamp = stamp_in(&tx, scope, &owner, record.last_seq).await?;
            Some(RunCompactReadback {
                run: record,
                receipt,
                view_stamp: stamp,
            })
        } else {
            None
        };
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
    async fn read_compact_request(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        life: &SessionLifetimeId,
        id: &str,
    ) -> Result<Option<CompactRunRequest>, RepositoryError> {
        operation(id)?;
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        permanent_life(
            &tx,
            &SessionWriterLeaseKey {
                scope: scope.clone(),
                session_key: key.clone(),
                session_lifetime_id: life.clone(),
            },
        )
        .await?;
        let mut values = scope_values(scope);
        values.extend([id.into(), key.0.clone(), life.0.clone()]);
        let row=tx.query_opt("SELECT CASE WHEN octet_length(l.request_json)<=16384 THEN l.request_json END,CASE WHEN octet_length(l.run_id)<=1024 THEN l.run_id END FROM compact_run_links l JOIN runs r ON (r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id)=(l.organization_id,l.tenant_id,l.project_id,l.agent_uid,l.run_id) WHERE l.organization_id=$1 AND l.tenant_id=$2 AND l.project_id=$3 AND l.agent_uid=$4 AND l.operation_id=$5 AND r.session_id=$6 AND r.lifetime=$7",&parameters(&values)).await.map_err(unavailable)?;
        let result = if let Some(row) = row {
            let request: CompactRunRequest =
                serde_json::from_str(&field(&row, 0)?).map_err(|_| CoordinationError::Corrupt)?;
            if request.session_key != *key
                || request.session_lifetime_id != *life
                || request.operation_id != id
            {
                return Err(CoordinationError::Corrupt.into());
            }
            let run = RunId(field(&row, 1)?);
            let owner = exact_owner_in(&tx, scope, &run).await?;
            let record = record_in(&tx, scope, &run).await?;
            compact_in(&tx, scope, &owner, &record)
                .await?
                .ok_or(CoordinationError::Corrupt)?;
            Some(request)
        } else {
            None
        };
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
    async fn read_compact_operation(
        &self,
        scope: &TenantContext,
        request: &CompactRunRequest,
    ) -> Result<Option<RunRecord>, RepositoryError> {
        operation(&request.operation_id)?;
        encode(request, 16384)?;
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let meta = metadata_in(&tx, scope, &request.session_key, false)
            .await?
            .ok_or(CoordinationError::Forbidden)?;
        if meta.lifetime != request.session_lifetime_id {
            return Err(AuthorizationError::StaleOwner.into());
        }
        let mut values = scope_values(scope);
        values.extend([
            request.operation_id.clone(),
            request.session_key.0.clone(),
            request.session_lifetime_id.0.clone(),
        ]);
        let row=tx.query_opt("SELECT CASE WHEN octet_length(l.run_id)<=1024 THEN l.run_id END,CASE WHEN octet_length(l.request_json)<=16384 THEN l.request_json END FROM compact_run_links l JOIN runs r ON (r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id)=(l.organization_id,l.tenant_id,l.project_id,l.agent_uid,l.run_id) WHERE l.organization_id=$1 AND l.tenant_id=$2 AND l.project_id=$3 AND l.agent_uid=$4 AND l.operation_id=$5 AND r.session_id=$6 AND r.lifetime=$7",&parameters(&values)).await.map_err(unavailable)?;
        let result = if let Some(row) = row {
            let saved: CompactRunRequest =
                serde_json::from_str(&field(&row, 1)?).map_err(|_| CoordinationError::Corrupt)?;
            if saved != *request {
                return Err(CoordinationError::Conflict.into());
            }
            let run = RunId(field(&row, 0)?);
            let owner = exact_owner_in(&tx, scope, &run).await?;
            let record = record_in(&tx, scope, &run).await?;
            compact_in(&tx, scope, &owner, &record)
                .await?
                .ok_or(CoordinationError::Corrupt)?;
            Some(record)
        } else {
            None
        };
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
}
