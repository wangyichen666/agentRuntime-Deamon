//! daemon 维护只收敛持久事实，不重新调用 Provider、工具、进程或 Hook。
use crate::canonical::{PgCanonicalStore, field, grant_in, key_values, proof_key, set_scope};
use crate::events::{append_in, seal_in};
use crate::executions::{commit_terminal_in, record_in};
use crate::identities::{exact_owner_in, saved_authorization_in};
use crate::sessions::encode;
use crate::{SCOPE, now, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::DaemonMaintenanceRepository;
use sha2::{Digest, Sha256};
use tokio_postgres::Transaction;

pub(crate) async fn artifact_write_gate(db: &Transaction<'_>) -> Result<(), CoordinationError> {
    db.query_one("SELECT pg_advisory_xact_lock_shared(174928,48)", &[])
        .await
        .map_err(unavailable)?;
    Ok(())
}
async fn hooks_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    key: &SessionKey,
) -> Result<(), CoordinationError> {
    let mut values = scope_values(scope);
    values.push(key.0.clone());
    let rows=db.query(&format!("SELECT CASE WHEN octet_length(operation_id) BETWEEN 1 AND 256 THEN operation_id END,CASE WHEN octet_length(lifetime) BETWEEN 1 AND 128 THEN lifetime END FROM hook_outcomes WHERE {SCOPE} AND session_id=$5 AND CASE WHEN pg_input_is_valid(outcome_json,'jsonb') THEN outcome_json::jsonb->>'status'='running' ELSE TRUE END ORDER BY storage_row_id LIMIT 1025"),&parameters(&values)).await.map_err(unavailable)?;
    if rows.len() > 1024 {
        return Err(CoordinationError::Corrupt);
    }
    for row in rows {
        let operation = field(&row, 0)?;
        let life = SessionLifetimeId(field(&row, 1)?);
        let mut outcome = crate::hooks::saved_in(db, scope, key, &life, &operation)
            .await?
            .ok_or(CoordinationError::Corrupt)?;
        if outcome.status == HookStatus::Running {
            outcome.status = HookStatus::UnknownAfterRestart;
            outcome.failure = Some(HookFailure::UnknownAfterRestart);
            outcome.effect = None;
            validate_hook_outcome(&outcome).map_err(|_| CoordinationError::Corrupt)?;
            let mut values = values.clone();
            values.extend([life.0, operation, encode(&outcome, 49152)?]);
            db.execute(&format!("UPDATE hook_outcomes SET outcome_json=$8 WHERE {SCOPE} AND session_id=$5 AND lifetime=$6 AND operation_id=$7"),&parameters(&values)).await.map_err(unavailable)?;
        }
    }
    Ok(())
}
#[async_trait::async_trait]
impl DaemonMaintenanceRepository for PgCanonicalStore {
    async fn read_storage_health(
        &self,
        scope: &TenantContext,
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
        let mut counts = serde_json::Map::new();
        for (label, table, predicate) in [
            ("sessions", "session_heads", "NOT deleted"),
            ("queued", "runs", "status='queued'"),
            (
                "running",
                "runs",
                "status IN ('running','waiting_interaction')",
            ),
            ("unknown_runs", "runs", "status='unknown_after_restart'"),
            (
                "maintenance_failed",
                "maintenance_diagnostics",
                "diagnostic='failed'",
            ),
            (
                "maintenance_skipped",
                "maintenance_diagnostics",
                "diagnostic='skipped_read_only'",
            ),
            ("orphaned_resources", "resources", "state='orphaned'"),
            ("pending_compacts", "compact_operations", "state='pending'"),
            ("legacy_quarantined_memories", "memories", "scope='legacy'"),
        ] {
            let count: i64 = tx
                .query_one(
                    &format!("SELECT count(*)::bigint FROM {table} WHERE {SCOPE} AND {predicate}"),
                    &parameters(&scope_values(scope)),
                )
                .await
                .map_err(unavailable)?
                .try_get(0)
                .map_err(unavailable)?;
            counts.insert(label.into(), count.into());
        }
        let mut values = scope_values(scope);
        values.push(now(&tx).await?.to_string());
        let writer=tx.query_one(&format!("SELECT count(*) FILTER(WHERE expires_at_ms>$5::text::bigint)::bigint,count(*) FILTER(WHERE expires_at_ms<=$5::text::bigint)::bigint FROM session_writer_leases WHERE {SCOPE}"),&parameters(&values)).await.map_err(unavailable)?;
        let version: i64 = tx
            .query_one("SELECT max(version) FROM canonical_migrations", &[])
            .await
            .map_err(unavailable)?
            .try_get(0)
            .map_err(unavailable)?;
        let result = serde_json::json!({"backend":"postgres","schema_version":version,"integrity":"constraints_enforced","counts":counts,"writer":{"active":writer.try_get::<_,i64>(0).map_err(unavailable)?,"expired":writer.try_get::<_,i64>(1).map_err(unavailable)?},"budgets":{"active_resources":16,"queued_per_session":64,"queued_total":1024,"tool_calls_per_batch":64,"resource_log_bytes":65536,"artifact_page_bytes":16384}});
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
    async fn recover_interrupted_work(
        &self,
        scope: &TenantContext,
    ) -> Result<usize, RepositoryError> {
        let candidates = {
            let mut client = self.inner.client.lock().await;
            let tx = client.transaction().await.map_err(unavailable)?;
            set_scope(&tx, scope).await?;
            let rows=tx.query(&format!("SELECT CASE WHEN octet_length(session_id) BETWEEN 1 AND 1024 THEN session_id END,CASE WHEN octet_length(lifetime) BETWEEN 1 AND 128 THEN lifetime END FROM session_heads h WHERE {SCOPE} AND NOT deleted AND (EXISTS(SELECT 1 FROM runs r WHERE (r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.session_id,r.lifetime)=(h.organization_id,h.tenant_id,h.project_id,h.agent_uid,h.session_id,h.lifetime) AND r.status IN ('running','waiting_interaction')) OR EXISTS(SELECT 1 FROM resources resource JOIN runs r ON (r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id)=(resource.organization_id,resource.tenant_id,resource.project_id,resource.agent_uid,resource.run_id) WHERE (r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.session_id,r.lifetime)=(h.organization_id,h.tenant_id,h.project_id,h.agent_uid,h.session_id,h.lifetime) AND resource.state IN ('created','starting','running','stopping')) OR EXISTS(SELECT 1 FROM compact_operations c JOIN runs r ON (r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id)=(c.organization_id,c.tenant_id,c.project_id,c.agent_uid,c.run_id) WHERE (r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.session_id,r.lifetime)=(h.organization_id,h.tenant_id,h.project_id,h.agent_uid,h.session_id,h.lifetime) AND c.state='pending') OR EXISTS(SELECT 1 FROM hook_outcomes o WHERE (o.organization_id,o.tenant_id,o.project_id,o.agent_uid,o.session_id)=(h.organization_id,h.tenant_id,h.project_id,h.agent_uid,h.session_id) AND CASE WHEN pg_input_is_valid(o.outcome_json,'jsonb') THEN o.outcome_json::jsonb->>'status'='running' ELSE TRUE END)) ORDER BY session_id LIMIT 1025"),&parameters(&scope_values(scope))).await.map_err(unavailable)?;
            if rows.len() > 1024 {
                return Err(CoordinationError::Conflict.into());
            }
            let mut output = Vec::new();
            for row in rows {
                output.push(SessionWriterLeaseKey {
                    scope: scope.clone(),
                    session_key: SessionKey(field(&row, 0)?),
                    session_lifetime_id: SessionLifetimeId(field(&row, 1)?),
                });
            }
            tx.commit().await.map_err(unavailable)?;
            output
        };
        let mut recovered = 0;
        for key in candidates {
            let id = proof_key(&key)?;
            let mut proofs = self.proofs.lock().await;
            if !proofs.contains_key(&id) && proofs.len() >= 1024 {
                return Err(CoordinationError::Conflict.into());
            }
            let mut client = self.inner.client.lock().await;
            let tx = client.transaction().await.map_err(unavailable)?;
            set_scope(&tx, scope).await?;
            // 活跃远端 owner 仍由数据库租约保护，恢复不得把它的任务判成未知。
            let proof = match grant_in(&tx, &key, &self.writer_owner, 30000).await {
                Ok(proof) => proof,
                Err(CoordinationError::LeaseBusy | CoordinationError::StaleLease) => continue,
                Err(error) => return Err(error.into()),
            };
            let values = key_values(&key);
            let rows=tx.query(&format!("SELECT CASE WHEN octet_length(id) BETWEEN 1 AND 1024 THEN id END FROM runs WHERE {SCOPE} AND session_id=$5 AND lifetime=$6 AND status IN ('running','waiting_interaction') ORDER BY storage_row_id LIMIT 1025"),&parameters(&values)).await.map_err(unavailable)?;
            if rows.len() > 1024 {
                return Err(CoordinationError::Corrupt.into());
            }
            hooks_in(&tx, scope, &key.session_key).await?;
            let mut terminal_envelopes = Vec::new();
            for row in rows {
                let run = RunId(field(&row, 0)?);
                let env = EnterpriseOwnerEnvelope {
                    scope: scope.clone(),
                    owner: exact_owner_in(&tx, scope, &run).await?,
                };
                let authorization = saved_authorization_in(&tx, &env)
                    .await?
                    .ok_or(CoordinationError::Corrupt)?;
                let record = record_in(&tx, scope, &run).await?;
                if record.kind == RunKind::Compact {
                    crate::compacts::terminal_in(
                        &tx,
                        &env,
                        "rejected",
                        "restart",
                        ProjectionGeneration(0),
                    )
                    .await?;
                } else {
                    commit_terminal_in(
                        &tx,
                        &env,
                        &TurnCommit {
                            owner: env.owner.clone(),
                            status: RunStatus::UnknownAfterRestart,
                            content: None,
                            error: Some((-32002, "执行结果未知，禁止自动重放".into())),
                        },
                        false,
                        authorization,
                    )
                    .await?
                    .ok_or(CoordinationError::Corrupt)?;
                }
                terminal_envelopes.push(env);
                recovered += 1;
            }
            tx.execute(&format!("UPDATE resources SET state='orphaned',terminal_reason='restart: process identity cannot be proven',updated_at_ms=$7::text::bigint WHERE {SCOPE} AND lifetime=$6 AND state IN ('starting','running','stopping') AND run_id IN(SELECT id FROM runs WHERE {SCOPE} AND session_id=$5 AND lifetime=$6)"),&parameters(&{let mut values=values.clone();values.push(now(&tx).await?.to_string());values})).await.map_err(unavailable)?;
            tx.execute(&format!("UPDATE compact_operations SET state='rejected',reason='restart: candidate not committed' WHERE {SCOPE} AND lifetime=$6 AND state='pending' AND run_id IN(SELECT id FROM runs WHERE {SCOPE} AND session_id=$5 AND lifetime=$6)"),&parameters(&values)).await.map_err(unavailable)?;
            // 资源/compact维护也是最终快照的一部分，所有事件最后统一封存。
            for env in terminal_envelopes {
                seal_in(&tx, &env).await?;
            }
            Self::before_commit(&tx, &proof).await?;
            tx.commit().await.map_err(unavailable)?;
            proofs.insert(id, proof);
        }
        Ok(recovered)
    }
    async fn collect_local_artifacts(
        &self,
        scope: &TenantContext,
        retention_ms: u64,
    ) -> Result<usize, RepositoryError> {
        if scope != &TenantContext::local() {
            return Err(AuthorizationError::Forbidden.into());
        }
        if !(60000..=365 * 86400000).contains(&retention_ms) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let files = self
            .local_files
            .clone()
            .ok_or(ArtifactError::StorageUnavailable)?;
        let retention = std::time::Duration::from_millis(retention_ms);
        let inner = self.inner.clone();
        let budget = self.file_budget.clone();
        let scope = scope.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        {
            let mut tasks = self.maintenance_tasks.lock().await;
            tasks.retain(|task| !task.is_finished());
            if tasks.len() >= 4 {
                return Err(CoordinationError::Conflict.into());
            }
            tasks.push(tokio::spawn(async move {
                let result = collect_files(inner, budget, files, scope, retention).await;
                let _ = sender.send(result);
            }));
        }
        receiver
            .await
            .map_err(|_| CoordinationError::StorageUnavailable)?
    }

    async fn reconcile_run_outcome(
        &self,
        env: &EnterpriseOwnerEnvelope,
        expected: EventSeq,
        status: RunStatus,
        content: Option<&str>,
        evidence: &str,
    ) -> Result<RunRecord, RepositoryError> {
        if expected.0 > i64::MAX as u64
            || content.is_some_and(|text| text.len() > 524288)
            || evidence.len() > 4096
            || evidence.trim().chars().count() < 16
            || !matches!(
                status,
                RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled
            )
            || (status == RunStatus::Completed && content.is_none_or(str::is_empty))
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        let authorization = Self::fence_envelope_in(&tx, &proof, env).await?;
        let run = record_in(&tx, &env.scope, &env.owner.run_id).await?;
        if run.kind == RunKind::Compact
            || run.status != RunStatus::UnknownAfterRestart
            || run.last_seq != expected
        {
            return Err(CoordinationError::Conflict.into());
        }
        let mut values = scope_values(&env.scope);
        values.push(env.owner.run_id.0.clone());
        let is_child = tx.query_opt(&format!("SELECT result_state FROM delegations WHERE {SCOPE} AND child_run_id=$5 FOR UPDATE"),&parameters(&values)).await.map_err(unavailable)?;
        if let Some(row) = &is_child {
            if field(row, 0)? != "unconsumed" {
                return Err(CoordinationError::Conflict.into());
            }
        }
        let at = now(&tx).await?;
        let content = content.filter(|_| status == RunStatus::Completed);
        let (error_code, error_message) = match status {
            RunStatus::Completed => (None, None),
            RunStatus::Failed => (Some(-32003_i64), Some("人工核验为失败")),
            RunStatus::Cancelled => (Some(-32800_i64), Some("人工核验为取消")),
            _ => return Err(CoordinationError::InvalidRequest.into()),
        };
        let mut update = values.clone();
        update.extend([status.as_str().into(), at.to_string()]);
        let mut params = parameters(&update);
        params.extend([
            &content as &(dyn tokio_postgres::types::ToSql + Sync),
            &error_code,
            &error_message,
        ]);
        tx.execute(&format!("UPDATE runs SET status=$6,updated_at_ms=$7::text::bigint,content=$8,error_code=$9,error_message=$10 WHERE {SCOPE} AND id=$5"),&params).await.map_err(unavailable)?;
        for table in ["turns", "queued_messages"] {
            tx.execute(
                &format!("UPDATE {table} SET status=$6 WHERE {SCOPE} AND run_id=$5"),
                &parameters(&update[..6]),
            )
            .await
            .map_err(unavailable)?;
        }
        if let Some(content) = content {
            append_in(
                &tx,
                env,
                "assistant_content",
                &serde_json::json!({"content":content,"manual":true}),
            )
            .await?;
        }
        append_in(&tx,env,"manual_resolution",&serde_json::json!({"from":"unknown_after_restart","status":status,"evidence_sha256":format!("{:x}",Sha256::digest(evidence.as_bytes())),"evidence_note":evidence,"previous_last_seq":expected})).await?;
        let result = record_in(&tx, &env.scope, &env.owner.run_id).await?;
        if is_child.is_some() {
            let mut delegate = update.clone();
            delegate.push(encode(&result, 131072)?);
            tx.execute(&format!("UPDATE delegations SET status=$6,finished_at_ms=$7::text::bigint,terminal_json=$8 WHERE {SCOPE} AND child_run_id=$5"),&parameters(&delegate)).await.map_err(unavailable)?;
        }
        crate::ledgers::append_audit_in(&tx,&AuditCommand{scope:env.scope.clone(),operation_id:format!("run-reconcile:{}:{}",env.owner.run_id.0,expected.0),subject_id:authorization.context.principal.subject_id,action:"run.reconcile".into(),resource:serde_json::json!({"owner":env.owner,"previous_last_seq":expected,"evidence_sha256":format!("{:x}",Sha256::digest(evidence.as_bytes()))}),outcome:status.as_str().into()},at).await?;
        seal_in(&tx, env).await?;
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
}

async fn collect_files(
    inner: std::sync::Arc<crate::PgPublicationStore>,
    budget: std::sync::Arc<tokio::sync::Semaphore>,
    files: std::sync::Arc<agent_artifact::FileArtifactDirectory>,
    scope: TenantContext,
    retention: std::time::Duration,
) -> Result<usize, RepositoryError> {
    let mut client = inner.client.lock().await;
    let tx = client.transaction().await.map_err(unavailable)?;
    set_scope(&tx, &scope).await?;
    // 受管 GC 必须在整个文件工作期间保有数据库锁，不能被 idle 超时释放。
    tx.batch_execute("SET LOCAL idle_in_transaction_session_timeout='0'")
        .await
        .map_err(unavailable)?;
    // 物理目录可能共享；GC必须看见所有域引用。普通RLS运行角色不可做跨域物理删除。
    let global: bool = tx
        .query_one(
            "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname=current_user",
            &[],
        )
        .await
        .map_err(unavailable)?
        .try_get(0)
        .map_err(unavailable)?;
    if !global {
        return Err(AuthorizationError::Forbidden.into());
    }
    tx.query_one("SELECT pg_advisory_xact_lock(174928,48)", &[])
        .await
        .map_err(unavailable)?;
    let permit = budget
        .clone()
        .try_acquire_owned()
        .map_err(|_| CoordinationError::Conflict)?;
    let directory = files.clone();
    let candidates = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        directory.candidates()
    })
    .await
    .map_err(|_| ArtifactError::StorageUnavailable)??;
    let mut deleted = 0;
    for path in candidates {
        let hash = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_prefix("sha256-"))
            .and_then(|name| name.strip_suffix(".txt"))
            .ok_or(ArtifactError::InvalidReference)?;
        let protected:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM tool_executions WHERE artifact_ref=$1) OR EXISTS(SELECT 1 FROM artifact_publications WHERE store_id=$2 AND sha256=$3)",&[&path.to_string_lossy().as_ref(),&files.store_id(),&hash]).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
        if !protected {
            let permit = budget
                .clone()
                .try_acquire_owned()
                .map_err(|_| CoordinationError::Conflict)?;
            let directory = files.clone();
            if tokio::task::spawn_blocking(move || {
                let _permit = permit;
                directory.remove_if_old(&path, retention)
            })
            .await
            .map_err(|_| ArtifactError::StorageUnavailable)??
            {
                deleted += 1;
            }
        }
    }
    let permit = budget
        .clone()
        .try_acquire_owned()
        .map_err(|_| CoordinationError::Conflict)?;
    deleted += tokio::task::spawn_blocking(move || {
        let _permit = permit;
        files.clean_abandoned_temporary(retention)
    })
    .await
    .map_err(|_| ArtifactError::StorageUnavailable)??;
    tx.commit().await.map_err(unavailable)?;
    Ok(deleted)
}
