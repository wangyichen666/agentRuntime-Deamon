//! native PostgreSQL运行准入、队列及终态；未知副作用沿用core归一化规则。
use crate::canonical::{PgCanonicalStore, field, grant_in, proof_key, set_scope, unsigned};
use crate::events::{append_in, seal_in};
use crate::identities::{authorization_in, exact_owner_in, identity_in};
use crate::sessions::{create_in, encode, insert_batch, metadata_in};
use crate::{SCOPE, now, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::RunExecutionRepository;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio_postgres::GenericClient;
pub(crate) enum AdmissionPurpose<'a> {
    Chat {
        workload: bool,
    },
    Compact(&'a CompactRunRequest),
    Continuation {
        parent: &'a EnterpriseOwnerEnvelope,
        operation: &'a str,
    },
    Delegation {
        parent: &'a EnterpriseOwnerEnvelope,
        request: &'a DelegationRequest,
    },
}
pub(crate) async fn record_in<C: GenericClient + Sync>(
    db: &C,
    scope: &TenantContext,
    run: &RunId,
) -> Result<RunRecord, CoordinationError> {
    let mut values = scope_values(scope);
    values.push(run.0.clone());
    let row=db.query_opt("SELECT CASE WHEN octet_length(r.session_id)<=1024 THEN r.session_id END,CASE WHEN octet_length(t.id)<=1024 THEN t.id END,CASE WHEN octet_length(r.request_id_wire_json)<=8192 THEN r.request_id_wire_json END,r.status,r.last_seq,r.content IS NULL,CASE WHEN octet_length(r.content)<=524288 THEN r.content END,r.error_code,r.error_message IS NULL,CASE WHEN octet_length(r.error_message)<=65536 THEN r.error_message END,EXISTS(SELECT 1 FROM compact_run_links c WHERE (c.organization_id,c.tenant_id,c.project_id,c.agent_uid,c.run_id)=(r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id)) FROM runs r JOIN turns t ON (t.organization_id,t.tenant_id,t.project_id,t.agent_uid,t.run_id)=(r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id) WHERE r.organization_id=$1 AND r.tenant_id=$2 AND r.project_id=$3 AND r.agent_uid=$4 AND r.id=$5",&parameters(&values)).await.map_err(unavailable)?.ok_or(CoordinationError::Forbidden)?;
    let optional = |null: usize, body: usize| -> Result<Option<String>, CoordinationError> {
        if row.try_get::<_, bool>(null).map_err(unavailable)? {
            Ok(None)
        } else {
            Ok(Some(field(&row, body)?))
        }
    };
    Ok(RunRecord {
        kind: if row.try_get::<_, bool>(10).map_err(unavailable)? {
            RunKind::Compact
        } else {
            RunKind::Chat
        },
        run_id: run.clone(),
        turn_id: TurnId(field(&row, 1)?),
        session_id: SessionKey(field(&row, 0)?),
        request_id: serde_json::from_str(&field(&row, 2)?)
            .map_err(|_| CoordinationError::Corrupt)?,
        status: RunStatus::parse(&row.try_get::<_, String>(3).map_err(unavailable)?)
            .map_err(|_| CoordinationError::Corrupt)?,
        last_seq: EventSeq(unsigned(row.try_get(4).map_err(unavailable)?)?),
        content: optional(5, 6)?,
        error_code: row.try_get(7).map_err(unavailable)?,
        error_message: optional(8, 9)?,
    })
}
impl PgCanonicalStore {
    pub(crate) async fn admit_in(
        &self,
        admission: &RunAdmission,
        snapshot: &RunSnapshot,
        request: &AuthorizationRequest,
        verifier: Arc<dyn AuthorizationVerifier>,
        purpose: AdmissionPurpose<'_>,
    ) -> Result<(Admission, ExactOwner), RepositoryError> {
        let (workload, compact, continuation, delegation) = match purpose {
            AdmissionPurpose::Chat { workload } => (workload, None, None, None),
            AdmissionPurpose::Compact(request) => (false, Some(request), None, None),
            AdmissionPurpose::Continuation { parent, operation } => {
                (false, None, Some((parent, operation)), None)
            }
            AdmissionPurpose::Delegation { parent, request } => {
                (false, None, None, Some((parent, request)))
            }
        };
        if admission.session_key.0.is_empty()
            || admission.session_key.0.len() > 1024
            || admission.input.len() > 65536
            || request.action != Action::SessionPrompt
            || request.context.request_id != admission.request_id
            || request.attributes.workspace != snapshot.cwd
            || request.attributes.entry_channel != snapshot.entry_channel
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        if workload && request.context.principal.kind != SubjectKind::Workload {
            return Err(AuthorizationError::Forbidden.into());
        }
        let frozen = encode(snapshot, 1048576)?;
        let wire = encode(&admission.request_id, 8192)?;
        encode(request, 65536)?;
        let scope = &request.context.scope;
        let mut proofs = self.proofs.lock().await;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        if admission.workflow_node.is_some(){tx.query_one("SELECT pg_advisory_xact_lock(hashtextextended($1,73))",&[&encode(scope,1024)?]).await.map_err(unavailable)?;}
        let mut checked = request.clone();
        checked.attributes.timestamp_ms = now(&tx).await?;
        let identity = identity_in(&tx, scope, &checked.context.principal.subject_id).await?;
        verifier.verify(&checked, &identity)?;
        if let Some(lease)=&admission.workflow_node {crate::workflow::native_in(&tx,request,admission,snapshot,lease,None).await?;}
        let lock = encode(scope, 4096)?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,43))",
            &[&lock],
        )
        .await
        .map_err(unavailable)?;
        let mut delegation_proof = None;
        let prepared_child = if let Some((parent, request)) = delegation {
            if admission.session_key != request.child_session_id
                || admission.input != request.task
                || admission.plan_execution.is_some()
                || admission.mode != AdmissionMode::Queue
            {
                return Err(CoordinationError::InvalidRequest.into());
            }
            let key = SessionWriterLeaseKey {
                scope: parent.scope.clone(),
                session_key: parent.owner.session_key.clone(),
                session_lifetime_id: parent.owner.session_lifetime_id.clone(),
            };
            let proof = proofs
                .get(&proof_key(&key)?)
                .cloned()
                .ok_or(CoordinationError::StaleLease)?;
            Self::fence_envelope_in(&tx, &proof, parent).await?;
            let prepared = crate::delegations::prepare_in(&tx, parent, request).await?;
            if encode(&prepared.snapshot, 1048576)? != frozen {
                return Err(AuthorizationError::Forbidden.into());
            }
            delegation_proof = Some(proof);
            Some(prepared)
        } else {
            None
        };
        let meta = if let Some(meta) = metadata_in(&tx, scope, &admission.session_key, true).await?
        {
            meta
        } else {
            if compact.is_some() {
                return Err(AuthorizationError::Forbidden.into());
            }
            let mut create = checked.clone();
            create.action = Action::SessionCreate;
            verifier.verify(&create, &identity)?;
            create_in(&tx, scope, &admission.session_key).await?
        };
        if meta.deleted
            || admission
                .expected_lifetime
                .as_ref()
                .is_some_and(|life| life != &meta.lifetime)
        {
            return Err(AuthorizationError::StaleOwner.into());
        }
        if continuation.is_none() && delegation.is_none() {let fixed=crate::registry::generation_in(&tx,scope,&meta.key,&meta.lifetime).await?;if fixed.as_ref()!=snapshot.installed_generation.as_ref(){return Err(CoordinationError::Conflict.into())}if let Some(g)=fixed{agent_registry::validate_runtime(&g,snapshot)?;}}
        let key = SessionWriterLeaseKey {
            scope: scope.clone(),
            session_key: meta.key.clone(),
            session_lifetime_id: meta.lifetime.clone(),
        };
        let key_id = proof_key(&key)?;
        if !proofs.contains_key(&key_id) && proofs.len() >= 1024 {
            return Err(CoordinationError::Conflict.into());
        }
        let grant = if let Some(bound) = proofs.get(&key_id) {
            Self::fence_in(&tx, bound).await?;
            bound.clone()
        } else {
            grant_in(&tx, &key, &self.writer_owner, 30_000).await?
        };
        if let Some((parent, operation)) = continuation {
            if admission.plan_execution.is_some() {
                return Err(CoordinationError::InvalidRequest.into());
            }
            Self::fence_envelope_in(&tx, &grant, parent).await?;
            crate::continuations::validate_in(&tx, parent, operation, &admission.input, snapshot)
                .await?;
        }
        let plan_input=serde_json::json!({"message":admission.input,"reject_if_busy":admission.mode==AdmissionMode::RejectIfBusy,"sandbox":snapshot.sandbox_requested,"context_read_only":snapshot.context_read_only}).to_string();
        if admission.plan_execution.is_some() && plan_input.len() > 65536 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let mut values = scope_values(scope);
        values.extend([meta.key.0.clone(), meta.lifetime.0.clone(), wire.clone()]);
        let mut existing=tx.query_opt(&format!("SELECT id,input FROM runs WHERE {SCOPE} AND session_id=$5 AND lifetime=$6 AND request_id_wire_json=$7"),&parameters(&values)).await.map_err(unavailable)?;
        if let Some(plan) = &admission.plan_execution {
            if admission.expected_lifetime.is_none()
                || admission.mode != AdmissionMode::RejectIfBusy
            {
                return Err(CoordinationError::InvalidRequest.into());
            }
            if let Some(receipt) =
                crate::plans::receipt_in(&tx, scope, &meta, plan, &plan_input).await?
            {
                let run = receipt.run_id.ok_or(CoordinationError::Corrupt)?;
                if existing
                    .as_ref()
                    .is_some_and(|row| row.try_get::<_, String>(0).ok().as_deref() != Some(&run.0))
                {
                    return Err(CoordinationError::Conflict.into());
                }
                let mut lookup = scope_values(scope);
                lookup.push(run.0);
                existing = tx
                    .query_opt(
                        &format!("SELECT id,input FROM runs WHERE {SCOPE} AND id=$5"),
                        &parameters(&lookup),
                    )
                    .await
                    .map_err(unavailable)?;
            }
        }
        if let Some(existing) = existing {
            if existing.try_get::<_, String>(1).map_err(unavailable)? != admission.input {
                return Err(CoordinationError::Conflict.into());
            }
            let run = RunId(existing.try_get(0).map_err(unavailable)?);
            let owner = exact_owner_in(&tx, scope, &run).await?;
            let env = EnterpriseOwnerEnvelope {
                scope: scope.clone(),
                owner: owner.clone(),
            };
            let frozen_authorization = authorization_in(&tx, &env)
                .await?
                .ok_or(AuthorizationError::Forbidden)?;
            if frozen_authorization.context.principal != checked.context.principal
                || frozen_authorization.context.request_id != checked.context.request_id
            {
                return Err(AuthorizationError::Forbidden.into());
            }
            let mut lookup = scope_values(scope);
            lookup.push(run.0.clone());
            let stored=tx.query_opt(&format!("SELECT identity_json FROM plan_decisions WHERE {SCOPE} AND run_id=$5 AND decision='execute'"),&parameters(&lookup)).await.map_err(unavailable)?;
            let stored: Option<PlanExecution> = stored
                .map(|row| {
                    serde_json::from_str(&row.try_get::<_, String>(0).map_err(unavailable)?)
                        .map_err(|_| CoordinationError::Corrupt)
                })
                .transpose()?;
            if stored != admission.plan_execution {
                return Err(CoordinationError::Conflict.into());
            }
            if let Some(lease)=&admission.workflow_node {crate::workflow::native_in(&tx,request,admission,snapshot,lease,Some(&run)).await?;}
        let record = record_in(&tx, scope, &run).await?;
            if let Some((parent, _)) = continuation {
                if crate::continuations::relation_in(&tx, scope, &parent.owner.run_id, false)
                    .await?
                    .as_ref()
                    != Some(&run)
                {
                    return Err(CoordinationError::Conflict.into());
                }
            }
            if (record.kind == RunKind::Compact) != compact.is_some() {
                return Err(CoordinationError::Conflict.into());
            }
            if let Some(compact) = compact {
                let receipt = crate::facts::compact_in(&tx, scope, &owner, &record)
                    .await?
                    .ok_or(CoordinationError::Corrupt)?;
                if receipt.operation_id != compact.operation_id
                    || receipt.source.source_end != compact.expected_revision
                    || receipt.source.generation != compact.expected_projection_generation
                {
                    return Err(CoordinationError::Conflict.into());
                }
            }
            if let Some(proof) = &delegation_proof {
                Self::before_commit(&tx, proof).await?;
            }
            Self::before_commit(&tx, &grant).await?;
            tx.commit().await.map_err(unavailable)?;
            proofs.insert(key_id, grant);
            return Ok((Admission::Existing(record), owner));
        }
        if let Some(compact) = compact {
            crate::compacts::admission_source_in(&tx, scope, compact, snapshot, &meta).await?;
        }
        let scoped = scope_values(scope);
        let total: i64 = tx
            .query_one(
                &format!("SELECT count(*) FROM runs WHERE {SCOPE} AND status='queued'"),
                &parameters(&scoped),
            )
            .await
            .map_err(unavailable)?
            .try_get(0)
            .map_err(unavailable)?;
        let mut session = scoped.clone();
        session.extend([meta.key.0.clone(), meta.lifetime.0.clone()]);
        let row=tx.query_one(&format!("SELECT count(*) FILTER(WHERE status='queued'),count(*) FILTER(WHERE status IN ('queued','running','waiting_interaction')) FROM runs WHERE {SCOPE} AND session_id=$5 AND lifetime=$6"),&parameters(&session)).await.map_err(unavailable)?;
        if total >= 1024
            || row.try_get::<_, i64>(0).map_err(unavailable)? >= 64
            || (admission.mode == AdmissionMode::RejectIfBusy
                && row.try_get::<_, i64>(1).map_err(unavailable)? != 0)
        {
            return Err(CoordinationError::Conflict.into());
        }
        let row=tx.query_one("INSERT INTO scope_run_counters VALUES($1,$2,$3,$4,1) ON CONFLICT(organization_id,tenant_id,project_id,agent_uid) DO UPDATE SET next_id=scope_run_counters.next_id+1 RETURNING next_id",&parameters(&scoped)).await.map_err(unavailable)?;
        let id = unsigned(row.try_get(0).map_err(unavailable)?)?;
        let run = RunId(format!("run-{id}"));
        let turn = TurnId(format!("turn-{id}"));
        let row=tx.query_one(&format!("UPDATE session_heads SET run_generation=run_generation+1 WHERE {SCOPE} AND session_id=$5 AND lifetime=$6 RETURNING run_generation"),&parameters(&session)).await.map_err(unavailable)?;
        let generation = RunGeneration(unsigned(row.try_get(0).map_err(unavailable)?)?);
        let stored_request = serde_json::json!([meta.lifetime, admission.request_id]).to_string();
        let at = now(&tx).await?;
        let mut values = scoped.clone();
        values.extend([
            run.0.clone(),
            meta.key.0.clone(),
            meta.lifetime.0.clone(),
            generation.0.to_string(),
            stored_request,
            wire,
            admission.input.clone(),
            at.to_string(),
        ]);
        tx.execute("INSERT INTO runs(organization_id,tenant_id,project_id,agent_uid,id,session_id,lifetime,generation,request_id_json,request_id_wire_json,status,input,created_at_ms,updated_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8::text::bigint,$9,$10,'queued',$11,$12::text::bigint,$12::text::bigint)",&parameters(&values)).await.map_err(unavailable)?;
        let mut values = scoped.clone();
        values.extend([turn.0.clone(), run.0.clone()]);
        tx.execute(
            "INSERT INTO turns VALUES($1,$2,$3,$4,$5,$6,'queued')",
            &parameters(&values),
        )
        .await
        .map_err(unavailable)?;
        let mut values = scoped.clone();
        values.extend([run.0.clone(), frozen.clone()]);
        tx.execute(
            "INSERT INTO run_snapshots VALUES($1,$2,$3,$4,$5,$6)",
            &parameters(&values),
        )
        .await
        .map_err(unavailable)?;
        if let Some(route) = &snapshot.route {
            let mut values = scoped.clone();
            values.extend([run.0.clone(), encode(route, 1048576)?]);
            tx.execute(
                "INSERT INTO run_routes VALUES($1,$2,$3,$4,$5,$6)",
                &parameters(&values),
            )
            .await
            .map_err(unavailable)?;
        }
        let owner = ExactOwner {
            session_key: meta.key.clone(),
            session_lifetime_id: meta.lifetime.clone(),
            run_id: run.clone(),
            run_generation: generation,
            turn_id: turn,
        };
        let env = EnterpriseOwnerEnvelope {
            scope: scope.clone(),
            owner: owner.clone(),
        };
        let authorization = AuthorizationSnapshot {
            envelope: env.clone(),
            context: checked.context.clone(),
            roles: identity.roles,
            policy: identity.policy,
            policy_digest: identity.policy_digest,
            attributes: checked.attributes,
            action: checked.action,
            run_snapshot_digest: format!("{:x}", Sha256::digest(frozen.as_bytes())),
        };
        let raw = encode(&authorization, 65536)?;
        let digest = format!("{:x}", Sha256::digest(raw.as_bytes()));
        let mut values = scoped.clone();
        values.extend([
            run.0.clone(),
            authorization.context.principal.subject_id.as_str().into(),
            authorization.context.principal.revocation_epoch.to_string(),
            raw,
            digest,
        ]);
        tx.execute(
            "INSERT INTO run_authorizations VALUES($1,$2,$3,$4,$5,$6,$7::text::bigint,$8,$9)",
            &parameters(&values),
        )
        .await
        .map_err(unavailable)?;
        if let Some((parent, operation)) = continuation {
            let mut link = scoped.clone();
            link.extend([
                parent.owner.run_id.0.clone(),
                run.0.clone(),
                operation.into(),
            ]);
            tx.execute(
                "INSERT INTO hook_continuations VALUES($1,$2,$3,$4,$5,$6,$7)",
                &parameters(&link),
            )
            .await
            .map_err(unavailable)?;
        }
        if let Some((parent, request)) = delegation {
            let prepared = prepared_child.as_ref().ok_or(CoordinationError::Corrupt)?;
            crate::delegations::insert_in(&tx, parent, &env, request, prepared, at).await?;
        }
        if let Some(plan) = &admission.plan_execution {
            crate::plans::admit_execution(&tx, &env, &meta, plan, &plan_input).await?;
        }
        if let Some(compact) = compact {
            crate::compacts::start_in(&tx, &env, compact, snapshot, &meta).await?;
        } else {
            append_in(
                &tx,
                &env,
                "user_message",
                &serde_json::json!({"content":admission.input}),
            )
            .await?;
            let mut values = scoped;
            values.extend([meta.key.0.clone(), run.0.clone(), admission.input.clone()]);
            tx.execute("INSERT INTO queued_messages(organization_id,tenant_id,project_id,agent_uid,session_id,run_id,message,status) VALUES($1,$2,$3,$4,$5,$6,$7,'queued')",&parameters(&values)).await.map_err(unavailable)?;
        }
        crate::billing::reserve_in(&tx,&env,snapshot,continuation.is_none()&&delegation.is_none()).await?;
        crate::ledgers::reserve_in(
            &tx,
            scope,
            &run,
            &format!("admission:{}", run.0),
            "runs",
            1,
            *scope == TenantContext::local(),
        )
        .await?;
        crate::ledgers::append_audit_in(&tx,&AuditCommand{scope:scope.clone(),operation_id:format!("run-admit:{}",run.0),subject_id:authorization.context.principal.subject_id,action:"run.admit".into(),resource:serde_json::json!({"run_id":run,"session_key":meta.key,"lifetime":meta.lifetime}),outcome:if compact.is_some(){"running".into()}else{"queued".into()}},at).await?;
        seal_in(&tx, &env).await?;
        if let Some((parent, _)) = delegation {
            seal_in(&tx, parent).await?;
            Self::before_commit(
                &tx,
                delegation_proof
                    .as_ref()
                    .ok_or(CoordinationError::Corrupt)?,
            )
            .await?;
        }
        Self::before_commit(&tx, &grant).await?;
        if let Some(lease)=&admission.workflow_node {crate::workflow::native_in(&tx,request,admission,snapshot,lease,Some(&run)).await?;}
        let record = record_in(&tx, scope, &run).await?;
        tx.commit().await.map_err(unavailable)?;
        proofs.insert(key_id, grant);
        Ok((Admission::New(record), owner))
    }
}
#[async_trait::async_trait]
impl RunExecutionRepository for PgCanonicalStore {
    async fn admit_workload_run(
        &self,
        admission: &RunAdmission,
        snapshot: &RunSnapshot,
        authorization: &AuthorizationRequest,
        verifier: Arc<dyn AuthorizationVerifier>,
    ) -> Result<(Admission, ExactOwner), RepositoryError> {
        self.admit_in(
            admission,
            snapshot,
            authorization,
            verifier,
            AdmissionPurpose::Chat { workload: true },
        )
        .await
    }
    async fn admit_run_authorized(
        &self,
        admission: &RunAdmission,
        snapshot: &RunSnapshot,
        authorization: &AuthorizationRequest,
        verifier: Arc<dyn AuthorizationVerifier>,
    ) -> Result<(Admission, ExactOwner), RepositoryError> {
        self.admit_in(
            admission,
            snapshot,
            authorization,
            verifier,
            AdmissionPurpose::Chat { workload: false },
        )
        .await
    }
    async fn start_queued_run(
        &self,
        env: &EnterpriseOwnerEnvelope,
    ) -> Result<bool, RepositoryError> {
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
        let meta = metadata_in(&tx, &env.scope, &env.owner.session_key, true)
            .await?
            .ok_or(CoordinationError::Forbidden)?;
        let mut values = scope_values(&env.scope);
        values.extend([meta.key.0.clone(), meta.lifetime.0.clone()]);
        let candidate=tx.query_opt("SELECT q.run_id FROM queued_messages q JOIN runs r ON (r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id)=(q.organization_id,q.tenant_id,q.project_id,q.agent_uid,q.run_id) WHERE q.organization_id=$1 AND q.tenant_id=$2 AND q.project_id=$3 AND q.agent_uid=$4 AND q.session_id=$5 AND r.lifetime=$6 AND q.status='queued' ORDER BY q.id LIMIT 1",&parameters(&values)).await.map_err(unavailable)?;
        let busy:bool=tx.query_one(&format!("SELECT EXISTS(SELECT 1 FROM runs WHERE {SCOPE} AND session_id=$5 AND lifetime=$6 AND status IN ('running','waiting_interaction'))"),&parameters(&values)).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
        if busy
            || candidate.as_ref().is_none_or(|row| {
                row.try_get::<_, String>(0).ok().as_deref() != Some(&env.owner.run_id.0)
            })
        {
            Self::before_commit(&tx, &proof).await?;
            tx.commit().await.map_err(unavailable)?;
            return Ok(false);
        }
        let mut update = scope_values(&env.scope);
        update.extend([env.owner.run_id.0.clone(), now(&tx).await?.to_string()]);
        let row=tx.query_opt(&format!("UPDATE runs SET status='running',updated_at_ms=$6::text::bigint WHERE {SCOPE} AND id=$5 AND status='queued' RETURNING input,created_at_ms"),&parameters(&update)).await.map_err(unavailable)?;
        let Some(row) = row else {
            Self::before_commit(&tx, &proof).await?;
            tx.commit().await.map_err(unavailable)?;
            return Ok(false);
        };
        let input: String = row.try_get(0).map_err(unavailable)?;
        insert_batch(
            &tx,
            &env.scope,
            &meta,
            &format!("{}:user-input", env.owner.run_id.0),
            Some(&env.owner.run_id),
            &[Message::text(Role::User, input)],
        )
        .await?;
        for table in ["turns", "queued_messages"] {
            tx.execute(
                &format!("UPDATE {table} SET status='running' WHERE {SCOPE} AND run_id=$5"),
                &parameters(&update[..5]),
            )
            .await
            .map_err(unavailable)?;
        }
        let delegation = update.clone();
        tx.execute(&format!("UPDATE delegations SET status='running',started_at_ms=$6::text::bigint WHERE {SCOPE} AND child_run_id=$5"),&parameters(&delegation)).await.map_err(unavailable)?;
        append_in(&tx,env,"run_started",&serde_json::json!({"queue_wait_ms":now(&tx).await?.saturating_sub(row.try_get::<_,i64>(1).map_err(unavailable)?)})).await?;
        seal_in(&tx, env).await?;
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(true)
    }
    async fn cancel_queued_run(
        &self,
        env: &EnterpriseOwnerEnvelope,
    ) -> Result<bool, RepositoryError> {
        self.commit_terminal(
            env,
            &TurnCommit {
                owner: env.owner.clone(),
                status: RunStatus::Cancelled,
                content: None,
                error: None,
            },
            true,
        )
        .await
        .map(|result| result.is_some())
    }
    async fn commit_run_turn(
        &self,
        env: &EnterpriseOwnerEnvelope,
        commit: &TurnCommit,
    ) -> Result<RunRecord, RepositoryError> {
        self.commit_terminal(env, commit, false)
            .await?
            .ok_or_else(|| CoordinationError::Conflict.into())
    }
}
impl PgCanonicalStore {
    async fn commit_terminal(
        &self,
        env: &EnterpriseOwnerEnvelope,
        commit: &TurnCommit,
        queued_only: bool,
    ) -> Result<Option<RunRecord>, RepositoryError> {
        if !commit.status.terminal()
            || commit.owner != env.owner
            || commit.content.as_ref().is_some_and(|v| v.len() > 524288)
            || commit.error.as_ref().is_some_and(|(_, v)| v.len() > 65536)
        {
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
        let authorization = Self::fence_envelope_in(&tx, &proof, env).await?;
        let result = commit_terminal_in(&tx, env, commit, queued_only, authorization).await?;
        if result.is_some() {
            seal_in(&tx, env).await?;
        }
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
}

/// 原终态事务逻辑；调用者完成同事务的控制变更后再封存事件并提交。
pub(crate) async fn commit_terminal_in(
    tx: &tokio_postgres::Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    commit: &TurnCommit,
    queued_only: bool,
    authorization: AuthorizationSnapshot,
) -> Result<Option<RunRecord>, RepositoryError> {
    let current = record_in(tx, &env.scope, &env.owner.run_id).await?;
    if current.kind == RunKind::Compact {
        return Err(CoordinationError::Conflict.into());
    }
    if queued_only && current.status != RunStatus::Queued {
        return Ok(None);
    }
    if current.status.terminal() {
        if current.status != commit.status
            || current.content != commit.content
            || current.error_code != commit.error.as_ref().map(|v| v.0)
            || current.error_message.as_ref() != commit.error.as_ref().map(|v| &v.1)
        {
            return Err(CoordinationError::Conflict.into());
        }
        return Ok(Some(current));
    }
    let mut values = scope_values(&env.scope);
    values.push(env.owner.run_id.0.clone());
    let unknown=tx.query_one(&format!("SELECT EXISTS(SELECT 1 FROM tool_executions WHERE {SCOPE} AND run_id=$5 AND (status<>'terminal' OR outcome='outcome_unknown')),EXISTS(SELECT 1 FROM provider_attempts WHERE {SCOPE} AND run_id=$5 AND status IN ('started','outcome_unknown','outcomeunknown')),EXISTS(SELECT 1 FROM tool_executions WHERE {SCOPE} AND run_id=$5 AND effect<>'read' AND started_at_ms IS NOT NULL AND status IN ('running','terminal'))"),&parameters(&values)).await.map_err(unavailable)?;
    let status = normalize_run_terminal_status(
        commit.status,
        unknown.try_get(0).map_err(unavailable)?,
        unknown.try_get(1).map_err(unavailable)?,
        unknown.try_get(2).map_err(unavailable)?,
    );
    let content = if status == RunStatus::Completed {
        commit.content.clone()
    } else {
        None
    };
    let error = if status == RunStatus::UnknownAfterRestart {
        Some((-32002, "执行结果未知，禁止自动重放".to_owned()))
    } else {
        commit.error.clone()
    };
    let meta = metadata_in(tx, &env.scope, &env.owner.session_key, true)
        .await?
        .ok_or(CoordinationError::Forbidden)?;
    let batches=tx.query(&format!("SELECT round,CASE WHEN octet_length(calls_json)<=1048576 THEN calls_json END FROM tool_batches WHERE {SCOPE} AND run_id=$5 AND state<>'closed' ORDER BY round LIMIT 1025"),&parameters(&values)).await.map_err(unavailable)?;
    if batches.len() > 1024 {
        return Err(CoordinationError::Corrupt.into());
    }
    for batch in batches {
        let round: i64 = batch.try_get(0).map_err(unavailable)?;
        let calls: Vec<ToolCall> =
            serde_json::from_str(&field(&batch, 1)?).map_err(|_| CoordinationError::Corrupt)?;
        if calls.is_empty() || calls.len() > 256 {
            return Err(CoordinationError::Corrupt.into());
        }
        let mut messages = vec![Message::assistant_tool_calls(calls.clone())];
        for call in &calls {
            let mut lookup = values.clone();
            lookup.extend([round.to_string(), call.id.clone()]);
            let row=tx.query_opt(&format!("SELECT status,receipt_json IS NULL,CASE WHEN octet_length(receipt_json)<=1048576 THEN receipt_json END FROM tool_executions WHERE {SCOPE} AND run_id=$5 AND round=$6::text::bigint AND call_id=$7"),&parameters(&lookup)).await.map_err(unavailable)?.ok_or(CoordinationError::Corrupt)?;
            let tool_state: String = row.try_get(0).map_err(unavailable)?;
            let output = if tool_state == "terminal" {
                let receipt: serde_json::Value =
                    if row.try_get::<_, bool>(1).map_err(unavailable)? {
                        serde_json::Value::Null
                    } else {
                        serde_json::from_str(&field(&row, 2)?)
                            .map_err(|_| CoordinationError::Corrupt)?
                    };
                receipt
                    .get("output")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("[outcome_unknown] 结果回执不含可恢复输出")
                    .to_owned()
            } else if tool_state == "prepared" {
                "[not_executed] 工具未启动".into()
            } else {
                "[outcome_unknown] 副作用结果未知，禁止自动重放".into()
            };
            messages.push(Message::tool_result(call, output));
            tx.execute(&format!("UPDATE tool_executions SET status='terminal',outcome=CASE WHEN status='prepared' THEN 'not_executed' ELSE 'outcome_unknown' END WHERE {SCOPE} AND run_id=$5 AND round=$6::text::bigint AND call_id=$7 AND status<>'terminal'"),&parameters(&lookup)).await.map_err(unavailable)?;
        }
        insert_batch(
            tx,
            &env.scope,
            &meta,
            &format!("{}:tool-batch:{round}", env.owner.run_id.0),
            Some(&env.owner.run_id),
            &messages,
        )
        .await?;
        let mut update = values.clone();
        update.push(round.to_string());
        tx.execute(&format!("UPDATE tool_batches SET state='closed' WHERE {SCOPE} AND run_id=$5 AND round=$6::text::bigint"),&parameters(&update)).await.map_err(unavailable)?;
    }
    let attempts=tx.query(&format!("SELECT attempt_id,CASE WHEN octet_length(data_json)<=1048576 THEN data_json END,status FROM provider_attempts WHERE {SCOPE} AND run_id=$5 ORDER BY round,candidate_index,attempt_id LIMIT 1001"),&parameters(&values)).await.map_err(unavailable)?;
    if attempts.len() > 1000 {
        return Err(CoordinationError::Corrupt.into());
    }
    let mut usage = ProviderUsage::default();
    let mut provider_error_kind = None;
    for row in attempts {
        let mut attempt: ProviderAttempt =
            serde_json::from_str(&field(&row, 1)?).map_err(|_| CoordinationError::Corrupt)?;
        if attempt.run_id != env.owner.run_id
            || attempt.attempt_id != row.try_get::<_, String>(0).map_err(unavailable)?
        {
            return Err(CoordinationError::Corrupt.into());
        }
        let saved_status = serde_json::to_value(attempt.status)
            .map_err(|_| CoordinationError::Corrupt)?
            .as_str()
            .ok_or(CoordinationError::Corrupt)?
            .to_owned();
        if saved_status != row.try_get::<_, String>(2).map_err(unavailable)? {
            return Err(CoordinationError::Corrupt.into());
        }
        if status == RunStatus::UnknownAfterRestart && attempt.status == AttemptStatus::Started {
            attempt.status = AttemptStatus::OutcomeUnknown;
            attempt.finished_at_ms = Some(now(tx).await?);
            crate::billing::provider_in(tx,env,&attempt).await?;
            let mut update = scope_values(&env.scope);
            update.extend([attempt.attempt_id.clone(), encode(&attempt, 1048576)?]);
            tx.execute(&format!("UPDATE provider_attempts SET status='outcome_unknown',data_json=$6 WHERE {SCOPE} AND attempt_id=$5 AND status='started'"),&parameters(&update)).await.map_err(unavailable)?;
        }
        if let Some(value) = &attempt.usage {
            usage.add_attempt(value);
        }
        provider_error_kind = attempt.error_kind;
    }
    if let Some(content) = &content {
        let staged=tx.query_opt(&format!("SELECT CASE WHEN octet_length(message_json)<=4194304 THEN message_json END FROM turn_outputs WHERE {SCOPE} AND run_id=$5"),&parameters(&values)).await.map_err(unavailable)?;
        let message = if let Some(row) = staged {
            serde_json::from_str::<Message>(&field(&row, 0)?)
                .map_err(|_| CoordinationError::Corrupt)?
        } else {
            Message::text(Role::Assistant, content)
        };
        if message.content.as_deref() != Some(content.as_str())
            || message.role != Role::Assistant
            || !message.tool_calls.is_empty()
            || message.tool_call_id.is_some()
        {
            return Err(CoordinationError::Conflict.into());
        }
        let last=tx.query_opt(&format!("SELECT CASE WHEN octet_length(messages_json)<=16777216 THEN messages_json END FROM transcript_batches WHERE {SCOPE} AND run_id=$5 ORDER BY end_seq DESC,start_seq DESC LIMIT 1"),&parameters(&values)).await.map_err(unavailable)?;
        let already = last
            .map(|row| {
                serde_json::from_str::<Vec<Message>>(&field(&row, 0)?)
                    .map_err(|_| CoordinationError::Corrupt)
            })
            .transpose()?
            .is_some_and(|batch| batch.last() == Some(&message));
        if !already {
            insert_batch(
                tx,
                &env.scope,
                &meta,
                &format!("{}:assistant-terminal", env.owner.run_id.0),
                Some(&env.owner.run_id),
                &[message],
            )
            .await?;
        }
    }
    let mut plan_revision = None;
    if status==RunStatus::Completed&&let Some(stage)=tx.query_opt(&format!("SELECT base_revision,revision,CASE WHEN octet_length(data_json)<=1048576 THEN data_json END FROM turn_plan_stages WHERE {SCOPE} AND run_id=$5"),&parameters(&values)).await.map_err(unavailable)?{
        let mut plan=scope_values(&env.scope);plan.push(env.owner.session_lifetime_id.0.clone());let old=tx.query_opt(&format!("SELECT revision FROM session_plans WHERE {SCOPE} AND lifetime=$5"),&parameters(&plan)).await.map_err(unavailable)?;
        let base=old.map(|row|row.try_get::<_,i64>(0).map_err(unavailable)).transpose()?.unwrap_or(0);if base!=stage.try_get::<_,i64>(0).map_err(unavailable)?{return Err(CoordinationError::Conflict.into());}
        let document:PlanDocument=serde_json::from_str(&field(&stage,2)?).map_err(|_|CoordinationError::Corrupt)?;let revision=unsigned(stage.try_get(1).map_err(unavailable)?)?;if document.revision!=revision{return Err(CoordinationError::Corrupt.into());}crate::plans::write_document(tx,&env.scope,&env.owner.session_lifetime_id,&document).await?;plan_revision=Some(revision);
    }
    let is_plan:bool=tx.query_one(&format!("SELECT EXISTS(SELECT 1 FROM plan_decisions WHERE {SCOPE} AND run_id=$5 AND decision='execute')"),&parameters(&values)).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
    if is_plan && status != RunStatus::Completed {
        let mut doc =
            crate::plans::document_in(tx, &env.scope, &env.owner.session_lifetime_id).await?;
        if !(status == RunStatus::Cancelled && doc.review == PlanReviewStatus::PendingExecution) {
            doc.review = PlanReviewStatus::Blocked;
            crate::plans::write_document(tx, &env.scope, &env.owner.session_lifetime_id, &doc)
                .await?;
        }
    }
    let mut commit_values = values.clone();
    commit_values.extend([
        encode(&env.owner, 16384)?,
        encode(&usage, 16384)?,
        status.as_str().into(),
    ]);
    let revision = plan_revision.map(|v| v as i64);
    let mut params = parameters(&commit_values);
    params.push(&revision);
    tx.execute(
        "INSERT INTO turn_commits VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
        &params,
    )
    .await
    .map_err(unavailable)?;
    let mut update = values.clone();
    update.push(status.as_str().into());
    let error_code = error.as_ref().map(|v| v.0);
    let error_message = error.as_ref().map(|v| v.1.as_str());
    let at = now(tx).await?;
    let mut params = parameters(&update);
    params.extend([
        &content as &(dyn tokio_postgres::types::ToSql + Sync),
        &error_code,
        &error_message,
        &at,
    ]);
    tx.execute(&format!("UPDATE runs SET status=$6,content=$7,error_code=$8,error_message=$9,updated_at_ms=$10 WHERE {SCOPE} AND id=$5"),&params).await.map_err(unavailable)?;
    for table in ["turns", "queued_messages"] {
        tx.execute(
            &format!("UPDATE {table} SET status=$6 WHERE {SCOPE} AND run_id=$5"),
            &parameters(&update),
        )
        .await
        .map_err(unavailable)?;
    }
    tx.execute(&format!("UPDATE interactions SET status='orphaned',revision=revision+1 WHERE {SCOPE} AND run_id=$5 AND status='pending'"),&parameters(&values)).await.map_err(unavailable)?;
    if let Some(content) = &content {
        append_in(
            tx,
            env,
            "assistant_content",
            &serde_json::json!({"content":content}),
        )
        .await?;
    }
    append_in(tx,env,"terminal",&serde_json::json!({"status":status,"error_code":error_code,"error_message":error_message,"provider_error_kind":if status==RunStatus::Failed{provider_error_kind}else{None}})).await?;
    let result = record_in(tx, &env.scope, &env.owner.run_id).await?;
    let terminal = encode(&result, 1048576)?;
    let mut delegate = values.clone();
    delegate.extend([status.as_str().into(), at.to_string(), terminal]);
    tx.execute(&format!("UPDATE delegations SET status=$6,finished_at_ms=$7::text::bigint,terminal_json=$8 WHERE {SCOPE} AND child_run_id=$5"),&parameters(&delegate)).await.map_err(unavailable)?;
    crate::ledgers::terminal_in(tx, env, status).await?;
    crate::ledgers::append_audit_in(tx,&AuditCommand{scope:env.scope.clone(),operation_id:format!("run-terminal:{}",env.owner.run_id.0),subject_id:authorization.context.principal.subject_id,action:"run.terminal".into(),resource:serde_json::json!({"run_id":env.owner.run_id,"lifetime":env.owner.session_lifetime_id,"generation":env.owner.run_generation}),outcome:status.as_str().into()},at).await?;
    Ok(Some(result))
}
