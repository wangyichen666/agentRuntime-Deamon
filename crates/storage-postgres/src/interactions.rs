//! 审批原 interaction、回答 CAS 和事件提交；数据库提交先于唤醒执行体。
use crate::canonical::{PgCanonicalStore, field, set_scope, unsigned};
use crate::events::{append_in, seal_in};
use crate::facts::stamp_in;
use crate::identities::exact_owner_in;
use crate::sessions::{encode, metadata_in};
use crate::{SCOPE, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::RunInteractionRepository;
use tokio_postgres::Transaction;
fn valid_id(id: &InteractionId) -> Result<(), CoordinationError> {
    if id.0.is_empty() || id.0.len() > 1024 {
        Err(CoordinationError::InvalidRequest)
    } else {
        Ok(())
    }
}
pub(crate) async fn interaction_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    id: &InteractionId,
) -> Result<Option<InteractionRecord>, CoordinationError> {
    let mut values = scope_values(scope);
    values.push(id.0.clone());
    let header=db.query_opt(&format!("SELECT CASE WHEN octet_length(run_id) BETWEEN 1 AND 1024 THEN run_id END,CASE WHEN octet_length(lifetime) BETWEEN 1 AND 128 THEN lifetime END,generation FROM interactions WHERE {SCOPE} AND id=$5"),&parameters(&values)).await.map_err(unavailable)?;
    let Some(header) = header else {
        return Ok(None);
    };
    let run = RunId(field(&header, 0)?);
    let owner = exact_owner_in(db, scope, &run).await?;
    if field(&header, 1)? != owner.session_lifetime_id.0
        || unsigned(header.try_get(2).map_err(unavailable)?)? != owner.run_generation.0
    {
        return Err(CoordinationError::Corrupt);
    }
    let row=db.query_one(&format!("SELECT CASE WHEN octet_length(kind) BETWEEN 1 AND 64 THEN kind END,CASE WHEN octet_length(status)<=32 THEN status END,revision,CASE WHEN octet_length(prompt)<=16384 THEN prompt END,CASE WHEN octet_length(payload_json)<=65536 THEN payload_json END,response_json IS NULL,CASE WHEN octet_length(response_json)<=65536 THEN response_json END FROM interactions WHERE {SCOPE} AND id=$5"),&parameters(&values)).await.map_err(unavailable)?;
    let record = InteractionRecord {
        interaction_id: id.clone(),
        session_id: owner.session_key,
        owner_run_id: run,
        kind: field(&row, 0)?,
        status: field(&row, 1)?,
        revision: row.try_get(2).map_err(unavailable)?,
        prompt: field(&row, 3)?,
        payload: serde_json::from_str(&field(&row, 4)?).map_err(|_| CoordinationError::Corrupt)?,
        response: if row.try_get::<_, bool>(5).map_err(unavailable)? {
            None
        } else {
            Some(serde_json::from_str(&field(&row, 6)?).map_err(|_| CoordinationError::Corrupt)?)
        },
    };
    record.validate_saved()?;
    Ok(Some(record))
}
pub(crate) async fn publish_approval_in(
    db: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    data: &serde_json::Value,
) -> Result<(), CoordinationError> {
    let approval = data
        .get("approval")
        .filter(|value| value.is_object())
        .ok_or(CoordinationError::InvalidRequest)?;
    let id = InteractionId(
        approval
            .get("id")
            .and_then(serde_json::Value::as_str)
            .ok_or(CoordinationError::InvalidRequest)?
            .into(),
    );
    valid_id(&id)?;
    let prompt = approval
        .get("prompt")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if prompt.len() > 16384 {
        return Err(CoordinationError::InvalidRequest);
    }
    let raw = encode(approval, 65536)?;
    let mut values = scope_values(&env.scope);
    values.extend([
        id.0.clone(),
        env.owner.run_id.0.clone(),
        env.owner.session_lifetime_id.0.clone(),
        env.owner.run_generation.0.to_string(),
        prompt.into(),
        raw,
    ]);
    db.execute("INSERT INTO interactions(organization_id,tenant_id,project_id,agent_uid,id,run_id,lifetime,generation,kind,status,prompt,payload_json) VALUES($1,$2,$3,$4,$5,$6,$7,$8::text::bigint,'approval','pending',$9,$10)",&parameters(&values)).await.map_err(unavailable)?;
    let mut values = scope_values(&env.scope);
    values.push(env.owner.run_id.0.clone());
    let changed=db.execute(&format!("UPDATE runs SET status='waiting_interaction' WHERE {SCOPE} AND id=$5 AND status IN ('running','waiting_interaction')"),&parameters(&values)).await.map_err(unavailable)?;
    if changed != 1 {
        return Err(CoordinationError::Conflict);
    }
    Ok(())
}
pub(crate) async fn list_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    meta: &SessionMetadata,
    pending: bool,
) -> Result<Vec<InteractionRecord>, CoordinationError> {
    let mut values = scope_values(scope);
    values.extend([
        meta.key.0.clone(),
        meta.lifetime.0.clone(),
        pending.to_string(),
    ]);
    let size=db.query_one("SELECT count(*)::bigint,COALESCE(sum(octet_length(i.id)+octet_length(i.prompt)+octet_length(i.payload_json)+COALESCE(octet_length(i.response_json),0)),0)::bigint FROM interactions i JOIN runs r ON (r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id)=(i.organization_id,i.tenant_id,i.project_id,i.agent_uid,i.run_id) WHERE i.organization_id=$1 AND i.tenant_id=$2 AND i.project_id=$3 AND i.agent_uid=$4 AND r.session_id=$5 AND r.lifetime=$6 AND ($7::text::boolean=FALSE OR i.status='pending')",&parameters(&values)).await.map_err(unavailable)?;
    if size.try_get::<_, i64>(0).map_err(unavailable)? > if pending { 256 } else { 1000 }
        || size.try_get::<_, i64>(1).map_err(unavailable)? > 1024 * 1024 - 16384
    {
        return Err(CoordinationError::InvalidRequest);
    }
    let rows=db.query("SELECT CASE WHEN octet_length(i.id) BETWEEN 1 AND 1024 THEN i.id END FROM interactions i JOIN runs r ON (r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id)=(i.organization_id,i.tenant_id,i.project_id,i.agent_uid,i.run_id) WHERE i.organization_id=$1 AND i.tenant_id=$2 AND i.project_id=$3 AND i.agent_uid=$4 AND r.session_id=$5 AND r.lifetime=$6 AND ($7::text::boolean=FALSE OR i.status='pending') ORDER BY r.storage_row_id,i.id LIMIT 1000",&parameters(&values)).await.map_err(unavailable)?;
    let mut output = Vec::new();
    for row in rows {
        output.push(
            interaction_in(db, scope, &InteractionId(field(&row, 0)?))
                .await?
                .ok_or(CoordinationError::Corrupt)?,
        );
    }
    encode(&output, 1024 * 1024 - 16384)?;
    Ok(output)
}
#[async_trait::async_trait]
impl RunInteractionRepository for PgCanonicalStore {
    async fn read_interaction_record(
        &self,
        scope: &TenantContext,
        id: &InteractionId,
    ) -> Result<Option<InteractionRecord>, RepositoryError> {
        valid_id(id)?;
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let record = interaction_in(&tx, scope, id).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(record)
    }
    async fn read_interaction_list(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
    ) -> Result<Vec<InteractionRecord>, RepositoryError> {
        if key.0.is_empty() || key.0.len() > 1024 {
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
        let meta = metadata_in(&tx, scope, key, false)
            .await?
            .ok_or(CoordinationError::Forbidden)?;
        let result = if meta.deleted {
            Vec::new()
        } else {
            list_in(&tx, scope, &meta, false).await?
        };
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
    async fn respond_run_interaction(
        &self,
        env: &EnterpriseOwnerEnvelope,
        id: &InteractionId,
        revision: i64,
        approved: bool,
    ) -> Result<InteractionRecord, RepositoryError> {
        self.respond_with_authorization(env, id, revision, approved, None)
            .await
    }
    async fn respond_interaction_authorized(
        &self,
        request: &AuthorizationRequest,
        env: &EnterpriseOwnerEnvelope,
        id: &InteractionId,
        revision: i64,
        approved: bool,
        verifier: std::sync::Arc<dyn AuthorizationVerifier>,
    ) -> Result<InteractionRecord, RepositoryError> {
        self.respond_with_authorization(
            env,
            id,
            revision,
            approved,
            Some((request, verifier.as_ref())),
        )
        .await
    }
    async fn read_interaction_stamp(
        &self,
        scope: &TenantContext,
        id: &InteractionId,
    ) -> Result<Option<ViewStamp>, RepositoryError> {
        valid_id(id)?;
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let Some(record) = interaction_in(&tx, scope, id).await? else {
            tx.commit().await.map_err(unavailable)?;
            return Ok(None);
        };
        let owner = exact_owner_in(&tx, scope, &record.owner_run_id).await?;
        let mut values = scope_values(scope);
        values.push(record.owner_run_id.0.clone());
        let rows=tx.query(&format!("SELECT seq,CASE WHEN octet_length(data_json)<=65536 THEN data_json END FROM events WHERE {SCOPE} AND run_id=$5 AND event='interaction_resolved' ORDER BY seq DESC LIMIT 1001"),&parameters(&values)).await.map_err(unavailable)?;
        if rows.len() > 1000 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let mut result = None;
        let mut bytes = 0;
        for row in rows {
            let raw = field(&row, 1)?;
            bytes += raw.len();
            if bytes > 1024 * 1024 {
                return Err(CoordinationError::InvalidRequest.into());
            }
            let data: serde_json::Value =
                serde_json::from_str(&raw).map_err(|_| CoordinationError::Corrupt)?;
            if data
                .get("interaction_id")
                .and_then(serde_json::Value::as_str)
                == Some(id.0.as_str())
            {
                result = stamp_in(
                    &tx,
                    scope,
                    &owner,
                    EventSeq(unsigned(row.try_get(0).map_err(unavailable)?)?),
                )
                .await?;
                break;
            }
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
}

impl PgCanonicalStore {
    async fn respond_with_authorization(
        &self,
        env: &EnterpriseOwnerEnvelope,
        id: &InteractionId,
        revision: i64,
        approved: bool,
        authorization: Option<(&AuthorizationRequest, &dyn AuthorizationVerifier)>,
    ) -> Result<InteractionRecord, RepositoryError> {
        valid_id(id)?;
        if revision < 0 || revision == i64::MAX {
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
        let current = interaction_in(&tx, &env.scope, id)
            .await?
            .ok_or(CoordinationError::Conflict)?;
        if current.owner_run_id != env.owner.run_id || current.session_id != env.owner.session_key {
            return Err(AuthorizationError::Forbidden.into());
        }
        if let Some((request, verifier)) = authorization {
            if request.context.scope != env.scope || request.action != Action::InteractionRespond {
                return Err(AuthorizationError::Forbidden.into());
            }
            let identity = crate::identities::identity_in(
                &tx,
                &env.scope,
                &request.context.principal.subject_id,
            )
            .await?;
            let mut checked = request.clone();
            checked.attributes.timestamp_ms = crate::now(&tx).await?;
            verifier.verify(&checked, &identity)?;
            checked.action = Action::SessionPrompt;
            verifier.verify(&checked, &identity)?;
            let snapshot = crate::identities::authorization_in(&tx, env)
                .await?
                .ok_or(AuthorizationError::Forbidden)?;
            if snapshot.policy.dual_approval
                && snapshot.context.principal.subject_id == request.context.principal.subject_id
            {
                return Err(AuthorizationError::Forbidden.into());
            }
            match vote_in(
                &tx,
                request,
                env,
                id,
                revision,
                approved,
                &current,
                snapshot.policy.dual_approval,
            )
            .await?
            {
                VoteProgress::Duplicate => {
                    Self::before_commit(&tx, &proof).await?;
                    tx.commit().await.map_err(unavailable)?;
                    return Ok(current);
                }
                VoteProgress::Waiting => {
                    let result = interaction_in(&tx, &env.scope, id)
                        .await?
                        .ok_or(CoordinationError::Corrupt)?;
                    Self::before_commit(&tx, &proof).await?;
                    tx.commit().await.map_err(unavailable)?;
                    return Ok(result);
                }
                VoteProgress::Resolve => {}
            }
        }
        let response = serde_json::json!({"approved":approved});
        if current.status != "pending" {
            if matches!(current.status.as_str(), "answered" | "rejected")
                && current.response == Some(response)
            {
                Self::before_commit(&tx, &proof).await?;
                tx.commit().await.map_err(unavailable)?;
                return Ok(current);
            }
            return Err(CoordinationError::Conflict.into());
        }
        if current.revision != revision {
            return Err(CoordinationError::Conflict.into());
        }
        let mut values = scope_values(&env.scope);
        values.push(env.owner.run_id.0.clone());
        let state: String = tx
            .query_one(
                &format!("SELECT status FROM runs WHERE {SCOPE} AND id=$5 FOR UPDATE"),
                &parameters(&values),
            )
            .await
            .map_err(unavailable)?
            .try_get(0)
            .map_err(unavailable)?;
        if state != "waiting_interaction" {
            return Err(CoordinationError::Conflict.into());
        }
        let mut answer = scope_values(&env.scope);
        answer.extend([
            id.0.clone(),
            revision.to_string(),
            if approved {
                "answered".into()
            } else {
                "rejected".into()
            },
            encode(&response, 65536)?,
        ]);
        let changed=tx.execute(&format!("UPDATE interactions SET status=$7,response_json=$8,revision=revision+1 WHERE {SCOPE} AND id=$5 AND revision=$6::text::bigint AND status='pending'"),&parameters(&answer)).await.map_err(unavailable)?;
        if changed != 1 {
            return Err(CoordinationError::Conflict.into());
        }
        let pending:i64=tx.query_one(&format!("SELECT count(*)::bigint FROM interactions WHERE {SCOPE} AND run_id=$5 AND status='pending'"),&parameters(&values)).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
        if pending == 0 {
            tx.execute(&format!("UPDATE runs SET status='running' WHERE {SCOPE} AND id=$5 AND status='waiting_interaction'"),&parameters(&values)).await.map_err(unavailable)?;
        }
        append_in(
            &tx,
            env,
            "interaction_resolved",
            &serde_json::json!({"interaction_id":id,"approved":approved,"revision":revision+1}),
        )
        .await?;
        seal_in(&tx, env).await?;
        Self::before_commit(&tx, &proof).await?;
        let result = interaction_in(&tx, &env.scope, id)
            .await?
            .ok_or(CoordinationError::Corrupt)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
}

enum VoteProgress {
    Duplicate,
    Waiting,
    Resolve,
}
#[allow(clippy::too_many_arguments)]
async fn vote_in(
    db: &Transaction<'_>,
    request: &AuthorizationRequest,
    env: &EnterpriseOwnerEnvelope,
    id: &InteractionId,
    revision: i64,
    approved: bool,
    current: &InteractionRecord,
    dual: bool,
) -> Result<VoteProgress, RepositoryError> {
    let principal = &request.context.principal;
    let raw = encode(&request.context.request_id, 4096)?;
    let mut v = scope_values(&env.scope);
    v.extend([id.0.clone(), principal.subject_id.as_str().into()]);
    let saved=db.query_opt(&format!("SELECT revocation_epoch,expected_revision,approved,request_json FROM interaction_votes WHERE {SCOPE} AND interaction_id=$5 AND subject_id=$6"),&parameters(&v)).await.map_err(unavailable)?;
    if let Some(saved) = saved {
        if unsigned(saved.try_get(0).map_err(unavailable)?)? != principal.revocation_epoch
            || saved.try_get::<_, i64>(1).map_err(unavailable)? != revision
            || saved.try_get::<_, bool>(2).map_err(unavailable)? != approved
            || saved.try_get::<_, String>(3).map_err(unavailable)? != raw
        {
            return Err(CoordinationError::Conflict.into());
        }
        return Ok(VoteProgress::Duplicate);
    }
    if current.status != "pending" || current.revision != revision {
        return Err(CoordinationError::Conflict.into());
    }
    let mut run = scope_values(&env.scope);
    run.push(env.owner.run_id.0.clone());
    let state: String = db
        .query_one(
            &format!("SELECT status FROM runs WHERE {SCOPE} AND id=$5 FOR UPDATE"),
            &parameters(&run),
        )
        .await
        .map_err(unavailable)?
        .try_get(0)
        .map_err(unavailable)?;
    if state != "waiting_interaction" {
        return Err(CoordinationError::Conflict.into());
    }
    let at = crate::now(db).await?;
    v.extend([
        principal.revocation_epoch.to_string(),
        revision.to_string(),
        approved.to_string(),
        raw,
        at.to_string(),
    ]);
    db.execute("INSERT INTO interaction_votes VALUES($1,$2,$3,$4,$5,$6,$7::text::bigint,$8::text::bigint,$9::text::boolean,$10,$11::text::bigint)",&parameters(&v)).await.map_err(unavailable)?;
    crate::ledgers::append_audit_in(db,&AuditCommand {scope:env.scope.clone(),operation_id:vote_operation(env,id,&principal.subject_id)?,subject_id:principal.subject_id.clone(),action:"interaction.vote".into(),resource:serde_json::json!({"owner":env.owner,"interaction_id":id,"revision":revision,"approved":approved}),outcome:if approved {"approved"}else{"rejected"}.into()},at).await?;
    if !approved || !dual {
        return Ok(VoteProgress::Resolve);
    }
    let mut v = scope_values(&env.scope);
    v.push(id.0.clone());
    // 锁定已投票主体，撤销不能在计数后、CAS前偷偷失效。
    let voters=db.query("SELECT p.subject_id FROM principals p JOIN interaction_votes v ON p.organization_id=v.organization_id AND p.tenant_id=v.tenant_id AND p.subject_id=v.subject_id AND p.revocation_epoch=v.revocation_epoch WHERE v.organization_id=$1 AND v.tenant_id=$2 AND v.project_id=$3 AND v.agent_uid=$4 AND v.interaction_id=$5 AND v.approved ORDER BY p.subject_id LIMIT 3 FOR SHARE OF p",&parameters(&v)).await.map_err(unavailable)?;
    if voters.len() >= 2 {
        return Ok(VoteProgress::Resolve);
    }
    v.push(revision.to_string());
    let changed=db.execute(&format!("UPDATE interactions SET revision=revision+1 WHERE {SCOPE} AND id=$5 AND revision=$6::text::bigint AND status='pending'"),&parameters(&v)).await.map_err(unavailable)?;
    if changed != 1 {
        return Err(CoordinationError::Conflict.into());
    }
    seal_in(db, env).await?;
    Ok(VoteProgress::Waiting)
}
fn vote_operation(
    env: &EnterpriseOwnerEnvelope,
    id: &InteractionId,
    subject: &SubjectId,
) -> Result<String, CoordinationError> {
    use sha2::{Digest, Sha256};
    Ok(format!(
        "vote:{:x}",
        Sha256::digest(
            serde_json::to_vec(&(env, id, subject))
                .map_err(|_| CoordinationError::InvalidRequest)?
        )
    ))
}
