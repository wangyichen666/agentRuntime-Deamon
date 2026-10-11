//! 原计划管理command；丢弃排队执行与计划receipt沿用同一个终态事务。
use crate::canonical::{
    PgCanonicalStore, field, grant_in, permanent_life, proof_key, set_scope, unsigned,
};
use crate::executions::commit_terminal_in;
use crate::identities::exact_owner_in;
use crate::plans::{
    decision_receipt_in, document_in, identity_matches, validate_document, write_document,
};
use crate::sessions::{encode, ensure_idle, metadata_in};
use crate::{SCOPE, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::PlanControlRepository;
use tokio_postgres::Transaction;

fn identity_input(key: &SessionKey, life: &SessionLifetimeId) -> Result<(), CoordinationError> {
    if key.0.is_empty() || key.0.len() > 1024 || life.0.is_empty() || life.0.len() > 128 {
        return Err(CoordinationError::InvalidRequest);
    }
    Ok(())
}
fn source_input(source: &str) -> Result<(), CoordinationError> {
    if source.is_empty() || source.len() > 16384 {
        return Err(CoordinationError::InvalidRequest);
    }
    Ok(())
}
pub(crate) async fn budget_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    life: &SessionLifetimeId,
) -> Result<(), CoordinationError> {
    let mut values = scope_values(scope);
    values.push(life.0.clone());
    let invalid:bool=db.query_one(&format!("SELECT EXISTS(SELECT 1 FROM session_plans WHERE {SCOPE} AND lifetime=$5 AND octet_length(data_json)>1048576) OR EXISTS(SELECT 1 FROM plan_versions WHERE {SCOPE} AND lifetime=$5 AND (octet_length(markdown)>1048576 OR octet_length(document_json)>1048576)) OR EXISTS(SELECT 1 FROM plan_decisions WHERE {SCOPE} AND lifetime=$5 AND (octet_length(identity_json)>16384 OR octet_length(receipt_json)>65536))"),&parameters(&values)).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
    if invalid {
        return Err(CoordinationError::Corrupt);
    }
    Ok(())
}

#[async_trait::async_trait]
impl PlanControlRepository for PgCanonicalStore {
    async fn read_session_plan(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
    ) -> Result<PlanReadback, RepositoryError> {
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
            .filter(|meta| !meta.deleted)
            .ok_or(CoordinationError::Forbidden)?;
        permanent_life(
            &tx,
            &SessionWriterLeaseKey {
                scope: scope.clone(),
                session_key: key.clone(),
                session_lifetime_id: meta.lifetime.clone(),
            },
        )
        .await?;
        budget_in(&tx, scope, &meta.lifetime).await?;
        let mut values = scope_values(scope);
        values.push(meta.lifetime.0.clone());
        let row=tx.query_opt(&format!("SELECT revision,CASE WHEN octet_length(data_json)<=1048576 THEN data_json END FROM session_plans WHERE {SCOPE} AND lifetime=$5"),&parameters(&values)).await.map_err(unavailable)?;
        let (plan, legacy_plan, markdown) = if let Some(row) = row {
            let value: serde_json::Value =
                serde_json::from_str(&field(&row, 1)?).map_err(|_| CoordinationError::Corrupt)?;
            if value.get("plan_id").is_some() {
                let doc: PlanDocument =
                    serde_json::from_value(value).map_err(|_| CoordinationError::Corrupt)?;
                validate_document(&doc)?;
                if doc.revision != unsigned(row.try_get(0).map_err(unavailable)?)? {
                    return Err(CoordinationError::Corrupt.into());
                }
                values.push(doc.revision.to_string());
                let version=tx.query_opt(&format!("SELECT CASE WHEN octet_length(markdown)<=1048576 THEN markdown END,CASE WHEN octet_length(document_json)<=1048576 THEN document_json END,plan_id,content_digest FROM plan_versions WHERE {SCOPE} AND lifetime=$5 AND revision=$6::text::bigint"),&parameters(&values)).await.map_err(unavailable)?.ok_or(CoordinationError::Corrupt)?;
                let saved: PlanDocument = serde_json::from_str(&field(&version, 1)?)
                    .map_err(|_| CoordinationError::Corrupt)?;
                let markdown = field(&version, 0)?;
                if saved != doc
                    || markdown != doc.markdown()
                    || version.try_get::<_, String>(2).map_err(unavailable)? != doc.plan_id
                    || version.try_get::<_, String>(3).map_err(unavailable)? != doc.content_digest
                {
                    return Err(CoordinationError::Corrupt.into());
                }
                (Some(doc), None, Some(markdown))
            } else {
                let definition: PlanDefinition = serde_json::from_value(value.clone())
                    .map_err(|_| CoordinationError::Corrupt)?;
                definition
                    .canonical_bytes()
                    .map_err(|_| CoordinationError::Corrupt)?;
                (None, Some(value), None)
            }
        } else {
            (None, None, None)
        };
        let revision: i64 = tx
            .query_one(
                &format!("SELECT revision FROM snapshot_clock WHERE {SCOPE} AND slot=1"),
                &parameters(&scope_values(scope)),
            )
            .await
            .map_err(unavailable)?
            .try_get(0)
            .map_err(unavailable)?;
        let read = PlanReadback {
            session_key: key.clone(),
            lifetime: meta.lifetime,
            snapshot_revision: SnapshotRevision(unsigned(revision)?),
            plan,
            legacy_plan,
            markdown,
        };
        encode(&read, 1024 * 1024 - 16384)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(read)
    }

    async fn discard_session_plan(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        life: &SessionLifetimeId,
        identity: &PlanExecution,
    ) -> Result<PlanDecisionReceipt, RepositoryError> {
        identity_input(key, life)?;
        encode(identity, 16384)?;
        if identity.operation_id.is_empty()
            || identity.operation_id.len() > 128
            || identity.operation_id.chars().any(char::is_control)
            || identity.revision > i64::MAX as u64
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let lease_key = SessionWriterLeaseKey {
            scope: scope.clone(),
            session_key: key.clone(),
            session_lifetime_id: life.clone(),
        };
        let id = proof_key(&lease_key)?;
        let mut proofs = self.proofs.lock().await;
        if !proofs.contains_key(&id) && proofs.len() >= 1024 {
            return Err(CoordinationError::Conflict.into());
        }
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let meta = metadata_in(&tx, scope, key, true)
            .await?
            .filter(|meta| !meta.deleted && meta.lifetime == *life)
            .ok_or(AuthorizationError::StaleOwner)?;
        let proof = if let Some(proof) = proofs.get(&id) {
            Self::fence_in(&tx, proof).await?;
            proof.clone()
        } else {
            grant_in(&tx, &lease_key, &self.writer_owner, 30000).await?
        };
        budget_in(&tx, scope, life).await?;
        if let Some(receipt) =
            decision_receipt_in(&tx, scope, &meta, identity, PlanDecision::Discard, "").await?
        {
            Self::before_commit(&tx, &proof).await?;
            tx.commit().await.map_err(unavailable)?;
            proofs.insert(id, proof);
            return Ok(receipt);
        }
        let mut doc = document_in(&tx, scope, life).await?;
        identity_matches(&doc, identity)?;
        let mut values = scope_values(scope);
        values.extend([key.0.clone(), life.0.clone()]);
        let mut cancelled = None;
        let mut cancelled_envelope = None;
        if doc.review == PlanReviewStatus::PendingExecution {
            let rows=tx.query("SELECT CASE WHEN octet_length(r.id) BETWEEN 1 AND 1024 THEN r.id END,CASE WHEN octet_length(d.identity_json)<=16384 THEN d.identity_json END,CASE WHEN octet_length(d.input)<=65536 THEN d.input END FROM runs r JOIN plan_decisions d ON (d.organization_id,d.tenant_id,d.project_id,d.agent_uid,d.run_id)=(r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id) WHERE r.organization_id=$1 AND r.tenant_id=$2 AND r.project_id=$3 AND r.agent_uid=$4 AND r.session_id=$5 AND r.lifetime=$6 AND r.status='queued' AND d.decision='execute' LIMIT 2",&parameters(&values)).await.map_err(unavailable)?;
            if rows.len() > 1 {
                return Err(CoordinationError::Corrupt.into());
            }
            if let Some(row) = rows.first() {
                let run = RunId(field(row, 0)?);
                let saved: PlanExecution = serde_json::from_str(&field(row, 1)?)
                    .map_err(|_| CoordinationError::Corrupt)?;
                identity_matches(&doc, &saved).map_err(|_| CoordinationError::Corrupt)?;
                let decision = decision_receipt_in(
                    &tx,
                    scope,
                    &meta,
                    &saved,
                    PlanDecision::Execute,
                    &field(row, 2)?,
                )
                .await?
                .ok_or(CoordinationError::Corrupt)?;
                if decision.run_id.as_ref() != Some(&run) {
                    return Err(CoordinationError::Corrupt.into());
                }
                let owner = exact_owner_in(&tx, scope, &run).await?;
                let env = EnterpriseOwnerEnvelope {
                    scope: scope.clone(),
                    owner,
                };
                let auth = Self::fence_envelope_in(&tx, &proof, &env).await?;
                commit_terminal_in(
                    &tx,
                    &env,
                    &TurnCommit {
                        owner: env.owner.clone(),
                        status: RunStatus::Cancelled,
                        content: None,
                        error: None,
                    },
                    true,
                    auth,
                )
                .await?
                .ok_or(CoordinationError::Conflict)?;
                cancelled = Some(run);
                cancelled_envelope = Some(env);
            }
        }
        ensure_idle(&tx, scope, &meta).await?;
        doc.review = PlanReviewStatus::Rejected;
        write_document(&tx, scope, life, &doc).await?;
        let receipt = PlanDecisionReceipt {
            session_key: key.clone(),
            lifetime: life.clone(),
            identity: identity.clone(),
            decision: PlanDecision::Discard,
            run_id: cancelled,
        };
        let mut values = scope_values(scope);
        values.extend([
            identity.operation_id.clone(),
            key.0.clone(),
            life.0.clone(),
            encode(identity, 16384)?,
            encode(&receipt, 65536)?,
        ]);
        tx.execute(
            "INSERT INTO plan_decisions VALUES($1,$2,$3,$4,$5,$6,$7,$8,'discard','',NULL,$9)",
            &parameters(&values),
        )
        .await
        .map_err(unavailable)?;
        if let Some(env) = cancelled_envelope {
            // stamp 必须包含计划拒绝和 decision receipt 提交后的最终 revision。
            crate::events::seal_in(&tx, &env).await?;
        }
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        proofs.insert(id, proof);
        Ok(receipt)
    }

    async fn read_legacy_plan_import(
        &self,
        scope: &TenantContext,
        source: &str,
    ) -> Result<bool, RepositoryError> {
        source_input(source)?;
        if scope != &TenantContext::local() {
            return Err(AuthorizationError::Forbidden.into());
        }
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let mut values = scope_values(scope);
        values.push(source.into());
        let row=tx.query_opt(&format!("SELECT CASE WHEN octet_length(lifetime) BETWEEN 1 AND 128 THEN lifetime END FROM legacy_plan_imports WHERE {SCOPE} AND source=$5"),&parameters(&values)).await.map_err(unavailable)?;
        let found = if let Some(row) = row {
            values[4] = field(&row, 0)?;
            let owner=tx.query_opt(&format!("SELECT CASE WHEN octet_length(session_id) BETWEEN 1 AND 1024 THEN session_id END FROM session_lifetime_owners WHERE {SCOPE} AND lifetime=$5 AND provenance='canonical'"),&parameters(&values)).await.map_err(unavailable)?.ok_or(CoordinationError::Corrupt)?;
            permanent_life(
                &tx,
                &SessionWriterLeaseKey {
                    scope: scope.clone(),
                    session_key: SessionKey(field(&owner, 0)?),
                    session_lifetime_id: SessionLifetimeId(values[4].clone()),
                },
            )
            .await?;
            true
        } else {
            false
        };
        tx.commit().await.map_err(unavailable)?;
        Ok(found)
    }

    async fn import_legacy_plan_file(
        &self,
        scope: &TenantContext,
        source: &str,
        digest: &str,
        key: &SessionKey,
        life: &SessionLifetimeId,
        value: &serde_json::Value,
    ) -> Result<(), RepositoryError> {
        source_input(source)?;
        identity_input(key, life)?;
        if scope != &TenantContext::local() {
            return Err(AuthorizationError::Forbidden.into());
        }
        if digest.len() != 64 || !digest.bytes().all(|c| c.is_ascii_hexdigit()) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let raw = encode(value, 1048576)?;
        let definition: PlanDefinition =
            serde_json::from_value(value.clone()).map_err(|_| CoordinationError::InvalidRequest)?;
        definition
            .canonical_bytes()
            .map_err(|_| CoordinationError::InvalidRequest)?;
        let lease_key = SessionWriterLeaseKey {
            scope: scope.clone(),
            session_key: key.clone(),
            session_lifetime_id: life.clone(),
        };
        let id = proof_key(&lease_key)?;
        let mut proofs = self.proofs.lock().await;
        if !proofs.contains_key(&id) && proofs.len() >= 1024 {
            return Err(CoordinationError::Conflict.into());
        }
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        metadata_in(&tx, scope, key, true)
            .await?
            .filter(|meta| !meta.deleted && meta.lifetime == *life)
            .ok_or(AuthorizationError::StaleOwner)?;
        let proof = if let Some(proof) = proofs.get(&id) {
            Self::fence_in(&tx, proof).await?;
            proof.clone()
        } else {
            grant_in(&tx, &lease_key, &self.writer_owner, 30000).await?
        };
        let mut values = scope_values(scope);
        values.push(source.into());
        if let Some(old) = tx
            .query_opt(
                &format!(
                    "SELECT digest,lifetime FROM legacy_plan_imports WHERE {SCOPE} AND source=$5"
                ),
                &parameters(&values),
            )
            .await
            .map_err(unavailable)?
        {
            if old.try_get::<_, String>(0).map_err(unavailable)? != digest
                || old.try_get::<_, String>(1).map_err(unavailable)? != life.0
            {
                return Err(CoordinationError::Conflict.into());
            }
        } else {
            let mut plan = scope_values(scope);
            plan.extend([life.0.clone(), raw]);
            tx.execute(
                "INSERT INTO session_plans VALUES($1,$2,$3,$4,$5,0,$6) ON CONFLICT DO NOTHING",
                &parameters(&plan),
            )
            .await
            .map_err(unavailable)?;
            tx.execute(&format!("INSERT INTO plan_legacy_evidence SELECT * FROM session_plans WHERE {SCOPE} AND lifetime=$5 ON CONFLICT DO NOTHING"),&parameters(&plan[..5])).await.map_err(unavailable)?;
            values.extend([digest.into(), life.0.clone()]);
            tx.execute(
                "INSERT INTO legacy_plan_imports VALUES($1,$2,$3,$4,$5,$6,$7)",
                &parameters(&values),
            )
            .await
            .map_err(unavailable)?;
        }
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        proofs.insert(id, proof);
        Ok(())
    }
}
