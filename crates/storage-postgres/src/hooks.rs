//! PostgreSQL原Hook claim/outcome/publication；历史只核永久归属，运行效果核固定授权与writer。
use crate::canonical::{
    PgCanonicalStore, field, integer, key_values, permanent_life, proof_key, set_scope, unsigned,
};
use crate::identities::exact_owner_in;
use crate::sessions::{encode, metadata_in};
use crate::{SCOPE, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::{HookExecutionRepository, HookObservationRepository};
use tokio_postgres::Transaction;
pub(crate) async fn saved_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    key: &SessionKey,
    life: &SessionLifetimeId,
    operation: &str,
) -> Result<Option<HookOutcome>, CoordinationError> {
    permanent_life(
        db,
        &SessionWriterLeaseKey {
            scope: scope.clone(),
            session_key: key.clone(),
            session_lifetime_id: life.clone(),
        },
    )
    .await?;
    let mut values = scope_values(scope);
    values.extend([key.0.clone(), life.0.clone(), operation.into()]);
    let row=db.query_opt(&format!("SELECT CASE WHEN octet_length(outcome_json)<=49152 THEN outcome_json END FROM hook_outcomes WHERE {SCOPE} AND session_id=$5 AND lifetime=$6 AND operation_id=$7"),&parameters(&values)).await.map_err(unavailable)?;
    if let Some(row) = row {
        let outcome: HookOutcome =
            serde_json::from_str(&field(&row, 0)?).map_err(|_| CoordinationError::Corrupt)?;
        validate_hook_outcome(&outcome).map_err(|_| CoordinationError::Corrupt)?;
        if outcome.payload.session_key != *key
            || outcome.payload.session_lifetime_id != *life
            || outcome.payload.operation_id != operation
        {
            return Err(CoordinationError::Corrupt);
        }
        if let Some(owner) = &outcome.payload.owner {
            if exact_owner_in(db, scope, &owner.run_id).await? != *owner {
                return Err(CoordinationError::Corrupt);
            }
        }
        Ok(Some(outcome))
    } else {
        Ok(None)
    }
}
async fn verify_payload(
    db: &Transaction<'_>,
    scope: &TenantContext,
    env: Option<&EnterpriseOwnerEnvelope>,
    payload: &HookPayload,
    proof: &SessionWriterLease,
) -> Result<(), RepositoryError> {
    permanent_life(
        db,
        &SessionWriterLeaseKey {
            scope: scope.clone(),
            session_key: payload.session_key.clone(),
            session_lifetime_id: payload.session_lifetime_id.clone(),
        },
    )
    .await?;
    if let Some(owner) = &payload.owner {
        let declared = EnterpriseOwnerEnvelope {
            scope: scope.clone(),
            owner: owner.clone(),
        };
        let envelope = env.unwrap_or(&declared);
        if envelope != &declared {
            return Err(AuthorizationError::Forbidden.into());
        }
        PgCanonicalStore::fence_envelope_in(db, proof, envelope).await?;
    } else {
        if env.is_some()
            || !matches!(
                payload.event,
                HookEvent::SessionStart | HookEvent::SessionEnd | HookEvent::Notification
            )
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let meta = metadata_in(db, scope, &payload.session_key, true)
            .await?
            .ok_or(CoordinationError::Forbidden)?;
        let mut values = key_values(&SessionWriterLeaseKey {
            scope: scope.clone(),
            session_key: payload.session_key.clone(),
            session_lifetime_id: payload.session_lifetime_id.clone(),
        });
        let ended:bool=db.query_one(&format!("SELECT EXISTS(SELECT 1 FROM session_tombstones WHERE {SCOPE} AND session_id=$5 AND lifetime=$6)"),&parameters(&values)).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
        values.push(payload.operation_id.clone());
        let publication=db.query_opt(&format!("SELECT CASE WHEN octet_length(payload_json)<=16384 THEN payload_json END FROM hook_publications WHERE {SCOPE} AND operation_id=$7 AND payload_json::jsonb->>'session_key'=$5 AND payload_json::jsonb->>'session_lifetime_id'=$6"),&parameters(&values)).await.map_err(unavailable)?;
        let historical_start = if let Some(row) = publication {
            let publication: HookPublication =
                serde_json::from_str(&field(&row, 0)?).map_err(|_| CoordinationError::Corrupt)?;
            publication.operation_id == payload.operation_id
                && publication.event == HookEvent::SessionStart
                && payload.event == HookEvent::SessionStart
        } else {
            false
        };
        if meta.lifetime != payload.session_lifetime_id
            && !(ended && (payload.event == HookEvent::SessionEnd || historical_start))
        {
            return Err(AuthorizationError::StaleOwner.into());
        }
        if proof.key.scope != *scope
            || proof.key.session_key != meta.key
            || proof.key.session_lifetime_id != meta.lifetime
        {
            return Err(CoordinationError::StaleLease.into());
        }
        PgCanonicalStore::before_commit(db, proof).await?;
    }
    Ok(())
}
#[async_trait::async_trait]
impl HookExecutionRepository for PgCanonicalStore {
    async fn read_hook_snapshot(
        &self,
        env: &EnterpriseOwnerEnvelope,
    ) -> Result<RunSnapshot, RepositoryError> {
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
        let mut values = scope_values(&env.scope);
        values.push(env.owner.run_id.0.clone());
        let row=tx.query_one(&format!("SELECT CASE WHEN octet_length(snapshot_json)<=1048576 THEN snapshot_json END FROM run_snapshots WHERE {SCOPE} AND run_id=$5"),&parameters(&values)).await.map_err(unavailable)?;
        let snapshot =
            serde_json::from_str(&field(&row, 0)?).map_err(|_| CoordinationError::Corrupt)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(snapshot)
    }
    async fn read_hook_interaction(
        &self,
        env: &EnterpriseOwnerEnvelope,
        id: &InteractionId,
    ) -> Result<Option<InteractionRecord>, RepositoryError> {
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
        let mut values = scope_values(&env.scope);
        values.extend([
            env.owner.run_id.0.clone(),
            id.0.clone(),
            env.owner.session_lifetime_id.0.clone(),
            env.owner.run_generation.0.to_string(),
        ]);
        let row=tx.query_opt(&format!("SELECT kind,status,revision,CASE WHEN octet_length(prompt)<=16384 THEN prompt END,CASE WHEN octet_length(payload_json)<=65536 THEN payload_json END,response_json IS NULL,CASE WHEN octet_length(response_json)<=65536 THEN response_json END FROM interactions WHERE {SCOPE} AND run_id=$5 AND id=$6 AND lifetime=$7 AND generation=$8::text::bigint"),&parameters(&values)).await.map_err(unavailable)?;
        let result = if let Some(row) = row {
            Some(InteractionRecord {
                interaction_id: id.clone(),
                session_id: env.owner.session_key.clone(),
                owner_run_id: env.owner.run_id.clone(),
                kind: row.try_get(0).map_err(unavailable)?,
                status: row.try_get(1).map_err(unavailable)?,
                revision: row.try_get(2).map_err(unavailable)?,
                prompt: field(&row, 3)?,
                payload: serde_json::from_str(&field(&row, 4)?)
                    .map_err(|_| CoordinationError::Corrupt)?,
                response: if row.try_get::<_, bool>(5).map_err(unavailable)? {
                    None
                } else {
                    Some(
                        serde_json::from_str(&field(&row, 6)?)
                            .map_err(|_| CoordinationError::Corrupt)?,
                    )
                },
            })
        } else {
            None
        };
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
    async fn claim_hook_effect(
        &self,
        scope: &TenantContext,
        env: Option<&EnterpriseOwnerEnvelope>,
        payload: &HookPayload,
    ) -> Result<HookClaim, RepositoryError> {
        encode(payload, 16384)?;
        let outcome = HookOutcome {
            payload: payload.clone(),
            status: HookStatus::Running,
            effect: None,
            failure: None,
        };
        validate_hook_outcome(&outcome)?;
        let proofs = self.proofs.lock().await;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let meta = metadata_in(&tx, scope, &payload.session_key, true)
            .await?
            .ok_or(CoordinationError::Forbidden)?;
        let key = SessionWriterLeaseKey {
            scope: scope.clone(),
            session_key: meta.key,
            session_lifetime_id: meta.lifetime,
        };
        let proof = proofs
            .get(&proof_key(&key)?)
            .ok_or(CoordinationError::StaleLease)?;
        verify_payload(&tx, scope, env, payload, proof).await?;
        if let Some(saved) = saved_in(
            &tx,
            scope,
            &payload.session_key,
            &payload.session_lifetime_id,
            &payload.operation_id,
        )
        .await?
        {
            if saved.payload != *payload {
                return Err(CoordinationError::Conflict.into());
            }
            Self::before_commit(&tx, proof).await?;
            tx.commit().await.map_err(unavailable)?;
            return Ok(HookClaim::Existing(Box::new(saved)));
        }
        let mut values = scope_values(scope);
        values.extend([
            payload.operation_id.clone(),
            payload.session_key.0.clone(),
            payload.session_lifetime_id.0.clone(),
            encode(&outcome, 49152)?,
        ]);
        tx.execute("INSERT INTO hook_outcomes(organization_id,tenant_id,project_id,agent_uid,operation_id,session_id,lifetime,outcome_json) VALUES($1,$2,$3,$4,$5,$6,$7,$8)",&parameters(&values)).await.map_err(unavailable)?;
        tx.execute(&format!("UPDATE hook_publications SET dispatched=TRUE WHERE {SCOPE} AND operation_id=$5 AND payload_json::jsonb->>'session_key'=$6 AND payload_json::jsonb->>'session_lifetime_id'=$7"),&parameters(&values[..7])).await.map_err(unavailable)?;
        Self::before_commit(&tx, proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(HookClaim::New)
    }
    async fn settle_hook_effect(
        &self,
        scope: &TenantContext,
        env: Option<&EnterpriseOwnerEnvelope>,
        outcome: &HookOutcome,
    ) -> Result<HookOutcome, RepositoryError> {
        let raw = encode(outcome, 49152)?;
        validate_hook_outcome(outcome)?;
        if outcome.status == HookStatus::Running {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let payload = &outcome.payload;
        let proofs = self.proofs.lock().await;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let meta = metadata_in(&tx, scope, &payload.session_key, true)
            .await?
            .ok_or(CoordinationError::Forbidden)?;
        let key = SessionWriterLeaseKey {
            scope: scope.clone(),
            session_key: meta.key,
            session_lifetime_id: meta.lifetime,
        };
        let proof = proofs
            .get(&proof_key(&key)?)
            .ok_or(CoordinationError::StaleLease)?;
        verify_payload(&tx, scope, env, payload, proof).await?;
        let old = saved_in(
            &tx,
            scope,
            &payload.session_key,
            &payload.session_lifetime_id,
            &payload.operation_id,
        )
        .await?
        .ok_or(CoordinationError::Conflict)?;
        if old.payload != *payload {
            return Err(CoordinationError::Conflict.into());
        }
        if old.status != HookStatus::Running {
            if old == *outcome || old.status == HookStatus::UnknownAfterRestart {
                Self::before_commit(&tx, proof).await?;
                tx.commit().await.map_err(unavailable)?;
                return Ok(old);
            }
            return Err(CoordinationError::Conflict.into());
        }
        let mut values = scope_values(scope);
        values.extend([
            payload.session_key.0.clone(),
            payload.session_lifetime_id.0.clone(),
            payload.operation_id.clone(),
            raw,
        ]);
        tx.execute(&format!("UPDATE hook_outcomes SET outcome_json=$8 WHERE {SCOPE} AND session_id=$5 AND lifetime=$6 AND operation_id=$7"),&parameters(&values)).await.map_err(unavailable)?;
        Self::before_commit(&tx, proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(outcome.clone())
    }
}
#[async_trait::async_trait]
impl HookObservationRepository for PgCanonicalStore {
    async fn read_hook_history(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        life: &SessionLifetimeId,
        after: u64,
        limit: usize,
    ) -> Result<HookReadback, RepositoryError> {
        integer(after)?;
        let limit = limit.clamp(1, 32);
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let mut values = key_values(&SessionWriterLeaseKey {
            scope: scope.clone(),
            session_key: key.clone(),
            session_lifetime_id: life.clone(),
        });
        permanent_life(
            &tx,
            &SessionWriterLeaseKey {
                scope: scope.clone(),
                session_key: key.clone(),
                session_lifetime_id: life.clone(),
            },
        )
        .await?;
        values.extend([after.to_string(), (limit + 1).to_string()]);
        let rows=tx.query(&format!("SELECT storage_row_id,CASE WHEN octet_length(operation_id) BETWEEN 1 AND 256 THEN operation_id END FROM hook_outcomes WHERE {SCOPE} AND session_id=$5 AND lifetime=$6 AND storage_row_id>$7::text::bigint ORDER BY storage_row_id LIMIT $8::text::bigint"),&parameters(&values)).await.map_err(unavailable)?;
        let has_more = rows.len() > limit;
        let mut cursor = after;
        let mut outcomes = Vec::new();
        for row in rows.into_iter().take(limit) {
            cursor = unsigned(row.try_get(0).map_err(unavailable)?)?;
            outcomes.push(
                saved_in(&tx, scope, key, life, &field(&row, 1)?)
                    .await?
                    .ok_or(CoordinationError::Corrupt)?,
            );
        }
        encode(&outcomes, 1024 * 1024 - 16384)?;
        let revision = tx
            .query_one(
                &format!("SELECT revision FROM snapshot_clock WHERE {SCOPE} AND slot=1"),
                &parameters(&scope_values(scope)),
            )
            .await
            .map_err(unavailable)?;
        let snapshot_revision =
            SnapshotRevision(unsigned(revision.try_get(0).map_err(unavailable)?)?);
        tx.commit().await.map_err(unavailable)?;
        Ok(HookReadback {
            schema_version: 1,
            session_key: key.clone(),
            session_lifetime_id: life.clone(),
            snapshot_revision,
            outcomes,
            cursor,
            has_more,
        })
    }
    async fn read_pending_hooks(
        &self,
        scope: &TenantContext,
    ) -> Result<Vec<HookPublication>, RepositoryError> {
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let rows=tx.query(&format!("SELECT CASE WHEN octet_length(operation_id) BETWEEN 1 AND 256 THEN operation_id END,CASE WHEN octet_length(payload_json)<=16384 THEN payload_json END FROM hook_publications WHERE {SCOPE} AND NOT dispatched ORDER BY storage_row_id LIMIT 64"),&parameters(&scope_values(scope))).await.map_err(unavailable)?;
        let mut publications = Vec::new();
        for row in rows {
            let publication: HookPublication =
                serde_json::from_str(&field(&row, 1)?).map_err(|_| CoordinationError::Corrupt)?;
            if publication.operation_id != field(&row, 0)? {
                return Err(CoordinationError::Corrupt.into());
            }
            permanent_life(
                &tx,
                &SessionWriterLeaseKey {
                    scope: scope.clone(),
                    session_key: publication.session_key.clone(),
                    session_lifetime_id: publication.session_lifetime_id.clone(),
                },
            )
            .await?;
            publications.push(publication);
        }
        encode(&publications, 1024 * 1024 - 16384)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(publications)
    }
    async fn acknowledge_pending_hook(
        &self,
        scope: &TenantContext,
        publication: &HookPublication,
    ) -> Result<(), RepositoryError> {
        encode(publication, 16384)?;
        if publication.operation_id.is_empty() || publication.operation_id.len() > 256 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let proofs = self.proofs.lock().await;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        permanent_life(
            &tx,
            &SessionWriterLeaseKey {
                scope: scope.clone(),
                session_key: publication.session_key.clone(),
                session_lifetime_id: publication.session_lifetime_id.clone(),
            },
        )
        .await?;
        let meta = metadata_in(&tx, scope, &publication.session_key, true)
            .await?
            .ok_or(CoordinationError::Forbidden)?;
        let proof = proofs
            .get(&proof_key(&SessionWriterLeaseKey {
                scope: scope.clone(),
                session_key: meta.key,
                session_lifetime_id: meta.lifetime,
            })?)
            .ok_or(CoordinationError::StaleLease)?;
        let mut values = scope_values(scope);
        values.push(publication.operation_id.clone());
        let saved=tx.query_opt(&format!("SELECT CASE WHEN octet_length(payload_json)<=16384 THEN payload_json END FROM hook_publications WHERE {SCOPE} AND operation_id=$5 FOR UPDATE"),&parameters(&values)).await.map_err(unavailable)?.ok_or(CoordinationError::Conflict)?;
        let saved: HookPublication =
            serde_json::from_str(&field(&saved, 0)?).map_err(|_| CoordinationError::Corrupt)?;
        if saved.operation_id != publication.operation_id
            || saved.session_key != publication.session_key
            || saved.session_lifetime_id != publication.session_lifetime_id
            || saved.event != publication.event
        {
            return Err(CoordinationError::Conflict.into());
        }
        Self::before_commit(&tx, proof).await?;
        tx.execute(
            &format!(
                "UPDATE hook_publications SET dispatched=TRUE WHERE {SCOPE} AND operation_id=$5"
            ),
            &parameters(&values),
        )
        .await
        .map_err(unavailable)?;
        Self::before_commit(&tx, proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }
}
