//! 对象发布两阶段事实；对象 I/O 在外部完成，原工具终态与 published 同事务。
use crate::canonical::{PgCanonicalStore, field, set_scope};
use crate::facts::{frozen_in, receipts_in};
use crate::sessions::encode;
use crate::tools::finish_in;
use crate::{SCOPE, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::{ArtifactPublicationRepository, verify_artifact_completion};
use sha2::{Digest, Sha256};
use tokio_postgres::Transaction;
fn mapped(error: RepositoryError) -> ArtifactError {
    match error {
        RepositoryError::Artifact(error) => error,
        RepositoryError::Authorization(_) | RepositoryError::Ledger(_) => ArtifactError::Forbidden,
        RepositoryError::Coordination(error) => match error {
            CoordinationError::Forbidden => ArtifactError::Forbidden,
            CoordinationError::StaleLease => ArtifactError::StaleWriter,
            CoordinationError::Corrupt => ArtifactError::Corrupt,
            CoordinationError::Conflict => ArtifactError::Conflict,
            CoordinationError::InvalidRequest => ArtifactError::InvalidReference,
            _ => ArtifactError::StorageUnavailable,
        },
    }
}
fn coordination(error: CoordinationError) -> ArtifactError {
    mapped(error.into())
}
fn same(a: &ObjectArtifactReservation, b: &ObjectArtifactReservation) -> bool {
    let mut a = a.clone();
    let mut b = b.clone();
    a.published = false;
    b.published = false;
    a == b
}
async fn fence(
    db: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    proof: &SessionWriterLease,
    store: &str,
) -> Result<(), ArtifactError> {
    PgCanonicalStore::fence_envelope_in(db, proof, env)
        .await
        .map_err(mapped)?;
    let snapshot = frozen_in(db, &env.scope, &env.owner.run_id)
        .await
        .map_err(coordination)?
        .ok_or(ArtifactError::Forbidden)?;
    if snapshot.artifact_store_id.as_deref() != Some(store) {
        return Err(ArtifactError::Forbidden);
    }
    Ok(())
}
async fn current(
    db: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    round: usize,
    call: &str,
) -> Result<Option<ObjectArtifactReservation>, ArtifactError> {
    let mut values = scope_values(&env.scope);
    values.extend([env.owner.run_id.0.clone(), round.to_string(), call.into()]);
    let row=db.query_opt(&format!("SELECT CASE WHEN octet_length(reservation_json)<=65536 THEN reservation_json END,CASE WHEN octet_length(reservation_digest)=64 THEN reservation_digest END,published,CASE WHEN octet_length(publication_id)=64 THEN publication_id END,CASE WHEN octet_length(store_id)=64 THEN store_id END,CASE WHEN octet_length(sha256)=64 THEN sha256 END,bytes FROM artifact_publications WHERE {SCOPE} AND run_id=$5 AND round=$6::text::bigint AND call_id=$7 FOR UPDATE"),&parameters(&values)).await.map_err(unavailable).map_err(coordination)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let raw = field(&row, 0).map_err(coordination)?;
    if format!("{:x}", Sha256::digest(raw.as_bytes())) != field(&row, 1).map_err(coordination)? {
        return Err(ArtifactError::Corrupt);
    }
    let mut value: ObjectArtifactReservation =
        serde_json::from_str(&raw).map_err(|_| ArtifactError::Corrupt)?;
    if !value.valid()
        || value.published
        || value.envelope != *env
        || value.round != round
        || value.call_id != call
        || value.publication_id != field(&row, 3).map_err(coordination)?
        || value.store_id != field(&row, 4).map_err(coordination)?
        || value.identity.sha256 != field(&row, 5).map_err(coordination)?
        || i64::try_from(value.identity.bytes).map_err(|_| ArtifactError::Corrupt)?
            != row
                .try_get::<_, i64>(6)
                .map_err(unavailable)
                .map_err(coordination)?
    {
        return Err(ArtifactError::Corrupt);
    }
    value.published = row.try_get(2).map_err(unavailable).map_err(coordination)?;
    Ok(Some(value))
}
async fn verify_tool(
    db: &Transaction<'_>,
    reservation: &ObjectArtifactReservation,
    completion: &ToolArtifactCompletion,
    published: bool,
) -> Result<(), ArtifactError> {
    let receipts = receipts_in(db, &reservation.envelope.scope, &reservation.envelope.owner)
        .await
        .map_err(coordination)?;
    let receipt = receipts
        .iter()
        .find(|item| item.round == reservation.round as i64 && item.call_id == reservation.call_id)
        .ok_or(ArtifactError::Forbidden)?;
    if !published {
        if receipt.status != "running" {
            return Err(ArtifactError::Conflict);
        }
    } else if receipt.status != "terminal"
        || receipt.outcome.as_deref() != Some(completion.outcome.as_str())
        || receipt.artifact_ref.as_deref() != Some(reservation.reference().as_str())
        || receipt.receipt.as_ref() != Some(&completion.receipt)
    {
        return Err(ArtifactError::Corrupt);
    }
    Ok(())
}
#[async_trait::async_trait]
impl ArtifactPublicationRepository for PgCanonicalStore {
    async fn reserve_object_artifact(
        &self,
        reservation: &ObjectArtifactReservation,
        completion: &ToolArtifactCompletion,
    ) -> Result<ObjectArtifactReservation, ArtifactError> {
        encode(completion, 1048576).map_err(coordination)?;
        verify_artifact_completion(reservation, completion)?;
        if reservation.published {
            return Err(ArtifactError::InvalidReference);
        }
        let raw = encode(reservation, 65536).map_err(coordination)?;
        let env = &reservation.envelope;
        let proof = self.execution_proof(env).await.map_err(coordination)?;
        let mut client = self.inner.client.lock().await;
        let tx = client
            .transaction()
            .await
            .map_err(unavailable)
            .map_err(coordination)?;
        set_scope(&tx, &env.scope).await.map_err(coordination)?;
        crate::maintenance::artifact_write_gate(&tx)
            .await
            .map_err(coordination)?;
        fence(&tx, env, &proof, &reservation.store_id).await?;
        let old = current(&tx, env, reservation.round, &reservation.call_id).await?;
        let result = if let Some(old) = old {
            if !same(&old, reservation) {
                return Err(ArtifactError::Conflict);
            }
            verify_tool(&tx, &old, completion, old.published).await?;
            old
        } else {
            verify_tool(&tx, reservation, completion, false).await?;
            let mut values = scope_values(&env.scope);
            values.extend([
                reservation.publication_id.clone(),
                env.owner.run_id.0.clone(),
                reservation.round.to_string(),
                reservation.call_id.clone(),
                reservation.store_id.clone(),
                reservation.identity.sha256.clone(),
                reservation.identity.bytes.to_string(),
                format!("{:x}", Sha256::digest(raw.as_bytes())),
                raw,
            ]);
            tx.execute("INSERT INTO artifact_publications(organization_id,tenant_id,project_id,agent_uid,publication_id,run_id,round,call_id,store_id,sha256,bytes,reservation_digest,reservation_json) VALUES($1,$2,$3,$4,$5,$6,$7::text::bigint,$8,$9,$10,$11::text::bigint,$12,$13)",&parameters(&values)).await.map_err(unavailable).map_err(coordination)?;
            reservation.clone()
        };
        Self::before_commit(&tx, &proof)
            .await
            .map_err(coordination)?;
        tx.commit()
            .await
            .map_err(unavailable)
            .map_err(coordination)?;
        Ok(result)
    }
    async fn commit_object_artifact(
        &self,
        reservation: &ObjectArtifactReservation,
        completion: &ToolArtifactCompletion,
        verified: &ArtifactIdentity,
    ) -> Result<(), ArtifactError> {
        encode(completion, 1048576).map_err(coordination)?;
        verify_artifact_completion(reservation, completion)?;
        if verified != &reservation.identity {
            return Err(ArtifactError::Corrupt);
        }
        let env = &reservation.envelope;
        let proof = self.execution_proof(env).await.map_err(coordination)?;
        let mut client = self.inner.client.lock().await;
        let tx = client
            .transaction()
            .await
            .map_err(unavailable)
            .map_err(coordination)?;
        set_scope(&tx, &env.scope).await.map_err(coordination)?;
        fence(&tx, env, &proof, &reservation.store_id).await?;
        let old = current(&tx, env, reservation.round, &reservation.call_id)
            .await?
            .ok_or(ArtifactError::Forbidden)?;
        if !same(&old, reservation) {
            return Err(ArtifactError::Conflict);
        }
        if !old.published {
            verify_tool(&tx, &old, completion, false).await?;
            finish_in(&tx, env, completion, &reservation.reference())
                .await
                .map_err(mapped)?;
            let mut values = scope_values(&env.scope);
            values.push(reservation.publication_id.clone());
            let changed=tx.execute(&format!("UPDATE artifact_publications SET published=TRUE WHERE {SCOPE} AND publication_id=$5 AND NOT published"),&parameters(&values)).await.map_err(unavailable).map_err(coordination)?;
            if changed != 1 {
                return Err(ArtifactError::Conflict);
            }
        }
        verify_tool(&tx, &old, completion, true).await?;
        Self::before_commit(&tx, &proof)
            .await
            .map_err(coordination)?;
        tx.commit()
            .await
            .map_err(unavailable)
            .map_err(coordination)?;
        Ok(())
    }
    async fn read_object_artifact(
        &self,
        env: &EnterpriseOwnerEnvelope,
        reference: &str,
    ) -> Result<ObjectArtifactReservation, ArtifactError> {
        let id = reference
            .strip_prefix("artifact:v1:")
            .filter(|id| {
                ArtifactIdentity {
                    sha256: (*id).into(),
                    bytes: 0,
                }
                .valid()
            })
            .ok_or(ArtifactError::InvalidReference)?;
        let proof = self.execution_proof(env).await.map_err(coordination)?;
        let mut client = self.inner.client.lock().await;
        let tx = client
            .transaction()
            .await
            .map_err(unavailable)
            .map_err(coordination)?;
        set_scope(&tx, &env.scope).await.map_err(coordination)?;
        Self::fence_envelope_in(&tx, &proof, env)
            .await
            .map_err(mapped)?;
        let mut values = scope_values(&env.scope);
        values.extend([id.into(), env.owner.run_id.0.clone()]);
        let row=tx.query_opt(&format!("SELECT round,CASE WHEN octet_length(call_id) BETWEEN 1 AND 1024 THEN call_id END FROM artifact_publications WHERE {SCOPE} AND publication_id=$5 AND run_id=$6 AND published"),&parameters(&values)).await.map_err(unavailable).map_err(coordination)?.ok_or(ArtifactError::Forbidden)?;
        let round = usize::try_from(
            row.try_get::<_, i64>(0)
                .map_err(unavailable)
                .map_err(coordination)?,
        )
        .map_err(|_| ArtifactError::Corrupt)?;
        let call = field(&row, 1).map_err(coordination)?;
        let reservation = current(&tx, env, round, &call)
            .await?
            .ok_or(ArtifactError::Forbidden)?;
        if !reservation.published || reservation.reference() != reference {
            return Err(ArtifactError::Forbidden);
        }
        fence(&tx, env, &proof, &reservation.store_id).await?;
        let receipts = receipts_in(&tx, &env.scope, &env.owner)
            .await
            .map_err(coordination)?;
        let receipt = receipts
            .into_iter()
            .find(|item| item.round == round as i64 && item.call_id == call)
            .ok_or(ArtifactError::Corrupt)?;
        let completion = ToolArtifactCompletion {
            round,
            call_id: call,
            outcome: receipt.outcome.ok_or(ArtifactError::Corrupt)?,
            receipt: receipt.receipt.ok_or(ArtifactError::Corrupt)?,
        };
        verify_artifact_completion(&reservation, &completion).map_err(|error| {
            if error == ArtifactError::Conflict {
                ArtifactError::Corrupt
            } else {
                error
            }
        })?;
        verify_tool(&tx, &reservation, &completion, true).await?;
        Self::before_commit(&tx, &proof)
            .await
            .map_err(coordination)?;
        tx.commit()
            .await
            .map_err(unavailable)
            .map_err(coordination)?;
        Ok(reservation)
    }
}

/// Local 开发对象后端复用原 no-follow 文件目录；发布事实仍由上面的原事务确认。
#[async_trait::async_trait]
impl agent_storage_ports::ArtifactStore for PgCanonicalStore {
    fn store_id(&self) -> String {
        self.local_files
            .as_ref()
            .map_or_else(String::new, |directory| directory.store_id())
    }
    async fn publish_object(
        &self,
        reservation: &ObjectArtifactReservation,
        body: &str,
    ) -> Result<ArtifactIdentity, ArtifactError> {
        let directory = self
            .local_files
            .clone()
            .ok_or(ArtifactError::StorageUnavailable)?;
        if !reservation.valid()
            || reservation.store_id != directory.store_id()
            || reservation.envelope.scope != TenantContext::local()
        {
            return Err(ArtifactError::Forbidden);
        }
        if body.len() as u64 > MAX_ARTIFACT_BYTES
            || body.len() as u64 != reservation.identity.bytes
            || format!("{:x}", Sha256::digest(body.as_bytes())) != reservation.identity.sha256
        {
            return Err(ArtifactError::Corrupt);
        }
        let permit = self
            .file_budget
            .clone()
            .try_acquire_owned()
            .map_err(|_| ArtifactError::Conflict)?;
        let body = body.to_owned();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let reference = directory.publish(&body)?;
            directory
                .read(&reference, 0, 1)
                .map(|(identity, _)| identity)
        })
        .await
        .map_err(|_| ArtifactError::StorageUnavailable)?
    }
    async fn read_object(
        &self,
        reservation: &ObjectArtifactReservation,
        offset: u64,
        limit: usize,
    ) -> Result<ArtifactPage, ArtifactError> {
        let directory = self
            .local_files
            .clone()
            .ok_or(ArtifactError::StorageUnavailable)?;
        if !reservation.valid()
            || reservation.store_id != directory.store_id()
            || reservation.envelope.scope != TenantContext::local()
        {
            return Err(ArtifactError::Forbidden);
        }
        let permit = self
            .file_budget
            .clone()
            .try_acquire_owned()
            .map_err(|_| ArtifactError::Conflict)?;
        let expected = reservation.identity.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let reference = directory.reference(&expected)?;
            let (identity, data) = directory.read(&reference, offset, limit)?;
            if identity != expected {
                return Err(ArtifactError::Corrupt);
            }
            Ok(ArtifactPage { identity, data })
        })
        .await
        .map_err(|_| ArtifactError::StorageUnavailable)?
    }
}
