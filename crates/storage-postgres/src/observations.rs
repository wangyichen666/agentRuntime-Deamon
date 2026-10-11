//! canonical 运行诊断与永久 artifact 读回；读取不准入运行、不获得 writer。
use crate::canonical::{PgCanonicalStore, field, permanent_life, set_scope, unsigned};
use crate::identities::exact_owner_in;
use crate::readback::{projection_in, snapshot_in};
use crate::sessions::metadata_in;
use crate::{SCOPE, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::{RuntimeObservationRepository, decode_transcript_batch};

fn valid_id(id: &str) -> Result<(), CoordinationError> {
    if id.is_empty() || id.len() > 1024 {
        return Err(CoordinationError::InvalidRequest);
    }
    Ok(())
}

#[async_trait::async_trait]
impl RuntimeObservationRepository for PgCanonicalStore {
    async fn read_run_transcript(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Vec<Message>, RepositoryError> {
        valid_id(&run.0)?;
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
        let mut values = scope_values(scope);
        values.push(run.0.clone());
        let row = tx.query_one(&format!("SELECT count(*)::bigint,COALESCE(sum(octet_length(messages_json)),0)::bigint,COALESCE(sum(end_seq-start_seq),0)::bigint FROM (SELECT messages_json,start_seq,end_seq FROM transcript_batches WHERE {SCOPE} AND run_id=$5 LIMIT 32769) b"), &parameters(&values)).await.map_err(unavailable)?;
        let count: i64 = row.try_get(0).map_err(unavailable)?;
        let bytes: i64 = row.try_get(1).map_err(unavailable)?;
        let messages: i64 = row.try_get(2).map_err(unavailable)?;
        if count > 32768 || bytes > 16 * 1024 * 1024 || !(0..=32768).contains(&messages) {
            return Err(CoordinationError::Corrupt.into());
        }
        let rows = tx.query(&format!("SELECT CASE WHEN octet_length(lifetime) BETWEEN 1 AND 128 THEN lifetime END,start_seq,end_seq,CASE WHEN octet_length(messages_json)<=16777216 THEN messages_json END,CASE WHEN octet_length(digest)=64 THEN digest END FROM transcript_batches WHERE {SCOPE} AND run_id=$5 ORDER BY start_seq,storage_row_id LIMIT 32768"), &parameters(&values)).await.map_err(unavailable)?;
        let mut output = Vec::new();
        let mut previous = 0;
        for row in rows {
            let start = unsigned(row.try_get(1).map_err(unavailable)?)?;
            let end = unsigned(row.try_get(2).map_err(unavailable)?)?;
            if field(&row, 0)? != owner.session_lifetime_id.0 || start < previous {
                return Err(CoordinationError::Corrupt.into());
            }
            output.extend(decode_transcript_batch(
                &field(&row, 3)?,
                &field(&row, 4)?,
                start,
                end,
            )?);
            previous = end;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(output)
    }

    async fn read_context_summary(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
    ) -> Result<SessionContextSummary, RepositoryError> {
        valid_id(&key.0)?;
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let metadata = metadata_in(&tx, scope, key, false)
            .await?
            .filter(|meta| !meta.deleted)
            .ok_or(CoordinationError::Forbidden)?;
        permanent_life(
            &tx,
            &SessionWriterLeaseKey {
                scope: scope.clone(),
                session_key: key.clone(),
                session_lifetime_id: metadata.lifetime.clone(),
            },
        )
        .await?;
        let snapshot = snapshot_in(&tx, scope, &metadata).await?;
        let projection = projection_in(&tx, scope, &snapshot).await?;
        let result = SessionContextSummary {
            session_key: key.clone(),
            lifetime: snapshot.lifetime,
            transcript_revision: snapshot.revision,
            projection_generation: projection.generation,
            projected_messages: projection.messages.len(),
        };
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }

    async fn read_legacy_artifact(
        &self,
        scope: &TenantContext,
        owner: &ExactOwner,
        reference: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<u8>, RepositoryError> {
        if scope != &TenantContext::local() {
            return Err(ArtifactError::Forbidden.into());
        }
        if reference.is_empty()
            || reference.len() > 16384
            || !(1..=16384).contains(&limit)
            || offset.checked_add(limit as u64).is_none()
        {
            return Err(ArtifactError::InvalidReference.into());
        }
        let receipts = {
            let mut client = self.inner.client.lock().await;
            let tx = client
                .build_transaction()
                .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
                .read_only(true)
                .start()
                .await
                .map_err(unavailable)?;
            set_scope(&tx, scope).await?;
            if exact_owner_in(&tx, scope, &owner.run_id).await? != *owner {
                return Err(ArtifactError::Forbidden.into());
            }
            let mut values = scope_values(scope);
            values.extend([owner.run_id.0.clone(), reference.into()]);
            let row = tx.query_one(&format!("SELECT count(*)::bigint,COALESCE(sum(octet_length(receipt_json)),0)::bigint FROM (SELECT receipt_json FROM tool_executions WHERE {SCOPE} AND run_id=$5 AND artifact_ref=$6 LIMIT 1025) t"), &parameters(&values)).await.map_err(unavailable)?;
            let count: i64 = row.try_get(0).map_err(unavailable)?;
            if count == 0 {
                return Err(ArtifactError::Forbidden.into());
            }
            if count > 1024 || row.try_get::<_, i64>(1).map_err(unavailable)? > 4 * 1024 * 1024 {
                return Err(ArtifactError::Corrupt.into());
            }
            let rows = tx.query(&format!("SELECT lifetime,generation,receipt_json IS NULL,CASE WHEN octet_length(receipt_json)<=1048576 THEN receipt_json END FROM tool_executions WHERE {SCOPE} AND run_id=$5 AND artifact_ref=$6 LIMIT 1024"), &parameters(&values)).await.map_err(unavailable)?;
            let mut receipts = Vec::new();
            for row in rows {
                if row.try_get::<_, String>(0).map_err(unavailable)? != owner.session_lifetime_id.0
                    || unsigned(row.try_get(1).map_err(unavailable)?)? != owner.run_generation.0
                {
                    return Err(ArtifactError::Corrupt.into());
                }
                if !row.try_get::<_, bool>(2).map_err(unavailable)? {
                    let receipt: serde_json::Value = serde_json::from_str(&field(&row, 3)?)
                        .map_err(|_| ArtifactError::Corrupt)?;
                    if !receipt.is_object() {
                        return Err(ArtifactError::Corrupt.into());
                    }
                    receipts.push(receipt);
                }
            }
            tx.commit().await.map_err(unavailable)?;
            receipts
        };
        let files = self
            .local_files
            .clone()
            .ok_or(ArtifactError::StorageUnavailable)?;
        let permit = self
            .file_budget
            .clone()
            .try_acquire_owned()
            .map_err(|_| CoordinationError::Conflict)?;
        let reference = reference.to_owned();
        let (identity, bytes) = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            files.read(&reference, offset, limit)
        })
        .await
        .map_err(|_| ArtifactError::StorageUnavailable)??;
        for receipt in receipts {
            agent_storage_ports::verify_artifact_receipt(&identity, &receipt, false)?;
        }
        Ok(bytes)
    }
}
