//! 原transcript、context和filesystem artifact只读适配，无业务状态副本。
use crate::{RunStore, RuntimeError, enterprise, execution, sessions};
use agent_core::*;
use rusqlite::params;
impl RunStore {
    pub(crate) fn run_transcript_scoped(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Vec<Message>, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let db = connection.transaction()?;
        enterprise::check_run_scope_in(&db, scope, run)?;
        let owner = sessions::owner_in(&db, run)?;
        sessions::lifetime_owner_in(&db, &owner.session_key, &owner.session_lifetime_id)?;
        let (count,bytes,messages):(u64,u64,i64)=db.query_row("SELECT count(*),COALESCE(sum(length(CAST(messages_json AS BLOB))),0),COALESCE(sum(end_seq-start_seq),0) FROM (SELECT messages_json,start_seq,end_seq FROM transcript_batches WHERE run_id=?1 LIMIT 32769)",params![run.0],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
        if count > 32768 || bytes > 16 * 1024 * 1024 || !(0..=32768).contains(&messages) {
            return Err(CoordinationError::Corrupt.into());
        }
        let mut query=db.prepare("SELECT CASE WHEN b.lifetime=r.lifetime AND o.session_id=r.session_id AND o.provenance='canonical' AND typeof(b.messages_json)='text' AND length(CAST(b.messages_json AS BLOB))<=16777216 THEN b.messages_json END,CASE WHEN length(CAST(b.digest AS BLOB))=64 THEN b.digest END,CASE WHEN typeof(b.start_seq)='integer' AND b.start_seq>=0 THEN b.start_seq END,CASE WHEN typeof(b.end_seq)='integer' AND b.end_seq>=b.start_seq THEN b.end_seq END FROM transcript_batches b JOIN runs r ON r.id=b.run_id LEFT JOIN session_lifetime_owners o ON o.lifetime=b.lifetime WHERE b.run_id=?1 ORDER BY b.start_seq,b.rowid LIMIT 32768")?;
        let mut rows = query.query(params![run.0])?;
        let mut output = Vec::new();
        let mut previous = 0;
        while let Some(row) = rows.next()? {
            let raw = row
                .get::<_, Option<String>>(0)?
                .ok_or(CoordinationError::Corrupt)?;
            let digest = row
                .get::<_, Option<String>>(1)?
                .ok_or(CoordinationError::Corrupt)?;
            let start = row
                .get::<_, Option<u64>>(2)?
                .ok_or(CoordinationError::Corrupt)?;
            let end = row
                .get::<_, Option<u64>>(3)?
                .ok_or(CoordinationError::Corrupt)?;
            if start < previous {
                return Err(CoordinationError::Corrupt.into());
            }
            let batch = sessions::decode_transcript_batch(&raw, &digest, start, end)?;
            output.extend(batch);
            previous = end;
            if output.len() > 32768 {
                return Err(CoordinationError::Corrupt.into());
            }
        }
        drop(rows);
        drop(query);
        db.commit()?;
        Ok(output)
    }
    pub(crate) fn read_legacy_artifact_scoped(
        &self,
        scope: &TenantContext,
        owner: &ExactOwner,
        reference: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<u8>, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let db = connection.transaction()?;
        enterprise::check_run_scope_in(&db, scope, &owner.run_id)?;
        let actual = sessions::owner_in(&db, &owner.run_id)?;
        if &actual != owner {
            return Err(ArtifactError::Forbidden.into());
        }
        sessions::lifetime_owner_in(&db, &actual.session_key, &actual.session_lifetime_id)?;
        let (count,bytes):(u64,u64)=db.query_row("SELECT count(*),COALESCE(sum(length(CAST(receipt_json AS BLOB))),0) FROM (SELECT receipt_json FROM tool_executions WHERE run_id=?1 AND artifact_ref=?2 LIMIT 1025)",params![owner.run_id.0,reference],|r|Ok((r.get(0)?,r.get(1)?)))?;
        if count == 0 {
            return Err(ArtifactError::Forbidden.into());
        }
        if count > 1024 || bytes > 4 * 1024 * 1024 {
            return Err(ArtifactError::Corrupt.into());
        }
        let mut query=db.prepare("SELECT receipt_json IS NULL,CASE WHEN typeof(receipt_json)='text' AND length(CAST(receipt_json AS BLOB))<=1048576 THEN receipt_json END FROM tool_executions WHERE run_id=?1 AND artifact_ref=?2 LIMIT 1024")?;
        let mut rows = query.query(params![owner.run_id.0, reference])?;
        let (identity, bytes) = self.file_artifacts.read(reference, offset, limit)?;
        while let Some(row) = rows.next()? {
            if row.get::<_, bool>(0)? {
                continue;
            }
            let raw = row
                .get::<_, Option<String>>(1)?
                .ok_or(ArtifactError::Corrupt)?;
            let receipt: serde_json::Value =
                serde_json::from_str(&raw).map_err(|_| ArtifactError::Corrupt)?;
            if !receipt.is_object()
                || receipt
                    .get("output_sha256")
                    .is_some_and(|hash| hash.as_str() != Some(identity.sha256.as_str()))
                || receipt
                    .get("output_bytes")
                    .is_some_and(|size| size.as_u64() != Some(identity.bytes))
            {
                return Err(ArtifactError::Corrupt.into());
            }
        }
        drop(rows);
        drop(query);
        db.commit()?;
        Ok(bytes)
    }
    fn context_summary_scoped(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
    ) -> Result<SessionContextSummary, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let db = connection.transaction()?;
        enterprise::check_session_scope_in(&db, scope, key)?;
        let metadata = sessions::metadata_in(&db, key)?.ok_or(CoordinationError::Forbidden)?;
        sessions::lifetime_owner_in(&db, key, &metadata.lifetime)?;
        let snapshot = sessions::snapshot_in(&db, key)?;
        let projection = crate::context_projection::projection_in(&db, &snapshot)?;
        let summary = SessionContextSummary {
            session_key: key.clone(),
            lifetime: snapshot.lifetime,
            transcript_revision: snapshot.revision,
            projection_generation: projection.generation,
            projected_messages: projection.messages.len(),
        };
        db.commit()?;
        Ok(summary)
    }
}
fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 1024
}
#[async_trait::async_trait]
impl agent_storage_ports::RuntimeObservationRepository for RunStore {
    async fn read_run_transcript(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Vec<Message>, RepositoryError> {
        if !valid_id(&run.0) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, run) = (scope.clone(), run.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .run_transcript_scoped(&scope, &run)
                .map_err(RuntimeError::repository_error)
        })
        .await
    }
    async fn read_context_summary(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
    ) -> Result<SessionContextSummary, RepositoryError> {
        if !valid_id(&key.0) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, key) = (scope.clone(), key.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .context_summary_scoped(&scope, &key)
                .map_err(RuntimeError::repository_error)
        })
        .await
    }
    async fn read_legacy_artifact(
        &self,
        scope: &TenantContext,
        owner: &ExactOwner,
        reference: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<u8>, RepositoryError> {
        if !execution::valid_owner(owner)
            || reference.is_empty()
            || reference.len() > 16384
            || !(1..=16384).contains(&limit)
            || offset.checked_add(limit as u64).is_none()
        {
            return Err(ArtifactError::InvalidReference.into());
        }
        let (scope, owner, reference) = (scope.clone(), owner.clone(), reference.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .read_legacy_artifact_scoped(&scope, &owner, &reference, offset, limit)
                .map_err(RuntimeError::repository_error)
        })
        .await
    }
}
#[async_trait::async_trait]
impl agent_storage_ports::LegacyMemoryImportRepository for RunStore {
    async fn import_legacy_memory_file(
        &self,
        scope: &TenantContext,
        source: &str,
        bytes: &[u8],
    ) -> Result<(), RepositoryError> {
        if source.is_empty()
            || source.len() > 16384
            || bytes.len() > 16 * 1024 * 1024
            || bytes.iter().filter(|b| **b == b'\n').count() > 32768
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, source, bytes) = (scope.clone(), source.to_owned(), bytes.to_vec());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .import_legacy_memory_scoped(&scope, &source, &bytes)
                .map_err(RuntimeError::repository_error)
        })
        .await
    }
}
