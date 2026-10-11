//! 引用 prepared/发布证据；原 tool_executions 继续唯一保存工具终态。
use crate::{RunStore, RuntimeError, artifact_publication, enterprise, sessions};
use agent_core::*;
use agent_storage_ports::{ArtifactPublicationRepository, ArtifactStore};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};
fn mapped(error: RuntimeError) -> ArtifactError {
    match error {
        RuntimeError::Artifact(e) => e,
        RuntimeError::Coordination(CoordinationError::StaleLease) => ArtifactError::StaleWriter,
        RuntimeError::Coordination(CoordinationError::Forbidden) => ArtifactError::Forbidden,
        RuntimeError::Coordination(CoordinationError::Corrupt) => ArtifactError::Corrupt,
        RuntimeError::Authorization(_) | RuntimeError::Protocol(_) => ArtifactError::Forbidden,
        _ => ArtifactError::StorageUnavailable,
    }
}
fn sql<T>(_: T) -> ArtifactError {
    ArtifactError::StorageUnavailable
}
fn digest(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}
fn verify_completion(
    reservation: &ObjectArtifactReservation,
    completion: &ToolArtifactCompletion,
) -> Result<(), ArtifactError> {
    agent_storage_ports::verify_artifact_completion(reservation, completion)
}
fn fence(db: &Connection, reservation: &ObjectArtifactReservation) -> Result<(), ArtifactError> {
    enterprise::check_session_scope_in(
        db,
        &reservation.envelope.scope,
        &reservation.envelope.owner.session_key,
    )
    .map_err(mapped)?;
    sessions::fence_in(db, &reservation.envelope.owner).map_err(mapped)?;
    let raw: Option<String> = db
        .query_row(
            "SELECT snapshot_json FROM run_snapshots WHERE run_id=?1",
            [&reservation.envelope.owner.run_id.0],
            |r| r.get(0),
        )
        .optional()
        .map_err(sql)?;
    let snapshot: RunSnapshot = serde_json::from_str(&raw.ok_or(ArtifactError::Forbidden)?)
        .map_err(|_| ArtifactError::Corrupt)?;
    if snapshot.artifact_store_id.as_deref() != Some(reservation.store_id.as_str()) {
        return Err(ArtifactError::Forbidden);
    }
    Ok(())
}
fn decode(row: (String, String, bool)) -> Result<ObjectArtifactReservation, ArtifactError> {
    if digest(&row.0) != row.1 {
        return Err(ArtifactError::Corrupt);
    }
    let mut value: ObjectArtifactReservation =
        serde_json::from_str(&row.0).map_err(|_| ArtifactError::Corrupt)?;
    if !value.valid() || value.published {
        return Err(ArtifactError::Corrupt);
    }
    value.published = row.2;
    Ok(value)
}
fn current(
    db: &Connection,
    reservation: &ObjectArtifactReservation,
) -> Result<Option<ObjectArtifactReservation>, ArtifactError> {
    let scope = &reservation.envelope.scope;
    let result = db.query_row("SELECT reservation_json,reservation_digest,published FROM artifact_publications WHERE organization_id=?1 AND tenant_id=?2 AND project_id=?3 AND agent_uid=?4 AND run_id=?5 AND round=?6 AND call_id=?7",params![scope.organization_id.as_str(),scope.tenant_id.as_str(),scope.project_id.as_str(),scope.agent_uid.as_str(),reservation.envelope.owner.run_id.0,reservation.round as i64,reservation.call_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional().map_err(sql)?.map(decode).transpose()?;
    if let Some(value) = &result {
        validate_columns(db, value)?;
    }
    Ok(result)
}
fn validate_columns(db: &Connection, res: &ObjectArtifactReservation) -> Result<(), ArtifactError> {
    let scope = &res.envelope.scope;
    let valid:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM artifact_publications WHERE publication_id=?1 AND organization_id=?2 AND tenant_id=?3 AND project_id=?4 AND agent_uid=?5 AND run_id=?6 AND round=?7 AND call_id=?8 AND store_id=?9 AND sha256=?10 AND bytes=?11 AND published=?12)",params![res.publication_id,scope.organization_id.as_str(),scope.tenant_id.as_str(),scope.project_id.as_str(),scope.agent_uid.as_str(),res.envelope.owner.run_id.0,res.round as i64,res.call_id,res.store_id,res.identity.sha256,res.identity.bytes as i64,res.published],|r|r.get(0)).map_err(sql)?;
    if !valid {
        return Err(ArtifactError::Corrupt);
    }
    Ok(())
}
fn ensure_running(
    db: &Connection,
    reservation: &ObjectArtifactReservation,
) -> Result<(), ArtifactError> {
    let running:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM tool_executions WHERE run_id=?1 AND round=?2 AND call_id=?3 AND status='running')",params![reservation.envelope.owner.run_id.0,reservation.round as i64,reservation.call_id],|r|r.get(0)).map_err(sql)?;
    if !running {
        return Err(ArtifactError::Conflict);
    }
    Ok(())
}
type NativeToolEvidence = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
);
fn verify_published(
    db: &Connection,
    res: &ObjectArtifactReservation,
    completion: &ToolArtifactCompletion,
) -> Result<(), ArtifactError> {
    let row:Option<NativeToolEvidence>=db.query_row("SELECT status,call_id,outcome,artifact_ref,receipt_json FROM tool_executions WHERE run_id=?1 AND round=?2 AND call_id=?3", params![res.envelope.owner.run_id.0,res.round as i64,res.call_id], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional().map_err(sql)?;
    let Some((status, call, outcome, reference, receipt)) = row else {
        return Err(ArtifactError::Corrupt);
    };
    let receipt: serde_json::Value = serde_json::from_str(&receipt.ok_or(ArtifactError::Corrupt)?)
        .map_err(|_| ArtifactError::Corrupt)?;
    if status != "terminal"
        || call != completion.call_id
        || outcome.as_deref() != Some(completion.outcome.as_str())
        || reference.as_deref() != Some(res.reference().as_str())
        || receipt != completion.receipt
    {
        return Err(ArtifactError::Corrupt);
    }
    Ok(())
}
fn same(a: &ObjectArtifactReservation, b: &ObjectArtifactReservation) -> bool {
    let mut a = a.clone();
    let mut b = b.clone();
    a.published = false;
    b.published = false;
    a == b
}
impl RunStore {
    fn reserve_object_artifact_sync(
        &self,
        reservation: &ObjectArtifactReservation,
        completion: &ToolArtifactCompletion,
    ) -> Result<ObjectArtifactReservation, ArtifactError> {
        verify_completion(reservation, completion)?;
        if reservation.published {
            return Err(ArtifactError::InvalidReference);
        }
        let mut connection = self.lock_connection().map_err(mapped)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        fence(&tx, reservation)?;
        if let Some(old) = current(&tx, reservation)? {
            if !same(&old, reservation) {
                return Err(ArtifactError::Conflict);
            }
            if old.published {
                verify_published(&tx, &old, completion)?;
            } else {
                ensure_running(&tx, reservation)?;
            }
            fence(&tx, reservation)?;
            tx.commit().map_err(sql)?;
            return Ok(old);
        }
        ensure_running(&tx, reservation)?;
        let raw = serde_json::to_string(reservation).map_err(sql)?;
        let scope = &reservation.envelope.scope;
        tx.execute("INSERT INTO artifact_publications(publication_id,organization_id,tenant_id,project_id,agent_uid,run_id,round,call_id,store_id,sha256,bytes,reservation_json,reservation_digest) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",params![reservation.publication_id,scope.organization_id.as_str(),scope.tenant_id.as_str(),scope.project_id.as_str(),scope.agent_uid.as_str(),reservation.envelope.owner.run_id.0,reservation.round as i64,reservation.call_id,reservation.store_id,reservation.identity.sha256,reservation.identity.bytes as i64,raw,digest(&raw)]).map_err(sql)?;
        fence(&tx, reservation)?;
        tx.commit().map_err(sql)?;
        Ok(reservation.clone())
    }
    fn commit_object_artifact_sync(
        &self,
        reservation: &ObjectArtifactReservation,
        completion: &ToolArtifactCompletion,
        verified: &ArtifactIdentity,
    ) -> Result<(), ArtifactError> {
        verify_completion(reservation, completion)?;
        if verified != &reservation.identity {
            return Err(ArtifactError::Corrupt);
        }
        let mut connection = self.lock_connection().map_err(mapped)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        fence(&tx, reservation)?;
        let current = current(&tx, reservation)?.ok_or(ArtifactError::Forbidden)?;
        if !same(&current, reservation) {
            return Err(ArtifactError::Conflict);
        }
        if !current.published {
            artifact_publication::finish_in(
                &tx,
                &reservation.envelope.owner.run_id,
                completion.round,
                &completion.call_id,
                &completion.outcome,
                Some(&reservation.reference()),
                &completion.receipt,
            )
            .map_err(mapped)?;
            tx.execute(
                "UPDATE artifact_publications SET published=1 WHERE publication_id=?1",
                [&reservation.publication_id],
            )
            .map_err(sql)?;
        }
        verify_published(&tx, &current, completion)?;
        fence(&tx, reservation)?;
        tx.commit().map_err(sql)?;
        Ok(())
    }
    fn read_object_artifact_sync(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
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
        let mut connection = self.lock_connection().map_err(mapped)?;
        let tx = connection.transaction().map_err(sql)?;
        enterprise::check_session_scope_in(&tx, &envelope.scope, &envelope.owner.session_key)
            .map_err(mapped)?;
        sessions::fence_in(&tx, &envelope.owner).map_err(mapped)?;
        let scope = &envelope.scope;
        let row=tx.query_row("SELECT a.reservation_json,a.reservation_digest,a.published FROM artifact_publications a JOIN tool_executions t ON t.run_id=a.run_id AND t.round=a.round AND t.call_id=a.call_id WHERE a.organization_id=?1 AND a.tenant_id=?2 AND a.project_id=?3 AND a.agent_uid=?4 AND a.publication_id=?5 AND a.run_id=?6 AND t.status='terminal' AND t.artifact_ref=?7",params![scope.organization_id.as_str(),scope.tenant_id.as_str(),scope.project_id.as_str(),scope.agent_uid.as_str(),id,envelope.owner.run_id.0,reference],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional().map_err(sql)?.ok_or(ArtifactError::Forbidden)?;
        let reservation = decode(row)?;
        validate_columns(&tx, &reservation)?;
        fence(&tx, &reservation)?;
        if !reservation.published
            || &reservation.envelope != envelope
            || reservation.reference() != reference
        {
            return Err(ArtifactError::Forbidden);
        }
        let (outcome,raw):(String,String)=tx.query_row("SELECT outcome,receipt_json FROM tool_executions WHERE run_id=?1 AND round=?2 AND call_id=?3",params![envelope.owner.run_id.0,reservation.round as i64,reservation.call_id],|r|Ok((r.get(0)?,r.get(1)?))).map_err(sql)?;
        let receipt = serde_json::from_str(&raw).map_err(|_| ArtifactError::Corrupt)?;
        let completion = ToolArtifactCompletion {
            round: reservation.round,
            call_id: reservation.call_id.clone(),
            outcome,
            receipt,
        };
        verify_completion(&reservation, &completion).map_err(|error| {
            if error == ArtifactError::Conflict {
                ArtifactError::Corrupt
            } else {
                error
            }
        })?;
        tx.commit().map_err(sql)?;
        Ok(reservation)
    }
}
impl RunStore {
    fn publish_object_sync(
        &self,
        reservation: &ObjectArtifactReservation,
        body: &str,
    ) -> Result<ArtifactIdentity, ArtifactError> {
        if !reservation.valid()
            || reservation.store_id != self.file_artifacts.store_id()
            || reservation.envelope.scope != TenantContext::local()
        {
            return Err(ArtifactError::Forbidden);
        }
        if body.len() as u64 != reservation.identity.bytes
            || format!("{:x}", Sha256::digest(body.as_bytes())) != reservation.identity.sha256
        {
            return Err(ArtifactError::Corrupt);
        }
        let reference = self.file_artifacts.publish(body)?;
        let (identity, _) = self.file_artifacts.read(&reference, 0, 1)?;
        Ok(identity)
    }
    fn read_object_sync(
        &self,
        reservation: &ObjectArtifactReservation,
        offset: u64,
        limit: usize,
    ) -> Result<ArtifactPage, ArtifactError> {
        if !reservation.valid()
            || reservation.store_id != self.file_artifacts.store_id()
            || reservation.envelope.scope != TenantContext::local()
        {
            return Err(ArtifactError::Forbidden);
        }
        let reference = self.file_artifacts.reference(&reservation.identity)?;
        let (identity, data) = self.file_artifacts.read(&reference, offset, limit)?;
        if identity != reservation.identity {
            return Err(ArtifactError::Corrupt);
        }
        Ok(ArtifactPage { identity, data })
    }
}

impl RunStore {
    async fn artifact_blocking<T: Send + 'static>(
        &self,
        operation: impl FnOnce(Self) -> Result<T, ArtifactError> + Send + 'static,
    ) -> Result<T, ArtifactError> {
        self.sqlite_blocking(ArtifactError::StorageUnavailable, operation)
            .await
    }
}
#[async_trait::async_trait]
impl ArtifactPublicationRepository for RunStore {
    async fn reserve_object_artifact(
        &self,
        reservation: &ObjectArtifactReservation,
        completion: &ToolArtifactCompletion,
    ) -> Result<ObjectArtifactReservation, ArtifactError> {
        let reservation = reservation.clone();
        let completion = completion.clone();
        self.artifact_blocking(move |store| {
            store.reserve_object_artifact_sync(&reservation, &completion)
        })
        .await
    }
    async fn commit_object_artifact(
        &self,
        reservation: &ObjectArtifactReservation,
        completion: &ToolArtifactCompletion,
        verified: &ArtifactIdentity,
    ) -> Result<(), ArtifactError> {
        let reservation = reservation.clone();
        let completion = completion.clone();
        let verified = verified.clone();
        self.artifact_blocking(move |store| {
            store.commit_object_artifact_sync(&reservation, &completion, &verified)
        })
        .await
    }
    async fn read_object_artifact(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        reference: &str,
    ) -> Result<ObjectArtifactReservation, ArtifactError> {
        let envelope = envelope.clone();
        let reference = reference.to_owned();
        self.artifact_blocking(move |store| store.read_object_artifact_sync(&envelope, &reference))
            .await
    }
}
#[async_trait::async_trait]
impl ArtifactStore for RunStore {
    fn store_id(&self) -> String {
        self.file_artifacts.store_id()
    }
    async fn publish_object(
        &self,
        reservation: &ObjectArtifactReservation,
        body: &str,
    ) -> Result<ArtifactIdentity, ArtifactError> {
        let reservation = reservation.clone();
        let body = body.to_owned();
        self.artifact_blocking(move |store| store.publish_object_sync(&reservation, &body))
            .await
    }
    async fn read_object(
        &self,
        reservation: &ObjectArtifactReservation,
        offset: u64,
        limit: usize,
    ) -> Result<ArtifactPage, ArtifactError> {
        let reservation = reservation.clone();
        self.artifact_blocking(move |store| store.read_object_sync(&reservation, offset, limit))
            .await
    }
}
