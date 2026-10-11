//! 原interaction发布、回答与版本投影，无第二套审批状态。
use crate::{RunStore, RuntimeError, enterprise, execution, sessions};
use agent_core::*;
use rusqlite::{Connection, OptionalExtension, params};

pub(crate) fn interaction_anchor_in(
    db: &Connection,
    scope: &TenantContext,
    id: &InteractionId,
) -> Result<Option<ExactOwner>, RuntimeError> {
    type Header = (Option<bool>, Option<String>, Option<bool>);
    let header:Option<Header>=db.query_row("SELECT t.organization_id=?2 AND s.tenant_id=?3 AND s.project_id=?4 AND s.agent_uid=?5,CASE WHEN t.organization_id=?2 AND s.tenant_id=?3 AND s.project_id=?4 AND s.agent_uid=?5 AND length(CAST(r.id AS BLOB)) BETWEEN 1 AND 1024 AND length(CAST(r.session_id AS BLOB)) BETWEEN 1 AND 1024 AND length(CAST(r.lifetime AS BLOB)) BETWEEN 1 AND 1024 THEN r.id END,i.lifetime=r.lifetime AND i.generation=r.generation FROM interactions i LEFT JOIN runs r ON r.id=i.run_id LEFT JOIN session_scopes s ON s.session_id=r.session_id LEFT JOIN tenants t ON t.id=s.tenant_id WHERE i.id=?1",params![id.0,scope.organization_id.as_str(),scope.tenant_id.as_str(),scope.project_id.as_str(),scope.agent_uid.as_str()],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    let Some((visible, run, bound)) = header else {
        return Ok(None);
    };
    if visible == Some(false) {
        return Err(AuthorizationError::Forbidden.into());
    }
    if visible != Some(true) || bound != Some(true) {
        return Err(CoordinationError::Corrupt.into());
    }
    let owner = sessions::owner_in(db, &RunId(run.ok_or(CoordinationError::Corrupt)?))?;
    if !execution::valid_owner(&owner) {
        return Err(CoordinationError::Corrupt.into());
    }
    sessions::lifetime_owner_in(db, &owner.session_key, &owner.session_lifetime_id)?;
    Ok(Some(owner))
}
fn valid_id(id: &InteractionId) -> bool {
    !id.0.is_empty() && id.0.len() <= 1024
}
impl RunStore {
    pub(crate) fn read_interaction_scoped(
        &self,
        scope: &TenantContext,
        id: &InteractionId,
    ) -> Result<Option<InteractionRecord>, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let db = connection.transaction()?;
        if interaction_anchor_in(&db, scope, id)?.is_none() {
            return Ok(None);
        }
        let record = crate::read_interaction_in(&db, &id.0)?;
        db.commit()?;
        Ok(record)
    }
    pub(crate) fn list_interactions_scoped(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
    ) -> Result<Vec<InteractionRecord>, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let db = connection.transaction()?;
        enterprise::check_session_scope_in(&db, scope, key)?;
        let meta = sessions::metadata_in(&db, key)?.ok_or(CoordinationError::Conflict)?;
        if meta.deleted {
            return Ok(Vec::new());
        }
        sessions::lifetime_owner_in(&db, key, &meta.lifetime)?;
        let (count,bytes):(u64,u64)=db.query_row("SELECT count(*),COALESCE(sum(length(CAST(i.id AS BLOB))+length(CAST(i.prompt AS BLOB))+length(CAST(i.payload_json AS BLOB))+COALESCE(length(CAST(i.response_json AS BLOB)),0)),0) FROM (SELECT i.* FROM interactions i JOIN runs r ON r.id=i.run_id WHERE r.session_id=?1 AND r.lifetime=?2 ORDER BY i.rowid LIMIT 1001) i",params![key.0,meta.lifetime.0],|r|Ok((r.get(0)?,r.get(1)?)))?;
        if count > 1000 || bytes > 1024 * 1024 - 16384 {
            return Err(CoordinationError::Corrupt.into());
        }
        let mut query=db.prepare("SELECT CASE WHEN length(CAST(i.id AS BLOB)) BETWEEN 1 AND 1024 THEN i.id END FROM interactions i JOIN runs r ON r.id=i.run_id WHERE r.session_id=?1 AND r.lifetime=?2 ORDER BY i.rowid LIMIT 1000")?;
        let mut rows = query.query(params![key.0, meta.lifetime.0])?;
        let mut records = Vec::new();
        while let Some(row) = rows.next()? {
            let id = InteractionId(
                row.get::<_, Option<String>>(0)?
                    .ok_or(CoordinationError::Corrupt)?,
            );
            let _ = interaction_anchor_in(&db, scope, &id)?.ok_or(CoordinationError::Corrupt)?;
            records
                .push(crate::read_interaction_in(&db, &id.0)?.ok_or(CoordinationError::Corrupt)?);
        }
        drop(rows);
        drop(query);
        db.commit()?;
        Ok(records)
    }
    pub(crate) fn interaction_stamp_scoped(
        &self,
        scope: &TenantContext,
        id: &InteractionId,
    ) -> Result<Option<ViewStamp>, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let db = connection.transaction()?;
        let Some(owner) = interaction_anchor_in(&db, scope, id)? else {
            return Ok(None);
        };
        let corrupt:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM events WHERE run_id=?1 AND event='interaction_resolved' AND (typeof(data_json)<>'text' OR length(CAST(data_json AS BLOB))>65536 OR NOT json_valid(data_json)))",params![owner.run_id.0],|r|r.get(0))?;
        if corrupt {
            return Err(CoordinationError::Corrupt.into());
        }
        let row:Option<u64>=db.query_row("SELECT e.seq FROM events e WHERE e.run_id=?1 AND e.event='interaction_resolved' AND CASE WHEN typeof(e.data_json)='text' AND length(CAST(e.data_json AS BLOB))<=65536 AND json_valid(e.data_json) THEN json_extract(e.data_json,'$.interaction_id')=?2 ELSE 0 END ORDER BY e.seq DESC LIMIT 1",params![owner.run_id.0,id.0],|r|r.get(0)).optional()?;
        let stamp = if let Some(seq) = row {
            let size:Option<u64>=db.query_row("SELECT length(CAST(stamp_json AS BLOB)) FROM event_view_stamps WHERE run_id=?1 AND seq=?2",params![owner.run_id.0,seq],|r|r.get(0)).optional()?;
            if size.is_some_and(|size| size > 16384) {
                return Err(CoordinationError::Corrupt.into());
            }
            let stamp = crate::views::stamp_in(&db, &owner.run_id, EventSeq(seq))?;
            let native: bool = db.query_row(
                "SELECT view_provenance='native' FROM events WHERE run_id=?1 AND seq=?2",
                params![owner.run_id.0, seq],
                |r| r.get(0),
            )?;
            if native && stamp.is_none() {
                return Err(CoordinationError::Corrupt.into());
            }
            stamp
        } else {
            None
        };
        db.commit()?;
        Ok(stamp)
    }
}
#[async_trait::async_trait]
impl agent_storage_ports::RunInteractionRepository for RunStore {
    async fn read_interaction_record(
        &self,
        scope: &TenantContext,
        id: &InteractionId,
    ) -> Result<Option<InteractionRecord>, RepositoryError> {
        if !valid_id(id) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, id) = (scope.clone(), id.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .read_interaction_scoped(&scope, &id)
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn read_interaction_list(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
    ) -> Result<Vec<InteractionRecord>, RepositoryError> {
        if key.0.is_empty() || key.0.len() > 1024 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, key) = (scope.clone(), key.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .list_interactions_scoped(&scope, &key)
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn respond_interaction_authorized(
        &self,
        request: &AuthorizationRequest,
        envelope: &EnterpriseOwnerEnvelope,
        id: &InteractionId,
        revision: i64,
        approved: bool,
        verifier: std::sync::Arc<dyn AuthorizationVerifier>,
    ) -> Result<InteractionRecord, RepositoryError> {
        if !execution::valid_owner(&envelope.owner)
            || !valid_id(id)
            || revision < 0
            || revision == i64::MAX
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (request, envelope, id) = (request.clone(), envelope.clone(), id.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .claim_interaction_authorized(
                    &id,
                    &envelope.owner.session_key,
                    &envelope.owner.run_id,
                    revision,
                    approved,
                    Some(&envelope),
                    Some((&request, verifier.as_ref())),
                )
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn respond_run_interaction(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        id: &InteractionId,
        revision: i64,
        approved: bool,
    ) -> Result<InteractionRecord, RepositoryError> {
        if !execution::valid_owner(&envelope.owner)
            || !valid_id(id)
            || revision < 0
            || revision == i64::MAX
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, id) = (envelope.clone(), id.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .claim_interaction_scoped(
                    &id,
                    &envelope.owner.session_key,
                    &envelope.owner.run_id,
                    revision,
                    approved,
                    Some(&envelope),
                )
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn read_interaction_stamp(
        &self,
        scope: &TenantContext,
        id: &InteractionId,
    ) -> Result<Option<ViewStamp>, RepositoryError> {
        if !valid_id(id) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, id) = (scope.clone(), id.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .interaction_stamp_scoped(&scope, &id)
                .map_err(execution::execution_error)
        })
        .await
    }
}

pub(crate) enum VoteProgress {
    Duplicate,
    Waiting,
    Resolve,
}
#[allow(clippy::too_many_arguments)]
pub(crate) fn vote_in(
    db: &Connection,
    request: &AuthorizationRequest,
    env: &EnterpriseOwnerEnvelope,
    id: &InteractionId,
    revision: i64,
    approved: bool,
    current: &InteractionRecord,
    dual: bool,
) -> Result<VoteProgress, RuntimeError> {
    let s = &env.scope;
    let p = &request.context.principal;
    let raw = serde_json::to_string(&request.context.request_id)
        .map_err(|_| CoordinationError::InvalidRequest)?;
    if raw.len() > 4096 {
        return Err(CoordinationError::InvalidRequest.into());
    }
    let saved:Option<(u64,i64,bool,String)>=db.query_row("SELECT revocation_epoch,expected_revision,approved,request_json FROM interaction_votes WHERE organization_id=?1 AND tenant_id=?2 AND project_id=?3 AND agent_uid=?4 AND interaction_id=?5 AND subject_id=?6",params![s.organization_id.as_str(),s.tenant_id.as_str(),s.project_id.as_str(),s.agent_uid.as_str(),id.0,p.subject_id.as_str()],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
    if let Some(saved) = saved {
        if saved != (p.revocation_epoch, revision, approved, raw) {
            return Err(CoordinationError::Conflict.into());
        }
        return Ok(VoteProgress::Duplicate);
    }
    if current.status != "pending" || current.revision != revision {
        return Err(CoordinationError::Conflict.into());
    }
    let state: String = db.query_row(
        "SELECT status FROM runs WHERE id=?1",
        [&env.owner.run_id.0],
        |r| r.get(0),
    )?;
    if state != "waiting_interaction" {
        return Err(CoordinationError::Conflict.into());
    }
    let at = crate::now_ms();
    db.execute(
        "INSERT INTO interaction_votes VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
        params![
            s.organization_id.as_str(),
            s.tenant_id.as_str(),
            s.project_id.as_str(),
            s.agent_uid.as_str(),
            id.0,
            p.subject_id.as_str(),
            p.revocation_epoch,
            revision,
            approved,
            raw,
            at
        ],
    )?;
    crate::ledgers::append_audit_in(
        db,
        &AuditCommand {
            scope: s.clone(),
            operation_id: {
                use sha2::{Digest, Sha256};
                format!(
                    "vote:{:x}",
                    Sha256::digest(
                        serde_json::to_vec(&(env, id, &p.subject_id))
                            .map_err(|_| CoordinationError::InvalidRequest)?
                    )
                )
            },
            subject_id: p.subject_id.clone(),
            action: "interaction.vote".into(),
            resource: serde_json::json!({"owner":env.owner,"interaction_id":id,"revision":revision,"approved":approved}),
            outcome: if approved { "approved" } else { "rejected" }.into(),
        },
        at,
    )?;
    if !approved || !dual {
        return Ok(VoteProgress::Resolve);
    }
    let votes:i64=db.query_row("SELECT count(*) FROM interaction_votes v JOIN principals p ON p.tenant_id=v.tenant_id AND p.subject_id=v.subject_id AND p.revocation_epoch=v.revocation_epoch WHERE v.organization_id=?1 AND v.tenant_id=?2 AND v.project_id=?3 AND v.agent_uid=?4 AND v.interaction_id=?5 AND v.approved=1",params![s.organization_id.as_str(),s.tenant_id.as_str(),s.project_id.as_str(),s.agent_uid.as_str(),id.0],|r|r.get(0))?;
    if votes >= 2 {
        return Ok(VoteProgress::Resolve);
    }
    let changed=db.execute("UPDATE interactions SET revision=revision+1 WHERE id=?1 AND revision=?2 AND status='pending'",params![id.0,revision])?;
    if changed != 1 {
        return Err(CoordinationError::Conflict.into());
    }
    Ok(VoteProgress::Waiting)
}
