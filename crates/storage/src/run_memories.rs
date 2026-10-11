//! 原记忆领域的异步端口；身份、可见性和保存事实在同一连接事务中复核。
use crate::{RunStore, RuntimeError, enterprise, execution, sessions};
use agent_core::*;
use rusqlite::{Connection, OptionalExtension, named_params, params};
use sha2::{Digest, Sha256};

const RECORD_BYTES: usize = 256 * 1024;
const PAGE_BYTES: usize = 4 * 1024 * 1024;

pub(crate) fn check_memory_proof_in(
    db: &Connection,
    owner: &ExactOwner,
    envelope: Option<&EnterpriseOwnerEnvelope>,
) -> Result<(), RuntimeError> {
    if let Some(envelope) = envelope {
        envelope
            .owner
            .fence(owner)
            .map_err(|_| AuthorizationError::StaleOwner)?;
        execution::check_execution_in(db, envelope)?;
        let _ = crate::requests::frozen_in(db, &owner.run_id)?;
    } else {
        enterprise::check_run_scope_in(db, &TenantContext::local(), &owner.run_id)?;
        sessions::fence_in(db, owner)?;
    }
    Ok(())
}
pub(crate) fn check_memory_read_in(
    db: &Connection,
    visibility: &MemoryVisibility,
    context: Option<(&TenantContext, &SessionKey)>,
) -> Result<(), RuntimeError> {
    if let Some((scope, key)) = context {
        enterprise::check_session_scope_in(db, scope, key)?;
        sessions::lifetime_owner_in(db, key, &visibility.lifetime)?;
        let active: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM session_heads WHERE session_id=?1 AND lifetime=?2 AND deleted=0)", params![key.0, visibility.lifetime.0], |r| r.get(0))?;
        if !active {
            return Err(AuthorizationError::StaleOwner.into());
        }
    }
    Ok(())
}

/// SQL source_run 是隔离依据；先屏蔽外域 payload，再校验保存 JSON 与原列绑定。
pub(crate) fn read_memory_in(
    db: &Connection,
    id: &str,
    scope: Option<&TenantContext>,
) -> Result<Option<MemoryRecord>, RuntimeError> {
    let local = TenantContext::local();
    let scope = scope.unwrap_or(&local);
    let allowed = "(t.organization_id=:organization AND s.tenant_id=:tenant AND s.project_id=:project AND s.agent_uid=:agent)";
    let sql = format!(
        "SELECT {allowed},CASE WHEN {allowed} AND typeof(m.data_json)='text' AND length(CAST(m.data_json AS BLOB)) BETWEEN 1 AND :budget THEN m.data_json END,CASE WHEN {allowed} AND length(CAST(m.lifetime AS BLOB))<=1024 THEN m.lifetime END,CASE WHEN {allowed} AND length(CAST(m.project AS BLOB))<=16384 THEN m.project END,CASE WHEN {allowed} AND length(CAST(m.scope AS BLOB))<=16 THEN m.scope END,CASE WHEN {allowed} AND length(CAST(m.source_run AS BLOB)) BETWEEN 1 AND 1024 THEN m.source_run END,CASE WHEN {allowed} THEN CASE WHEN typeof(m.revision)='integer' AND m.revision>=0 THEN m.revision END ELSE 0 END FROM memories m LEFT JOIN runs r ON r.id=m.source_run LEFT JOIN session_scopes s ON s.session_id=r.session_id LEFT JOIN tenants t ON t.id=s.tenant_id WHERE m.id=:id"
    );
    type Row = (
        Option<bool>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<u64>,
    );
    let row: Option<Row> = db.query_row(&sql, named_params!{":organization":scope.organization_id.as_str(),":tenant":scope.tenant_id.as_str(),":project":scope.project_id.as_str(),":agent":scope.agent_uid.as_str(),":budget":RECORD_BYTES,":id":id}, |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?))).optional()?;
    let Some((allowed, raw, lifetime, project, saved_scope, source_run, revision)) = row else {
        return Ok(None);
    };
    if allowed == Some(false) {
        return Err(AuthorizationError::Forbidden.into());
    }
    if allowed != Some(true) {
        return Err(CoordinationError::Corrupt.into());
    }
    let raw = raw.ok_or(CoordinationError::Corrupt)?;
    let revision = revision.ok_or(CoordinationError::Corrupt)?;
    let entry: MemoryRecord = serde_json::from_str(&raw).map_err(|_| CoordinationError::Corrupt)?;
    let source = entry.source.as_ref().ok_or(CoordinationError::Corrupt)?;
    let scope_bound = match &entry.scope {
        MemoryScope::Session(life) => {
            saved_scope.as_deref() == Some("session")
                && lifetime.as_ref() == Some(&life.0)
                && project.is_none()
        }
        MemoryScope::Project(identity) => {
            saved_scope.as_deref() == Some("project")
                && project.as_ref() == Some(identity)
                && lifetime.is_none()
                && entry.confirmed_by_user
        }
        MemoryScope::Global => {
            saved_scope.as_deref() == Some("global")
                && lifetime.is_none()
                && project.is_none()
                && entry.confirmed_by_user
                && entry.layer == MemoryLayer::Semantic
        }
        MemoryScope::Legacy(_) => false,
    };
    if entry.id != id
        || entry.id.len() > 512
        || entry.content.len() > 32768
        || entry.content.is_empty()
        || entry.confidence > 100
        || entry.revision != revision
        || !scope_bound
        || source_run.as_ref() != Some(&source.run_id.0)
        || entry.content_digest != format!("{:x}", Sha256::digest(entry.content.as_bytes()))
        || entry.source_message_ids.len() > 2048
        || entry.source_message_ids.iter().any(|id| id.len() > 512)
    {
        return Err(CoordinationError::Corrupt.into());
    }
    let owner = sessions::owner_in(db, &source.run_id)?;
    owner
        .fence(source)
        .map_err(|_| CoordinationError::Corrupt)?;
    sessions::lifetime_owner_in(db, &source.session_key, &source.session_lifetime_id)
        .map_err(|_| CoordinationError::Corrupt)?;
    Ok(Some(entry))
}

fn memory_list_in(
    db: &Connection,
    visibility: &MemoryVisibility,
    after: Option<&str>,
    limit: usize,
    paged: bool,
    context: Option<(&TenantContext, &SessionKey)>,
) -> Result<Vec<MemoryRecord>, RuntimeError> {
    check_memory_read_in(db, visibility, context)?;
    let local = TenantContext::local();
    let scope = context.map_or(&local, |(scope, _)| scope);
    let order = if paged { "m.id" } else { "m.rowid DESC" };
    // 在 LIMIT 前按原 source_run JOIN 全部隔离维度，跨域行不能挤占本页。
    let sql = format!(
        "SELECT CASE WHEN typeof(m.id)='text' AND length(CAST(m.id AS BLOB)) BETWEEN 1 AND 512 THEN m.id END,length(CAST(m.data_json AS BLOB)) FROM memories m JOIN runs r ON r.id=m.source_run JOIN session_scopes s ON s.session_id=r.session_id JOIN tenants t ON t.id=s.tenant_id WHERE t.organization_id=:organization AND s.tenant_id=:tenant AND s.project_id=:project AND s.agent_uid=:agent AND (:paged=0 OR m.id>:after) AND ((m.scope='session' AND m.lifetime=:lifetime) OR (m.scope='project' AND m.project=:workspace) OR (m.scope='global' AND :global AND CASE WHEN length(CAST(m.data_json AS BLOB))<=:budget AND json_valid(m.data_json) THEN json_extract(m.data_json,'$.confirmed_by_user')=1 AND json_extract(m.data_json,'$.layer')='semantic' ELSE 1 END)) AND CASE WHEN length(CAST(m.data_json AS BLOB))<=:budget AND json_valid(m.data_json) THEN json_extract(m.data_json,'$.expires_at') IS NULL OR json_extract(m.data_json,'$.expires_at')>:now ELSE 1 END ORDER BY {order} LIMIT :limit"
    );
    let mut query = db.prepare(&sql)?;
    let mut rows=query.query(named_params!{":organization":scope.organization_id.as_str(),":tenant":scope.tenant_id.as_str(),":project":scope.project_id.as_str(),":agent":scope.agent_uid.as_str(),":paged":paged,":after":after.unwrap_or(""),":lifetime":visibility.lifetime.0,":workspace":visibility.project,":global":visibility.allow_confirmed_global,":budget":RECORD_BYTES,":now":crate::now_ms()/1000,":limit":if paged{limit.clamp(1,101)}else{1000}})?;
    let mut entries = Vec::new();
    let mut bytes = 0usize;
    while let Some(row) = rows.next()? {
        let id = row
            .get::<_, Option<String>>(0)?
            .ok_or(CoordinationError::Corrupt)?;
        let size: u64 = row.get(1)?;
        if size > RECORD_BYTES as u64 {
            return Err(CoordinationError::Corrupt.into());
        }
        bytes = bytes.saturating_add(size as usize);
        if bytes > PAGE_BYTES {
            return Err(CoordinationError::Corrupt.into());
        }
        let entry = read_memory_in(db, &id, Some(scope))?.ok_or(CoordinationError::Corrupt)?;
        if !visibility.allows(&entry, crate::now_ms() as u64 / 1000) {
            return Err(CoordinationError::Corrupt.into());
        }
        entries.push(entry);
    }
    Ok(entries)
}
pub(crate) fn memory_list(
    store: &RunStore,
    visibility: &MemoryVisibility,
    after: Option<&str>,
    limit: usize,
    paged: bool,
    context: Option<(&TenantContext, &SessionKey)>,
) -> Result<Vec<MemoryRecord>, RuntimeError> {
    let mut connection = store.lock_connection()?;
    let db = connection.transaction()?;
    let entries = memory_list_in(&db, visibility, after, limit, paged, context)?;
    db.commit()?;
    Ok(entries)
}
impl RunStore {
    pub(crate) fn memory_assessments_scoped(
        &self,
        visibility: &MemoryVisibility,
        context: Option<(&TenantContext, &SessionKey)>,
    ) -> Result<Vec<MemoryAssessment>, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let db = connection.transaction()?;
        let candidates = memory_list_in(&db, visibility, None, 1000, false, context)?;
        let mut assessments = Vec::new();
        for candidate in candidates {
            let row:Option<(Option<String>,Option<String>)>=db.query_row("SELECT CASE WHEN length(CAST(content_digest AS BLOB))=64 THEN content_digest END,CASE WHEN length(CAST(feedback AS BLOB))<=64 THEN feedback END FROM memory_assessments WHERE lifetime=?1 AND memory_id=?2",params![visibility.lifetime.0,candidate.id],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
            if let Some((digest, feedback)) = row {
                let digest = digest.ok_or(CoordinationError::Corrupt)?;
                let feedback: MemoryFeedback =
                    serde_json::from_str(&feedback.ok_or(CoordinationError::Corrupt)?)
                        .map_err(|_| CoordinationError::Corrupt)?;
                assessments.push(MemoryAssessment {
                    memory_id: candidate.id,
                    content_digest: digest,
                    feedback,
                });
            }
        }
        db.commit()?;
        Ok(assessments)
    }
}
fn valid_read(key: &SessionKey, visibility: &MemoryVisibility) -> bool {
    !key.0.is_empty()
        && key.0.len() <= 1024
        && !visibility.lifetime.0.is_empty()
        && visibility.lifetime.0.len() <= 1024
        && visibility.project.len() <= 16384
}

#[async_trait::async_trait]
impl agent_storage_ports::ScopedMemoryRepository for RunStore {
    async fn read_memory_settings(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<RunSnapshot, RepositoryError> {
        if !execution::valid_owner(&envelope.owner) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let envelope = envelope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            let read = || -> Result<RunSnapshot, RuntimeError> {
                let mut connection = store.lock_connection()?;
                let db = connection.transaction()?;
                execution::check_execution_in(&db, &envelope)?;
                let frozen = crate::requests::frozen_in(&db, &envelope.owner.run_id)?;
                db.commit()?;
                Ok(frozen)
            };
            read().map_err(execution::execution_error)
        })
        .await
    }
    async fn read_memory_candidates(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        visibility: &MemoryVisibility,
    ) -> Result<Vec<MemoryRecord>, RepositoryError> {
        if !valid_read(key, visibility) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, key, visibility) = (scope.clone(), key.clone(), visibility.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            memory_list(&store, &visibility, None, 1000, false, Some((&scope, &key)))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn read_memory_page(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        visibility: &MemoryVisibility,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MemoryRecord>, RepositoryError> {
        if !valid_read(key, visibility) || after.is_some_and(|id| id.len() > 512) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, key, visibility, after) = (
            scope.clone(),
            key.clone(),
            visibility.clone(),
            after.map(str::to_owned),
        );
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            memory_list(
                &store,
                &visibility,
                after.as_deref(),
                limit,
                true,
                Some((&scope, &key)),
            )
            .map_err(execution::execution_error)
        })
        .await
    }
    async fn read_memory_evidence(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        visibility: &MemoryVisibility,
        id: &str,
        after: usize,
        limit: usize,
    ) -> Result<MemoryEvidencePage, RepositoryError> {
        if !valid_read(key, visibility) || id.is_empty() || id.len() > 512 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, key, visibility, id) = (
            scope.clone(),
            key.clone(),
            visibility.clone(),
            id.to_owned(),
        );
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .memory_evidence_scoped(&visibility, &id, after, limit, Some((&scope, &key)))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn read_memory_assessments(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        visibility: &MemoryVisibility,
    ) -> Result<Vec<MemoryAssessment>, RepositoryError> {
        if !valid_read(key, visibility) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, key, visibility) = (scope.clone(), key.clone(), visibility.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .memory_assessments_scoped(&visibility, Some((&scope, &key)))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn read_memory_flywheel(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
    ) -> Result<serde_json::Value, RepositoryError> {
        let visibility = MemoryVisibility {
            lifetime: lifetime.clone(),
            project: String::new(),
            allow_confirmed_global: false,
        };
        if !valid_read(key, &visibility) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, key, lifetime) = (scope.clone(), key.clone(), lifetime.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .flywheel_report_scoped(&lifetime, Some((&scope, &key)))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn store_run_memory(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        entry: &MemoryRecord,
    ) -> Result<MemoryRecord, RepositoryError> {
        if !execution::valid_owner(&envelope.owner)
            || entry.id.is_empty()
            || entry.id.len() > 512
            || entry.source.as_ref() != Some(&envelope.owner)
            || !execution::within_json_budget(entry, RECORD_BYTES)
            || entry.source_message_ids.len() > 2048
            || entry.source_message_ids.iter().any(|id| id.len() > 512)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, entry) = (envelope.clone(), entry.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .store_memory_scoped(&envelope.owner, &entry, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn forget_run_memory(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        visibility: &MemoryVisibility,
        id: &str,
        revision: u64,
    ) -> Result<bool, RepositoryError> {
        if !execution::valid_owner(&envelope.owner)
            || !valid_read(&envelope.owner.session_key, visibility)
            || visibility.lifetime != envelope.owner.session_lifetime_id
            || id.is_empty()
            || id.len() > 512
            || revision > i64::MAX as u64
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, visibility, id) = (envelope.clone(), visibility.clone(), id.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .forget_memory_scoped(&envelope.owner, &visibility, &id, revision, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn record_run_memory_exposures(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        entries: &[MemoryExposure],
        channel: &str,
        policy: &str,
    ) -> Result<(), RepositoryError> {
        if !execution::valid_owner(&envelope.owner)
            || entries.len() > 20
            || !execution::within_json_budget(&entries, 16384)
            || channel.len() > 80
            || policy.len() > 80
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, entries, channel, policy) = (
            envelope.clone(),
            entries.to_vec(),
            channel.to_owned(),
            policy.to_owned(),
        );
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .record_memory_exposures_scoped(
                    &envelope.owner,
                    &entries,
                    &channel,
                    &policy,
                    Some(&envelope),
                )
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn record_run_memory_feedback(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        operation: &str,
        id: &str,
        feedback: MemoryFeedback,
    ) -> Result<(), RepositoryError> {
        if !execution::valid_owner(&envelope.owner)
            || operation.is_empty()
            || operation.len() > 128
            || id.is_empty()
            || id.len() > 512
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, operation, id) = (envelope.clone(), operation.to_owned(), id.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .memory_feedback_scoped(&envelope.owner, &operation, &id, feedback, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn ingest_run_memory(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<(), RepositoryError> {
        if !execution::valid_owner(&envelope.owner) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let envelope = envelope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .ingest_memory_scoped(&envelope.owner, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn record_memory_maintenance(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        outcome: &str,
    ) -> Result<(), RepositoryError> {
        if !execution::valid_owner(&envelope.owner)
            || !matches!(outcome, "completed" | "failed" | "skipped_read_only")
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, outcome) = (envelope.clone(), outcome.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .write_memory_maintenance_scoped(&envelope, &outcome)
                .map_err(execution::execution_error)
        })
        .await
    }
}

impl RunStore {
    pub(crate) fn write_memory_maintenance_scoped(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        outcome: &str,
    ) -> Result<(), RuntimeError> {
        let mut connection = self.lock_connection()?;
        let db = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        execution::check_execution_in(&db, envelope)?;
        crate::ports::maintenance_diagnostic_in(&db, &envelope.owner, "memory_ingest", outcome)?;
        db.commit()?;
        Ok(())
    }
}
#[async_trait::async_trait]
impl agent_storage_ports::MemoryMaintenanceRepository for RunStore {
    async fn maintain_memory_turns(
        &self,
        scope: &TenantContext,
        limit: usize,
    ) -> Result<(usize, Option<i64>), RepositoryError> {
        let scope = scope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            let completed = store
                .recover_memory_ingests_scoped(&scope, limit, true)
                .map_err(execution::execution_error)?;
            let next_due = store
                .next_memory_ingest_due_scoped(&scope)
                .map_err(execution::execution_error)?;
            Ok((completed, next_due))
        })
        .await
    }
}
