//! 会话与 transcript 唯一持久写协议，所有 CAS 在同一 SQLite 写锁/事务内。
use super::{RunStore, RuntimeError, now_ms};
use agent_core::*;
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};

pub trait SessionQuery: Send + Sync {
    fn session_preview(
        &self,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
    ) -> Result<Option<String>, RuntimeError>;
    fn preferred_session(&self) -> Result<Option<SessionKey>, RuntimeError>;
    fn run_messages(&self, run: &RunId) -> Result<Vec<Message>, RuntimeError>;
    fn session_metadata(&self, key: &SessionKey) -> Result<Option<SessionMetadata>, RuntimeError>;
    fn session_metadata_page(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SessionMetadata>, RuntimeError>;
    fn session_metadata_list(&self) -> Result<Vec<SessionMetadata>, RuntimeError>;
    fn session_snapshot(&self, key: &SessionKey) -> Result<SessionSnapshot, RuntimeError>;
    fn session_readback(
        &self,
        key: &SessionKey,
        mode: HistoryReadMode,
    ) -> Result<SessionReadback, RuntimeError>;
    fn run_owner(&self, run: &RunId) -> Result<ExactOwner, RuntimeError>;
}
pub trait SessionLifecycle: Send + Sync {
    fn close_receipt(
        &self,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        operation: &str,
    ) -> Result<Option<SessionMetadata>, RuntimeError>;
    fn execute_lifecycle(
        &self,
        command: &SessionCommand,
        expected: Option<&SessionLifetimeId>,
        operation: &str,
    ) -> Result<SessionMetadata, RuntimeError>;
    fn set_preferred_session(&self, key: &SessionKey) -> Result<(), RuntimeError>;
    fn create_session(&self, key: &SessionKey) -> Result<SessionMetadata, RuntimeError>;
    fn end_session(
        &self,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        delete: bool,
    ) -> Result<(), RuntimeError>;
    fn fork_session(
        &self,
        source: &SessionKey,
        target: &SessionKey,
        revision: TranscriptSeq,
    ) -> Result<SessionMetadata, RuntimeError>;
}
pub trait TranscriptStore: Send + Sync {
    fn import_legacy(&self, key: &SessionKey, messages: &[Message]) -> Result<(), RuntimeError>;
    fn append_transcript(
        &self,
        owner: &ExactOwner,
        operation: &str,
        messages: &[Message],
    ) -> Result<TranscriptSeq, RuntimeError>;
}

pub(crate) fn metadata_in(
    db: &Connection,
    key: &SessionKey,
) -> Result<Option<SessionMetadata>, RuntimeError> {
    Ok(db.query_row("SELECT lifetime, deleted, revision, updated_at_ms,legacy_imported FROM session_heads WHERE session_id=?1",params![key.0], |r| Ok(SessionMetadata{key:key.clone(),lifetime:SessionLifetimeId(r.get(0)?),deleted:r.get(1)?,legacy_imported:r.get(4)?,revision:TranscriptSeq(r.get(2)?),updated_at_ms:r.get(3)?})).optional()?)
}
pub(crate) fn scoped_metadata_in(
    db: &Connection,
    scope: &TenantContext,
    key: &SessionKey,
) -> Result<SessionMetadata, RuntimeError> {
    db.query_row("SELECT h.lifetime,h.deleted,h.revision,h.updated_at_ms,h.legacy_imported FROM session_heads h JOIN session_scopes s ON s.session_id=h.session_id JOIN tenants t ON t.id=s.tenant_id WHERE t.organization_id=?1 AND s.tenant_id=?2 AND s.project_id=?3 AND s.agent_uid=?4 AND h.session_id=?5 AND h.deleted=0", params![scope.organization_id.as_str(),scope.tenant_id.as_str(),scope.project_id.as_str(),scope.agent_uid.as_str(),key.0], |r|Ok(SessionMetadata{key:key.clone(),lifetime:SessionLifetimeId(r.get(0)?),deleted:r.get(1)?,revision:TranscriptSeq(r.get(2)?),updated_at_ms:r.get(3)?,legacy_imported:r.get(4)?}))
        .optional()?.ok_or_else(|| AuthorizationError::Forbidden.into())
}

pub(crate) fn lifetime_owner_in(
    db: &Connection,
    key: &SessionKey,
    life: &SessionLifetimeId,
) -> Result<(), RuntimeError> {
    let valid = db.query_row("SELECT 1 FROM session_lifetime_owners WHERE lifetime=?1 AND session_id=?2 AND provenance='canonical'",params![life.0,key.0],|r|r.get::<_,i64>(0)).optional()?;
    if valid.is_none() {
        return Err(CoordinationError::Corrupt.into());
    }
    Ok(())
}

pub(crate) fn metadata_page_in(
    db: &Connection,
    scope: &TenantContext,
    after: Option<&str>,
    limit: usize,
    newest_first: bool,
) -> Result<Vec<SessionMetadata>, RuntimeError> {
    let order = if newest_first {
        "h.updated_at_ms DESC,h.session_id"
    } else {
        "h.session_id"
    };
    let sql = format!(
        "SELECT h.session_id,h.lifetime,h.deleted,h.revision,h.updated_at_ms,h.legacy_imported FROM session_heads h JOIN session_scopes s ON s.session_id=h.session_id JOIN tenants t ON t.id=s.tenant_id WHERE t.organization_id=?1 AND s.tenant_id=?2 AND s.project_id=?3 AND s.agent_uid=?4 AND h.deleted=0 AND h.session_id>?5 ORDER BY {order} LIMIT ?6"
    );
    let mut q = db.prepare(&sql)?;
    let rows = q
        .query_map(
            params![
                scope.organization_id.as_str(),
                scope.tenant_id.as_str(),
                scope.project_id.as_str(),
                scope.agent_uid.as_str(),
                after.unwrap_or(""),
                limit.clamp(1, 1001) as i64
            ],
            |r| {
                Ok(SessionMetadata {
                    key: SessionKey(r.get(0)?),
                    lifetime: SessionLifetimeId(r.get(1)?),
                    deleted: r.get(2)?,
                    revision: TranscriptSeq(r.get(3)?),
                    updated_at_ms: r.get(4)?,
                    legacy_imported: r.get(5)?,
                })
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

pub(crate) fn preview_in(
    db: &Connection,
    key: &SessionKey,
    life: &SessionLifetimeId,
) -> Result<Option<String>, RuntimeError> {
    lifetime_owner_in(db, key, life)?;
    Ok(db.query_row("SELECT substr(json_extract(m.value,'$.content'),1,160) FROM transcript_batches b JOIN session_lifetime_owners o ON o.lifetime=b.lifetime,json_each(b.messages_json) m WHERE b.lifetime=?1 AND o.session_id=?2 AND json_extract(m.value,'$.role')='user' AND json_extract(m.value,'$.content') IS NOT NULL ORDER BY b.start_seq,m.key LIMIT 1",params![life.0,key.0],|r|r.get(0)).optional()?)
}
pub(crate) fn owner_in(db: &Connection, run: &RunId) -> Result<ExactOwner, RuntimeError> {
    Ok(db.query_row("SELECT r.session_id, r.lifetime, r.generation, t.id FROM runs r JOIN turns t ON t.run_id=r.id WHERE r.id=?1",params![run.0],|r|Ok(ExactOwner{session_key:SessionKey(r.get(0)?),session_lifetime_id:SessionLifetimeId(r.get(1)?),run_id:run.clone(),run_generation:RunGeneration(r.get(2)?),turn_id:TurnId(r.get(3)?)}))?)
}
pub(crate) fn fence_in(db: &Connection, owner: &ExactOwner) -> Result<(), RuntimeError> {
    let meta = metadata_in(db, &owner.session_key)?
        .ok_or_else(|| RuntimeError::Protocol("session 不存在".into()))?;
    if meta.deleted || meta.lifetime != owner.session_lifetime_id {
        return Err(RuntimeError::Protocol("stale session lifetime".into()));
    }
    owner_in(db, &owner.run_id)?
        .fence(owner)
        .map_err(|e| RuntimeError::Protocol(e.to_string()))?;
    super::writer::fence_in(db, &owner.session_key, &owner.session_lifetime_id)?;
    crate::enterprise::authorization_fence_in(db, owner)
}
pub(crate) fn create_in(
    db: &Connection,
    key: &SessionKey,
) -> Result<SessionMetadata, RuntimeError> {
    if let Some(meta) = metadata_in(db, key)? {
        if !meta.deleted {
            return Err(RuntimeError::Protocol("session 已存在".into()));
        }
        super::writer::acquire_in(db, key, &meta.lifetime)?;
        db.execute("UPDATE session_heads SET lifetime=lower(hex(randomblob(16))), deleted=0, revision=0, projection_generation=0, legacy_imported=1, updated_at_ms=?2 WHERE session_id=?1",params![key.0,now_ms()])?;
    } else {
        db.execute(
            "INSERT INTO sessions(id,created_at_ms) VALUES (?1,?2)",
            params![key.0, now_ms()],
        )?;
        db.execute(
            "UPDATE session_heads SET legacy_imported=1 WHERE session_id=?1",
            params![key.0],
        )?;
    }
    let metadata = metadata_in(db, key)?
        .ok_or_else(|| RuntimeError::Internal("缺少创建后的 session".into()))?;
    super::writer::acquire_in(db, key, &metadata.lifetime)?;
    super::registry::bind_in(db,key,&metadata.lifetime)?;
    super::hooks::publish_in(db, &metadata, HookEvent::SessionStart)?;
    Ok(metadata)
}
pub(crate) fn insert_batch(
    db: &Connection,
    key: &SessionKey,
    lifetime: &SessionLifetimeId,
    operation: &str,
    run: Option<&RunId>,
    messages: &[Message],
) -> Result<TranscriptSeq, RuntimeError> {
    lifetime_owner_in(db, key, lifetime)?;
    super::writer::acquire_in(db, key, lifetime)?;
    let operation = run.map_or_else(
        || operation.to_owned(),
        |run| format!("{}:{operation}", run.0),
    );
    let payload =
        serde_json::to_string(messages).map_err(|e| RuntimeError::Protocol(e.to_string()))?;
    let digest = format!("{:x}", Sha256::digest(payload.as_bytes()));
    if let Some((old, end)) = db
        .query_row(
            "SELECT digest,end_seq FROM transcript_batches WHERE lifetime=?1 AND operation_id=?2",
            params![lifetime.0, operation],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, u64>(1)?)),
        )
        .optional()?
    {
        if old != digest {
            return Err(RuntimeError::Protocol(
                "transcript operation id 冲突".into(),
            ));
        }
        return Ok(TranscriptSeq(end));
    }
    let start: u64 = db.query_row(
        "SELECT revision FROM session_heads WHERE session_id=?1",
        params![key.0],
        |r| r.get(0),
    )?;
    let end = start
        .checked_add(messages.len() as u64)
        .filter(|n| *n <= i64::MAX as u64)
        .ok_or_else(|| RuntimeError::Protocol("transcript revision 耗尽".into()))?;
    db.execute("INSERT INTO transcript_batches(lifetime,operation_id,run_id,start_seq,end_seq,messages_json,digest) VALUES (?1,?2,?3,?4,?5,?6,?7)",params![lifetime.0,operation,run.map(|r|r.0.as_str()),start,end,payload,digest])?;
    db.execute(
        "UPDATE session_heads SET revision=?2,updated_at_ms=?3 WHERE session_id=?1",
        params![key.0, end, now_ms()],
    )?;
    Ok(TranscriptSeq(end))
}
impl SessionQuery for RunStore {
    fn session_preview(
        &self,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
    ) -> Result<Option<String>, RuntimeError> {
        let db = self.lock_connection()?;
        if &scoped_metadata_in(&db, &TenantContext::local(), key)?.lifetime != lifetime {
            return Err(RuntimeError::Protocol(
                "session list projection stale".into(),
            ));
        }
        preview_in(&db, key, lifetime)
    }
    fn preferred_session(&self) -> Result<Option<SessionKey>, RuntimeError> {
        self.selected_session_scoped(&TenantContext::local())
    }
    fn run_messages(&self, run: &RunId) -> Result<Vec<Message>, RuntimeError> {
        self.run_transcript_scoped(&TenantContext::local(), run)
    }
    fn session_metadata(&self, key: &SessionKey) -> Result<Option<SessionMetadata>, RuntimeError> {
        metadata_in(&*self.lock_connection()?, key)
    }
    fn session_metadata_page(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SessionMetadata>, RuntimeError> {
        let db = self.lock_connection()?;
        metadata_page_in(&db, &TenantContext::local(), after, limit, false)
    }
    fn session_metadata_list(&self) -> Result<Vec<SessionMetadata>, RuntimeError> {
        let db = self.lock_connection()?;
        metadata_page_in(&db, &TenantContext::local(), None, 1000, true)
    }
    fn session_readback(
        &self,
        key: &SessionKey,
        mode: HistoryReadMode,
    ) -> Result<SessionReadback, RuntimeError> {
        self.readback(key, mode)
    }
    fn run_owner(&self, run: &RunId) -> Result<ExactOwner, RuntimeError> {
        owner_in(&*self.lock_connection()?, run)
    }
    fn session_snapshot(&self, key: &SessionKey) -> Result<SessionSnapshot, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        let snapshot = snapshot_in(&tx, key)?;
        super::views::commit(tx)?;
        Ok(snapshot)
    }
}
pub(crate) fn snapshot_budget_in(
    db: &rusqlite::Connection,
    key: &SessionKey,
    lifetime: &SessionLifetimeId,
) -> Result<(), RuntimeError> {
    let (rows, bytes, messages): (u64, u64, u64) = db.query_row(
        "SELECT count(*),COALESCE(sum(length(CAST(messages_json AS BLOB))),0),COALESCE(sum(end_seq-start_seq),0) FROM (SELECT messages_json,end_seq,start_seq FROM transcript_batches WHERE lifetime=?1 LIMIT 32769)",
        [&lifetime.0],
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)),
    )?;
    let projection:u64=db.query_row("SELECT COALESCE((SELECT length(CAST(messages_json AS BLOB)) FROM context_heads WHERE lifetime=?1),0)",[&lifetime.0],|r|r.get(0))?;
    // 保持完整 canonical 前缀，超出预算明确拒绝，不能截断或变为空历史。
    lifetime_owner_in(db, key, lifetime)?;
    if rows > 32768 || messages > 32768 || bytes > 16 * 1024 * 1024 || projection > 4 * 1024 * 1024
    {
        return Err(CoordinationError::InvalidRequest.into());
    }
    Ok(())
}

pub(crate) fn snapshot_in(
    db: &Connection,
    key: &SessionKey,
) -> Result<SessionSnapshot, RuntimeError> {
    let meta = metadata_in(db, key)?
        .filter(|m| !m.deleted)
        .ok_or_else(|| RuntimeError::Protocol("session 已删除或不存在".into()))?;
    snapshot_budget_in(db, key, &meta.lifetime)?;
    let generation = db.query_row(
        "SELECT projection_generation FROM session_heads WHERE session_id=?1",
        params![key.0],
        |r| r.get(0),
    )?;
    let mut q=db.prepare("SELECT b.messages_json,b.digest,b.start_seq,b.end_seq FROM transcript_batches b JOIN session_lifetime_owners o ON o.lifetime=b.lifetime WHERE b.lifetime=?1 AND o.session_id=?2 ORDER BY b.start_seq,b.rowid")?;
    let payloads = q
        .query_map(params![meta.lifetime.0, key.0], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, u64>(2)?,
                r.get::<_, u64>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut messages = Vec::new();
    let mut hasher = Sha256::new();
    let mut batch_ranges = Vec::new();
    for (payload, digest, start, end) in payloads {
        if start != messages.len() as u64 || end < start {
            return Err(RuntimeError::Protocol("transcript batch seq 不连续".into()));
        }
        batch_ranges.push((start, end));
        let batch = decode_transcript_batch(&payload, &digest, start, end)?;
        hasher.update(payload.as_bytes());
        messages.extend(batch);
    }
    if messages.len() as u64 != meta.revision.0 {
        return Err(RuntimeError::Protocol(
            "transcript revision 与内容不一致".into(),
        ));
    }
    Ok(SessionSnapshot {
        session_key: key.clone(),
        lifetime: meta.lifetime,
        revision: meta.revision,
        projection_generation: ProjectionGeneration(generation),
        messages,
        batch_ranges,
        prefix_digest: format!("{:x}", hasher.finalize()),
    })
}

impl SessionLifecycle for RunStore {
    fn close_receipt(
        &self,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        operation: &str,
    ) -> Result<Option<SessionMetadata>, RuntimeError> {
        self.close_receipt_scoped(&TenantContext::local(), key, lifetime, operation)
    }
    fn set_preferred_session(&self, key: &SessionKey) -> Result<(), RuntimeError> {
        self.select_session_scoped(&TenantContext::local(), key, None)
    }
    fn execute_lifecycle(
        &self,
        command: &SessionCommand,
        expected: Option<&SessionLifetimeId>,
        operation: &str,
    ) -> Result<SessionMetadata, RuntimeError> {
        self.lifecycle_scoped(&TenantContext::local(), command, expected, operation)
    }
    fn create_session(&self, key: &SessionKey) -> Result<SessionMetadata, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        let meta = create_in(&tx, key)?;
        super::views::commit(tx)?;
        Ok(meta)
    }
    fn end_session(
        &self,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        delete: bool,
    ) -> Result<(), RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        end_in(&tx, key, lifetime, delete)?;
        super::views::commit(tx)?;
        Ok(())
    }
    fn fork_session(
        &self,
        source: &SessionKey,
        target: &SessionKey,
        revision: TranscriptSeq,
    ) -> Result<SessionMetadata, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        let child = fork_in(&tx, source, target, revision)?;
        super::views::commit(tx)?;
        Ok(child)
    }
}
impl TranscriptStore for RunStore {
    fn import_legacy(&self, key: &SessionKey, messages: &[Message]) -> Result<(), RuntimeError> {
        self.import_legacy_scoped(&TenantContext::local(), key, None, messages, false)
    }
    fn append_transcript(
        &self,
        owner: &ExactOwner,
        operation: &str,
        messages: &[Message],
    ) -> Result<TranscriptSeq, RuntimeError> {
        self.append_transcript_scoped(owner, operation, messages, None)
    }
}

fn end_in(
    db: &Connection,
    key: &SessionKey,
    lifetime: &SessionLifetimeId,
    delete: bool,
) -> Result<(), RuntimeError> {
    let meta =
        metadata_in(db, key)?.ok_or_else(|| RuntimeError::Protocol("session 不存在".into()))?;
    if meta.deleted || meta.lifetime != *lifetime {
        return Err(RuntimeError::Protocol("stale lifecycle owner".into()));
    }
    super::writer::acquire_in(db, key, lifetime)?;
    ensure_idle_in(db, key, lifetime)?;
    db.execute("DELETE FROM memories WHERE lifetime=?1 AND json_extract(data_json,'$.kind')='turn_summary'",params![lifetime.0])?;
    for table in [
        "memory_exposures",
        "memory_assessments",
        "memory_feedback_receipts",
    ] {
        db.execute(
            &format!("DELETE FROM {table} WHERE lifetime=?1"),
            params![lifetime.0],
        )?;
    }
    db.execute(
        "INSERT INTO session_tombstones(lifetime,session_id,deleted_at_ms) VALUES (?1,?2,?3)",
        params![lifetime.0, key.0, now_ms()],
    )?;
    let generation: i64 = db.query_row(
        "SELECT projection_generation FROM session_heads WHERE session_id=?1",
        params![key.0],
        |r| r.get(0),
    )?;
    let next = generation
        .checked_add(1)
        .ok_or_else(|| RuntimeError::Protocol("projection generation 耗尽".into()))?;
    db.execute("UPDATE session_heads SET deleted=?2, lifetime=CASE WHEN ?2 THEN lifetime ELSE lower(hex(randomblob(16))) END,revision=0,projection_generation=?4,legacy_imported=1,updated_at_ms=?3 WHERE session_id=?1",params![key.0,delete,now_ms(),next])?;
    super::hooks::publish_in(db, &meta, HookEvent::SessionEnd)?;
    let current = metadata_in(db, key)?
        .ok_or_else(|| RuntimeError::Internal("缺少 lifecycle head".into()))?;
    super::writer::acquire_in(db, key, &current.lifetime)?;
    Ok(())
}

fn fork_in(
    db: &Connection,
    source: &SessionKey,
    target: &SessionKey,
    revision: TranscriptSeq,
) -> Result<SessionMetadata, RuntimeError> {
    let meta = metadata_in(db, source)?
        .filter(|m| !m.deleted && m.revision == revision)
        .ok_or_else(|| RuntimeError::Protocol("fork source 已变化".into()))?;
    super::writer::acquire_in(db, source, &meta.lifetime)?;
    let snapshot = snapshot_in(db, source)?;
    if snapshot.lifetime != meta.lifetime || snapshot.revision != revision {
        return Err(RuntimeError::Protocol("fork source stale".into()));
    }
    let messages = snapshot.messages;
    let child = create_in(db, target)?;
    insert_batch(db, target, &child.lifetime, "fork", None, &messages)?;
    Ok(child)
}

pub(crate) fn ensure_idle_in(
    db: &Connection,
    key: &SessionKey,
    lifetime: &SessionLifetimeId,
) -> Result<(), RuntimeError> {
    let busy:i64=db.query_row("SELECT count(*) FROM runs WHERE session_id=?1 AND lifetime=?2 AND status IN ('queued','running','waiting_interaction')",params![key.0,lifetime.0],|r|r.get(0))?;
    let resources:i64=db.query_row("SELECT count(*) FROM resources WHERE lifetime=?1 AND state IN ('starting','running','stopping')",params![lifetime.0],|r|r.get(0))?;
    if busy != 0 || resources != 0 {
        return Err(RuntimeError::Protocol(
            "session 仍有受管 run，必须先 cancel/join".into(),
        ));
    }
    Ok(())
}

impl RunStore {
    fn lifecycle_scoped(
        &self,
        scope: &TenantContext,
        command: &SessionCommand,
        expected: Option<&SessionLifetimeId>,
        operation: &str,
    ) -> Result<SessionMetadata, RuntimeError> {
        self.lifecycle_with_authorization(scope, command, expected, operation, None)
    }
    fn lifecycle_with_authorization(
        &self,
        scope: &TenantContext,
        command: &SessionCommand,
        expected: Option<&SessionLifetimeId>,
        operation: &str,
        authorization: Option<(&AuthorizationRequest, &dyn AuthorizationVerifier)>,
    ) -> Result<SessionMetadata, RuntimeError> {
        if operation.is_empty() || operation.len() > 256 {
            return Err(RuntimeError::Protocol("operation id 无效".into()));
        }
        let payload = serde_json::to_string(&(command, expected))
            .map_err(|_| RuntimeError::Coordination(CoordinationError::Corrupt))?;
        let mut db = self.lock_connection()?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        if let Some((request, verifier)) = authorization {
            if request.context.scope != *scope || request.action != session_command_action(command)
            {
                return Err(AuthorizationError::Forbidden.into());
            }
            let identity =
                crate::enterprise::identity_in(&tx, scope, &request.context.principal.subject_id)?;
            let mut current = request.clone();
            current.attributes.timestamp_ms = tx.query_row(
                "SELECT CAST((julianday('now')-2440587.5)*86400000 AS INTEGER)",
                [],
                |r| r.get(0),
            )?;
            verifier.verify(&current, &identity)?;
        }
        lifecycle_scope_in(&tx, scope, command)?;
        if let Some((old, result)) = tx
            .query_row(
                "SELECT command_json,result_json FROM lifecycle_receipts WHERE operation_id=?1",
                params![operation],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
            )
            .optional()?
        {
            // v6-v13 receipt 使用单 command；只允许原身份的无写入兼容读回。
            if old != payload {
                let legacy = serde_json::to_string(command)
                    .map_err(|_| RuntimeError::Coordination(CoordinationError::Corrupt))?;
                let prior = lifecycle_result_in(&tx, lifecycle_result_key(command), &result)?;
                if old != legacy || expected.is_some_and(|e| e != &prior.lifetime) {
                    return Err(RuntimeError::Protocol(
                        "lifecycle operation id / lifetime 冲突".into(),
                    ));
                }
            }
            return lifecycle_result_in(&tx, lifecycle_result_key(command), &result);
        }
        let source = match command {
            SessionCommand::Create { key }
            | SessionCommand::Close { key }
            | SessionCommand::End { key, .. } => key,
            SessionCommand::Fork { source, .. } => source,
        };
        if let Some(meta) = metadata_in(&tx, source)?
            && !meta.deleted
        {
            super::writer::acquire_in(&tx, source, &meta.lifetime)?;
        }
        let meta = match command {
            SessionCommand::Create { key } => create_in(&tx, key)?,
            SessionCommand::Close { key } => {
                let lifetime = expected
                    .ok_or_else(|| RuntimeError::Protocol("close 缺少 expected lifetime".into()))?;
                let meta = metadata_in(&tx, key)?
                    .filter(|meta| !meta.deleted && &meta.lifetime == lifetime)
                    .ok_or_else(|| RuntimeError::Protocol("close stale lifetime".into()))?;
                ensure_idle_in(&tx, key, lifetime)?;
                super::hooks::publish_in(&tx, &meta, HookEvent::SessionEnd)?;
                meta
            }
            SessionCommand::End { key, delete } => {
                let lifetime = expected.ok_or_else(|| {
                    RuntimeError::Protocol("缺少 lifecycle expected lifetime".into())
                })?;
                end_in(&tx, key, lifetime, *delete)?;
                metadata_in(&tx, key)?
                    .ok_or_else(|| RuntimeError::Internal("缺少 lifecycle head".into()))?
            }
            SessionCommand::Fork {
                source,
                target,
                revision,
            } => {
                if let Some(expected) = expected {
                    if metadata_in(&tx, source)?
                        .is_none_or(|m| m.deleted || &m.lifetime != expected)
                    {
                        return Err(RuntimeError::Protocol("fork stale lifetime".into()));
                    }
                }
                fork_in(&tx, source, target, *revision)?
            }
        };
        let result = serde_json::to_string(&meta)
            .map_err(|_| RuntimeError::Coordination(CoordinationError::Corrupt))?;
        tx.execute(
            "INSERT INTO lifecycle_receipts VALUES (?1,?2,?3)",
            params![operation, payload, result],
        )?;
        if let Some((request, _)) = authorization {
            crate::ledgers::append_audit_in(
                &tx,
                &AuditCommand {
                    scope: scope.clone(),
                    operation_id: format!("lifecycle:{:x}", Sha256::digest(operation.as_bytes())),
                    subject_id: request.context.principal.subject_id.clone(),
                    action: "session.lifecycle".into(),
                    resource: serde_json::json!({"key":meta.key,"lifetime":meta.lifetime,"command":command}),
                    outcome: "committed".into(),
                },
                tx.query_row(
                    "SELECT CAST((julianday('now')-2440587.5)*86400000 AS INTEGER)",
                    [],
                    |r| r.get(0),
                )?,
            )?;
        }
        super::views::commit(tx)?;
        Ok(meta)
    }
    fn close_receipt_scoped(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        operation: &str,
    ) -> Result<Option<SessionMetadata>, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        crate::enterprise::check_session_scope_in(&tx, scope, key)?;
        let result = close_receipt_in(&tx, key, lifetime, operation)?;
        tx.commit()?;
        Ok(result)
    }
}
pub(crate) fn lifecycle_scope_in(
    db: &Connection,
    scope: &TenantContext,
    command: &SessionCommand,
) -> Result<(), RuntimeError> {
    if scope != &TenantContext::local() {
        return Err(AuthorizationError::Forbidden.into());
    }
    let source = match command {
        SessionCommand::Create { key }
        | SessionCommand::Close { key }
        | SessionCommand::End { key, .. } => key,
        SessionCommand::Fork { source, .. } => source,
    };
    let exists: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM sessions WHERE id=?1)",
        [&source.0],
        |r| r.get(0),
    )?;
    if exists || !matches!(command, SessionCommand::Create { .. }) {
        crate::enterprise::check_session_scope_in(db, scope, source)?;
    }
    if let SessionCommand::Fork { target, .. } = command {
        let target_exists: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sessions WHERE id=?1)",
            [&target.0],
            |r| r.get(0),
        )?;
        if target_exists {
            crate::enterprise::check_session_scope_in(db, scope, target)?;
        }
    }
    Ok(())
}

fn close_receipt_in(
    db: &Connection,
    key: &SessionKey,
    lifetime: &SessionLifetimeId,
    operation: &str,
) -> Result<Option<SessionMetadata>, RuntimeError> {
    let payload =
        serde_json::to_string(&(SessionCommand::Close { key: key.clone() }, Some(lifetime)))
            .map_err(|_| RuntimeError::Coordination(CoordinationError::Corrupt))?;
    let prior = db
        .query_row(
            "SELECT command_json,result_json FROM lifecycle_receipts WHERE operation_id=?1",
            params![operation],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    prior
        .map(|(command, result)| {
            if command != payload {
                return Err(RuntimeError::Protocol("close operation 身份冲突".into()));
            }
            lifecycle_result_in(db, key, &result)
        })
        .transpose()
}

fn lifecycle_result_key(command: &SessionCommand) -> &SessionKey {
    match command {
        SessionCommand::Create { key }
        | SessionCommand::Close { key }
        | SessionCommand::End { key, .. } => key,
        SessionCommand::Fork { target, .. } => target,
    }
}

fn lifecycle_result_in(
    db: &Connection,
    key: &SessionKey,
    result: &str,
) -> Result<SessionMetadata, RuntimeError> {
    let meta: SessionMetadata = serde_json::from_str(result)
        .map_err(|_| RuntimeError::Coordination(CoordinationError::Corrupt))?;
    if &meta.key != key {
        return Err(CoordinationError::Corrupt.into());
    }
    lifetime_owner_in(db, key, &meta.lifetime)?;
    Ok(meta)
}

fn lifecycle_error(error: RuntimeError) -> CoordinationError {
    match error {
        RuntimeError::Protocol(_) => CoordinationError::Conflict,
        error => crate::readback::read_error(error),
    }
}
fn lifecycle_key_valid(key: &SessionKey) -> bool {
    !key.0.is_empty() && key.0.len() <= 1024
}
fn lifecycle_command_valid(command: &SessionCommand) -> bool {
    match command {
        SessionCommand::Create { key }
        | SessionCommand::Close { key }
        | SessionCommand::End { key, .. } => lifecycle_key_valid(key),
        SessionCommand::Fork { source, target, .. } => {
            lifecycle_key_valid(source) && lifecycle_key_valid(target)
        }
    }
}
#[async_trait::async_trait]
impl agent_storage_ports::SessionLifecycleRepository for RunStore {
    async fn commit_session_authorized(
        &self,
        request: &AuthorizationRequest,
        command: &SessionCommand,
        expected: Option<&SessionLifetimeId>,
        operation: &str,
        verifier: std::sync::Arc<dyn AuthorizationVerifier>,
    ) -> Result<SessionMetadata, RepositoryError> {
        if !lifecycle_command_valid(command) || operation.is_empty() || operation.len() > 256 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (request, command, expected, operation) = (
            request.clone(),
            command.clone(),
            expected.cloned(),
            operation.to_owned(),
        );
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .lifecycle_with_authorization(
                    &request.context.scope,
                    &command,
                    expected.as_ref(),
                    &operation,
                    Some((&request, verifier.as_ref())),
                )
                .map_err(RuntimeError::repository_error)
        })
        .await
    }

    async fn commit_session_command(
        &self,
        scope: &TenantContext,
        command: &SessionCommand,
        expected: Option<&SessionLifetimeId>,
        operation: &str,
    ) -> Result<SessionMetadata, CoordinationError> {
        if !lifecycle_command_valid(command)
            || operation.is_empty()
            || operation.len() > 256
            || expected.is_some_and(|life| life.0.is_empty() || life.0.len() > 128)
        {
            return Err(CoordinationError::InvalidRequest);
        }
        let (scope, command, expected, operation) = (
            scope.clone(),
            command.clone(),
            expected.cloned(),
            operation.to_owned(),
        );
        self.sqlite_blocking(CoordinationError::StorageUnavailable, move |store| {
            store
                .lifecycle_scoped(&scope, &command, expected.as_ref(), &operation)
                .map_err(lifecycle_error)
        })
        .await
    }
    async fn read_close_receipt(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        operation: &str,
    ) -> Result<Option<SessionMetadata>, CoordinationError> {
        if !lifecycle_key_valid(key)
            || lifetime.0.is_empty()
            || lifetime.0.len() > 128
            || operation.is_empty()
            || operation.len() > 256
        {
            return Err(CoordinationError::InvalidRequest);
        }
        let (scope, key, lifetime, operation) = (
            scope.clone(),
            key.clone(),
            lifetime.clone(),
            operation.to_owned(),
        );
        self.sqlite_blocking(CoordinationError::StorageUnavailable, move |store| {
            store
                .close_receipt_scoped(&scope, &key, &lifetime, &operation)
                .map_err(lifecycle_error)
        })
        .await
    }
}

impl RunStore {
    pub(crate) fn append_transcript_scoped(
        &self,
        owner: &ExactOwner,
        operation: &str,
        messages: &[Message],
        envelope: Option<&EnterpriseOwnerEnvelope>,
    ) -> Result<TranscriptSeq, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction_with_behavior(if envelope.is_some() {
            rusqlite::TransactionBehavior::Immediate
        } else {
            rusqlite::TransactionBehavior::Deferred
        })?;
        if let Some(envelope) = envelope {
            crate::execution::check_execution_in(&tx, envelope)?;
        }
        fence_in(&tx, owner)?;
        let status: String = tx.query_row(
            "SELECT status FROM runs WHERE id=?1",
            params![owner.run_id.0],
            |r| r.get(0),
        )?;
        if !matches!(status.as_str(), "running" | "waiting_interaction") {
            return Err(RuntimeError::Protocol(
                "run 不持有 session writer permit".into(),
            ));
        }
        let revision = insert_batch(
            &tx,
            &owner.session_key,
            &owner.session_lifetime_id,
            operation,
            Some(&owner.run_id),
            messages,
        )?;
        super::views::commit(tx)?;
        Ok(revision)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    fn path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "session-repo-{}-{}.sqlite3",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }
    #[test]
    fn close_receipts_preserve_lifetime_history_and_are_idempotent_on_both_backends() {
        fn close_contract(store: &RunStore) {
            let key = SessionKey("session-close.jsonl".into());
            let meta = store.create_session(&key).unwrap();
            let Admission::New(run) = store
                .admit_with_lifetime(
                    key.clone(),
                    Some(&meta.lifetime),
                    RequestId::Number(99),
                    "输入",
                    AdmissionMode::Queue,
                    None,
                )
                .unwrap()
            else {
                panic!("new run")
            };
            assert!(store.try_start_queued(&run.run_id).unwrap());
            let owner = store.run_owner(&run.run_id).unwrap();
            store
                .append_transcript(
                    &owner,
                    "canonical",
                    &[Message::text(Role::User, "保留原文")],
                )
                .unwrap();
            let command = SessionCommand::Close { key: key.clone() };
            assert!(
                store
                    .execute_lifecycle(&command, Some(&meta.lifetime), "close-op")
                    .is_err()
            );
            assert!(
                store
                    .close_receipt(&key, &meta.lifetime, "close-op")
                    .unwrap()
                    .is_none()
            );
            store
                .finish(&run.run_id, RunStatus::Completed, Some("完成"), None)
                .unwrap();
            let before = store.session_snapshot(&key).unwrap();
            let closed = store
                .execute_lifecycle(&command, Some(&meta.lifetime), "close-op")
                .unwrap();
            assert_eq!(closed.lifetime, meta.lifetime);
            assert_eq!(closed.revision, before.revision);
            assert_eq!(
                store.session_snapshot(&key).unwrap().messages,
                before.messages
            );
            assert_eq!(
                store
                    .close_receipt(&key, &meta.lifetime, "close-op")
                    .unwrap()
                    .unwrap()
                    .lifetime,
                meta.lifetime
            );
            assert_eq!(
                store
                    .execute_lifecycle(&command, Some(&meta.lifetime), "close-op")
                    .unwrap()
                    .revision,
                before.revision
            );
            assert!(
                store
                    .execute_lifecycle(
                        &command,
                        Some(&SessionLifetimeId("foreign".into())),
                        "close-op"
                    )
                    .is_err()
            );
            assert!(
                store
                    .execute_lifecycle(
                        &SessionCommand::End { key, delete: true },
                        Some(&meta.lifetime),
                        "close-op"
                    )
                    .is_err()
            );
            let db = store.lock_connection().unwrap();
            assert_eq!(
                db.query_row::<u64, _, _>(
                    "SELECT count(*) FROM hook_publications WHERE json_extract(payload_json,'$.event')='session_end'",
                    [],
                    |row| row.get(0)
                )
                .unwrap(),
                1
            );
        }
        close_contract(&RunStore::open(std::path::Path::new(":memory:")).unwrap());
        let path = path();
        {
            close_contract(&RunStore::open(&path).unwrap());
        }
        let reopened = RunStore::open(&path).unwrap();
        let meta = reopened
            .session_metadata(&SessionKey("session-close.jsonl".into()))
            .unwrap()
            .unwrap();
        assert!(
            reopened
                .close_receipt(&meta.key, &meta.lifetime, "close-op")
                .unwrap()
                .is_some()
        );
        drop(reopened);
        std::fs::remove_file(path).unwrap();
    }

    fn contract(store: &RunStore) {
        let key = SessionKey("session-contract.jsonl".into());
        let meta = store.create_session(&key).unwrap();
        let Admission::New(run) = store
            .admit_with_lifetime(
                key.clone(),
                Some(&meta.lifetime),
                RequestId::Number(1),
                "输入",
                AdmissionMode::Queue,
                None,
            )
            .unwrap()
        else {
            panic!("new run")
        };
        assert!(store.try_start_queued(&run.run_id).unwrap());
        let owner = store.run_owner(&run.run_id).unwrap();
        let messages = vec![Message::text(Role::Assistant, "回答")];
        assert_eq!(
            store
                .append_transcript(&owner, "batch-1", &messages)
                .unwrap(),
            TranscriptSeq(2)
        );
        assert_eq!(
            store
                .append_transcript(&owner, "batch-1", &messages)
                .unwrap(),
            TranscriptSeq(2)
        );
        assert!(store.append_transcript(&owner, "batch-1", &[]).is_err());
        assert!(store.end_session(&key, &meta.lifetime, true).is_err());
        store
            .finish(&run.run_id, RunStatus::Completed, Some("回答"), None)
            .unwrap();
        let snapshot = store.session_snapshot(&key).unwrap();
        let mut expected = vec![Message::text(Role::User, "输入")];
        expected.extend(messages.clone());
        assert_eq!(snapshot.messages, expected);
        let fork = SessionKey("session-fork.jsonl".into());
        assert!(store.fork_session(&key, &fork, TranscriptSeq(1)).is_err());
        store.fork_session(&key, &fork, TranscriptSeq(2)).unwrap();
        assert_eq!(store.session_snapshot(&fork).unwrap().messages, expected);
        store
            .execute_lifecycle(
                &SessionCommand::End {
                    key: key.clone(),
                    delete: true,
                },
                Some(&meta.lifetime),
                "delete-1",
            )
            .unwrap();
        store
            .execute_lifecycle(
                &SessionCommand::End {
                    key: key.clone(),
                    delete: true,
                },
                Some(&meta.lifetime),
                "delete-1",
            )
            .unwrap();
        assert!(store.session_snapshot(&key).is_err());
        let replacement = store.create_session(&key).unwrap();
        assert_ne!(replacement.lifetime, meta.lifetime);
        assert!(store.append_transcript(&owner, "late", &messages).is_err());
        assert!(
            store
                .append_event(&run.run_id, "late", &serde_json::json!({}))
                .is_err()
        );
        assert!(store.start_provider_attempt(&attempt(&run.run_id)).is_err());
        assert!(
            store
                .admit_with_lifetime(
                    key.clone(),
                    Some(&meta.lifetime),
                    RequestId::Number(2),
                    "迟到",
                    AdmissionMode::Queue,
                    None
                )
                .is_err()
        );
        assert!(store.session_snapshot(&key).unwrap().messages.is_empty());
        let Admission::New(new) = store
            .admit_with_lifetime(
                key.clone(),
                Some(&replacement.lifetime),
                RequestId::Number(1),
                "新输入",
                AdmissionMode::Queue,
                None,
            )
            .unwrap()
        else {
            panic!("new incarnation run")
        };
        assert_ne!(new.run_id, run.run_id);
        let current = store.run_owner(&new.run_id).unwrap();
        assert!(current.run_generation > owner.run_generation);
        store.remove_queued(&new.run_id).unwrap();
        store
            .end_session(&key, &replacement.lifetime, false)
            .unwrap();
        assert!(
            store
                .append_transcript(&current, "after-clear", &messages)
                .is_err()
        );
    }
    fn attempt(run: &RunId) -> ProviderAttempt {
        ProviderAttempt {
            attempt_id: "late-attempt".into(),
            run_id: run.clone(),
            round: 1,
            candidate_index: 0,
            provider_profile_id: "p".into(),
            api_type: ApiType::OpenaiChat,
            model: "m".into(),
            status: AttemptStatus::Started,
            error_kind: None,
            diagnostic: None,
            retry_after_ms: None,
            stream_committed: false,
            started_at_ms: 0,
            first_event_at_ms: None,
            finished_at_ms: None,
            usage: None,
        }
    }
    #[test]
    fn lifetime_transcript_contract_is_shared_by_file_and_memory_sqlite() {
        let file = path();
        contract(&RunStore::open(&file).unwrap());
        contract(&RunStore::open(std::path::Path::new(":memory:")).unwrap());
    }
    #[test]
    fn snapshot_and_lifetime_survive_restart_and_corruption_fails_closed() {
        let file = path();
        let key = SessionKey("restart".into());
        let store = RunStore::open(&file).unwrap();
        store
            .import_legacy(&key, &[Message::text(Role::User, "旧数据")])
            .unwrap();
        let before = store.session_snapshot(&key).unwrap();
        drop(store);
        let store = RunStore::open(&file).unwrap();
        let after = store.session_snapshot(&key).unwrap();
        assert_eq!(after.lifetime, before.lifetime);
        assert_eq!(after.messages, before.messages);
        store
            .lock_connection()
            .unwrap()
            .execute("UPDATE transcript_batches SET messages_json='[]'", [])
            .unwrap();
        assert!(store.session_snapshot(&key).is_err());
    }
    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_lifecycle_caller_keeps_committed_operation_and_budget_until_real_work_finishes()
     {
        let root = std::env::temp_dir().join(format!("ma-lifecycle-cancel-{}", std::process::id()));
        let store = RunStore::open(&root.join("runtime.sqlite3")).unwrap();
        let worker = store.clone();
        let gate = std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        struct Release(std::sync::Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);
        impl Drop for Release {
            fn drop(&mut self) {
                let (lock, signal) = &*self.0;
                *lock.lock().unwrap_or_else(|e| e.into_inner()) = true;
                signal.notify_all();
            }
        }
        let _release = Release(gate.clone());
        let (committed_tx, committed_rx) = tokio::sync::oneshot::channel();
        let waiting = gate.clone();
        let caller = tokio::spawn(async move {
            worker
                .sqlite_blocking(CoordinationError::StorageUnavailable, move |db| {
                    let result = db
                        .lifecycle_scoped(
                            &TenantContext::local(),
                            &SessionCommand::Create {
                                key: SessionKey("committed".into()),
                            },
                            None,
                            "cancel-after-commit",
                        )
                        .map_err(lifecycle_error)?;
                    committed_tx.send(()).unwrap();
                    let (lock, signal) = &*waiting;
                    let mut released = lock.lock().unwrap();
                    while !*released {
                        released = signal.wait(released).unwrap();
                    }
                    Ok(result)
                })
                .await
        });
        committed_rx.await.unwrap();
        assert_eq!(store.blocking_budget.available_permits(), 3);
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert_eq!(store.blocking_budget.available_permits(), 3);
        let db = Connection::open(root.join("runtime.sqlite3")).unwrap();
        assert_eq!(
            db.query_row::<u64, _, _>(
                "SELECT count(*) FROM lifecycle_receipts WHERE operation_id='cancel-after-commit'",
                [],
                |r| r.get(0)
            )
            .unwrap(),
            1
        );
        let clock: u64 = db
            .query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
        use agent_storage_ports::SessionLifecycleRepository;
        let result = store
            .commit_session_command(
                &TenantContext::local(),
                &SessionCommand::Create {
                    key: SessionKey("committed".into()),
                },
                None,
                "cancel-after-commit",
            )
            .await
            .unwrap();
        assert_eq!(result.key.0, "committed");
        assert_eq!(
            db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
                .unwrap(),
            clock
        );
        assert_eq!(db.query_row::<u64,_,_>("SELECT count(*) FROM hook_publications WHERE json_extract(payload_json,'$.session_key')='committed'",[],|r|r.get(0)).unwrap(),1);
        {
            let (lock, signal) = &*gate;
            *lock.lock().unwrap() = true;
            signal.notify_all();
        }
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while store.blocking_budget.available_permits() != 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(db);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
}

impl RunStore {
    pub(crate) fn select_session_scoped(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        expected: Option<&SessionLifetimeId>,
    ) -> Result<(), RuntimeError> {
        if scope != &TenantContext::local() {
            return Err(AuthorizationError::Forbidden.into());
        }
        let mut db = self.lock_connection()?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        super::enterprise::check_session_scope_in(&tx, scope, key)?;
        let meta = metadata_in(&tx, key)?
            .filter(|meta| !meta.deleted)
            .ok_or_else(|| RuntimeError::Protocol("preferred session 无效".into()))?;
        if expected.is_some_and(|expected| expected != &meta.lifetime) {
            return Err(AuthorizationError::StaleOwner.into());
        }
        lifetime_owner_in(&tx, key, &meta.lifetime)?;
        super::writer::acquire_in(&tx, key, &meta.lifetime)?;
        tx.execute("INSERT INTO runtime_settings(name,value) VALUES('active_session',?1) ON CONFLICT(name) DO UPDATE SET value=excluded.value",params![key.0])?;
        super::writer::before_commit(&tx)?;
        tx.commit()?;
        Ok(())
    }
    pub(crate) fn import_legacy_scoped(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        expected: Option<&SessionLifetimeId>,
        messages: &[Message],
        strict: bool,
    ) -> Result<(), RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        if scope != &TenantContext::local() {
            return Err(AuthorizationError::Forbidden.into());
        }
        let previous = metadata_in(&tx, key)?;
        if previous.is_some() {
            super::enterprise::check_session_scope_in(&tx, scope, key)?;
        }
        if strict {
            match (&previous, expected) {
                (Some(meta), Some(life)) if !meta.deleted && &meta.lifetime == life => {}
                (None, None) => {}
                (Some(meta), None) if !meta.deleted && meta.legacy_imported => {}
                _ => return Err(AuthorizationError::StaleOwner.into()),
            }
        }
        tx.execute(
            "INSERT OR IGNORE INTO sessions(id,created_at_ms) VALUES (?1,?2)",
            params![key.0, now_ms()],
        )?;
        let meta = metadata_in(&tx, key)?
            .ok_or_else(|| RuntimeError::Internal("缺少 import head".into()))?;
        if !meta.deleted {
            super::writer::acquire_in(&tx, key, &meta.lifetime)?;
        }
        let imported: bool = tx.query_row(
            "SELECT legacy_imported FROM session_heads WHERE session_id=?1",
            params![key.0],
            |r| r.get(0),
        )?;
        if !imported && !meta.deleted {
            insert_batch(&tx, key, &meta.lifetime, "legacy-import", None, messages)?;
            tx.execute(
                "UPDATE session_heads SET legacy_imported=1 WHERE session_id=?1",
                params![key.0],
            )?;
        }
        super::views::commit(tx)?;
        Ok(())
    }
}

/// canonical snapshot与run诊断共用唯一批次decoder。
pub(crate) fn decode_transcript_batch(
    payload: &str,
    digest: &str,
    start: u64,
    end: u64,
) -> Result<Vec<Message>, RuntimeError> {
    agent_storage_ports::decode_transcript_batch(payload, digest, start, end).map_err(Into::into)
}
