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
        .map_err(|e| RuntimeError::Protocol(e.to_string()))
}
fn create_in(db: &Connection, key: &SessionKey) -> Result<SessionMetadata, RuntimeError> {
    if let Some(meta) = metadata_in(db, key)? {
        if !meta.deleted {
            return Err(RuntimeError::Protocol("session 已存在".into()));
        }
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
    metadata_in(db, key)?.ok_or_else(|| RuntimeError::Internal("缺少创建后的 session".into()))
}
pub(crate) fn insert_batch(
    db: &Connection,
    key: &SessionKey,
    lifetime: &SessionLifetimeId,
    operation: &str,
    run: Option<&RunId>,
    messages: &[Message],
) -> Result<TranscriptSeq, RuntimeError> {
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
        if metadata_in(&db, key)?.is_none_or(|m| m.deleted || &m.lifetime != lifetime) {
            return Err(RuntimeError::Protocol(
                "session list projection stale".into(),
            ));
        }
        Ok(db.query_row("SELECT substr(json_extract(m.value,'$.content'),1,160) FROM transcript_batches b,json_each(b.messages_json) m WHERE b.lifetime=?1 AND json_extract(m.value,'$.role')='user' AND json_extract(m.value,'$.content') IS NOT NULL ORDER BY b.start_seq,m.key LIMIT 1",params![lifetime.0],|r|r.get(0)).optional()?)
    }
    fn preferred_session(&self) -> Result<Option<SessionKey>, RuntimeError> {
        Ok(self
            .lock_connection()?
            .query_row(
                "SELECT value FROM runtime_settings WHERE name='active_session'",
                [],
                |r| r.get::<_, String>(0),
            )
            .optional()?
            .map(SessionKey))
    }
    fn run_messages(&self, run: &RunId) -> Result<Vec<Message>, RuntimeError> {
        let db = self.lock_connection()?;
        let mut q = db.prepare(
            "SELECT messages_json FROM transcript_batches WHERE run_id=?1 ORDER BY start_seq",
        )?;
        let payloads = q
            .query_map(params![run.0], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        let mut messages = Vec::new();
        for payload in payloads {
            messages.extend(
                serde_json::from_str::<Vec<Message>>(&payload)
                    .map_err(|e| RuntimeError::Protocol(e.to_string()))?,
            );
        }
        Ok(messages)
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
        let mut q=db.prepare("SELECT session_id FROM session_heads WHERE deleted=0 AND session_id>?1 ORDER BY session_id LIMIT ?2")?;
        let keys = q
            .query_map(
                params![after.unwrap_or(""), limit.clamp(1, 1001) as i64],
                |r| r.get::<_, String>(0),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        keys.into_iter()
            .map(|key| {
                metadata_in(&db, &SessionKey(key))?
                    .ok_or_else(|| RuntimeError::Internal("缺少 session head".into()))
            })
            .collect()
    }
    fn session_metadata_list(&self) -> Result<Vec<SessionMetadata>, RuntimeError> {
        let db = self.lock_connection()?;
        let mut q=db.prepare("SELECT session_id FROM session_heads WHERE deleted=0 ORDER BY updated_at_ms DESC,session_id LIMIT 1000")?;
        let keys = q
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        keys.into_iter()
            .map(|key| {
                metadata_in(&db, &SessionKey(key))?
                    .ok_or_else(|| RuntimeError::Internal("缺少 session head".into()))
            })
            .collect()
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
        tx.commit()?;
        Ok(snapshot)
    }
}
pub(crate) fn snapshot_in(
    db: &Connection,
    key: &SessionKey,
) -> Result<SessionSnapshot, RuntimeError> {
    let meta = metadata_in(db, key)?
        .filter(|m| !m.deleted)
        .ok_or_else(|| RuntimeError::Protocol("session 已删除或不存在".into()))?;
    let generation = db.query_row(
        "SELECT projection_generation FROM session_heads WHERE session_id=?1",
        params![key.0],
        |r| r.get(0),
    )?;
    let mut q=db.prepare("SELECT messages_json,digest,start_seq,end_seq FROM transcript_batches WHERE lifetime=?1 ORDER BY start_seq,rowid")?;
    let payloads = q
        .query_map(params![meta.lifetime.0], |r| {
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
        if format!("{:x}", Sha256::digest(payload.as_bytes())) != digest {
            return Err(RuntimeError::Protocol("transcript digest 损坏".into()));
        }
        hasher.update(payload.as_bytes());
        let batch: Vec<Message> =
            serde_json::from_str(&payload).map_err(|e| RuntimeError::Protocol(e.to_string()))?;
        if end - start != batch.len() as u64 {
            return Err(RuntimeError::Protocol("transcript batch 长度不匹配".into()));
        }
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
    fn set_preferred_session(&self, key: &SessionKey) -> Result<(), RuntimeError> {
        let db = self.lock_connection()?;
        if metadata_in(&db, key)?.is_none_or(|m| m.deleted) {
            return Err(RuntimeError::Protocol("preferred session 无效".into()));
        }
        db.execute("INSERT INTO runtime_settings(name,value) VALUES('active_session',?1) ON CONFLICT(name) DO UPDATE SET value=excluded.value",params![key.0])?;
        Ok(())
    }
    fn execute_lifecycle(
        &self,
        command: &SessionCommand,
        expected: Option<&SessionLifetimeId>,
        operation: &str,
    ) -> Result<SessionMetadata, RuntimeError> {
        if operation.is_empty() || operation.len() > 256 {
            return Err(RuntimeError::Protocol("operation id 无效".into()));
        }
        let payload = serde_json::to_string(&(command, expected))
            .map_err(|e| RuntimeError::Protocol(e.to_string()))?;
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
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
                    .map_err(|e| RuntimeError::Protocol(e.to_string()))?;
                let prior: SessionMetadata = serde_json::from_str(&result)
                    .map_err(|e| RuntimeError::Protocol(e.to_string()))?;
                if old != legacy || expected.is_some_and(|e| e != &prior.lifetime) {
                    return Err(RuntimeError::Protocol(
                        "lifecycle operation id / lifetime 冲突".into(),
                    ));
                }
            }
            return serde_json::from_str(&result)
                .map_err(|e| RuntimeError::Protocol(e.to_string()));
        }
        let meta = match command {
            SessionCommand::Create { key } => create_in(&tx, key)?,
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
        let result =
            serde_json::to_string(&meta).map_err(|e| RuntimeError::Protocol(e.to_string()))?;
        tx.execute(
            "INSERT INTO lifecycle_receipts VALUES (?1,?2,?3)",
            params![operation, payload, result],
        )?;
        tx.commit()?;
        Ok(meta)
    }
    fn create_session(&self, key: &SessionKey) -> Result<SessionMetadata, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        let meta = create_in(&tx, key)?;
        tx.commit()?;
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
        tx.commit()?;
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
        tx.commit()?;
        Ok(child)
    }
}
impl TranscriptStore for RunStore {
    fn import_legacy(&self, key: &SessionKey, messages: &[Message]) -> Result<(), RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        tx.execute(
            "INSERT OR IGNORE INTO sessions(id,created_at_ms) VALUES (?1,?2)",
            params![key.0, now_ms()],
        )?;
        let meta = metadata_in(&tx, key)?
            .ok_or_else(|| RuntimeError::Internal("缺少 import head".into()))?;
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
        tx.commit()?;
        Ok(())
    }
    fn append_transcript(
        &self,
        owner: &ExactOwner,
        operation: &str,
        messages: &[Message],
    ) -> Result<TranscriptSeq, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
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
        tx.commit()?;
        Ok(revision)
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
    let busy:i64=db.query_row("SELECT count(*) FROM runs WHERE session_id=?1 AND lifetime=?2 AND status IN ('queued','running','waiting_interaction')",params![key.0,lifetime.0],|r|r.get(0))?;
    let resources:i64=db.query_row("SELECT count(*) FROM resources WHERE lifetime=?1 AND state IN ('starting','running','stopping')",params![lifetime.0],|r|r.get(0))?;
    if busy != 0 || resources != 0 {
        return Err(RuntimeError::Protocol(
            "session 仍有受管 run，必须先 cancel/join".into(),
        ));
    }
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
    let snapshot = snapshot_in(db, source)?;
    if snapshot.lifetime != meta.lifetime || snapshot.revision != revision {
        return Err(RuntimeError::Protocol("fork source stale".into()));
    }
    let messages = snapshot.messages;
    let child = create_in(db, target)?;
    insert_batch(db, target, &child.lifetime, "fork", None, &messages)?;
    Ok(child)
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
}
