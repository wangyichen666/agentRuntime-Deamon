use super::{RunStore, RuntimeError, sessions::fence_in};
use agent_core::*;
use rusqlite::{OptionalExtension, params};
use sha2::{Digest, Sha256};

use agent_storage_ports::{
    MemoryEpisodeExcerpt as EpisodeExcerpt, redact_memory_evidence as redact_evidence,
};

pub trait MemoryRepository: Send + Sync {
    fn memory_evidence(
        &self,
        visibility: &MemoryVisibility,
        memory_id: &str,
        after_source: usize,
        limit: usize,
    ) -> Result<MemoryEvidencePage, RuntimeError>;
    fn next_memory_ingest_due(&self) -> Result<Option<i64>, RuntimeError>;
    fn memory_assessments(
        &self,
        visibility: &MemoryVisibility,
    ) -> Result<Vec<MemoryAssessment>, RuntimeError>;
    fn record_memory_exposures(
        &self,
        owner: &ExactOwner,
        entries: &[MemoryExposure],
        channel: &str,
        policy: &str,
    ) -> Result<(), RuntimeError>;
    fn memory_feedback(
        &self,
        owner: &ExactOwner,
        operation: &str,
        memory_id: &str,
        feedback: MemoryFeedback,
    ) -> Result<(), RuntimeError>;
    fn flywheel_report(
        &self,
        lifetime: &SessionLifetimeId,
    ) -> Result<serde_json::Value, RuntimeError>;
    fn recover_memory_ingests(&self, limit: usize) -> Result<usize, RuntimeError>;
    fn memory_candidates(
        &self,
        visibility: &MemoryVisibility,
    ) -> Result<Vec<MemoryRecord>, RuntimeError>;
    fn memory_page(
        &self,
        visibility: &MemoryVisibility,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MemoryRecord>, RuntimeError>;
    fn store_memory(
        &self,
        owner: &ExactOwner,
        entry: &MemoryRecord,
    ) -> Result<MemoryRecord, RuntimeError>;
    fn forget_memory(
        &self,
        owner: &ExactOwner,
        visibility: &MemoryVisibility,
        id: &str,
        revision: u64,
    ) -> Result<bool, RuntimeError>;
    fn ingest_committed_turn(&self, owner: &ExactOwner) -> Result<(), RuntimeError>;
}
fn write_in(db: &rusqlite::Connection, entry: &MemoryRecord) -> Result<(), RuntimeError> {
    let (scope, lifetime, project) = match &entry.scope {
        MemoryScope::Session(l) => ("session", Some(l.0.as_str()), None),
        MemoryScope::Project(p) => ("project", None, Some(p.as_str())),
        MemoryScope::Global => ("global", None, None),
        MemoryScope::Legacy(_) => ("legacy", None, None),
    };
    if entry.content.is_empty()
        || entry.content.len() > 32768
        || entry.confidence > 100
        || entry.revision > i64::MAX as u64
    {
        return Err(RuntimeError::Protocol("memory 内容或元数据超出预算".into()));
    }
    if entry.content_digest != format!("{:x}", Sha256::digest(entry.content.as_bytes())) {
        return Err(RuntimeError::Protocol("memory digest 不匹配".into()));
    }
    db.execute(
        "INSERT INTO memories VALUES(?1,?2,?3,?4,?5,?6,?7)",
        params![
            entry.id,
            lifetime,
            project,
            scope,
            entry.source.as_ref().map(|o| o.run_id.0.as_str()),
            entry.revision,
            serde_json::to_string(entry).map_err(|e| RuntimeError::Protocol(e.to_string()))?
        ],
    )?;
    Ok(())
}
pub(crate) fn writable(db: &rusqlite::Connection, owner: &ExactOwner) -> Result<(), RuntimeError> {
    fence_in(db, owner)?;
    let raw:Option<Option<String>>=db.query_row("SELECT CASE WHEN typeof(snapshot_json)='text' AND length(CAST(snapshot_json AS BLOB))<=1048576 THEN snapshot_json END FROM run_snapshots WHERE run_id=?1",params![owner.run_id.0],|r|r.get(0)).optional()?;
    let raw = raw
        .map(|value| value.ok_or(CoordinationError::Corrupt))
        .transpose()?;
    if raw
        .map(|raw| serde_json::from_str::<RunSnapshot>(&raw))
        .transpose()
        .map_err(|e| RuntimeError::Protocol(e.to_string()))?
        .is_some_and(|s| s.context_read_only)
    {
        return Err(RuntimeError::Protocol(
            "context_read_only 禁止 memory 写入".into(),
        ));
    }
    Ok(())
}
pub(crate) fn read_source_evidence(
    db: &rusqlite::Connection,
    source: &ExactOwner,
    id: &str,
    char_limit: usize,
) -> Result<MemorySourceEvidence, RuntimeError> {
    if id.len() > 512 {
        return Err(RuntimeError::Protocol("来源ID超出预算".into()));
    }
    let (lifetime, seq) = id
        .rsplit_once(':')
        .ok_or_else(|| RuntimeError::Protocol("来源ID无效".into()))?;
    let seq = seq
        .parse::<u64>()
        .map_err(|_| RuntimeError::Protocol("来源序号无效".into()))?;
    if lifetime != source.session_lifetime_id.0 {
        return Err(RuntimeError::Protocol("来源lifetime不匹配".into()));
    }
    let batch:Option<(Option<String>,u64,Option<String>,Option<String>)>=db.query_row("SELECT CASE WHEN length(CAST(messages_json AS BLOB))<=4194304 THEN messages_json END,start_seq,CASE WHEN length(CAST(digest AS BLOB))=64 THEN digest END,CASE WHEN length(CAST(run_id AS BLOB)) BETWEEN 1 AND 1024 THEN run_id END FROM transcript_batches WHERE lifetime=?1 AND start_seq<=?2 AND end_seq>?2 ORDER BY start_seq DESC LIMIT 1",params![lifetime,seq],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
    let (raw, start, digest, run) =
        batch.ok_or_else(|| RuntimeError::Protocol("来源证据已缺失".into()))?;
    let (raw, digest, run) = (
        raw.ok_or(CoordinationError::Corrupt)?,
        digest.ok_or(CoordinationError::Corrupt)?,
        run.ok_or(CoordinationError::Corrupt)?,
    );
    if digest != format!("{:x}", Sha256::digest(raw.as_bytes())) || run != source.run_id.0 {
        return Err(RuntimeError::Protocol("来源digest或owner不匹配".into()));
    }
    let messages: Vec<Message> =
        serde_json::from_str(&raw).map_err(|e| RuntimeError::Protocol(e.to_string()))?;
    let message = messages
        .get((seq - start) as usize)
        .ok_or_else(|| RuntimeError::Protocol("来源序号越界".into()))?;
    let safe = redact_evidence(
        message.content.as_deref().unwrap_or_default(),
        char_limit + 1,
    );
    Ok(MemorySourceEvidence {
        source_id: id.into(),
        role: message.role.clone(),
        truncated: safe.chars().count() > char_limit,
        text: safe.chars().take(char_limit).collect(),
    })
}
impl MemoryRepository for RunStore {
    fn memory_evidence(
        &self,
        visibility: &MemoryVisibility,
        memory_id: &str,
        after_source: usize,
        limit: usize,
    ) -> Result<MemoryEvidencePage, RuntimeError> {
        self.memory_evidence_scoped(visibility, memory_id, after_source, limit, None)
    }
    fn next_memory_ingest_due(&self) -> Result<Option<i64>, RuntimeError> {
        RunStore::next_memory_ingest_due(self)
    }
    fn memory_assessments(
        &self,
        visibility: &MemoryVisibility,
    ) -> Result<Vec<MemoryAssessment>, RuntimeError> {
        RunStore::memory_assessments(self, visibility)
    }
    fn record_memory_exposures(
        &self,
        owner: &ExactOwner,
        entries: &[MemoryExposure],
        channel: &str,
        policy: &str,
    ) -> Result<(), RuntimeError> {
        RunStore::record_memory_exposures(self, owner, entries, channel, policy)
    }
    fn memory_feedback(
        &self,
        owner: &ExactOwner,
        operation: &str,
        memory_id: &str,
        feedback: MemoryFeedback,
    ) -> Result<(), RuntimeError> {
        RunStore::memory_feedback(self, owner, operation, memory_id, feedback)
    }
    fn flywheel_report(
        &self,
        lifetime: &SessionLifetimeId,
    ) -> Result<serde_json::Value, RuntimeError> {
        RunStore::flywheel_report(self, lifetime)
    }
    fn recover_memory_ingests(&self, limit: usize) -> Result<usize, RuntimeError> {
        RunStore::recover_memory_ingests(self, limit)
    }
    fn memory_candidates(
        &self,
        visibility: &MemoryVisibility,
    ) -> Result<Vec<MemoryRecord>, RuntimeError> {
        crate::run_memories::memory_list(self, visibility, None, 1000, false, None)
    }
    fn memory_page(
        &self,
        visibility: &MemoryVisibility,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MemoryRecord>, RuntimeError> {
        crate::run_memories::memory_list(self, visibility, after, limit, true, None)
    }
    fn store_memory(
        &self,
        owner: &ExactOwner,
        entry: &MemoryRecord,
    ) -> Result<MemoryRecord, RuntimeError> {
        self.store_memory_scoped(owner, entry, None)
    }
    fn forget_memory(
        &self,
        owner: &ExactOwner,
        visibility: &MemoryVisibility,
        id: &str,
        revision: u64,
    ) -> Result<bool, RuntimeError> {
        self.forget_memory_scoped(owner, visibility, id, revision, None)
    }
    fn ingest_committed_turn(&self, owner: &ExactOwner) -> Result<(), RuntimeError> {
        self.ingest_memory_scoped(owner, None)
    }
}
impl RunStore {
    pub fn import_legacy_memory(&self, source: &str, bytes: &[u8]) -> Result<(), RuntimeError> {
        self.import_legacy_memory_scoped(&TenantContext::local(), source, bytes)
    }
    pub(crate) fn import_legacy_memory_scoped(
        &self,
        scope: &TenantContext,
        source: &str,
        bytes: &[u8],
    ) -> Result<(), RuntimeError> {
        if scope != &TenantContext::local() {
            return Err(AuthorizationError::Forbidden.into());
        }
        let mut db = self.lock_connection()?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        if tx.query_row(
            "SELECT count(*) FROM memory_legacy_imports WHERE source=?1",
            params![source],
            |r| r.get::<_, i64>(0),
        )? != 0
        {
            return Ok(());
        }
        let digest = format!("{:x}", Sha256::digest(bytes));
        for (index, line) in String::from_utf8_lossy(bytes).lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            // 坏行也保留为隔离审计条目，绝不推断其来源或升级作用域。
            let content = serde_json::from_str::<serde_json::Value>(line)
                .ok()
                .and_then(|v| v.get("content").and_then(|v| v.as_str()).map(str::to_owned))
                .unwrap_or_else(|| line.into());
            let content = content.chars().take(8000).collect::<String>();
            let entry = MemoryRecord {
                id: format!("legacy:{digest}:{index}"),
                layer: MemoryLayer::Episode,
                scope: MemoryScope::Legacy(source.into()),
                kind: MemoryKind::Explicit,
                content_digest: format!("{:x}", Sha256::digest(content.as_bytes())),
                content,
                source: None,
                source_message_ids: vec![],
                event_time: 0,
                created_at: 0,
                updated_at: 0,
                expires_at: None,
                confidence: 0,
                confirmed_by_user: false,
                revision: 0,
            };
            write_in(&tx, &entry)?;
        }
        tx.execute(
            "INSERT INTO memory_legacy_imports VALUES(?1,?2)",
            params![source, digest],
        )?;
        super::views::commit(tx)?;
        Ok(())
    }
}

impl RunStore {
    pub(crate) fn store_memory_scoped(
        &self,
        owner: &ExactOwner,
        entry: &MemoryRecord,
        envelope: Option<&EnterpriseOwnerEnvelope>,
    ) -> Result<MemoryRecord, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        crate::run_memories::check_memory_proof_in(&tx, owner, envelope)?;
        writable(&tx, owner)?;
        if entry.source.as_ref() != Some(owner) {
            return Err(RuntimeError::Protocol("memory source owner 不匹配".into()));
        }
        match &entry.scope {
            MemoryScope::Session(l) if l == &owner.session_lifetime_id => {}
            MemoryScope::Project(_) if entry.confirmed_by_user => {}
            MemoryScope::Global
                if entry.confirmed_by_user && entry.layer == MemoryLayer::Semantic => {}
            _ => return Err(RuntimeError::Protocol("memory scope 未授权".into())),
        }
        let previous =
            crate::run_memories::read_memory_in(&tx, &entry.id, envelope.map(|e| &e.scope))?;
        if let Some(previous) = previous {
            let old_ttl = previous
                .expires_at
                .map(|t| t.saturating_sub(previous.created_at));
            let new_ttl = entry.expires_at.map(|t| t.saturating_sub(entry.created_at));
            if previous.content_digest != entry.content_digest
                || previous.scope != entry.scope
                || previous.source != entry.source
                || previous.kind != entry.kind
                || previous.layer != entry.layer
                || previous.confirmed_by_user != entry.confirmed_by_user
                || old_ttl != new_ttl
            {
                return Err(RuntimeError::Protocol("memory id 幂等冲突".into()));
            }
            super::views::commit(tx)?;
            return Ok(previous);
        } else {
            let forgotten: i64 = tx.query_row(
                "SELECT count(*) FROM memory_forget_receipts WHERE memory_id=?1 AND result=1",
                params![entry.id],
                |r| r.get(0),
            )?;
            if forgotten > 0 {
                return Err(RuntimeError::Protocol(
                    "该 memory operation 已被遗忘，不能通过重试复活".into(),
                ));
            }
            write_in(&tx, entry)?;
        }
        super::views::commit(tx)?;
        Ok(entry.clone())
    }
    pub(crate) fn forget_memory_scoped(
        &self,
        owner: &ExactOwner,
        visibility: &MemoryVisibility,
        id: &str,
        revision: u64,
        envelope: Option<&EnterpriseOwnerEnvelope>,
    ) -> Result<bool, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        crate::run_memories::check_memory_proof_in(&tx, owner, envelope)?;
        writable(&tx, owner)?;
        if visibility.lifetime != owner.session_lifetime_id {
            return Err(RuntimeError::Protocol(
                "memory visibility owner 不匹配".into(),
            ));
        }
        let receipt:Option<bool>=tx.query_row("SELECT result FROM memory_forget_receipts WHERE run_id=?1 AND memory_id=?2 AND revision=?3",params![owner.run_id.0,id,revision],|r|r.get(0)).optional()?;
        if let Some(result) = receipt {
            return Ok(result);
        }
        let saved = crate::run_memories::read_memory_in(&tx, id, envelope.map(|e| &e.scope))?;
        let Some(entry) = saved else {
            tx.execute(
                "INSERT INTO memory_forget_receipts VALUES(?1,?2,?3,0)",
                params![owner.run_id.0, id, revision],
            )?;
            super::views::commit(tx)?;
            return Ok(false);
        };
        if !visibility.allows(&entry, super::now_ms() as u64 / 1000) || entry.revision != revision {
            return Err(RuntimeError::Protocol(
                "memory 不可见或 revision 冲突".into(),
            ));
        }
        tx.execute(
            "DELETE FROM memories WHERE id=?1 AND revision=?2",
            params![id, revision],
        )?;
        tx.execute(
            "INSERT INTO memory_forget_receipts VALUES(?1,?2,?3,1)",
            params![owner.run_id.0, id, revision],
        )?;
        super::views::commit(tx)?;
        Ok(true)
    }
    pub(crate) fn ingest_memory_scoped(
        &self,
        owner: &ExactOwner,
        envelope: Option<&EnterpriseOwnerEnvelope>,
    ) -> Result<(), RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        crate::run_memories::check_memory_proof_in(&tx, owner, envelope)?;
        writable(&tx, owner)?;
        if super::read_run_in(&tx, &owner.run_id.0)?.is_some_and(|run| run.kind == RunKind::Compact)
        {
            return Err(RuntimeError::Protocol(
                "compact turn 不能摄入聊天记忆".into(),
            ));
        }
        let status: String = tx.query_row(
            "SELECT status FROM turn_commits WHERE run_id=?1",
            params![owner.run_id.0],
            |r| r.get(0),
        )?;
        if status != "completed" {
            return Err(RuntimeError::Protocol("只能摄入成功提交的 turn".into()));
        }
        if tx.query_row(
            "SELECT count(*) FROM memory_ingests WHERE run_id=?1",
            params![owner.run_id.0],
            |r| r.get::<_, i64>(0),
        )? != 0
        {
            return Ok(());
        }
        let mut q=tx.prepare("SELECT CASE WHEN typeof(messages_json)='text' AND length(CAST(messages_json AS BLOB))<=4194304 THEN messages_json END,start_seq FROM transcript_batches WHERE run_id=?1 ORDER BY start_seq LIMIT 32769")?;
        let rows = q.query_map(params![owner.run_id.0], |r| {
            Ok((r.get::<_, Option<String>>(0)?, r.get::<_, u64>(1)?))
        })?;
        let mut excerpt = EpisodeExcerpt::default();
        let mut rows_seen = 0usize;
        let mut bytes_seen = 0usize;
        for row in rows {
            rows_seen += 1;
            if rows_seen > 32768 {
                return Err(CoordinationError::Corrupt.into());
            }
            let (raw, start) = row?;
            let raw = raw.ok_or(CoordinationError::Corrupt)?;
            bytes_seen = bytes_seen.saturating_add(raw.len());
            if bytes_seen > 16 * 1024 * 1024 {
                return Err(CoordinationError::Corrupt.into());
            }
            let messages: Vec<Message> =
                serde_json::from_str(&raw).map_err(|e| RuntimeError::Protocol(e.to_string()))?;
            for (index, message) in messages.into_iter().enumerate() {
                excerpt.push(
                    &message,
                    format!("{}:{}", owner.session_lifetime_id.0, start + index as u64),
                );
            }
            if excerpt.full {
                break;
            }
        }
        drop(q);
        let now = super::now_ms() as u64 / 1000;
        let entry = MemoryRecord {
            id: format!("turn:{}", owner.run_id.0),
            layer: MemoryLayer::Episode,
            scope: MemoryScope::Session(owner.session_lifetime_id.clone()),
            kind: MemoryKind::TurnSummary,
            content_digest: format!("{:x}", Sha256::digest(excerpt.text.as_bytes())),
            content: excerpt.text,
            source: Some(owner.clone()),
            source_message_ids: excerpt.source_message_ids,
            event_time: now,
            created_at: now,
            updated_at: now,
            expires_at: None,
            confidence: 70,
            confirmed_by_user: false,
            revision: 0,
        };
        if !entry.content.is_empty() {
            write_in(&tx, &entry)?;
        }
        tx.execute(
            "INSERT INTO memory_ingests VALUES(?1,?2,'completed',NULL)",
            params![owner.run_id.0, owner.session_lifetime_id.0],
        )?;
        super::views::commit(tx)?;
        Ok(())
    }
    pub(crate) fn memory_evidence_scoped(
        &self,
        visibility: &MemoryVisibility,
        memory_id: &str,
        after_source: usize,
        limit: usize,
        context: Option<(&TenantContext, &SessionKey)>,
    ) -> Result<MemoryEvidencePage, RuntimeError> {
        if memory_id.len() > 512 {
            return Err(RuntimeError::Protocol("memory evidence标识超出预算".into()));
        }
        let mut connection = self.lock_connection()?;
        let db = connection.transaction()?;
        crate::run_memories::check_memory_read_in(&db, visibility, context)?;
        let active: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM session_heads WHERE lifetime=?1 AND deleted=0)",
            params![visibility.lifetime.0],
            |r| r.get(0),
        )?;
        if !active {
            return Err(RuntimeError::Protocol(
                "memory evidence lifetime 已失效".into(),
            ));
        }
        let entry =
            crate::run_memories::read_memory_in(&db, memory_id, context.map(|(scope, _)| scope))?
                .ok_or(CoordinationError::Conflict)?;
        if !visibility.allows(&entry, super::now_ms() as u64 / 1000) {
            return Err(RuntimeError::Protocol("memory 不可见".into()));
        }
        let restricted = entry.source.as_ref().is_some_and(|s| {
            s.session_lifetime_id != visibility.lifetime || fence_in(&db, s).is_err()
        });
        let mut page = MemoryEvidencePage {
            memory_id: entry.id,
            content_digest: entry.content_digest,
            sources: vec![],
            next_source: after_source,
            has_more: false,
            source_scope_restricted: restricted,
        };
        if serde_json::to_vec(&page)
            .map_err(|e| RuntimeError::Protocol(e.to_string()))?
            .len()
            > 32 * 1024
        {
            return Err(RuntimeError::Protocol(
                "memory evidence元数据超出预算".into(),
            ));
        }
        if restricted || entry.source.is_none() {
            return Ok(page);
        }
        let source = entry
            .source
            .ok_or_else(|| RuntimeError::Protocol("memory 没有来源 owner".into()))?;
        let mut bytes = serde_json::to_vec(&page)
            .map_err(|e| RuntimeError::Protocol(e.to_string()))?
            .len()
            + 128;
        for (index, id) in entry
            .source_message_ids
            .iter()
            .enumerate()
            .skip(after_source)
            .take(limit.clamp(1, 16))
        {
            let item = read_source_evidence(&db, &source, id, 4096)?;
            let size = serde_json::to_vec(&item)
                .map_err(|e| RuntimeError::Protocol(e.to_string()))?
                .len();
            if bytes + size + 1 > 32 * 1024 {
                break;
            }
            bytes += size + 1;
            page.sources.push(item);
            page.next_source = index + 1;
        }
        page.has_more = page.next_source < entry.source_message_ids.len();
        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SessionLifecycle, SessionQuery};
    #[test]
    fn episode_excerpt_is_utf8_bounded_and_marks_loss() {
        let mut excerpt = EpisodeExcerpt::default();
        excerpt.push(&Message::text(Role::Assistant, "  \n"), "empty".into());
        assert!(excerpt.text.is_empty());
        for index in 0..100 {
            excerpt.push(
                &Message::text(Role::Assistant, "中文😀".repeat(500)),
                index.to_string(),
            );
        }
        assert!(excerpt.text.len() <= EPISODE_BYTES);
        assert!(excerpt.text.contains("该消息已截断"));
        assert!(excerpt.text.ends_with(OMITTED));
        assert!(excerpt.source_message_ids.len() < 100);
        assert!(!excerpt.source_message_ids.contains(&"empty".into()));
    }
    #[test]
    fn automatic_excerpt_filters_credentials_and_keeps_safe_evidence() {
        let text = "构建使用 cargo\nAPI_KEY=sk-do-not-store\nAuthorization: Bearer private-token\n-----BEGIN PRIVATE KEY-----\nprivate-material\n-----END PRIVATE KEY-----\nhttps://user:password@example.com\n继续使用 Rust";
        let mut excerpt = EpisodeExcerpt::default();
        excerpt.push(&Message::text(Role::User, text), "source".into());
        assert!(excerpt.text.contains("构建使用 cargo"));
        assert!(excerpt.text.contains("继续使用 Rust"));
        assert!(excerpt.text.contains("凭据行已过滤"));
        for secret in [
            "sk-do-not-store",
            "private-token",
            "private-material",
            "password",
        ] {
            assert!(!excerpt.text.contains(secret));
        }
        let mut empty = EpisodeExcerpt::default();
        empty.push(
            &Message::text(Role::User, "API_KEY=sk-do-not-store"),
            "secret".into(),
        );
        assert!(empty.text.is_empty());
        assert!(empty.source_message_ids.is_empty());
        assert_eq!(
            redact_episode("普通 token budget: 1000"),
            "普通 token budget: 1000"
        );
    }
    fn admission(store: &RunStore, key: &str) -> ExactOwner {
        let key = SessionKey(key.into());
        store.create_session(&key).unwrap();
        let Admission::New(run) = store
            .admit_with_route(
                key,
                RequestId::Number(1),
                "memory fact",
                AdmissionMode::Queue,
                None,
            )
            .unwrap()
        else {
            panic!("new")
        };
        store.try_start_queued(&run.run_id).unwrap();
        store.run_owner(&run.run_id).unwrap()
    }
    fn entry(owner: &ExactOwner, scope: MemoryScope, id: &str) -> MemoryRecord {
        MemoryRecord {
            id: id.into(),
            layer: MemoryLayer::Semantic,
            scope,
            kind: MemoryKind::Explicit,
            content: "memory fact".into(),
            source: Some(owner.clone()),
            source_message_ids: vec![],
            event_time: 1,
            created_at: 1,
            updated_at: 1,
            expires_at: None,
            confidence: 100,
            confirmed_by_user: true,
            content_digest: format!("{:x}", Sha256::digest(b"memory fact")),
            revision: 0,
        }
    }
    fn visible(owner: &ExactOwner) -> MemoryVisibility {
        MemoryVisibility {
            lifetime: owner.session_lifetime_id.clone(),
            project: "p".into(),
            allow_confirmed_global: true,
        }
    }
    #[test]
    fn evidence_pages_verify_sources_redact_secrets_and_obey_forget() {
        use crate::TranscriptStore;
        let store = RunStore::open(std::path::Path::new(":memory:")).unwrap();
        let owner = admission(&store, "evidence");
        let mut evidence = Message::assistant_with_thinking(
            "安全证据\nAPI_KEY=sk-private",
            Some("私有思考".into()),
        );
        evidence.image_urls.push("私有图片".into());
        store
            .append_transcript(&owner, "evidence", &[evidence])
            .unwrap();
        store
            .finish(&owner.run_id, RunStatus::Completed, Some("完成"), None)
            .unwrap();
        store.ingest_committed_turn(&owner).unwrap();
        let id = format!("turn:{}", owner.run_id.0);
        let first = store.memory_evidence(&visible(&owner), &id, 0, 1).unwrap();
        assert_eq!(first.sources.len(), 1);
        assert_eq!(
            first.sources[0].source_id,
            format!("{}:0", owner.session_lifetime_id.0)
        );
        assert_eq!(first.next_source, 1);
        assert!(first.has_more);
        let second = store
            .memory_evidence(&visible(&owner), &id, first.next_source, 1)
            .unwrap();
        assert!(second.sources[0].text.contains("安全证据"));
        let raw = serde_json::to_string(&second).unwrap();
        for secret in ["sk-private", "私有思考", "私有图片"] {
            assert!(!raw.contains(secret));
        }
        let third = store
            .memory_evidence(&visible(&owner), &id, second.next_source, 16)
            .unwrap();
        assert_eq!(third.sources[0].text, "完成");
        assert!(!third.has_more);
        assert_eq!(
            store.flywheel_report(&owner.session_lifetime_id).unwrap()["exposure_count"],
            0
        );
        assert!(
            store
                .forget_memory(&owner, &visible(&owner), &id, 0)
                .unwrap()
        );
        assert!(store.memory_evidence(&visible(&owner), &id, 0, 4).is_err());
    }
    #[test]
    fn evidence_does_not_expand_cross_session_authorization_or_accept_bad_digest() {
        let store = RunStore::open(std::path::Path::new(":memory:")).unwrap();
        let a = admission(&store, "evidence-a");
        let b = admission(&store, "evidence-b");
        let mut global = entry(&a, MemoryScope::Global, "global-source");
        global.source_message_ids = vec![format!("{}:0", a.session_lifetime_id.0)];
        store.store_memory(&a, &global).unwrap();
        let foreign = store
            .memory_evidence(&visible(&b), &global.id, 0, 4)
            .unwrap();
        assert!(foreign.source_scope_restricted);
        assert!(foreign.sources.is_empty());
        let own = store
            .memory_evidence(&visible(&a), &global.id, 0, 4)
            .unwrap();
        assert_eq!(own.sources[0].text, "memory fact");
        let local = entry(
            &a,
            MemoryScope::Session(a.session_lifetime_id.clone()),
            "local-source",
        );
        store.store_memory(&a, &local).unwrap();
        assert!(
            store
                .memory_evidence(&visible(&b), &local.id, 0, 4)
                .is_err()
        );
        store
            .finish(&a.run_id, RunStatus::Completed, Some("完成"), None)
            .unwrap();
        store
            .lock_connection()
            .unwrap()
            .execute(
                "UPDATE transcript_batches SET digest='bad' WHERE lifetime=?1",
                params![a.session_lifetime_id.0],
            )
            .unwrap();
        assert!(
            store
                .memory_evidence(&visible(&a), &global.id, 0, 4)
                .is_err()
        );
        store
            .end_session(&a.session_key, &a.session_lifetime_id, false)
            .unwrap();
        assert!(
            store
                .memory_evidence(&visible(&a), &global.id, 0, 4)
                .is_err()
        );
    }
    #[test]
    fn evidence_has_unicode_and_serialized_page_budgets() {
        use crate::TranscriptStore;
        let store = RunStore::open(std::path::Path::new(":memory:")).unwrap();
        let owner = admission(&store, "evidence-budget");
        store
            .append_transcript(
                &owner,
                "large",
                &(0..8)
                    .map(|_| Message::text(Role::Assistant, "😀".repeat(5000)))
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        let mut record = entry(
            &owner,
            MemoryScope::Session(owner.session_lifetime_id.clone()),
            "large-source",
        );
        record.source_message_ids = (1..=8)
            .map(|seq| format!("{}:{seq}", owner.session_lifetime_id.0))
            .collect();
        store.store_memory(&owner, &record).unwrap();
        let page = store
            .memory_evidence(&visible(&owner), &record.id, 0, 16)
            .unwrap();
        assert!(serde_json::to_vec(&page).unwrap().len() <= 32 * 1024);
        assert_eq!(page.sources.len(), 1);
        assert_eq!(page.sources[0].text.chars().count(), 4096);
        assert!(page.sources[0].truncated && page.has_more);
        assert!(
            store
                .memory_evidence(&visible(&owner), &"x".repeat(513), 0, 4)
                .is_err()
        );
        let next = store
            .memory_evidence(&visible(&owner), &record.id, page.next_source, 16)
            .unwrap();
        assert_ne!(page.sources[0].source_id, next.sources[0].source_id);
    }
    #[test]
    fn empty_ingest_commits_receipt_without_memory_and_large_turn_is_bounded() {
        use crate::TranscriptStore;
        let store = RunStore::open(std::path::Path::new(":memory:")).unwrap();
        let owner = admission(&store, "large");
        let messages = (0..100)
            .map(|_| Message::text(Role::Assistant, "中文😀".repeat(500)))
            .collect::<Vec<_>>();
        store
            .append_transcript(&owner, "large-batch", &messages)
            .unwrap();
        store
            .finish(&owner.run_id, RunStatus::Completed, Some("最终答复"), None)
            .unwrap();
        store.ingest_committed_turn(&owner).unwrap();
        store.ingest_committed_turn(&owner).unwrap();
        let visibility = MemoryVisibility {
            lifetime: owner.session_lifetime_id.clone(),
            project: "".into(),
            allow_confirmed_global: false,
        };
        let records = store.memory_candidates(&visibility).unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0].content.len() <= EPISODE_BYTES);
        assert!(records[0].content.ends_with(OMITTED));
        assert!(records[0].source_message_ids.len() < 102);

        // 空输入合法创建并启动：验证真实事务回执，不模拟删 transcript。
        let key = SessionKey("empty".into());
        store.create_session(&key).unwrap();
        let Admission::New(run) = store
            .admit_with_route(key, RequestId::Number(2), "  ", AdmissionMode::Queue, None)
            .unwrap()
        else {
            panic!("new")
        };
        store.try_start_queued(&run.run_id).unwrap();
        let owner = store.run_owner(&run.run_id).unwrap();
        store
            .finish(&owner.run_id, RunStatus::Completed, Some("  \n"), None)
            .unwrap();
        store.ingest_committed_turn(&owner).unwrap();
        store.ingest_committed_turn(&owner).unwrap();
        let db = store.lock_connection().unwrap();
        assert_eq!(
            db.query_row(
                "SELECT count(*) FROM memories WHERE id=?1",
                params![format!("turn:{}", owner.run_id.0)],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
        assert_eq!(
            db.query_row(
                "SELECT count(*) FROM memory_ingests WHERE run_id=?1",
                params![owner.run_id.0],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
    }
    #[test]
    fn visibility_and_completed_ingest_obey_lifetime_and_commit_barriers() {
        let store = RunStore::open(std::path::Path::new(":memory:")).unwrap();
        let a = admission(&store, "a");
        let b = admission(&store, "b");
        store
            .store_memory(
                &a,
                &entry(
                    &a,
                    MemoryScope::Session(a.session_lifetime_id.clone()),
                    "session",
                ),
            )
            .unwrap();
        store
            .store_memory(&a, &entry(&a, MemoryScope::Project("p".into()), "project"))
            .unwrap();
        store
            .store_memory(&a, &entry(&a, MemoryScope::Global, "global"))
            .unwrap();
        let visibility = MemoryVisibility {
            lifetime: b.session_lifetime_id.clone(),
            project: "p".into(),
            allow_confirmed_global: true,
        };
        let entries = store.memory_candidates(&visibility).unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|e| visibility.allows(e, 1)));
        store
            .import_legacy_memory("legacy", b"{\"content\":\"memory fact\"}\nbroken")
            .unwrap();
        assert_eq!(store.memory_candidates(&visibility).unwrap().len(), 2);
        assert!(store.ingest_committed_turn(&a).is_err());
        store
            .finish(
                &a.run_id,
                RunStatus::Completed,
                Some("committed answer"),
                None,
            )
            .unwrap();
        store.ingest_committed_turn(&a).unwrap();
        store.ingest_committed_turn(&a).unwrap();
        let count: i64 = store
            .lock_connection()
            .unwrap()
            .query_row("SELECT count(*) FROM memory_ingests", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
        store
            .end_session(&a.session_key, &a.session_lifetime_id, true)
            .unwrap();
        store.create_session(&a.session_key).unwrap();
        assert!(store.ingest_committed_turn(&a).is_err());
        assert!(
            store
                .store_memory(&a, &entry(&a, MemoryScope::Global, "late"))
                .is_err()
        );
        let entries = store.memory_candidates(&visibility).unwrap();
        assert_eq!(entries.len(), 2);
    }
    #[test]
    fn read_only_blocks_memory_write_and_forget_and_restart_preserves_scope() {
        let dir = std::env::temp_dir().join(format!(
            "memory-scope-{}-{}",
            std::process::id(),
            super::super::now_ms()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("runtime.sqlite3");
        let store = RunStore::open(&path).unwrap();
        let owner = admission(&store, "a");
        let item = entry(
            &owner,
            MemoryScope::Session(owner.session_lifetime_id.clone()),
            "memory",
        );
        store.store_memory(&owner, &item).unwrap();
        let snapshot = RunSnapshot {
            workflow: None,
            installed_generation:None,
            billing_policy: None,
            artifact_store_id: None,
            entry_channel: agent_core::HookChannel::DaemonRpc,
            route: None,
            tools: vec![],
            cwd: "p".into(),
            permission_mode: "risk".into(),
            sandbox_requested: "native".into(),
            sandbox_effective: "native".into(),
            sandbox_notice: None,
            docker_image: None,
            delegation_context: None,
            context_read_only: true,
            context_token_budget: 1000,
            context_policy_fingerprint: None,
            tool_catalog_digest: "empty".into(),
            memory_entry_budget: 8,
            memory_token_budget: 1024,
            max_tool_calls: None,
            config_generation: 0,
        };
        store
            .lock_connection()
            .unwrap()
            .execute(
                "INSERT INTO run_snapshots VALUES(?1,?2)",
                params![owner.run_id.0, serde_json::to_string(&snapshot).unwrap()],
            )
            .unwrap();
        assert!(
            store
                .store_memory(&owner, &entry(&owner, MemoryScope::Global, "blocked"))
                .is_err()
        );
        let visibility = MemoryVisibility {
            lifetime: owner.session_lifetime_id.clone(),
            project: "p".into(),
            allow_confirmed_global: true,
        };
        assert!(
            store
                .forget_memory(&owner, &visibility, "memory", 0)
                .is_err()
        );
        drop(store);
        let store = RunStore::open(&path).unwrap();
        assert_eq!(store.memory_candidates(&visibility).unwrap().len(), 1);
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn forget_receipt_replays_exact_result_and_old_lifetime_is_rejected() {
        let store = RunStore::open(std::path::Path::new(":memory:")).unwrap();
        let owner = admission(&store, "forget");
        let item = entry(
            &owner,
            MemoryScope::Session(owner.session_lifetime_id.clone()),
            "fact",
        );
        store.store_memory(&owner, &item).unwrap();
        let visibility = MemoryVisibility {
            lifetime: owner.session_lifetime_id.clone(),
            project: "p".into(),
            allow_confirmed_global: true,
        };
        assert!(
            store
                .forget_memory(&owner, &visibility, &item.id, item.revision)
                .unwrap()
        );
        assert!(
            store
                .forget_memory(&owner, &visibility, &item.id, item.revision)
                .unwrap()
        );
        assert!(store.store_memory(&owner, &item).is_err());
        store
            .finish(&owner.run_id, RunStatus::Completed, Some("done"), None)
            .unwrap();
        store
            .end_session(&owner.session_key, &owner.session_lifetime_id, false)
            .unwrap();
        assert!(
            store
                .forget_memory(&owner, &visibility, &item.id, item.revision)
                .is_err()
        );
    }
}
