use super::{RunStore, RuntimeError, sessions::fence_in};
use agent_core::*;
use rusqlite::{OptionalExtension, params};
use sha2::{Digest, Sha256};

const EPISODE_BYTES: usize = 16 * 1024;
const OMITTED: &str = "\n[会话摘录已截断；完整证据保留在 transcript 中]\n";

/// 摘录不是模型提炼。字节与单消息字符双重上限，永不产生无标记的截断。
#[derive(Default)]
struct EpisodeExcerpt {
    text: String,
    source_message_ids: Vec<String>,
    full: bool,
}
impl EpisodeExcerpt {
    fn push(&mut self, message: &Message, source: String) {
        if self.full
            || !matches!(message.role, Role::User | Role::Assistant)
            || !message.tool_calls.is_empty()
        {
            return;
        }
        let Some(content) = message.content.as_deref().filter(|s| !s.trim().is_empty()) else {
            return;
        };
        let mut chars = content.chars();
        let excerpt: String = chars.by_ref().take(1000).collect();
        let clipped = chars.next().is_some();
        let line = format!(
            "{:?}: {excerpt}{}\n",
            message.role,
            if clipped { " [该消息已截断]" } else { "" }
        );
        let remaining = EPISODE_BYTES.saturating_sub(OMITTED.len() + self.text.len());
        if line.len() <= remaining {
            self.text.push_str(&line);
            self.source_message_ids.push(source);
        } else {
            let mut end = remaining.min(line.len());
            while !line.is_char_boundary(end) {
                end -= 1;
            }
            if end > 0 {
                self.text.push_str(&line[..end]);
                self.source_message_ids.push(source);
            }
            self.text.push_str(OMITTED);
            self.full = true;
        }
    }
}

pub trait MemoryRepository: Send + Sync {
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
fn writable(db: &rusqlite::Connection, owner: &ExactOwner) -> Result<(), RuntimeError> {
    fence_in(db, owner)?;
    let raw: Option<String> = db
        .query_row(
            "SELECT snapshot_json FROM run_snapshots WHERE run_id=?1",
            params![owner.run_id.0],
            |r| r.get(0),
        )
        .optional()?;
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
impl MemoryRepository for RunStore {
    fn memory_candidates(
        &self,
        visibility: &MemoryVisibility,
    ) -> Result<Vec<MemoryRecord>, RuntimeError> {
        let db = self.lock_connection()?;
        let mut q=db.prepare("SELECT data_json FROM memories WHERE ((scope='session' AND lifetime=?1) OR (scope='project' AND project=?2) OR (scope='global' AND ?3 AND json_extract(data_json,'$.confirmed_by_user')=1 AND json_extract(data_json,'$.layer')='semantic')) AND (json_extract(data_json,'$.expires_at') IS NULL OR json_extract(data_json,'$.expires_at')>?4) ORDER BY rowid DESC LIMIT 1000")?;
        let rows = q
            .query_map(
                params![
                    visibility.lifetime.0,
                    visibility.project,
                    visibility.allow_confirmed_global,
                    super::now_ms() / 1000
                ],
                |r| r.get::<_, String>(0),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|raw| {
                serde_json::from_str(&raw).map_err(|e| RuntimeError::Protocol(e.to_string()))
            })
            .collect()
    }
    fn memory_page(
        &self,
        visibility: &MemoryVisibility,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MemoryRecord>, RuntimeError> {
        let db = self.lock_connection()?;
        let mut q=db.prepare("SELECT data_json FROM memories WHERE id>?5 AND ((scope='session' AND lifetime=?1) OR (scope='project' AND project=?2) OR (scope='global' AND ?3 AND json_extract(data_json,'$.confirmed_by_user')=1 AND json_extract(data_json,'$.layer')='semantic')) AND (json_extract(data_json,'$.expires_at') IS NULL OR json_extract(data_json,'$.expires_at')>?4) ORDER BY id LIMIT ?6")?;
        let rows = q
            .query_map(
                params![
                    visibility.lifetime.0,
                    visibility.project,
                    visibility.allow_confirmed_global,
                    super::now_ms() / 1000,
                    after.unwrap_or(""),
                    limit.clamp(1, 101) as i64
                ],
                |r| r.get::<_, String>(0),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|raw| {
                serde_json::from_str(&raw).map_err(|e| RuntimeError::Protocol(e.to_string()))
            })
            .collect()
    }
    fn store_memory(
        &self,
        owner: &ExactOwner,
        entry: &MemoryRecord,
    ) -> Result<MemoryRecord, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
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
        let old: Option<String> = tx
            .query_row(
                "SELECT data_json FROM memories WHERE id=?1",
                params![entry.id],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(old) = old {
            let previous: MemoryRecord =
                serde_json::from_str(&old).map_err(|e| RuntimeError::Protocol(e.to_string()))?;
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
            tx.commit()?;
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
        tx.commit()?;
        Ok(entry.clone())
    }
    fn forget_memory(
        &self,
        owner: &ExactOwner,
        visibility: &MemoryVisibility,
        id: &str,
        revision: u64,
    ) -> Result<bool, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
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
        let raw: Option<String> = tx
            .query_row(
                "SELECT data_json FROM memories WHERE id=?1",
                params![id],
                |r| r.get(0),
            )
            .optional()?;
        let Some(raw) = raw else {
            tx.execute(
                "INSERT INTO memory_forget_receipts VALUES(?1,?2,?3,0)",
                params![owner.run_id.0, id, revision],
            )?;
            tx.commit()?;
            return Ok(false);
        };
        let entry: MemoryRecord =
            serde_json::from_str(&raw).map_err(|e| RuntimeError::Protocol(e.to_string()))?;
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
        tx.commit()?;
        Ok(true)
    }
    fn ingest_committed_turn(&self, owner: &ExactOwner) -> Result<(), RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        writable(&tx, owner)?;
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
        let mut q=tx.prepare("SELECT messages_json,start_seq FROM transcript_batches WHERE run_id=?1 ORDER BY start_seq")?;
        let rows = q.query_map(params![owner.run_id.0], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, u64>(1)?))
        })?;
        let mut excerpt = EpisodeExcerpt::default();
        for row in rows {
            let (raw, start) = row?;
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
        tx.commit()?;
        Ok(())
    }
}
impl RunStore {
    pub fn import_legacy_memory(&self, source: &str, bytes: &[u8]) -> Result<(), RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
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
        tx.commit()?;
        Ok(())
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
            route: None,
            tools: vec![],
            cwd: "p".into(),
            permission_mode: "risk".into(),
            sandbox_requested: "native".into(),
            sandbox_effective: "native".into(),
            sandbox_notice: None,
            docker_image: None,
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
