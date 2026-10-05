//! 学习信号只接受用户反馈，查询原文与记忆内容不复制到学习表。
use super::{MaintenanceRepository, MemoryRepository, RunStore, RuntimeError, SessionQuery};
use agent_core::*;
use rusqlite::{OptionalExtension, params};

impl RunStore {
    pub fn next_memory_ingest_due(&self) -> Result<Option<i64>, RuntimeError> {
        let db = self.lock_connection()?;
        Ok(db.query_row("SELECT MIN(COALESCE(b.retry_at_ms,?1)) FROM turn_commits t JOIN runs r ON r.id=t.run_id JOIN session_heads h ON h.lifetime=r.lifetime AND h.deleted=0 LEFT JOIN memory_ingest_retries b ON b.run_id=t.run_id WHERE t.status='completed' AND NOT EXISTS(SELECT 1 FROM memory_ingests i WHERE i.run_id=t.run_id) AND NOT EXISTS(SELECT 1 FROM run_snapshots s WHERE s.run_id=t.run_id AND json_extract(s.snapshot_json,'$.context_read_only')=1) AND COALESCE(b.attempts,0)<3",params![super::now_ms()],|r|r.get(0))?)
    }
    pub fn memory_assessments(
        &self,
        visibility: &MemoryVisibility,
    ) -> Result<Vec<MemoryAssessment>, RuntimeError> {
        let db = self.lock_connection()?;
        // 与候选集的上限对齐，不能因新评价挤掉仍在候选集中的旧负反馈。
        let mut q = db.prepare("SELECT memory_id,content_digest,feedback FROM memory_assessments WHERE lifetime=?1 AND memory_id IN (SELECT id FROM memories WHERE ((scope='session' AND lifetime=?1) OR (scope='project' AND project=?2) OR (scope='global' AND ?3 AND json_extract(data_json,'$.confirmed_by_user')=1 AND json_extract(data_json,'$.layer')='semantic')) AND (json_extract(data_json,'$.expires_at') IS NULL OR json_extract(data_json,'$.expires_at')>?4) ORDER BY rowid DESC LIMIT 1000) ORDER BY memory_id")?;
        let rows = q
            .query_map(
                params![
                    visibility.lifetime.0,
                    visibility.project,
                    visibility.allow_confirmed_global,
                    super::now_ms() / 1000
                ],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|(memory_id, content_digest, raw)| {
                Ok(MemoryAssessment {
                    memory_id,
                    content_digest,
                    feedback: serde_json::from_str(&raw)
                        .map_err(|e| RuntimeError::Protocol(e.to_string()))?,
                })
            })
            .collect()
    }

    pub fn record_memory_exposures(
        &self,
        owner: &ExactOwner,
        entries: &[MemoryExposure],
        channel: &str,
        policy: &str,
    ) -> Result<(), RuntimeError> {
        if entries.len() > 20
            || !matches!(channel, "context_prepared" | "tool_result_prepared")
            || policy.len() > 80
        {
            return Err(RuntimeError::Protocol(
                "memory 曝光超出预算或来源无效".into(),
            ));
        }
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        super::sessions::fence_in(&tx, owner)?;
        let snapshot: Option<String> = tx
            .query_row(
                "SELECT snapshot_json FROM run_snapshots WHERE run_id=?1",
                params![owner.run_id.0],
                |r| r.get(0),
            )
            .optional()?;
        let snapshot = snapshot
            .map(|raw| serde_json::from_str::<RunSnapshot>(&raw))
            .transpose()
            .map_err(|e| RuntimeError::Protocol(e.to_string()))?;
        // 只读运行可以读取已有评价，但不产生学习数据。
        if snapshot.as_ref().is_some_and(|s| s.context_read_only) {
            return Ok(());
        }
        let visibility = MemoryVisibility {
            lifetime: owner.session_lifetime_id.clone(),
            project: snapshot.map_or_else(String::new, |s| s.cwd),
            allow_confirmed_global: true,
        };
        for entry in entries {
            let raw: Option<String> = tx
                .query_row(
                    "SELECT data_json FROM memories WHERE id=?1",
                    params![entry.id],
                    |r| r.get(0),
                )
                .optional()?;
            // 并发遗忘时只忽略已不存在的项，绝不复活它。
            let Some(raw) = raw else {
                continue;
            };
            let saved: MemoryRecord =
                serde_json::from_str(&raw).map_err(|e| RuntimeError::Protocol(e.to_string()))?;
            if !visibility.allows(&saved, super::now_ms() as u64 / 1000)
                || saved.content_digest != entry.content_digest
                || saved.revision != entry.revision
            {
                return Err(RuntimeError::Protocol(
                    "memory 曝光版本或作用域不匹配".into(),
                ));
            }
            tx.execute(
                "INSERT OR IGNORE INTO memory_exposures VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    owner.run_id.0,
                    entry.id,
                    owner.session_lifetime_id.0,
                    entry.content_digest,
                    entry.revision,
                    channel,
                    policy,
                    super::now_ms()
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn memory_feedback(
        &self,
        owner: &ExactOwner,
        operation: &str,
        memory_id: &str,
        feedback: MemoryFeedback,
    ) -> Result<(), RuntimeError> {
        if operation.is_empty() || operation.len() > 128 || memory_id.len() > 512 {
            return Err(RuntimeError::Protocol("feedback 标识超出预算".into()));
        }
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        super::memory::writable(&tx, owner)?;
        let command = serde_json::to_string(&(memory_id, feedback))
            .map_err(|e| RuntimeError::Protocol(e.to_string()))?;
        let old:Option<String>=tx.query_row("SELECT command_json FROM memory_feedback_receipts WHERE run_id=?1 AND operation_id=?2",params![owner.run_id.0,operation],|r|r.get(0)).optional()?;
        if let Some(old) = old {
            if old != command {
                return Err(RuntimeError::Protocol("feedback operation 幂等冲突".into()));
            }
            return Ok(());
        }
        let status: String = tx.query_row(
            "SELECT status FROM runs WHERE id=?1",
            params![owner.run_id.0],
            |r| r.get(0),
        )?;
        if matches!(
            status.as_str(),
            "queued" | "running" | "waiting_interaction"
        ) {
            return Err(RuntimeError::Protocol("请在运行终态后提交反馈".into()));
        }
        let digest:Option<String>=tx.query_row("SELECT e.content_digest FROM memory_exposures e JOIN memories m ON m.id=e.memory_id WHERE e.run_id=?1 AND e.memory_id=?2 AND e.lifetime=?3 AND e.content_digest=json_extract(m.data_json,'$.content_digest') AND e.revision=m.revision",params![owner.run_id.0,memory_id,owner.session_lifetime_id.0],|r|r.get(0)).optional()?;
        let digest = digest.ok_or_else(|| {
            RuntimeError::Protocol("memory 缺少该运行的有效曝光，或已遗忘".into())
        })?;
        let raw =
            serde_json::to_string(&feedback).map_err(|e| RuntimeError::Protocol(e.to_string()))?;
        tx.execute("INSERT INTO memory_assessments VALUES(?1,?2,?3,?4,?5) ON CONFLICT(lifetime,memory_id) DO UPDATE SET content_digest=excluded.content_digest,feedback=excluded.feedback,updated_at_ms=excluded.updated_at_ms",params![owner.session_lifetime_id.0,memory_id,digest,raw,super::now_ms()])?;
        tx.execute(
            "INSERT INTO memory_feedback_receipts VALUES(?1,?2,?3,?4,?5)",
            params![
                owner.run_id.0,
                operation,
                memory_id,
                owner.session_lifetime_id.0,
                command
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn flywheel_report(
        &self,
        lifetime: &SessionLifetimeId,
    ) -> Result<serde_json::Value, RuntimeError> {
        let db = self.lock_connection()?;
        let active: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM session_heads WHERE lifetime=?1 AND deleted=0)",
            params![lifetime.0],
            |r| r.get(0),
        )?;
        if !active {
            return Err(RuntimeError::Protocol("flywheel lifetime 已失效".into()));
        }
        let exposure_count: i64 = db.query_row(
            "SELECT count(*) FROM memory_exposures WHERE lifetime=?1",
            params![lifetime.0],
            |r| r.get(0),
        )?;
        let assessment_count: i64 = db.query_row(
            "SELECT count(*) FROM memory_assessments WHERE lifetime=?1",
            params![lifetime.0],
            |r| r.get(0),
        )?;
        let mut q=db.prepare("SELECT e.run_id,e.memory_id,e.content_digest,e.channel,e.policy,r.status,a.feedback FROM memory_exposures e JOIN runs r ON r.id=e.run_id LEFT JOIN memory_assessments a ON a.lifetime=e.lifetime AND a.memory_id=e.memory_id AND a.content_digest=e.content_digest WHERE e.lifetime=?1 ORDER BY e.created_at_ms DESC,e.run_id,e.memory_id LIMIT 100")?;
        let rows=q.query_map(params![lifetime.0],|r| Ok(serde_json::json!({"run_id":r.get::<_,String>(0)?,"memory_id":r.get::<_,String>(1)?,"content_digest":r.get::<_,String>(2)?,"channel":r.get::<_,String>(3)?,"policy":r.get::<_,String>(4)?,"run_status":r.get::<_,String>(5)?,"feedback":r.get::<_,Option<String>>(6)?.and_then(|s|serde_json::from_str::<serde_json::Value>(&s).ok())})))?.collect::<Result<Vec<_>,_>>()?;
        let mut feedback = serde_json::Map::new();
        for label in ["helpful", "irrelevant", "incorrect", "outdated"] {
            let count: i64 = db.query_row(
                "SELECT count(*) FROM memory_assessments WHERE lifetime=?1 AND feedback=?2",
                params![lifetime.0, format!("\"{label}\"")],
                |r| r.get(0),
            )?;
            feedback.insert(label.into(), count.into());
        }
        let pending:i64=db.query_row("SELECT count(*) FROM turn_commits t JOIN runs r ON r.id=t.run_id WHERE r.lifetime=?1 AND t.status='completed' AND NOT EXISTS(SELECT 1 FROM memory_ingests i WHERE i.run_id=t.run_id) AND NOT EXISTS(SELECT 1 FROM run_snapshots s WHERE s.run_id=t.run_id AND json_extract(s.snapshot_json,'$.context_read_only')=1)",params![lifetime.0],|r|r.get(0))?;
        let exhausted:i64=db.query_row("SELECT count(*) FROM memory_ingest_retries b JOIN runs r ON r.id=b.run_id WHERE r.lifetime=?1 AND b.attempts>=3 AND NOT EXISTS(SELECT 1 FROM memory_ingests i WHERE i.run_id=b.run_id)",params![lifetime.0],|r|r.get(0))?;
        Ok(
            serde_json::json!({"lifetime":lifetime,"exposure_count":exposure_count,"assessed_memory_count":assessment_count,"feedback":feedback,"pending_ingests":pending,"retry_exhausted":exhausted,"exposures":rows,"has_more":exposure_count>100,"sample_limit":100,"signal_note":"曝光是请求材料准备记录，不证明模型采用；运行成功不等于记忆有用。评价仅用于本会话 lifetime，未改变事实或作用域。"}),
        )
    }

    pub fn recover_memory_ingests(&self, limit: usize) -> Result<usize, RuntimeError> {
        let runs = {
            let db = self.lock_connection()?;
            let mut q=db.prepare("SELECT t.run_id FROM turn_commits t JOIN runs r ON r.id=t.run_id JOIN session_heads h ON h.lifetime=r.lifetime AND h.deleted=0 WHERE t.status='completed' AND NOT EXISTS(SELECT 1 FROM memory_ingests i WHERE i.run_id=t.run_id) AND NOT EXISTS(SELECT 1 FROM run_snapshots s WHERE s.run_id=t.run_id AND json_extract(s.snapshot_json,'$.context_read_only')=1) AND NOT EXISTS(SELECT 1 FROM memory_ingest_retries b WHERE b.run_id=t.run_id AND (b.attempts>=3 OR b.retry_at_ms>?1)) ORDER BY t.rowid LIMIT ?2")?;
            q.query_map(params![super::now_ms(), limit.min(32) as i64], |r| {
                r.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?
        };
        let mut completed = 0;
        for run in runs {
            let owner = self.run_owner(&RunId(run.clone()))?;
            match self.ingest_committed_turn(&owner) {
                Ok(()) => {
                    self.record_maintenance(&owner, "memory_ingest", "completed")?;
                    completed += 1;
                }
                Err(error) => {
                    tracing::warn!(run_id=%run,error=%error,"记忆恢复维护失败");
                    let db = self.lock_connection()?;
                    // 竞争 clear/delete 后不再写旧 lifetime；其他故障有界退避。
                    if super::sessions::fence_in(&db, &owner).is_ok() {
                        db.execute("INSERT INTO memory_ingest_retries VALUES(?1,1,?2) ON CONFLICT(run_id) DO UPDATE SET attempts=attempts+1,retry_at_ms=excluded.retry_at_ms",params![run,super::now_ms()+60_000])?;
                        drop(db);
                        self.record_maintenance(&owner, "memory_ingest", "failed")?;
                    }
                }
            }
        }
        Ok(completed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SessionLifecycle;
    use sha2::{Digest, Sha256};
    static NEXT_TEMP: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    fn start(store: &RunStore, key: &str, request: u64) -> ExactOwner {
        let key = SessionKey(key.into());
        if store
            .session_metadata(&key)
            .unwrap()
            .is_none_or(|m| m.deleted)
        {
            store.create_session(&key).unwrap();
        }
        let Admission::New(run) = store
            .admit_with_route(
                key,
                RequestId::Number(request),
                "cargo 约定",
                AdmissionMode::Queue,
                None,
            )
            .unwrap()
        else {
            panic!("new");
        };
        assert!(store.try_start_queued(&run.run_id).unwrap());
        store.run_owner(&run.run_id).unwrap()
    }
    fn memory(store: &RunStore, owner: &ExactOwner, id: &str) -> MemoryRecord {
        let entry = MemoryRecord {
            id: id.into(),
            layer: MemoryLayer::Semantic,
            scope: MemoryScope::Session(owner.session_lifetime_id.clone()),
            kind: MemoryKind::Explicit,
            content: "cargo 约定".into(),
            source: Some(owner.clone()),
            source_message_ids: vec![],
            event_time: 1,
            created_at: 1,
            updated_at: 1,
            expires_at: None,
            confidence: 100,
            confirmed_by_user: true,
            content_digest: format!("{:x}", Sha256::digest("cargo 约定".as_bytes())),
            revision: 0,
        };
        store.store_memory(owner, &entry).unwrap()
    }
    fn visibility(owner: &ExactOwner) -> MemoryVisibility {
        MemoryVisibility {
            lifetime: owner.session_lifetime_id.clone(),
            project: "".into(),
            allow_confirmed_global: true,
        }
    }
    fn expose(store: &RunStore, owner: &ExactOwner, entry: &MemoryRecord) {
        store
            .record_memory_exposures(
                owner,
                &[entry.into()],
                "context_prepared",
                "lexical-feedback-v1",
            )
            .unwrap();
    }
    fn finish(store: &RunStore, owner: &ExactOwner) {
        store
            .finish(&owner.run_id, RunStatus::Completed, Some("完成"), None)
            .unwrap();
    }
    fn temp() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "fw-{}-{}-{}",
            std::process::id(),
            crate::now_ms(),
            NEXT_TEMP.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
    #[test]
    fn feedback_is_durable_idempotent_and_bound_to_exposure_and_digest() {
        let dir = temp();
        let path = dir.join("runtime.sqlite3");
        let store = RunStore::open(&path).unwrap();
        let owner = start(&store, "a", 1);
        let entry = memory(&store, &owner, "m");
        assert!(
            store
                .memory_feedback(&owner, "before", "m", MemoryFeedback::Helpful)
                .is_err()
        );
        expose(&store, &owner, &entry);
        expose(&store, &owner, &entry);
        assert_eq!(
            store.flywheel_report(&owner.session_lifetime_id).unwrap()["exposure_count"],
            1
        );
        assert!(
            store
                .memory_feedback(&owner, "early", "m", MemoryFeedback::Helpful)
                .is_err()
        );
        let mut invalid = MemoryExposure::from(&entry);
        invalid.content_digest = "wrong".into();
        assert!(
            store
                .record_memory_exposures(&owner, &[invalid], "context_prepared", "v1")
                .is_err()
        );
        finish(&store, &owner);
        store
            .memory_feedback(&owner, "vote", "m", MemoryFeedback::Incorrect)
            .unwrap();
        store
            .memory_feedback(&owner, "vote", "m", MemoryFeedback::Incorrect)
            .unwrap();
        assert!(
            store
                .memory_feedback(&owner, "vote", "m", MemoryFeedback::Helpful)
                .is_err()
        );
        assert!(
            store
                .memory_feedback(&owner, "foreign", "missing", MemoryFeedback::Helpful)
                .is_err()
        );
        drop(store);
        let store = RunStore::open(&path).unwrap();
        let assessments = store.memory_assessments(&visibility(&owner)).unwrap();
        assert_eq!(assessments.len(), 1);
        assert_eq!(assessments[0].feedback, MemoryFeedback::Incorrect);
        store
            .memory_feedback(&owner, "corrected", "m", MemoryFeedback::Helpful)
            .unwrap();
        // 重发旧幂等命令不可撤销新的评价。
        store
            .memory_feedback(&owner, "vote", "m", MemoryFeedback::Incorrect)
            .unwrap();
        assert_eq!(
            store.memory_assessments(&visibility(&owner)).unwrap()[0].feedback,
            MemoryFeedback::Helpful
        );
        assert_eq!(
            store.flywheel_report(&owner.session_lifetime_id).unwrap()["feedback"]["helpful"],
            1
        );
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn feedback_cannot_cross_lifetime_and_forget_purges_learning_without_resurrection() {
        let store = RunStore::open(std::path::Path::new(":memory:")).unwrap();
        let a = start(&store, "a", 1);
        let b = start(&store, "b", 1);
        let item = memory(&store, &a, "m");
        assert!(
            store
                .record_memory_exposures(
                    &b,
                    &[MemoryExposure::from(&item)],
                    "context_prepared",
                    "v1"
                )
                .is_err()
        );
        expose(&store, &a, &item);
        finish(&store, &a);
        finish(&store, &b);
        assert!(
            store
                .memory_feedback(&b, "foreign", "m", MemoryFeedback::Helpful)
                .is_err()
        );
        store
            .memory_feedback(&a, "vote", "m", MemoryFeedback::Helpful)
            .unwrap();
        assert!(
            store
                .memory_assessments(&visibility(&b))
                .unwrap()
                .is_empty()
        );
        store.forget_memory(&a, &visibility(&a), "m", 0).unwrap();
        let report = store.flywheel_report(&a.session_lifetime_id).unwrap();
        assert_eq!(report["exposure_count"], 0);
        assert_eq!(report["assessed_memory_count"], 0);
        assert!(
            store
                .memory_feedback(&a, "vote", "m", MemoryFeedback::Helpful)
                .is_err()
        );
        expose(&store, &a, &item);
        assert_eq!(
            store.flywheel_report(&a.session_lifetime_id).unwrap()["exposure_count"],
            0
        );
        assert!(store.store_memory(&a, &item).is_err());
        store
            .end_session(&a.session_key, &a.session_lifetime_id, false)
            .unwrap();
        assert!(store.flywheel_report(&a.session_lifetime_id).is_err());
        assert!(
            store
                .record_memory_exposures(&a, &[], "context_prepared", "v1")
                .is_err()
        );
        assert!(
            store
                .memory_feedback(&a, "late", "m", MemoryFeedback::Helpful)
                .is_err()
        );
    }
    #[test]
    fn clear_purges_shared_learning_and_readonly_never_learns_or_ingests() {
        let store = RunStore::open(std::path::Path::new(":memory:")).unwrap();
        let owner = start(&store, "a", 1);
        let entry = memory(&store, &owner, "m");
        expose(&store, &owner, &entry);
        finish(&store, &owner);
        store
            .memory_feedback(&owner, "vote", "m", MemoryFeedback::Helpful)
            .unwrap();
        store
            .end_session(&owner.session_key, &owner.session_lifetime_id, false)
            .unwrap();
        for table in [
            "memory_exposures",
            "memory_assessments",
            "memory_feedback_receipts",
        ] {
            assert_eq!(
                store
                    .lock_connection()
                    .unwrap()
                    .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                        .get::<_, i64>(0))
                    .unwrap(),
                0
            );
        }
        let owner = start(&store, "a", 2);
        let item = memory(&store, &owner, "readonly");
        let snapshot = RunSnapshot {
            route: None,
            tools: vec![],
            cwd: "".into(),
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
        expose(&store, &owner, &item);
        finish(&store, &owner);
        assert_eq!(
            store.flywheel_report(&owner.session_lifetime_id).unwrap()["exposure_count"],
            0
        );
        assert!(
            store
                .memory_feedback(&owner, "vote", "readonly", MemoryFeedback::Helpful)
                .is_err()
        );
        assert_eq!(store.recover_memory_ingests(32).unwrap(), 0);
    }
    #[test]
    fn restart_recovers_bounded_missing_ingests_and_never_revives_forgotten_episode() {
        let dir = temp();
        let path = dir.join("runtime.sqlite3");
        let store = RunStore::open(&path).unwrap();
        let mut owners = Vec::new();
        for i in 1..=3 {
            let owner = start(&store, "a", i);
            finish(&store, &owner);
            owners.push(owner);
        }
        let old = start(&store, "deleted", 1);
        finish(&store, &old);
        store
            .end_session(&old.session_key, &old.session_lifetime_id, true)
            .unwrap();
        drop(store);
        let store = RunStore::open(&path).unwrap();
        assert_eq!(store.recover_memory_ingests(0).unwrap(), 0);
        assert_eq!(store.recover_memory_ingests(1).unwrap(), 1);
        assert_eq!(store.recover_memory_ingests(32).unwrap(), 2);
        assert_eq!(store.recover_memory_ingests(32).unwrap(), 0);
        let a = &owners[0];
        let id = format!("turn:{}", a.run_id.0);
        assert_eq!(store.memory_candidates(&visibility(a)).unwrap().len(), 3);
        store.forget_memory(a, &visibility(a), &id, 0).unwrap();
        assert_eq!(store.recover_memory_ingests(32).unwrap(), 0);
        assert_eq!(store.memory_candidates(&visibility(a)).unwrap().len(), 2);
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn schema12_migration_preserves_memory_and_adds_empty_learning_tables() {
        let dir = temp();
        let path = dir.join("runtime.sqlite3");
        let store = RunStore::open(&path).unwrap();
        let owner = start(&store, "a", 1);
        memory(&store, &owner, "m");
        store.lock_connection().unwrap().execute_batch("DROP TABLE memory_exposures; DROP TABLE memory_assessments; DROP TABLE memory_feedback_receipts; DROP TABLE memory_ingest_retries; DELETE FROM schema_migrations WHERE version=13;").unwrap();
        drop(store);
        let store = RunStore::open(&path).unwrap();
        assert_eq!(
            store.memory_candidates(&visibility(&owner)).unwrap().len(),
            1
        );
        assert_eq!(
            store.flywheel_report(&owner.session_lifetime_id).unwrap()["exposure_count"],
            0
        );
        assert_eq!(store.health_report().unwrap()["schema_version"], 13);
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn recovery_failures_backoff_without_starving_good_turns_and_exhaust_visibly() {
        let store = RunStore::open(std::path::Path::new(":memory:")).unwrap();
        let bad = start(&store, "bad", 1);
        finish(&store, &bad);
        let good = start(&store, "good", 1);
        finish(&store, &good);
        store
            .lock_connection()
            .unwrap()
            .execute(
                "UPDATE transcript_batches SET messages_json='broken' WHERE run_id=?1",
                params![bad.run_id.0],
            )
            .unwrap();
        assert_eq!(store.recover_memory_ingests(32).unwrap(), 1);
        assert_eq!(store.recover_memory_ingests(32).unwrap(), 0);
        for _ in 0..2 {
            store
                .lock_connection()
                .unwrap()
                .execute(
                    "UPDATE memory_ingest_retries SET retry_at_ms=0 WHERE run_id=?1",
                    params![bad.run_id.0],
                )
                .unwrap();
            assert_eq!(store.recover_memory_ingests(32).unwrap(), 0);
        }
        let report = store.flywheel_report(&bad.session_lifetime_id).unwrap();
        assert_eq!(report["pending_ingests"], 1);
        assert_eq!(report["retry_exhausted"], 1);
        assert_eq!(
            store.flywheel_report(&good.session_lifetime_id).unwrap()["pending_ingests"],
            0
        );
        store
            .lock_connection()
            .unwrap()
            .execute(
                "UPDATE memory_ingest_retries SET retry_at_ms=0 WHERE run_id=?1",
                params![bad.run_id.0],
            )
            .unwrap();
        assert_eq!(store.recover_memory_ingests(32).unwrap(), 0);
        assert_eq!(
            store
                .lock_connection()
                .unwrap()
                .query_row(
                    "SELECT attempts FROM memory_ingest_retries WHERE run_id=?1",
                    params![bad.run_id.0],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            3
        );
    }
}
