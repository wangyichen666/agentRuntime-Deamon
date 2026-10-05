//! 单事务恢复读取：不构造执行对象，不兼容导入，不写入业务事实。
use crate::sessions::{metadata_in, owner_in, snapshot_in};
use crate::{RunStore, RuntimeError, read_interaction_in, read_run_in};
use agent_core::*;
use rusqlite::{OptionalExtension, params};
use sha2::{Digest, Sha256};

impl RunStore {
    pub(crate) fn readback(
        &self,
        key: &SessionKey,
        mode: HistoryReadMode,
    ) -> Result<SessionReadback, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        let meta = metadata_in(&tx, key)?
            .filter(|m| !m.deleted)
            .ok_or_else(|| RuntimeError::Protocol("session 已删除或不存在".into()))?;
        let revision = SnapshotRevision(tx.query_row(
            "SELECT revision FROM snapshot_clock WHERE id=1",
            [],
            |r| r.get(0),
        )?);
        let (metadata_revision, generation): (u64, u64) = tx.query_row(
            "SELECT metadata_revision,projection_generation FROM session_heads WHERE session_id=?1",
            params![key.0],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let mut omitted = Vec::new();
        let (messages, batch_ranges) = match mode {
            HistoryReadMode::Omitted => {
                omitted.push("messages".into());
                omitted.push("batch_ranges".into());
                (Vec::new(), Vec::new())
            }
            HistoryReadMode::Canonical => {
                let snapshot = snapshot_in(&tx, key)?;
                (snapshot.messages, snapshot.batch_ranges)
            }
            HistoryReadMode::Model => {
                let snapshot = snapshot_in(&tx, key)?;
                let projection = crate::context_projection::projection_in(&tx, &snapshot)?;
                omitted.extend(
                    ["stable_prefix", "retrieved_context", "turn_overlay"].map(String::from),
                );
                omitted.push("canonical_messages".into());
                omitted.push("batch_ranges".into());
                (projection.messages, Vec::new())
            }
        };
        let mut query = tx.prepare("SELECT id FROM runs WHERE lifetime=?1 AND status IN ('queued','running','waiting_interaction') ORDER BY generation LIMIT 66")?;
        let ids = query
            .query_map(params![meta.lifetime.0], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        drop(query);
        if ids.len() > 65 {
            return Err(RuntimeError::Protocol(
                "active readback 超过 session 预算".into(),
            ));
        }
        let active_runs = ids
            .iter()
            .map(|id| {
                read_run_in(&tx, id)?
                    .ok_or_else(|| RuntimeError::Protocol("active run 消失".into()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let running = active_runs
            .iter()
            .filter(|r| matches!(r.status, RunStatus::Running | RunStatus::WaitingInteraction))
            .collect::<Vec<_>>();
        if running.len() > 1 {
            return Err(RuntimeError::Protocol("session 存在多个 writer".into()));
        }
        let active_owner = running
            .first()
            .map(|run| owner_in(&tx, &run.run_id))
            .transpose()?;
        let mut query = tx.prepare("SELECT q.id,q.run_id,q.message,q.status FROM queued_messages q JOIN runs r ON r.id=q.run_id WHERE r.lifetime=?1 AND q.status='queued' ORDER BY q.id LIMIT 65")?;
        let queue_rows = query
            .query_map(params![meta.lifetime.0], |r| {
                Ok(QueuedMessage {
                    id: r.get(0)?,
                    position: 0,
                    session_id: key.clone(),
                    run_id: RunId(r.get(1)?),
                    message: r.get(2)?,
                    status: r.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(query);
        if queue_rows.len() > 64 {
            return Err(RuntimeError::Protocol(
                "queue readback 超过 session 预算".into(),
            ));
        }
        let queue_rows = queue_rows
            .into_iter()
            .enumerate()
            .map(|(i, mut row)| {
                row.position = (i + 1) as i64;
                row
            })
            .collect::<Vec<_>>();
        let mut query = tx.prepare("SELECT i.id FROM interactions i JOIN runs r ON r.id=i.run_id WHERE r.lifetime=?1 AND i.status='pending' ORDER BY i.rowid LIMIT 257")?;
        let ids = query
            .query_map(params![meta.lifetime.0], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        drop(query);
        if ids.len() > 256 {
            return Err(RuntimeError::Protocol(
                "interaction readback 超过预算，请分页查询".into(),
            ));
        }
        let pending_interactions = ids
            .iter()
            .map(|id| {
                read_interaction_in(&tx, id)?
                    .ok_or_else(|| RuntimeError::Protocol("interaction 消失".into()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let last: Option<String> = tx.query_row("SELECT r.id FROM runs r JOIN events e ON e.run_id=r.id WHERE r.lifetime=?1 AND r.status IN ('completed','failed','cancelled','unknown_after_restart') AND e.event='terminal' ORDER BY e.rowid DESC LIMIT 1", params![meta.lifetime.0], |r| r.get(0)).optional()?;
        let last_durable_terminal = last.map(|id| read_run_in(&tx, &id)).transpose()?.flatten();
        let plan: Option<(u64, String)> = tx
            .query_row(
                "SELECT revision,data_json FROM session_plans WHERE lifetime=?1",
                params![meta.lifetime.0],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let current_plan = plan
            .map(|(revision, raw)| {
                Ok::<_, RuntimeError>(PlanSnapshot {
                    revision,
                    value: serde_json::from_str(&raw)
                        .map_err(|e| RuntimeError::Protocol(e.to_string()))?,
                })
            })
            .transpose()?;
        let plan_digest = current_plan
            .as_ref()
            .map(|plan| {
                serde_json::to_vec(&plan.value).map(|bytes| format!("{:x}", Sha256::digest(bytes)))
            })
            .transpose()
            .map_err(|e| RuntimeError::Protocol(e.to_string()))?;
        // 旧计划只读兼容：digest 用于展示，不授予 execute 身份。
        omitted.push("plan_execution_identity".into());
        let ledger: Option<String> = tx.query_row("SELECT l.envelope_json FROM context_ledgers l JOIN runs r ON r.id=l.run_id WHERE r.lifetime=?1 AND json_extract(l.envelope_json,'$.source.generation')=?2 ORDER BY r.generation DESC,l.round DESC LIMIT 1", params![meta.lifetime.0,generation], |r| r.get(0)).optional()?;
        let context_usage = ledger
            .map(|raw| {
                serde_json::from_str(&raw).map_err(|e| RuntimeError::Protocol(e.to_string()))
            })
            .transpose()?;
        let result = SessionReadback {
            schema_version: 1,
            session_id: key.clone(),
            session_lifetime_id: meta.lifetime.clone(),
            metadata: meta.clone(),
            snapshot_revision: revision,
            metadata_revision: SnapshotRevision(metadata_revision),
            transcript_revision: meta.revision,
            projection_generation: ProjectionGeneration(generation),
            history_mode: mode,
            omitted,
            messages,
            batch_ranges,
            active_owner,
            active_runs,
            queue_cursor: queue_rows.last().map(|r| r.id),
            queue_rows,
            pending_interactions,
            last_durable_terminal,
            current_plan,
            plan_digest,
            context_usage,
        };
        tx.commit()?;
        Ok(result)
    }
}

#[cfg(test)]
pub(crate) fn remove_v14_for_fixture(db: &rusqlite::Connection) {
    let mut query = db.prepare("SELECT name FROM sqlite_master WHERE type='trigger' AND (name LIKE 'snapshot_%' OR name='session_metadata_revision')").unwrap();
    let names = query
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(query);
    for name in names {
        db.execute_batch(&format!("DROP TRIGGER {name}")).unwrap();
    }
    db.execute_batch("ALTER TABLE session_heads DROP COLUMN metadata_revision; DROP TABLE snapshot_clock; DELETE FROM schema_migrations WHERE version=14;").unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Admission, AdmissionMode, SessionLifecycle, SessionQuery};
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    fn path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "readback-{}-{}.sqlite3",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }
    fn admit(store: &RunStore, key: &SessionKey, request: u64) -> RunRecord {
        let Admission::New(run) = store
            .admit_with_route(
                key.clone(),
                RequestId::Number(request),
                "用户输入",
                AdmissionMode::Queue,
                None,
            )
            .unwrap()
        else {
            panic!("首次准入");
        };
        run
    }
    fn contract(store: &RunStore) {
        let key = SessionKey("readback.jsonl".into());
        let metadata = store.create_session(&key).unwrap();
        let first = store
            .session_readback(&key, HistoryReadMode::Canonical)
            .unwrap();
        assert_eq!(first.session_lifetime_id, metadata.lifetime);
        assert!(first.active_owner.is_none());
        let run = admit(store, &key, 1);
        let next = admit(store, &key, 2);
        let queued = store
            .session_readback(&key, HistoryReadMode::Omitted)
            .unwrap();
        assert_eq!(queued.queue_rows.len(), 2);
        assert!(queued.messages.is_empty() && queued.omitted.contains(&"messages".into()));
        assert!(queued.snapshot_revision > first.snapshot_revision);
        assert!(store.try_start_queued(&run.run_id).unwrap());
        let active = store
            .session_readback(&key, HistoryReadMode::Canonical)
            .unwrap();
        assert_eq!(active.active_owner.unwrap().run_id, run.run_id);
        assert_eq!(active.queue_rows[0].id, queued.queue_rows[1].id);
        assert_eq!(active.queue_rows[0].run_id, next.run_id);
        assert_eq!(active.messages.len(), 1);
        let before_cancel = active.snapshot_revision;
        store.remove_queued(&next.run_id).unwrap();
        let cancelled = store
            .session_readback(&key, HistoryReadMode::Omitted)
            .unwrap();
        assert!(cancelled.queue_rows.is_empty() && cancelled.snapshot_revision > before_cancel);
        assert_eq!(
            cancelled.last_durable_terminal.unwrap().status,
            RunStatus::Cancelled
        );
        store
            .finish(&run.run_id, RunStatus::Completed, Some("完成"), None)
            .unwrap();
        let completed = store
            .session_readback(&key, HistoryReadMode::Canonical)
            .unwrap();
        assert!(completed.active_runs.is_empty());
        assert_eq!(completed.messages.len(), 2);
        assert_eq!(
            completed.last_durable_terminal.as_ref().unwrap().run_id,
            run.run_id
        );
        let again = store
            .session_readback(&key, HistoryReadMode::Canonical)
            .unwrap();
        assert_eq!(
            completed.snapshot_revision, again.snapshot_revision,
            "读取必须零写入"
        );
        let command = SessionCommand::End {
            key: key.clone(),
            delete: true,
        };
        store
            .execute_lifecycle(&command, Some(&metadata.lifetime), "delete-original")
            .unwrap();
        assert!(
            store
                .session_readback(&key, HistoryReadMode::Omitted)
                .is_err()
        );
        store.create_session(&key).unwrap();
        let fresh = store
            .session_readback(&key, HistoryReadMode::Canonical)
            .unwrap();
        assert_ne!(fresh.session_lifetime_id, metadata.lifetime);
        assert!(fresh.supersedes(&completed) && !completed.supersedes(&fresh));
        assert!(fresh.messages.is_empty() && fresh.last_durable_terminal.is_none());
        // 迟到 destructive mutation 与同 operation 不同 owner 均零写入。
        assert!(
            store
                .execute_lifecycle(&command, Some(&metadata.lifetime), "late-delete")
                .is_err()
        );
        assert!(
            store
                .execute_lifecycle(
                    &command,
                    Some(&fresh.session_lifetime_id),
                    "delete-original"
                )
                .is_err()
        );
        assert_eq!(
            store
                .session_readback(&key, HistoryReadMode::Omitted)
                .unwrap()
                .snapshot_revision,
            fresh.snapshot_revision
        );
    }
    #[test]
    fn canonical_readback_and_lifecycle_fences_share_file_and_memory_contract() {
        contract(&RunStore::open(std::path::Path::new(":memory:")).unwrap());
        let file = path();
        contract(&RunStore::open(&file).unwrap());
    }
    #[test]
    fn schema13_upgrade_keeps_identity_and_restarts_with_same_cursor() {
        let file = path();
        let store = RunStore::open(&file).unwrap();
        let key = SessionKey("old.jsonl".into());
        let metadata = store.create_session(&key).unwrap();
        let run = admit(&store, &key, 1);
        store.try_start_queued(&run.run_id).unwrap();
        store
            .finish(&run.run_id, RunStatus::Completed, Some("旧回答"), None)
            .unwrap();
        remove_v14_for_fixture(&store.lock_connection().unwrap());
        drop(store);
        let upgraded = RunStore::open(&file).unwrap();
        let snapshot = upgraded
            .session_readback(&key, HistoryReadMode::Canonical)
            .unwrap();
        assert_eq!(snapshot.session_lifetime_id, metadata.lifetime);
        assert_eq!(snapshot.messages.len(), 2);
        assert!(file.with_extension("v13.backup.sqlite3").exists());
        assert!(file.with_extension("v13.backup.verified.json").exists());
        drop(upgraded);
        let restarted = RunStore::open(&file).unwrap();
        assert_eq!(
            restarted
                .session_readback(&key, HistoryReadMode::Canonical)
                .unwrap()
                .snapshot_revision,
            snapshot.snapshot_revision
        );
    }
    #[test]
    fn corrupt_transcript_cannot_be_forked_or_recovered_as_empty_and_sparse_read_is_explicit() {
        let store = RunStore::open(std::path::Path::new(":memory:")).unwrap();
        let key = SessionKey("broken.jsonl".into());
        store.create_session(&key).unwrap();
        let run = admit(&store, &key, 1);
        store.try_start_queued(&run.run_id).unwrap();
        store
            .finish(&run.run_id, RunStatus::Completed, Some("证据"), None)
            .unwrap();
        let before = store
            .session_readback(&key, HistoryReadMode::Omitted)
            .unwrap();
        store
            .lock_connection()
            .unwrap()
            .execute(
                "UPDATE transcript_batches SET digest='bad' WHERE lifetime=?1",
                params![before.session_lifetime_id.0],
            )
            .unwrap();
        assert!(
            store
                .session_readback(&key, HistoryReadMode::Canonical)
                .is_err()
        );
        assert!(
            store
                .session_readback(&key, HistoryReadMode::Model)
                .is_err()
        );
        let target = SessionKey("fork-corrupt.jsonl".into());
        assert!(
            store
                .fork_session(&key, &target, before.transcript_revision)
                .is_err()
        );
        assert!(store.session_metadata(&target).unwrap().is_none());
        let sparse = store
            .session_readback(&key, HistoryReadMode::Omitted)
            .unwrap();
        assert!(sparse.omitted.contains(&"messages".into()));
        assert_eq!(
            sparse.snapshot_revision, before.snapshot_revision,
            "失败的 publication 必须回滚 revision"
        );
    }
    #[test]
    fn failed_terminal_rolls_back_snapshot_revision_and_pending_state() {
        let store = RunStore::open(std::path::Path::new(":memory:")).unwrap();
        let key = SessionKey("failure.jsonl".into());
        store.create_session(&key).unwrap();
        let run = admit(&store, &key, 1);
        store.try_start_queued(&run.run_id).unwrap();
        let before = store
            .session_readback(&key, HistoryReadMode::Canonical)
            .unwrap();
        store.lock_connection().unwrap().execute_batch("CREATE TRIGGER fail_terminal BEFORE INSERT ON events WHEN NEW.event='terminal' BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
        assert!(
            store
                .finish(&run.run_id, RunStatus::Completed, Some("不应发布"), None)
                .is_err()
        );
        let after = store
            .session_readback(&key, HistoryReadMode::Canonical)
            .unwrap();
        assert_eq!(before.snapshot_revision, after.snapshot_revision);
        assert_eq!(before.transcript_revision, after.transcript_revision);
        assert!(after.last_durable_terminal.is_none());
        assert_eq!(after.active_owner.unwrap().run_id, run.run_id);
    }
    #[test]
    fn simultaneous_reader_and_writer_connections_never_mix_turn_boundaries() {
        let file = path();
        let writer = RunStore::open(&file).unwrap();
        let reader = RunStore::open(&file).unwrap();
        let key = SessionKey("concurrent.jsonl".into());
        writer.create_session(&key).unwrap();
        let writer_key = key.clone();
        let thread = std::thread::spawn(move || {
            for i in 0..32 {
                let run = admit(&writer, &writer_key, i);
                writer.try_start_queued(&run.run_id).unwrap();
                writer
                    .finish(&run.run_id, RunStatus::Completed, Some("已提交"), None)
                    .unwrap();
            }
        });
        let mut revision = SnapshotRevision(0);
        for _ in 0..128 {
            let snapshot = reader
                .session_readback(&key, HistoryReadMode::Canonical)
                .unwrap();
            assert!(snapshot.snapshot_revision >= revision);
            revision = snapshot.snapshot_revision;
            assert_eq!(
                snapshot.messages.len() as u64,
                snapshot.transcript_revision.0
            );
            assert_eq!(
                snapshot.messages.len() % 2,
                usize::from(snapshot.active_owner.is_some()),
                "running user boundary 与 terminal 必须同一读事务"
            );
            if let Some(owner) = snapshot.active_owner {
                assert!(
                    snapshot
                        .active_runs
                        .iter()
                        .any(|run| run.run_id == owner.run_id && run.status == RunStatus::Running)
                );
            }
        }
        thread.join().unwrap();
        assert_eq!(
            reader
                .session_readback(&key, HistoryReadMode::Canonical)
                .unwrap()
                .messages
                .len(),
            64
        );
    }
}
