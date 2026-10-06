//! 发布事务内冻结版本，不用读取时的当前版本替历史事件背书。
use super::{RunStore, RuntimeError, read_run_in, sessions};
use agent_core::*;
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};

pub trait ViewRepository: Send + Sync {
    fn run_view_readback(
        &self,
        run: &RunId,
    ) -> Result<Option<(RunRecord, Option<ViewStamp>)>, RuntimeError>;
    fn event_view_stamp(
        &self,
        run: &RunId,
        seq: EventSeq,
    ) -> Result<Option<ViewStamp>, RuntimeError>;
    fn interaction_view_stamp(&self, id: &InteractionId)
    -> Result<Option<ViewStamp>, RuntimeError>;
    fn latest_view_event(
        &self,
        run: &RunId,
        event: &str,
        child: Option<&RunId>,
    ) -> Result<Option<StoredEvent>, RuntimeError>;
}
pub(crate) fn publish_in(
    db: &Connection,
    run: &RunId,
    seq: EventSeq,
    event: &str,
    data: &serde_json::Value,
) -> Result<(), RuntimeError> {
    let owner = sessions::owner_in(db, run)?;
    let (snapshot,metadata,transcript,projection):(u64,u64,u64,u64)=db.query_row("SELECT c.revision,h.metadata_revision,h.revision,h.projection_generation FROM snapshot_clock c JOIN session_heads h ON h.session_id=?1 AND h.lifetime=?2 AND h.deleted=0 WHERE c.id=1",params![owner.session_key.0,owner.session_lifetime_id.0],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)))?;

    let interaction = data
        .get("interaction")
        .and_then(|item| item.get("interaction_id"))
        .or_else(|| data.get("interaction_id"))
        .or_else(|| data.get("approval").and_then(|item| item.get("id")))
        .and_then(serde_json::Value::as_str)
        .map(|id| {
            db.query_row(
                "SELECT revision,status FROM interactions WHERE id=?1 AND run_id=?2",
                params![id, run.0],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map(|found| {
                found.map(|(revision, status)| ViewInteraction {
                    interaction_id: InteractionId(id.into()),
                    revision,
                    pending: status == "pending",
                })
            })
        })
        .transpose()?
        .flatten();
    let visible = serde_json::to_string(VISIBLE_VIEW_EVENTS)
        .map_err(|error| RuntimeError::Protocol(error.to_string()))?;
    let previous: u64 = db.query_row("SELECT COALESCE(MAX(seq),0) FROM events WHERE run_id=?1 AND seq<?2 AND event IN (SELECT value FROM json_each(?3)) AND (event!='run_started' OR json_extract(data_json,'$.kind')='compact')",params![run.0,seq.0,visible],|row|row.get(0))?;
    let stamp = ViewStamp {
        schema_version: 1,
        session_key: owner.session_key.clone(),
        session_lifetime_id: owner.session_lifetime_id.clone(),
        snapshot_revision: SnapshotRevision(snapshot),
        metadata_revision: SnapshotRevision(metadata),
        transcript_revision: TranscriptSeq(transcript),
        projection_generation: ProjectionGeneration(projection),
        owner: Some(owner),
        event_seq: Some(seq),
        previous_visible_seq: Some(EventSeq(previous)),
        terminal: (event == "compact_terminal")
            || (event == "terminal"
                && data
                    .get("status")
                    .and_then(serde_json::Value::as_str)
                    .map(RunStatus::parse)
                    .transpose()
                    .map_err(|error| RuntimeError::Protocol(error.to_string()))?
                    .is_some_and(|status| status.terminal())),
        interaction,
    };
    if !stamp.valid() {
        return Err(RuntimeError::Protocol("event 版本标记无效".into()));
    }
    let raw =
        serde_json::to_string(&stamp).map_err(|error| RuntimeError::Protocol(error.to_string()))?;
    let digest = format!("{:x}", Sha256::digest(raw.as_bytes()));
    db.execute(
        "INSERT INTO event_view_stamps(run_id,seq,stamp_json,stamp_digest) VALUES(?1,?2,?3,?4)",
        params![run.0, seq.0, raw, digest],
    )?;
    Ok(())
}
pub(crate) fn strip_transport_metadata(data: &mut serde_json::Value) {
    if let Some(object) = data.as_object_mut() {
        object.remove("_my_agent_view");
        object.remove("_my_agent_replay");
        object.remove("_my_agent_view_decision");
    }
}

pub(crate) fn stamp_in(
    db: &Connection,
    run: &RunId,
    seq: EventSeq,
) -> Result<Option<ViewStamp>, RuntimeError> {
    let row: Option<(String, String,u64)> = db
        .query_row(
            "SELECT stamp_json,stamp_digest,published FROM event_view_stamps WHERE run_id=?1 AND seq=?2",
            params![run.0, seq.0],
            |row| Ok((row.get(0)?, row.get(1)?,row.get(2)?)),
        )
        .optional()?;
    let Some((raw, digest, published)) = row else {
        return Ok(None);
    };
    if published != 1 || format!("{:x}", Sha256::digest(raw.as_bytes())) != digest {
        return Err(RuntimeError::Protocol("event 版本摘要损坏".into()));
    }
    let stamp: ViewStamp =
        serde_json::from_str(&raw).map_err(|error| RuntimeError::Protocol(error.to_string()))?;
    let owner = sessions::owner_in(db, run)?;
    if !stamp.valid() || stamp.owner.as_ref() != Some(&owner) || stamp.event_seq != Some(seq) {
        return Err(RuntimeError::Protocol("event 版本身份损坏".into()));
    }
    Ok(Some(stamp))
}
impl ViewRepository for RunStore {
    fn run_view_readback(
        &self,
        run: &RunId,
    ) -> Result<Option<(RunRecord, Option<ViewStamp>)>, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        let result = read_run_in(&tx, &run.0)?
            .map(|record| stamp_in(&tx, run, record.last_seq).map(|stamp| (record, stamp)))
            .transpose()?;
        tx.commit()?;
        Ok(result)
    }

    fn interaction_view_stamp(
        &self,
        id: &InteractionId,
    ) -> Result<Option<ViewStamp>, RuntimeError> {
        let db = self.lock_connection()?;
        let row:Option<(String,u64)>=db.query_row("SELECT e.run_id,e.seq FROM events e JOIN interactions i ON i.run_id=e.run_id AND i.id=?1 WHERE e.event='interaction_resolved' AND json_extract(e.data_json,'$.interaction_id')=?1 ORDER BY e.seq DESC LIMIT 1",params![id.0],|row|Ok((row.get(0)?,row.get(1)?))).optional()?;
        row.map(|(run, seq)| stamp_in(&db, &RunId(run), EventSeq(seq)))
            .transpose()
            .map(Option::flatten)
    }

    fn event_view_stamp(
        &self,
        run: &RunId,
        seq: EventSeq,
    ) -> Result<Option<ViewStamp>, RuntimeError> {
        let db = self.lock_connection()?;
        stamp_in(&db, run, seq)
    }
    fn latest_view_event(
        &self,
        run: &RunId,
        event: &str,
        child: Option<&RunId>,
    ) -> Result<Option<StoredEvent>, RuntimeError> {
        let db = self.lock_connection()?;
        let row:Option<(u64,String)>=db.query_row("SELECT seq,data_json FROM events WHERE run_id=?1 AND event=?2 AND (?3 IS NULL OR json_extract(data_json,'$.child_run_id')=?3) ORDER BY seq DESC LIMIT 1",params![run.0,event,child.map(|run|&run.0)],|row|Ok((row.get(0)?,row.get(1)?))).optional()?;
        row.map(|(seq, raw)| {
            let mut data: serde_json::Value = serde_json::from_str(&raw)
                .map_err(|error| RuntimeError::Protocol(error.to_string()))?;
            strip_transport_metadata(&mut data);
            if let Some(stamp) = stamp_in(&db, run, EventSeq(seq))? {
                data.as_object_mut()
                    .ok_or_else(|| RuntimeError::Protocol("事件data非object".into()))?
                    .insert(
                        "_my_agent_view".into(),
                        serde_json::to_value(stamp)
                            .map_err(|error| RuntimeError::Protocol(error.to_string()))?,
                    );
            }
            Ok(StoredEvent {
                run_id: run.clone(),
                seq: EventSeq(seq),
                event: event.into(),
                data,
            })
        })
        .transpose()
    }
}

/// 唯一提交封存点；只写本事务的新事件，读事务与已发布历史保持零写入。
pub(crate) fn commit(tx: rusqlite::Transaction<'_>) -> Result<(), RuntimeError> {
    let pending = {
        let mut query=tx.prepare("SELECT run_id,seq,stamp_json,stamp_digest FROM event_view_stamps WHERE published=0 ORDER BY run_id,seq")?;
        query
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (run, seq, raw, source_digest) in pending {
        let mut stamp: ViewStamp = serde_json::from_str(&raw)
            .map_err(|error| RuntimeError::Protocol(error.to_string()))?;
        if !stamp.valid()
            || format!("{:x}", Sha256::digest(raw.as_bytes())) != source_digest
            || stamp.owner.as_ref() != Some(&sessions::owner_in(&tx, &RunId(run.clone()))?)
            || stamp.event_seq != Some(EventSeq(seq))
        {
            return Err(RuntimeError::Protocol("待封存事件身份或摘要损坏".into()));
        }
        if stamp.terminal && !read_run_in(&tx, &run)?.is_some_and(|record| record.status.terminal())
        {
            return Err(RuntimeError::Protocol(
                "terminal 标记缺 canonical run 终态".into(),
            ));
        }
        stamp.snapshot_revision = SnapshotRevision(tx.query_row(
            "SELECT revision FROM snapshot_clock WHERE id=1",
            [],
            |row| row.get(0),
        )?);
        let current:Option<(String,u64,u64,u64)>=tx.query_row("SELECT lifetime,metadata_revision,revision,projection_generation FROM session_heads WHERE session_id=?1",params![stamp.session_key.0],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).optional()?;
        if let Some((lifetime, metadata, transcript, projection)) =
            current.filter(|(lifetime, _, _, _)| lifetime == &stamp.session_lifetime_id.0)
        {
            let _ = lifetime;
            stamp.metadata_revision = SnapshotRevision(metadata);
            stamp.transcript_revision = TranscriptSeq(transcript);
            stamp.projection_generation = ProjectionGeneration(projection);
        }
        if !stamp.valid() {
            return Err(RuntimeError::Protocol("提交事件版本无效".into()));
        }
        let raw = serde_json::to_string(&stamp)
            .map_err(|error| RuntimeError::Protocol(error.to_string()))?;
        let digest = format!("{:x}", Sha256::digest(raw.as_bytes()));
        tx.execute("UPDATE event_view_stamps SET stamp_json=?3,stamp_digest=?4,published=1 WHERE run_id=?1 AND seq=?2 AND published=0",params![run,seq,raw,digest])?;
    }
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SessionLifecycle, SessionQuery};
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    fn path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "agent-view-{}-{}.sqlite3",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }
    fn fixture(path: &std::path::Path) -> (RunStore, ExactOwner) {
        let store = RunStore::open(path).unwrap();
        let metadata = store.create_session(&SessionKey("view".into())).unwrap();
        let snapshot:RunSnapshot=serde_json::from_value(json!({"tools":[],"route":null,"cwd":".","permission_mode":"risk_approval","sandbox_requested":"native","sandbox_effective":"native","context_read_only":false,"max_tool_calls":8,"config_generation":0,"tool_catalog_digest":format!("{:x}",Sha256::digest(b"[]"))})).unwrap();
        let admission = RunAdmission {
            session_key: metadata.key,
            expected_lifetime: Some(metadata.lifetime),
            request_id: RequestId::Number(1),
            input: "原始请求".into(),
            mode: AdmissionMode::Queue,
            plan_execution: None,
        };
        let Admission::New(run) = store.admit_run(&admission, &snapshot).unwrap() else {
            panic!("new")
        };
        store.try_start_queued(&run.run_id).unwrap();
        let owner = store.run_owner(&run.run_id).unwrap();
        (store, owner)
    }
    fn clock(store: &RunStore) -> u64 {
        store
            .lock_connection()
            .unwrap()
            .query_row(
                "SELECT revision FROM snapshot_clock WHERE id=1",
                [],
                |row| row.get(0),
            )
            .unwrap()
    }
    #[test]
    fn transaction_versions_seal_atomically_and_read_only_replay_keeps_source_on_both_backends() {
        let file = path();
        for target in [std::path::Path::new(":memory:"), file.as_path()] {
            let (store, owner) = fixture(target);
            let first = store
                .append_event(&owner.run_id, "turn_started", &json!({}))
                .unwrap();
            let original = store
                .event_view_stamp(&owner.run_id, first)
                .unwrap()
                .unwrap();
            assert_eq!(
                original.previous_visible_seq,
                Some(EventSeq(0)),
                "聊天 run_started 是内部准入证据"
            );
            assert_eq!(original.snapshot_revision.0, clock(&store));
            store
                .append_event(&owner.run_id, "provider_audit", &json!({"attempt":1}))
                .unwrap();
            let delta = store
                .append_event(&owner.run_id, "text_delta", &json!({"delta":"回答"}))
                .unwrap();
            assert_eq!(
                store
                    .event_view_stamp(&owner.run_id, delta)
                    .unwrap()
                    .unwrap()
                    .previous_visible_seq,
                Some(first)
            );
            let before = clock(&store);
            for _ in 0..3 {
                assert_eq!(
                    store.event_view_stamp(&owner.run_id, first).unwrap(),
                    Some(original.clone())
                );
                assert_eq!(
                    store.events_after(&owner.run_id, EventSeq(0), 100).unwrap()
                        [first.0 as usize - 1]
                        .data["_my_agent_view"],
                    json!(original)
                );
                store
                    .session_readback(&owner.session_key, HistoryReadMode::Omitted)
                    .unwrap();
            }
            assert_eq!(clock(&store), before, "读回不得推进全局版本");
            store.lock_connection().unwrap().execute_batch("CREATE TRIGGER stamp_fault BEFORE UPDATE ON event_view_stamps WHEN NEW.published=1 BEGIN SELECT RAISE(ABORT,'stamp sealing fault'); END").unwrap();
            let cursor = store.read_run(&owner.run_id).unwrap().unwrap().last_seq;
            assert!(
                store
                    .append_event(&owner.run_id, "text_delta", &json!({"delta":"不得发布"}))
                    .is_err()
            );
            assert_eq!(
                store.read_run(&owner.run_id).unwrap().unwrap().last_seq,
                cursor
            );
            assert_eq!(clock(&store), before);
            store
                .lock_connection()
                .unwrap()
                .execute_batch("DROP TRIGGER stamp_fault")
                .unwrap();
            store
                .finish(&owner.run_id, RunStatus::Completed, Some("最终回答"), None)
                .unwrap();
            let terminal = store
                .latest_view_event(&owner.run_id, "terminal", None)
                .unwrap()
                .unwrap();
            let stamp: ViewStamp =
                serde_json::from_value(terminal.data["_my_agent_view"].clone()).unwrap();
            assert!(stamp.terminal);
            assert_eq!(
                stamp.snapshot_revision.0,
                clock(&store),
                "终态与所有同事务更改共享最终提交版本"
            );
            let final_snapshot = store
                .session_readback(&owner.session_key, HistoryReadMode::Omitted)
                .unwrap();
            assert_eq!(stamp.metadata_revision, final_snapshot.metadata_revision);
            assert_eq!(
                stamp.transcript_revision,
                final_snapshot.transcript_revision
            );
            assert_eq!(
                stamp.projection_generation,
                final_snapshot.projection_generation
            );
            store
                .lock_connection()
                .unwrap()
                .execute(
                    "UPDATE event_view_stamps SET stamp_json='{}' WHERE run_id=?1 AND seq=?2",
                    params![owner.run_id.0, first.0],
                )
                .unwrap();
            assert!(
                store.events_after(&owner.run_id, EventSeq(0), 100).is_err(),
                "损坏标记不能降级为无版本历史"
            );
        }
        std::fs::remove_file(file).unwrap();
    }
    #[test]
    fn pending_and_resolved_interaction_revisions_come_from_the_publishing_transaction() {
        let (store, owner) = fixture(std::path::Path::new(":memory:"));
        let seq = store
            .append_event(
                &owner.run_id,
                "approval_required",
                &json!({"approval":{"id":"i","prompt":"确认"}}),
            )
            .unwrap();
        let stamp = store.event_view_stamp(&owner.run_id, seq).unwrap().unwrap();
        assert_eq!(
            stamp.interaction,
            Some(ViewInteraction {
                interaction_id: InteractionId("i".into()),
                revision: 0,
                pending: true
            })
        );
        let record = store
            .claim_interaction(
                &InteractionId("i".into()),
                &owner.session_key,
                &owner.run_id,
                0,
                true,
            )
            .unwrap();
        let resolved = store
            .interaction_view_stamp(&record.interaction_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            resolved.interaction,
            Some(ViewInteraction {
                interaction_id: record.interaction_id.clone(),
                revision: 1,
                pending: false
            })
        );
        assert_eq!(resolved.snapshot_revision.0, clock(&store));
        assert_eq!(resolved.previous_visible_seq, Some(seq));
    }
    #[test]
    fn schema18_stamp_upgrade_keeps_unversioned_history_and_rolls_back_on_fault() {
        let file = path();
        let (store, owner) = fixture(&file);
        store.lock_connection().unwrap().execute_batch("DROP TABLE provider_requests; DROP TABLE event_view_stamps; DELETE FROM schema_migrations WHERE version>=19; CREATE TRIGGER stamp_migration_fault BEFORE INSERT ON schema_migrations WHEN NEW.version=19 BEGIN SELECT RAISE(ABORT,'stamp migration fault'); END").unwrap();
        drop(store);
        assert!(RunStore::open(&file).is_err());
        let db = Connection::open(&file).unwrap();
        assert_eq!(
            db.query_row("SELECT max(version) FROM schema_migrations", [], |row| row
                .get::<_, u64>(
                0
            ))
            .unwrap(),
            18
        );
        assert_eq!(
            db.query_row(
                "SELECT count(*) FROM sqlite_master WHERE name='event_view_stamps'",
                [],
                |row| row.get::<_, u64>(0)
            )
            .unwrap(),
            0
        );
        assert!(file.with_extension("v18.backup.sqlite3").exists());
        assert!(file.with_extension("v18.backup.verified.json").exists());
        db.execute_batch("DROP TRIGGER stamp_migration_fault")
            .unwrap();
        drop(db);
        let store = RunStore::open(&file).unwrap();
        assert_eq!(store.run_owner(&owner.run_id).unwrap(), owner);
        assert!(
            store
                .events_after(&owner.run_id, EventSeq(0), 100)
                .unwrap()
                .iter()
                .all(|event| event.data.get("_my_agent_view").is_none()),
            "迁移不能给旧事件贴当前版本"
        );
        store
            .append_event(&owner.run_id, "turn_started", &json!({}))
            .unwrap();
        drop(store);
        let store = RunStore::open(&file).unwrap();
        assert!(
            store
                .events_after(&owner.run_id, EventSeq(0), 100)
                .unwrap()
                .last()
                .unwrap()
                .data
                .get("_my_agent_view")
                .is_some()
        );
        store
            .lock_connection()
            .unwrap()
            .execute("INSERT INTO schema_migrations VALUES(21,0)", [])
            .unwrap();
        drop(store);
        assert!(RunStore::open(&file).is_err());
        std::fs::remove_file(file).unwrap();
    }
}
