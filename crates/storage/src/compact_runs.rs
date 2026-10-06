//! 独立 compact run 与已有准入/intent/terminal 的同事务关联。
use super::{RunStore, RuntimeError, read_run_in, sessions};
use agent_core::*;
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};

pub trait CompactRunRepository: Send + Sync {
    fn admit_compact_run(
        &self,
        request: &CompactRunRequest,
        snapshot: &RunSnapshot,
    ) -> Result<Admission, RuntimeError>;
    fn compact_run(&self, run: &RunId) -> Result<Option<CompactRunReadback>, RuntimeError>;
    fn compact_snapshot(
        &self,
        run: &RunId,
    ) -> Result<(RunRecord, CompactRunReadback), RuntimeError>;
    fn compact_request(
        &self,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        operation: &str,
    ) -> Result<Option<CompactRunRequest>, RuntimeError>;
    fn compact_operation(
        &self,
        request: &CompactRunRequest,
    ) -> Result<Option<RunRecord>, RuntimeError>;
}

fn protocol(error: serde_json::Error) -> RuntimeError {
    RuntimeError::Protocol(error.to_string())
}

impl CompactRunRepository for RunStore {
    fn admit_compact_run(
        &self,
        request: &CompactRunRequest,
        snapshot: &RunSnapshot,
    ) -> Result<Admission, RuntimeError> {
        let admission = RunAdmission {
            plan_execution: None,
            session_key: request.session_key.clone(),
            expected_lifetime: Some(request.session_lifetime_id.clone()),
            request_id: RequestId::String(format!("compact:{}", request.operation_id)),
            input: serde_json::to_string(request).map_err(protocol)?,
            mode: AdmissionMode::RejectIfBusy,
        };
        self.admit_run_inner(
            &admission,
            snapshot.route.as_ref(),
            Some(snapshot),
            None,
            Some(request),
        )
    }
    fn compact_operation(
        &self,
        request: &CompactRunRequest,
    ) -> Result<Option<RunRecord>, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        let result = existing_in(&tx, request)?;
        super::views::commit(tx)?;
        Ok(result)
    }
    fn compact_request(
        &self,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        operation: &str,
    ) -> Result<Option<CompactRunRequest>, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        let raw: Option<String> = tx
            .query_row(
                "SELECT request_json FROM compact_run_links WHERE operation_id=?1",
                params![operation],
                |row| row.get(0),
            )
            .optional()?;
        let request = raw
            .map(|raw| serde_json::from_str::<CompactRunRequest>(&raw).map_err(protocol))
            .transpose()?;
        if let Some(request) = &request {
            if request.session_key != *key
                || request.session_lifetime_id != *lifetime
                || request.operation_id != operation
            {
                return Err(RuntimeError::Protocol("compact operation 身份冲突".into()));
            }
            existing_in(&tx, request)?
                .ok_or_else(|| RuntimeError::Protocol("compact operation 缺 run".into()))?;
        }
        super::views::commit(tx)?;
        Ok(request)
    }
    fn compact_snapshot(
        &self,
        run: &RunId,
    ) -> Result<(RunRecord, CompactRunReadback), RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        let record = read_run_in(&tx, &run.0)?
            .ok_or_else(|| RuntimeError::Protocol("compact run 缺失".into()))?;
        let receipt = readback_in(&tx, run)?
            .ok_or_else(|| RuntimeError::Protocol("compact 缺 canonical linkage".into()))?;
        super::views::commit(tx)?;
        Ok((record, receipt))
    }
    fn compact_run(&self, run: &RunId) -> Result<Option<CompactRunReadback>, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        let result = readback_in(&tx, run)?;
        super::views::commit(tx)?;
        Ok(result)
    }
}

pub(crate) fn existing_in(
    db: &Connection,
    request: &CompactRunRequest,
) -> Result<Option<RunRecord>, RuntimeError> {
    let meta = sessions::metadata_in(db, &request.session_key)?
        .filter(|meta| !meta.deleted && meta.lifetime == request.session_lifetime_id)
        .ok_or_else(|| RuntimeError::Protocol("compact lifetime 已变化".into()))?;
    let row: Option<(String, String)> = db
        .query_row(
            "SELECT run_id,request_json FROM compact_run_links WHERE operation_id=?1",
            params![request.operation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((run, raw)) = row {
        let stored: CompactRunRequest = serde_json::from_str(&raw).map_err(protocol)?;
        if stored != *request || stored.session_lifetime_id != meta.lifetime {
            return Err(RuntimeError::Protocol(
                "compact operation 身份或 source 冲突".into(),
            ));
        }
        let run = read_run_in(db, &run)?
            .ok_or_else(|| RuntimeError::Protocol("compact run 缺失".into()))?;
        readback_in(db, &run.run_id)?
            .ok_or_else(|| RuntimeError::Protocol("compact linkage 损坏".into()))?;
        return Ok(Some(run));
    }
    Ok(None)
}

pub(crate) fn source_check_in(
    db: &Connection,
    request: &CompactRunRequest,
    snapshot: Option<&RunSnapshot>,
) -> Result<(), RuntimeError> {
    if request.operation_id.is_empty() || request.operation_id.len() > 256 {
        return Err(RuntimeError::Protocol("compact operation 无效".into()));
    }
    let source = sessions::snapshot_in(db, &request.session_key)?;
    if source.lifetime != request.session_lifetime_id
        || source.revision != request.expected_revision
        || source.projection_generation != request.expected_projection_generation
    {
        return Err(RuntimeError::Protocol("compact source CAS 已变化".into()));
    }
    let snapshot =
        snapshot.ok_or_else(|| RuntimeError::Protocol("compact 缺 frozen snapshot".into()))?;
    if snapshot.context_read_only || snapshot.context_policy_fingerprint.is_none() {
        return Err(RuntimeError::Protocol(
            "compact 缺 writable frozen policy".into(),
        ));
    }
    if let Some(run) = &request.compatibility_owner_run_id {
        let owner = sessions::owner_in(db, run)?;
        sessions::fence_in(db, &owner)?;
        if owner.session_key != request.session_key
            || owner.session_lifetime_id != request.session_lifetime_id
        {
            return Err(RuntimeError::Protocol(
                "旧 compact 来源 owner 不匹配".into(),
            ));
        }
        let raw: Option<String> = db
            .query_row(
                "SELECT snapshot_json FROM run_snapshots WHERE run_id=?1",
                params![run.0],
                |row| row.get(0),
            )
            .optional()?;
        let frozen = raw
            .map(|raw| serde_json::from_str::<RunSnapshot>(&raw).map_err(protocol))
            .transpose()?;
        if frozen
            .as_ref()
            .is_some_and(|snapshot| snapshot.context_read_only)
        {
            return Err(RuntimeError::Protocol(
                "旧 compact 来源 owner 为只读上下文".into(),
            ));
        }
    }
    let legacy: i64 = db.query_row(
        "SELECT count(*) FROM compact_operations WHERE operation=?1",
        params![request.operation_id],
        |row| row.get(0),
    )?;
    if legacy != 0 {
        return Err(RuntimeError::Protocol(
            "旧 compact receipt 不能补 native 执行身份".into(),
        ));
    }
    Ok(())
}

pub(crate) fn start_in(
    db: &Connection,
    request: &CompactRunRequest,
    run: &RunId,
    snapshot: Option<&RunSnapshot>,
) -> Result<(), RuntimeError> {
    let snapshot =
        snapshot.ok_or_else(|| RuntimeError::Protocol("compact snapshot 缺失".into()))?;
    let canonical = sessions::snapshot_in(db, &request.session_key)?;
    let route = snapshot
        .route
        .as_ref()
        .and_then(|route| route.candidates.first())
        .map_or("default", |candidate| candidate.model.as_str());
    let source = ContextSource {
        lifetime: canonical.lifetime,
        source_start: TranscriptSeq(0),
        source_end: canonical.revision,
        prefix_digest: format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&canonical.messages).map_err(protocol)?)
        ),
        generation: canonical.projection_generation,
        policy_fingerprint: snapshot
            .context_policy_fingerprint
            .clone()
            .ok_or_else(|| RuntimeError::Protocol("compact policy 缺失".into()))?,
        pressure_route: route.into(),
        summary_route: route.into(),
    };
    let raw = serde_json::to_string(&source).map_err(protocol)?;
    db.execute(
        "INSERT INTO compact_run_links VALUES(?1,?2,?3,?4)",
        params![
            run.0,
            request.operation_id,
            serde_json::to_string(request).map_err(protocol)?,
            raw
        ],
    )?;
    db.execute("INSERT INTO compact_operations(operation,run_id,lifetime,source_json,state,reason) VALUES(?1,?2,?3,?4,'pending',NULL)",params![request.operation_id,run.0,request.session_lifetime_id.0,raw])?;
    db.execute(
        "UPDATE runs SET status='running' WHERE id=?1",
        params![run.0],
    )?;
    db.execute(
        "UPDATE turns SET status='running' WHERE run_id=?1",
        params![run.0],
    )?;
    super::insert_event(
        db,
        run,
        "run_started",
        &serde_json::json!({"kind":"compact","operation_id":request.operation_id,"source_revision":source.source_end,"projection_generation":source.generation}),
    )?;
    Ok(())
}

pub(crate) fn readback_in(
    db: &Connection,
    run: &RunId,
) -> Result<Option<CompactRunReadback>, RuntimeError> {
    struct Row {
        operation: String,
        source: String,
        linked: String,
        state: String,
        reason: Option<String>,
        generation: Option<u64>,
        request: String,
        receipt_source: String,
        lifetime: String,
    }
    let row = db.query_row("SELECT l.operation_id,l.source_json,c.run_id,c.state,c.reason,c.result_generation,l.request_json,c.source_json,c.lifetime FROM compact_run_links l LEFT JOIN compact_operations c ON c.operation=l.operation_id WHERE l.run_id=?1",params![run.0],|row|Ok(Row{operation:row.get(0)?,source:row.get(1)?,linked:row.get(2)?,state:row.get(3)?,reason:row.get(4)?,generation:row.get(5)?,request:row.get(6)?,receipt_source:row.get(7)?,lifetime:row.get(8)?})).optional()?;
    let Some(Row {
        operation,
        source: raw,
        linked,
        state,
        reason,
        generation,
        request,
        receipt_source,
        lifetime,
    }) = row
    else {
        return Ok(None);
    };
    let owner = sessions::owner_in(db, run)?;
    let source: ContextSource = serde_json::from_str(&raw).map_err(protocol)?;
    let request: CompactRunRequest = serde_json::from_str(&request).map_err(protocol)?;
    if linked != run.0
        || source.lifetime != owner.session_lifetime_id
        || lifetime != owner.session_lifetime_id.0
        || raw != receipt_source
        || request.session_key != owner.session_key
        || request.session_lifetime_id != source.lifetime
        || request.operation_id != operation
        || request.expected_revision != source.source_end
        || request.expected_projection_generation != source.generation
        || source.source_start != TranscriptSeq(0)
        || source.policy_fingerprint.is_empty()
        || source.prefix_digest.len() != 64
        || generation.is_some_and(|generation| {
            generation
                != if state == "committed" {
                    source.generation.0.saturating_add(1)
                } else {
                    source.generation.0
                }
        })
    {
        return Err(RuntimeError::Protocol("compact readback 身份损坏".into()));
    }
    let record = read_run_in(db, &run.0)?
        .ok_or_else(|| RuntimeError::Protocol("compact readback 缺 run".into()))?;
    let outcome = match record.status {
        RunStatus::Running if state == "pending" => CompactRunOutcome::Started,
        RunStatus::Completed if state == "committed" && generation.is_some() => {
            CompactRunOutcome::Committed
        }
        RunStatus::Completed if state == "rejected" && reason.as_deref() == Some("no_gain") => {
            CompactRunOutcome::NoGain
        }
        RunStatus::Failed if state == "rejected" && reason.as_deref() == Some("rejected") => {
            CompactRunOutcome::Rejected
        }
        RunStatus::Failed if state == "rejected" => CompactRunOutcome::Failed,
        RunStatus::Cancelled if state == "rejected" => CompactRunOutcome::Cancelled,
        RunStatus::UnknownAfterRestart => CompactRunOutcome::Unknown,
        _ => {
            return Err(RuntimeError::Protocol(
                "compact receipt 与 native terminal 冲突".into(),
            ));
        }
    };
    Ok(Some(CompactRunReadback {
        schema_version: 1,
        owner,
        operation_id: operation,
        source,
        outcome,
        result_generation: generation.map(ProjectionGeneration),
    }))
}

pub(crate) fn terminal_in(
    db: &Connection,
    run: &RunId,
    state: &str,
    reason: &str,
    generation: Option<ProjectionGeneration>,
) -> Result<(), RuntimeError> {
    let native: i64 = db.query_row(
        "SELECT count(*) FROM compact_run_links WHERE run_id=?1",
        params![run.0],
        |row| row.get(0),
    )?;
    if native == 0 {
        return Ok(());
    }
    let (status, outcome, error) = if state == "committed" {
        (RunStatus::Completed, CompactRunOutcome::Committed, None)
    } else {
        match reason {
            "no_gain" => (RunStatus::Completed, CompactRunOutcome::NoGain, None),
            "cancelled" => (
                RunStatus::Cancelled,
                CompactRunOutcome::Cancelled,
                Some((-32800, "compact 已取消")),
            ),
            "rejected" => (
                RunStatus::Failed,
                CompactRunOutcome::Rejected,
                Some((-32603, "compact 已拒绝")),
            ),
            _ => (
                RunStatus::Failed,
                CompactRunOutcome::Failed,
                Some((-32603, "compact 摘要或 source 失败")),
            ),
        }
    };
    super::turns::commit_output_in(db, run, status, None)?;
    super::insert_event(
        db,
        run,
        "compact_terminal",
        &serde_json::json!({"outcome":outcome,"projection_generation":generation}),
    )?;
    super::insert_event(
        db,
        run,
        "terminal",
        &serde_json::json!({"status":status,"error_code":error.map(|error|error.0),"error_message":error.map(|error|error.1)}),
    )?;
    db.execute(
        "UPDATE runs SET status=?2,error_code=?3,error_message=?4 WHERE id=?1",
        params![
            run.0,
            status.as_str(),
            error.map(|error| error.0),
            error.map(|error| error.1)
        ],
    )?;
    db.execute(
        "UPDATE turns SET status=?2 WHERE run_id=?1",
        params![run.0, status.as_str()],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ContextRepository, MemoryRepository, SessionLifecycle, SessionQuery, TurnRepository,
    };
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    fn path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "native-compact-{}-{}.sqlite3",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }
    fn fixture(path: &std::path::Path) -> (RunStore, CompactRunRequest, RunSnapshot) {
        let store = RunStore::open(path).unwrap();
        let meta = store
            .create_session(&SessionKey("native-compact".into()))
            .unwrap();
        for (id, input) in [(1, "旧约束".repeat(400)), (2, "当前约束".into())] {
            let Admission::New(run) = store
                .admit_with_lifetime(
                    meta.key.clone(),
                    Some(&meta.lifetime),
                    RequestId::Number(id),
                    &input,
                    AdmissionMode::Queue,
                    None,
                )
                .unwrap()
            else {
                panic!("new chat")
            };
            store.try_start_queued(&run.run_id).unwrap();
            store
                .commit_turn(&TurnCommit {
                    owner: store.run_owner(&run.run_id).unwrap(),
                    status: RunStatus::Completed,
                    content: Some("完成".into()),
                    error: None,
                })
                .unwrap();
        }
        let source = store.session_snapshot(&meta.key).unwrap();
        let request = CompactRunRequest {
            session_key: meta.key,
            session_lifetime_id: meta.lifetime,
            operation_id: "native-operation".into(),
            expected_revision: source.revision,
            expected_projection_generation: source.projection_generation,
            compatibility_owner_run_id: None,
        };
        let snapshot: RunSnapshot=serde_json::from_value(serde_json::json!({"entry_channel":"cli","route":null,"tools":[],"cwd":".","permission_mode":"request_approval","sandbox_requested":"native","sandbox_effective":"native","sandbox_notice":null,"docker_image":null,"delegation_context":null,"context_read_only":false,"context_token_budget":4096,"context_policy_fingerprint":"test","tool_catalog_digest":"empty","memory_entry_budget":0,"memory_token_budget":0,"max_tool_calls":0,"config_generation":0})).unwrap();
        (store, request, snapshot)
    }
    fn admitted(
        store: &RunStore,
        request: &CompactRunRequest,
        snapshot: &RunSnapshot,
    ) -> (RunRecord, CompactIntent) {
        let Admission::New(run) = store.admit_compact_run(request, snapshot).unwrap() else {
            panic!("new compact")
        };
        let fact = store.compact_run(&run.run_id).unwrap().unwrap();
        (
            run,
            CompactIntent {
                operation: fact.operation_id,
                owner: fact.owner,
                source: fact.source,
            },
        )
    }
    #[test]
    fn native_admission_source_cas_idempotency_busy_and_terminal_are_atomic_on_both_backends() {
        for path in [std::path::PathBuf::from(":memory:"), path()] {
            let (store, request, snapshot) = fixture(&path);
            assert_eq!(store.recover_memory_ingests(32).unwrap(), 2);
            let before = store.session_snapshot(&request.session_key).unwrap();
            let mut stale = request.clone();
            stale.expected_revision = TranscriptSeq(0);
            assert!(store.admit_compact_run(&stale, &snapshot).is_err());
            stale = request.clone();
            stale.expected_projection_generation = ProjectionGeneration(99);
            assert!(store.admit_compact_run(&stale, &snapshot).is_err());
            let (run, intent) = admitted(&store, &request, &snapshot);
            assert_eq!(run.kind, RunKind::Compact);
            assert_eq!(run.status, RunStatus::Running);
            let events = store.events_after(&run.run_id, EventSeq(0), 100).unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].event, "run_started");
            assert_eq!(
                store
                    .session_snapshot(&request.session_key)
                    .unwrap()
                    .messages,
                before.messages
            );
            assert!(
                matches!(store.admit_compact_run(&request,&snapshot).unwrap(),Admission::Existing(existing) if existing.run_id==run.run_id)
            );
            stale = request.clone();
            stale.operation_id = "other".into();
            assert!(store.admit_compact_run(&stale, &snapshot).is_err());
            stale = request.clone();
            stale.expected_revision = TranscriptSeq(1);
            assert!(store.compact_operation(&stale).is_err());
            let collision = RunAdmission {
                plan_execution: None,
                session_key: request.session_key.clone(),
                expected_lifetime: Some(request.session_lifetime_id.clone()),
                request_id: run.request_id.clone(),
                input: serde_json::to_string(&request).unwrap(),
                mode: AdmissionMode::Queue,
            };
            assert!(store.admit_run(&collision, &snapshot).is_err());
            assert!(
                store
                    .commit_turn(&TurnCommit {
                        owner: intent.owner.clone(),
                        status: RunStatus::Completed,
                        content: Some("伪造聊天".into()),
                        error: None
                    })
                    .is_err()
            );
            store.begin_compact(&intent).unwrap();
            store.lock_connection().unwrap().execute_batch("CREATE TRIGGER compact_fault BEFORE INSERT ON events WHEN NEW.event='terminal' BEGIN SELECT RAISE(ABORT,'compact terminal fault'); END").unwrap();
            let candidate = vec![
                Message::text(Role::System, "摘要"),
                Message::text(Role::User, "当前约束"),
                Message::text(Role::Assistant, "完成"),
            ];
            assert!(
                store
                    .settle_compact(&intent, Some(&candidate), "summary")
                    .is_err()
            );
            assert_eq!(
                store.compact_run(&run.run_id).unwrap().unwrap().outcome,
                CompactRunOutcome::Started
            );
            assert_eq!(
                store
                    .session_snapshot(&request.session_key)
                    .unwrap()
                    .projection_generation,
                ProjectionGeneration(0)
            );
            assert_eq!(
                store
                    .events_after(&run.run_id, EventSeq(0), 100)
                    .unwrap()
                    .len(),
                1
            );
            store
                .lock_connection()
                .unwrap()
                .execute_batch("DROP TRIGGER compact_fault")
                .unwrap();
            store
                .settle_compact(&intent, Some(&candidate), "summary")
                .unwrap();
            let (record, fact) = store.compact_snapshot(&run.run_id).unwrap();
            assert_eq!(record.status, RunStatus::Completed);
            assert_eq!(fact.outcome, CompactRunOutcome::Committed);
            assert_eq!(fact.result_generation, Some(ProjectionGeneration(1)));
            assert!(store.ingest_committed_turn(&intent.owner).is_err());
            assert_eq!(store.recover_memory_ingests(32).unwrap(), 0);
            assert_eq!(store.next_memory_ingest_due().unwrap(), None);
            assert_eq!(
                store.flywheel_report(&request.session_lifetime_id).unwrap()["pending_ingests"],
                0
            );
            assert_eq!(
                store
                    .session_snapshot(&request.session_key)
                    .unwrap()
                    .messages,
                before.messages
            );
            assert!(
                matches!(store.admit_compact_run(&request,&snapshot).unwrap(),Admission::Existing(existing) if existing.run_id==run.run_id)
            );
            assert!(
                store
                    .settle_compact(&intent, Some(&candidate), "summary")
                    .is_err()
            );
            drop(store);
            if path.to_str() != Some(":memory:") {
                std::fs::remove_file(path).unwrap();
            }
        }
    }
    #[test]
    fn native_no_gain_cancel_failure_recovery_and_corruption_fail_closed() {
        for path in [std::path::PathBuf::from(":memory:"), path()] {
            let (store, mut request, snapshot) = fixture(&path);
            for (reason, outcome, status) in [
                ("no_gain", CompactRunOutcome::NoGain, RunStatus::Completed),
                (
                    "cancelled",
                    CompactRunOutcome::Cancelled,
                    RunStatus::Cancelled,
                ),
                ("rejected", CompactRunOutcome::Rejected, RunStatus::Failed),
                (
                    "summary_failed",
                    CompactRunOutcome::Failed,
                    RunStatus::Failed,
                ),
            ] {
                request.operation_id = reason.into();
                let (run, intent) = admitted(&store, &request, &snapshot);
                store.settle_compact(&intent, None, reason).unwrap();
                let (record, fact) = store.compact_snapshot(&run.run_id).unwrap();
                assert_eq!(fact.outcome, outcome);
                assert_eq!(record.status, status);
                assert_eq!(
                    store
                        .session_snapshot(&request.session_key)
                        .unwrap()
                        .revision,
                    request.expected_revision
                );
            }
            request.operation_id = "unknown".into();
            let (run, _) = admitted(&store, &request, &snapshot);
            assert_eq!(store.recover().unwrap(), 1);
            assert_eq!(store.recover().unwrap(), 0);
            let (record, fact) = store.compact_snapshot(&run.run_id).unwrap();
            assert_eq!(record.status, RunStatus::UnknownAfterRestart);
            assert_eq!(fact.outcome, CompactRunOutcome::Unknown);
            assert!(
                store
                    .reconcile_unknown(
                        &run.run_id,
                        &request.session_key,
                        record.last_seq,
                        RunStatus::Completed,
                        Some("伪摘要"),
                        "人工证据长度符合原聊天接口要求"
                    )
                    .is_err()
            );
            assert!(
                matches!(store.admit_compact_run(&request,&snapshot).unwrap(),Admission::Existing(existing) if existing.run_id==run.run_id)
            );
            let events = store.events_after(&run.run_id, EventSeq(0), 100).unwrap();
            assert_eq!(
                events
                    .iter()
                    .filter(|event| event.event == "compact_terminal")
                    .count(),
                1
            );
            store
                .lock_connection()
                .unwrap()
                .execute(
                    "UPDATE compact_operations SET source_json='{}' WHERE operation=?1",
                    params![request.operation_id],
                )
                .unwrap();
            assert!(store.compact_snapshot(&run.run_id).is_err());
            assert!(store.compact_operation(&request).is_err());
            drop(store);
            if path.to_str() != Some(":memory:") {
                std::fs::remove_file(path).unwrap();
            }
        }
    }
    #[test]
    fn schema16_compact_upgrade_has_verified_backup_rollback_and_future_rejection() {
        let path = path();
        let (store, request, _) = fixture(&path);
        store.lock_connection().unwrap().execute_batch("DROP TABLE provider_requests; DROP TABLE event_view_stamps; DROP TABLE tool_discoveries; DROP TABLE compact_run_links; DELETE FROM schema_migrations WHERE version>=17; CREATE TRIGGER migration_fault BEFORE INSERT ON schema_migrations WHEN NEW.version=17 BEGIN SELECT RAISE(ABORT,'compact migration fault'); END").unwrap();
        drop(store);
        assert!(RunStore::open(&path).is_err());
        let db = Connection::open(&path).unwrap();
        assert_eq!(
            db.query_row("SELECT max(version) FROM schema_migrations", [], |row| row
                .get::<_, u64>(
                0
            ))
            .unwrap(),
            16
        );
        assert_eq!(
            db.query_row(
                "SELECT count(*) FROM sqlite_master WHERE name='compact_run_links'",
                [],
                |row| row.get::<_, u64>(0)
            )
            .unwrap(),
            0
        );
        assert!(path.with_extension("v16.backup.sqlite3").exists());
        assert!(path.with_extension("v16.backup.verified.json").exists());
        db.execute_batch("DROP TRIGGER migration_fault").unwrap();
        drop(db);
        let store = RunStore::open(&path).unwrap();
        assert_eq!(
            store
                .session_metadata(&request.session_key)
                .unwrap()
                .unwrap()
                .lifetime,
            request.session_lifetime_id
        );
        store
            .lock_connection()
            .unwrap()
            .execute("INSERT INTO schema_migrations VALUES(21,0)", [])
            .unwrap();
        drop(store);
        assert!(RunStore::open(&path).is_err());
        std::fs::remove_file(path).unwrap();
    }
}
