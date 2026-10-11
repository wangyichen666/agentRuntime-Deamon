//! compact 原调用链的隔离与迟到写入复现。
use agent_core::*;
use agent_storage::{
    CompactRunRepository, ContextRepository, RunStore, SessionLifecycle, SessionQuery,
    TurnRepository,
};
use rusqlite::{Connection, params};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    root: std::path::PathBuf,
    store: RunStore,
    db: Connection,
    request: CompactRunRequest,
    snapshot: RunSnapshot,
}
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "ma-compact-port-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let path = root.join("runtime.sqlite3");
        let store = RunStore::open(&path).unwrap();
        let key = SessionKey("compact-port".into());
        let meta = store.create_session(&key).unwrap();
        let Admission::New(run) = store
            .admit_with_lifetime(
                key.clone(),
                Some(&meta.lifetime),
                RequestId::Number(1),
                "当前约束",
                AdmissionMode::Queue,
                None,
            )
            .unwrap()
        else {
            panic!("new")
        };
        store.try_start_queued(&run.run_id).unwrap();
        store
            .commit_turn(&TurnCommit {
                owner: store.run_owner(&run.run_id).unwrap(),
                status: RunStatus::Completed,
                content: Some("历史回答".into()),
                error: None,
            })
            .unwrap();
        let source = store.session_snapshot(&key).unwrap();
        let request = CompactRunRequest {
            session_key: key,
            session_lifetime_id: meta.lifetime,
            operation_id: "compact-operation".into(),
            expected_revision: source.revision,
            expected_projection_generation: source.projection_generation,
            compatibility_owner_run_id: None,
        };
        let snapshot:RunSnapshot=serde_json::from_value(serde_json::json!({"entry_channel":"daemon_rpc","tools":[],"cwd":".","permission_mode":"risk_approval","sandbox_requested":"native","sandbox_effective":"native","context_read_only":false,"context_token_budget":4096,"context_policy_fingerprint":"test","tool_catalog_digest":format!("{:x}",Sha256::digest(b"[]")),"config_generation":0})).unwrap();
        Self {
            root,
            store,
            db: Connection::open(path).unwrap(),
            request,
            snapshot,
        }
    }
    fn admitted(&self) -> (RunRecord, CompactIntent) {
        let Admission::New(run) = self
            .store
            .admit_compact_run(&self.request, &self.snapshot)
            .unwrap()
        else {
            panic!("new compact")
        };
        let fact = self.store.compact_run(&run.run_id).unwrap().unwrap();
        (
            run,
            CompactIntent {
                operation: fact.operation_id,
                owner: fact.owner,
                source: fact.source,
            },
        )
    }
    fn clock(&self) -> u64 {
        self.db
            .query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap()
    }
    fn foreign(&self) {
        self.db.execute_batch("INSERT INTO organizations VALUES('foreign');INSERT INTO tenants VALUES('foreign','foreign');INSERT INTO projects VALUES('foreign','foreign');INSERT INTO agents VALUES('foreign','foreign','foreign');UPDATE session_scopes SET tenant_id='foreign',project_id='foreign',agent_uid='foreign' WHERE session_id='compact-port'").unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
#[test]
fn compact_admission_denies_foreign_session_without_run_or_event() {
    let f = Fixture::new();
    f.foreign();
    let before = f.clock();
    let result = f.store.admit_compact_run(&f.request, &f.snapshot);
    assert!(
        matches!(
            result,
            Err(agent_storage::RuntimeError::Authorization(
                AuthorizationError::Forbidden
            ))
        ),
        "{result:?}"
    );
    assert_eq!(f.clock(), before);
}
#[test]
fn compact_fact_denies_foreign_session_before_payload_read() {
    let f = Fixture::new();
    let (run, _) = f.admitted();
    f.foreign();
    let result = f.store.compact_snapshot(&run.run_id);
    assert!(
        matches!(
            result,
            Err(agent_storage::RuntimeError::Authorization(
                AuthorizationError::Forbidden
            ))
        ),
        "{result:?}"
    );
}
#[test]
fn compact_begin_denies_foreign_session_before_idempotent_success() {
    let f = Fixture::new();
    let (_, intent) = f.admitted();
    f.foreign();
    let before = f.clock();
    let result = f.store.begin_compact(&intent);
    assert!(
        matches!(
            result,
            Err(agent_storage::RuntimeError::Authorization(
                AuthorizationError::Forbidden
            ))
        ),
        "{result:?}"
    );
    assert_eq!(f.clock(), before);
}
#[test]
fn compact_settle_denies_foreign_session_without_terminal() {
    let f = Fixture::new();
    let (run, intent) = f.admitted();
    f.foreign();
    let before = f.clock();
    let result = f.store.settle_compact(&intent, None, "no_gain");
    assert!(
        matches!(
            result,
            Err(agent_storage::RuntimeError::Authorization(
                AuthorizationError::Forbidden
            ))
        ),
        "{result:?}"
    );
    assert_eq!(f.clock(), before);
    assert_eq!(
        f.store.read_run(&run.run_id).unwrap().unwrap().status,
        RunStatus::Running
    );
}
#[test]
fn late_compact_owner_cannot_reject_or_finalize_new_generation() {
    let f = Fixture::new();
    let (run, intent) = f.admitted();
    f.db.execute(
        "UPDATE runs SET generation=generation+1 WHERE id=?1",
        [&run.run_id.0],
    )
    .unwrap();
    let before = f.clock();
    assert!(
        f.store
            .settle_compact(&intent, None, "summary_failed")
            .is_err()
    );
    assert_eq!(f.clock(), before);
    assert_eq!(
        f.store.read_run(&run.run_id).unwrap().unwrap().status,
        RunStatus::Running
    );
    assert_eq!(
        f.db.query_row::<String, _, _>(
            "SELECT state FROM compact_operations WHERE operation=?1",
            [&intent.operation],
            |r| r.get(0)
        )
        .unwrap(),
        "pending"
    );
}
#[test]
fn complete_legal_linkage_moved_to_chat_run_is_corrupt() {
    let f = Fixture::new();
    let (_, intent) = f.admitted();
    f.store.settle_compact(&intent, None, "no_gain").unwrap();
    let chat: String =
        f.db.query_row(
            "SELECT id FROM runs WHERE id NOT IN (SELECT run_id FROM compact_run_links) LIMIT 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    f.db.execute(
        "UPDATE compact_run_links SET run_id=?1 WHERE operation_id=?2",
        params![chat, intent.operation],
    )
    .unwrap();
    f.db.execute(
        "UPDATE compact_operations SET run_id=?1 WHERE operation=?2",
        params![chat, intent.operation],
    )
    .unwrap();
    let result = f.store.compact_snapshot(&RunId(chat));
    assert!(
        matches!(
            result,
            Err(agent_storage::RuntimeError::Coordination(
                CoordinationError::Corrupt
            ))
        ),
        "{result:?}"
    );
}

#[tokio::test]
async fn canonical_readback_rejects_many_valid_empty_batches_before_allocation() {
    use agent_storage_ports::SessionReadRepository;
    let f = Fixture::new();
    let digest = format!("{:x}", Sha256::digest(b"[]"));
    f.db.execute("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<32769) INSERT INTO transcript_batches(lifetime,operation_id,start_seq,end_seq,messages_json,digest) SELECT ?1,'empty-'||i,?2,?2,'[]',?3 FROM n",params![f.request.session_lifetime_id.0,f.request.expected_revision.0,digest]).unwrap();
    let before = f.clock();
    assert_eq!(
        f.store
            .read_session(
                &TenantContext::local(),
                &f.request.session_key,
                HistoryReadMode::Canonical
            )
            .await
            .unwrap_err(),
        CoordinationError::InvalidRequest
    );
    assert_eq!(f.clock(), before);
}

impl Fixture {
    async fn native(&mut self) -> (EnterpriseOwnerEnvelope, CompactIntent) {
        use agent_storage_ports::{IdentityReadRepository, RunContextRepository};
        self.store = self.store.clone().enable_session_writer().unwrap();
        let identity = self
            .store
            .read_identity(
                &TenantContext::local(),
                &"local-subject".to_owned().try_into().unwrap(),
            )
            .await
            .unwrap();
        let auth = AuthorizationRequest {
            context: RequestContext {
                scope: identity.scope.clone(),
                principal: identity.principal,
                request_id: RequestId::String(format!("compact:{}", self.request.operation_id)),
                trace: TraceContext {
                    trace_id: "compact-contract".into(),
                },
            },
            expected_policy: identity.policy.version,
            action: Action::SessionPrompt,
            attributes: AuthorizationAttributes {
                workspace: self.snapshot.cwd.clone(),
                entry_channel: HookChannel::DaemonRpc,
                tool_effect: None,
                timestamp_ms: 0,
                network: NetworkBoundary::None,
            },
        };
        let (Admission::New(run), owner) = self
            .store
            .admit_compaction(
                &self.request,
                &self.snapshot,
                &auth,
                std::sync::Arc::new(agent_policy::PolicyEngine),
            )
            .await
            .unwrap()
        else {
            panic!("new")
        };
        let env = EnterpriseOwnerEnvelope {
            scope: identity.scope,
            owner,
        };
        let read = self
            .store
            .read_compact_fact(&env.scope, &run.run_id)
            .await
            .unwrap()
            .unwrap();
        (
            env,
            CompactIntent {
                operation: read.receipt.operation_id,
                owner: read.receipt.owner,
                source: read.receipt.source,
            },
        )
    }
}

#[tokio::test]
async fn native_compact_all_scope_and_owner_dimensions_are_fenced_before_reads_or_writes() {
    use agent_storage_ports::RunContextRepository;
    let mut f = Fixture::new();
    let (env, intent) = f.native().await;
    for dimension in 0..9 {
        let mut bad = env.clone();
        match dimension {
            0 => bad.scope.organization_id = "foreign".to_owned().try_into().unwrap(),
            1 => bad.scope.tenant_id = "foreign".to_owned().try_into().unwrap(),
            2 => bad.scope.project_id = "foreign".to_owned().try_into().unwrap(),
            3 => bad.scope.agent_uid = "foreign".to_owned().try_into().unwrap(),
            4 => bad.owner.session_key.0.push('x'),
            5 => bad.owner.session_lifetime_id.0.push('x'),
            6 => bad.owner.run_id = RunId("run-1".into()),
            7 => bad.owner.run_generation.0 += 1,
            _ => bad.owner.turn_id.0.push('x'),
        }
        let before = f.clock();
        let expected = if dimension < 4 {
            RepositoryError::Authorization(AuthorizationError::Forbidden)
        } else {
            RepositoryError::Authorization(AuthorizationError::StaleOwner)
        };
        assert_eq!(
            f.store.read_execution_context(&bad).await.unwrap_err(),
            expected,
            "context {dimension}"
        );
        assert_eq!(
            f.store
                .read_compaction_result(&bad, &intent.operation, None)
                .await
                .unwrap_err(),
            expected,
            "result {dimension}"
        );
        assert_eq!(
            f.store.begin_compaction(&bad, &intent).await.unwrap_err(),
            expected,
            "begin {dimension}"
        );
        assert_eq!(
            f.store
                .settle_compaction(&bad, &intent, None, "no_gain")
                .await
                .unwrap_err(),
            expected,
            "settle {dimension}"
        );
        assert_eq!(f.clock(), before);
    }
    let before = f.clock();
    f.store.begin_compaction(&env, &intent).await.unwrap();
    assert_eq!(f.clock(), before);
    let read = f.store.read_execution_context(&env).await.unwrap();
    assert_eq!(read.snapshot.messages, read.projection.messages);
    assert_eq!(read.frozen.tools, f.snapshot.tools);
}

#[tokio::test]
async fn native_complete_compact_install_has_one_terminal_preserves_canonical_and_reads_without_lease_reacquisition()
 {
    use agent_storage_ports::{
        RunContextRepository, RunFactRepository, SessionLifecycleRepository,
    };
    let mut f = Fixture::new();
    let (env, intent) = f.native().await;
    let before = f.store.read_execution_context(&env).await.unwrap();
    assert_eq!(
        f.store
            .settle_compaction(&env, &intent, Some(&before.snapshot.messages), "summary")
            .await
            .unwrap(),
        ProjectionGeneration(1)
    );
    let clock = f.clock();
    let epoch: u64 =
        f.db.query_row("SELECT epoch FROM session_writer_leases", [], |r| r.get(0))
            .unwrap();
    let read = f.store.read_execution_context(&env).await.unwrap();
    assert_eq!(read.snapshot.messages, before.snapshot.messages);
    assert_eq!(read.projection.messages, before.snapshot.messages);
    assert_eq!(read.projection.generation, ProjectionGeneration(1));
    let fact = f
        .store
        .read_compact_fact(&env.scope, &env.owner.run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fact.run.status, RunStatus::Completed);
    assert_eq!(fact.receipt.outcome, CompactRunOutcome::Committed);
    assert_eq!(
        f.store
            .read_compaction_result(&env, &intent.operation, None)
            .await
            .unwrap(),
        Some(ProjectionGeneration(1))
    );
    assert_eq!(
        f.store
            .read_exact_run_owner(&env.scope, &env.owner.run_id)
            .await
            .unwrap(),
        env.owner
    );
    assert_eq!(f.clock(), clock);
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT epoch FROM session_writer_leases", [], |r| r.get(0))
            .unwrap(),
        epoch
    );
    assert_eq!(
        f.store.begin_compaction(&env, &intent).await.unwrap_err(),
        CoordinationError::Conflict.into()
    );
    assert_eq!(
        f.store
            .settle_compaction(&env, &intent, None, "summary_failed")
            .await
            .unwrap_err(),
        CoordinationError::Conflict.into()
    );
    assert_eq!(f.clock(), clock);
    assert_eq!(
        f.db.query_row::<u64, _, _>(
            "SELECT count(*) FROM events WHERE run_id=?1 AND event='terminal'",
            [&env.owner.run_id.0],
            |r| r.get(0)
        )
        .unwrap(),
        1
    );
    f.store
        .commit_session_command(
            &env.scope,
            &SessionCommand::End {
                key: env.owner.session_key.clone(),
                delete: false,
            },
            Some(&env.owner.session_lifetime_id),
            "clear-after-compact",
        )
        .await
        .unwrap();
    let clock = f.clock();
    assert_eq!(
        f.store
            .read_compact_fact(&env.scope, &env.owner.run_id)
            .await
            .unwrap()
            .unwrap()
            .receipt
            .owner,
        env.owner
    );
    assert_eq!(
        f.store
            .read_exact_run_owner(&env.scope, &env.owner.run_id)
            .await
            .unwrap(),
        env.owner
    );
    assert_eq!(f.clock(), clock);
}

#[tokio::test]
async fn native_terminal_insert_fault_rolls_back_compaction_and_run_together() {
    use agent_storage_ports::RunContextRepository;
    let mut f = Fixture::new();
    let (env, intent) = f.native().await;
    f.db.execute_batch("CREATE TRIGGER compact_terminal_fault BEFORE INSERT ON events WHEN NEW.event='terminal' BEGIN SELECT RAISE(ABORT,'compact terminal fault');END").unwrap();
    let before = f.clock();
    assert!(
        f.store
            .settle_compaction(&env, &intent, None, "no_gain")
            .await
            .is_err()
    );
    assert_eq!(f.clock(), before);
    assert_eq!(
        f.store
            .read_compact_fact(&env.scope, &env.owner.run_id)
            .await
            .unwrap()
            .unwrap()
            .receipt
            .outcome,
        CompactRunOutcome::Started
    );
    f.db.execute_batch("DROP TRIGGER compact_terminal_fault")
        .unwrap();
    f.store
        .settle_compaction(&env, &intent, None, "no_gain")
        .await
        .unwrap();
    assert_eq!(
        f.store
            .read_compact_fact(&env.scope, &env.owner.run_id)
            .await
            .unwrap()
            .unwrap()
            .receipt
            .outcome,
        CompactRunOutcome::NoGain
    );
}

#[tokio::test]
async fn native_compact_revocation_expiry_and_saved_source_budget_are_zero_new_epoch_or_clock() {
    use agent_storage_ports::RunContextRepository;
    for fault in ["revoked", "expired", "oversized"] {
        let mut f = Fixture::new();
        let (env, intent) = f.native().await;
        let epoch: u64 =
            f.db.query_row("SELECT epoch FROM session_writer_leases", [], |r| r.get(0))
                .unwrap();
        let expected = match fault {
            "revoked" => {
                f.db.execute_batch(
                    "UPDATE principals SET revocation_epoch=1 WHERE subject_id='local-subject'",
                )
                .unwrap();
                RepositoryError::Authorization(AuthorizationError::Revoked)
            }
            "expired" => {
                f.db.execute_batch("UPDATE session_writer_leases SET expires_at_ms=0")
                    .unwrap();
                RepositoryError::Coordination(CoordinationError::StaleLease)
            }
            _ => {
                f.db.execute_batch(
                    "UPDATE compact_operations SET source_json=CAST(zeroblob(16385) AS TEXT)",
                )
                .unwrap();
                RepositoryError::Coordination(CoordinationError::Corrupt)
            }
        };
        let before = f.clock();
        assert_eq!(
            f.store.begin_compaction(&env, &intent).await.unwrap_err(),
            expected
        );
        assert_eq!(
            f.store
                .settle_compaction(&env, &intent, None, "no_gain")
                .await
                .unwrap_err(),
            expected
        );
        assert_eq!(
            f.store
                .read_compaction_result(&env, &intent.operation, None)
                .await
                .unwrap_err(),
            expected
        );
        assert_eq!(f.clock(), before);
        assert_eq!(
            f.db.query_row::<u64, _, _>("SELECT epoch FROM session_writer_leases", [], |r| r
                .get(0))
                .unwrap(),
            epoch
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn native_compact_real_sqlite_lock_wait_keeps_timer_live_and_cancelled_awaiter_keeps_real_commit()
 {
    use agent_storage_ports::RunContextRepository;
    let mut f = Fixture::new();
    let (env, intent) = f.native().await;
    let blocker = Connection::open(f.root.join("runtime.sqlite3")).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let (started, rx) = tokio::sync::oneshot::channel();
    let worker = std::thread::spawn(move || {
        started.send(()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(300));
        blocker.execute_batch("COMMIT").unwrap();
    });
    rx.await.unwrap();
    let now = std::time::Instant::now();
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(25),
            f.store.settle_compaction(&env, &intent, None, "no_gain")
        )
        .await
        .is_err()
    );
    assert!(now.elapsed() < std::time::Duration::from_millis(150));
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if f.store
            .read_compact_fact(&env.scope, &env.owner.run_id)
            .await
            .unwrap()
            .unwrap()
            .run
            .status
            == RunStatus::Completed
        {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    worker.join().unwrap();
    assert_eq!(
        f.db.query_row::<u64, _, _>(
            "SELECT count(*) FROM events WHERE run_id=?1 AND event='terminal'",
            [&env.owner.run_id.0],
            |r| r.get(0)
        )
        .unwrap(),
        1
    );
}

#[tokio::test]
async fn dedicated_compact_run_denies_another_operation_without_any_terminal() {
    use agent_storage_ports::RunContextRepository;
    let mut f = Fixture::new();
    let (env, intent) = f.native().await;
    let mut other = intent.clone();
    other.operation = "another-compact".into();
    let before = f.clock();
    let result = f.store.begin_compaction(&env, &other).await;
    assert_eq!(
        result,
        Err(CoordinationError::Conflict.into()),
        "专用run不得创建第二operation，实际为 {result:?}"
    );
    assert_eq!(f.clock(), before);
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM compact_operations", [], |r| r.get(0))
            .unwrap(),
        1
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>(
            "SELECT count(*) FROM events WHERE run_id=?1 AND event='terminal'",
            [&env.owner.run_id.0],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn native_chat_compaction_source_cas_rejects_later_projection_without_terminal_or_clock() {
    use agent_storage_ports::{
        IdentityReadRepository, RunContextRepository, RunExecutionRepository,
    };
    let mut f = Fixture::new();
    let (compact_env, compact_intent) = f.native().await;
    f.store
        .settle_compaction(&compact_env, &compact_intent, None, "no_gain")
        .await
        .unwrap();
    let identity = f
        .store
        .read_identity(
            &TenantContext::local(),
            &"local-subject".to_owned().try_into().unwrap(),
        )
        .await
        .unwrap();
    let auth = AuthorizationRequest {
        context: agent_identity::local_context(&identity, RequestId::Number(3)).unwrap(),
        expected_policy: identity.policy.version,
        action: Action::SessionPrompt,
        attributes: AuthorizationAttributes {
            workspace: f.snapshot.cwd.clone(),
            entry_channel: HookChannel::DaemonRpc,
            tool_effect: None,
            timestamp_ms: 0,
            network: NetworkBoundary::None,
        },
    };
    let (_, owner) = f
        .store
        .admit_run_authorized(
            &RunAdmission {
                workflow_node: None,
                plan_execution: None,
                session_key: f.request.session_key.clone(),
                expected_lifetime: Some(f.request.session_lifetime_id.clone()),
                request_id: auth.context.request_id.clone(),
                input: "竞争输入".into(),
                mode: AdmissionMode::Queue,
            },
            &f.snapshot,
            &auth,
            std::sync::Arc::new(agent_policy::PolicyEngine),
        )
        .await
        .unwrap();
    let env = EnterpriseOwnerEnvelope {
        scope: identity.scope,
        owner,
    };
    assert!(f.store.start_queued_run(&env).await.unwrap());
    let read = f.store.read_execution_context(&env).await.unwrap();
    let first = CompactIntent {
        operation: "implicit-one".into(),
        owner: env.owner.clone(),
        source: ContextSource {
            lifetime: read.snapshot.lifetime.clone(),
            source_start: TranscriptSeq(0),
            source_end: read.snapshot.revision,
            prefix_digest: agent_context::digest(&read.snapshot.messages).unwrap(),
            generation: read.snapshot.projection_generation,
            policy_fingerprint: "test".into(),
            pressure_route: "default".into(),
            summary_route: "default".into(),
        },
    };
    let mut second = first.clone();
    second.operation = "implicit-two".into();
    f.store.begin_compaction(&env, &first).await.unwrap();
    f.store.begin_compaction(&env, &second).await.unwrap();
    assert_eq!(
        f.store
            .settle_compaction(&env, &first, Some(&read.snapshot.messages), "summary")
            .await
            .unwrap(),
        ProjectionGeneration(1)
    );
    let before = f.clock();
    assert_eq!(
        f.store
            .settle_compaction(&env, &second, None, "summary_failed")
            .await
            .unwrap_err(),
        CoordinationError::Conflict.into()
    );
    assert_eq!(f.clock(), before);
    assert_eq!(
        f.db.query_row::<String, _, _>(
            "SELECT state FROM compact_operations WHERE operation=?1",
            [&second.operation],
            |r| r.get(0)
        )
        .unwrap(),
        "pending"
    );
    assert_eq!(
        f.db.query_row::<String, _, _>(
            "SELECT status FROM runs WHERE id=?1",
            [&env.owner.run_id.0],
            |r| r.get(0)
        )
        .unwrap(),
        "running"
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>(
            "SELECT count(*) FROM events WHERE run_id=?1 AND event='terminal'",
            [&env.owner.run_id.0],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn native_compact_saved_frozen_snapshot_budget_is_checked_before_decode() {
    use agent_storage_ports::RunContextRepository;
    let f = Fixture::new();
    let (_, intent) = f.admitted();
    let env = EnterpriseOwnerEnvelope {
        scope: TenantContext::local(),
        owner: intent.owner.clone(),
    };
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM run_authorizations", [], |r| r.get(0))
            .unwrap(),
        0
    );
    let mut saved = f.snapshot.clone();
    saved.cwd = "x".repeat(1024 * 1024 + 1);
    f.db.execute(
        "UPDATE run_snapshots SET snapshot_json=?2 WHERE run_id=?1",
        params![env.owner.run_id.0, serde_json::to_string(&saved).unwrap()],
    )
    .unwrap();
    let clock = f.clock();
    let result = f.store.begin_compaction(&env, &intent).await;
    assert_eq!(
        result,
        Err(CoordinationError::Corrupt.into()),
        "合法大冻结快照不能先装入Rust，实际为 {result:?}"
    );
    assert_eq!(f.clock(), clock);
}
#[tokio::test]
async fn native_compact_settlement_rechecks_frozen_read_only_before_any_terminal() {
    use agent_storage_ports::RunContextRepository;
    let mut f = Fixture::new();
    let (env, intent) = f.native().await;
    let mut saved = f.snapshot.clone();
    saved.context_read_only = true;
    f.db.execute(
        "UPDATE run_snapshots SET snapshot_json=?2 WHERE run_id=?1",
        params![env.owner.run_id.0, serde_json::to_string(&saved).unwrap()],
    )
    .unwrap();
    let clock = f.clock();
    let result = f
        .store
        .settle_compaction(&env, &intent, None, "no_gain")
        .await;
    assert_eq!(
        result,
        Err(AuthorizationError::Forbidden.into()),
        "结算必须再次复核frozen只读政策，实际为 {result:?}"
    );
    assert_eq!(f.clock(), clock);
    assert_eq!(
        f.db.query_row::<String, _, _>(
            "SELECT state FROM compact_operations WHERE operation=?1",
            [&intent.operation],
            |r| r.get(0)
        )
        .unwrap(),
        "pending"
    );
}

// 复用run_context_contract::Fixture，门禁结束后补真实RED及修复。
#[tokio::test]
async fn compact_result_rejects_saved_source_json_lifetime_mismatch_before_returning_generation() {
    use agent_storage_ports::RunContextRepository;
    let f = Fixture::new();
    let (_, intent) = f.admitted();
    f.store.settle_compact(&intent, None, "no_gain").unwrap();
    let mut source = intent.source.clone();
    source.lifetime.0 = "another-lifetime".into();
    f.db.execute(
        "UPDATE compact_operations SET source_json=?2 WHERE operation=?1",
        params![intent.operation, serde_json::to_string(&source).unwrap()],
    )
    .unwrap();
    let env = EnterpriseOwnerEnvelope {
        scope: TenantContext::local(),
        owner: intent.owner.clone(),
    };
    let result = f
        .store
        .read_compaction_result(&env, &intent.operation, None)
        .await;
    assert_eq!(
        result,
        Err(CoordinationError::Corrupt.into()),
        "源JSON必须核lifetime而非只SQL筛选，实际为 {result:?}"
    );
}

#[tokio::test]
async fn compact_result_distinguishes_revision_conflict_and_failed_summary_from_corruption() {
    use agent_storage_ports::RunContextRepository;
    for reason in ["no_gain", "summary_failed"] {
        let f = Fixture::new();
        let (_, intent) = f.admitted();
        f.store.settle_compact(&intent, None, reason).unwrap();
        let env = EnterpriseOwnerEnvelope {
            scope: TenantContext::local(),
            owner: intent.owner.clone(),
        };
        let before = f.clock();
        assert_eq!(
            f.store
                .read_compaction_result(
                    &env,
                    &intent.operation,
                    Some(TranscriptSeq(intent.source.source_end.0 + 1))
                )
                .await,
            Err(CoordinationError::Conflict.into())
        );
        let result = f
            .store
            .read_compaction_result(&env, &intent.operation, None)
            .await;
        if reason == "no_gain" {
            assert_eq!(result, Ok(Some(intent.source.generation)));
        } else {
            assert_eq!(result, Err(CoordinationError::Conflict.into()));
        }
        assert_eq!(f.clock(), before);
    }
}
#[tokio::test]
async fn compact_result_rejects_inconsistent_saved_generation_and_noncanonical_start() {
    use agent_storage_ports::RunContextRepository;
    for corrupt_generation in [false, true] {
        let f = Fixture::new();
        let (_, intent) = f.admitted();
        f.store.settle_compact(&intent, None, "no_gain").unwrap();
        if corrupt_generation {
            f.db.execute("UPDATE compact_operations SET result_generation=result_generation+1 WHERE operation=?1", [&intent.operation]).unwrap();
        } else {
            let mut source = intent.source.clone();
            source.source_start = TranscriptSeq(1);
            f.db.execute(
                "UPDATE compact_operations SET source_json=?2 WHERE operation=?1",
                params![intent.operation, serde_json::to_string(&source).unwrap()],
            )
            .unwrap();
        }
        let env = EnterpriseOwnerEnvelope {
            scope: TenantContext::local(),
            owner: intent.owner.clone(),
        };
        let before = f.clock();
        assert_eq!(
            f.store
                .read_compaction_result(&env, &intent.operation, None)
                .await,
            Err(CoordinationError::Corrupt.into())
        );
        assert_eq!(f.clock(), before);
    }
}
