//! 原 turn/transcript 的 scope 合同；初始 RED 直接调用原同步生产 helper。
use agent_core::*;
use agent_storage::{RunStore, SessionQuery};
use agent_storage_ports::RunTurnRepository;
use rusqlite::Connection;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    root: std::path::PathBuf,
    store: RunStore,
    db: Connection,
    owner: ExactOwner,
}
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "ma-turn-port-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let path = root.join("runtime.sqlite3");
        let store = RunStore::open(&path).unwrap();
        let Admission::New(run) = store
            .admit(
                SessionKey("turn-session".into()),
                RequestId::Number(1),
                "原turn事实",
            )
            .unwrap()
        else {
            panic!("new")
        };
        store.try_start_queued(&run.run_id).unwrap();
        let owner = store.run_owner(&run.run_id).unwrap();
        Self {
            root,
            store,
            db: Connection::open(path).unwrap(),
            owner,
        }
    }
    fn envelope(&self) -> EnterpriseOwnerEnvelope {
        EnterpriseOwnerEnvelope {
            scope: TenantContext::local(),
            owner: self.owner.clone(),
        }
    }
    fn foreign(&self) {
        self.db.execute_batch("INSERT INTO organizations VALUES('foreign');INSERT INTO tenants VALUES('foreign','foreign');INSERT INTO projects VALUES('foreign','foreign');INSERT INTO agents VALUES('foreign','foreign','foreign');UPDATE session_scopes SET tenant_id='foreign',project_id='foreign',agent_uid='foreign' WHERE session_id='turn-session';").unwrap();
    }
    fn clock(&self) -> u64 {
        self.db
            .query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
#[tokio::test]
async fn local_turn_caller_cannot_stage_assistant_in_foreign_legacy_scope() {
    let f = Fixture::new();
    f.foreign();
    let before = f.clock();
    assert!(matches!(
        f.store
            .stage_run_assistant(
                &f.envelope(),
                &Message::text(Role::Assistant, "foreign text")
            )
            .await,
        Err(RepositoryError::Authorization(
            AuthorizationError::Forbidden
        ))
    ));
    assert_eq!(f.clock(), before);
}
#[tokio::test]
async fn local_turn_caller_cannot_append_transcript_in_foreign_legacy_scope() {
    let f = Fixture::new();
    f.foreign();
    let before = f.clock();
    assert!(matches!(
        f.store
            .append_run_transcript(
                &f.envelope(),
                "foreign-append",
                &[Message::text(Role::User, "foreign text")]
            )
            .await,
        Err(RepositoryError::Authorization(
            AuthorizationError::Forbidden
        ))
    ));
    assert_eq!(f.clock(), before);
}
#[tokio::test]
async fn local_turn_caller_cannot_reject_tool_exchange_in_foreign_legacy_scope() {
    let f = Fixture::new();
    f.foreign();
    let before = f.clock();
    let calls = [ToolCall {
        id: "call".into(),
        name: "effect".into(),
        arguments: serde_json::json!({}),
    }];
    assert!(matches!(
        f.store
            .reject_run_tool_batch(&f.envelope(), 1, &calls, "reject")
            .await,
        Err(RepositoryError::Authorization(
            AuthorizationError::Forbidden
        ))
    ));
    assert_eq!(f.clock(), before);
}
#[tokio::test]
async fn local_turn_caller_cannot_read_plan_in_foreign_legacy_scope() {
    let f = Fixture::new();
    f.foreign();
    let before = f.clock();
    assert!(matches!(
        f.store.read_run_plan(&f.envelope()).await,
        Err(RepositoryError::Authorization(
            AuthorizationError::Forbidden
        ))
    ));
    assert_eq!(f.clock(), before);
}

#[tokio::test]
async fn local_turn_caller_cannot_stage_plan_in_foreign_legacy_scope() {
    let f = Fixture::new();
    f.foreign();
    let before = f.clock();
    assert!(matches!(
        f.store
            .stage_run_plan(&f.envelope(), 0, &serde_json::json!({"steps":[]}))
            .await,
        Err(RepositoryError::Authorization(
            AuthorizationError::Forbidden
        ))
    ));
    assert_eq!(f.clock(), before);
}

#[tokio::test]
async fn local_turn_caller_cannot_begin_plan_execution_in_foreign_legacy_scope() {
    let f = Fixture::new();
    f.foreign();
    let before = f.clock();
    assert!(matches!(
        f.store.start_run_plan(&f.envelope()).await,
        Err(RepositoryError::Authorization(
            AuthorizationError::Forbidden
        ))
    ));
    assert_eq!(f.clock(), before);
}

fn calls() -> [ToolCall; 1] {
    [ToolCall {
        id: "call".into(),
        name: "effect".into(),
        arguments: serde_json::json!({}),
    }]
}
fn plan() -> serde_json::Value {
    serde_json::json!({"steps":[{"id":"a","description":"验证原计划","status":"pending"}]})
}
#[tokio::test]
async fn turn_methods_deny_all_scope_and_fixed_owner_dimensions_without_creating_any_stage() {
    let f = Fixture::new();
    let before = f.clock();
    for dimension in 0..8 {
        let mut e = f.envelope();
        match dimension {
            0 => e.scope.organization_id = "foreign".to_owned().try_into().unwrap(),
            1 => e.scope.tenant_id = "foreign".to_owned().try_into().unwrap(),
            2 => e.scope.project_id = "foreign".to_owned().try_into().unwrap(),
            3 => e.scope.agent_uid = "foreign".to_owned().try_into().unwrap(),
            4 => e.owner.session_key.0.push('x'),
            5 => e.owner.session_lifetime_id.0.push('x'),
            6 => e.owner.run_generation.0 += 1,
            _ => e.owner.turn_id.0.push('x'),
        }
        let expected: RepositoryError = if dimension < 4 {
            AuthorizationError::Forbidden.into()
        } else {
            AuthorizationError::StaleOwner.into()
        };
        for result in [
            f.store.read_run_plan(&e).await.map(|_| ()),
            f.store.stage_run_plan(&e, 0, &plan()).await.map(|_| ()),
            f.store.start_run_plan(&e).await,
            f.store
                .reject_run_tool_batch(&e, 1, &calls(), "reject")
                .await
                .map(|_| ()),
            f.store
                .stage_run_assistant(&e, &Message::text(Role::Assistant, "result"))
                .await,
            f.store
                .append_run_transcript(&e, "operation", &[Message::text(Role::User, "input")])
                .await
                .map(|_| ()),
        ] {
            assert_eq!(result, Err(expected), "维度 {dimension}");
        }
    }
    assert_eq!(f.clock(), before);
    for table in ["turn_outputs", "turn_plan_stages", "tool_batches"] {
        assert_eq!(
            f.db.query_row::<u64, _, _>(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
                .unwrap(),
            0
        );
    }
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM transcript_batches", [], |r| r.get(0))
            .unwrap(),
        1
    );
}
#[tokio::test]
async fn original_turn_commit_publishes_stages_and_transcript_once_and_survives_reopen() {
    use agent_storage::TurnRepository;
    use agent_storage_ports::RunExecutionRepository;
    let f = Fixture::new();
    let e = f.envelope();
    let before = f.clock();
    let read = f.store.read_run_plan(&e).await.unwrap();
    assert_eq!(read.snapshot.revision, 0);
    assert!(!read.has_legacy_plan);
    assert_eq!(f.clock(), before);
    let staged = f.store.stage_run_plan(&e, 0, &plan()).await.unwrap();
    assert_eq!(staged.revision, 1);
    let stage_clock = f.clock();
    let repeated = f.store.stage_run_plan(&e, 1, &plan()).await.unwrap();
    assert_eq!(repeated.value, staged.value);
    assert_eq!(f.clock(), stage_clock);
    assert!(matches!(
        f.store.stage_run_plan(&e, 0, &plan()).await,
        Err(RepositoryError::Coordination(CoordinationError::Conflict))
    ));
    assert_eq!(f.clock(), stage_clock);
    let output = Message::text(Role::Assistant, "canonical output");
    f.store.stage_run_assistant(&e, &output).await.unwrap();
    let output_clock = f.clock();
    f.store.stage_run_assistant(&e, &output).await.unwrap();
    assert_eq!(f.clock(), output_clock);
    let messages = [Message::text(Role::User, "additional input")];
    let revision = f
        .store
        .append_run_transcript(&e, "operation", &messages)
        .await
        .unwrap();
    let append_clock = f.clock();
    assert_eq!(
        f.store
            .append_run_transcript(&e, "operation", &messages)
            .await
            .unwrap(),
        revision
    );
    assert_eq!(f.clock(), append_clock);
    assert_eq!(
        f.store
            .append_run_transcript(&e, "operation", &[Message::text(Role::User, "changed")])
            .await,
        Err(CoordinationError::Conflict.into())
    );
    assert_eq!(f.clock(), append_clock);
    let commit = TurnCommit {
        owner: e.owner.clone(),
        status: RunStatus::Completed,
        content: Some("canonical output".into()),
        error: None,
    };
    f.store.commit_run_turn(&e, &commit).await.unwrap();
    let terminal_clock = f.clock();
    f.store.commit_run_turn(&e, &commit).await.unwrap();
    assert_eq!(f.clock(), terminal_clock);
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM turn_commits", [], |r| r.get(0))
            .unwrap(),
        1
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM plan_versions", [], |r| r.get(0))
            .unwrap(),
        1
    );
    let reopened = RunStore::open(&f.root.join("runtime.sqlite3")).unwrap();
    let snapshot = reopened.session_snapshot(&e.owner.session_key).unwrap();
    assert_eq!(
        snapshot
            .messages
            .iter()
            .filter(|m| m.content.as_deref() == Some("canonical output"))
            .count(),
        1
    );
    assert_eq!(
        reopened.read_run_plan(&e).await.unwrap().snapshot.revision,
        1
    );
    assert!(matches!(
        reopened.stage_run_assistant(&e, &output).await,
        Err(RepositoryError::Coordination(CoordinationError::Conflict))
    ));
    assert_eq!(f.clock(), terminal_clock);
    assert_eq!(f.store.read_plan(&e.owner).unwrap().revision, 1);
    assert_eq!(
        reopened.start_run_plan(&e).await,
        Err(CoordinationError::Conflict.into()),
        "已结算 run 不能取得计划执行 permit，即使没有决策行"
    );
}
#[tokio::test]
async fn rejected_exchange_keeps_original_not_executed_receipts_and_never_prepares_an_effect() {
    use agent_storage_ports::RunToolRepository;
    let f = Fixture::new();
    let e = f.envelope();
    let messages = f
        .store
        .reject_run_tool_batch(&e, 1, &calls(), "hard deny")
        .await
        .unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[1].tool_call_id.as_deref(), Some("call"));
    let clock = f.clock();
    assert_eq!(
        f.store
            .reject_run_tool_batch(&e, 1, &calls(), "hard deny")
            .await
            .unwrap_err(),
        CoordinationError::Conflict.into()
    );
    assert_eq!(
        f.store.start_run_tool(&e, 1, "call").await,
        Err(CoordinationError::Conflict.into())
    );
    assert_eq!(f.clock(), clock);
    let (status, outcome): (String, String) =
        f.db.query_row("SELECT status,outcome FROM tool_executions", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!(status, "terminal");
    assert_eq!(outcome, "not_executed");
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM transcript_batches", [], |r| r.get(0))
            .unwrap(),
        2
    );
}
#[tokio::test]
async fn original_turn_faults_roll_back_each_mutation_without_new_clock_or_partial_exchange() {
    let f = Fixture::new();
    let e = f.envelope();
    let before = f.clock();
    for (table, operation) in [
        ("turn_outputs", 0),
        ("turn_plan_stages", 1),
        ("transcript_batches", 2),
        ("tool_executions", 3),
    ] {
        f.db.execute_batch(&format!("CREATE TRIGGER fault BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT,'controlled'); END;")).unwrap();
        let result = match operation {
            0 => {
                f.store
                    .stage_run_assistant(&e, &Message::text(Role::Assistant, "output"))
                    .await
            }
            1 => f.store.stage_run_plan(&e, 0, &plan()).await.map(|_| ()),
            2 => f
                .store
                .append_run_transcript(&e, "fault", &[Message::text(Role::User, "input")])
                .await
                .map(|_| ()),
            _ => f
                .store
                .reject_run_tool_batch(&e, 1, &calls(), "reject")
                .await
                .map(|_| ()),
        };
        assert_eq!(
            result,
            Err(CoordinationError::StorageUnavailable.into()),
            "{table}"
        );
        assert_eq!(f.clock(), before);
        f.db.execute_batch("DROP TRIGGER fault;").unwrap();
    }
    for table in [
        "turn_outputs",
        "turn_plan_stages",
        "tool_batches",
        "tool_executions",
    ] {
        assert_eq!(
            f.db.query_row::<u64, _, _>(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
                .unwrap(),
            0
        );
    }
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM transcript_batches", [], |r| r.get(0))
            .unwrap(),
        1
    );
}
#[tokio::test]
async fn two_original_connections_plan_cas_and_transcript_replay_share_one_fact() {
    let f = Fixture::new();
    let e = f.envelope();
    let other = RunStore::open(&f.root.join("runtime.sqlite3")).unwrap();
    let definition = plan();
    let (a, b) = tokio::join!(
        f.store.stage_run_plan(&e, 0, &definition),
        other.stage_run_plan(&e, 0, &definition)
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    let loser = if a.is_err() {
        a.err().unwrap()
    } else {
        b.err().unwrap()
    };
    assert_eq!(loser, CoordinationError::Conflict.into());
    let messages = [Message::text(Role::User, "same append")];
    let (a, b) = tokio::join!(
        f.store.append_run_transcript(&e, "same", &messages),
        other.append_run_transcript(&e, "same", &messages)
    );
    assert_eq!(a.unwrap(), b.unwrap());
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM transcript_batches", [], |r| r.get(0))
            .unwrap(),
        2
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM turn_plan_stages", [], |r| r.get(0))
            .unwrap(),
        1
    );
}
#[tokio::test(flavor = "current_thread")]
async fn native_turn_waiting_for_real_sqlite_write_lock_keeps_timer_responsive() {
    let f = Fixture::new();
    let e = f.envelope();
    let path = f.root.join("runtime.sqlite3");
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let holder = std::thread::spawn(move || {
        let db = Connection::open(path).unwrap();
        db.execute_batch("BEGIN IMMEDIATE;").unwrap();
        tx.send(()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(300));
        db.execute_batch("COMMIT;").unwrap();
    });
    rx.recv().unwrap();
    let begin = std::time::Instant::now();
    let message = Message::text(Role::Assistant, "result");
    let (result, elapsed) = tokio::join!(f.store.stage_run_assistant(&e, &message), async {
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        begin.elapsed()
    });
    holder.join().unwrap();
    result.unwrap();
    assert!(
        elapsed < std::time::Duration::from_millis(150),
        "timer阻塞 {elapsed:?}"
    );
}

#[tokio::test]
async fn native_turn_every_method_rechecks_frozen_revocation_and_expired_proof_without_acquiring_new_epoch()
 {
    use agent_storage_ports::{IdentityReadRepository, RunExecutionRepository};
    for fault in ["revoked", "expired"] {
        let root = std::env::temp_dir().join(format!(
            "ma-turn-fence-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let path = root.join("runtime.sqlite3");
        let store = RunStore::open(&path)
            .unwrap()
            .enable_session_writer()
            .unwrap();
        let identity = store
            .read_identity(
                &TenantContext::local(),
                &"local-subject".to_owned().try_into().unwrap(),
            )
            .await
            .unwrap();
        let admission = RunAdmission {
            workflow_node: None,
            plan_execution: None,
            session_key: SessionKey("tool-session".into()),
            expected_lifetime: None,
            request_id: RequestId::Number(1),
            input: "原工具复核".into(),
            mode: AdmissionMode::Queue,
        };
        let snapshot:RunSnapshot=serde_json::from_value(serde_json::json!({"cwd":".","tools":[],"permission_mode":"risk","sandbox_requested":"native","sandbox_effective":"native","context_read_only":false,"config_generation":0})).unwrap();
        let authorization = AuthorizationRequest {
            context: agent_identity::local_context(&identity, admission.request_id.clone())
                .unwrap(),
            expected_policy: identity.policy.version,
            action: Action::SessionPrompt,
            attributes: AuthorizationAttributes {
                workspace: ".".into(),
                entry_channel: HookChannel::DaemonRpc,
                tool_effect: None,
                timestamp_ms: 0,
                network: NetworkBoundary::None,
            },
        };
        let (_, owner) = store
            .admit_run_authorized(
                &admission,
                &snapshot,
                &authorization,
                std::sync::Arc::new(agent_policy::PolicyEngine),
            )
            .await
            .unwrap();
        let e = EnterpriseOwnerEnvelope {
            scope: TenantContext::local(),
            owner,
        };
        assert!(store.start_queued_run(&e).await.unwrap());
        let db = Connection::open(path).unwrap();
        let expected: RepositoryError = if fault == "revoked" {
            db.execute("UPDATE principals SET revocation_epoch=1", [])
                .unwrap();
            AuthorizationError::Revoked.into()
        } else {
            db.execute("UPDATE session_writer_leases SET expires_at_ms=0", [])
                .unwrap();
            CoordinationError::StaleLease.into()
        };
        let before: u64 = db
            .query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
        let epoch: u64 = db
            .query_row("SELECT epoch FROM session_writer_leases", [], |r| r.get(0))
            .unwrap();
        let messages = [Message::text(Role::User, "input")];
        for result in [
            store.read_run_plan(&e).await.map(|_| ()),
            store.stage_run_plan(&e, 0, &plan()).await.map(|_| ()),
            store.start_run_plan(&e).await,
            store
                .reject_run_tool_batch(&e, 1, &calls(), "reject")
                .await
                .map(|_| ()),
            store
                .stage_run_assistant(&e, &Message::text(Role::Assistant, "output"))
                .await,
            store
                .append_run_transcript(&e, "operation", &messages)
                .await
                .map(|_| ()),
        ] {
            assert_eq!(result, Err(expected), "{fault}");
        }
        assert_eq!(
            db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
                .unwrap(),
            before
        );
        assert_eq!(
            db.query_row::<u64, _, _>("SELECT epoch FROM session_writer_leases", [], |r| r.get(0))
                .unwrap(),
            epoch
        );
        assert!(!root.join("runtime.artifacts").exists());
        drop(store);
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }
}

#[tokio::test]
async fn native_plan_and_output_corruption_fail_closed_before_reading_unbounded_saved_json() {
    let f = Fixture::new();
    let e = f.envelope();
    f.store.stage_run_plan(&e, 0, &plan()).await.unwrap();
    let (saved, revision): (String, u64) =
        f.db.query_row("SELECT data_json,revision FROM turn_plan_stages", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    let before = f.clock();
    for bad in [
        "{".to_owned(),
        serde_json::json!({"payload":"x".repeat(1024*1024)}).to_string(),
    ] {
        f.db.execute("UPDATE turn_plan_stages SET data_json=?1", [&bad])
            .unwrap();
        assert!(matches!(
            f.store.read_run_plan(&e).await,
            Err(RepositoryError::Coordination(CoordinationError::Corrupt))
        ));
        assert!(matches!(
            f.store.stage_run_plan(&e, revision, &plan()).await,
            Err(RepositoryError::Coordination(CoordinationError::Corrupt))
        ));
        assert_eq!(f.clock(), before);
    }
    f.db.execute("UPDATE turn_plan_stages SET data_json=?1", [saved])
        .unwrap();
    let output = Message::text(Role::Assistant, "output");
    f.store.stage_run_assistant(&e, &output).await.unwrap();
    let before = f.clock();
    f.db.execute("UPDATE turn_outputs SET message_json='{'", [])
        .unwrap();
    assert_eq!(
        f.store.stage_run_assistant(&e, &output).await,
        Err(CoordinationError::Corrupt.into())
    );
    assert_eq!(f.clock(), before);
    assert!(f.store.read_run_plan(&e).await.is_ok());
}
#[tokio::test]
async fn native_turn_input_budgets_and_legacy_plan_readback_do_not_create_or_overwrite_facts() {
    let f = Fixture::new();
    let e = f.envelope();
    let before = f.clock();
    let large = serde_json::json!({"steps":[],"title":"x".repeat(512*1024)});
    assert_eq!(
        f.store.stage_run_plan(&e, 0, &large).await.unwrap_err(),
        CoordinationError::InvalidRequest.into()
    );
    assert_eq!(
        f.store
            .stage_run_assistant(
                &e,
                &Message::text(Role::Assistant, "x".repeat(4 * 1024 * 1024))
            )
            .await,
        Err(CoordinationError::InvalidRequest.into())
    );
    assert_eq!(
        f.store
            .stage_run_assistant(&e, &Message::text(Role::User, "not assistant"))
            .await,
        Err(CoordinationError::InvalidRequest.into())
    );
    assert_eq!(
        f.store
            .append_run_transcript(
                &e,
                "operation",
                &[Message::text(Role::User, "x".repeat(4 * 1024 * 1024))]
            )
            .await,
        Err(CoordinationError::InvalidRequest.into())
    );
    assert_eq!(
        f.store
            .reject_run_tool_batch(&e, 0, &calls(), "reject")
            .await
            .unwrap_err(),
        CoordinationError::InvalidRequest.into()
    );
    assert_eq!(
        f.store
            .reject_run_tool_batch(&e, 1, &[calls()[0].clone(), calls()[0].clone()], "reject")
            .await
            .unwrap_err(),
        CoordinationError::InvalidRequest.into()
    );
    assert_eq!(f.clock(), before);
    f.db.execute(
        "INSERT INTO session_plans VALUES(?1,0,?2)",
        rusqlite::params![e.owner.session_lifetime_id.0, plan().to_string()],
    )
    .unwrap();
    let before = f.clock();
    let read = f.store.read_run_plan(&e).await.unwrap();
    assert!(read.has_legacy_plan);
    assert_eq!(read.snapshot.revision, 0);
    assert!(read.snapshot.value.get("plan_id").is_none());
    assert_eq!(f.clock(), before);
    f.store.stage_run_plan(&e, 0, &plan()).await.unwrap();
    let before = f.clock();
    let read = f.store.read_run_plan(&e).await.unwrap();
    assert!(read.has_legacy_plan);
    assert!(read.snapshot.value.get("plan_id").is_some());
    assert_eq!(f.clock(), before);
}

#[tokio::test]
async fn native_plan_execution_validates_original_decision_receipt_identity_before_transition() {
    use agent_storage_ports::RunExecutionRepository;
    let f = Fixture::new();
    let first = f.envelope();
    let staged = f.store.stage_run_plan(&first, 0, &plan()).await.unwrap();
    let doc: PlanDocument = serde_json::from_value(staged.value).unwrap();
    f.store
        .commit_run_turn(
            &first,
            &TurnCommit {
                owner: first.owner.clone(),
                status: RunStatus::Completed,
                content: Some("plan built".into()),
                error: None,
            },
        )
        .await
        .unwrap();
    let snapshot:RunSnapshot=serde_json::from_value(serde_json::json!({"cwd":".","tools":[],"permission_mode":"risk","sandbox_requested":"native","sandbox_effective":"native","context_read_only":false,"config_generation":0})).unwrap();
    let admission = RunAdmission {
        workflow_node: None,
        session_key: first.owner.session_key.clone(),
        expected_lifetime: Some(first.owner.session_lifetime_id.clone()),
        request_id: RequestId::Number(2),
        input: "execute plan".into(),
        mode: AdmissionMode::RejectIfBusy,
        plan_execution: Some(PlanExecution {
            plan_id: doc.plan_id,
            revision: doc.revision,
            content_digest: doc.content_digest,
            operation_id: "execute-plan".into(),
        }),
    };
    let Admission::New(run) = f.store.admit_run(&admission, &snapshot).unwrap() else {
        panic!("new")
    };
    f.store.try_start_queued(&run.run_id).unwrap();
    let e = EnterpriseOwnerEnvelope {
        scope: TenantContext::local(),
        owner: f.store.run_owner(&run.run_id).unwrap(),
    };
    let raw: String =
        f.db.query_row("SELECT receipt_json FROM plan_decisions", [], |r| r.get(0))
            .unwrap();
    for field in ["run_id", "decision", "session_key", "lifetime"] {
        let mut receipt: serde_json::Value = serde_json::from_str(&raw).unwrap();
        receipt[field] = serde_json::json!(if field == "decision" {
            "discard"
        } else {
            "wrong-owner"
        });
        f.db.execute(
            "UPDATE plan_decisions SET receipt_json=?1",
            [receipt.to_string()],
        )
        .unwrap();
        let before = f.clock();
        assert_eq!(
            f.store.start_run_plan(&e).await,
            Err(CoordinationError::Corrupt.into()),
            "字段 {field}"
        );
        assert_eq!(f.clock(), before);
    }
    f.db.execute("UPDATE plan_decisions SET receipt_json=?1", [&raw])
        .unwrap();
    for field in ["operation_id", "session_id", "lifetime", "identity_json"] {
        let saved: String =
            f.db.query_row(&format!("SELECT {field} FROM plan_decisions"), [], |row| {
                row.get(0)
            })
            .unwrap();
        let changed = if field == "identity_json" {
            let mut identity: serde_json::Value = serde_json::from_str(&saved).unwrap();
            identity["content_digest"] = serde_json::json!("wrong");
            identity.to_string()
        } else {
            "wrong".into()
        };
        f.db.execute(&format!("UPDATE plan_decisions SET {field}=?1"), [&changed])
            .unwrap();
        let before = f.clock();
        assert_eq!(
            f.store.start_run_plan(&e).await,
            Err(CoordinationError::Corrupt.into()),
            "SQL字段 {field}"
        );
        assert_eq!(f.clock(), before);
        f.db.execute(&format!("UPDATE plan_decisions SET {field}=?1"), [&saved])
            .unwrap();
    }
    let before = f.clock();
    f.db.execute_batch("CREATE TRIGGER plan_fault BEFORE UPDATE ON session_plans BEGIN SELECT RAISE(ABORT,'controlled'); END;").unwrap();
    assert_eq!(
        f.store.start_run_plan(&e).await,
        Err(CoordinationError::StorageUnavailable.into())
    );
    assert_eq!(f.clock(), before);
    f.db.execute_batch("DROP TRIGGER plan_fault;").unwrap();
    f.store.start_run_plan(&e).await.unwrap();
    let read = f.store.read_run_plan(&e).await.unwrap();
    assert_eq!(read.snapshot.value["review"], "executing");
}
