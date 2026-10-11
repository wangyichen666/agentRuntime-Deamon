//! 原 canonical terminal 的真实越域及迟到 writer 合同。
use agent_core::*;
use agent_storage::{IdentityRepository, RunStore, RuntimeError, SessionQuery};
use agent_storage_ports::RunExecutionRepository;
use rusqlite::Connection;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    root: PathBuf,
    store: RunStore,
    db: Connection,
    envelope: EnterpriseOwnerEnvelope,
}
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "ma-run-execution-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let path = root.join("runtime.sqlite3");
        let store = RunStore::open(&path).unwrap();
        let Admission::New(run) = store
            .admit(
                SessionKey("execution".into()),
                RequestId::Number(1),
                "私密输入",
            )
            .unwrap()
        else {
            panic!("new")
        };
        store.try_start_queued(&run.run_id).unwrap();
        let envelope = EnterpriseOwnerEnvelope {
            scope: TenantContext::local(),
            owner: store.run_owner(&run.run_id).unwrap(),
        };
        Self {
            db: Connection::open(path).unwrap(),
            root,
            store,
            envelope,
        }
    }
    fn commit(&self) -> TurnCommit {
        TurnCommit {
            owner: self.envelope.owner.clone(),
            status: RunStatus::Completed,
            content: Some("私密结果".into()),
            error: None,
        }
    }
    fn clock(&self) -> u64 {
        self.db
            .query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap()
    }
    fn foreign(&self) {
        self.db.execute_batch("INSERT INTO organizations VALUES('other-org');INSERT INTO tenants VALUES('other-tenant','other-org');INSERT INTO projects VALUES('other-project','other-tenant');INSERT INTO agents VALUES('other-agent','other-tenant','other-project');UPDATE session_scopes SET tenant_id='other-tenant',project_id='other-project',agent_uid='other-agent' WHERE session_id='execution';").unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
async fn commit(
    store: &RunStore,
    envelope: &EnterpriseOwnerEnvelope,
    turn: &TurnCommit,
) -> Result<RunRecord, RuntimeError> {
    store
        .commit_run_turn(envelope, turn)
        .await
        .map_err(Into::into)
}
#[tokio::test]
async fn execution_terminal_rejects_foreign_legacy_run_without_frozen_authorization() {
    let f = Fixture::new();
    f.foreign();
    let before = f.clock();
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM run_authorizations", [], |r| r.get(0))
            .unwrap(),
        0
    );
    let result = commit(&f.store, &f.envelope, &f.commit()).await;
    assert!(
        matches!(
            result,
            Err(RuntimeError::Authorization(AuthorizationError::Forbidden))
        ),
        "旧无授权记录也必须按完整scope拒绝terminal，实际为 {result:?}"
    );
    assert_eq!(f.clock(), before);
    assert_eq!(
        f.store
            .read_run(&f.envelope.owner.run_id)
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Running
    );
}
#[tokio::test]
async fn execution_terminal_keeps_admitted_owner_instead_of_adopting_later_generation() {
    let f = Fixture::new();
    f.db.execute(
        "UPDATE runs SET generation=generation+1 WHERE id=?1",
        [&f.envelope.owner.run_id.0],
    )
    .unwrap();
    let before = f.clock();
    let result = commit(&f.store, &f.envelope, &f.commit()).await;
    assert!(
        matches!(
            result,
            Err(RuntimeError::Authorization(AuthorizationError::StaleOwner))
        ),
        "迟到caller不得重读并采用后来的generation，实际为 {result:?}"
    );
    assert_eq!(f.clock(), before);
    assert_eq!(
        f.store
            .read_run(&f.envelope.owner.run_id)
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Running
    );
}

#[tokio::test]
async fn execution_mutations_check_all_scope_and_owner_dimensions_before_any_queue_or_terminal_result()
 {
    let f = Fixture::new();
    let before = f.clock();
    for field in 0..4 {
        let mut e = f.envelope.clone();
        let unknown = "unknown".to_owned();
        match field {
            0 => e.scope.organization_id = unknown.try_into().unwrap(),
            1 => e.scope.tenant_id = unknown.try_into().unwrap(),
            2 => e.scope.project_id = unknown.try_into().unwrap(),
            _ => e.scope.agent_uid = unknown.try_into().unwrap(),
        }
        for result in [
            f.store.start_queued_run(&e).await.map(|_| ()),
            f.store.cancel_queued_run(&e).await.map(|_| ()),
            f.store
                .commit_run_turn(
                    &e,
                    &TurnCommit {
                        owner: e.owner.clone(),
                        ..f.commit()
                    },
                )
                .await
                .map(|_| ()),
        ] {
            assert_eq!(result, Err(AuthorizationError::Forbidden.into()));
        }
    }
    let mut absent = f.envelope.clone();
    absent.owner.run_id = RunId("absent".into());
    assert_eq!(
        f.store.start_queued_run(&absent).await,
        Err(AuthorizationError::Forbidden.into())
    );
    assert_eq!(
        f.store.cancel_queued_run(&absent).await,
        Err(AuthorizationError::Forbidden.into())
    );
    assert_eq!(
        f.store
            .commit_run_turn(
                &absent,
                &TurnCommit {
                    owner: absent.owner.clone(),
                    ..f.commit()
                }
            )
            .await
            .unwrap_err(),
        RepositoryError::Authorization(AuthorizationError::Forbidden)
    );
    // run 身份是查询资源：不存在 run 拒 Forbidden，其余四维 mismatch 是 StaleOwner。
    for field in 0..4 {
        let mut e = f.envelope.clone();
        match field {
            0 => e.owner.session_key.0.push('x'),
            1 => e.owner.session_lifetime_id.0.push('x'),
            2 => e.owner.run_generation.0 += 1,
            _ => e.owner.turn_id.0.push('x'),
        }
        assert_eq!(
            f.store.start_queued_run(&e).await,
            Err(AuthorizationError::StaleOwner.into())
        );
        assert_eq!(
            f.store.cancel_queued_run(&e).await,
            Err(AuthorizationError::StaleOwner.into())
        );
        assert_eq!(
            f.store
                .commit_run_turn(
                    &e,
                    &TurnCommit {
                        owner: e.owner.clone(),
                        ..f.commit()
                    }
                )
                .await
                .unwrap_err(),
            RepositoryError::Authorization(AuthorizationError::StaleOwner)
        );
    }
    assert_eq!(f.clock(), before);
    assert_eq!(
        f.store
            .read_run(&f.envelope.owner.run_id)
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Running
    );
}
#[tokio::test]
async fn execution_terminal_fault_rolls_back_original_transcript_turn_event_outbox_and_retry_is_idempotent()
 {
    let f = Fixture::new();
    let before = f.clock();
    let counts = |db: &Connection| {
        [
            "events",
            "event_outbox",
            "transcript_batches",
            "turn_commits",
        ]
        .map(|table| {
            db.query_row::<u64, _, _>(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
                .unwrap()
        })
    };
    let baseline = counts(&f.db);
    f.db.execute_batch("CREATE TRIGGER fail_execution_terminal BEFORE INSERT ON events WHEN NEW.event='terminal' BEGIN SELECT RAISE(ABORT,'PRIVATE_SQL_EXECUTION_FAILURE'); END;").unwrap();
    let error = f
        .store
        .commit_run_turn(&f.envelope, &f.commit())
        .await
        .unwrap_err();
    assert_eq!(
        error,
        RepositoryError::Coordination(CoordinationError::StorageUnavailable)
    );
    assert!(!error.to_string().contains("PRIVATE_SQL"));
    assert_eq!(f.clock(), before);
    assert_eq!(counts(&f.db), baseline);
    assert_eq!(
        f.db.query_row::<String, _, _>(
            "SELECT status FROM turns WHERE run_id=?1",
            [&f.envelope.owner.run_id.0],
            |r| r.get(0)
        )
        .unwrap(),
        "running"
    );
    f.db.execute_batch("DROP TRIGGER fail_execution_terminal;")
        .unwrap();
    let done = f
        .store
        .commit_run_turn(&f.envelope, &f.commit())
        .await
        .unwrap();
    let after = f.clock();
    assert_eq!(done.status, RunStatus::Completed);
    let committed = counts(&f.db);
    let reopened = RunStore::open(&f.root.join("runtime.sqlite3")).unwrap();
    let duplicate = reopened
        .commit_run_turn(&f.envelope, &f.commit())
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(done).unwrap(),
        serde_json::to_value(duplicate).unwrap()
    );
    assert_eq!(counts(&f.db), committed);
    assert_eq!(f.clock(), after);
    let changed = TurnCommit {
        content: Some("changed".into()),
        ..f.commit()
    };
    assert_eq!(
        reopened
            .commit_run_turn(&f.envelope, &changed)
            .await
            .unwrap_err(),
        RepositoryError::Coordination(CoordinationError::Conflict)
    );
    assert_eq!(f.clock(), after);
}
#[tokio::test]
async fn execution_unknown_provider_is_settled_in_original_terminal_transaction_without_replay() {
    let f = Fixture::new();
    let attempt = ProviderAttempt {
        attempt_id: "execution-unknown".into(),
        run_id: f.envelope.owner.run_id.clone(),
        round: 1,
        candidate_index: 0,
        provider_profile_id: "frozen".into(),
        api_type: ApiType::Ollama,
        model: "frozen".into(),
        status: AttemptStatus::Started,
        error_kind: None,
        diagnostic: None,
        retry_after_ms: None,
        stream_committed: false,
        started_at_ms: 1,
        first_event_at_ms: None,
        finished_at_ms: None,
        usage: None,
    };
    f.store.start_provider_attempt(&attempt).unwrap();
    let unknown = f
        .store
        .commit_run_turn(&f.envelope, &f.commit())
        .await
        .unwrap();
    assert_eq!(unknown.status, RunStatus::UnknownAfterRestart);
    assert!(unknown.content.is_none());
    assert_eq!(unknown.error_code, Some(-32002));
    let attempts = f.store.provider_attempts(&f.envelope.owner.run_id).unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].status, AttemptStatus::OutcomeUnknown);
    let before = f.clock();
    let reopened = RunStore::open(&f.root.join("runtime.sqlite3")).unwrap();
    let duplicate = TurnCommit {
        status: unknown.status,
        content: unknown.content.clone(),
        error: Some((
            unknown.error_code.unwrap(),
            unknown.error_message.clone().unwrap(),
        )),
        owner: f.envelope.owner.clone(),
    };
    assert_eq!(
        reopened
            .commit_run_turn(&f.envelope, &duplicate)
            .await
            .unwrap()
            .status,
        RunStatus::UnknownAfterRestart
    );
    assert_eq!(f.clock(), before);
    assert_eq!(
        reopened
            .commit_run_turn(&f.envelope, &f.commit())
            .await
            .unwrap_err(),
        RepositoryError::Coordination(CoordinationError::Conflict)
    );
}

fn snapshot() -> RunSnapshot {
    RunSnapshot {
        workflow: None,
        installed_generation:None,
        billing_policy: None,
            artifact_store_id: None,
        entry_channel: HookChannel::DaemonRpc,
        route: None,
        tools: vec![],
        cwd: ".".into(),
        permission_mode: "risk".into(),
        sandbox_requested: "native".into(),
        sandbox_effective: "native".into(),
        sandbox_notice: None,
        docker_image: None,
        delegation_context: None,
        context_read_only: false,
        context_token_budget: 4096,
        context_policy_fingerprint: None,
        tool_catalog_digest: "empty".into(),
        memory_entry_budget: 8,
        memory_token_budget: 1024,
        max_tool_calls: None,
        config_generation: 0,
    }
}
fn request(store: &RunStore, n: u64) -> AuthorizationRequest {
    let identity = store
        .identity_readback(
            &TenantContext::local(),
            &"local-subject".to_owned().try_into().unwrap(),
        )
        .unwrap();
    AuthorizationRequest {
        context: agent_identity::local_context(&identity, RequestId::Number(n)).unwrap(),
        expected_policy: identity.policy.version,
        action: Action::SessionPrompt,
        attributes: AuthorizationAttributes {
            workspace: ".".into(),
            entry_channel: HookChannel::DaemonRpc,
            tool_effect: None,
            timestamp_ms: 100,
            network: NetworkBoundary::None,
        },
    }
}
fn admission(n: u64) -> RunAdmission {
    RunAdmission {
        workflow_node: None,
        plan_execution: None,
        session_key: SessionKey("execution".into()),
        expected_lifetime: None,
        request_id: RequestId::Number(n),
        input: "运行".into(),
        mode: AdmissionMode::Queue,
    }
}
#[tokio::test]
async fn execution_admission_two_original_connections_freeze_one_owner_and_one_request_fact() {
    let f = Fixture::new();
    let second = RunStore::open(&f.root.join("runtime.sqlite3")).unwrap();
    let auth = request(&f.store, 2);
    let before = f.clock();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(3));
    let mut tasks = Vec::new();
    for store in [f.store.clone(), second.clone()] {
        let barrier = barrier.clone();
        let auth = auth.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            store
                .admit_run_authorized(
                    &admission(2),
                    &snapshot(),
                    &auth,
                    std::sync::Arc::new(agent_policy::PolicyEngine),
                )
                .await
        }));
    }
    barrier.wait().await;
    let a = tasks.remove(0).await.unwrap().unwrap();
    let b = tasks.remove(0).await.unwrap().unwrap();
    assert_eq!(a.1, b.1);
    assert_eq!(
        usize::from(matches!(a.0, Admission::New(_)))
            + usize::from(matches!(b.0, Admission::New(_))),
        1
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM runs", [], |r| r.get(0))
            .unwrap(),
        2
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM run_authorizations", [], |r| r.get(0))
            .unwrap(),
        1
    );
    assert!(f.clock() > before);
    let after = f.clock();
    let duplicate = f
        .store
        .admit_run_authorized(
            &admission(2),
            &snapshot(),
            &auth,
            std::sync::Arc::new(agent_policy::PolicyEngine),
        )
        .await
        .unwrap();
    assert!(matches!(duplicate.0, Admission::Existing(_)));
    assert_eq!(duplicate.1, a.1);
    assert_eq!(f.clock(), after);
    assert_eq!(
        f.db.query_row::<u64, _, _>(
            "SELECT count(*) FROM event_outbox WHERE run_id=?1",
            [&a.1.run_id.0],
            |r| r.get(0)
        )
        .unwrap(),
        1
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>(
            "SELECT count(*) FROM events WHERE run_id=?1 AND event='user_message'",
            [&a.1.run_id.0],
            |r| r.get(0)
        )
        .unwrap(),
        1
    );
    let mut different = admission(2);
    different.input = "different".into();
    assert_eq!(
        f.store
            .admit_run_authorized(
                &different,
                &snapshot(),
                &auth,
                std::sync::Arc::new(agent_policy::PolicyEngine)
            )
            .await
            .unwrap_err(),
        RepositoryError::Coordination(CoordinationError::Conflict)
    );
    let e = EnterpriseOwnerEnvelope {
        scope: auth.context.scope.clone(),
        owner: a.1,
    };
    assert!(
        !f.store.start_queued_run(&e).await.unwrap(),
        "原前一个运行阻塞该排队run"
    );
    assert!(f.store.cancel_queued_run(&e).await.unwrap());
    let cancelled = f.clock();
    assert!(!second.cancel_queued_run(&e).await.unwrap());
    assert_eq!(f.clock(), cancelled);
    assert_eq!(
        second.read_run(&e.owner.run_id).unwrap().unwrap().status,
        RunStatus::Cancelled
    );
}
#[tokio::test]
async fn execution_queued_mutation_rechecks_original_revocation_and_writer_expiry_without_reacquiring()
 {
    let f = Fixture::new();
    let auth = request(&f.store, 2);
    let (_, owner) = f
        .store
        .admit_run_authorized(
            &admission(2),
            &snapshot(),
            &auth,
            std::sync::Arc::new(agent_policy::PolicyEngine),
        )
        .await
        .unwrap();
    let e = EnterpriseOwnerEnvelope {
        scope: auth.context.scope,
        owner,
    };
    let before = f.clock();
    f.db.execute("UPDATE principals SET revocation_epoch=1", [])
        .unwrap();
    assert_eq!(
        f.store.start_queued_run(&e).await,
        Err(AuthorizationError::Revoked.into())
    );
    assert_eq!(
        f.store.cancel_queued_run(&e).await,
        Err(AuthorizationError::Revoked.into())
    );
    assert_eq!(f.clock(), before);
    f.db.execute("UPDATE principals SET revocation_epoch=0", [])
        .unwrap();
    assert!(
        matches!(
            RunStore::open(&f.root.join("runtime.sqlite3"))
                .unwrap()
                .enable_session_writer(),
            Err(RuntimeError::Coordination(CoordinationError::Conflict))
        ),
        "有活legacy运行不能靠开启开关冒充安全接管"
    );
    let writer_path = f.root.join("native-writer.sqlite3");
    let writer = RunStore::open(&writer_path)
        .unwrap()
        .enable_session_writer()
        .unwrap();
    let writer_db = Connection::open(writer_path).unwrap();
    // writer 从原空库准入时绑定，过期 proof 不得由任一 mutation 重新 acquire。
    let auth = request(&writer, 3);
    let (_, owner) = writer
        .admit_run_authorized(
            &admission(3),
            &snapshot(),
            &auth,
            std::sync::Arc::new(agent_policy::PolicyEngine),
        )
        .await
        .unwrap();
    let e = EnterpriseOwnerEnvelope {
        scope: auth.context.scope,
        owner,
    };
    writer_db
        .execute("UPDATE session_writer_leases SET expires_at_ms=0", [])
        .unwrap();
    let epoch = writer_db
        .query_row::<u64, _, _>("SELECT epoch FROM session_writer_leases", [], |r| r.get(0))
        .unwrap();
    let before = writer_db
        .query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
        .unwrap();
    for result in [
        writer.clone().start_queued_run(&e).await,
        writer.cancel_queued_run(&e).await,
    ] {
        assert_eq!(result, Err(CoordinationError::StaleLease.into()));
    }
    assert_eq!(
        writer
            .commit_run_turn(
                &e,
                &TurnCommit {
                    owner: e.owner.clone(),
                    status: RunStatus::Completed,
                    content: Some("late".into()),
                    error: None
                }
            )
            .await
            .unwrap_err(),
        RepositoryError::Coordination(CoordinationError::StaleLease)
    );
    assert_eq!(
        writer_db
            .query_row::<u64, _, _>("SELECT epoch FROM session_writer_leases", [], |r| r.get(0))
            .unwrap(),
        epoch
    );
    assert_eq!(
        writer_db
            .query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap(),
        before
    );
}
#[tokio::test]
async fn execution_admission_rechecks_server_time_and_rolls_back_original_authorization_and_queue_on_fault()
 {
    let f = Fixture::new();
    let auth = request(&f.store, 2);
    let before = f.clock();
    f.db.execute_batch("CREATE TRIGGER fail_execution_queue BEFORE INSERT ON queued_messages BEGIN SELECT RAISE(ABORT,'PRIVATE_QUEUE_FAILURE'); END;").unwrap();
    let result = f
        .store
        .admit_run_authorized(
            &admission(2),
            &snapshot(),
            &auth,
            std::sync::Arc::new(agent_policy::PolicyEngine),
        )
        .await
        .unwrap_err();
    assert_eq!(
        result,
        RepositoryError::Coordination(CoordinationError::StorageUnavailable)
    );
    assert_eq!(f.clock(), before);
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM runs", [], |r| r.get(0))
            .unwrap(),
        1
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM run_authorizations", [], |r| r.get(0))
            .unwrap(),
        0
    );
    f.db.execute_batch("DROP TRIGGER fail_execution_queue;")
        .unwrap();
    let mut policy = PolicyDocument::local();
    policy.version = "execution-expired".to_owned().try_into().unwrap();
    policy.conditions.expires_at_ms = Some(1000);
    f.db.execute(
        "INSERT INTO identity_policies VALUES('local-tenant','local-project','local-agent',?1,?2)",
        rusqlite::params![
            policy.version.as_str(),
            serde_json::to_string(&policy).unwrap()
        ],
    )
    .unwrap();
    f.db.execute(
        "UPDATE identity_policy_heads SET version=?1",
        [policy.version.as_str()],
    )
    .unwrap();
    let auth = request(&f.store, 2);
    let identity = f
        .store
        .identity_readback(&TenantContext::local(), &auth.context.principal.subject_id)
        .unwrap();
    assert!(agent_policy::PolicyEngine.verify(&auth, &identity).is_ok());
    assert_eq!(
        f.store
            .admit_run_authorized(
                &admission(2),
                &snapshot(),
                &auth,
                std::sync::Arc::new(agent_policy::PolicyEngine)
            )
            .await
            .unwrap_err(),
        RepositoryError::Authorization(AuthorizationError::Forbidden)
    );
    assert_eq!(f.clock(), before);
    let mut huge = admission(2);
    huge.input = "x".repeat(65537);
    assert_eq!(
        f.store
            .admit_run_authorized(
                &huge,
                &snapshot(),
                &auth,
                std::sync::Arc::new(agent_policy::PolicyEngine)
            )
            .await
            .unwrap_err(),
        RepositoryError::Coordination(CoordinationError::InvalidRequest)
    );
    assert_eq!(f.clock(), before);
}

#[tokio::test]
async fn execution_terminal_encoding_budget_rejects_large_and_escaped_content_without_committing() {
    let f = Fixture::new();
    let before = f.clock();
    for content in ["x".repeat(4 * 1024 * 1024), "\0".repeat(750_000)] {
        let oversized = TurnCommit {
            content: Some(content),
            ..f.commit()
        };
        assert_eq!(
            f.store
                .commit_run_turn(&f.envelope, &oversized)
                .await
                .unwrap_err(),
            RepositoryError::Coordination(CoordinationError::InvalidRequest)
        );
        assert_eq!(f.clock(), before);
        assert_eq!(
            f.store
                .read_run(&f.envelope.owner.run_id)
                .unwrap()
                .unwrap()
                .status,
            RunStatus::Running
        );
    }
}
