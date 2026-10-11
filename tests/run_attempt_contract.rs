//! 原 Provider attempt 与 run unknown 关系的真实业务合同。
use agent_core::*;
use agent_storage::{RunStore, RuntimeError};
use rusqlite::Connection;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    root: PathBuf,
    store: RunStore,
    db: Connection,
    attempt: ProviderAttempt,
}
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "ma-attempt-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let path = root.join("runtime.sqlite3");
        let store = RunStore::open(&path).unwrap();
        let Admission::New(run) = store
            .admit_with_route(
                SessionKey("attempt".into()),
                RequestId::Number(1),
                "input",
                AdmissionMode::Queue,
                None,
            )
            .unwrap()
        else {
            panic!("new")
        };
        store.try_start_queued(&run.run_id).unwrap();
        let attempt = ProviderAttempt {
            attempt_id: "original-attempt".into(),
            run_id: run.run_id,
            round: 1,
            candidate_index: 0,
            provider_profile_id: "frozen-profile".into(),
            api_type: ApiType::Ollama,
            model: "frozen-model".into(),
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
        store.start_provider_attempt(&attempt).unwrap();
        Self {
            db: Connection::open(path).unwrap(),
            root,
            store,
            attempt,
        }
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
fn immutable_identity(dimension: usize) {
    let f = Fixture::new();
    let before = f.clock();
    let mut completed = f.attempt.clone();
    completed.status = AttemptStatus::Succeeded;
    completed.finished_at_ms = Some(2);
    match dimension {
        0 => completed.provider_profile_id = "other-profile".into(),
        1 => completed.api_type = ApiType::OpenaiChat,
        2 => completed.model = "other-model".into(),
        _ => completed.started_at_ms = 0,
    }
    let result = f.store.finish_provider_attempt(&completed);
    assert!(
        matches!(
            result,
            Err(RuntimeError::Coordination(CoordinationError::Conflict))
        ),
        "原 Provider 身份维度 {dimension} 不能在终态改写，实际 {result:?}"
    );
    assert_eq!(f.clock(), before);
    let original = f.store.provider_attempts(&f.attempt.run_id).unwrap();
    assert_eq!(
        serde_json::to_value(&original[0]).unwrap(),
        serde_json::to_value(&f.attempt).unwrap()
    );
}
#[test]
fn provider_terminal_cannot_rewrite_original_profile() {
    immutable_identity(0);
}
#[test]
fn provider_terminal_cannot_rewrite_original_api() {
    immutable_identity(1);
}
#[test]
fn provider_terminal_cannot_rewrite_original_model() {
    immutable_identity(2);
}
#[test]
fn provider_terminal_cannot_rewrite_original_start_time() {
    immutable_identity(3);
}
#[test]
fn provider_outcome_unknown_cannot_be_projected_as_successful_run() {
    let f = Fixture::new();
    let mut unknown = f.attempt.clone();
    unknown.status = AttemptStatus::OutcomeUnknown;
    unknown.finished_at_ms = Some(2);
    f.store.finish_provider_attempt(&unknown).unwrap();
    let result = f
        .store
        .finish(
            &f.attempt.run_id,
            RunStatus::Completed,
            Some("不应成功"),
            None,
        )
        .unwrap();
    assert_eq!(
        result.status,
        RunStatus::UnknownAfterRestart,
        "已持久化 Provider 未知结果不能被 canonical run 结算成成功"
    );
    assert!(result.content.is_none());
    assert_eq!(
        f.store.provider_attempts(&f.attempt.run_id).unwrap()[0].status,
        AttemptStatus::OutcomeUnknown
    );
}

use agent_storage::SessionQuery;
use agent_storage_ports::RunAttemptRepository;
impl Fixture {
    fn envelope(&self) -> EnterpriseOwnerEnvelope {
        EnterpriseOwnerEnvelope {
            scope: TenantContext::local(),
            owner: self.store.run_owner(&self.attempt.run_id).unwrap(),
        }
    }
}
#[tokio::test]
async fn native_attempt_all_methods_reject_foreign_scope_stale_owner_and_absent_run_before_results()
{
    let f = Fixture::new();
    let before = f.clock();
    for field in 0..4 {
        let mut e = f.envelope();
        let unknown = "foreign".to_owned();
        match field {
            0 => e.scope.organization_id = unknown.try_into().unwrap(),
            1 => e.scope.tenant_id = unknown.try_into().unwrap(),
            2 => e.scope.project_id = unknown.try_into().unwrap(),
            _ => e.scope.agent_uid = unknown.try_into().unwrap(),
        }
        let mut finished = f.attempt.clone();
        finished.status = AttemptStatus::Succeeded;
        finished.finished_at_ms = Some(2);
        for result in [
            f.store.start_run_attempt(&e, &f.attempt).await,
            f.store.finish_run_attempt(&e, &finished).await,
            f.store.read_attempt_usage(&e).await.map(|_| ()),
            f.store
                .append_provider_event(&e, "provider_circuit", &serde_json::json!({"state":"open"}))
                .await
                .map(|_| ()),
        ] {
            assert_eq!(result, Err(AuthorizationError::Forbidden.into()));
        }
    }
    for field in 0..4 {
        let mut e = f.envelope();
        match field {
            0 => e.owner.session_key.0.push('x'),
            1 => e.owner.session_lifetime_id.0.push('x'),
            2 => e.owner.run_generation.0 += 1,
            _ => e.owner.turn_id.0.push('x'),
        }
        assert_eq!(
            f.store.start_run_attempt(&e, &f.attempt).await,
            Err(AuthorizationError::StaleOwner.into())
        );
        assert_eq!(
            f.store.read_attempt_usage(&e).await,
            Err(AuthorizationError::StaleOwner.into())
        );
    }
    let mut absent = f.envelope();
    absent.owner.run_id = RunId("absent".into());
    let mut attempt = f.attempt.clone();
    attempt.run_id = absent.owner.run_id.clone();
    assert_eq!(
        f.store.start_run_attempt(&absent, &attempt).await,
        Err(AuthorizationError::Forbidden.into())
    );
    assert_eq!(
        f.store.read_attempt_usage(&absent).await,
        Err(AuthorizationError::Forbidden.into())
    );
    assert_eq!(f.clock(), before);
}
#[tokio::test]
async fn native_attempt_replay_unknown_and_original_transaction_fault_preserve_one_marker_and_payload()
 {
    let f = Fixture::new();
    let e = f.envelope();
    let before = f.clock();
    f.store.start_run_attempt(&e, &f.attempt).await.unwrap();
    assert_eq!(f.clock(), before);
    f.db.execute_batch("CREATE TRIGGER fail_attempt_terminal BEFORE INSERT ON events WHEN NEW.event='provider_attempt_terminal' BEGIN SELECT RAISE(ABORT,'PRIVATE_ATTEMPT_SQL'); END;").unwrap();
    let mut completed = f.attempt.clone();
    completed.status = AttemptStatus::Succeeded;
    completed.finished_at_ms = Some(2);
    completed.usage = Some(ProviderUsage {
        input_tokens: Some(7),
        output_tokens: Some(3),
        ..Default::default()
    });
    let result = f
        .store
        .finish_run_attempt(&e, &completed)
        .await
        .unwrap_err();
    assert_eq!(
        result,
        RepositoryError::Coordination(CoordinationError::StorageUnavailable)
    );
    assert!(!result.to_string().contains("PRIVATE"));
    assert_eq!(f.clock(), before);
    assert_eq!(
        f.store.provider_attempts(&f.attempt.run_id).unwrap()[0].status,
        AttemptStatus::Started
    );
    f.db.execute_batch("DROP TRIGGER fail_attempt_terminal;")
        .unwrap();
    completed.status = AttemptStatus::OutcomeUnknown;
    f.store.finish_run_attempt(&e, &completed).await.unwrap();
    let after = f.clock();
    assert_eq!(
        f.store.read_attempt_usage(&e).await.unwrap(),
        completed.usage.clone().unwrap()
    );
    let reopened = RunStore::open(&f.root.join("runtime.sqlite3")).unwrap();
    reopened.finish_run_attempt(&e, &completed).await.unwrap();
    assert_eq!(f.clock(), after);
    assert_eq!(
        reopened.start_run_attempt(&e, &f.attempt).await,
        Err(CoordinationError::Conflict.into())
    );
    assert_eq!(f.clock(), after);
    assert_eq!(
        f.db.query_row::<u64, _, _>(
            "SELECT count(*) FROM events WHERE event='provider_attempt_started'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        1
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>(
            "SELECT count(*) FROM events WHERE event='provider_attempt_terminal'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        1
    );
    let result = reopened
        .finish(&f.attempt.run_id, RunStatus::Completed, Some("false"), None)
        .unwrap();
    assert_eq!(result.status, RunStatus::UnknownAfterRestart);
}
#[tokio::test]
async fn native_attempt_decoder_rejects_raw_identity_corruption_and_database_fault_without_sql_disclosure()
 {
    let f = Fixture::new();
    let e = f.envelope();
    let before = f.clock();
    let mut raw = serde_json::to_value(&f.attempt).unwrap();
    raw["round"] = serde_json::json!(99);
    f.db.execute(
        "UPDATE provider_attempts SET data_json=?1 WHERE attempt_id=?2",
        rusqlite::params![raw.to_string(), f.attempt.attempt_id],
    )
    .unwrap();
    let damaged = f.clock();
    assert!(damaged >= before);
    assert_eq!(
        f.store.read_attempt_usage(&e).await,
        Err(CoordinationError::Corrupt.into())
    );
    assert_eq!(
        f.store.start_run_attempt(&e, &f.attempt).await,
        Err(CoordinationError::Corrupt.into())
    );
    assert_eq!(f.clock(), damaged);
    f.db.execute_batch("ALTER TABLE provider_attempts RENAME TO private_attempt_fixture_fault;")
        .unwrap();
    let error = f.store.read_attempt_usage(&e).await.unwrap_err();
    assert_eq!(
        error,
        RepositoryError::Coordination(CoordinationError::StorageUnavailable)
    );
    assert!(!error.to_string().contains("provider_attempts"));
    assert_eq!(
        f.store
            .append_provider_event(&e, "terminal", &serde_json::json!({"status":"completed"}))
            .await,
        Err(CoordinationError::InvalidRequest.into())
    );
    assert_eq!(f.clock(), damaged);
}

#[tokio::test]
async fn native_provider_start_must_match_the_frozen_route_in_original_transaction() {
    let f = Fixture::new();
    let route = RouteSnapshot {
        candidates: vec![RouteCandidate {
            profile_id: f.attempt.provider_profile_id.clone(),
            api_type: f.attempt.api_type,
            model: f.attempt.model.clone(),
            base_url_sha256: "fixture".into(),
        }],
        retry_policy: RetryPolicy::default(),
        timeout_policy: TimeoutPolicy::default(),
        context_policy: ContextPolicySnapshot { token_budget: 1000 },
        config_generation: 1,
    };
    f.db.execute(
        "INSERT INTO run_routes(run_id,snapshot_json) VALUES(?1,?2)",
        rusqlite::params![f.attempt.run_id.0, serde_json::to_string(&route).unwrap()],
    )
    .unwrap();
    let before = f.clock();
    for dimension in 0..4 {
        let mut attempt = f.attempt.clone();
        attempt.attempt_id = format!("forged-{dimension}");
        match dimension {
            0 => attempt.provider_profile_id = "unfrozen".into(),
            1 => attempt.api_type = ApiType::AnthropicMessages,
            2 => attempt.model = "unfrozen".into(),
            _ => attempt.candidate_index = 1,
        }
        assert_eq!(
            f.store.start_run_attempt(&f.envelope(), &attempt).await,
            Err(CoordinationError::Conflict.into()),
            "route维度 {dimension}"
        );
        assert_eq!(f.clock(), before);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn native_provider_attempt_waiting_for_sqlite_keeps_async_timer_responsive() {
    let f = Fixture::new();
    let before = f.clock();
    let path = f.root.join("runtime.sqlite3");
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let db = Connection::open(path).unwrap();
        db.execute_batch("BEGIN IMMEDIATE;").unwrap();
        ready_tx.send(()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(300));
        db.execute_batch("COMMIT;").unwrap();
    });
    ready_rx.recv().unwrap();
    let begun = std::time::Instant::now();
    let envelope = f.envelope();
    let work = f.store.start_run_attempt(&envelope, &f.attempt);
    let (result, elapsed) = tokio::join!(work, async {
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        begun.elapsed()
    });
    holder.join().unwrap();
    result.unwrap();
    assert!(
        elapsed < std::time::Duration::from_millis(150),
        "timer阻塞 {elapsed:?}"
    );
    assert_eq!(f.clock(), before);
}

#[tokio::test]
async fn native_attempt_two_original_connections_commit_one_terminal_and_reject_conflicting_payload()
 {
    let f = Fixture::new();
    let second = RunStore::open(&f.root.join("runtime.sqlite3")).unwrap();
    let e = f.envelope();
    let mut completed = f.attempt.clone();
    completed.status = AttemptStatus::Succeeded;
    completed.finished_at_ms = Some(2);
    let mut different = completed.clone();
    different.status = AttemptStatus::Failed;
    let (a, b) = tokio::join!(
        f.store.finish_run_attempt(&e, &completed),
        second.finish_run_attempt(&e, &different)
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    for error in [a, b].into_iter().filter_map(Result::err) {
        assert_eq!(error, CoordinationError::Conflict.into());
    }
    assert_eq!(
        f.db.query_row::<u64, _, _>(
            "SELECT count(*) FROM events WHERE event='provider_attempt_terminal'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        1
    );
    let persisted = f
        .store
        .provider_attempts(&f.attempt.run_id)
        .unwrap()
        .remove(0);
    let before = f.clock();
    second.finish_run_attempt(&e, &persisted).await.unwrap();
    assert_eq!(f.clock(), before);
}

#[tokio::test]
async fn native_attempt_each_method_rechecks_revocation_and_expired_writer_without_new_epoch() {
    use agent_storage_ports::{IdentityReadRepository, RunExecutionRepository};
    for fault in ["revoked", "expired"] {
        let f = Fixture::new();
        let path = f.root.join("native.sqlite3");
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
            session_key: SessionKey("native".into()),
            expected_lifetime: None,
            request_id: RequestId::Number(1),
            input: "原事务复核".into(),
            mode: AdmissionMode::Queue,
        };
        let snapshot: RunSnapshot = serde_json::from_value(serde_json::json!({"cwd":".","tools":[],"permission_mode":"risk","sandbox_requested":"native","sandbox_effective":"native","context_read_only":false,"config_generation":0})).unwrap();
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
        let envelope = EnterpriseOwnerEnvelope {
            scope: TenantContext::local(),
            owner,
        };
        assert!(store.start_queued_run(&envelope).await.unwrap());
        let mut started = f.attempt.clone();
        started.run_id = envelope.owner.run_id.clone();
        store.start_run_attempt(&envelope, &started).await.unwrap();
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
        let mut finished = started.clone();
        finished.status = AttemptStatus::Succeeded;
        finished.finished_at_ms = Some(2);
        for result in [
            store.start_run_attempt(&envelope, &started).await,
            store.finish_run_attempt(&envelope, &finished).await,
            store.read_attempt_usage(&envelope).await.map(|_| ()),
            store
                .append_provider_event(&envelope, "provider_retry", &serde_json::json!({}))
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
    }
}
