//! 原请求捕获路径的真实隔离与完整合法行身份 RED。
use agent_core::*;
use agent_storage::{RequestRepository, RunStore, SessionLifecycle, SessionQuery};
use agent_storage_ports::{RunExecutionRepository, RunRequestRepository};
use rusqlite::{Connection, params};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    root: std::path::PathBuf,
    store: RunStore,
    capture: ProviderRequestCapture,
    context: ContextEnvelope,
    db: Connection,
}
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "ma-request-port-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let path = root.join("runtime.sqlite3");
        let (store, capture, context) = fixture(&path);
        Self {
            root,
            store,
            capture,
            context,
            db: Connection::open(path).unwrap(),
        }
    }
    fn envelope(&self) -> EnterpriseOwnerEnvelope {
        EnterpriseOwnerEnvelope {
            scope: TenantContext::local(),
            owner: self.capture.owner.clone(),
        }
    }
    fn foreign(&self) {
        self.db.execute_batch("INSERT INTO organizations VALUES('foreign');INSERT INTO tenants VALUES('foreign','foreign');INSERT INTO projects VALUES('foreign','foreign');INSERT INTO agents VALUES('foreign','foreign','foreign');UPDATE session_scopes SET tenant_id='foreign',project_id='foreign',agent_uid='foreign' WHERE session_id='request';").unwrap();
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
fn fixture(path: &std::path::Path) -> (RunStore, ProviderRequestCapture, ContextEnvelope) {
    let store = RunStore::open(path).unwrap();
    let metadata = store.create_session(&SessionKey("request".into())).unwrap();
    let frozen:RunSnapshot=serde_json::from_value(serde_json::json!({"tools":[],"route":null,"cwd":".","permission_mode":"risk_approval","sandbox_requested":"native","sandbox_effective":"native","context_read_only":false,"max_tool_calls":8,"config_generation":0,"tool_catalog_digest":format!("{:x}",Sha256::digest(b"[]")),"context_policy_fingerprint":"4096:12:60:85","context_token_budget":4096})).unwrap();
    let Admission::New(run) = store
        .admit_run(
            &RunAdmission {
                workflow_node: None,
                session_key: metadata.key,
                expected_lifetime: Some(metadata.lifetime),
                request_id: RequestId::Number(1),
                input: "请求".into(),
                mode: AdmissionMode::Queue,
                plan_execution: None,
            },
            &frozen,
        )
        .unwrap()
    else {
        panic!("new")
    };
    store.try_start_queued(&run.run_id).unwrap();
    let owner = store.run_owner(&run.run_id).unwrap();
    let (snapshot, revision, calibration) = store
        .request_context_source(&owner, "primary", "identity")
        .unwrap();
    let source = ContextSource {
        lifetime: snapshot.lifetime,
        source_start: TranscriptSeq(0),
        source_end: snapshot.revision,
        prefix_digest: format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&snapshot.messages).unwrap())
        ),
        generation: snapshot.projection_generation,
        policy_fingerprint: "4096:12:60:85".into(),
        pressure_route: "primary".into(),
        summary_route: "primary".into(),
    };
    let mut capture = ProviderRequestCapture {
        schema_version: 1,
        capture_id: "one".into(),
        owner,
        round: 1,
        candidate_index: 0,
        snapshot_revision: revision,
        input: ProviderRequestInput {
            purpose: ProviderRequestPurpose::Model,
            source: source.clone(),
            messages: vec![Message::text(Role::User, "材料")],
            tools: vec![],
            catalog: vec![],
            route: "primary".into(),
            provider_identity: "identity".into(),
            images: false,
            tool_calls: true,
            budget: 4096,
            calibration,
        },
        policy_fingerprint: format!("{:x}", Sha256::digest(serde_json::to_vec(&frozen).unwrap())),
        request_digest: "a".repeat(64),
    };
    let envelope = ContextEnvelope {
        source,
        route: "primary".into(),
        provider_identity: "identity".into(),
        tool_catalog_digest: format!("{:x}", Sha256::digest(b"[]")),
        stable_tokens: 8,
        history_tokens: 0,
        retrieved_tokens: 0,
        overlay_tokens: 8,
        calibrated_input_tokens: 16,
        output_reserve: 409,
        budget: 4096,
    };
    let assembled = agent_context::assemble_request(&capture.input).unwrap();
    capture.request_digest = assembled.request_digest;
    let _ = envelope;
    (store, capture, assembled.envelope)
}

#[tokio::test]
async fn source_read_denies_foreign_scope() {
    let f = Fixture::new();
    f.foreign();
    let before = f.clock();
    assert!(
        f.store
            .read_run_request_source(&f.envelope(), "primary", "identity")
            .await
            .is_err()
    );
    assert_eq!(f.clock(), before);
}
#[tokio::test]
async fn capture_write_denies_foreign_scope() {
    let f = Fixture::new();
    f.foreign();
    let before = f.clock();
    assert!(
        f.store
            .capture_run_request(&f.envelope(), &f.capture, &f.context)
            .await
            .is_err()
    );
    assert_eq!(f.clock(), before);
}
#[tokio::test]
async fn readback_denies_foreign_scope() {
    let f = Fixture::new();
    f.store
        .capture_run_request(&f.envelope(), &f.capture, &f.context)
        .await
        .unwrap();
    f.foreign();
    let before = f.clock();
    assert!(
        f.store
            .read_provider_request(
                &TenantContext::local(),
                &f.capture.owner.session_key,
                &f.capture.owner.session_lifetime_id,
                None,
                None
            )
            .await
            .is_err()
    );
    assert_eq!(f.clock(), before);
}
#[tokio::test]
async fn complete_valid_json_cannot_override_selected_sql_capture_identity() {
    let f = Fixture::new();
    f.store
        .capture_run_request(&f.envelope(), &f.capture, &f.context)
        .await
        .unwrap();
    f.db.execute(
        "UPDATE provider_requests SET capture_id='other' WHERE run_id=?1",
        params![f.capture.owner.run_id.0],
    )
    .unwrap();
    let before = f.clock();
    assert!(
        f.store
            .read_provider_request(
                &TenantContext::local(),
                &f.capture.owner.session_key,
                &f.capture.owner.session_lifetime_id,
                None,
                None
            )
            .await
            .is_err()
    );
    assert_eq!(f.clock(), before);
}

#[tokio::test]
async fn fixed_scope_and_all_owner_dimensions_reject_both_methods_without_writes() {
    let f = Fixture::new();
    let before = f.clock();
    for dim in 0..9 {
        let mut e = f.envelope();
        match dim {
            0 => e.scope.organization_id = "other".to_owned().try_into().unwrap(),
            1 => e.scope.tenant_id = "other".to_owned().try_into().unwrap(),
            2 => e.scope.project_id = "other".to_owned().try_into().unwrap(),
            3 => e.scope.agent_uid = "other".to_owned().try_into().unwrap(),
            4 => e.owner.session_key = SessionKey("other".into()),
            5 => e.owner.session_lifetime_id = SessionLifetimeId("other".into()),
            6 => e.owner.run_id = RunId("other".into()),
            7 => e.owner.run_generation.0 += 1,
            _ => e.owner.turn_id = TurnId("other".into()),
        }
        assert!(
            f.store
                .read_run_request_source(&e, "primary", "identity")
                .await
                .is_err(),
            "source {dim}"
        );
        assert!(
            f.store
                .capture_run_request(&e, &f.capture, &f.context)
                .await
                .is_err(),
            "capture {dim}"
        );
        assert_eq!(f.clock(), before);
    }
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM provider_requests", [], |r| r.get(0))
            .unwrap(),
        0
    );
}
#[tokio::test]
async fn capture_fixed_owner_mismatch_and_terminal_source_reject_but_readback_stays_readonly() {
    let f = Fixture::new();
    let e = f.envelope();
    let before = f.clock();
    for dim in 0..5 {
        let mut c = f.capture.clone();
        match dim {
            0 => c.owner.session_key.0.push('x'),
            1 => c.owner.session_lifetime_id.0.push('x'),
            2 => c.owner.run_id.0.push('x'),
            3 => c.owner.run_generation.0 += 1,
            _ => c.owner.turn_id.0.push('x'),
        }
        assert_eq!(
            f.store.capture_run_request(&e, &c, &f.context).await,
            Err(AuthorizationError::StaleOwner.into())
        );
        assert_eq!(f.clock(), before);
    }
    f.store
        .capture_run_request(&e, &f.capture, &f.context)
        .await
        .unwrap();
    f.store
        .commit_run_turn(
            &e,
            &TurnCommit {
                owner: e.owner.clone(),
                status: RunStatus::Completed,
                content: Some("done".into()),
                error: None,
            },
        )
        .await
        .unwrap();
    let before = f.clock();
    assert!(matches!(
        f.store
            .read_run_request_source(&e, "primary", "identity")
            .await,
        Err(RepositoryError::Coordination(CoordinationError::Conflict))
    ));
    assert_eq!(
        f.store
            .capture_run_request(&e, &f.capture, &f.context)
            .await,
        Err(CoordinationError::Conflict.into())
    );
    for _ in 0..3 {
        let (c, _) = f
            .store
            .read_provider_request(
                &e.scope,
                &e.owner.session_key,
                &e.owner.session_lifetime_id,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(c.capture_id, f.capture.capture_id);
    }
    assert_eq!(f.clock(), before);
}
#[tokio::test]
async fn original_capture_and_ledger_are_atomic_idempotent_and_fault_rolls_back() {
    let f = Fixture::new();
    let e = f.envelope();
    f.store
        .capture_run_request(&e, &f.capture, &f.context)
        .await
        .unwrap();
    let before = f.clock();
    f.store
        .capture_run_request(&e, &f.capture, &f.context)
        .await
        .unwrap();
    assert_eq!(f.clock(), before);
    let mut different = f.capture.clone();
    different.input.messages[0].content = Some("different".into());
    assert_eq!(
        f.store
            .capture_run_request(&e, &different, &f.context)
            .await,
        Err(CoordinationError::Conflict.into())
    );
    f.db.execute_batch("CREATE TRIGGER ledger_fault BEFORE INSERT ON context_ledgers BEGIN SELECT RAISE(ABORT,'ledger fault'); END;").unwrap();
    different = f.capture.clone();
    different.capture_id = "rollback".into();
    different.round = 2;
    assert_eq!(
        f.store
            .capture_run_request(&e, &different, &f.context)
            .await,
        Err(CoordinationError::StorageUnavailable.into())
    );
    assert_eq!(f.clock(), before);
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM provider_requests", [], |r| r.get(0))
            .unwrap(),
        1
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM context_ledgers", [], |r| r.get(0))
            .unwrap(),
        1
    );
    let reopened = RunStore::open(&f.root.join("runtime.sqlite3")).unwrap();
    let (c, _) = reopened
        .read_provider_request(
            &e.scope,
            &e.owner.session_key,
            &e.owner.session_lifetime_id,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(c.capture_id, "one");
    assert_eq!(f.clock(), before);
}
#[tokio::test]
async fn complete_valid_capture_rejects_every_sql_identity_dimension_and_hashed_json_corruption() {
    let f = Fixture::new();
    let e = f.envelope();
    f.store
        .capture_run_request(&e, &f.capture, &f.context)
        .await
        .unwrap();
    let original = serde_json::to_value(&f.capture).unwrap();
    let before = f.clock();
    for field in ["capture_id", "round", "candidate_index"] {
        let sql = match field {
            "capture_id" => "UPDATE provider_requests SET capture_id='other'",
            "round" => "UPDATE provider_requests SET round=2",
            _ => "UPDATE provider_requests SET candidate_index=1",
        };
        f.db.execute_batch(sql).unwrap();
        assert!(
            matches!(
                f.store
                    .read_provider_request(
                        &e.scope,
                        &e.owner.session_key,
                        &e.owner.session_lifetime_id,
                        None,
                        None
                    )
                    .await,
                Err(RepositoryError::Coordination(CoordinationError::Corrupt))
            ),
            "SQL {field}"
        );
        f.db.execute_batch(
            "UPDATE provider_requests SET capture_id='one',round=1,candidate_index=0",
        )
        .unwrap();
    }
    for field in [
        "run_id",
        "session_key",
        "session_lifetime_id",
        "turn_id",
        "run_generation",
    ] {
        let mut value = original.clone();
        value["owner"][field] = if field == "run_generation" {
            serde_json::json!(999)
        } else {
            serde_json::json!("other")
        };
        let raw = value.to_string();
        let digest = format!("{:x}", Sha256::digest(raw.as_bytes()));
        f.db.execute(
            "UPDATE provider_requests SET capture_json=?1,capture_digest=?2",
            params![raw, digest],
        )
        .unwrap();
        assert!(
            matches!(
                f.store
                    .read_provider_request(
                        &e.scope,
                        &e.owner.session_key,
                        &e.owner.session_lifetime_id,
                        None,
                        None
                    )
                    .await,
                Err(RepositoryError::Coordination(CoordinationError::Corrupt))
            ),
            "JSON {field}"
        );
    }
    assert_eq!(f.clock(), before);
}
#[tokio::test]
async fn complete_legal_capture_from_previous_run_cannot_replace_another_run_row() {
    let f = Fixture::new();
    let e = f.envelope();
    f.store
        .capture_run_request(&e, &f.capture, &f.context)
        .await
        .unwrap();
    let (raw, digest): (String, String) =
        f.db.query_row(
            "SELECT capture_json,capture_digest FROM provider_requests",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    let frozen = f
        .store
        .read_run_request_source(&e, "primary", "identity")
        .await
        .unwrap()
        .frozen;
    f.store
        .commit_run_turn(
            &e,
            &TurnCommit {
                owner: e.owner.clone(),
                status: RunStatus::Completed,
                content: Some("done".into()),
                error: None,
            },
        )
        .await
        .unwrap();
    let Admission::New(run) = f
        .store
        .admit_run(
            &RunAdmission {
                workflow_node: None,
                session_key: e.owner.session_key.clone(),
                expected_lifetime: Some(e.owner.session_lifetime_id.clone()),
                request_id: RequestId::Number(2),
                input: "second".into(),
                mode: AdmissionMode::Queue,
                plan_execution: None,
            },
            &frozen,
        )
        .unwrap()
    else {
        panic!("new")
    };
    f.store.try_start_queued(&run.run_id).unwrap();
    let owner = f.store.run_owner(&run.run_id).unwrap();
    let second = EnterpriseOwnerEnvelope {
        scope: e.scope.clone(),
        owner,
    };
    let source = f
        .store
        .read_run_request_source(&second, "primary", "identity")
        .await
        .unwrap();
    let mut c = f.capture.clone();
    c.owner = second.owner.clone();
    c.capture_id = "two".into();
    c.input.source.source_end = source.snapshot.revision;
    c.input.source.prefix_digest = agent_context::digest(&source.snapshot.messages).unwrap();
    let mut context = f.context.clone();
    context.source = c.input.source.clone();
    f.store
        .capture_run_request(&second, &c, &context)
        .await
        .unwrap();
    // 保持旧 JSON 完全合法及正确摘要；它确实属于同 session/lifetime 的另一原 run。
    f.db.execute(
        "UPDATE provider_requests SET capture_json=?1,capture_digest=?2 WHERE run_id=?3",
        params![raw, digest, second.owner.run_id.0],
    )
    .unwrap();
    let before = f.clock();
    assert!(matches!(
        f.store
            .read_provider_request(
                &e.scope,
                &e.owner.session_key,
                &e.owner.session_lifetime_id,
                None,
                None
            )
            .await,
        Err(RepositoryError::Coordination(CoordinationError::Corrupt))
    ));
    assert_eq!(f.clock(), before);
}
#[tokio::test]
async fn two_connections_capture_same_id_once_and_conflicting_payload_never_overwrites() {
    let f = Fixture::new();
    let e = f.envelope();
    let other = RunStore::open(&f.root.join("runtime.sqlite3")).unwrap();
    let (a, b) = tokio::join!(
        f.store.capture_run_request(&e, &f.capture, &f.context),
        other.capture_run_request(&e, &f.capture, &f.context)
    );
    a.unwrap();
    b.unwrap();
    let before = f.clock();
    let mut conflict = f.capture.clone();
    conflict.input.messages[0].content = Some("conflict".into());
    let (a, b) = tokio::join!(
        f.store.capture_run_request(&e, &conflict, &f.context),
        other.capture_run_request(&e, &f.capture, &f.context)
    );
    assert_eq!(a, Err(CoordinationError::Conflict.into()));
    b.unwrap();
    assert_eq!(f.clock(), before);
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM provider_requests", [], |r| r.get(0))
            .unwrap(),
        1
    );
}
#[tokio::test(flavor = "current_thread")]
async fn real_sqlite_lock_wait_keeps_timer_live_and_cancelled_awaiter_does_not_erase_capture() {
    let f = Fixture::new();
    let e = f.envelope();
    let path = f.root.join("runtime.sqlite3");
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let holder = std::thread::spawn(move || {
        let db = Connection::open(path).unwrap();
        db.execute_batch("BEGIN IMMEDIATE").unwrap();
        tx.send(()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(300));
        db.execute_batch("COMMIT").unwrap();
    });
    rx.recv().unwrap();
    let begin = std::time::Instant::now();
    let outcome = tokio::time::timeout(
        std::time::Duration::from_millis(25),
        f.store.capture_run_request(&e, &f.capture, &f.context),
    )
    .await;
    assert!(outcome.is_err());
    assert!(begin.elapsed() < std::time::Duration::from_millis(150));
    // 原 worker 仍完成自己的事务，取消 await 不能撤销已启动持久写入。
    holder.join().unwrap();
    let (capture, _) = f
        .store
        .read_provider_request(
            &e.scope,
            &e.owner.session_key,
            &e.owner.session_lifetime_id,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(capture.capture_id, "one");
}
#[tokio::test]
async fn input_and_saved_json_budgets_fail_closed_without_truncating_canonical_source() {
    let f = Fixture::new();
    let e = f.envelope();
    let before = f.clock();
    let mut huge = f.capture.clone();
    huge.input.messages[0].content = Some("x".repeat(16 * 1024 * 1024));
    assert_eq!(
        f.store.capture_run_request(&e, &huge, &f.context).await,
        Err(CoordinationError::InvalidRequest.into())
    );
    assert_eq!(f.clock(), before);
    f.store
        .capture_run_request(&e, &f.capture, &f.context)
        .await
        .unwrap();
    let before = f.clock();
    f.db.execute_batch(
        "UPDATE provider_requests SET capture_json=CAST(zeroblob(16777217) AS TEXT)",
    )
    .unwrap();
    assert!(matches!(
        f.store
            .read_provider_request(
                &e.scope,
                &e.owner.session_key,
                &e.owner.session_lifetime_id,
                None,
                None
            )
            .await,
        Err(RepositoryError::Coordination(CoordinationError::Corrupt))
    ));
    f.db.execute_batch(
        "UPDATE transcript_batches SET messages_json=CAST(zeroblob(16777217) AS TEXT)",
    )
    .unwrap();
    assert!(matches!(
        f.store
            .read_run_request_source(&e, "primary", "identity")
            .await,
        Err(RepositoryError::Coordination(
            CoordinationError::InvalidRequest
        ))
    ));
    assert_eq!(f.clock(), before);
}

#[tokio::test]
async fn many_valid_empty_transcript_batches_are_rejected_before_materializing_source() {
    let f = Fixture::new();
    let e = f.envelope();
    let revision: u64 =
        f.db.query_row(
            "SELECT revision FROM session_heads WHERE session_id='request'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let digest = format!("{:x}", Sha256::digest(b"[]"));
    f.db.execute("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<32769) INSERT INTO transcript_batches(lifetime,operation_id,start_seq,end_seq,messages_json,digest) SELECT ?1,'empty-'||i,?2,?2,'[]',?3 FROM n",params![e.owner.session_lifetime_id.0,revision,digest]).unwrap();
    let before = f.clock();
    assert_eq!(
        f.store
            .read_run_request_source(&e, "primary", "identity")
            .await
            .unwrap_err(),
        CoordinationError::InvalidRequest.into()
    );
    assert_eq!(f.clock(), before);
}

#[tokio::test]
async fn active_request_source_and_capture_recheck_revocation_and_expired_writer_without_new_epoch()
{
    use agent_storage_ports::IdentityReadRepository;
    for fault in ["revoked", "expired"] {
        let mut f = Fixture::new();
        let frozen = f
            .store
            .read_run_request_source(&f.envelope(), "primary", "identity")
            .await
            .unwrap()
            .frozen;
        let old = f.envelope();
        f.store
            .commit_run_turn(
                &old,
                &TurnCommit {
                    owner: old.owner.clone(),
                    status: RunStatus::Completed,
                    content: Some("fixture completed".into()),
                    error: None,
                },
            )
            .await
            .unwrap();
        f.store = f.store.clone().enable_session_writer().unwrap();
        let identity = f
            .store
            .read_identity(
                &TenantContext::local(),
                &"local-subject".to_owned().try_into().unwrap(),
            )
            .await
            .unwrap();
        let admission = RunAdmission {
            workflow_node: None,
            session_key: SessionKey("native-request".into()),
            expected_lifetime: None,
            request_id: RequestId::Number(7),
            input: "native".into(),
            mode: AdmissionMode::Queue,
            plan_execution: None,
        };
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
        let (_, owner) = f
            .store
            .admit_run_authorized(
                &admission,
                &frozen,
                &authorization,
                std::sync::Arc::new(agent_policy::PolicyEngine),
            )
            .await
            .unwrap();
        let e = EnterpriseOwnerEnvelope {
            scope: TenantContext::local(),
            owner,
        };
        f.store.start_queued_run(&e).await.unwrap();
        let source = f
            .store
            .read_run_request_source(&e, "primary", "identity")
            .await
            .unwrap();
        let mut c = f.capture.clone();
        c.owner = e.owner.clone();
        c.input.source.lifetime = e.owner.session_lifetime_id.clone();
        c.input.source.source_end = source.snapshot.revision;
        c.input.source.prefix_digest = agent_context::digest(&source.snapshot.messages).unwrap();
        c.policy_fingerprint = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&source.frozen).unwrap())
        );
        let mut context = f.context.clone();
        context.source = c.input.source.clone();
        f.store.capture_run_request(&e, &c, &context).await.unwrap();
        let expected: RepositoryError = if fault == "revoked" {
            f.db.execute_batch("UPDATE principals SET revocation_epoch=1")
                .unwrap();
            AuthorizationError::Revoked.into()
        } else {
            f.db.execute_batch("UPDATE session_writer_leases SET expires_at_ms=0")
                .unwrap();
            CoordinationError::StaleLease.into()
        };
        let before = f.clock();
        let epoch: u64 =
            f.db.query_row(
                "SELECT epoch FROM session_writer_leases WHERE session_key='native-request'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            f.store
                .read_run_request_source(&e, "primary", "identity")
                .await
                .unwrap_err(),
            expected
        );
        assert_eq!(
            f.store.capture_run_request(&e, &c, &context).await,
            Err(expected)
        );
        // 历史重建是只读查询，允许当前 scope 的读者读取，不能重获 writer。
        f.store
            .read_provider_request(
                &e.scope,
                &e.owner.session_key,
                &e.owner.session_lifetime_id,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(f.clock(), before);
        assert_eq!(
            f.db.query_row::<u64, _, _>(
                "SELECT epoch FROM session_writer_leases WHERE session_key='native-request'",
                [],
                |r| r.get(0)
            )
            .unwrap(),
            epoch
        );
    }
}

#[tokio::test]
async fn native_capture_must_match_frozen_route_before_persisting_material() {
    let f = Fixture::new();
    let e = f.envelope();
    let mut frozen = f
        .store
        .read_run_request_source(&e, "primary", "identity")
        .await
        .unwrap()
        .frozen;
    let route = RouteSnapshot {
        candidates: vec![RouteCandidate {
            profile_id: "primary".into(),
            api_type: ApiType::Ollama,
            model: "frozen".into(),
            base_url_sha256: "fixture".into(),
        }],
        retry_policy: RetryPolicy::default(),
        timeout_policy: TimeoutPolicy::default(),
        context_policy: ContextPolicySnapshot { token_budget: 4096 },
        config_generation: 0,
    };
    frozen.route = Some(route.clone());
    f.db.execute(
        "UPDATE run_snapshots SET snapshot_json=?1",
        [serde_json::to_string(&frozen).unwrap()],
    )
    .unwrap();
    let mut capture = f.capture.clone();
    capture.policy_fingerprint =
        format!("{:x}", Sha256::digest(serde_json::to_vec(&frozen).unwrap()));
    let before = f.clock();
    // 原冻结catalog/policy摘要均合法，只有实际路由材料与冻结候选不一致。
    assert_eq!(
        f.store.capture_run_request(&e, &capture, &f.context).await,
        Err(CoordinationError::Conflict.into())
    );
    assert_eq!(f.clock(), before);
    capture.input.route = "frozen".into();
    capture.input.provider_identity = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&route.candidates[0]).unwrap())
    );
    let mut context = f.context.clone();
    context.route = capture.input.route.clone();
    context.provider_identity = capture.input.provider_identity.clone();
    f.store
        .capture_run_request(&e, &capture, &context)
        .await
        .unwrap();
}
