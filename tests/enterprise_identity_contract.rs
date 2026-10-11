//! 授权领域实现与真实 SQLite 事务的合同；不创建第二运行 owner。
use agent_core::*;
use agent_storage::{IdentityRepository, RunStore, RuntimeError};
use agent_storage_ports::IdentityReadRepository;
use rusqlite::{Connection, params};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
static NEXT: AtomicU64 = AtomicU64::new(0);

#[tokio::test]
async fn async_identity_and_frozen_authorization_preserve_scope_epoch_and_exact_owner_errors() {
    let file = path();
    let store = RunStore::open(&file).unwrap();
    let scope = TenantContext::local();
    let subject: SubjectId = "local-subject".to_owned().try_into().unwrap();
    let identity = store.read_identity(&scope, &subject).await.unwrap();
    assert_eq!(identity, store.identity_readback(&scope, &subject).unwrap());
    let auth = request(&store, 1);
    let Admission::New(run) = store
        .admit_authorized_run(
            &admission(1),
            &snapshot(),
            &auth,
            &agent_policy::PolicyEngine,
        )
        .unwrap()
    else {
        panic!("新准入")
    };
    let owner = envelope(&store, &run.run_id);
    let frozen = store.read_authorization(&owner).await.unwrap().unwrap();
    let db = Connection::open(&file).unwrap();
    let before: u64 = db
        .query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
        .unwrap();
    for field in 0..4 {
        let mut foreign = scope.clone();
        match field {
            0 => foreign.organization_id = "foreign".to_owned().try_into().unwrap(),
            1 => foreign.tenant_id = "foreign".to_owned().try_into().unwrap(),
            2 => foreign.project_id = "foreign".to_owned().try_into().unwrap(),
            _ => foreign.agent_uid = "foreign".to_owned().try_into().unwrap(),
        }
        assert_eq!(
            store.read_identity(&foreign, &subject).await,
            Err(RepositoryError::Authorization(
                AuthorizationError::Forbidden
            ))
        );
        let mut candidate = owner.clone();
        candidate.scope = foreign;
        assert_eq!(
            store.read_authorization(&candidate).await,
            Err(RepositoryError::Authorization(
                AuthorizationError::Forbidden
            ))
        );
    }
    let mut absent = owner.clone();
    absent.owner.run_id = RunId("absent".into());
    assert_eq!(
        store.read_authorization(&absent).await,
        Err(RepositoryError::Authorization(
            AuthorizationError::Forbidden
        ))
    );
    for field in 0..3 {
        let mut stale = owner.clone();
        match field {
            0 => stale.owner.run_generation.0 += 1,
            1 => stale.owner.turn_id = TurnId("stale".into()),
            _ => stale.owner.session_lifetime_id = SessionLifetimeId("stale".into()),
        }
        assert_eq!(
            store.read_authorization(&stale).await,
            Err(RepositoryError::Authorization(
                AuthorizationError::StaleOwner
            ))
        );
    }
    let mut policy = PolicyDocument::local();
    policy.version = "async-policy-2".to_owned().try_into().unwrap();
    publish(&db, &policy);
    assert_eq!(
        store.read_identity(&scope, &subject).await.unwrap().policy,
        policy
    );
    assert_eq!(
        store.read_authorization(&owner).await.unwrap(),
        Some(frozen.clone()),
        "普通发布不能修改旧 run 冻结策略"
    );
    db.execute("UPDATE principals SET revocation_epoch=1", [])
        .unwrap();
    assert_eq!(
        store.read_authorization(&owner).await,
        Err(RepositoryError::Authorization(AuthorizationError::Revoked))
    );
    assert_eq!(
        store
            .read_identity(&scope, &subject)
            .await
            .unwrap()
            .principal
            .revocation_epoch,
        1
    );
    assert_eq!(
        db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap(),
        before
    );
    assert_eq!(count(&db, "runs"), 1);
    db.execute_batch("PRAGMA foreign_keys=OFF; DROP TABLE principals;")
        .unwrap();
    let unavailable = store.read_identity(&scope, &subject).await.unwrap_err();
    assert_eq!(
        unavailable,
        RepositoryError::Coordination(CoordinationError::StorageUnavailable)
    );
    assert_eq!(unavailable.to_string(), "存储服务不可用");
    assert_eq!(unavailable.rpc_code(), -32053);
    assert_eq!(unavailable.http_status(), 503);
    drop(store);
    drop(db);
    std::fs::remove_file(file).unwrap();
}

#[tokio::test]
async fn async_authorization_uses_shared_native_writer_proof_and_never_reacquires_expired_epoch() {
    let file = path();
    let store = RunStore::open(&file)
        .unwrap()
        .enable_session_writer()
        .unwrap();
    let auth = request(&store, 1);
    let Admission::New(run) = store
        .admit_authorized_run(
            &admission(1),
            &snapshot(),
            &auth,
            &agent_policy::PolicyEngine,
        )
        .unwrap()
    else {
        panic!("新准入")
    };
    let owner = envelope(&store, &run.run_id);
    let cloned = store.clone();
    assert!(cloned.read_authorization(&owner).await.unwrap().is_some());
    let db = Connection::open(&file).unwrap();
    let clock: u64 = db
        .query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
        .unwrap();
    let epoch: u64 = db
        .query_row("SELECT epoch FROM session_writer_leases", [], |r| r.get(0))
        .unwrap();
    db.execute("UPDATE session_writer_leases SET expires_at_ms=0", [])
        .unwrap();
    assert_eq!(
        cloned.read_authorization(&owner).await,
        Err(RepositoryError::Coordination(CoordinationError::StaleLease))
    );
    assert_eq!(
        db.query_row::<u64, _, _>("SELECT epoch FROM session_writer_leases", [], |r| r.get(0))
            .unwrap(),
        epoch
    );
    assert_eq!(
        db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap(),
        clock
    );
    drop(cloned);
    drop(store);
    drop(db);
    std::fs::remove_file(file).unwrap();
}
fn path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "ma-enterprise-{}-{}.sqlite3",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
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
        session_key: SessionKey("same-public-key".into()),
        expected_lifetime: None,
        request_id: RequestId::Number(n),
        input: "运行".into(),
        mode: AdmissionMode::Queue,
    }
}
fn envelope(store: &RunStore, run: &RunId) -> EnterpriseOwnerEnvelope {
    EnterpriseOwnerEnvelope {
        scope: TenantContext::local(),
        owner: agent_storage::SessionQuery::run_owner(store, run).unwrap(),
    }
}
fn count(db: &Connection, table: &str) -> i64 {
    db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
        .unwrap()
}
fn publish(db: &Connection, policy: &PolicyDocument) {
    db.execute(
        "INSERT INTO identity_policies VALUES('local-tenant','local-project','local-agent',?1,?2)",
        params![
            policy.version.as_str(),
            serde_json::to_string(policy).unwrap()
        ],
    )
    .unwrap();
    db.execute(
        "UPDATE identity_policy_heads SET version=?1",
        [policy.version.as_str()],
    )
    .unwrap();
}
#[test]
fn authorized_admission_freezes_policy_and_restart_readback_without_new_facts() {
    let path = path();
    let store = RunStore::open(&path).unwrap();
    let auth = request(&store, 1);
    let Admission::New(run) = store
        .admit_authorized_run(
            &admission(1),
            &snapshot(),
            &auth,
            &agent_policy::PolicyEngine,
        )
        .unwrap()
    else {
        panic!("必须新准入")
    };
    let owner = envelope(&store, &run.run_id);
    let first = store.authorization_readback(&owner).unwrap().unwrap();
    let db = Connection::open(&path).unwrap();
    let clock = count(&db, "events");
    let mut policy = PolicyDocument::local();
    policy.version = "policy-2".to_owned().try_into().unwrap();
    publish(&db, &policy);
    assert_eq!(
        store.authorization_readback(&owner).unwrap(),
        Some(first.clone()),
        "普通发布不改旧 run"
    );
    let mut stale_auth = auth.clone();
    stale_auth.context.request_id = RequestId::Number(2);
    assert!(matches!(
        store.admit_authorized_run(
            &admission(2),
            &snapshot(),
            &stale_auth,
            &agent_policy::PolicyEngine
        ),
        Err(RuntimeError::Authorization(AuthorizationError::StalePolicy))
    ));
    assert_eq!(count(&db, "runs"), 1);
    assert_eq!(count(&db, "events"), clock);
    let newer = request(&store, 2);
    let Admission::New(run2) = store
        .admit_authorized_run(
            &admission(2),
            &snapshot(),
            &newer,
            &agent_policy::PolicyEngine,
        )
        .unwrap()
    else {
        panic!("必须新准入")
    };
    assert_eq!(
        store
            .authorization_readback(&envelope(&store, &run2.run_id))
            .unwrap()
            .unwrap()
            .policy,
        policy
    );
    drop(store);
    let store = RunStore::open(&path).unwrap();
    let clock = count(&db, "events");
    assert_eq!(store.authorization_readback(&owner).unwrap(), Some(first));
    assert_eq!(count(&db, "events"), clock);
    std::fs::remove_file(path).unwrap();
}
#[test]
fn deny_scope_revocation_and_corruption_never_commit_business_facts() {
    let path = path();
    let store = RunStore::open(&path).unwrap();
    let db = Connection::open(&path).unwrap();
    let mut auth = request(&store, 1);
    auth.context.scope.tenant_id = "foreign".to_owned().try_into().unwrap();
    assert!(matches!(
        store.admit_authorized_run(
            &admission(1),
            &snapshot(),
            &auth,
            &agent_policy::PolicyEngine
        ),
        Err(RuntimeError::Authorization(AuthorizationError::Forbidden))
    ));
    assert_eq!(count(&db, "sessions"), 0);
    assert_eq!(count(&db, "runs"), 0);
    let mut policy = PolicyDocument::local();
    policy.version = "deny-policy".to_owned().try_into().unwrap();
    policy.hard_denies.push(Action::SessionPrompt);
    publish(&db, &policy);
    auth = request(&store, 1);
    assert!(matches!(
        store.admit_authorized_run(
            &admission(1),
            &snapshot(),
            &auth,
            &agent_policy::PolicyEngine
        ),
        Err(RuntimeError::Authorization(AuthorizationError::Forbidden))
    ));
    assert_eq!(count(&db, "sessions"), 0);
    policy.version = "allow-policy".to_owned().try_into().unwrap();
    policy.hard_denies.clear();
    publish(&db, &policy);
    auth = request(&store, 1);
    let Admission::New(run) = store
        .admit_authorized_run(
            &admission(1),
            &snapshot(),
            &auth,
            &agent_policy::PolicyEngine,
        )
        .unwrap()
    else {
        panic!("必须新准入")
    };
    let owner = envelope(&store, &run.run_id);
    let before = count(&db, "events");
    let mut foreign = owner.clone();
    foreign.scope.project_id = "foreign".to_owned().try_into().unwrap();
    let mut absent = foreign.clone();
    absent.owner.run_id = RunId("absent".into());
    for candidate in [&foreign, &absent] {
        assert!(matches!(
            store.authorization_readback(candidate),
            Err(RuntimeError::Authorization(AuthorizationError::Forbidden))
        ));
    }
    db.execute("UPDATE principals SET revocation_epoch=1", [])
        .unwrap();
    assert!(matches!(
        store.try_start_queued(&run.run_id),
        Err(RuntimeError::Authorization(AuthorizationError::Revoked))
    ));
    assert!(matches!(
        agent_storage::TurnRepository::commit_turn(
            &store,
            &TurnCommit {
                owner: owner.owner.clone(),
                status: RunStatus::Completed,
                content: Some("迟到".into()),
                error: None
            }
        ),
        Err(RuntimeError::Authorization(AuthorizationError::Revoked))
    ));
    assert_eq!(count(&db, "events"), before);
    db.execute("UPDATE principals SET revocation_epoch=0", [])
        .unwrap();
    let mut stale = owner.clone();
    stale.owner.run_generation.0 += 1;
    assert!(store.authorization_readback(&stale).is_err());
    db.execute_batch("DROP TRIGGER immutable_run_authorization; UPDATE run_authorizations SET snapshot_json='{}';").unwrap();
    assert!(matches!(
        store.authorization_readback(&owner),
        Err(RuntimeError::Authorization(AuthorizationError::Forbidden))
    ));
    assert_eq!(count(&db, "events"), before);
    std::fs::remove_file(path).unwrap();
}
#[test]
fn authorization_insert_failure_rolls_back_session_run_queue_and_transcript() {
    let path = path();
    let store = RunStore::open(&path).unwrap();
    let db = Connection::open(&path).unwrap();
    db.execute_batch("CREATE TRIGGER fail_authorization BEFORE INSERT ON run_authorizations BEGIN SELECT RAISE(ABORT,'注入提交前故障'); END;").unwrap();
    assert!(
        store
            .admit_authorized_run(
                &admission(1),
                &snapshot(),
                &request(&store, 1),
                &agent_policy::PolicyEngine
            )
            .is_err()
    );
    for table in [
        "runs",
        "turns",
        "sessions",
        "session_scopes",
        "queued_messages",
        "events",
        "run_snapshots",
        "run_authorizations",
        "transcript_batches",
    ] {
        assert_eq!(count(&db, table), 0, "{table} 必须回滚");
    }
    std::fs::remove_file(path).unwrap();
}
#[test]
fn concurrent_duplicate_operation_commits_one_authorization_and_payload_conflicts() {
    let path = path();
    let store = Arc::new(RunStore::open(&path).unwrap());
    let auth = request(&store, 1);
    let barrier = Arc::new(Barrier::new(3));
    let threads = (0..2)
        .map(|_| {
            let store = store.clone();
            let auth = auth.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store
                    .admit_authorized_run(
                        &admission(1),
                        &snapshot(),
                        &auth,
                        &agent_policy::PolicyEngine,
                    )
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    let results = threads
        .into_iter()
        .map(|t| t.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Admission::New(_)))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Admission::Existing(_)))
            .count(),
        1
    );
    let mut different = admission(1);
    different.input = "另一输入".into();
    assert!(
        store
            .admit_authorized_run(&different, &snapshot(), &auth, &agent_policy::PolicyEngine)
            .is_err()
    );
    let db = Connection::open(&path).unwrap();
    assert_eq!(count(&db, "runs"), 1);
    assert_eq!(count(&db, "run_authorizations"), 1);
    std::fs::remove_file(path).unwrap();
}
#[test]
fn v20_upgrade_maps_legacy_sessions_without_fabricating_run_authorizations_and_failure_is_atomic() {
    let path = path();
    let store = RunStore::open(&path).unwrap();
    let Admission::New(run) = store
        .admit_with_route(
            SessionKey("legacy".into()),
            RequestId::Number(1),
            "历史",
            AdmissionMode::Queue,
            None,
        )
        .unwrap()
    else {
        panic!("必须新准入")
    };
    drop(store);
    let db = Connection::open(&path).unwrap();
    db.execute_batch("DELETE FROM role_bindings WHERE tenant_id='local-tenant' AND subject_id='local-cron-workload'; DELETE FROM principals WHERE tenant_id='local-tenant' AND subject_id='local-cron-workload'; DELETE FROM schema_migrations WHERE version=27; DROP TRIGGER native_event_provenance_insert; DROP TRIGGER immutable_event_provenance_update; DROP TRIGGER immutable_event_stamp_delete; ALTER TABLE events DROP COLUMN view_provenance; DELETE FROM schema_migrations WHERE version=26; DROP TRIGGER bind_lifetime_insert; DROP TRIGGER bind_lifetime_update; DROP TRIGGER immutable_run_incarnation; DROP TRIGGER immutable_lifetime_owner_update; DROP TRIGGER immutable_lifetime_owner_delete; DROP TABLE session_lifetime_owners; DELETE FROM schema_migrations WHERE version=25; DROP TABLE session_writer_leases; DROP INDEX session_scope_writer_key; DROP INDEX tenant_writer_domain; DELETE FROM schema_migrations WHERE version=24; DROP TRIGGER artifact_publication_immutable; DROP TRIGGER artifact_publication_monotonic; DROP TABLE artifact_publications; DELETE FROM schema_migrations WHERE version=23; DROP TRIGGER outbox_immutable; DROP TABLE publication_inbox; DROP TABLE event_outbox; DROP TABLE delivery_leases; DELETE FROM schema_migrations WHERE version=22; DROP TRIGGER local_session_scope; DROP TRIGGER immutable_policy; DROP TRIGGER immutable_run_authorization; DROP TABLE run_authorizations; DROP TABLE session_scopes; DROP TABLE identity_policy_heads; DROP TABLE identity_policies; DROP TABLE role_bindings; DROP TABLE principals; DROP TABLE agents; DROP TABLE projects; DROP TABLE tenants; DROP TABLE organizations; DELETE FROM schema_migrations WHERE version=21; CREATE TRIGGER fail_v21 BEFORE INSERT ON schema_migrations WHEN NEW.version=21 BEGIN SELECT RAISE(ABORT,'身份迁移故障'); END;").unwrap();
    assert!(RunStore::open(&path).is_err());
    assert_eq!(
        db.query_row::<i64, _, _>(
            "SELECT count(*) FROM sqlite_master WHERE name='organizations'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT max(version) FROM schema_migrations", [], |r| r
            .get(0))
            .unwrap(),
        20
    );
    db.execute_batch("DROP TRIGGER fail_v21;").unwrap();
    let store = RunStore::open(&path).unwrap();
    assert_eq!(count(&db, "session_scopes"), 1);
    assert_eq!(
        store
            .authorization_readback(&envelope(&store, &run.run_id))
            .unwrap(),
        None
    );
    let mut legacy = admission(1);
    legacy.session_key = SessionKey("legacy".into());
    legacy.input = "历史".into();
    assert!(matches!(
        store.admit_authorized_run(
            &legacy,
            &snapshot(),
            &request(&store, 1),
            &agent_policy::PolicyEngine
        ),
        Ok(Admission::Existing(_))
    ));
    assert_eq!(count(&db, "run_authorizations"), 0);

    db.execute("INSERT INTO schema_migrations VALUES(28,0)", [])
        .unwrap();
    drop(store);
    assert!(RunStore::open(&path).is_err());
    std::fs::remove_file(path).unwrap();
}

#[test]
fn policy_publish_during_admission_cannot_commit_stale_authorization() {
    struct PausedVerifier {
        observed: Arc<Barrier>,
        published: Arc<Barrier>,
    }
    impl AuthorizationVerifier for PausedVerifier {
        fn verify(
            &self,
            request: &AuthorizationRequest,
            identity: &IdentityReadback,
        ) -> Result<(), AuthorizationError> {
            self.observed.wait();
            self.published.wait();
            agent_policy::PolicyEngine.verify(request, identity)
        }
    }
    let path = path();
    let store = Arc::new(RunStore::open(&path).unwrap());
    let auth = request(&store, 1);
    let observed = Arc::new(Barrier::new(2));
    let published = Arc::new(Barrier::new(2));
    let worker = {
        let store = store.clone();
        let observed = observed.clone();
        let published = published.clone();
        std::thread::spawn(move || {
            store.admit_authorized_run(
                &admission(1),
                &snapshot(),
                &auth,
                &PausedVerifier {
                    observed,
                    published,
                },
            )
        })
    };
    observed.wait();
    let db = Connection::open(&path).unwrap();
    let mut policy = PolicyDocument::local();
    policy.version = "published-concurrently".to_owned().try_into().unwrap();
    publish(&db, &policy);
    published.wait();
    assert!(
        worker.join().unwrap().is_err(),
        "WAL 旧读事务不能提升为写事务后提交旧授权"
    );
    assert_eq!(count(&db, "runs"), 0);
    assert_eq!(count(&db, "sessions"), 0);
    assert_eq!(count(&db, "run_authorizations"), 0);
    assert_eq!(request(&store, 1).expected_policy, policy.version);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn expired_time_claim_is_rechecked_after_entering_admission_transaction() {
    let path = path();
    let store = RunStore::open(&path).unwrap();
    let db = Connection::open(&path).unwrap();
    let mut policy = PolicyDocument::local();
    policy.version = "expired-policy".to_owned().try_into().unwrap();
    policy.conditions.expires_at_ms = Some(1000);
    publish(&db, &policy);
    let auth = request(&store, 1);
    assert_eq!(auth.attributes.timestamp_ms, 100);
    // 使用请求的旧时间可以通过纯判定，但 canonical 准入必须使用当前服务器时间。
    let identity = store
        .identity_readback(&TenantContext::local(), &auth.context.principal.subject_id)
        .unwrap();
    assert!(agent_policy::PolicyEngine.verify(&auth, &identity).is_ok());
    assert!(matches!(
        store.admit_authorized_run(
            &admission(1),
            &snapshot(),
            &auth,
            &agent_policy::PolicyEngine
        ),
        Err(RuntimeError::Authorization(AuthorizationError::Forbidden))
    ));
    assert_eq!(count(&db, "runs"), 0);
    assert_eq!(count(&db, "sessions"), 0);
    std::fs::remove_file(path).unwrap();
}
