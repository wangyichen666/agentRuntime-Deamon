//! 两真实 SQLite 连接与原 run/turn/tool caller；租约不是第二业务状态机。
use agent_core::*;
use agent_storage::{RunStore, SessionLifecycle, SessionQuery, TranscriptStore, TurnRepository};
use agent_storage_ports::SessionWriterRepository;
use rusqlite::Connection;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    root: PathBuf,
    path: PathBuf,
    store: Arc<RunStore>,
    owner: ExactOwner,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
impl Fixture {
    fn new(native: bool) -> Self {
        let root = std::env::temp_dir().join(format!(
            "ma-native-writer-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("runtime.sqlite3");
        let store = RunStore::open(&path).unwrap();
        let store = Arc::new(if native {
            store.enable_session_writer().unwrap()
        } else {
            store
        });
        store
            .create_session(&SessionKey("writer-session".into()))
            .unwrap();
        let Admission::New(run) = store
            .admit(
                SessionKey("writer-session".into()),
                RequestId::Number(1),
                "writer",
            )
            .unwrap()
        else {
            panic!("new run");
        };
        assert!(store.try_start_queued(&run.run_id).unwrap());
        let owner = store.run_owner(&run.run_id).unwrap();
        Self {
            root,
            path,
            store,
            owner,
        }
    }
    fn key(&self) -> SessionWriterLeaseKey {
        SessionWriterLeaseKey {
            scope: TenantContext::local(),
            session_key: self.owner.session_key.clone(),
            session_lifetime_id: self.owner.session_lifetime_id.clone(),
        }
    }
    fn expire(&self) {
        Connection::open(&self.path)
            .unwrap()
            .execute("UPDATE session_writer_leases SET expires_at_ms=0", [])
            .unwrap();
    }
}
async fn shared_contract(store: &dyn SessionWriterRepository, key: &SessionWriterLeaseKey) {
    let a = store
        .acquire_writer_lease(key, "writer-a", 10000)
        .await
        .unwrap();
    assert_eq!(a.epoch, 1);
    assert_eq!(
        store.acquire_writer_lease(key, "writer-b", 10000).await,
        Err(CoordinationError::LeaseBusy)
    );
    let renewed = store.renew_writer_lease(&a, 10000).await.unwrap();
    assert_eq!(renewed.epoch, a.epoch);
    assert!(renewed.expires_at_ms >= a.expires_at_ms);
    assert_eq!(
        store.read_writer_lease(key).await.unwrap(),
        Some(renewed.clone())
    );
    let mut foreign = key.clone();
    foreign.scope.tenant_id = "foreign".to_owned().try_into().unwrap();
    assert_eq!(
        store.acquire_writer_lease(&foreign, "writer-a", 1000).await,
        Err(CoordinationError::Forbidden)
    );
    let mut forged = renewed.clone();
    forged.epoch += 1;
    assert_eq!(
        store.renew_writer_lease(&forged, 1000).await,
        Err(CoordinationError::StaleLease)
    );
    assert_eq!(
        store.release_writer_lease(&forged).await,
        Err(CoordinationError::StaleLease)
    );
    store.release_writer_lease(&renewed).await.unwrap();
    let b = store
        .acquire_writer_lease(key, "writer-b", 10000)
        .await
        .unwrap();
    assert_eq!(b.epoch, a.epoch + 1);
    assert_eq!(
        store.renew_writer_lease(&a, 1000).await,
        Err(CoordinationError::StaleLease)
    );
}
#[tokio::test]
async fn sqlite_writer_repository_uses_database_time_complete_scope_and_epoch_cas() {
    let f = Fixture::new(false);
    shared_contract(f.store.as_ref(), &f.key()).await;
}
#[tokio::test]
async fn native_writer_old_callback_terminal_transcript_and_recovery_are_fenced_after_takeover() {
    let f = Fixture::new(true);
    let a = f.store.read_writer_lease(&f.key()).await.unwrap().unwrap();
    let second = RunStore::open(&f.path)
        .unwrap()
        .enable_session_writer()
        .unwrap();
    assert!(matches!(
        second.recover(),
        Err(agent_storage::RuntimeError::Coordination(
            CoordinationError::LeaseBusy
        ))
    ));
    assert_eq!(
        f.store.read_run(&f.owner.run_id).unwrap().unwrap().status,
        RunStatus::Running
    );
    f.expire();
    assert_eq!(second.recover().unwrap(), 1);
    let b = second.read_writer_lease(&f.key()).await.unwrap().unwrap();
    assert_eq!(b.epoch, a.epoch + 1);
    assert_ne!(b.owner_id, a.owner_id);
    let before = second
        .events_after(&f.owner.run_id, EventSeq(0), 100)
        .unwrap();
    let revision = second
        .session_snapshot(&f.owner.session_key)
        .unwrap()
        .revision;
    assert!(matches!(
        f.store.commit_turn(&TurnCommit {
            owner: f.owner.clone(),
            status: RunStatus::Completed,
            content: Some("迟到成功".into()),
            error: None
        }),
        Err(agent_storage::RuntimeError::Coordination(
            CoordinationError::StaleLease
        ))
    ));
    assert!(matches!(
        f.store.append_transcript(
            &f.owner,
            "late",
            &[Message::text(Role::Assistant, "迟到正文")]
        ),
        Err(agent_storage::RuntimeError::Coordination(
            CoordinationError::StaleLease
        ))
    ));
    assert_eq!(
        second.read_run(&f.owner.run_id).unwrap().unwrap().status,
        RunStatus::UnknownAfterRestart
    );
    assert_eq!(
        second
            .session_snapshot(&f.owner.session_key)
            .unwrap()
            .revision,
        revision
    );
    assert_eq!(
        second
            .events_after(&f.owner.run_id, EventSeq(0), 100)
            .unwrap()
            .len(),
        before.len()
    );
}
#[tokio::test]
async fn native_writer_expired_process_cannot_self_reacquire_and_unconfigured_store_cannot_bypass()
{
    let f = Fixture::new(true);
    f.expire();
    assert!(matches!(
        f.store.admit(
            f.owner.session_key.clone(),
            RequestId::Number(2),
            "不能自动重新 acquire"
        ),
        Err(agent_storage::RuntimeError::Coordination(
            CoordinationError::StaleLease
        ))
    ));
    let plain = RunStore::open(&f.path).unwrap();
    assert!(matches!(
        plain.recover(),
        Err(agent_storage::RuntimeError::Coordination(
            CoordinationError::StaleLease
        ))
    ));
    assert!(matches!(
        plain.commit_turn(&TurnCommit {
            owner: f.owner.clone(),
            status: RunStatus::Completed,
            content: None,
            error: None
        }),
        Err(agent_storage::RuntimeError::Coordination(
            CoordinationError::StaleLease
        ))
    ));
    assert_eq!(
        plain.read_run(&f.owner.run_id).unwrap().unwrap().status,
        RunStatus::Running
    );
}
#[tokio::test]
async fn native_writer_renewal_preserves_epoch_and_does_not_advance_business_event_clock() {
    let f = Fixture::new(true);
    let db = Connection::open(&f.path).unwrap();
    let before: u64 = db
        .query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
        .unwrap();
    let a = f.store.read_writer_lease(&f.key()).await.unwrap().unwrap();
    assert_eq!(f.store.renew_bound_writers().await.unwrap(), 1);
    let b = f.store.read_writer_lease(&f.key()).await.unwrap().unwrap();
    assert_eq!(b.epoch, a.epoch);
    assert_eq!(b.owner_id, a.owner_id);
    assert!(b.expires_at_ms >= a.expires_at_ms);
    assert_eq!(
        db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap(),
        before
    );
}
#[tokio::test]
async fn writer_two_independent_connections_have_one_winner_with_controlled_barrier() {
    let f = Fixture::new(false);
    let second = Arc::new(RunStore::open(&f.path).unwrap());
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let run = |store: Arc<RunStore>, owner: &str| {
        let owner = owner.to_owned();
        let key = f.key();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            store.acquire_writer_lease(&key, &owner, 10000).await
        })
    };
    let a = run(f.store.clone(), "a");
    let b = run(second, "b");
    let result = [a.await.unwrap(), b.await.unwrap()];
    assert_eq!(result.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        result
            .iter()
            .filter(|r| matches!(r, Err(CoordinationError::LeaseBusy)))
            .count(),
        1
    );
}

fn stale<T>(result: Result<T, agent_storage::RuntimeError>) {
    assert!(matches!(
        result,
        Err(agent_storage::RuntimeError::Coordination(
            CoordinationError::StaleLease
        ))
    ));
}
#[tokio::test]
async fn lost_writer_cannot_settle_native_tools_provider_resource_or_ownerless_hook() {
    use agent_storage::{HookRepository, MaintenanceRepository, ResourceRepository};
    use serde_json::json;
    let f = Fixture::new(true);
    let call = ToolCall {
        id: "late-tool".into(),
        name: "read".into(),
        arguments: json!({}),
    };
    f.store
        .prepare_tool_batch(
            &f.owner.run_id,
            1,
            &[(call, "read_only".into(), "digest".into(), false)],
        )
        .unwrap();
    f.store.start_tool(&f.owner.run_id, 1, "late-tool").unwrap();
    let mut attempt:ProviderAttempt=serde_json::from_value(json!({"attempt_id":"late-provider","run_id":f.owner.run_id,"round":1,"candidate_index":0,"provider_profile_id":"mock","api_type":"ollama","model":"mock","status":"started","stream_committed":false,"started_at_ms":0})).unwrap();
    f.store.start_provider_attempt(&attempt).unwrap();
    attempt.status = AttemptStatus::Succeeded;
    let resource = f
        .store
        .create_resource(&f.owner, ".", "native", "native")
        .unwrap();
    f.store
        .update_resource(
            &f.owner,
            resource.id,
            "running",
            Some("test-no-process"),
            None,
        )
        .unwrap();
    let payload = HookPayload {
        schema_version: 1,
        event: HookEvent::Notification,
        session_key: f.owner.session_key.clone(),
        session_lifetime_id: f.owner.session_lifetime_id.clone(),
        owner: None,
        cwd: ".".into(),
        channel: "daemon_rpc".into(),
        permission_mode: "observation".into(),
        route_digest: None,
        data: json!({}),
        operation_id: "late-hook".into(),
    };
    assert_eq!(f.store.claim_hook(&payload).unwrap(), HookClaim::New);
    let outcome = HookOutcome {
        payload,
        status: HookStatus::Succeeded,
        effect: Some(HookEffect::Observe {}),
        failure: None,
    };
    let publication = f
        .store
        .hook_publications()
        .unwrap()
        .into_iter()
        .find(|p| p.session_key == f.owner.session_key)
        .unwrap();
    f.expire();
    let second = RunStore::open(&f.path)
        .unwrap()
        .enable_session_writer()
        .unwrap();
    second.recover().unwrap();
    let db = Connection::open(&f.path).unwrap();
    let clock: u64 = db
        .query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
        .unwrap();
    let rows: String = db
        .query_row(
            "SELECT json_group_array(receipt_json) FROM tool_executions",
            [],
            |r| r.get(0),
        )
        .unwrap();
    stale(f.store.finish_tool(
        &f.owner.run_id,
        1,
        "late-tool",
        "success",
        None,
        &json!({"output":"late"}),
    ));
    stale(f.store.finish_provider_attempt(&attempt));
    stale(
        f.store
            .append_resource_log(&f.owner, resource.id, "late log"),
    );
    stale(
        f.store
            .update_resource(&f.owner, resource.id, "completed", None, None),
    );
    stale(f.store.settle_hook(&outcome));
    stale(
        f.store
            .acknowledge_hook_publication(&publication.operation_id),
    );
    assert_eq!(
        db.query_row::<String, _, _>(
            "SELECT json_group_array(receipt_json) FROM tool_executions",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        rows
    );
    assert_eq!(
        f.store.provider_attempts(&f.owner.run_id).unwrap()[0].status,
        AttemptStatus::OutcomeUnknown
    );
    assert_eq!(
        f.store.read_resource(resource.id).unwrap().state,
        "orphaned"
    );
    assert_eq!(
        db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap(),
        clock
    );
    let health = second.health_report().unwrap();
    assert_eq!(health["writer"]["stale_bound"], 0);
    assert_eq!(f.store.health_report().unwrap()["writer"]["stale_bound"], 1);
}
#[test]
fn protected_database_rejects_unconfigured_new_session_without_partial_facts() {
    let f = Fixture::new(true);
    let plain = RunStore::open(&f.path).unwrap();
    stale(plain.admit(
        SessionKey("cannot-bypass-with-another-session".into()),
        RequestId::Number(2),
        "bypass",
    ));
    assert!(
        !plain
            .session_exists("cannot-bypass-with-another-session")
            .unwrap()
    );
}
#[test]
fn writer_schema_migration_backup_atomic_rollback_and_future_version_are_verified() {
    let f = Fixture::new(false);
    let db = Connection::open(&f.path).unwrap();
    db.execute_batch("DELETE FROM role_bindings WHERE tenant_id='local-tenant' AND subject_id='local-cron-workload'; DELETE FROM principals WHERE tenant_id='local-tenant' AND subject_id='local-cron-workload'; DELETE FROM schema_migrations WHERE version=27; DROP TRIGGER native_event_provenance_insert; DROP TRIGGER immutable_event_provenance_update; DROP TRIGGER immutable_event_stamp_delete; ALTER TABLE events DROP COLUMN view_provenance; DELETE FROM schema_migrations WHERE version=26; DROP TRIGGER bind_lifetime_insert; DROP TRIGGER bind_lifetime_update; DROP TRIGGER immutable_run_incarnation; DROP TRIGGER immutable_lifetime_owner_update; DROP TRIGGER immutable_lifetime_owner_delete; DROP TABLE session_lifetime_owners; DELETE FROM schema_migrations WHERE version=25; DROP TABLE session_writer_leases;DROP INDEX session_scope_writer_key;DROP INDEX tenant_writer_domain;DELETE FROM schema_migrations WHERE version=24;CREATE TRIGGER writer_migration_fault BEFORE INSERT ON schema_migrations WHEN NEW.version=24 BEGIN SELECT RAISE(ABORT,'native writer migration fault');END;").unwrap();
    assert!(RunStore::open(&f.path).is_err());
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT max(version) FROM schema_migrations", [], |r| r
            .get(0))
            .unwrap(),
        23
    );
    assert_eq!(db.query_row::<u64,_,_>("SELECT count(*) FROM sqlite_master WHERE name IN ('session_writer_leases','session_scope_writer_key','tenant_writer_domain')",[],|r|r.get(0)).unwrap(),0);
    let backup = f.root.join("runtime.v23.backup.sqlite3");
    let marker: serde_json::Value = serde_json::from_slice(
        &std::fs::read(f.root.join("runtime.v23.backup.verified.json")).unwrap(),
    )
    .unwrap();
    use sha2::{Digest, Sha256};
    assert_eq!(
        marker["sha256"],
        format!("{:x}", Sha256::digest(std::fs::read(&backup).unwrap()))
    );
    db.execute_batch("DROP TRIGGER writer_migration_fault")
        .unwrap();
    let migrated = RunStore::open(&f.path).unwrap();
    assert_eq!(
        migrated.read_run(&f.owner.run_id).unwrap().unwrap().status,
        RunStatus::Running
    );
    db.execute("INSERT INTO schema_migrations VALUES(28,0)", [])
        .unwrap();
    assert!(RunStore::open(&f.path).is_err());
}

#[tokio::test]
async fn native_lifecycle_fork_rotation_tombstone_and_graceful_release_preserve_writer_fence() {
    use agent_storage::MaintenanceRepository;
    let f = Fixture::new(true);
    f.store
        .commit_turn(&TurnCommit {
            owner: f.owner.clone(),
            status: RunStatus::Completed,
            content: Some("完成".into()),
            error: None,
        })
        .unwrap();
    let second = RunStore::open(&f.path)
        .unwrap()
        .enable_session_writer()
        .unwrap();
    let revision = f
        .store
        .session_snapshot(&f.owner.session_key)
        .unwrap()
        .revision;
    assert!(matches!(
        second.fork_session(
            &f.owner.session_key,
            &SessionKey("unowned-fork".into()),
            revision
        ),
        Err(agent_storage::RuntimeError::Coordination(
            CoordinationError::LeaseBusy
        ))
    ));
    assert!(!second.session_exists("unowned-fork").unwrap());
    f.store
        .end_session(&f.owner.session_key, &f.owner.session_lifetime_id, false)
        .unwrap();
    let meta = f
        .store
        .session_metadata(&f.owner.session_key)
        .unwrap()
        .unwrap();
    assert_ne!(meta.lifetime, f.owner.session_lifetime_id);
    assert_eq!(f.store.health_report().unwrap()["writer"]["held"], 1);
    f.store
        .end_session(&meta.key, &meta.lifetime, true)
        .unwrap();
    assert!(matches!(
        second.create_session(&meta.key),
        Err(agent_storage::RuntimeError::Coordination(
            CoordinationError::LeaseBusy
        ))
    ));
    assert_eq!(f.store.release_bound_writers().await.unwrap(), 1);
    stale(f.store.create_session(&meta.key));
    second.recover().unwrap();
    let recreated = second.create_session(&meta.key).unwrap();
    assert_ne!(recreated.lifetime, meta.lifetime);
}

#[tokio::test]
async fn expired_native_writer_cannot_publish_an_object_as_a_retryable_storage_failure() {
    use agent_storage_ports::{
        ArtifactPublicationRepository, ArtifactStore, reserve_object_artifact,
    };
    use serde_json::json;
    use sha2::{Digest, Sha256};
    let f = Fixture::new(true);
    f.store
        .commit_turn(&TurnCommit {
            owner: f.owner.clone(),
            status: RunStatus::Completed,
            content: Some("完成前轮".into()),
            error: None,
        })
        .unwrap();
    let snapshot:RunSnapshot=serde_json::from_value(json!({"artifact_store_id":f.store.store_id(),"route":null,"tools":[],"cwd":".","permission_mode":"risk","sandbox_requested":"native","sandbox_effective":"native","context_read_only":false,"max_tool_calls":null,"config_generation":0})).unwrap();
    let Admission::New(run) = f
        .store
        .admit_run(
            &RunAdmission {
                workflow_node: None,
                plan_execution: None,
                session_key: f.owner.session_key.clone(),
                expected_lifetime: None,
                request_id: RequestId::Number(2),
                input: "对象发布".into(),
                mode: AdmissionMode::Queue,
            },
            &snapshot,
        )
        .unwrap()
    else {
        panic!("new run")
    };
    f.store.try_start_queued(&run.run_id).unwrap();
    let owner = f.store.run_owner(&run.run_id).unwrap();
    f.store
        .prepare_tool_batch(
            &run.run_id,
            1,
            &[(
                ToolCall {
                    id: "object".into(),
                    name: "read".into(),
                    arguments: json!({}),
                },
                "read_only".into(),
                "digest".into(),
                false,
            )],
        )
        .unwrap();
    f.store.start_tool(&run.run_id, 1, "object").unwrap();
    let body = "已经写入对象，但原 writer 不再有效。";
    let identity = ArtifactIdentity {
        sha256: format!("{:x}", Sha256::digest(body.as_bytes())),
        bytes: body.len() as u64,
    };
    let completion = ToolArtifactCompletion {
        round: 1,
        call_id: "object".into(),
        outcome: "success".into(),
        receipt: json!({"output_sha256":identity.sha256,"output_bytes":identity.bytes,"output":"预览"}),
    };
    let reservation = reserve_object_artifact(
        EnterpriseOwnerEnvelope {
            scope: TenantContext::local(),
            owner,
        },
        &completion,
        identity,
        f.store.store_id(),
    )
    .unwrap();
    let reservation = f
        .store
        .reserve_object_artifact(&reservation, &completion)
        .await
        .unwrap();
    let verified = f.store.publish_object(&reservation, body).await.unwrap();
    f.expire();
    let second = RunStore::open(&f.path)
        .unwrap()
        .enable_session_writer()
        .unwrap();
    second.recover().unwrap();
    assert_eq!(
        f.store
            .commit_object_artifact(&reservation, &completion, &verified)
            .await,
        Err(ArtifactError::StaleWriter)
    );
    assert_eq!(ArtifactError::StaleWriter.http_status(), 409);
    let db = Connection::open(&f.path).unwrap();
    assert!(
        !db.query_row::<bool, _, _>(
            "SELECT published FROM artifact_publications WHERE publication_id=?1",
            [&reservation.publication_id],
            |r| r.get(0)
        )
        .unwrap()
    );
    assert!(
        f.store.tool_receipts(&run.run_id).unwrap()[0]
            .artifact_ref
            .is_none()
    );
}

#[test]
fn enabling_writer_does_not_take_over_an_unleased_live_legacy_run() {
    let f = Fixture::new(false);
    let other = RunStore::open(&f.path).unwrap();
    assert!(matches!(
        other.enable_session_writer(),
        Err(agent_storage::RuntimeError::Coordination(
            CoordinationError::Conflict
        ))
    ));
    assert_eq!(
        f.store.read_run(&f.owner.run_id).unwrap().unwrap().status,
        RunStatus::Running
    );
    let db = Connection::open(&f.path).unwrap();
    assert_eq!(
        db.query_row::<u64, _, _>("SELECT count(*) FROM session_writer_leases", [], |r| r
            .get(0))
            .unwrap(),
        0
    );
}

#[test]
fn expiry_in_native_commit_rolls_back_tool_receipt_turn_event_and_clock_together() {
    use serde_json::json;
    let f = Fixture::new(true);
    f.store
        .prepare_tool_batch(
            &f.owner.run_id,
            1,
            &[(
                ToolCall {
                    id: "commit-expiry".into(),
                    name: "read".into(),
                    arguments: json!({}),
                },
                "read_only".into(),
                "digest".into(),
                false,
            )],
        )
        .unwrap();
    f.store
        .start_tool(&f.owner.run_id, 1, "commit-expiry")
        .unwrap();
    let db = Connection::open(&f.path).unwrap();
    let expiry: i64 = db
        .query_row("SELECT expires_at_ms FROM session_writer_leases", [], |r| {
            r.get(0)
        })
        .unwrap();
    db.execute_batch("CREATE TRIGGER tool_commit_expiry AFTER UPDATE ON tool_executions WHEN NEW.status='terminal' BEGIN UPDATE session_writer_leases SET expires_at_ms=0;END;").unwrap();
    stale(f.store.finish_tool(
        &f.owner.run_id,
        1,
        "commit-expiry",
        "success",
        None,
        &json!({"output":"完成"}),
    ));
    assert_eq!(
        db.query_row::<String, _, _>("SELECT status FROM tool_executions", [], |r| r.get(0))
            .unwrap(),
        "running"
    );
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT expires_at_ms FROM session_writer_leases", [], |r| r
            .get(0))
            .unwrap(),
        expiry
    );
    db.execute_batch("DROP TRIGGER tool_commit_expiry").unwrap();
    f.store
        .finish_tool(
            &f.owner.run_id,
            1,
            "commit-expiry",
            "success",
            None,
            &json!({"output":"完成"}),
        )
        .unwrap();
    let before: (u64, u64) = db
        .query_row(
            "SELECT (SELECT revision FROM snapshot_clock),(SELECT count(*) FROM events)",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    let transcript = f
        .store
        .session_snapshot(&f.owner.session_key)
        .unwrap()
        .revision;
    db.execute_batch("CREATE TRIGGER view_commit_expiry AFTER UPDATE ON snapshot_clock BEGIN UPDATE session_writer_leases SET expires_at_ms=0;END;").unwrap();
    stale(f.store.commit_turn(&TurnCommit {
        owner: f.owner.clone(),
        status: RunStatus::Completed,
        content: Some("提交时失效".into()),
        error: None,
    }));
    assert_eq!(
        f.store.read_run(&f.owner.run_id).unwrap().unwrap().status,
        RunStatus::Running
    );
    assert_eq!(
        f.store
            .session_snapshot(&f.owner.session_key)
            .unwrap()
            .revision,
        transcript
    );
    assert_eq!(
        db.query_row::<(u64, u64), _, _>(
            "SELECT (SELECT revision FROM snapshot_clock),(SELECT count(*) FROM events)",
            [],
            |r| Ok((r.get(0)?, r.get(1)?))
        )
        .unwrap(),
        before
    );
}

#[test]
fn interrupted_provider_settlement_is_unknown_atomic_and_rejects_late_success() {
    for terminal in [
        RunStatus::Completed,
        RunStatus::Failed,
        RunStatus::Cancelled,
    ] {
        let f = Fixture::new(true);
        let attempt: ProviderAttempt=serde_json::from_value(serde_json::json!({"attempt_id":"interrupted","run_id":f.owner.run_id,"round":1,"candidate_index":0,"provider_profile_id":"mock","api_type":"ollama","model":"mock","status":"started","stream_committed":false,"started_at_ms":0})).unwrap();
        f.store.start_provider_attempt(&attempt).unwrap();
        let settled = f
            .store
            .finish(&f.owner.run_id, terminal, Some("不能据文本证明成功"), None)
            .unwrap();
        assert_eq!(settled.status, RunStatus::UnknownAfterRestart);
        assert!(settled.content.is_none());
        let attempts = f.store.provider_attempts(&f.owner.run_id).unwrap();
        assert_eq!(attempts[0].status, AttemptStatus::OutcomeUnknown);
        assert!(attempts[0].finished_at_ms.is_some());
        let messages = f
            .store
            .session_snapshot(&f.owner.session_key)
            .unwrap()
            .messages;
        assert!(!messages.iter().any(|m| m.role == Role::Assistant));
    }
    let f = Fixture::new(true);
    let mut attempt: ProviderAttempt=serde_json::from_value(serde_json::json!({"attempt_id":"rollback-unknown","run_id":f.owner.run_id,"round":1,"candidate_index":0,"provider_profile_id":"mock","api_type":"ollama","model":"mock","status":"started","stream_committed":false,"started_at_ms":0})).unwrap();
    f.store.start_provider_attempt(&attempt).unwrap();
    let db = Connection::open(&f.path).unwrap();
    let before: (u64, u64) = db
        .query_row(
            "SELECT (SELECT revision FROM snapshot_clock),(SELECT count(*) FROM events)",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    db.execute_batch("CREATE TRIGGER reject_provider_unknown BEFORE UPDATE OF status ON provider_attempts WHEN NEW.status='outcome_unknown' BEGIN SELECT RAISE(ABORT,'controlled provider unknown failure'); END;").unwrap();
    assert!(f.store.recover().is_err());
    assert_eq!(
        f.store.read_run(&f.owner.run_id).unwrap().unwrap().status,
        RunStatus::Running
    );
    assert_eq!(
        f.store.provider_attempts(&f.owner.run_id).unwrap()[0].status,
        AttemptStatus::Started
    );
    assert_eq!(
        db.query_row::<(u64, u64), _, _>(
            "SELECT (SELECT revision FROM snapshot_clock),(SELECT count(*) FROM events)",
            [],
            |r| Ok((r.get(0)?, r.get(1)?))
        )
        .unwrap(),
        before
    );
    db.execute_batch("DROP TRIGGER reject_provider_unknown")
        .unwrap();
    assert_eq!(f.store.recover().unwrap(), 1);
    let before = serde_json::to_value(f.store.provider_attempts(&f.owner.run_id).unwrap()).unwrap();
    attempt.status = AttemptStatus::Succeeded;
    assert!(f.store.finish_provider_attempt(&attempt).is_err());
    assert_eq!(
        serde_json::to_value(f.store.provider_attempts(&f.owner.run_id).unwrap()).unwrap(),
        before
    );
    assert_eq!(before[0]["status"], "outcome_unknown");
}

#[test]
fn corrupted_provider_receipt_cannot_redirect_unknown_to_another_attempt() {
    let f = Fixture::new(true);
    let Admission::New(other) = f
        .store
        .admit(
            SessionKey("other-provider-session".into()),
            RequestId::Number(2),
            "another run",
        )
        .unwrap()
    else {
        panic!("new");
    };
    assert!(f.store.try_start_queued(&other.run_id).unwrap());
    for (id, run) in [
        ("source-attempt", &f.owner.run_id),
        ("target-attempt", &other.run_id),
    ] {
        let attempt: ProviderAttempt=serde_json::from_value(serde_json::json!({"attempt_id":id,"run_id":run,"round":1,"candidate_index":0,"provider_profile_id":"mock","api_type":"ollama","model":"mock","status":"started","stream_committed":false,"started_at_ms":0})).unwrap();
        f.store.start_provider_attempt(&attempt).unwrap();
    }
    let db = Connection::open(&f.path).unwrap();
    db.execute("UPDATE provider_attempts SET data_json=json_set(data_json,'$.attempt_id','target-attempt') WHERE attempt_id='source-attempt'",[]).unwrap();
    let before: String = db
        .query_row(
            "SELECT json_group_array(data_json) FROM provider_attempts",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let events: u64 = db
        .query_row("SELECT count(*) FROM events", [], |r| r.get(0))
        .unwrap();
    assert!(
        f.store
            .finish(
                &f.owner.run_id,
                RunStatus::Completed,
                Some("不能重定向"),
                None
            )
            .is_err(),
        "损坏JSON身份必须在修改另一条收据之前拒绝"
    );
    assert_eq!(
        db.query_row::<String, _, _>(
            "SELECT json_group_array(data_json) FROM provider_attempts",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        before
    );
    assert_eq!(
        db.query_row::<u64, _, _>("SELECT count(*) FROM events", [], |r| r.get(0))
            .unwrap(),
        events
    );
    assert_eq!(
        f.store.read_run(&f.owner.run_id).unwrap().unwrap().status,
        RunStatus::Running
    );
    assert_eq!(
        f.store.read_run(&other.run_id).unwrap().unwrap().status,
        RunStatus::Running
    );
}
