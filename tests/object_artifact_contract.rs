//! 原工具 owner + 两种真实对象存储；不使用内存对象替代 S3 验收。
use agent_artifact::{ArtifactService, S3ArtifactConfig, S3ArtifactStore};
use agent_core::*;
use agent_storage::{MaintenanceRepository, RunStore, SessionQuery};
use agent_storage_ports::{ArtifactPublicationRepository, ArtifactStore, reserve_object_artifact};
use rusqlite::Connection;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
static NEXT: AtomicU64 = AtomicU64::new(0);
const BODY: &str = "真正的对象输出：你好，世界。";
struct Fixture {
    root: PathBuf,
    store: Arc<RunStore>,
    objects: Arc<dyn ArtifactStore>,
    owner: ExactOwner,
    completion: ToolArtifactCompletion,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
impl Fixture {
    fn new(objects: Option<Arc<dyn ArtifactStore>>) -> Self {
        let root = std::env::temp_dir().join(format!(
            "agent-object-contract-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let store = Arc::new(RunStore::open(&root.join("runtime.sqlite3")).unwrap());
        let objects = objects.unwrap_or_else(|| store.clone());
        let snapshot:RunSnapshot=serde_json::from_value(json!({"artifact_store_id":objects.store_id(),"route":null,"tools":[],"cwd":".","permission_mode":"risk","sandbox_requested":"native","sandbox_effective":"native","context_read_only":false,"max_tool_calls":null,"config_generation":0})).unwrap();
        let Admission::New(run) = store
            .admit_run(
                &RunAdmission {
                    workflow_node: None,
                    plan_execution: None,
                    session_key: SessionKey("object-contract".into()),
                    expected_lifetime: None,
                    request_id: RequestId::Number(1),
                    input: "对象测试".into(),
                    mode: AdmissionMode::Queue,
                },
                &snapshot,
            )
            .unwrap()
        else {
            panic!("新 run");
        };
        store.try_start_queued(&run.run_id).unwrap();
        let owner = store.run_owner(&run.run_id).unwrap();
        let call = ToolCall {
            id: "object-call".into(),
            name: "read".into(),
            arguments: json!({}),
        };
        store
            .prepare_tool_batch(
                &run.run_id,
                1,
                &[(call, "read_only".into(), "digest".into(), false)],
            )
            .unwrap();
        store.start_tool(&run.run_id, 1, "object-call").unwrap();
        let completion = ToolArtifactCompletion {
            round: 1,
            call_id: "object-call".into(),
            outcome: "success".into(),
            receipt: json!({"output_sha256":format!("{:x}",Sha256::digest(BODY.as_bytes())),"output_bytes":BODY.len(),"output":"preview"}),
        };
        Self {
            root,
            store,
            objects,
            owner,
            completion,
        }
    }
    fn service(&self) -> ArtifactService {
        ArtifactService::new(
            self.store.clone(),
            self.objects.clone(),
            TenantContext::local(),
        )
    }
    fn reserve(&self) -> ObjectArtifactReservation {
        reserve_object_artifact(
            EnterpriseOwnerEnvelope {
                scope: TenantContext::local(),
                owner: self.owner.clone(),
            },
            &self.completion,
            ArtifactIdentity {
                sha256: format!("{:x}", Sha256::digest(BODY.as_bytes())),
                bytes: BODY.len() as u64,
            },
            self.objects.store_id(),
        )
        .unwrap()
    }
    fn db(&self) -> Connection {
        Connection::open(self.root.join("runtime.sqlite3")).unwrap()
    }
}
async fn contract(f: &Fixture) {
    let reserved = f
        .store
        .reserve_object_artifact(&f.reserve(), &f.completion)
        .await
        .unwrap();
    assert!(!reserved.published);
    assert_eq!(
        f.store.tool_receipts(&f.owner.run_id).unwrap()[0].status,
        "running"
    );
    assert_eq!(
        f.service()
            .read(&f.owner, &reserved.reference(), 0, 100)
            .await,
        Err(ArtifactError::Forbidden)
    );
    let reference = f
        .service()
        .complete(&f.owner, &f.completion, BODY)
        .await
        .unwrap();
    assert_eq!(reference, reserved.reference());
    assert!(reference.starts_with("artifact:v1:"));
    let receipt = f.store.tool_receipts(&f.owner.run_id).unwrap().remove(0);
    assert_eq!(receipt.status, "terminal");
    assert_eq!(receipt.artifact_ref.as_deref(), Some(reference.as_str()));
    assert_eq!(receipt.receipt, Some(f.completion.receipt.clone()));
    assert_eq!(
        f.service()
            .read(&f.owner, &reference, 0, 16384)
            .await
            .unwrap()
            .data,
        BODY.as_bytes()
    );
    assert_eq!(
        f.service()
            .read(&f.owner, &reference, 3, 7)
            .await
            .unwrap()
            .data,
        &BODY.as_bytes()[3..10]
    );
    assert_eq!(
        f.service()
            .complete(&f.owner, &f.completion, BODY)
            .await
            .unwrap(),
        reference
    );
    let mut changed = f.completion.clone();
    changed.receipt["output"] = "different".into();
    assert_eq!(
        f.service().complete(&f.owner, &changed, BODY).await,
        Err(ArtifactError::Conflict)
    );
    let mut wrong = f.owner.clone();
    wrong.run_generation.0 += 1;
    assert_eq!(
        f.service().read(&wrong, &reference, 0, 10).await,
        Err(ArtifactError::Forbidden)
    );
    let restored = Arc::new(RunStore::open(&f.root.join("runtime.sqlite3")).unwrap());
    let reopened =
        ArtifactService::new(restored.clone(), f.objects.clone(), TenantContext::local());
    assert_eq!(
        reopened
            .read(&f.owner, &reference, 0, 16384)
            .await
            .unwrap()
            .data,
        BODY.as_bytes()
    );
    assert_eq!(
        restored.tool_receipts(&f.owner.run_id).unwrap()[0]
            .artifact_ref
            .as_deref(),
        Some(reference.as_str())
    );
}
#[tokio::test]
async fn file_object_port_has_prepared_native_receipt_idempotency_and_restart_contract() {
    contract(&Fixture::new(None)).await;
}
#[tokio::test]
async fn object_io_failure_keeps_original_tool_running_and_prepared_evidence() {
    let f = Fixture::new(None);
    let outside = f.root.join("outside");
    std::fs::create_dir(&outside).unwrap();
    std::os::unix::fs::symlink(outside, f.root.join("runtime.artifacts")).unwrap();
    assert!(
        f.service()
            .complete(&f.owner, &f.completion, BODY)
            .await
            .is_err()
    );
    assert_eq!(
        f.store.tool_receipts(&f.owner.run_id).unwrap()[0].status,
        "running"
    );
    assert!(
        !f.store
            .reserve_object_artifact(&f.reserve(), &f.completion)
            .await
            .unwrap()
            .published
    );
}
async fn commit_fault(f: &Fixture) {
    let db = f.db();
    db.execute_batch("CREATE TRIGGER object_commit_fault BEFORE UPDATE OF published ON artifact_publications BEGIN SELECT RAISE(ABORT,'注入发布提交故障'); END;").unwrap();
    assert_eq!(
        f.service().complete(&f.owner, &f.completion, BODY).await,
        Err(ArtifactError::StorageUnavailable)
    );
    let receipt = &f.store.tool_receipts(&f.owner.run_id).unwrap()[0];
    assert_eq!(receipt.status, "running");
    assert!(receipt.artifact_ref.is_none());
    assert_eq!(
        f.service()
            .read(&f.owner, &f.reserve().reference(), 0, 100)
            .await,
        Err(ArtifactError::Forbidden)
    );
    // 两阶段最终对象已存在，只有经过原 owner 的明确 reconcile 才能发布；不执行工具。
    db.execute_batch("DROP TRIGGER object_commit_fault;")
        .unwrap();
    let reference = f
        .service()
        .complete(&f.owner, &f.completion, BODY)
        .await
        .unwrap();
    assert_eq!(
        f.service()
            .read(&f.owner, &reference, 0, 100)
            .await
            .unwrap()
            .data,
        BODY.as_bytes()
    );
}
#[tokio::test]
async fn object_reference_and_tool_terminal_rollback_in_the_same_native_transaction() {
    commit_fault(&Fixture::new(None)).await;
}
#[tokio::test]
async fn object_backend_is_bound_to_frozen_run_and_prepared_gc_protects_file() {
    let f = Fixture::new(None);
    let reservation = f
        .store
        .reserve_object_artifact(&f.reserve(), &f.completion)
        .await
        .unwrap();
    f.objects.publish_object(&reservation, BODY).await.unwrap();
    assert_eq!(f.store.gc_artifacts(std::time::Duration::ZERO).unwrap(), 0);
    let mut wrong = reservation.clone();
    wrong.store_id = "a".repeat(64);
    assert_eq!(
        f.store.reserve_object_artifact(&wrong, &f.completion).await,
        Err(ArtifactError::Forbidden)
    );
    let mut foreign = reservation.clone();
    foreign.envelope.scope.tenant_id = "foreign".to_owned().try_into().unwrap();
    assert_eq!(
        f.store
            .reserve_object_artifact(&foreign, &f.completion)
            .await,
        Err(ArtifactError::Forbidden)
    );
}
#[tokio::test]
async fn object_denormalized_column_corruption_and_native_receipt_tampering_fail_closed() {
    let f = Fixture::new(None);
    let reference = f
        .service()
        .complete(&f.owner, &f.completion, BODY)
        .await
        .unwrap();
    let db = f.db();
    let mut altered = f.completion.receipt.clone();
    altered["output"] = "篡改预览但保留 SHA/size".into();
    db.execute(
        "UPDATE tool_executions SET receipt_json=?1 WHERE run_id=?2",
        rusqlite::params![altered.to_string(), f.owner.run_id.0],
    )
    .unwrap();
    assert_eq!(
        f.service().read(&f.owner, &reference, 0, 10).await,
        Err(ArtifactError::Corrupt)
    );
    db.execute(
        "UPDATE tool_executions SET receipt_json='{}' WHERE run_id=?1",
        [&f.owner.run_id.0],
    )
    .unwrap();
    assert_eq!(
        f.service().complete(&f.owner, &f.completion, BODY).await,
        Err(ArtifactError::Corrupt)
    );
    assert_eq!(
        f.service().read(&f.owner, &reference, 0, 10).await,
        Err(ArtifactError::Corrupt)
    );
    db.execute(
        "UPDATE tool_executions SET receipt_json=?1 WHERE run_id=?2",
        rusqlite::params![f.completion.receipt.to_string(), f.owner.run_id.0],
    )
    .unwrap();
    db.execute_batch(
        "DROP TRIGGER artifact_publication_immutable;UPDATE artifact_publications SET bytes=1;",
    )
    .unwrap();
    assert_eq!(
        f.service().read(&f.owner, &reference, 0, 10).await,
        Err(ArtifactError::Corrupt)
    );
}
async fn s3() -> Arc<dyn ArtifactStore> {
    let required = |name| std::env::var(name).expect("独立 S3 lane 必须配置真实 TLS 对象存储");
    Arc::new(
        S3ArtifactStore::new(S3ArtifactConfig {
            endpoint: required("AGENT_S3_ENDPOINT"),
            bucket: required("AGENT_S3_BUCKET"),
            region: required("AGENT_S3_REGION"),
            prefix: required("AGENT_S3_PREFIX"),
            access_key: required("AGENT_S3_ACCESS_KEY"),
            secret_key: required("AGENT_S3_SECRET_KEY"),
            root_ca_pem: Some(std::fs::read(required("AGENT_S3_ROOT_CA")).unwrap()),
        })
        .await
        .unwrap(),
    )
}
#[tokio::test]
#[ignore = "必须使用隔离 TLS MinIO 的真实 S3 lane"]
async fn s3_native_receipt_idempotency_restart_and_commit_fault_contract() {
    let objects = s3().await;
    contract(&Fixture::new(Some(objects.clone()))).await;
    commit_fault(&Fixture::new(Some(objects))).await;
}
#[tokio::test]
async fn s3_cleartext_endpoint_and_unverified_certificate_configuration_fail_closed() {
    for endpoint in [
        "http://127.0.0.1:1",
        "https://user:pass@localhost",
        "https://localhost/path",
        "https://localhost/?x=1",
    ] {
        let config = S3ArtifactConfig {
            endpoint: endpoint.into(),
            bucket: "artifacts".into(),
            region: "us-east-1".into(),
            prefix: "lane".into(),
            access_key: "test".into(),
            secret_key: "test".into(),
            root_ca_pem: None,
        };
        assert!(matches!(
            S3ArtifactStore::new(config).await,
            Err(ArtifactError::InvalidReference)
        ));
    }
}

#[tokio::test]
async fn object_io_does_not_hold_canonical_sqlite_transaction_and_late_owner_is_fenced() {
    let f = Fixture::new(None);
    let reserved = f
        .store
        .reserve_object_artifact(&f.reserve(), &f.completion)
        .await
        .unwrap();
    let db = f.db();
    db.execute_batch("BEGIN IMMEDIATE;").unwrap();
    // 另一个真实连接占有数据库写事务时，对象 I/O 仍可完成；没有跨 I/O 的 DB lease。
    let verified = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        f.objects.publish_object(&reserved, BODY),
    )
    .await
    .unwrap()
    .unwrap();
    db.execute(
        "UPDATE runs SET generation=generation+1 WHERE id=?1",
        [&f.owner.run_id.0],
    )
    .unwrap();
    db.execute_batch("COMMIT;").unwrap();
    assert_eq!(
        f.store
            .commit_object_artifact(&reserved, &f.completion, &verified)
            .await,
        Err(ArtifactError::Forbidden)
    );
    assert_eq!(
        f.store.tool_receipts(&f.owner.run_id).unwrap()[0].status,
        "running"
    );
}
#[test]
fn object_schema_upgrade_has_verified_backup_atomic_failure_and_future_version_rejection() {
    let f = Fixture::new(None);
    let db = f.db();
    db.execute_batch("DELETE FROM role_bindings WHERE tenant_id='local-tenant' AND subject_id='local-cron-workload'; DELETE FROM principals WHERE tenant_id='local-tenant' AND subject_id='local-cron-workload'; DELETE FROM schema_migrations WHERE version=27; DROP TRIGGER native_event_provenance_insert; DROP TRIGGER immutable_event_provenance_update; DROP TRIGGER immutable_event_stamp_delete; ALTER TABLE events DROP COLUMN view_provenance; DELETE FROM schema_migrations WHERE version=26; DROP TRIGGER bind_lifetime_insert; DROP TRIGGER bind_lifetime_update; DROP TRIGGER immutable_run_incarnation; DROP TRIGGER immutable_lifetime_owner_update; DROP TRIGGER immutable_lifetime_owner_delete; DROP TABLE session_lifetime_owners; DELETE FROM schema_migrations WHERE version=25; DROP TABLE session_writer_leases; DROP INDEX session_scope_writer_key; DROP INDEX tenant_writer_domain; DELETE FROM schema_migrations WHERE version=24; DROP TRIGGER artifact_publication_immutable;DROP TRIGGER artifact_publication_monotonic;DROP TABLE artifact_publications;DELETE FROM schema_migrations WHERE version=23;CREATE TRIGGER object_migration_fault BEFORE INSERT ON schema_migrations WHEN NEW.version=23 BEGIN SELECT RAISE(ABORT,'注入 v23 迁移故障'); END;").unwrap();
    assert!(RunStore::open(&f.root.join("runtime.sqlite3")).is_err());
    assert_eq!(
        db.query_row("SELECT max(version) FROM schema_migrations", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        22
    );
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM sqlite_master WHERE name='artifact_publications'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    assert!(f.root.join("runtime.v22.backup.sqlite3").exists());
    assert!(f.root.join("runtime.v22.backup.verified.json").exists());
    db.execute_batch("DROP TRIGGER object_migration_fault;")
        .unwrap();
    let upgraded = RunStore::open(&f.root.join("runtime.sqlite3")).unwrap();
    assert_eq!(upgraded.health_report().unwrap()["schema_version"], 27);
    db.execute("INSERT INTO schema_migrations VALUES(28,0)", [])
        .unwrap();
    assert!(RunStore::open(&f.root.join("runtime.sqlite3")).is_err());
}

#[tokio::test]
async fn interrupted_native_tool_cannot_be_replayed_from_prepared_artifact_evidence() {
    let f = Fixture::new(None);
    let reserved = f
        .store
        .reserve_object_artifact(&f.reserve(), &f.completion)
        .await
        .unwrap();
    f.db().execute("UPDATE tool_executions SET status='terminal',outcome='outcome_unknown',receipt_json='{}' WHERE run_id=?1",[&f.owner.run_id.0]).unwrap();
    assert_eq!(
        f.store
            .reserve_object_artifact(&reserved, &f.completion)
            .await,
        Err(ArtifactError::Conflict)
    );
    assert_eq!(
        f.service().complete(&f.owner, &f.completion, BODY).await,
        Err(ArtifactError::Conflict)
    );
    assert!(!f.root.join("runtime.artifacts").exists());
}
