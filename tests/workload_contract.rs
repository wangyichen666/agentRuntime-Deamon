//! Local后台准入与原session/run同事务、独立主体及迁移合同。
use agent_core::*;
use agent_storage::{MaintenanceRepository, RunStore};
use agent_storage_ports::{IdentityReadRepository, RunExecutionRepository};
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    root: std::path::PathBuf,
    store: RunStore,
    db: Connection,
}
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "ma-workload-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let path = root.join("runtime.sqlite3");
        let store = RunStore::open(&path)
            .unwrap()
            .enable_session_writer()
            .unwrap();
        Self {
            root,
            store,
            db: Connection::open(path).unwrap(),
        }
    }
    fn admission(&self) -> RunAdmission {
        RunAdmission {
            workflow_node: None,
            session_key: SessionKey("session-cron-contract".into()),
            expected_lifetime: None,
            request_id: RequestId::Number(1),
            input: "background".into(),
            mode: AdmissionMode::RejectIfBusy,
            plan_execution: None,
        }
    }
    fn snapshot(&self) -> RunSnapshot {
        serde_json::from_value(serde_json::json!({"entry_channel":"cron","tools":[],"cwd":".","permission_mode":"risk_approval","sandbox_requested":"native","sandbox_effective":"native","context_read_only":false,"context_token_budget":4096,"context_policy_fingerprint":"4096:2:60:85","tool_catalog_digest":format!("{:x}",Sha256::digest(b"[]")),"config_generation":0})).unwrap()
    }
    async fn authorization(&self) -> AuthorizationRequest {
        let identity = self
            .store
            .read_identity(
                &TenantContext::local(),
                &LOCAL_CRON_SUBJECT.to_owned().try_into().unwrap(),
            )
            .await
            .unwrap();
        AuthorizationRequest {
            context: RequestContext {
                scope: identity.scope,
                principal: identity.principal,
                request_id: RequestId::Number(1),
                trace: TraceContext {
                    trace_id: "workload-contract".into(),
                },
            },
            expected_policy: identity.policy.version,
            action: Action::SessionPrompt,
            attributes: AuthorizationAttributes {
                workspace: ".".into(),
                entry_channel: HookChannel::Cron,
                tool_effect: None,
                timestamp_ms: 0,
                network: NetworkBoundary::None,
            },
        }
    }
    fn count(&self, table: &str) -> u64 {
        self.db
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    }
    fn clock(&self) -> u64 {
        self.db
            .query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap()
    }
    fn assert_no_work(&self, before: u64) {
        for table in [
            "sessions",
            "session_scopes",
            "session_lifetime_owners",
            "runs",
            "run_authorizations",
            "session_writer_leases",
            "event_outbox",
            "hook_publications",
        ] {
            assert_eq!(self.count(table), 0, "{table}");
        }
        assert_eq!(self.clock(), before);
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
struct DenyCreate;
impl AuthorizationVerifier for DenyCreate {
    fn verify(
        &self,
        request: &AuthorizationRequest,
        identity: &IdentityReadback,
    ) -> Result<(), AuthorizationError> {
        if request.action == Action::SessionCreate {
            Err(AuthorizationError::Forbidden)
        } else {
            agent_policy::PolicyEngine.verify(request, identity)
        }
    }
}
#[tokio::test]
async fn session_create_denied_after_prompt_authorized_is_zero_canonical_work() {
    let f = Fixture::new();
    let before = f.clock();
    let auth = f.authorization().await;
    assert_eq!(
        f.store
            .admit_workload_run(&f.admission(), &f.snapshot(), &auth, Arc::new(DenyCreate))
            .await
            .unwrap_err(),
        AuthorizationError::Forbidden.into()
    );
    f.assert_no_work(before);
}
#[tokio::test]
async fn workload_authorization_insert_fault_rolls_back_session_start_lifetime_proof_run_and_clock()
{
    let f = Fixture::new();
    let before = f.clock();
    let auth = f.authorization().await;
    f.db.execute_batch("CREATE TRIGGER workload_fault BEFORE INSERT ON run_authorizations BEGIN SELECT RAISE(ABORT,'workload freeze fault'); END;").unwrap();
    assert_eq!(
        f.store
            .admit_workload_run(
                &f.admission(),
                &f.snapshot(),
                &auth,
                Arc::new(agent_policy::PolicyEngine)
            )
            .await
            .unwrap_err(),
        CoordinationError::StorageUnavailable.into()
    );
    f.assert_no_work(before);
    f.db.execute_batch("DROP TRIGGER workload_fault").unwrap();
    f.store
        .admit_workload_run(
            &f.admission(),
            &f.snapshot(),
            &auth,
            Arc::new(agent_policy::PolicyEngine),
        )
        .await
        .unwrap();
    assert_eq!(f.count("sessions"), 1);
    assert_eq!(f.count("run_authorizations"), 1);
}
#[tokio::test]
async fn workload_cannot_borrow_human_identity_or_forge_scope_channel_or_expected_lifetime() {
    let f = Fixture::new();
    let before = f.clock();
    for dim in 0..7 {
        let mut auth = f.authorization().await;
        let mut admission = f.admission();
        let mut snapshot = f.snapshot();
        match dim {
            0 => auth.context.principal.subject_id = "local-subject".to_owned().try_into().unwrap(),
            1 => auth.context.principal.kind = SubjectKind::Human,
            2 => auth.context.scope.tenant_id = "other".to_owned().try_into().unwrap(),
            3 => auth.attributes.entry_channel = HookChannel::DaemonRpc,
            4 => snapshot.entry_channel = HookChannel::DaemonRpc,
            5 => admission.expected_lifetime = Some(SessionLifetimeId("unowned".into())),
            _ => auth.context.principal.revocation_epoch += 1,
        }
        assert!(
            f.store
                .admit_workload_run(
                    &admission,
                    &snapshot,
                    &auth,
                    Arc::new(agent_policy::PolicyEngine)
                )
                .await
                .is_err(),
            "{dim}"
        );
        f.assert_no_work(before);
    }
}
#[tokio::test]
async fn workload_same_request_reuses_original_owner_without_second_session_start_or_authorization()
{
    let f = Fixture::new();
    let auth = f.authorization().await;
    let admission = f.admission();
    let snapshot = f.snapshot();
    let (first, owner) = f
        .store
        .admit_workload_run(
            &admission,
            &snapshot,
            &auth,
            Arc::new(agent_policy::PolicyEngine),
        )
        .await
        .unwrap();
    assert!(matches!(first, Admission::New(_)));
    let before = f.clock();
    let (again, actual) = f
        .store
        .admit_workload_run(
            &admission,
            &snapshot,
            &auth,
            Arc::new(agent_policy::PolicyEngine),
        )
        .await
        .unwrap();
    assert!(matches!(again, Admission::Existing(_)));
    assert_eq!(owner, actual);
    assert_eq!(f.clock(), before);
    assert_eq!(f.count("sessions"), 1);
    assert_eq!(f.count("hook_publications"), 1);
    assert_eq!(f.count("run_authorizations"), 1);
    let raw: String =
        f.db.query_row("SELECT snapshot_json FROM run_authorizations", [], |r| {
            r.get(0)
        })
        .unwrap();
    let frozen: AuthorizationSnapshot = serde_json::from_str(&raw).unwrap();
    assert_eq!(frozen.context.principal.kind, SubjectKind::Workload);
    assert_eq!(
        frozen.context.principal.subject_id.as_str(),
        LOCAL_CRON_SUBJECT
    );
    assert_eq!(frozen.roles, vec![EnterpriseRole::Developer]);
    assert_eq!(frozen.envelope.owner, owner);
}
#[tokio::test]
async fn role_removal_is_checked_in_admission_and_produces_no_new_session_or_run() {
    let f = Fixture::new();
    let before = f.clock();
    let auth = f.authorization().await;
    f.db.execute(
        "DELETE FROM role_bindings WHERE subject_id=?1",
        [LOCAL_CRON_SUBJECT],
    )
    .unwrap();
    assert_eq!(
        f.store
            .admit_workload_run(
                &f.admission(),
                &f.snapshot(),
                &auth,
                Arc::new(agent_policy::PolicyEngine)
            )
            .await
            .unwrap_err(),
        AuthorizationError::Forbidden.into()
    );
    f.assert_no_work(before);
}
#[test]
fn workload_v27_migration_has_verified_v26_backup_atomic_fault_and_future28_rejection() {
    let f = Fixture::new();
    let path = f.root.join("runtime.sqlite3");
    f.db.execute_batch("DELETE FROM role_bindings WHERE subject_id='local-cron-workload';DELETE FROM principals WHERE subject_id='local-cron-workload';DELETE FROM schema_migrations WHERE version=27;CREATE TRIGGER workload_migration_fault BEFORE INSERT ON schema_migrations WHEN NEW.version=27 BEGIN SELECT RAISE(ABORT,'workload migration fault');END;").unwrap();
    assert!(RunStore::open(&path).is_err());
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT max(version) FROM schema_migrations", [], |r| r
            .get(0))
            .unwrap(),
        26
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>(
            "SELECT count(*) FROM principals WHERE subject_id='local-cron-workload'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    let backup = path.with_extension("v26.backup.sqlite3");
    let bytes = std::fs::read(&backup).unwrap();
    let marker: serde_json::Value = serde_json::from_slice(
        &std::fs::read(path.with_extension("v26.backup.verified.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(marker["schema"], 26);
    assert_eq!(marker["sha256"], format!("{:x}", Sha256::digest(&bytes)));
    assert_eq!(marker["bytes"], bytes.len());
    f.db.execute_batch("DROP TRIGGER workload_migration_fault")
        .unwrap();
    let upgraded = RunStore::open(&path).unwrap();
    assert_eq!(upgraded.health_report().unwrap()["schema_version"], 27);
    drop(upgraded);
    f.db.execute_batch("INSERT INTO schema_migrations VALUES(28,0)")
        .unwrap();
    assert!(RunStore::open(&path).is_err());
}

#[tokio::test]
async fn existing_foreign_session_is_denied_before_reading_metadata_or_acquiring_writer() {
    use agent_storage_ports::SessionLifecycleRepository;
    let f = Fixture::new();
    let admission = f.admission();
    f.store
        .commit_session_command(
            &TenantContext::local(),
            &SessionCommand::Create {
                key: admission.session_key.clone(),
            },
            None,
            "fixture-create",
        )
        .await
        .unwrap();
    f.db.execute_batch("INSERT INTO organizations VALUES('foreign');INSERT INTO tenants VALUES('foreign','foreign');INSERT INTO projects VALUES('foreign','foreign');INSERT INTO agents VALUES('foreign','foreign','foreign');DELETE FROM session_writer_leases WHERE session_key='session-cron-contract';UPDATE session_scopes SET tenant_id='foreign',project_id='foreign',agent_uid='foreign' WHERE session_id='session-cron-contract';INSERT INTO session_writer_leases SELECT 'foreign','foreign','foreign','foreign',session_id,lifetime,'foreign-writer',1,9223372036854775807 FROM session_heads WHERE session_id='session-cron-contract'").unwrap();
    let before = f.clock();
    let leases = f.count("session_writer_leases");
    let hooks = f.count("hook_publications");
    let auth = f.authorization().await;
    assert_eq!(
        f.store
            .admit_workload_run(
                &admission,
                &f.snapshot(),
                &auth,
                Arc::new(agent_policy::PolicyEngine)
            )
            .await
            .unwrap_err(),
        AuthorizationError::Forbidden.into()
    );
    assert_eq!(f.count("sessions"), 1);
    assert_eq!(f.count("runs"), 0);
    assert_eq!(f.count("session_writer_leases"), leases);
    assert_eq!(f.count("hook_publications"), hooks);
    assert_eq!(f.clock(), before);
}
