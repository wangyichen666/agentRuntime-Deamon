//! 原 Hook 观察事实的范围、只读版本与保存预算合同。
use agent_core::*;
use agent_storage::{HookRepository, RunStore, RuntimeError, SessionLifecycle, SessionQuery};
use rusqlite::{Connection, params};
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    root: std::path::PathBuf,
    store: RunStore,
    db: Connection,
    meta: SessionMetadata,
    payload: HookPayload,
}
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "ma-hook-observation-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let path = root.join("runtime.sqlite3");
        let store = RunStore::open(&path).unwrap();
        let meta = store
            .create_session(&SessionKey("hook-observation".into()))
            .unwrap();
        let payload = HookPayload {
            schema_version: 1,
            event: HookEvent::Notification,
            session_key: meta.key.clone(),
            session_lifetime_id: meta.lifetime.clone(),
            owner: None,
            cwd: ".".into(),
            channel: "daemon_rpc".into(),
            permission_mode: "observation".into(),
            route_digest: None,
            data: serde_json::json!({"private":"私有观察输入"}),
            operation_id: "notification-operation".into(),
        };
        store.claim_hook(&payload).unwrap();
        let db = Connection::open(path).unwrap();
        Self {
            root,
            store,
            db,
            meta,
            payload,
        }
    }
    fn clock(&self) -> u64 {
        self.db
            .query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap()
    }
    fn foreign(&self) {
        self.db.execute_batch("INSERT INTO organizations VALUES('foreign');INSERT INTO tenants VALUES('foreign','foreign');INSERT INTO projects VALUES('foreign','foreign');INSERT INTO agents VALUES('foreign','foreign','foreign');UPDATE session_scopes SET tenant_id='foreign',project_id='foreign',agent_uid='foreign' WHERE session_id='hook-observation'").unwrap();
    }
    fn publication(&self) -> HookPublication {
        self.store.hook_publications().unwrap().remove(0)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
#[test]
fn hook_history_foreign_must_be_forbidden_before_corrupt_private_json() {
    let f = Fixture::new();
    f.foreign();
    f.db.execute("UPDATE hook_outcomes SET outcome_json='not-json'", [])
        .unwrap();
    let before = f.clock();
    let result = f.store.hook_readback(&f.meta.key, &f.meta.lifetime, 0, 32);
    assert!(
        matches!(
            result,
            Err(RuntimeError::Authorization(AuthorizationError::Forbidden))
        ),
        "必须先拒绝范围，实际 {result:?}"
    );
    assert_eq!(f.clock(), before);
}
#[test]
fn pending_hook_publications_do_not_expose_foreign_lifecycle() {
    let f = Fixture::new();
    f.foreign();
    let before = f.clock();
    let result = f.store.hook_publications().unwrap();
    assert!(
        result.is_empty(),
        "不应交付外域待发布记录，实际行数 {}",
        result.len()
    );
    assert_eq!(f.clock(), before);
}
#[test]
fn hook_publication_ack_foreign_must_not_write_dispatched() {
    let f = Fixture::new();
    let p = f.publication();
    f.foreign();
    let before = f.clock();
    let result = f.store.acknowledge_hook_publication(&p.operation_id);
    assert!(
        matches!(
            result,
            Err(RuntimeError::Authorization(AuthorizationError::Forbidden))
        ),
        "必须先拒绝范围，实际 {result:?}"
    );
    assert_eq!(f.clock(), before);
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT dispatched FROM hook_publications", [], |r| r.get(0))
            .unwrap(),
        0
    );
}
#[test]
fn hook_readback_does_not_create_a_new_snapshot_revision() {
    let f = Fixture::new();
    let before = f.clock();
    let result = f
        .store
        .hook_readback(&f.meta.key, &f.meta.lifetime, 0, 32)
        .unwrap();
    assert_eq!(result.outcomes.len(), 1);
    assert_eq!(result.snapshot_revision.0, before);
    assert_eq!(f.clock(), before, "只读不能推进原全局版本");
}
#[test]
fn saved_hook_publication_operation_must_match_original_sql_key() {
    let f = Fixture::new();
    let mut p = f.publication();
    p.operation_id = "other-valid-operation".into();
    f.db.execute(
        "UPDATE hook_publications SET payload_json=?1",
        [serde_json::to_string(&p).unwrap()],
    )
    .unwrap();
    let before = f.clock();
    let result = f.store.hook_publications().map(|v| v.len());
    assert!(
        matches!(
            result,
            Err(RuntimeError::Coordination(CoordinationError::Corrupt))
        ),
        "保存行身份不一致必须损坏，实际 {result:?}"
    );
    assert_eq!(f.clock(), before);
}
#[test]
fn saved_legal_hook_effect_has_whole_outcome_byte_budget() {
    let f = Fixture::new();
    let Admission::New(run) = f
        .store
        .admit_with_route(
            f.meta.key.clone(),
            RequestId::Number(1),
            "输入",
            AdmissionMode::Queue,
            None,
        )
        .unwrap()
    else {
        panic!("准入")
    };
    let mut payload = f.payload.clone();
    payload.event = HookEvent::UserPromptSubmit;
    payload.owner = Some(f.store.run_owner(&run.run_id).unwrap());
    let saved = HookOutcome {
        payload,
        status: HookStatus::Succeeded,
        effect: Some(HookEffect::Deny {
            reason: "字".repeat(400000),
        }),
        failure: None,
    };
    f.db.execute(
        "UPDATE hook_outcomes SET outcome_json=?1",
        [serde_json::to_string(&saved).unwrap()],
    )
    .unwrap();
    let before = f.clock();
    let result = f
        .store
        .hook_readback(&f.meta.key, &f.meta.lifetime, 0, 32)
        .map(|v| v.outcomes.len());
    assert!(
        matches!(
            result,
            Err(RuntimeError::Coordination(CoordinationError::Corrupt))
        ),
        "整个结果也须SQL预算，实际 {result:?}"
    );
    assert_eq!(f.clock(), before);
}
#[test]
fn saved_hook_run_owner_must_match_canonical_run_incarnation() {
    let f = Fixture::new();
    let Admission::New(run) = f
        .store
        .admit_with_route(
            f.meta.key.clone(),
            RequestId::Number(1),
            "输入",
            AdmissionMode::Queue,
            None,
        )
        .unwrap()
    else {
        panic!("准入")
    };
    let mut payload = f.payload.clone();
    let mut owner = f.store.run_owner(&run.run_id).unwrap();
    owner.run_generation.0 += 1;
    payload.owner = Some(owner);
    let saved = HookOutcome {
        payload,
        status: HookStatus::Running,
        effect: None,
        failure: None,
    };
    f.db.execute(
        "UPDATE hook_outcomes SET outcome_json=?1",
        params![serde_json::to_string(&saved).unwrap()],
    )
    .unwrap();
    let before = f.clock();
    let result = f
        .store
        .hook_readback(&f.meta.key, &f.meta.lifetime, 0, 32)
        .map(|v| v.outcomes.len());
    assert!(
        matches!(
            result,
            Err(RuntimeError::Coordination(CoordinationError::Corrupt))
        ),
        "保存owner不能借另一generation背书，实际 {result:?}"
    );
    assert_eq!(f.clock(), before);
}

#[tokio::test]
async fn async_hook_history_preserves_scope_errors_cursor_and_retired_lifetime_without_writer() {
    use agent_storage_ports::HookObservationRepository;
    let f = Fixture::new();
    for i in 2..=3 {
        let mut p = f.payload.clone();
        p.operation_id = format!("operation-{i}");
        f.store.claim_hook(&p).unwrap();
    }
    f.db.execute(
        "UPDATE hook_outcomes SET outcome_json='not-json' WHERE operation_id='operation-3'",
        [],
    )
    .unwrap();
    let scope = TenantContext::local();
    let before = f.clock();
    let first = f
        .store
        .read_hook_history(&scope, &f.meta.key, &f.meta.lifetime, 0, 2)
        .await
        .unwrap();
    assert_eq!(first.outcomes.len(), 2);
    assert!(first.has_more);
    assert_eq!(f.clock(), before);
    assert_eq!(
        f.store
            .read_hook_history(&scope, &f.meta.key, &f.meta.lifetime, first.cursor, 2)
            .await
            .unwrap_err(),
        CoordinationError::Corrupt.into()
    );
    for dimension in ["organization", "tenant", "project", "agent"] {
        let mut other = scope.clone();
        match dimension {
            "organization" => other.organization_id = "foreign".to_owned().try_into().unwrap(),
            "tenant" => other.tenant_id = "foreign".to_owned().try_into().unwrap(),
            "project" => other.project_id = "foreign".to_owned().try_into().unwrap(),
            _ => other.agent_uid = "foreign".to_owned().try_into().unwrap(),
        };
        assert_eq!(
            f.store
                .read_hook_history(&other, &f.meta.key, &f.meta.lifetime, 0, 2)
                .await
                .unwrap_err(),
            AuthorizationError::Forbidden.into()
        );
    }
    let old = f.meta.lifetime.clone();
    f.store.end_session(&f.meta.key, &old, false).unwrap();
    assert_ne!(
        f.store
            .session_metadata(&f.meta.key)
            .unwrap()
            .unwrap()
            .lifetime,
        old
    );
    let before = f.clock();
    let retired = f
        .store
        .read_hook_history(&scope, &f.meta.key, &old, 0, 2)
        .await
        .unwrap();
    assert_eq!(retired.outcomes.len(), 2);
    assert_eq!(retired.cursor, first.cursor);
    assert_eq!(f.clock(), before);
}
#[tokio::test]
async fn async_ack_foreign_scope_precedes_corrupt_publication_and_never_dispatches() {
    use agent_storage_ports::HookObservationRepository;
    let f = Fixture::new();
    let p = f.publication();
    f.foreign();
    f.db.execute("UPDATE hook_publications SET payload_json='not-json'", [])
        .unwrap();
    let before = f.clock();
    assert_eq!(
        f.store
            .acknowledge_pending_hook(&TenantContext::local(), &p)
            .await
            .unwrap_err(),
        AuthorizationError::Forbidden.into()
    );
    assert_eq!(f.clock(), before);
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT dispatched FROM hook_publications", [], |r| r.get(0))
            .unwrap(),
        0
    );
}
#[tokio::test]
async fn original_pending_hook_ack_preserves_retired_publication_identity_and_idempotence() {
    use agent_storage_ports::HookObservationRepository;
    let f = Fixture::new();
    let p = f.publication();
    f.store
        .end_session(&f.meta.key, &f.meta.lifetime, true)
        .unwrap();
    f.store.create_session(&f.meta.key).unwrap();
    let scope = TenantContext::local();
    let before = f.clock();
    let pending = f.store.read_pending_hooks(&scope).await.unwrap();
    assert_eq!(pending.len(), 3);
    assert_eq!(pending[0].session_lifetime_id, p.session_lifetime_id);
    f.store.acknowledge_pending_hook(&scope, &p).await.unwrap();
    f.store.acknowledge_pending_hook(&scope, &p).await.unwrap();
    assert_eq!(f.clock(), before);
    assert_eq!(f.store.read_pending_hooks(&scope).await.unwrap().len(), 2);
    let mut wrong = p.clone();
    wrong.event = HookEvent::SessionEnd;
    assert_eq!(
        f.store
            .acknowledge_pending_hook(&scope, &wrong)
            .await
            .unwrap_err(),
        CoordinationError::Conflict.into()
    );
    assert_eq!(f.clock(), before);
}
#[tokio::test]
async fn pending_sql_scope_filter_precedes_limit_and_never_starves_local_publications() {
    use agent_storage_ports::HookObservationRepository;
    let f = Fixture::new();
    let p = f.publication();
    f.foreign();
    for i in 0..70 {
        let mut other = p.clone();
        other.operation_id = format!("foreign-publication-{i}");
        f.db.execute(
            "INSERT INTO hook_publications VALUES(?1,?2,0)",
            params![other.operation_id, serde_json::to_string(&other).unwrap()],
        )
        .unwrap();
    }
    let own = f
        .store
        .create_session(&SessionKey("own-after-foreign".into()))
        .unwrap();
    let before = f.clock();
    let pending = f
        .store
        .read_pending_hooks(&TenantContext::local())
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].session_key, own.key);
    assert_eq!(f.clock(), before);
}
#[test]
fn legal_hook_pages_reject_cumulative_budget_without_truncation_or_any_write() {
    let f = Fixture::new();
    let Admission::New(run) = f
        .store
        .admit_with_route(
            f.meta.key.clone(),
            RequestId::Number(1),
            "输入",
            AdmissionMode::Queue,
            None,
        )
        .unwrap()
    else {
        panic!("准入")
    };
    let owner = f.store.run_owner(&run.run_id).unwrap();
    for i in 0..26 {
        let mut payload = f.payload.clone();
        payload.operation_id = format!("large-{i}");
        payload.owner = Some(owner.clone());
        payload.event = HookEvent::UserPromptSubmit;
        let outcome = HookOutcome {
            payload,
            status: HookStatus::Succeeded,
            effect: Some(HookEffect::Deny {
                reason: "x".repeat(40000),
            }),
            failure: None,
        };
        f.db.execute(
            "INSERT INTO hook_outcomes VALUES(?1,?2,?3,?4)",
            params![
                outcome.payload.operation_id,
                f.meta.key.0,
                f.meta.lifetime.0,
                serde_json::to_string(&outcome).unwrap()
            ],
        )
        .unwrap();
    }
    let before = f.clock();
    assert_eq!(
        f.store
            .hook_readback(&f.meta.key, &f.meta.lifetime, 0, 1)
            .unwrap()
            .outcomes
            .len(),
        1
    );
    assert!(matches!(
        f.store.hook_readback(&f.meta.key, &f.meta.lifetime, 0, 32),
        Err(RuntimeError::Coordination(CoordinationError::Corrupt))
    ));
    assert_eq!(f.clock(), before);
}
#[tokio::test]
async fn oversized_or_malformed_saved_hook_publication_is_not_skipped_or_repaired() {
    use agent_storage_ports::HookObservationRepository;
    for mode in ["oversized", "malformed", "wrong-operation"] {
        let f = Fixture::new();
        let p = f.publication();
        let raw = match mode {
            "oversized" => {
                let mut p = p.clone();
                p.operation_id = "字".repeat(400000);
                serde_json::to_string(&p).unwrap()
            }
            "malformed" => "not-json".into(),
            _ => {
                let mut p = p.clone();
                p.operation_id = "other-valid-operation".into();
                serde_json::to_string(&p).unwrap()
            }
        };
        f.db.execute("UPDATE hook_publications SET payload_json=?1", [raw])
            .unwrap();
        let before = f.clock();
        let scope = TenantContext::local();
        assert_eq!(
            f.store.read_pending_hooks(&scope).await.unwrap_err(),
            CoordinationError::Corrupt.into()
        );
        assert_eq!(
            f.store
                .acknowledge_pending_hook(&scope, &p)
                .await
                .unwrap_err(),
            CoordinationError::Corrupt.into()
        );
        assert_eq!(f.clock(), before);
        assert_eq!(
            f.db.query_row::<u64, _, _>("SELECT dispatched FROM hook_publications", [], |r| r
                .get(0))
                .unwrap(),
            0
        );
    }
}

#[tokio::test]
async fn publication_budget_preserves_legitimate_escaped_session_keys() {
    use agent_storage_ports::HookObservationRepository;
    let f = Fixture::new();
    let key = SessionKey("\u{1}".repeat(1024));
    let meta = f.store.create_session(&key).unwrap();
    let scope = TenantContext::local();
    let pending = f.store.read_pending_hooks(&scope).await.unwrap();
    let publication = pending
        .into_iter()
        .find(|p| p.session_key == meta.key)
        .unwrap();
    let before = f.clock();
    f.store
        .acknowledge_pending_hook(&scope, &publication)
        .await
        .unwrap();
    assert_eq!(f.clock(), before);
}
