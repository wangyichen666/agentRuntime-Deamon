//! 原 lifecycle command/receipt 的授权合同；RED 用原真实事务。
use agent_core::*;
use agent_storage::{RunStore, SessionLifecycle};
use agent_storage_ports::SessionLifecycleRepository;
use rusqlite::Connection;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    root: PathBuf,
    store: RunStore,
    db: Connection,
    meta: SessionMetadata,
}
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "ma-lifecycle-port-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let store = RunStore::open(&root.join("runtime.sqlite3")).unwrap();
        let meta = store
            .create_session(&SessionKey("lifecycle".into()))
            .unwrap();
        let db = Connection::open(root.join("runtime.sqlite3")).unwrap();
        Self {
            root,
            store,
            db,
            meta,
        }
    }
    fn make_foreign(&self) {
        self.db.execute_batch("INSERT INTO organizations VALUES('other-org');INSERT INTO tenants VALUES('other-tenant','other-org');INSERT INTO projects VALUES('other-project','other-tenant');INSERT INTO agents VALUES('other-agent','other-tenant','other-project');UPDATE session_scopes SET tenant_id='other-tenant',project_id='other-project',agent_uid='other-agent' WHERE session_id='lifecycle';").unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
async fn commit(
    store: &RunStore,
    command: &SessionCommand,
    expected: Option<&SessionLifetimeId>,
    operation: &str,
) -> Result<SessionMetadata, CoordinationError> {
    store
        .commit_session_command(&TenantContext::local(), command, expected, operation)
        .await
}
async fn receipt(
    store: &RunStore,
    key: &SessionKey,
    life: &SessionLifetimeId,
    operation: &str,
) -> Result<Option<SessionMetadata>, CoordinationError> {
    store
        .read_close_receipt(&TenantContext::local(), key, life, operation)
        .await
}

#[tokio::test]
async fn lifecycle_mutation_never_closes_a_foreign_session_or_creates_its_receipt() {
    let f = Fixture::new();
    f.make_foreign();
    let clock: u64 =
        f.db.query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
    let result = commit(
        &f.store,
        &SessionCommand::Close {
            key: f.meta.key.clone(),
        },
        Some(&f.meta.lifetime),
        "foreign-close",
    )
    .await;
    assert!(
        matches!(result, Err(CoordinationError::Forbidden)),
        "不能提交越域会话和 Hook/receipt：{result:?}"
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap(),
        clock
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>(
            "SELECT count(*) FROM lifecycle_receipts WHERE operation_id='foreign-close'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn lifecycle_replay_and_close_receipt_check_scope_before_returning_historical_result() {
    let f = Fixture::new();
    let command = SessionCommand::Close {
        key: f.meta.key.clone(),
    };
    commit(&f.store, &command, Some(&f.meta.lifetime), "prior-close")
        .await
        .unwrap();
    f.make_foreign();
    for result in [
        commit(&f.store, &command, Some(&f.meta.lifetime), "prior-close")
            .await
            .map(Some),
        receipt(&f.store, &f.meta.key, &f.meta.lifetime, "prior-close").await,
    ] {
        assert!(
            matches!(result, Err(CoordinationError::Forbidden)),
            "已有 operation 不能绕过资源 scope：{result:?}"
        );
    }
}

#[tokio::test]
async fn lifecycle_scope_and_unknown_resources_do_not_reveal_operations() {
    let f = Fixture::new();
    let command = SessionCommand::Close {
        key: f.meta.key.clone(),
    };
    f.store
        .commit_session_command(
            &TenantContext::local(),
            &command,
            Some(&f.meta.lifetime),
            "close-known",
        )
        .await
        .unwrap();
    let clock: u64 =
        f.db.query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
    for dimension in 0..4 {
        let mut scope = TenantContext::local();
        match dimension {
            0 => scope.organization_id = "foreign".to_owned().try_into().unwrap(),
            1 => scope.tenant_id = "foreign".to_owned().try_into().unwrap(),
            2 => scope.project_id = "foreign".to_owned().try_into().unwrap(),
            _ => scope.agent_uid = "foreign".to_owned().try_into().unwrap(),
        }
        assert_eq!(
            f.store
                .commit_session_command(&scope, &command, Some(&f.meta.lifetime), "close-known")
                .await
                .unwrap_err(),
            CoordinationError::Forbidden
        );
        assert_eq!(
            f.store
                .read_close_receipt(&scope, &f.meta.key, &f.meta.lifetime, "close-known")
                .await
                .unwrap_err(),
            CoordinationError::Forbidden
        );
    }
    let missing = SessionKey("absent".into());
    assert_eq!(
        f.store
            .commit_session_command(
                &TenantContext::local(),
                &SessionCommand::Close {
                    key: missing.clone()
                },
                Some(&f.meta.lifetime),
                "unknown"
            )
            .await
            .unwrap_err(),
        CoordinationError::Forbidden
    );
    assert_eq!(
        f.store
            .read_close_receipt(
                &TenantContext::local(),
                &missing,
                &f.meta.lifetime,
                "unknown"
            )
            .await
            .unwrap_err(),
        CoordinationError::Forbidden
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap(),
        clock
    );
}

#[tokio::test]
async fn original_lifecycle_transaction_rolls_back_head_hook_lifetime_and_receipt_then_replays_once()
 {
    use agent_storage::SessionQuery;
    let f = Fixture::new();
    let command = SessionCommand::End {
        key: f.meta.key.clone(),
        delete: false,
    };
    let clock: u64 =
        f.db.query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
    let owners: u64 =
        f.db.query_row("SELECT count(*) FROM session_lifetime_owners", [], |r| {
            r.get(0)
        })
        .unwrap();
    let hooks: u64 =
        f.db.query_row("SELECT count(*) FROM hook_publications", [], |r| r.get(0))
            .unwrap();
    f.db.execute_batch("CREATE TRIGGER lifecycle_insert_fault BEFORE INSERT ON lifecycle_receipts BEGIN SELECT RAISE(ABORT,'fixture receipt fault'); END;").unwrap();
    let failed = f
        .store
        .commit_session_command(
            &TenantContext::local(),
            &command,
            Some(&f.meta.lifetime),
            "clear-once",
        )
        .await
        .unwrap_err();
    assert_eq!(failed, CoordinationError::StorageUnavailable);
    assert!(!failed.to_string().contains("receipt"));
    assert_eq!(
        f.store
            .session_metadata(&f.meta.key)
            .unwrap()
            .unwrap()
            .lifetime,
        f.meta.lifetime
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap(),
        clock
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM session_lifetime_owners", [], |r| r
            .get(0))
            .unwrap(),
        owners
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM hook_publications", [], |r| r.get(0))
            .unwrap(),
        hooks
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>(
            "SELECT count(*) FROM lifecycle_receipts WHERE operation_id='clear-once'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    f.db.execute_batch("DROP TRIGGER lifecycle_insert_fault;")
        .unwrap();
    let done = f
        .store
        .commit_session_command(
            &TenantContext::local(),
            &command,
            Some(&f.meta.lifetime),
            "clear-once",
        )
        .await
        .unwrap();
    assert_ne!(done.lifetime, f.meta.lifetime);
    let after: u64 =
        f.db.query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
    let restarted = RunStore::open(&f.root.join("runtime.sqlite3")).unwrap();
    assert_eq!(
        serde_json::to_value(
            restarted
                .commit_session_command(
                    &TenantContext::local(),
                    &command,
                    Some(&f.meta.lifetime),
                    "clear-once"
                )
                .await
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(done).unwrap()
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap(),
        after
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>(
            "SELECT count(*) FROM lifecycle_receipts WHERE operation_id='clear-once'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        1
    );
    assert_eq!(
        restarted
            .commit_session_command(
                &TenantContext::local(),
                &SessionCommand::End {
                    key: f.meta.key.clone(),
                    delete: true
                },
                Some(&f.meta.lifetime),
                "clear-once"
            )
            .await
            .unwrap_err(),
        CoordinationError::Conflict
    );
}

#[tokio::test]
async fn two_real_connections_commit_one_lifecycle_operation_with_controlled_start_barrier() {
    let f = Fixture::new();
    let second = RunStore::open(&f.root.join("runtime.sqlite3")).unwrap();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(3));
    let mut tasks = Vec::new();
    for store in [f.store.clone(), second] {
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            store
                .commit_session_command(
                    &TenantContext::local(),
                    &SessionCommand::Create {
                        key: SessionKey("same-create".into()),
                    },
                    None,
                    "create-once",
                )
                .await
        }));
    }
    barrier.wait().await;
    let first = tasks.remove(0).await.unwrap().unwrap();
    let second = tasks.remove(0).await.unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(first).unwrap(),
        serde_json::to_value(second).unwrap()
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>(
            "SELECT count(*) FROM lifecycle_receipts WHERE operation_id='create-once'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        1
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>(
            "SELECT count(*) FROM session_lifetime_owners WHERE session_id='same-create'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        1
    );
    assert_eq!(f.db.query_row::<u64,_,_>("SELECT count(*) FROM hook_publications WHERE json_extract(payload_json,'$.session_key')='same-create'",[],|r|r.get(0)).unwrap(),1);
}

#[tokio::test]
async fn lifecycle_mutation_uses_clone_writer_proof_and_rejects_expired_epoch_without_reacquiring()
{
    let f = Fixture::new();
    let native = f.store.clone().enable_session_writer().unwrap();
    let command = SessionCommand::Close {
        key: f.meta.key.clone(),
    };
    let original = native
        .commit_session_command(
            &TenantContext::local(),
            &command,
            Some(&f.meta.lifetime),
            "bound-close",
        )
        .await
        .unwrap();
    let clock: u64 =
        f.db.query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
    f.db.execute(
        "UPDATE session_writer_leases SET expires_at_ms=0 WHERE session_key=?1",
        [&f.meta.key.0],
    )
    .unwrap();
    assert_eq!(
        native
            .clone()
            .commit_session_command(
                &TenantContext::local(),
                &SessionCommand::End {
                    key: f.meta.key.clone(),
                    delete: false
                },
                Some(&f.meta.lifetime),
                "late-clear"
            )
            .await
            .unwrap_err(),
        CoordinationError::StaleLease
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap(),
        clock
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>(
            "SELECT count(*) FROM lifecycle_receipts WHERE operation_id='late-clear'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(
        native
            .read_close_receipt(
                &TenantContext::local(),
                &f.meta.key,
                &f.meta.lifetime,
                "bound-close"
            )
            .await
            .unwrap()
            .unwrap()
            .lifetime,
        original.lifetime
    );
}

#[tokio::test]
async fn corrupted_receipt_cannot_return_another_key_or_unowned_lifetime() {
    let f = Fixture::new();
    let command = SessionCommand::Close {
        key: f.meta.key.clone(),
    };
    let original = commit(&f.store, &command, Some(&f.meta.lifetime), "corrupt-close")
        .await
        .unwrap();
    let other = f.store.create_session(&SessionKey("other".into())).unwrap();
    let clock: u64 =
        f.db.query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
    for replacement in [
        other.clone(),
        SessionMetadata {
            key: original.key.clone(),
            lifetime: other.lifetime.clone(),
            ..original.clone()
        },
    ] {
        f.db.execute(
            "UPDATE lifecycle_receipts SET result_json=?1 WHERE operation_id='corrupt-close'",
            [serde_json::to_string(&replacement).unwrap()],
        )
        .unwrap();
        let replay = commit(&f.store, &command, Some(&f.meta.lifetime), "corrupt-close").await;
        let readback = receipt(&f.store, &f.meta.key, &f.meta.lifetime, "corrupt-close").await;
        assert!(
            matches!(replay, Err(CoordinationError::Corrupt)),
            "损坏结果不能重放返回其它归属：{replay:?}"
        );
        assert!(
            matches!(readback, Err(CoordinationError::Corrupt)),
            "损坏结果不能读回返回其它归属：{readback:?}"
        );
        assert_eq!(
            f.db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
                .unwrap(),
            clock
        );
    }
}
