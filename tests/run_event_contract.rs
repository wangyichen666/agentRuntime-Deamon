//! 原事件的scope、来源、预算与实时写入合同；RED使用原生产同步路径。
use agent_core::*;
use agent_storage::{MaintenanceRepository, RunStore, RuntimeError};
use agent_storage_ports::RunEventRepository;
use rusqlite::Connection;
use sha2::Digest;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    root: PathBuf,
    store: RunStore,
    db: Connection,
    run: RunRecord,
}
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "ma-event-read-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let path = root.join("runtime.sqlite3");
        let store = RunStore::open(&path).unwrap();
        let db = Connection::open(path).unwrap();
        let Admission::New(run) = store
            .admit(
                SessionKey("event-session".into()),
                RequestId::Number(1),
                "私密输入",
            )
            .unwrap()
        else {
            panic!("新准入")
        };
        Self {
            root,
            store,
            db,
            run,
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
async fn read_events(
    store: &RunStore,
    scope: &TenantContext,
    run: &RunId,
) -> Result<Vec<StoredEvent>, RuntimeError> {
    store
        .read_run_events(scope, run, EventSeq(0), 1000)
        .await
        .map(|page| page.events)
        .map_err(Into::into)
}

#[tokio::test]
async fn event_readback_denies_foreign_and_absent_runs_before_reading_payloads() {
    let f = Fixture::new();
    f.db.execute_batch("INSERT INTO organizations VALUES('other-org');INSERT INTO tenants VALUES('other-tenant','other-org');INSERT INTO projects VALUES('other-project','other-tenant');INSERT INTO agents VALUES('other-agent','other-tenant','other-project');UPDATE session_scopes SET tenant_id='other-tenant',project_id='other-project',agent_uid='other-agent' WHERE session_id='event-session';").unwrap();
    for run in [&f.run.run_id, &RunId("absent".into())] {
        let result = read_events(&f.store, &TenantContext::local(), run).await;
        assert!(
            matches!(
                result,
                Err(RuntimeError::Coordination(CoordinationError::Forbidden))
            ),
            "越域与不存在同一错误，不能返回私密事件或空成功：{result:?}"
        );
    }
}

#[tokio::test]
async fn event_readback_limits_serialized_bytes_in_addition_to_row_count() {
    let f = Fixture::new();
    for index in 0..4 {
        f.store
            .append_event(
                &f.run.run_id,
                "large_delta",
                &serde_json::json!({"index":index,"delta":"x".repeat(350*1024)}),
            )
            .unwrap();
    }
    let page = read_events(&f.store, &TenantContext::local(), &f.run.run_id)
        .await
        .unwrap();
    assert!(!page.is_empty());
    let size = serde_json::to_vec(&page).unwrap().len();
    assert!(
        size <= 1024 * 1024,
        "合法的有限行仍可能生成无限字节响应；必须分页，当前 {size} bytes"
    );
}

#[tokio::test]
async fn event_pages_preserve_exact_cursors_stamps_and_restart_without_writes() {
    let f = Fixture::new();
    for index in 0..4 {
        f.store
            .append_event(
                &f.run.run_id,
                "large_delta",
                &serde_json::json!({"index":index,"delta":"x".repeat(350*1024)}),
            )
            .unwrap();
    }
    let expected = serde_json::to_value(
        f.store
            .events_after(&f.run.run_id, EventSeq(0), 1000)
            .unwrap(),
    )
    .unwrap();
    let clock: u64 =
        f.db.query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
    let outbox: u64 =
        f.db.query_row("SELECT count(*) FROM event_outbox", [], |r| r.get(0))
            .unwrap();
    for limit in [2, 1000] {
        let mut cursor = EventSeq(0);
        let mut all = Vec::new();
        loop {
            let page = f
                .store
                .read_run_events(&TenantContext::local(), &f.run.run_id, cursor, limit)
                .await
                .unwrap();
            assert!(serde_json::to_vec(&page).unwrap().len() <= 1024 * 1024);
            assert!(page.events.len() <= limit as usize);
            assert!(!page.events.is_empty());
            assert!(page.events.iter().all(|event| event.seq > cursor));
            assert_eq!(page.cursor, page.events.last().unwrap().seq);
            cursor = page.cursor;
            all.extend(page.events);
            if !page.has_more {
                break;
            }
        }
        assert_eq!(serde_json::to_value(all).unwrap(), expected);
        let empty = f
            .store
            .read_run_events(&TenantContext::local(), &f.run.run_id, cursor, limit)
            .await
            .unwrap();
        assert!(empty.events.is_empty());
        assert!(!empty.has_more);
        assert_eq!(empty.cursor, cursor);
    }
    let restarted = RunStore::open(&f.root.join("runtime.sqlite3")).unwrap();
    let original = f
        .store
        .read_run_events(&TenantContext::local(), &f.run.run_id, EventSeq(0), 2)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(
            restarted
                .read_run_events(&TenantContext::local(), &f.run.run_id, EventSeq(0), 2)
                .await
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(original).unwrap()
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap(),
        clock
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM event_outbox", [], |r| r.get(0))
            .unwrap(),
        outbox
    );
}

#[tokio::test]
async fn event_port_validates_every_scope_dimension_and_wire_budgets() {
    let f = Fixture::new();
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
                .read_run_events(&scope, &f.run.run_id, EventSeq(0), 1)
                .await
                .unwrap_err(),
            CoordinationError::Forbidden
        );
    }
    for (run, after, limit) in [
        (f.run.run_id.clone(), EventSeq(0), 0),
        (f.run.run_id.clone(), EventSeq(0), 1001),
        (f.run.run_id.clone(), EventSeq(u64::MAX), 1),
        (RunId(String::new()), EventSeq(0), 1),
    ] {
        assert_eq!(
            f.store
                .read_run_events(&TenantContext::local(), &run, after, limit)
                .await
                .unwrap_err(),
            CoordinationError::InvalidRequest
        );
    }
}

#[tokio::test]
async fn oversized_and_corrupt_events_never_silently_skip_a_cursor() {
    let f = Fixture::new();
    let before = f.store.read_run(&f.run.run_id).unwrap().unwrap().last_seq;
    f.store
        .append_event(
            &f.run.run_id,
            "oversized",
            &serde_json::json!({"delta":"x".repeat(2*1024*1024)}),
        )
        .unwrap();
    let prefix = f
        .store
        .read_run_events(&TenantContext::local(), &f.run.run_id, EventSeq(0), 1000)
        .await
        .unwrap();
    assert!(prefix.has_more);
    assert_eq!(prefix.cursor, before);
    assert_eq!(
        f.store
            .read_run_events(&TenantContext::local(), &f.run.run_id, before, 1000)
            .await
            .unwrap_err(),
        CoordinationError::InvalidRequest
    );
    f.db.execute(
        "UPDATE event_view_stamps SET stamp_digest='corrupted' WHERE run_id=?1",
        [&f.run.run_id.0],
    )
    .unwrap();
    assert_eq!(
        f.store
            .read_run_events(&TenantContext::local(), &f.run.run_id, EventSeq(0), 1)
            .await
            .unwrap_err(),
        CoordinationError::Corrupt
    );
    f.db.execute_batch("PRAGMA foreign_keys=OFF; DROP TABLE events;")
        .unwrap();
    let error = f
        .store
        .read_run_events(&TenantContext::local(), &f.run.run_id, EventSeq(0), 1)
        .await
        .unwrap_err();
    assert_eq!(error, CoordinationError::StorageUnavailable);
    assert!(!error.to_string().contains("events"));
}

#[tokio::test]
async fn retired_run_events_keep_permanent_lifetime_identity() {
    use agent_storage::{SessionLifecycle, SessionQuery};
    let f = Fixture::new();
    let owner = f.store.run_owner(&f.run.run_id).unwrap();
    f.store
        .finish(&f.run.run_id, RunStatus::Completed, Some("完成"), None)
        .unwrap();
    let before = f
        .store
        .read_run_events(&TenantContext::local(), &f.run.run_id, EventSeq(0), 1000)
        .await
        .unwrap();
    f.store
        .end_session(&owner.session_key, &owner.session_lifetime_id, false)
        .unwrap();
    let current = f
        .store
        .session_metadata(&owner.session_key)
        .unwrap()
        .unwrap();
    assert_ne!(current.lifetime, owner.session_lifetime_id);
    let clock: u64 =
        f.db.query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
    let after = f
        .store
        .read_run_events(&TenantContext::local(), &f.run.run_id, EventSeq(0), 1000)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(after).unwrap(),
        serde_json::to_value(before).unwrap()
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap(),
        clock
    );
}

#[tokio::test]
async fn native_event_with_deleted_stamp_is_corrupt_and_cannot_be_legacy() {
    let f = Fixture::new();
    // 若已有删除保护，显式拆除它来模拟持久介质损坏；不能借拒绝DELETE证明decoder安全。
    let guard:u64=f.db.query_row("SELECT count(*) FROM sqlite_master WHERE type='trigger' AND name='immutable_event_stamp_delete'",[],|r|r.get(0)).unwrap();
    if guard != 0 {
        f.db.execute_batch("DROP TRIGGER immutable_event_stamp_delete")
            .unwrap();
    }
    f.db.execute(
        "DELETE FROM event_view_stamps WHERE run_id=?1",
        [&f.run.run_id.0],
    )
    .unwrap();
    let before: u64 =
        f.db.query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
    assert_eq!(
        f.store
            .read_run_events(&TenantContext::local(), &f.run.run_id, EventSeq(0), 1000)
            .await
            .unwrap_err(),
        CoordinationError::Corrupt
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap(),
        before
    );
}
#[test]
fn direct_native_view_stamp_read_cannot_downgrade_missing_stamp_to_legacy() {
    use agent_storage::ViewRepository;
    let f = Fixture::new();
    let guard:u64=f.db.query_row("SELECT count(*) FROM sqlite_master WHERE type='trigger' AND name='immutable_event_stamp_delete'",[],|r|r.get(0)).unwrap();
    if guard != 0 {
        f.db.execute_batch("DROP TRIGGER immutable_event_stamp_delete")
            .unwrap();
    }
    f.db.execute(
        "DELETE FROM event_view_stamps WHERE run_id=?1",
        [&f.run.run_id.0],
    )
    .unwrap();
    let seq: u64 =
        f.db.query_row(
            "SELECT min(seq) FROM events WHERE run_id=?1",
            [&f.run.run_id.0],
            |r| r.get(0),
        )
        .unwrap();
    assert!(matches!(
        f.store.event_view_stamp(&f.run.run_id, EventSeq(seq)),
        Err(RuntimeError::Coordination(CoordinationError::Corrupt))
    ));
}

fn remove_v26_for_fixture(db: &Connection) {
    db.execute_batch("DELETE FROM role_bindings WHERE tenant_id='local-tenant' AND subject_id='local-cron-workload'; DELETE FROM principals WHERE tenant_id='local-tenant' AND subject_id='local-cron-workload'; DELETE FROM schema_migrations WHERE version=27; DROP TRIGGER native_event_provenance_insert;DROP TRIGGER immutable_event_provenance_update;DROP TRIGGER immutable_event_stamp_delete;ALTER TABLE events DROP COLUMN view_provenance;DELETE FROM schema_migrations WHERE version=26;").unwrap();
}
#[test]
fn event_provenance_and_published_stamp_are_immutable_and_new_events_cannot_claim_legacy() {
    let f = Fixture::new();
    let seq: u64 =
        f.db.query_row("SELECT min(seq) FROM events", [], |r| r.get(0))
            .unwrap();
    assert!(
        f.db.execute(
            "UPDATE events SET view_provenance='legacy' WHERE run_id=?1",
            [&f.run.run_id.0]
        )
        .is_err()
    );
    assert!(
        f.db.execute(
            "DELETE FROM event_view_stamps WHERE run_id=?1",
            [&f.run.run_id.0]
        )
        .is_err()
    );
    assert!(f.db.execute("INSERT INTO events(run_id,seq,event,data_json,created_at_ms,view_provenance) VALUES(?1,?2,'forged','{}',0,'legacy')",rusqlite::params![f.run.run_id.0,seq+1]).is_err());
    f.store
        .append_event(
            &f.run.run_id,
            "native-event",
            &serde_json::json!({"fact":true}),
        )
        .unwrap();
    assert_eq!(
        f.db.query_row::<u64, _, _>(
            "SELECT count(*) FROM events WHERE view_provenance<>'native'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(f.db.query_row::<u64,_,_>("SELECT count(*) FROM events e LEFT JOIN event_view_stamps s ON s.run_id=e.run_id AND s.seq=e.seq WHERE s.published IS NOT 1",[],|r|r.get(0)).unwrap(),0);
}
#[tokio::test]
async fn v25_upgrade_classifies_both_persistent_native_proofs_and_keeps_true_legacy_unversioned() {
    let f = Fixture::new();
    for n in 0..2 {
        f.store
            .append_event(&f.run.run_id, "old-event", &serde_json::json!({"n":n}))
            .unwrap();
    }
    remove_v26_for_fixture(&f.db);
    // seq1: 两种证据均无，合法旧legacy；seq2:只stamp；seq3:只原outbox。
    f.db.execute_batch("DELETE FROM event_view_stamps WHERE seq IN (1,3);DELETE FROM event_outbox WHERE event_seq IN (1,2);").unwrap();
    let path = f.root.join("runtime.sqlite3");
    let upgraded = RunStore::open(&path).unwrap();
    assert_eq!(upgraded.health_report().unwrap()["schema_version"], 27);
    let kinds: Vec<(u64, String)> =
        f.db.prepare("SELECT seq,view_provenance FROM events ORDER BY seq")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
    assert_eq!(
        kinds,
        vec![
            (1, "legacy".into()),
            (2, "native".into()),
            (3, "native".into())
        ]
    );
    let before: u64 =
        f.db.query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
    let page = upgraded
        .read_run_events(&TenantContext::local(), &f.run.run_id, EventSeq(0), 2)
        .await
        .unwrap();
    assert_eq!(page.events.len(), 2);
    assert!(page.events[0].data.get("_my_agent_view").is_none());
    assert!(page.events[1].data.get("_my_agent_view").is_some());
    assert_eq!(
        upgraded
            .read_run_events(&TenantContext::local(), &f.run.run_id, EventSeq(2), 2)
            .await
            .unwrap_err(),
        CoordinationError::Corrupt,
        "只outbox证据也不能把损坏的native当legacy"
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap(),
        before
    );
    let backup = path.with_extension("v25.backup.sqlite3");
    let bytes = std::fs::read(&backup).unwrap();
    let marker: serde_json::Value = serde_json::from_slice(
        &std::fs::read(path.with_extension("v25.backup.verified.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(marker["schema"], 25);
    assert_eq!(marker["bytes"].as_u64().unwrap(), bytes.len() as u64);
    assert_eq!(
        marker["sha256"],
        format!("{:x}", sha2::Sha256::digest(&bytes))
    );
    let backed =
        Connection::open_with_flags(&backup, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    assert_eq!(
        backed
            .query_row::<u64, _, _>("SELECT max(version) FROM schema_migrations", [], |r| r
                .get(0))
            .unwrap(),
        25
    );
    assert_eq!(
        backed
            .query_row::<String, _, _>("PRAGMA integrity_check", [], |r| r.get(0))
            .unwrap(),
        "ok"
    );
}
#[test]
fn v26_migration_fault_rolls_back_column_triggers_and_marker_and_future_schema_is_rejected() {
    let f = Fixture::new();
    remove_v26_for_fixture(&f.db);
    let path = f.root.join("runtime.sqlite3");
    f.db.execute_batch("CREATE TRIGGER v26_fault BEFORE INSERT ON schema_migrations WHEN NEW.version=26 BEGIN SELECT RAISE(ABORT,'provenance migration fault'); END;").unwrap();
    assert!(RunStore::open(&path).is_err());
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT max(version) FROM schema_migrations", [], |r| r
            .get(0))
            .unwrap(),
        25
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>(
            "SELECT count(*) FROM pragma_table_info('events') WHERE name='view_provenance'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(f.db.query_row::<u64,_,_>("SELECT count(*) FROM sqlite_master WHERE type='trigger' AND name IN ('native_event_provenance_insert','immutable_event_provenance_update','immutable_event_stamp_delete')",[],|r|r.get(0)).unwrap(),0);
    let backup = path.with_extension("v25.backup.sqlite3");
    assert!(backup.exists());
    assert!(path.with_extension("v25.backup.verified.json").exists());
    f.db.execute_batch("DROP TRIGGER v26_fault").unwrap();
    let upgraded = RunStore::open(&path).unwrap();
    assert_eq!(upgraded.health_report().unwrap()["schema_version"], 27);
    drop(upgraded);
    f.db.execute_batch("INSERT INTO schema_migrations VALUES(28,0)")
        .unwrap();
    assert!(RunStore::open(&path).is_err());
    // 回滚材料是停机后配对旧程序使用的完整v25备份；这里只在独立夹具路径恢复并重新升级验证。
    let restored = f.root.join("restored.sqlite3");
    std::fs::copy(backup, &restored).unwrap();
    let old = Connection::open(&restored).unwrap();
    assert_eq!(
        old.query_row::<u64, _, _>("SELECT max(version) FROM schema_migrations", [], |r| r
            .get(0))
            .unwrap(),
        25
    );
    old.execute_batch("DROP TRIGGER v26_fault").unwrap();
    drop(old);
    let restored = RunStore::open(&restored).unwrap();
    assert_eq!(restored.health_report().unwrap()["schema_version"], 27);
    assert_eq!(
        restored.read_run(&f.run.run_id).unwrap().unwrap().run_id,
        f.run.run_id
    );
}
#[tokio::test]
async fn saved_native_stamp_budget_is_checked_before_loading_or_parsing_json() {
    let f = Fixture::new();
    let before: u64 =
        f.db.query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
    f.db.execute_batch("UPDATE event_view_stamps SET stamp_json=CAST(zeroblob(16385) AS TEXT)")
        .unwrap();
    assert_eq!(
        f.store
            .read_run_events(&TenantContext::local(), &f.run.run_id, EventSeq(0), 1000)
            .await
            .unwrap_err(),
        CoordinationError::Corrupt
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap(),
        before
    );
}

#[test]
fn latest_view_event_denies_foreign_before_reading_payload() {
    use agent_storage::ViewRepository;
    let f = Fixture::new();
    f.store
        .append_event(
            &f.run.run_id,
            "terminal",
            &serde_json::json!({"private":"租户私密终态"}),
        )
        .unwrap();
    f.db.execute_batch("INSERT INTO organizations VALUES('foreign');INSERT INTO tenants VALUES('foreign','foreign');INSERT INTO projects VALUES('foreign','foreign');INSERT INTO agents VALUES('foreign','foreign','foreign');UPDATE session_scopes SET tenant_id='foreign',project_id='foreign',agent_uid='foreign' WHERE session_id='event-session'").unwrap();
    let result = f.store.latest_view_event(&f.run.run_id, "terminal", None);
    assert!(
        matches!(
            result,
            Err(RuntimeError::Authorization(AuthorizationError::Forbidden))
                | Err(RuntimeError::Coordination(CoordinationError::Forbidden))
        ),
        "最新事件必须完整scope先核，实际为 {result:?}"
    );
}
#[test]
fn latest_view_event_denies_legal_oversized_payload_before_decode() {
    use agent_storage::ViewRepository;
    let f = Fixture::new();
    f.store
        .append_event(
            &f.run.run_id,
            "terminal",
            &serde_json::json!({"content":"x".repeat(2*1024*1024)}),
        )
        .unwrap();
    let result = f.store.latest_view_event(&f.run.run_id, "terminal", None);
    assert!(
        matches!(
            result,
            Err(RuntimeError::Coordination(
                CoordinationError::InvalidRequest
            ))
        ),
        "合法大事件不能无限字节读回，实际success={},error={:?}",
        result.is_ok(),
        result.as_ref().err()
    );
}

#[tokio::test]
async fn latest_event_port_filters_exact_child_preserves_original_stamp_and_never_writes() {
    let f = Fixture::new();
    let one = RunId("child-one".into());
    let two = RunId("child-two".into());
    let first = f
        .store
        .append_event(
            &f.run.run_id,
            "delegation_terminal",
            &serde_json::json!({"child_run_id":one,"content":"第一个"}),
        )
        .unwrap();
    let second = f
        .store
        .append_event(
            &f.run.run_id,
            "delegation_terminal",
            &serde_json::json!({"child_run_id":two,"content":"第二个"}),
        )
        .unwrap();
    let before: u64 =
        f.db.query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
    let all = f
        .store
        .events_after(&f.run.run_id, EventSeq(0), 1000)
        .unwrap();
    for (child, seq) in [(Some(&one), first), (Some(&two), second), (None, second)] {
        let event = f
            .store
            .read_run_view_event(
                &TenantContext::local(),
                &f.run.run_id,
                "delegation_terminal",
                child,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.seq, seq);
        assert_eq!(
            serde_json::to_value(event).unwrap(),
            serde_json::to_value(all.iter().find(|event| event.seq == seq).unwrap()).unwrap()
        );
    }
    assert!(
        f.store
            .read_run_view_event(
                &TenantContext::local(),
                &f.run.run_id,
                "delegation_terminal",
                Some(&RunId("absent-child".into()))
            )
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn latest_event_port_checks_all_scope_dimensions_and_absence_before_any_payload() {
    let f = Fixture::new();
    f.store
        .append_event(
            &f.run.run_id,
            "terminal",
            &serde_json::json!({"secret":"秘密"}),
        )
        .unwrap();
    for dimension in 0..4 {
        let mut scope = TenantContext::local();
        match dimension {
            0 => scope.organization_id = "other".to_owned().try_into().unwrap(),
            1 => scope.tenant_id = "other".to_owned().try_into().unwrap(),
            2 => scope.project_id = "other".to_owned().try_into().unwrap(),
            _ => scope.agent_uid = "other".to_owned().try_into().unwrap(),
        }
        for run in [&f.run.run_id, &RunId("absent".into())] {
            assert_eq!(
                f.store
                    .read_run_view_event(&scope, run, "terminal", None)
                    .await
                    .unwrap_err(),
                CoordinationError::Forbidden
            );
        }
    }
}

#[tokio::test]
async fn latest_event_port_reads_retired_lifetime_and_rejects_valid_stamp_json_with_wrong_digest() {
    use agent_storage_ports::SessionLifecycleRepository;
    let f = Fixture::new();
    f.store.try_start_queued(&f.run.run_id).unwrap();
    f.store
        .finish(&f.run.run_id, RunStatus::Completed, Some("完成"), None)
        .unwrap();
    let event = f
        .store
        .read_run_view_event(&TenantContext::local(), &f.run.run_id, "terminal", None)
        .await
        .unwrap()
        .unwrap();
    let owner = serde_json::from_value::<ViewStamp>(event.data["_my_agent_view"].clone())
        .unwrap()
        .owner
        .unwrap();
    f.store
        .commit_session_command(
            &TenantContext::local(),
            &SessionCommand::End {
                key: owner.session_key.clone(),
                delete: false,
            },
            Some(&owner.session_lifetime_id),
            "event-retire",
        )
        .await
        .unwrap();
    let clock: u64 =
        f.db.query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
    let read = f
        .store
        .read_run_view_event(&TenantContext::local(), &f.run.run_id, "terminal", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(read).unwrap(),
        serde_json::to_value(&event).unwrap()
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM session_writer_leases", [], |r| r
            .get(0))
            .unwrap(),
        0
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap(),
        clock
    );
    let mut changed_stamp = event.data["_my_agent_view"].clone();
    changed_stamp["snapshot_revision"] = serde_json::json!(clock + 1);
    f.db.execute(
        "UPDATE event_view_stamps SET stamp_json=?2 WHERE run_id=?1 AND seq=?3",
        rusqlite::params![f.run.run_id.0, changed_stamp.to_string(), event.seq.0],
    )
    .unwrap();
    assert_eq!(
        f.store
            .read_run_view_event(&TenantContext::local(), &f.run.run_id, "terminal", None)
            .await
            .unwrap_err(),
        CoordinationError::Corrupt
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap(),
        clock
    );
}

#[tokio::test]
async fn latest_child_filter_never_parses_or_skips_oversized_or_malformed_json() {
    let f = Fixture::new();
    let seq = f
        .store
        .append_event(
            &f.run.run_id,
            "delegation_terminal",
            &serde_json::json!({"child_run_id":"child","content":"x".repeat(2*1024*1024)}),
        )
        .unwrap();
    assert_eq!(
        f.store
            .read_run_view_event(
                &TenantContext::local(),
                &f.run.run_id,
                "delegation_terminal",
                Some(&RunId("child".into()))
            )
            .await
            .unwrap_err(),
        CoordinationError::InvalidRequest
    );
    f.db.execute(
        "UPDATE events SET data_json='{' WHERE run_id=?1 AND seq=?2",
        rusqlite::params![f.run.run_id.0, seq.0],
    )
    .unwrap();
    assert_eq!(
        f.store
            .read_run_view_event(
                &TenantContext::local(),
                &f.run.run_id,
                "delegation_terminal",
                Some(&RunId("child".into()))
            )
            .await
            .unwrap_err(),
        CoordinationError::Corrupt
    );
}

// 复用run_event_contract原Fixture，在本批门禁结束后附加、实际运行再实施。
#[test]
fn live_event_append_denies_foreign_legacy_run_before_any_mutation() {
    let f = Fixture::new();
    f.db.execute_batch("INSERT INTO organizations VALUES('foreign');INSERT INTO tenants VALUES('foreign','foreign');INSERT INTO projects VALUES('foreign','foreign');INSERT INTO agents VALUES('foreign','foreign','foreign');UPDATE session_scopes SET tenant_id='foreign',project_id='foreign',agent_uid='foreign' WHERE session_id='event-session'").unwrap();
    let before: u64 =
        f.db.query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
    let result = f.store.append_event(
        &f.run.run_id,
        "text_delta",
        &serde_json::json!({"delta":"迟到跨域数据"}),
    );
    assert!(
        matches!(
            result,
            Err(RuntimeError::Authorization(AuthorizationError::Forbidden))
        ),
        "原live写入必须先scope，实际为 {result:?}"
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn live_event_callback_cannot_promote_captured_owner_to_new_generation() {
    use agent_storage::SessionQuery;
    let f = Fixture::new();
    assert!(f.store.try_start_queued(&f.run.run_id).unwrap());
    let env = EnterpriseOwnerEnvelope {
        scope: TenantContext::local(),
        owner: f.store.run_owner(&f.run.run_id).unwrap(),
    };
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM run_authorizations", [], |r| r.get(0))
            .unwrap(),
        0,
        "只复现无冻结授权旧兼容caller，不能称原frozen SHA被绕过"
    );
    f.db.execute(
        "UPDATE runs SET generation=generation+1 WHERE id=?1",
        [&f.run.run_id.0],
    )
    .unwrap();
    f.db.execute(
        "UPDATE session_heads SET run_generation=run_generation+1 WHERE session_id=?1",
        [&env.owner.session_key.0],
    )
    .unwrap();
    let before: u64 =
        f.db.query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
    let result = f
        .store
        .append_run_live_event(
            &env,
            "text_delta",
            &serde_json::json!({"delta":"旧callback"}),
        )
        .await;
    assert!(
        result.is_err(),
        "原callback固定准入owner不可重取新generation，实际为 {result:?}"
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap(),
        before
    );
}

// 原live事务的scope、固定owner与预算合同。
#[tokio::test]
async fn native_live_events_keep_admission_owner_and_reject_terminal_and_bad_payload() {
    use agent_storage::SessionQuery;
    let f = Fixture::new();
    assert!(f.store.try_start_queued(&f.run.run_id).unwrap());
    let env = EnterpriseOwnerEnvelope {
        scope: TenantContext::local(),
        owner: f.store.run_owner(&f.run.run_id).unwrap(),
    };
    for dimension in 0..9 {
        let mut bad = env.clone();
        match dimension {
            0 => bad.scope.organization_id = "foreign".to_owned().try_into().unwrap(),
            1 => bad.scope.tenant_id = "foreign".to_owned().try_into().unwrap(),
            2 => bad.scope.project_id = "foreign".to_owned().try_into().unwrap(),
            3 => bad.scope.agent_uid = "foreign".to_owned().try_into().unwrap(),
            4 => bad.owner.session_key.0 = "foreign".into(),
            5 => bad.owner.session_lifetime_id.0 = "foreign".into(),
            6 => bad.owner.turn_id.0 = "foreign".into(),
            7 => bad.owner.run_generation.0 += 1,
            _ => bad.owner.run_id.0 = "missing".into(),
        }
        let before: u64 =
            f.db.query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
                .unwrap();
        let result = f
            .store
            .append_run_live_event(
                &bad,
                "text_delta",
                &serde_json::json!({"delta":"跨域迟到写入"}),
            )
            .await;
        let expected = if dimension < 4 || dimension == 8 {
            AuthorizationError::Forbidden
        } else {
            AuthorizationError::StaleOwner
        };
        assert_eq!(result, Err(expected.into()), "{dimension}: {result:?}");
        assert_eq!(
            f.db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
                .unwrap(),
            before
        );
    }
    for (name, data) in [
        ("terminal", serde_json::json!({})),
        ("provider_retry", serde_json::json!({})),
        ("text_delta", serde_json::json!(null)),
        (
            "text_delta",
            serde_json::json!({"delta":"x".repeat(1024*1024)}),
        ),
    ] {
        assert_eq!(
            f.store.append_run_live_event(&env, name, &data).await,
            Err(CoordinationError::InvalidRequest.into())
        );
    }
    let seq = f
        .store
        .append_run_live_event(&env, "text_delta", &serde_json::json!({"delta":"正常流"}))
        .await
        .unwrap();
    let page = f
        .store
        .read_run_events(&env.scope, &env.owner.run_id, EventSeq(seq.0 - 1), 1)
        .await
        .unwrap();
    assert_eq!(page.events[0].seq, seq);
    assert_eq!(page.events[0].data["delta"], "正常流");
    assert!(page.events[0].data["_my_agent_view"].is_object());
    f.store
        .finish(&f.run.run_id, RunStatus::Completed, None, None)
        .unwrap();
    let before: u64 =
        f.db.query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
    assert_eq!(
        f.store
            .append_run_live_event(&env, "text_delta", &serde_json::json!({"delta":"终态后"}))
            .await,
        Err(CoordinationError::Conflict.into())
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap(),
        before
    );
}
#[tokio::test]
async fn native_live_approval_failure_rolls_back_original_interaction_event_stamp_and_outbox() {
    use agent_storage::SessionQuery;
    let f = Fixture::new();
    assert!(f.store.try_start_queued(&f.run.run_id).unwrap());
    let env = EnterpriseOwnerEnvelope {
        scope: TenantContext::local(),
        owner: f.store.run_owner(&f.run.run_id).unwrap(),
    };
    let counts = |db: &Connection| {
        db.query_row("SELECT (SELECT count(*) FROM interactions),(SELECT count(*) FROM events),(SELECT count(*) FROM event_outbox),(SELECT count(*) FROM event_view_stamps),(SELECT revision FROM snapshot_clock),(SELECT last_seq FROM runs WHERE id=?1)",[&f.run.run_id.0],|r|Ok((r.get::<_,u64>(0)?,r.get::<_,u64>(1)?,r.get::<_,u64>(2)?,r.get::<_,u64>(3)?,r.get::<_,u64>(4)?,r.get::<_,u64>(5)?))).unwrap()
    };
    for table in ["events", "event_outbox", "event_view_stamps"] {
        f.db.execute_batch(&format!("CREATE TRIGGER live_failure BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT,'live event fault'); END")).unwrap();
        let before = counts(&f.db);
        let result = f
            .store
            .append_run_live_event(
                &env,
                "approval_required",
                &serde_json::json!({"approval":{"id":"live-approval","prompt":"批准写入?"}}),
            )
            .await;
        assert!(result.is_err(), "{table}: {result:?}");
        assert_eq!(counts(&f.db), before);
        assert_eq!(
            f.store.read_run(&f.run.run_id).unwrap().unwrap().status,
            RunStatus::Running
        );
        f.db.execute_batch("DROP TRIGGER live_failure").unwrap();
    }
    let seq = f
        .store
        .append_run_live_event(
            &env,
            "approval_required",
            &serde_json::json!({"approval":{"id":"live-approval","prompt":"批准写入?"}}),
        )
        .await
        .unwrap();
    assert_eq!(
        f.store.read_run(&f.run.run_id).unwrap().unwrap().status,
        RunStatus::WaitingInteraction
    );
    assert_eq!(
        f.db.query_row::<String, _, _>(
            "SELECT status FROM interactions WHERE id='live-approval'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        "pending"
    );
    let page = f
        .store
        .read_run_events(&env.scope, &env.owner.run_id, EventSeq(seq.0 - 1), 1)
        .await
        .unwrap();
    assert!(page.events[0].data["_my_agent_view"].is_object());
    assert!(
        f.store
            .append_run_live_event(
                &env,
                "tool_started",
                &serde_json::json!({"id":"after-approval"})
            )
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn two_real_connections_append_distinct_live_sequences_with_original_stamps_and_outbox() {
    use agent_storage::SessionQuery;
    use std::sync::Arc;
    let f = Fixture::new();
    assert!(f.store.try_start_queued(&f.run.run_id).unwrap());
    let env = EnterpriseOwnerEnvelope {
        scope: TenantContext::local(),
        owner: f.store.run_owner(&f.run.run_id).unwrap(),
    };
    let cursor = f.store.read_run(&f.run.run_id).unwrap().unwrap().last_seq;
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let mut workers = Vec::new();
    for delta in ["连接一", "连接二"] {
        let store = RunStore::open(&f.root.join("runtime.sqlite3")).unwrap();
        let env = env.clone();
        let barrier = barrier.clone();
        workers.push(tokio::spawn(async move {
            barrier.wait().await;
            store
                .append_run_live_event(&env, "text_delta", &serde_json::json!({"delta": delta}))
                .await
        }));
    }
    barrier.wait().await;
    let mut sequences = Vec::new();
    for worker in workers {
        sequences.push(worker.await.unwrap().unwrap());
    }
    sequences.sort_by_key(|seq| seq.0);
    assert_eq!(
        sequences,
        vec![EventSeq(cursor.0 + 1), EventSeq(cursor.0 + 2)]
    );
    let page = f
        .store
        .read_run_events(&env.scope, &env.owner.run_id, cursor, 10)
        .await
        .unwrap();
    assert_eq!(page.events.len(), 2);
    for event in &page.events {
        let stamp: ViewStamp =
            serde_json::from_value(event.data["_my_agent_view"].clone()).unwrap();
        assert_eq!(stamp.owner, Some(env.owner.clone()));
        assert_eq!(stamp.event_seq, Some(event.seq));
        assert!(stamp.valid());
        assert_eq!(
            f.db.query_row::<u64, _, _>(
                "SELECT count(*) FROM event_outbox WHERE run_id=?1 AND event_seq=?2",
                rusqlite::params![env.owner.run_id.0, event.seq.0],
                |r| r.get(0)
            )
            .unwrap(),
            1
        );
    }
}
