//! 原 canonical 会话读回的隔离合同；损坏注入不允许读成另一会话。
use agent_core::*;
use agent_storage::{
    MaintenanceRepository, RunStore, RuntimeError, SessionLifecycle, SessionQuery, TranscriptStore,
};
use agent_storage_ports::SessionReadRepository;
use rusqlite::{Connection, params};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    path: PathBuf,
    store: RunStore,
    db: Connection,
}
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "ma-readback-{}-{}.sqlite3",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let store = RunStore::open(&path).unwrap();
        let db = Connection::open(&path).unwrap();
        Self { path, store, db }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_file(self.path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(self.path.with_extension("sqlite3-shm"));
    }
}
fn foreign_session(f: &Fixture, key: &SessionKey) {
    f.db.execute_batch("INSERT INTO organizations VALUES('other-org'); INSERT INTO tenants VALUES('other-tenant','other-org'); INSERT INTO projects VALUES('other-project','other-tenant'); INSERT INTO agents VALUES('other-agent','other-tenant','other-project');").unwrap();
    f.db.execute("UPDATE session_scopes SET tenant_id='other-tenant',project_id='other-project',agent_uid='other-agent' WHERE session_id=?1",[&key.0]).unwrap();
}

#[test]
fn local_readback_never_reads_a_foreign_or_absent_session() {
    let f = Fixture::new();
    let key = SessionKey("同名会话".into());
    f.store.create_session(&key).unwrap();
    foreign_session(&f, &key);
    for key in [key, SessionKey("不存在".into())] {
        for mode in [
            HistoryReadMode::Canonical,
            HistoryReadMode::Model,
            HistoryReadMode::Omitted,
        ] {
            assert!(
                matches!(
                    f.store.session_readback(&key, mode),
                    Err(RuntimeError::Authorization(AuthorizationError::Forbidden))
                ),
                "跨域与不存在必须返回同一授权错误"
            );
        }
    }
}

#[test]
fn local_session_page_filters_scope_before_limit_and_cursor() {
    let f = Fixture::new();
    let hidden = SessionKey("a-hidden".into());
    let visible = SessionKey("b-visible".into());
    f.store.create_session(&hidden).unwrap();
    f.store.create_session(&visible).unwrap();
    foreign_session(&f, &hidden);
    let page = f.store.session_metadata_page(None, 1).unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].key, visible);
    assert_eq!(f.store.session_metadata_list().unwrap().len(), 1);
}

#[test]
fn retired_lifetime_cannot_disclose_ownerless_transcript_to_another_session() {
    let f = Fixture::new();
    let first = SessionKey("a".into());
    let second = SessionKey("b".into());
    f.store
        .import_legacy(&first, &[Message::text(Role::User, "a 的私密内容")])
        .unwrap();
    f.store
        .import_legacy(&second, &[Message::text(Role::User, "b 的私密内容")])
        .unwrap();
    let life = f.store.session_metadata(&first).unwrap().unwrap().lifetime;
    f.store.end_session(&first, &life, false).unwrap();
    assert!(
        f.db.execute(
            "UPDATE session_heads SET lifetime=?1 WHERE session_id=?2",
            params![life.0, second.0]
        )
        .is_err(),
        "退休 lifetime 不得重新绑定，不能读取 A 的 ownerless 历史"
    );
    assert_eq!(
        f.store
            .session_readback(&second, HistoryReadMode::Canonical)
            .unwrap()
            .messages[0]
            .content
            .as_deref(),
        Some("b 的私密内容")
    );
}

#[test]
fn damaged_run_lifetime_cannot_project_another_sessions_terminal() {
    let f = Fixture::new();
    let first = SessionKey("first".into());
    let second = SessionKey("second".into());
    let Admission::New(a) = f
        .store
        .admit(first.clone(), RequestId::Number(1), "a")
        .unwrap()
    else {
        panic!("新准入")
    };
    let Admission::New(b) = f.store.admit(second, RequestId::Number(2), "b").unwrap() else {
        panic!("新准入")
    };
    f.store
        .finish(&b.run_id, RunStatus::Completed, Some("b 的私密终态"), None)
        .unwrap();
    let life = f.store.session_metadata(&first).unwrap().unwrap().lifetime;
    let changed = f.db.execute(
        "UPDATE runs SET lifetime=?1 WHERE id=?2",
        params![life.0, b.run_id.0],
    );
    // v25 在写入前拒绝；v24 损坏 fixture 仍必须由 readback SQL 隔离。
    if changed.is_err() {
        return;
    }
    let snapshot = f.store.session_readback(&first, HistoryReadMode::Omitted);
    if let Ok(snapshot) = snapshot {
        assert!(
            snapshot.last_durable_terminal.is_none(),
            "不能把 B 的终态投影给 A"
        );
        assert_eq!(snapshot.active_runs[0].run_id, a.run_id);
    }
}

fn remove_v25(db: &Connection) {
    db.execute_batch("DELETE FROM role_bindings WHERE tenant_id='local-tenant' AND subject_id='local-cron-workload'; DELETE FROM principals WHERE tenant_id='local-tenant' AND subject_id='local-cron-workload'; DELETE FROM schema_migrations WHERE version=27; DROP TRIGGER native_event_provenance_insert; DROP TRIGGER immutable_event_provenance_update; DROP TRIGGER immutable_event_stamp_delete; ALTER TABLE events DROP COLUMN view_provenance; DELETE FROM schema_migrations WHERE version=26; DROP TRIGGER bind_lifetime_insert; DROP TRIGGER bind_lifetime_update; DROP TRIGGER immutable_run_incarnation; DROP TRIGGER immutable_lifetime_owner_update; DROP TRIGGER immutable_lifetime_owner_delete; DROP TABLE session_lifetime_owners; DELETE FROM schema_migrations WHERE version=25;").unwrap();
}

#[tokio::test]
async fn async_scoped_readback_and_page_share_canonical_facts_after_restart() {
    let f = Fixture::new();
    let first = SessionKey("a-local".into());
    let other = SessionKey("b-other".into());
    f.store
        .import_legacy(&first, &[Message::text(Role::User, "本地摘要")])
        .unwrap();
    f.store
        .import_legacy(&other, &[Message::text(Role::User, "另一租户摘要")])
        .unwrap();
    foreign_session(&f, &other);
    let scope = TenantContext::local();
    let expected = f
        .store
        .session_readback(&first, HistoryReadMode::Canonical)
        .unwrap();
    let revision = expected.snapshot_revision;
    let repo = RunStore::open(&f.path).unwrap();
    let readback = repo
        .read_session(&scope, &first, HistoryReadMode::Canonical)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(readback).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
    let page = repo.list_sessions(&scope, None, 1).await.unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].metadata.key, first);
    assert_eq!(page[0].preview.as_deref(), Some("本地摘要"));
    assert!(
        repo.list_sessions(&scope, Some(&first.0), 1)
            .await
            .unwrap()
            .is_empty()
    );
    let other_scope = TenantContext {
        organization_id: "other-org".to_owned().try_into().unwrap(),
        tenant_id: "other-tenant".to_owned().try_into().unwrap(),
        project_id: "other-project".to_owned().try_into().unwrap(),
        agent_uid: "other-agent".to_owned().try_into().unwrap(),
    };
    assert_eq!(
        repo.list_sessions(&other_scope, None, 1).await.unwrap()[0]
            .metadata
            .key,
        other
    );
    for key in [&other, &SessionKey("absent".into())] {
        assert!(matches!(
            repo.read_session(&scope, key, HistoryReadMode::Omitted)
                .await,
            Err(CoordinationError::Forbidden)
        ));
    }
    for changed in 0..4 {
        let mut scope = scope.clone();
        match changed {
            0 => scope.organization_id = "absent".to_owned().try_into().unwrap(),
            1 => scope.tenant_id = "absent".to_owned().try_into().unwrap(),
            2 => scope.project_id = "absent".to_owned().try_into().unwrap(),
            _ => scope.agent_uid = "absent".to_owned().try_into().unwrap(),
        }
        assert!(matches!(
            repo.read_session(&scope, &first, HistoryReadMode::Canonical)
                .await,
            Err(CoordinationError::Forbidden)
        ));
        assert!(
            repo.list_sessions(&scope, None, 1)
                .await
                .unwrap()
                .is_empty()
        );
    }
    assert!(matches!(
        repo.list_sessions(&scope, None, 0).await,
        Err(CoordinationError::InvalidRequest)
    ));
    assert!(matches!(
        repo.list_sessions(&scope, None, 1002).await,
        Err(CoordinationError::InvalidRequest)
    ));
    assert_eq!(
        repo.read_session(&scope, &first, HistoryReadMode::Omitted)
            .await
            .unwrap()
            .snapshot_revision,
        revision,
        "读回不能修改 canonical clock"
    );
}

#[test]
fn v24_upgrade_keeps_ambiguous_legacy_history_quarantined_with_verified_backup() {
    let f = Fixture::new();
    let first = SessionKey("legacy-a".into());
    let second = SessionKey("legacy-b".into());
    f.store
        .import_legacy(&first, &[Message::text(Role::User, "旧历史必须保留")])
        .unwrap();
    let life = f.store.session_metadata(&first).unwrap().unwrap().lifetime;
    let payload: String =
        f.db.query_row(
            "SELECT messages_json FROM transcript_batches WHERE lifetime=?1",
            [&life.0],
            |r| r.get(0),
        )
        .unwrap();
    f.store.end_session(&first, &life, false).unwrap();
    f.store.create_session(&second).unwrap();
    remove_v25(&f.db);
    let upgraded = RunStore::open(&f.path).unwrap();
    assert_eq!(upgraded.health_report().unwrap()["schema_version"], 27);
    let owner: (Option<String>, String) =
        f.db.query_row(
            "SELECT session_id,provenance FROM session_lifetime_owners WHERE lifetime=?1",
            [&life.0],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(owner, (None, "legacy_orphan".into()));
    assert!(
        f.db.execute(
            "UPDATE session_heads SET lifetime=?1 WHERE session_id=?2",
            params![life.0, second.0]
        )
        .is_err()
    );
    assert!(f.db.execute("UPDATE session_lifetime_owners SET session_id=?1,provenance='canonical' WHERE lifetime=?2",params![second.0,life.0]).is_err());
    assert!(
        f.db.execute(
            "DELETE FROM session_lifetime_owners WHERE lifetime=?1",
            [&life.0]
        )
        .is_err()
    );
    assert_eq!(
        f.db.query_row(
            "SELECT messages_json FROM transcript_batches WHERE lifetime=?1",
            [&life.0],
            |r| r.get::<_, String>(0)
        )
        .unwrap(),
        payload
    );
    let backup = f.path.with_extension("v24.backup.sqlite3");
    let marker = f.path.with_extension("v24.backup.verified.json");
    assert!(backup.exists());
    assert!(marker.exists());
    let evidence: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&marker).unwrap()).unwrap();
    use sha2::Digest;
    assert_eq!(
        evidence["sha256"],
        format!(
            "{:x}",
            sha2::Sha256::digest(std::fs::read(&backup).unwrap())
        )
    );
    std::fs::remove_file(backup).unwrap();
    std::fs::remove_file(marker).unwrap();
}

#[test]
fn conflicting_v24_lifetime_owners_and_migration_failure_leave_schema_and_facts_unchanged() {
    for ambiguous in [true, false] {
        let f = Fixture::new();
        let Admission::New(a) = f
            .store
            .admit(SessionKey("a".into()), RequestId::Number(1), "a")
            .unwrap()
        else {
            panic!("新准入")
        };
        let Admission::New(b) = f
            .store
            .admit(SessionKey("b".into()), RequestId::Number(2), "b")
            .unwrap()
        else {
            panic!("新准入")
        };
        remove_v25(&f.db);
        if ambiguous {
            let life = f
                .store
                .session_metadata(&a.session_id)
                .unwrap()
                .unwrap()
                .lifetime;
            f.db.execute(
                "UPDATE runs SET lifetime=?1 WHERE id=?2",
                params![life.0, b.run_id.0],
            )
            .unwrap();
        } else {
            f.db.execute_batch("CREATE TRIGGER fail_v25 BEFORE INSERT ON schema_migrations WHEN NEW.version=25 BEGIN SELECT RAISE(ABORT,'迁移故障'); END;").unwrap();
        }
        let before: i64 =
            f.db.query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
                .unwrap();
        assert!(RunStore::open(&f.path).is_err());
        assert_eq!(
            f.db.query_row("SELECT max(version) FROM schema_migrations", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            24
        );
        assert_eq!(
            f.db.query_row(
                "SELECT count(*) FROM sqlite_master WHERE name='session_lifetime_owners'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
        assert_eq!(
            f.db.query_row("SELECT revision FROM snapshot_clock", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            before
        );
        assert_eq!(
            f.db.query_row("SELECT count(*) FROM runs", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
        std::fs::remove_file(f.path.with_extension("v24.backup.sqlite3")).unwrap();
        std::fs::remove_file(f.path.with_extension("v24.backup.verified.json")).unwrap();
    }
}
