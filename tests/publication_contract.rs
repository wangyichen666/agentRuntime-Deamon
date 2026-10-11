//! 同一合同在 SQLite 真实业务事件与独立 PostgreSQL 发布协调适配器执行。
use agent_core::*;
use agent_storage::{RunStore, SessionQuery};
use agent_storage_ports::{PublicationRepository, publish_to_local_inbox};
use agent_storage_postgres::PgPublicationStore;
use rusqlite::Connection;
use serde_json::json;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    file: PathBuf,
    store: RunStore,
    run: RunId,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.file);
        let _ = std::fs::remove_file(self.file.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(self.file.with_extension("sqlite3-shm"));
    }
}
fn fixture() -> Fixture {
    let file = std::env::temp_dir().join(format!(
        "ma-outbox-{}-{}.sqlite3",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let store = RunStore::open(&file).unwrap();
    let Admission::New(record) = store
        .admit_with_route(
            SessionKey("same-public-session".into()),
            RequestId::Number(1),
            "测试原始请求",
            AdmissionMode::Queue,
            None,
        )
        .unwrap()
    else {
        panic!("新准入")
    };
    Fixture {
        file,
        store,
        run: record.run_id,
    }
}
async fn seed(f: &Fixture) -> OutboxRecord {
    f.store
        .read_publications(&TenantContext::local(), 0, 1)
        .await
        .unwrap()
        .remove(0)
}
async fn common_contract(repo: &dyn PublicationRepository, item: &OutboxRecord) {
    let scope = TenantContext::local();
    let key = item.event.lease_key();
    assert_eq!(repo.append_publication(&item.event).await.unwrap(), *item);
    let mut changed = item.event.clone();
    changed.data = json!({"不同内容":true});
    assert_eq!(
        repo.append_publication(&changed).await,
        Err(CoordinationError::Conflict)
    );
    assert_eq!(
        repo.read_publications(&scope, 0, 0).await,
        Err(CoordinationError::InvalidRequest)
    );
    assert_eq!(
        repo.read_publications(&scope, u64::MAX, 1).await,
        Err(CoordinationError::InvalidRequest)
    );
    let lease = repo
        .acquire_delivery_lease(&key, "owner-1", 60000)
        .await
        .unwrap();
    assert_eq!(
        repo.acquire_delivery_lease(&key, "owner-2", 60000).await,
        Err(CoordinationError::LeaseBusy)
    );
    let renewed = repo
        .acquire_delivery_lease(&key, "owner-1", 60000)
        .await
        .unwrap();
    assert_eq!(renewed.epoch, lease.epoch);
    let claimed = repo.claim_publications(&lease, 32).await.unwrap();
    assert!(claimed.iter().any(|row| row.event_id == item.event_id));
    assert_eq!(
        repo.acknowledge_publication(&lease, &item.event_id, &item.payload_digest)
            .await,
        Err(CoordinationError::Conflict),
        "没有提交的 inbox 收据不得 ack"
    );
    let mut stale = lease.clone();
    stale.epoch += 1;
    assert_eq!(
        repo.acknowledge_publication(&stale, &item.event_id, &item.payload_digest)
            .await,
        Err(CoordinationError::StaleLease)
    );
    let mut forged = item.clone();
    forged.event.data = json!({"伪造":true});
    assert_eq!(
        repo.deliver_to_inbox(&lease, "contract", &forged).await,
        Err(CoordinationError::Conflict)
    );
    assert!(
        repo.deliver_to_inbox(&lease, "contract", item)
            .await
            .unwrap()
    );
    assert!(
        !repo
            .deliver_to_inbox(&lease, "contract", item)
            .await
            .unwrap()
    );
    assert_eq!(repo.inbox_count(&scope, "contract").await.unwrap(), 1);
    let mut foreign_scope = scope.clone();
    foreign_scope.tenant_id = "foreign-tenant".to_owned().try_into().unwrap();
    assert!(
        repo.read_publications(&foreign_scope, 0, 32)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        repo.inbox_count(&foreign_scope, "contract").await.unwrap(),
        0
    );
    let mut foreign_key = key.clone();
    foreign_key.scope = foreign_scope;
    let foreign = repo
        .acquire_delivery_lease(&foreign_key, "foreign-owner", 60000)
        .await
        .unwrap();
    assert_eq!(
        repo.acknowledge_publication(&foreign, &item.event_id, &item.payload_digest)
            .await,
        Err(CoordinationError::Forbidden)
    );
    assert_eq!(repo.claim_publications(&foreign, 32).await.unwrap(), []);
    repo.release_delivery_lease(&foreign).await.unwrap();
    repo.release_delivery_lease(&lease).await.unwrap();
    let next = repo
        .acquire_delivery_lease(&key, "owner-2", 60000)
        .await
        .unwrap();
    assert_eq!(next.epoch, lease.epoch + 1);
    assert_eq!(
        repo.acknowledge_publication(&lease, &item.event_id, &item.payload_digest)
            .await,
        Err(CoordinationError::StaleLease)
    );
    let replay = repo.claim_publications(&next, 32).await.unwrap();
    assert_eq!(replay, claimed);
    assert!(
        !repo
            .deliver_to_inbox(&next, "contract", item)
            .await
            .unwrap()
    );
    repo.acknowledge_publication(&next, &item.event_id, &item.payload_digest)
        .await
        .unwrap();
    repo.acknowledge_publication(&next, &item.event_id, &item.payload_digest)
        .await
        .unwrap();
    repo.release_delivery_lease(&next).await.unwrap();
    let read = repo.read_publications(&scope, 0, 32).await.unwrap();
    assert_eq!(read.len(), claimed.len());
    assert_eq!(read[0].event_id, item.event_id);
    assert!(read[0].delivered);
}
#[tokio::test]
async fn sqlite_shared_publication_contract() {
    let f = fixture();
    let item = seed(&f).await;
    common_contract(&f.store, &item).await;
}
#[tokio::test]
async fn sqlite_restart_after_inbox_commit_before_ack_replays_receipt_without_business_writes() {
    let f = fixture();
    let item = seed(&f).await;
    let scope = TenantContext::local();
    let lease = f
        .store
        .acquire_delivery_lease(&item.event.lease_key(), "crashed", 60000)
        .await
        .unwrap();
    f.store.claim_publications(&lease, 32).await.unwrap();
    f.store
        .deliver_to_inbox(&lease, "local-journal", &item)
        .await
        .unwrap();
    let db = Connection::open(&f.file).unwrap();
    let before: (u64, u64) = db
        .query_row(
            "SELECT (SELECT revision FROM snapshot_clock WHERE id=1),(SELECT count(*) FROM runs)",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    // 故障夹具只推进持久租约过期；没有 sleep 猜竞态，没有修改业务事实。
    db.execute("UPDATE delivery_leases SET expires_at_ms=0", [])
        .unwrap();
    let reopened = RunStore::open(&f.file).unwrap();
    assert_eq!(
        reopened.read_publications(&scope, 0, 32).await.unwrap()[0],
        item
    );
    assert_eq!(
        publish_to_local_inbox(&reopened, &scope, "recovery")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        reopened.inbox_count(&scope, "local-journal").await.unwrap(),
        1
    );
    assert_eq!(
        reopened
            .acknowledge_publication(&lease, &item.event_id, &item.payload_digest)
            .await,
        Err(CoordinationError::StaleLease)
    );
    let after: (u64, u64) = db
        .query_row(
            "SELECT (SELECT revision FROM snapshot_clock WHERE id=1),(SELECT count(*) FROM runs)",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(before, after);
}
#[tokio::test]
async fn sqlite_outbox_failure_rolls_back_original_event_and_terminal_transaction() {
    let f = fixture();
    let before = seed(&f).await;
    let db = Connection::open(&f.file).unwrap();
    db.execute_batch("CREATE TRIGGER outbox_fault BEFORE INSERT ON event_outbox BEGIN SELECT RAISE(ABORT,'故障注入'); END;").unwrap();
    let count: u64 = db
        .query_row("SELECT COUNT(*) FROM events", [], |r| r.get(0))
        .unwrap();
    assert!(
        f.store
            .append_event(&f.run, "provider_audit", &json!({"status":"observed"}))
            .is_err()
    );
    assert!(
        f.store
            .finish(&f.run, RunStatus::Completed, Some("禁止提交"), None)
            .is_err()
    );
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM events", [], |r| r.get::<_, u64>(0))
            .unwrap(),
        count
    );
    assert_eq!(
        f.store.read_run(&f.run).unwrap().unwrap().status,
        RunStatus::Queued
    );
    assert_eq!(
        f.store
            .read_publications(&TenantContext::local(), 0, 32)
            .await
            .unwrap(),
        vec![before]
    );
    db.execute_batch("DROP TRIGGER outbox_fault;").unwrap();
    f.store
        .finish(&f.run, RunStatus::Completed, Some("真实提交"), None)
        .unwrap();
    let last = f
        .store
        .read_publications(&TenantContext::local(), 0, 32)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert!(last.event.stamp.terminal);
    assert_eq!(
        last.event.envelope.owner,
        f.store.run_owner(&f.run).unwrap()
    );
}
#[tokio::test]
async fn sqlite_independent_connections_compete_with_controlled_barrier_and_single_lease_winner() {
    let f = fixture();
    let item = seed(&f).await;
    let barrier = Arc::new(Barrier::new(3));
    let handles = (0..2)
        .map(|n| {
            let barrier = barrier.clone();
            let file = f.file.clone();
            let key = item.event.lease_key();
            std::thread::spawn(move || {
                let store = RunStore::open(&file).unwrap();
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                barrier.wait();
                runtime.block_on(store.acquire_delivery_lease(&key, &format!("owner-{n}"), 60000))
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    let values = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(values.iter().filter(|v| v.is_ok()).count(), 1);
    assert_eq!(
        values
            .iter()
            .filter(|v| **v == Err(CoordinationError::LeaseBusy))
            .count(),
        1
    );
}
#[tokio::test]
async fn sqlite_v21_migration_does_not_backfill_old_events_and_verified_backup_reopens() {
    let f = fixture();
    let old = seed(&f).await;
    let db = Connection::open(&f.file).unwrap();
    db.execute_batch("DELETE FROM role_bindings WHERE tenant_id='local-tenant' AND subject_id='local-cron-workload'; DELETE FROM principals WHERE tenant_id='local-tenant' AND subject_id='local-cron-workload'; DELETE FROM schema_migrations WHERE version=27; DROP TRIGGER native_event_provenance_insert; DROP TRIGGER immutable_event_provenance_update; DROP TRIGGER immutable_event_stamp_delete; ALTER TABLE events DROP COLUMN view_provenance; DELETE FROM schema_migrations WHERE version=26; DROP TRIGGER bind_lifetime_insert; DROP TRIGGER bind_lifetime_update; DROP TRIGGER immutable_run_incarnation; DROP TRIGGER immutable_lifetime_owner_update; DROP TRIGGER immutable_lifetime_owner_delete; DROP TABLE session_lifetime_owners; DELETE FROM schema_migrations WHERE version=25; DROP TABLE session_writer_leases; DROP INDEX session_scope_writer_key; DROP INDEX tenant_writer_domain; DELETE FROM schema_migrations WHERE version=24; DROP TRIGGER artifact_publication_immutable; DROP TRIGGER artifact_publication_monotonic; DROP TABLE artifact_publications; DELETE FROM schema_migrations WHERE version=23; DROP TRIGGER outbox_immutable;DROP TABLE publication_inbox;DROP TABLE event_outbox;DROP TABLE delivery_leases;DELETE FROM schema_migrations WHERE version=22;").unwrap();
    let upgraded = RunStore::open(&f.file).unwrap();
    assert!(
        upgraded
            .read_publications(&TenantContext::local(), 0, 32)
            .await
            .unwrap()
            .is_empty()
    );
    let backup = f.file.with_extension("v21.backup.sqlite3");
    assert!(backup.exists());
    assert!(backup.with_extension("verified.json").exists());
    let restored = f.file.with_extension("restored.sqlite3");
    let backup_bytes = std::fs::read(&backup).unwrap();
    std::fs::copy(&backup, &restored).unwrap();
    let upgraded_backup = RunStore::open(&restored).unwrap();
    assert!(
        upgraded_backup
            .read_publications(&TenantContext::local(), 0, 32)
            .await
            .unwrap()
            .is_empty()
    );
    let next = upgraded
        .append_event(&f.run, "provider_audit", &json!({}))
        .unwrap();
    let records = upgraded
        .read_publications(&TenantContext::local(), 0, 32)
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].event.event_seq, next);
    assert_ne!(records[0].event_id, old.event_id);
    db.execute("INSERT INTO schema_migrations VALUES(28,0)", [])
        .unwrap();
    assert!(RunStore::open(&f.file).is_err());
    drop(upgraded_backup);
    assert_eq!(std::fs::read(&backup).unwrap(), backup_bytes);
    let _ = std::fs::remove_file(restored);
    let _ = std::fs::remove_file(backup.with_extension("verified.json"));
    let _ = std::fs::remove_file(backup);
}
#[tokio::test]
async fn sqlite_tamper_is_rejected_and_sql_trigger_prevents_body_rewrites() {
    let f = fixture();
    let _ = seed(&f).await;
    let db = Connection::open(&f.file).unwrap();
    assert!(
        db.execute("UPDATE event_outbox SET payload_json='{}'", [])
            .is_err()
    );
    db.execute_batch("DELETE FROM role_bindings WHERE tenant_id='local-tenant' AND subject_id='local-cron-workload'; DELETE FROM principals WHERE tenant_id='local-tenant' AND subject_id='local-cron-workload'; DELETE FROM schema_migrations WHERE version=27; DROP TRIGGER native_event_provenance_insert; DROP TRIGGER immutable_event_provenance_update; DROP TRIGGER immutable_event_stamp_delete; ALTER TABLE events DROP COLUMN view_provenance; DELETE FROM schema_migrations WHERE version=26; DROP TRIGGER bind_lifetime_insert; DROP TRIGGER bind_lifetime_update; DROP TRIGGER immutable_run_incarnation; DROP TRIGGER immutable_lifetime_owner_update; DROP TRIGGER immutable_lifetime_owner_delete; DROP TABLE session_lifetime_owners; DELETE FROM schema_migrations WHERE version=25; DROP TABLE session_writer_leases; DROP INDEX session_scope_writer_key; DROP INDEX tenant_writer_domain; DELETE FROM schema_migrations WHERE version=24; DROP TRIGGER artifact_publication_immutable; DROP TRIGGER artifact_publication_monotonic; DROP TABLE artifact_publications; DELETE FROM schema_migrations WHERE version=23; DROP TRIGGER outbox_immutable;UPDATE event_outbox SET payload_json='{}';")
        .unwrap();
    assert_eq!(
        f.store
            .read_publications(&TenantContext::local(), 0, 32)
            .await,
        Err(CoordinationError::Corrupt)
    );
}
async fn pg(schema: &str) -> PgPublicationStore {
    PgPublicationStore::connect_local(
        &std::env::var("AGENT_TEST_POSTGRES_DB").expect("真实 PG lane 必须显式给出隔离数据库"),
        &std::env::var("AGENT_TEST_POSTGRES_USER").expect("真实 PG lane 必须显式用户"),
        schema,
    )
    .await
    .unwrap()
}
fn pg_schema(suffix: &str) -> String {
    format!(
        "contract_{}_{}_{suffix}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}
#[tokio::test]
#[ignore = "必须通过独立真实 PostgreSQL lane 运行，不把普通本地测试当成 PostgreSQL 验证"]
async fn postgres_shared_publication_contract() {
    let f = fixture();
    let item = seed(&f).await;
    let store = pg(&pg_schema("shared")).await;
    let imported = store.append_publication(&item.event).await.unwrap();
    assert_eq!(imported, item);
    common_contract(&store, &item).await;
}
#[tokio::test]
#[ignore = "独立真实 PostgreSQL lane"]
async fn postgres_reconnect_after_inbox_commit_uses_same_ids_and_no_duplicate_receipt() {
    let f = fixture();
    let item = seed(&f).await;
    let schema = pg_schema("restart");
    let scope = TenantContext::local();
    let store = pg(&schema).await;
    store.append_publication(&item.event).await.unwrap();
    let lease = store
        .acquire_delivery_lease(&item.event.lease_key(), "before-restart", 60000)
        .await
        .unwrap();
    store.claim_publications(&lease, 32).await.unwrap();
    store
        .deliver_to_inbox(&lease, "local-journal", &item)
        .await
        .unwrap();
    // 仅释放 transport owner 模拟可恢复断线，独立连接重开不新建业务事实。
    store.release_delivery_lease(&lease).await.unwrap();
    drop(store);
    let reopened = pg(&schema).await;
    assert_eq!(
        reopened.read_publications(&scope, 0, 32).await.unwrap()[0],
        item
    );
    assert_eq!(
        publish_to_local_inbox(&reopened, &scope, "after-restart")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        reopened.inbox_count(&scope, "local-journal").await.unwrap(),
        1
    );
    assert_eq!(
        reopened
            .acknowledge_publication(&lease, &item.event_id, &item.payload_digest)
            .await,
        Err(CoordinationError::StaleLease)
    );
}
#[tokio::test]
#[ignore = "独立真实 PostgreSQL lane"]
async fn postgres_two_connections_have_one_winner_without_sleep() {
    let f = fixture();
    let item = seed(&f).await;
    let schema = pg_schema("race");
    let first = pg(&schema).await;
    let second = pg(&schema).await;
    first.append_publication(&item.event).await.unwrap();
    let key = item.event.lease_key();
    let (a, b) = tokio::join!(
        first.acquire_delivery_lease(&key, "first", 60000),
        second.acquire_delivery_lease(&key, "second", 60000)
    );
    assert_ne!(a.is_ok(), b.is_ok());
    assert!(a == Err(CoordinationError::LeaseBusy) || b == Err(CoordinationError::LeaseBusy));
}

fn pg_admin(schema: &str, sql: &str) {
    assert!(
        schema
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_')
    );
    let result = std::process::Command::new("psql")
        .args([
            "-X",
            "-w",
            "-h",
            "/tmp",
            "-U",
            &std::env::var("AGENT_TEST_POSTGRES_USER").unwrap(),
            "-d",
            &std::env::var("AGENT_TEST_POSTGRES_DB").unwrap(),
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            &format!("SET search_path TO \"{schema}\";{sql}"),
        ])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}
#[tokio::test]
#[ignore = "独立真实 PostgreSQL lane"]
async fn postgres_expired_epoch_and_crash_before_ack_are_recovered_by_real_worker() {
    let f = fixture();
    let item = seed(&f).await;
    let schema = pg_schema("worker");
    let scope = TenantContext::local();
    let store = pg(&schema).await;
    store.append_publication(&item.event).await.unwrap();
    let lease = store
        .acquire_delivery_lease(&item.event.lease_key(), "crashed-worker", 60000)
        .await
        .unwrap();
    store.claim_publications(&lease, 32).await.unwrap();
    store
        .deliver_to_inbox(&lease, "local-journal", &item)
        .await
        .unwrap();
    pg_admin(&schema, "UPDATE delivery_leases SET expires_at_ms=0;");
    assert_eq!(
        store.claim_publications(&lease, 32).await,
        Err(CoordinationError::StaleLease)
    );
    store.close().await;
    let output = tokio::process::Command::new(
        std::env::var("AGENT_TEST_PUBLICATION_WORKER").expect("lane 必须构建真实 worker binary"),
    )
    .arg("--once")
    .env(
        "AGENT_PUBLICATION_DB",
        std::env::var("AGENT_TEST_POSTGRES_DB").unwrap(),
    )
    .env(
        "AGENT_PUBLICATION_USER",
        std::env::var("AGENT_TEST_POSTGRES_USER").unwrap(),
    )
    .env("AGENT_PUBLICATION_SCHEMA", &schema)
    .output()
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["delivered"], 1);
    let reopened = pg(&schema).await;
    assert_eq!(
        reopened.inbox_count(&scope, "local-journal").await.unwrap(),
        1
    );
    let read = reopened.read_publications(&scope, 0, 32).await.unwrap();
    assert_eq!(read[0].event_id, item.event_id);
    assert!(read[0].delivered);
    assert_eq!(
        reopened
            .acknowledge_publication(&lease, &item.event_id, &item.payload_digest)
            .await,
        Err(CoordinationError::StaleLease)
    );
}
#[tokio::test]
#[ignore = "独立真实 PostgreSQL lane"]
async fn postgres_immutable_payload_corruption_future_schema_and_disconnect_fail_closed() {
    let f = fixture();
    let item = seed(&f).await;
    let schema = pg_schema("fault");
    let store = pg(&schema).await;
    store.append_publication(&item.event).await.unwrap();
    pg_admin(
        &schema,
        "ALTER TABLE event_outbox DISABLE TRIGGER outbox_immutable;UPDATE event_outbox SET payload_json='{}';ALTER TABLE event_outbox ENABLE TRIGGER outbox_immutable;",
    );
    assert_eq!(
        store
            .read_publications(&TenantContext::local(), 0, 32)
            .await,
        Err(CoordinationError::Corrupt)
    );
    pg_admin(&schema, "INSERT INTO coordination_migrations VALUES(2);");
    let future = PgPublicationStore::connect_local(
        &std::env::var("AGENT_TEST_POSTGRES_DB").unwrap(),
        &std::env::var("AGENT_TEST_POSTGRES_USER").unwrap(),
        &schema,
    )
    .await;
    assert!(matches!(future, Err(CoordinationError::SchemaTooNew)));
    // 只中断本次隔离数据库连接；不接触 postgres 或用户现有数据库。
    pg_admin(
        &schema,
        &format!(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=current_database() AND application_name='agent-publication-{schema}' AND pid<>pg_backend_pid();"
        ),
    );
    assert_eq!(
        store
            .read_publications(&TenantContext::local(), 0, 32)
            .await,
        Err(CoordinationError::StorageUnavailable)
    );
}

#[tokio::test]
#[ignore = "独立真实 PostgreSQL lane"]
async fn postgres_equal_public_session_and_run_keys_remain_isolated_by_full_scope() {
    let f = fixture();
    let local = seed(&f).await;
    let store = pg(&pg_schema("scope")).await;
    let mut foreign = local.event.clone();
    foreign.envelope.scope.tenant_id = "other-tenant".to_owned().try_into().unwrap();
    foreign.envelope.scope.project_id = "other-project".to_owned().try_into().unwrap();
    // 是协调 envelope 的域隔离，不把它称为第二租户已具备业务准入。
    let foreign_scope = foreign.envelope.scope.clone();
    let a = store.append_publication(&local.event).await.unwrap();
    let b = store.append_publication(&foreign).await.unwrap();
    assert_ne!(a.event_id, b.event_id);
    let lease_a = store
        .acquire_delivery_lease(&a.event.lease_key(), "a", 60000)
        .await
        .unwrap();
    let lease_b = store
        .acquire_delivery_lease(&b.event.lease_key(), "b", 60000)
        .await
        .unwrap();
    assert_eq!(
        store.claim_publications(&lease_a, 32).await.unwrap(),
        vec![a.clone()]
    );
    assert_eq!(
        store.claim_publications(&lease_b, 32).await.unwrap(),
        vec![b.clone()]
    );
    assert_eq!(
        store.deliver_to_inbox(&lease_a, "local-journal", &b).await,
        Err(CoordinationError::Forbidden)
    );
    store
        .deliver_to_inbox(&lease_b, "local-journal", &b)
        .await
        .unwrap();
    store
        .acknowledge_publication(&lease_b, &b.event_id, &b.payload_digest)
        .await
        .unwrap();
    assert_eq!(
        store
            .inbox_count(&TenantContext::local(), "local-journal")
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .inbox_count(&foreign_scope, "local-journal")
            .await
            .unwrap(),
        1
    );
    assert!(
        !store
            .read_publications(&TenantContext::local(), 0, 32)
            .await
            .unwrap()[0]
            .delivered
    );
    assert!(
        store
            .read_publications(&foreign_scope, 0, 32)
            .await
            .unwrap()[0]
            .delivered
    );
    assert_eq!(
        f.store.append_publication(&foreign).await,
        Err(CoordinationError::Forbidden),
        "SQLite 端口不能绕过真实业务事务伪造外域事件"
    );
}

#[tokio::test]
async fn sqlite_receipt_foreign_scope_and_wrong_digest_are_rejected_by_database_foreign_key() {
    let f = fixture();
    let item = seed(&f).await;
    let db = Connection::open(&f.file).unwrap();
    db.pragma_update(None, "foreign_keys", "ON").unwrap();
    for (tenant, digest) in [
        ("foreign-tenant", item.payload_digest.as_str()),
        ("local-tenant", "incorrect-digest"),
    ] {
        let result=db.execute("INSERT INTO publication_inbox VALUES('local-organization',?1,'local-project','local-agent','injected',?2,?3)",rusqlite::params![tenant,item.event_id,digest]);
        assert!(
            matches!(result,Err(rusqlite::Error::SqliteFailure(error,_)) if error.extended_code==rusqlite::ffi::SQLITE_CONSTRAINT_FOREIGNKEY)
        );
    }
    assert_eq!(
        f.store
            .inbox_count(&TenantContext::local(), "injected")
            .await
            .unwrap(),
        0
    );
}

#[tokio::test(flavor = "current_thread")]
async fn sqlite_async_delivery_write_yields_the_runtime_during_real_database_lock() {
    let f = fixture();
    let key = seed(&f).await.event.lease_key();
    let blocker = Connection::open(&f.file).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let started = std::time::Instant::now();
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        f.store
            .acquire_delivery_lease(&key, "cancelled-acquire", 5000),
    )
    .await;
    let elapsed = started.elapsed();
    blocker.execute_batch("ROLLBACK").unwrap();
    assert!(
        result.is_err(),
        "异步端口不能在执行器中同步等五秒SQLite写锁：{result:?}"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "数据库busy不能阻塞单线程运行时定时器：{elapsed:?}"
    );
    // 取消不证明 SQL 回滚；解除真实写锁后，原事实可读回，不能猜测不存在租约。
    let records = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        f.store.read_publications(&TenantContext::local(), 0, 1),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(records.len(), 1);
}

async fn foreign_corrupt_payload_contract(repo: &dyn PublicationRepository, item: &OutboxRecord) {
    let mut results = Vec::with_capacity(12);
    for dimension in 0..6 {
        let mut key = item.event.lease_key();
        match dimension {
            0 => key.scope.organization_id = "foreign-org".to_owned().try_into().unwrap(),
            1 => key.scope.tenant_id = "foreign-tenant".to_owned().try_into().unwrap(),
            2 => key.scope.project_id = "foreign-project".to_owned().try_into().unwrap(),
            3 => key.scope.agent_uid = "foreign-agent".to_owned().try_into().unwrap(),
            4 => key.session_key.0 = "foreign-session".into(),
            _ => key.session_lifetime_id.0 = "foreign-lifetime".into(),
        }
        let lease = repo
            .acquire_delivery_lease(&key, "foreign-worker", 60000)
            .await
            .unwrap();
        results.push((
            dimension,
            "ACK",
            repo.acknowledge_publication(&lease, &item.event_id, &item.payload_digest)
                .await,
        ));
        results.push((
            dimension,
            "inbox",
            repo.deliver_to_inbox(&lease, "foreign-contract", item)
                .await
                .map(|_| ()),
        ));
        assert!(
            repo.claim_publications(&lease, 32)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            repo.inbox_count(&key.scope, "foreign-contract")
                .await
                .unwrap(),
            0
        );
        repo.release_delivery_lease(&lease).await.unwrap();
    }
    assert!(
        results
            .iter()
            .all(|(_, _, result)| *result == Err(CoordinationError::Forbidden)),
        "六维外域应在SQL拒绝，而非先解码私有payload：{results:?}"
    );
}
#[tokio::test]
async fn sqlite_foreign_corrupt_outbox_must_be_forbidden_before_decode() {
    let f = fixture();
    let item = seed(&f).await;
    let db = Connection::open(&f.file).unwrap();
    // 只在隔离fixture暂时拆不可变payload触发器，构造保存事实损坏。
    let mut corrupt = item.event.clone();
    corrupt.data = serde_json::json!({"changed":"保存的合法领域JSON，旧SHA"});
    db.execute_batch("DROP TRIGGER outbox_immutable").unwrap();
    db.execute(
        "UPDATE event_outbox SET payload_json=?1 WHERE event_id=?2",
        rusqlite::params![serde_json::to_string(&corrupt).unwrap(), item.event_id],
    )
    .unwrap();
    let before:(u64,u64,u64)=db.query_row("SELECT (SELECT revision FROM snapshot_clock),(SELECT count(*) FROM publication_inbox),(SELECT count(*) FROM event_outbox WHERE delivered=1)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
    foreign_corrupt_payload_contract(&f.store, &item).await;
    assert_eq!(db.query_row::<(u64,u64,u64),_,_>("SELECT (SELECT revision FROM snapshot_clock),(SELECT count(*) FROM publication_inbox),(SELECT count(*) FROM event_outbox WHERE delivered=1)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap(),before);
    assert_eq!(
        f.store
            .read_publications(&item.event.envelope.scope, 0, 32)
            .await,
        Err(CoordinationError::Corrupt)
    );
}
#[tokio::test]
#[ignore = "独立真实PostgreSQL lane"]
async fn postgres_foreign_corrupt_outbox_must_be_forbidden_before_decode() {
    let f = fixture();
    let item = seed(&f).await;
    let schema = pg_schema("foreign_corrupt");
    let store = pg(&schema).await;
    store.append_publication(&item.event).await.unwrap();
    let mut corrupt = item.event.clone();
    corrupt.data = serde_json::json!({"changed":"保存的合法领域JSON，旧SHA"});
    let raw = serde_json::to_string(&corrupt).unwrap().replace('\'', "''");
    pg_admin(
        &schema,
        &format!(
            "ALTER TABLE event_outbox DISABLE TRIGGER outbox_immutable;UPDATE event_outbox SET payload_json='{raw}' WHERE event_id='{}';ALTER TABLE event_outbox ENABLE TRIGGER outbox_immutable;",
            item.event_id
        ),
    );
    foreign_corrupt_payload_contract(&store, &item).await;
    assert_eq!(
        store
            .read_publications(&item.event.envelope.scope, 0, 32)
            .await,
        Err(CoordinationError::Corrupt)
    );
}

#[tokio::test]
async fn sqlite_outbox_sql_binding_mismatch_must_not_return_original_private_envelope() {
    let mut observed = Vec::new();
    for (dimension, column) in [
        "organization_id",
        "tenant_id",
        "project_id",
        "agent_uid",
        "session_key",
        "lifetime",
        "run_id",
        "event_seq",
    ]
    .into_iter()
    .enumerate()
    {
        let f = fixture();
        let item = seed(&f).await;
        let db = Connection::open(&f.file).unwrap();
        // 仅替换为真实events外键目标，保留FK约束；移除其协调row避免唯一键碰撞。
        let value = if column == "run_id" {
            let Admission::New(other) = f
                .store
                .admit_with_route(
                    item.event.envelope.owner.session_key.clone(),
                    RequestId::Number(99),
                    "字段绑定目标",
                    AdmissionMode::Queue,
                    None,
                )
                .unwrap()
            else {
                panic!("新准入")
            };
            db.execute(
                "DELETE FROM event_outbox WHERE run_id=?1",
                [&other.run_id.0],
            )
            .unwrap();
            other.run_id.0
        } else if column == "event_seq" {
            let seq = f
                .store
                .append_event(&f.run, "provider_audit", &json!({"binding_target":true}))
                .unwrap();
            db.execute(
                "DELETE FROM event_outbox WHERE run_id=?1 AND event_seq=?2",
                rusqlite::params![f.run.0, seq.0],
            )
            .unwrap();
            seq.0.to_string()
        } else {
            "binding-foreign".into()
        };
        db.execute_batch("DROP TRIGGER outbox_immutable").unwrap();
        db.execute(
            &format!("UPDATE event_outbox SET {column}=?1 WHERE event_id=?2"),
            rusqlite::params![value, item.event_id],
        )
        .unwrap();
        let mut scope = item.event.envelope.scope.clone();
        match dimension {
            0 => scope.organization_id = "binding-foreign".to_owned().try_into().unwrap(),
            1 => scope.tenant_id = "binding-foreign".to_owned().try_into().unwrap(),
            2 => scope.project_id = "binding-foreign".to_owned().try_into().unwrap(),
            3 => scope.agent_uid = "binding-foreign".to_owned().try_into().unwrap(),
            _ => {}
        }
        let before: u64 = db
            .query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
        let result = f.store.read_publications(&scope, 0, 32).await;
        observed.push((column, result.map(|rows| rows.len())));
        assert_eq!(
            db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
                .unwrap(),
            before
        );
    }
    assert!(
        observed
            .iter()
            .all(|(_, result)| *result == Err(CoordinationError::Corrupt)),
        "保存SQL列与JSON不同，不能返回原私密envelope：{observed:?}"
    );
}
#[tokio::test]
#[ignore = "独立真实PostgreSQL lane"]
async fn postgres_outbox_sql_binding_mismatch_must_not_return_original_private_envelope() {
    let mut observed = Vec::new();
    for (dimension, column) in [
        "organization_id",
        "tenant_id",
        "project_id",
        "agent_uid",
        "session_key",
        "lifetime",
        "run_id",
        "event_seq",
    ]
    .into_iter()
    .enumerate()
    {
        let f = fixture();
        let item = seed(&f).await;
        let schema = pg_schema(&format!("binding_{dimension}"));
        let store = pg(&schema).await;
        store.append_publication(&item.event).await.unwrap();
        let value = if column == "event_seq" {
            "12345"
        } else {
            "binding-foreign"
        };
        pg_admin(
            &schema,
            &format!(
                "ALTER TABLE event_outbox DISABLE TRIGGER outbox_immutable;UPDATE event_outbox SET {column}='{value}' WHERE event_id='{}';ALTER TABLE event_outbox ENABLE TRIGGER outbox_immutable;",
                item.event_id
            ),
        );
        let mut scope = item.event.envelope.scope.clone();
        match dimension {
            0 => scope.organization_id = "binding-foreign".to_owned().try_into().unwrap(),
            1 => scope.tenant_id = "binding-foreign".to_owned().try_into().unwrap(),
            2 => scope.project_id = "binding-foreign".to_owned().try_into().unwrap(),
            3 => scope.agent_uid = "binding-foreign".to_owned().try_into().unwrap(),
            _ => {}
        }
        observed.push((
            column,
            store
                .read_publications(&scope, 0, 32)
                .await
                .map(|rows| rows.len()),
        ));
    }
    assert!(
        observed
            .iter()
            .all(|(_, result)| *result == Err(CoordinationError::Corrupt)),
        "保存SQL列与JSON不同，不能返回原私密envelope：{observed:?}"
    );
}

#[tokio::test]
async fn sqlite_pending_delivery_keys_reject_oversized_saved_identity() {
    for column in ["session_key", "lifetime"] {
        let f = fixture();
        let item = seed(&f).await;
        let db = Connection::open(&f.file).unwrap();
        db.execute_batch("DROP TRIGGER outbox_immutable").unwrap();
        db.execute(
            &format!("UPDATE event_outbox SET {column}=?1 WHERE event_id=?2"),
            rusqlite::params!["x".repeat(1024 * 1024 + 1), item.event_id],
        )
        .unwrap();
        let before: u64 = db
            .query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
        let observed = f
            .store
            .pending_delivery_keys(&item.event.envelope.scope, 16)
            .await
            .map(|keys| keys.len());
        assert_eq!(
            observed,
            Err(CoordinationError::Corrupt),
            "超预算保存元数据 {column}"
        );
        assert_eq!(
            db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
                .unwrap(),
            before
        );
    }
}
#[tokio::test]
#[ignore = "独立真实PostgreSQL lane"]
async fn postgres_pending_delivery_keys_reject_oversized_saved_identity() {
    for column in ["session_key", "lifetime"] {
        let f = fixture();
        let item = seed(&f).await;
        let schema = pg_schema("pending_budget");
        let store = pg(&schema).await;
        store.append_publication(&item.event).await.unwrap();
        pg_admin(
            &schema,
            &format!(
                "ALTER TABLE event_outbox DISABLE TRIGGER outbox_immutable;UPDATE event_outbox SET {column}=repeat('x',1048577) WHERE event_id='{}';ALTER TABLE event_outbox ENABLE TRIGGER outbox_immutable;",
                item.event_id
            ),
        );
        let observed = store
            .pending_delivery_keys(&item.event.envelope.scope, 16)
            .await
            .map(|keys| keys.len());
        assert_eq!(
            observed,
            Err(CoordinationError::Corrupt),
            "超预算保存元数据 {column}"
        );
    }
}

fn oversized_saved_publication(item: &OutboxRecord) -> String {
    let mut event = item.event.clone();
    event.data = serde_json::json!({"padding":"字".repeat(1500000)});
    serde_json::to_string(&event).unwrap()
}
#[tokio::test]
async fn sqlite_saved_large_publication_rejects_before_any_claim_inbox_ack_or_clock_repair() {
    let f = fixture();
    let item = seed(&f).await;
    let scope = item.event.envelope.scope.clone();
    let lease = f
        .store
        .acquire_delivery_lease(&item.event.lease_key(), "bounded-contract", 60000)
        .await
        .unwrap();
    let db = Connection::open(&f.file).unwrap();
    db.execute_batch("DROP TRIGGER outbox_immutable").unwrap();
    let raw = oversized_saved_publication(&item);
    db.execute(
        "UPDATE event_outbox SET payload_json=?1 WHERE event_id=?2",
        rusqlite::params![raw, item.event_id],
    )
    .unwrap();
    let before:(u64,u64,u64,u64)=db.query_row("SELECT (SELECT revision FROM snapshot_clock),(SELECT count(*) FROM publication_inbox),(SELECT count(*) FROM event_outbox WHERE delivered=1),(SELECT count(*) FROM event_outbox WHERE claim_owner IS NOT NULL)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
    assert_eq!(
        f.store.read_publications(&scope, 0, 32).await,
        Err(CoordinationError::Corrupt)
    );
    assert_eq!(
        f.store.claim_publications(&lease, 32).await,
        Err(CoordinationError::Corrupt)
    );
    assert_eq!(
        f.store
            .deliver_to_inbox(&lease, "bounded-contract", &item)
            .await,
        Err(CoordinationError::Corrupt)
    );
    assert_eq!(
        f.store
            .acknowledge_publication(&lease, &item.event_id, &item.payload_digest)
            .await,
        Err(CoordinationError::Corrupt)
    );
    assert_eq!(db.query_row::<(u64,u64,u64,u64),_,_>("SELECT (SELECT revision FROM snapshot_clock),(SELECT count(*) FROM publication_inbox),(SELECT count(*) FROM event_outbox WHERE delivered=1),(SELECT count(*) FROM event_outbox WHERE claim_owner IS NOT NULL)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap(),before);
    f.store.release_delivery_lease(&lease).await.unwrap();
}
#[tokio::test]
#[ignore = "独立真实PostgreSQL lane"]
async fn postgres_saved_large_publication_rejects_before_any_claim_inbox_ack_or_repair() {
    let f = fixture();
    let item = seed(&f).await;
    let schema = pg_schema("large_publication");
    let store = pg(&schema).await;
    store.append_publication(&item.event).await.unwrap();
    let scope = item.event.envelope.scope.clone();
    let lease = store
        .acquire_delivery_lease(&item.event.lease_key(), "bounded-contract", 60000)
        .await
        .unwrap();
    // 在隔离DB生成大JSON，避免把大payload放进进程命令行。
    pg_admin(
        &schema,
        &format!(
            "ALTER TABLE event_outbox DISABLE TRIGGER outbox_immutable;UPDATE event_outbox SET payload_json=jsonb_set(payload_json::jsonb,'{{data}}',jsonb_build_object('padding',repeat('字',1500000)))::text WHERE event_id='{}';ALTER TABLE event_outbox ENABLE TRIGGER outbox_immutable;",
            item.event_id
        ),
    );
    assert_eq!(
        store.read_publications(&scope, 0, 32).await,
        Err(CoordinationError::Corrupt)
    );
    assert_eq!(
        store.claim_publications(&lease, 32).await,
        Err(CoordinationError::Corrupt)
    );
    assert_eq!(
        store
            .deliver_to_inbox(&lease, "bounded-contract", &item)
            .await,
        Err(CoordinationError::Corrupt)
    );
    assert_eq!(
        store
            .acknowledge_publication(&lease, &item.event_id, &item.payload_digest)
            .await,
        Err(CoordinationError::Corrupt)
    );
    assert_eq!(
        store.inbox_count(&scope, "bounded-contract").await.unwrap(),
        0
    );
    pg_admin(
        &schema,
        "DO $$ BEGIN IF EXISTS(SELECT 1 FROM event_outbox WHERE delivered OR claim_owner IS NOT NULL) THEN RAISE EXCEPTION '故障读取不得写claim/ack'; END IF; END $$;",
    );
    store.release_delivery_lease(&lease).await.unwrap();
}
