//! 发现是 exact run/frozen generation 的持久许可，索引缓存没有执行权。
use super::{RunStore, RuntimeError, read_run_in, sessions};
use agent_core::*;
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};

pub trait DiscoveryRepository: Send + Sync {
    fn publish_discovery(
        &self,
        receipt: &ToolDiscoveryReceipt,
    ) -> Result<ToolDiscoveryReceipt, RuntimeError>;
    fn discovered_tool(
        &self,
        owner: &ExactOwner,
        generation: &str,
        name: &str,
    ) -> Result<bool, RuntimeError>;
    fn discoveries(&self, run: &RunId) -> Result<Vec<ToolDiscoveryReceipt>, RuntimeError>;
    fn request_cancellation(&self, owner: &ExactOwner) -> Result<(), RuntimeError>;
}

fn check_discovery_owner(
    db: &Connection,
    owner: &ExactOwner,
    execution: Option<&EnterpriseOwnerEnvelope>,
) -> Result<(), RuntimeError> {
    let local;
    let envelope = if let Some(execution) = execution {
        execution
    } else {
        local = EnterpriseOwnerEnvelope {
            scope: TenantContext::local(),
            owner: owner.clone(),
        };
        &local
    };
    crate::execution::check_execution_in(db, envelope)?;
    if envelope.owner != *owner {
        return Err(AuthorizationError::StaleOwner.into());
    }
    Ok(())
}
fn discovery_budget_in(db: &Connection, run: &RunId) -> Result<u64, RuntimeError> {
    let (count,bytes):(u64,u64)=db.query_row("SELECT count(*),COALESCE(sum(length(CAST(receipt_json AS BLOB))+length(CAST(operation_id AS BLOB))+length(CAST(receipt_digest AS BLOB))),0) FROM (SELECT receipt_json,operation_id,receipt_digest FROM tool_discoveries WHERE run_id=?1 LIMIT 129)",[&run.0],|r|Ok((r.get(0)?,r.get(1)?)))?;
    if count > 128 || bytes > 1024 * 1024 {
        return Err(CoordinationError::Corrupt.into());
    }
    Ok(bytes)
}
fn protocol(error: serde_json::Error) -> RuntimeError {
    RuntimeError::Protocol(error.to_string())
}
fn catalog_in(
    db: &Connection,
    owner: &ExactOwner,
    generation: &str,
) -> Result<RunSnapshot, RuntimeError> {
    let snapshot = super::requests::frozen_in(db, &owner.run_id)?;
    agent_storage_ports::validate_frozen_tool_catalog(&snapshot, generation)?;
    Ok(snapshot)
}
fn validate(
    receipt: &ToolDiscoveryReceipt,
    snapshot: &RunSnapshot,
    owner: &ExactOwner,
) -> Result<(), RuntimeError> {
    agent_storage_ports::validate_discovery_receipt(receipt, snapshot, owner)
        .map_err(|error| RuntimeError::Protocol(error.to_string()))
}
fn receipts_in(db: &Connection, run: &RunId) -> Result<Vec<ToolDiscoveryReceipt>, RuntimeError> {
    let owner = sessions::owner_in(db, run)?;
    sessions::lifetime_owner_in(db, &owner.session_key, &owner.session_lifetime_id)?;
    discovery_budget_in(db, run)?;
    let mut query=db.prepare("SELECT operation_id,receipt_json,receipt_digest FROM tool_discoveries WHERE run_id=?1 ORDER BY rowid LIMIT 129")?;
    let rows = query
        .query_map(params![run.0], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    if rows.len() > 128 {
        return Err(RuntimeError::Protocol("发现记录超过预算".into()));
    }
    rows.into_iter()
        .map(|(operation, raw, digest)| {
            if format!("{:x}", Sha256::digest(raw.as_bytes())) != digest {
                return Err(RuntimeError::Protocol("发现 receipt 摘要损坏".into()));
            }
            let receipt: ToolDiscoveryReceipt = serde_json::from_str(&raw).map_err(protocol)?;
            let snapshot =
                catalog_in(db, &owner, &receipt.result.catalog_generation).map_err(|error| {
                    match error {
                        RuntimeError::Coordination(CoordinationError::Conflict) => {
                            CoordinationError::Corrupt.into()
                        }
                        error => error,
                    }
                })?;
            validate(&receipt, &snapshot, &owner)?;
            if operation != receipt.operation_id {
                return Err(RuntimeError::Protocol("发现 operation 身份损坏".into()));
            }
            Ok(receipt)
        })
        .collect()
}
impl DiscoveryRepository for RunStore {
    fn publish_discovery(
        &self,
        receipt: &ToolDiscoveryReceipt,
    ) -> Result<ToolDiscoveryReceipt, RuntimeError> {
        self.publish_discovery_scoped(receipt, None)
    }
    fn discovered_tool(
        &self,
        owner: &ExactOwner,
        generation: &str,
        name: &str,
    ) -> Result<bool, RuntimeError> {
        self.discovered_tool_scoped(owner, generation, name, None)
    }
    fn discoveries(&self, run: &RunId) -> Result<Vec<ToolDiscoveryReceipt>, RuntimeError> {
        self.discoveries_scoped(&TenantContext::local(), run)
    }
    fn request_cancellation(&self, owner: &ExactOwner) -> Result<(), RuntimeError> {
        self.request_cancellation_scoped(owner, None)
    }
}

impl RunStore {
    pub(crate) fn publish_discovery_scoped(
        &self,
        receipt: &ToolDiscoveryReceipt,
        execution: Option<&EnterpriseOwnerEnvelope>,
    ) -> Result<ToolDiscoveryReceipt, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction_with_behavior(if execution.is_some() {
            rusqlite::TransactionBehavior::Immediate
        } else {
            rusqlite::TransactionBehavior::Deferred
        })?;
        check_discovery_owner(&tx, &receipt.owner, execution)?;
        let saved_bytes = discovery_budget_in(&tx, &receipt.owner.run_id)?;
        let snapshot = catalog_in(&tx, &receipt.owner, &receipt.result.catalog_generation)?;
        validate(receipt, &snapshot, &receipt.owner)?;
        let raw = serde_json::to_string(receipt).map_err(protocol)?;
        let digest = format!("{:x}", Sha256::digest(raw.as_bytes()));
        let existing: Option<(String, String, String)> = tx
            .query_row(
                "SELECT run_id,receipt_json,receipt_digest FROM tool_discoveries WHERE operation_id=?1 AND run_id=?2",
                params![receipt.operation_id, receipt.owner.run_id.0],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        if let Some((run_id, existing, saved_digest)) = existing {
            if format!("{:x}", Sha256::digest(existing.as_bytes())) != saved_digest {
                return Err(CoordinationError::Corrupt.into());
            }
            let saved: ToolDiscoveryReceipt =
                serde_json::from_str(&existing).map_err(|_| CoordinationError::Corrupt)?;
            validate(&saved, &snapshot, &receipt.owner).map_err(|_| CoordinationError::Corrupt)?;
            if run_id != saved.owner.run_id.0 || saved.operation_id != receipt.operation_id {
                return Err(CoordinationError::Corrupt.into());
            }
            if saved != *receipt {
                return Err(RuntimeError::Protocol("发现 operation 输入冲突".into()));
            }
            tx.commit()?;
            return Ok(saved);
        }
        let run = read_run_in(&tx, &receipt.owner.run_id.0)?
            .ok_or_else(|| RuntimeError::Protocol("发现缺 run".into()))?;
        let cancelled: i64 = tx.query_row(
            "SELECT count(*) FROM events WHERE run_id=?1 AND event='cancellation_requested'",
            params![run.run_id.0],
            |row| row.get(0),
        )?;
        let count: i64 = tx.query_row(
            "SELECT count(*) FROM tool_discoveries WHERE run_id=?1",
            params![run.run_id.0],
            |row| row.get(0),
        )?;
        if run.kind != RunKind::Chat
            || run.status != RunStatus::Running
            || cancelled != 0
            || count >= 128
        {
            return Err(RuntimeError::Protocol(
                "发现发布 owner 已停止/取消或预算耗尽".into(),
            ));
        }
        let incoming_bytes = raw.len() + receipt.operation_id.len() + digest.len();
        if saved_bytes.saturating_add(incoming_bytes as u64) > 1024 * 1024 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        tx.execute(
            "INSERT INTO tool_discoveries VALUES(?1,?2,?3,?4)",
            params![receipt.operation_id, run.run_id.0, raw, digest],
        )
        .map_err(|error| match &error {
            rusqlite::Error::SqliteFailure(code, _)
                if code.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY
                    || code.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE =>
            {
                RuntimeError::Coordination(CoordinationError::Conflict)
            }
            _ => RuntimeError::from(error),
        })?;
        super::insert_event(
            &tx,
            &run.run_id,
            "tool_discovered",
            &serde_json::json!({"operation_id":receipt.operation_id,"catalog_generation":receipt.result.catalog_generation,"names":receipt.result.tools.iter().map(|tool|&tool.name).collect::<Vec<_>>()}),
        )?;
        super::views::commit(tx)?;
        Ok(receipt.clone())
    }
    pub(crate) fn discovered_tool_scoped(
        &self,
        owner: &ExactOwner,
        generation: &str,
        name: &str,
        execution: Option<&EnterpriseOwnerEnvelope>,
    ) -> Result<bool, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        check_discovery_owner(&tx, owner, execution)?;
        catalog_in(&tx, owner, generation)?;
        let run = read_run_in(&tx, &owner.run_id.0)?
            .ok_or_else(|| RuntimeError::Protocol("发现缺 run".into()))?;
        let cancelled: i64 = tx.query_row(
            "SELECT count(*) FROM events WHERE run_id=?1 AND event='cancellation_requested'",
            params![owner.run_id.0],
            |row| row.get(0),
        )?;
        if run.status != RunStatus::Running || cancelled != 0 {
            return Err(CoordinationError::Conflict.into());
        }
        let result = receipts_in(&tx, &owner.run_id)?.iter().any(|receipt| {
            receipt.result.catalog_generation == generation
                && receipt.result.tools.iter().any(|tool| tool.name == name)
        });
        tx.commit()?;
        Ok(result)
    }
    pub(crate) fn discoveries_scoped(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Vec<ToolDiscoveryReceipt>, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        crate::enterprise::check_run_scope_in(&tx, scope, run)?;
        let owner = sessions::owner_in(&tx, run)?;
        sessions::lifetime_owner_in(&tx, &owner.session_key, &owner.session_lifetime_id)?;
        let result = receipts_in(&tx, run)?;
        tx.commit()?;
        Ok(result)
    }
    pub(crate) fn request_cancellation_scoped(
        &self,
        owner: &ExactOwner,
        execution: Option<&EnterpriseOwnerEnvelope>,
    ) -> Result<(), RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction_with_behavior(if execution.is_some() {
            rusqlite::TransactionBehavior::Immediate
        } else {
            rusqlite::TransactionBehavior::Deferred
        })?;
        check_discovery_owner(&tx, owner, execution)?;
        let run = read_run_in(&tx, &owner.run_id.0)?
            .ok_or_else(|| RuntimeError::Protocol("取消缺 run".into()))?;
        if !matches!(
            run.status,
            RunStatus::Running | RunStatus::WaitingInteraction
        ) {
            return Err(RuntimeError::Protocol("取消 owner 已结束".into()));
        }
        let count: i64 = tx.query_row(
            "SELECT count(*) FROM events WHERE run_id=?1 AND event='cancellation_requested'",
            params![run.run_id.0],
            |row| row.get(0),
        )?;
        if count == 0 {
            super::insert_event(
                &tx,
                &run.run_id,
                "cancellation_requested",
                &serde_json::json!({"owner":owner}),
            )?;
        }
        super::views::commit(tx)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SessionLifecycle, SessionQuery, TurnRepository};
    use serde_json::json;

    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    fn path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "discovery-{}-{}.sqlite3",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ))
    }
    fn fixture(path: &std::path::Path) -> (RunStore, ToolDiscoveryReceipt) {
        let store = RunStore::open(path).unwrap();
        let meta = store
            .create_session(&SessionKey("discovery".into()))
            .unwrap();
        let tools = vec![ToolSpec {
            name: "memory__recall".into(),
            description: "读取记忆".into(),
            parameters: json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}),
        }];
        let generation = format!("{:x}", Sha256::digest(serde_json::to_vec(&tools).unwrap()));
        let snapshot: RunSnapshot = serde_json::from_value(json!({"tools":tools,"route":null,"cwd":".","permission_mode":"request_approval","sandbox_requested":"native","sandbox_effective":"native","context_read_only":false,"max_tool_calls":8,"config_generation":0,"tool_catalog_digest":generation})).unwrap();
        let admission = RunAdmission {
            workflow_node: None,
            session_key: meta.key,
            expected_lifetime: Some(meta.lifetime),
            request_id: RequestId::Number(1),
            input: "检索".into(),
            mode: AdmissionMode::Queue,
            plan_execution: None,
        };
        let Admission::New(run) = store.admit_run(&admission, &snapshot).unwrap() else {
            panic!("new")
        };
        store.try_start_queued(&run.run_id).unwrap();
        let query = ToolSearchQuery {
            query: "select:memory__recall".into(),
            limit: Some(1),
        };
        let result = ToolSearchIndex::new(snapshot.tools, generation)
            .unwrap()
            .search(&query)
            .unwrap();
        let receipt = ToolDiscoveryReceipt {
            schema_version: 1,
            owner: store.run_owner(&run.run_id).unwrap(),
            operation_id: "discovery-1".into(),
            query,
            result,
        };
        (store, receipt)
    }

    #[test]
    fn discovery_exact_owner_generation_idempotency_cancel_rollback_and_corruption() {
        for file in [std::path::PathBuf::from(":memory:"), path()] {
            let (store, receipt) = fixture(&file);
            assert!(
                !store
                    .discovered_tool(
                        &receipt.owner,
                        &receipt.result.catalog_generation,
                        "memory__recall"
                    )
                    .unwrap()
            );
            let revision = store
                .session_readback(&receipt.owner.session_key, HistoryReadMode::Canonical)
                .unwrap()
                .snapshot_revision;
            assert!(store.discoveries(&receipt.owner.run_id).unwrap().is_empty());
            assert_eq!(
                store
                    .session_readback(&receipt.owner.session_key, HistoryReadMode::Canonical)
                    .unwrap()
                    .snapshot_revision,
                revision,
                "读取不得写入"
            );
            let mut bad = receipt.clone();
            bad.result.catalog_generation = "changed".into();
            assert!(store.publish_discovery(&bad).is_err());
            bad = receipt.clone();
            bad.result.tools[0].parameters = json!({"type":"string"});
            assert!(store.publish_discovery(&bad).is_err());
            bad = receipt.clone();
            bad.result.tools[0].name = "denied".into();
            assert!(store.publish_discovery(&bad).is_err());
            store.lock_connection().unwrap().execute_batch("CREATE TRIGGER publish_fault BEFORE INSERT ON events WHEN NEW.event='tool_discovered' BEGIN SELECT RAISE(ABORT,'publish fault'); END").unwrap();
            assert!(store.publish_discovery(&receipt).is_err());
            assert!(store.discoveries(&receipt.owner.run_id).unwrap().is_empty());
            store
                .lock_connection()
                .unwrap()
                .execute_batch("DROP TRIGGER publish_fault")
                .unwrap();
            assert_eq!(store.publish_discovery(&receipt).unwrap(), receipt);
            let revision = store
                .session_readback(&receipt.owner.session_key, HistoryReadMode::Canonical)
                .unwrap()
                .snapshot_revision;
            assert_eq!(store.publish_discovery(&receipt).unwrap(), receipt);
            assert_eq!(
                store
                    .session_readback(&receipt.owner.session_key, HistoryReadMode::Canonical)
                    .unwrap()
                    .snapshot_revision,
                revision
            );
            assert!(
                store
                    .discovered_tool(
                        &receipt.owner,
                        &receipt.result.catalog_generation,
                        "memory__recall"
                    )
                    .unwrap()
            );
            assert!(
                !store
                    .discovered_tool(&receipt.owner, &receipt.result.catalog_generation, "denied")
                    .unwrap()
            );
            bad = receipt.clone();
            bad.query.query = "list".into();
            assert!(store.publish_discovery(&bad).is_err());
            let other = store.create_session(&SessionKey("other".into())).unwrap();
            let snapshot = store.run_snapshot(&receipt.owner.run_id).unwrap().unwrap();
            let Admission::New(run) = store
                .admit_run(
                    &RunAdmission {
                        workflow_node: None,
                        session_key: other.key,
                        expected_lifetime: Some(other.lifetime),
                        request_id: RequestId::Number(2),
                        input: "独立许可".into(),
                        mode: AdmissionMode::Queue,
                        plan_execution: None,
                    },
                    &snapshot,
                )
                .unwrap()
            else {
                panic!("new")
            };
            store.try_start_queued(&run.run_id).unwrap();
            let owner = store.run_owner(&run.run_id).unwrap();
            assert!(
                !store
                    .discovered_tool(&owner, &receipt.result.catalog_generation, "memory__recall")
                    .unwrap(),
                "同 generation 的缓存不得共享发现许可"
            );
            bad = receipt.clone();
            bad.owner = owner;
            assert!(
                store.publish_discovery(&bad).is_err(),
                "全局 operation 不能换 owner"
            );
            store.request_cancellation(&receipt.owner).unwrap();
            store.request_cancellation(&receipt.owner).unwrap();
            bad = receipt.clone();
            bad.operation_id = "late-worker".into();
            assert!(store.publish_discovery(&bad).is_err());
            assert!(
                store
                    .discovered_tool(
                        &receipt.owner,
                        &receipt.result.catalog_generation,
                        "memory__recall"
                    )
                    .is_err()
            );
            assert_eq!(
                store.discoveries(&receipt.owner.run_id).unwrap(),
                vec![receipt.clone()]
            );
            store.recover().unwrap();
            assert_eq!(
                store.discoveries(&receipt.owner.run_id).unwrap(),
                vec![receipt.clone()]
            );
            assert!(store.publish_discovery(&bad).is_err());
            store
                .lock_connection()
                .unwrap()
                .execute(
                    "UPDATE tool_discoveries SET receipt_json='{}' WHERE operation_id=?1",
                    params![receipt.operation_id],
                )
                .unwrap();
            assert!(store.discoveries(&receipt.owner.run_id).is_err());
            drop(store);
            if file.to_str() != Some(":memory:") {
                std::fs::remove_file(file).unwrap();
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn discovery_ports_yield_on_real_connection_lock_and_detached_worker_finishes_once() {
        use agent_storage_ports::RunDiscoveryRepository;
        for operation in ["publish", "permission", "history", "cancel"] {
            let file = path();
            let (store, receipt) = fixture(&file);
            let env = EnterpriseOwnerEnvelope {
                scope: TenantContext::local(),
                owner: receipt.owner.clone(),
            };
            if operation != "publish" {
                store.publish_discovery(&receipt).unwrap();
            }
            let occupied = store.clone();
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
            let holder = std::thread::spawn(move || {
                let db = occupied.lock_connection().unwrap();
                ready_tx.send(()).unwrap();
                let _ = release_rx.recv_timeout(std::time::Duration::from_secs(5));
                drop(db);
            });
            ready_rx.await.unwrap();
            let started = std::time::Instant::now();
            let result = tokio::time::timeout(std::time::Duration::from_millis(25), async {
                match operation {
                    "publish" => store
                        .publish_run_discovery(&env, &receipt)
                        .await
                        .map(|_| ()),
                    "permission" => store
                        .read_discovered_tool(
                            &env,
                            &receipt.result.catalog_generation,
                            "memory__recall",
                        )
                        .await
                        .map(|_| ()),
                    "history" => store
                        .read_run_discoveries(&env.scope, &env.owner.run_id)
                        .await
                        .map(|_| ()),
                    _ => store.request_run_cancellation(&env).await,
                }
            })
            .await;
            let elapsed = started.elapsed();
            let permits = store.blocking_budget.available_permits();
            let _ = release_tx.send(());
            holder.join().unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while store.blocking_budget.available_permits() != 4 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(result.is_err(), "连接锁仍持有 {operation}");
            assert!(
                elapsed < std::time::Duration::from_millis(150),
                "SQL锁阻塞Tokio timer {operation}: {elapsed:?}"
            );
            assert_eq!(permits, 3, "等待者取消后worker仍持有permit {operation}");
            assert_eq!(store.discoveries(&env.owner.run_id).unwrap(), vec![receipt]);
            let cancellations:u64=store.lock_connection().unwrap().query_row("SELECT count(*) FROM events WHERE run_id=?1 AND event='cancellation_requested'",[&env.owner.run_id.0],|r|r.get(0)).unwrap();
            assert_eq!(cancellations, u64::from(operation == "cancel"));
            drop(store);
            std::fs::remove_file(file).unwrap();
        }
    }
    #[test]
    fn schema17_discovery_upgrade_backup_transaction_rollback_and_future_rejection() {
        let file = path();
        let (store, receipt) = fixture(&file);
        crate::enterprise::remove_v21_for_fixture(&store.lock_connection().unwrap());
        store.lock_connection().unwrap().execute_batch("DROP TABLE provider_requests; DROP TABLE event_view_stamps; DROP TABLE tool_discoveries; DELETE FROM schema_migrations WHERE version>=18; CREATE TRIGGER discovery_fault BEFORE INSERT ON schema_migrations WHEN NEW.version=18 BEGIN SELECT RAISE(ABORT,'discovery migration fault'); END").unwrap();
        drop(store);
        assert!(RunStore::open(&file).is_err());
        let db = Connection::open(&file).unwrap();
        assert_eq!(
            db.query_row("SELECT max(version) FROM schema_migrations", [], |row| row
                .get::<_, u64>(
                0
            ))
            .unwrap(),
            17
        );
        assert_eq!(
            db.query_row(
                "SELECT count(*) FROM sqlite_master WHERE name='tool_discoveries'",
                [],
                |row| row.get::<_, u64>(0)
            )
            .unwrap(),
            0
        );
        assert!(file.with_extension("v17.backup.sqlite3").exists());
        assert!(file.with_extension("v17.backup.verified.json").exists());
        db.execute_batch("DROP TRIGGER discovery_fault").unwrap();
        drop(db);
        let store = RunStore::open(&file).unwrap();
        assert_eq!(
            store.run_owner(&receipt.owner.run_id).unwrap(),
            receipt.owner
        );
        store
            .lock_connection()
            .unwrap()
            .execute("INSERT INTO schema_migrations VALUES(28,0)", [])
            .unwrap();
        drop(store);
        assert!(RunStore::open(&file).is_err());
        std::fs::remove_file(file).unwrap();
    }
}
