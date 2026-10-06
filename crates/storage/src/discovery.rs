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

fn protocol(error: serde_json::Error) -> RuntimeError {
    RuntimeError::Protocol(error.to_string())
}
fn catalog_in(
    db: &Connection,
    owner: &ExactOwner,
    generation: &str,
) -> Result<RunSnapshot, RuntimeError> {
    sessions::fence_in(db, owner)?;
    let raw: String = db.query_row(
        "SELECT snapshot_json FROM run_snapshots WHERE run_id=?1",
        params![owner.run_id.0],
        |row| row.get(0),
    )?;
    let snapshot: RunSnapshot = serde_json::from_str(&raw).map_err(protocol)?;
    let digest = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&snapshot.tools).map_err(protocol)?)
    );
    if digest != generation
        || snapshot.tool_catalog_digest != generation
        || snapshot
            .tools
            .windows(2)
            .any(|pair| pair[0].name >= pair[1].name)
    {
        return Err(RuntimeError::Protocol(
            "发现许可的 frozen catalog/generation 损坏或过期".into(),
        ));
    }
    Ok(snapshot)
}
fn validate(
    receipt: &ToolDiscoveryReceipt,
    snapshot: &RunSnapshot,
    owner: &ExactOwner,
) -> Result<(), RuntimeError> {
    if receipt.schema_version != 1
        || receipt.owner != *owner
        || receipt.operation_id.is_empty()
        || receipt.operation_id.len() > 512
        || receipt.result.schema_version != 1
        || receipt.result.catalog_generation != snapshot.tool_catalog_digest
        || receipt.query.query.trim().is_empty()
        || receipt.query.query.len() > 2048
        || !(1..=TOOL_SEARCH_LIMIT).contains(&receipt.query.limit.unwrap_or(5))
        || receipt.result.tools.len() > receipt.query.limit.unwrap_or(5)
        || serde_json::to_vec(&receipt.result).map_err(protocol)?.len() > TOOL_SEARCH_BYTES
        || receipt
            .result
            .tools
            .iter()
            .any(|tool| !snapshot.tools.contains(tool))
        || receipt
            .result
            .tools
            .iter()
            .map(|tool| &tool.name)
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != receipt.result.tools.len()
    {
        return Err(RuntimeError::Protocol(
            "发现 receipt 身份/schema/授权集合无效".into(),
        ));
    }
    Ok(())
}
fn receipts_in(db: &Connection, run: &RunId) -> Result<Vec<ToolDiscoveryReceipt>, RuntimeError> {
    let owner = sessions::owner_in(db, run)?;
    sessions::fence_in(db, &owner)?;
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
            let snapshot = catalog_in(db, &owner, &receipt.result.catalog_generation)?;
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
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        let snapshot = catalog_in(&tx, &receipt.owner, &receipt.result.catalog_generation)?;
        validate(receipt, &snapshot, &receipt.owner)?;
        let raw = serde_json::to_string(receipt).map_err(protocol)?;
        let digest = format!("{:x}", Sha256::digest(raw.as_bytes()));
        let existing: Option<(String, String, String)> = tx
            .query_row(
                "SELECT run_id,receipt_json,receipt_digest FROM tool_discoveries WHERE operation_id=?1",
                params![receipt.operation_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        if let Some((run_id, existing, saved_digest)) = existing {
            if format!("{:x}", Sha256::digest(existing.as_bytes())) != saved_digest {
                return Err(RuntimeError::Protocol("发现 receipt 摘要损坏".into()));
            }
            let saved: ToolDiscoveryReceipt = serde_json::from_str(&existing).map_err(protocol)?;
            if run_id != receipt.owner.run_id.0 || saved != *receipt {
                return Err(RuntimeError::Protocol("发现 operation 输入冲突".into()));
            }
            super::views::commit(tx)?;
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
        tx.execute(
            "INSERT INTO tool_discoveries VALUES(?1,?2,?3,?4)",
            params![receipt.operation_id, run.run_id.0, raw, digest],
        )?;
        super::insert_event(
            &tx,
            &run.run_id,
            "tool_discovered",
            &serde_json::json!({"operation_id":receipt.operation_id,"catalog_generation":receipt.result.catalog_generation,"names":receipt.result.tools.iter().map(|tool|&tool.name).collect::<Vec<_>>()}),
        )?;
        super::views::commit(tx)?;
        Ok(receipt.clone())
    }
    fn discovered_tool(
        &self,
        owner: &ExactOwner,
        generation: &str,
        name: &str,
    ) -> Result<bool, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        catalog_in(&tx, owner, generation)?;
        let run = read_run_in(&tx, &owner.run_id.0)?
            .ok_or_else(|| RuntimeError::Protocol("发现缺 run".into()))?;
        let cancelled: i64 = tx.query_row(
            "SELECT count(*) FROM events WHERE run_id=?1 AND event='cancellation_requested'",
            params![owner.run_id.0],
            |row| row.get(0),
        )?;
        if run.status != RunStatus::Running || cancelled != 0 {
            return Err(RuntimeError::Protocol("执行许可 owner 已结束/取消".into()));
        }
        let result = receipts_in(&tx, &owner.run_id)?.iter().any(|receipt| {
            receipt.result.catalog_generation == generation
                && receipt.result.tools.iter().any(|tool| tool.name == name)
        });
        super::views::commit(tx)?;
        Ok(result)
    }
    fn discoveries(&self, run: &RunId) -> Result<Vec<ToolDiscoveryReceipt>, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        let result = receipts_in(&tx, run)?;
        super::views::commit(tx)?;
        Ok(result)
    }
    fn request_cancellation(&self, owner: &ExactOwner) -> Result<(), RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        sessions::fence_in(&tx, owner)?;
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

    fn path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "discovery-{}-{}.sqlite3",
            std::process::id(),
            super::super::now_ms()
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

    #[test]
    fn schema17_discovery_upgrade_backup_transaction_rollback_and_future_rejection() {
        let file = path();
        let (store, receipt) = fixture(&file);
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
            .execute("INSERT INTO schema_migrations VALUES(21,0)", [])
            .unwrap();
        drop(store);
        assert!(RunStore::open(&file).is_err());
        std::fs::remove_file(file).unwrap();
    }
}
