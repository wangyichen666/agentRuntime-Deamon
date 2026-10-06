//! 发送前捕获与只读重建材料，读取不启动执行对象。
use crate::{RunStore, RuntimeError, sessions};
use agent_core::*;
use rusqlite::{OptionalExtension, params};
use sha2::{Digest, Sha256};

pub type RequestContextSource = (SessionSnapshot, SnapshotRevision, Option<(u64, u64)>);

pub trait RequestRepository: Send + Sync {
    fn request_context_source(
        &self,
        owner: &ExactOwner,
        route: &str,
        identity: &str,
    ) -> Result<RequestContextSource, RuntimeError>;
    fn record_provider_request(
        &self,
        capture: &ProviderRequestCapture,
        envelope: &ContextEnvelope,
    ) -> Result<(), RuntimeError>;
    fn provider_request(
        &self,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        run: Option<&RunId>,
        capture: Option<&str>,
    ) -> Result<(ProviderRequestCapture, RunSnapshot), RuntimeError>;
}

impl RequestRepository for RunStore {
    fn request_context_source(
        &self,
        owner: &ExactOwner,
        route: &str,
        identity: &str,
    ) -> Result<RequestContextSource, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        sessions::fence_in(&tx, owner)?;
        let snapshot = sessions::snapshot_in(&tx, &owner.session_key)?;
        // 同事务验证投影，损坏不降级为空历史。
        crate::context_projection::projection_in(&tx, &snapshot)?;
        let revision = SnapshotRevision(tx.query_row(
            "SELECT revision FROM snapshot_clock WHERE id=1",
            [],
            |row| row.get(0),
        )?);
        let calibration = crate::context_projection::context_anchor_in(
            &tx,
            owner,
            snapshot.projection_generation,
            route,
            identity,
        )?;
        tx.commit()?;
        Ok((snapshot, revision, calibration))
    }
    fn record_provider_request(
        &self,
        capture: &ProviderRequestCapture,
        envelope: &ContextEnvelope,
    ) -> Result<(), RuntimeError> {
        if capture.schema_version != 1
            || capture.round == 0
            || capture.capture_id.is_empty()
            || capture.capture_id.len() > 256
            || capture.owner.session_lifetime_id != capture.input.source.lifetime
            || capture.policy_fingerprint.len() != 64
            || capture.request_digest.len() != 64
            || envelope.source != capture.input.source
        {
            return Err(RuntimeError::Protocol("请求 capture 身份无效".into()));
        }
        let raw = serde_json::to_string(capture)
            .map_err(|error| RuntimeError::Protocol(error.to_string()))?;
        if raw.len() > 16 * 1024 * 1024 {
            return Err(RuntimeError::Protocol("请求 capture 超出预算".into()));
        }
        let digest = format!("{:x}", Sha256::digest(raw.as_bytes()));
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        sessions::fence_in(&tx, &capture.owner)?;
        crate::context_projection::source_check(
            &tx,
            &capture.owner.session_key,
            &capture.input.source,
        )?;
        let snapshot: RunSnapshot = serde_json::from_str(&tx.query_row(
            "SELECT snapshot_json FROM run_snapshots WHERE run_id=?1",
            params![capture.owner.run_id.0],
            |row| row.get::<_, String>(0),
        )?)
        .map_err(|error| RuntimeError::Protocol(error.to_string()))?;
        if capture.policy_fingerprint
            != format!(
                "{:x}",
                Sha256::digest(
                    serde_json::to_vec(&snapshot)
                        .map_err(|error| RuntimeError::Protocol(error.to_string()))?
                )
            )
            || snapshot.tools != capture.input.catalog
            || snapshot.tool_catalog_digest
                != format!(
                    "{:x}",
                    Sha256::digest(
                        serde_json::to_vec(&capture.input.catalog)
                            .map_err(|error| RuntimeError::Protocol(error.to_string()))?
                    )
                )
            || snapshot.context_token_budget as u64 != capture.input.budget
            || snapshot.context_policy_fingerprint.as_deref()
                != Some(&capture.input.source.policy_fingerprint)
        {
            return Err(RuntimeError::Protocol("请求 catalog/policy 未冻结".into()));
        }
        let previous: Option<String> = tx
            .query_row(
                "SELECT capture_digest FROM provider_requests WHERE run_id=?1 AND capture_id=?2",
                params![capture.owner.run_id.0, capture.capture_id],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(previous) = previous {
            if previous != digest {
                return Err(RuntimeError::Protocol("请求 capture 幂等冲突".into()));
            }
            tx.commit()?;
            return Ok(());
        }
        let count: u64 = tx.query_row(
            "SELECT count(*) FROM provider_requests WHERE run_id=?1",
            params![capture.owner.run_id.0],
            |row| row.get(0),
        )?;
        if count >= 1024 {
            return Err(RuntimeError::Protocol("请求 capture 数量超过预算".into()));
        }
        tx.execute(
            "INSERT INTO provider_requests VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                capture.owner.run_id.0,
                capture.capture_id,
                capture.round,
                capture.candidate_index,
                raw,
                digest
            ],
        )?;
        tx.execute("INSERT INTO context_ledgers VALUES(?1,?2,?3) ON CONFLICT(run_id,round) DO UPDATE SET envelope_json=excluded.envelope_json",params![capture.owner.run_id.0,capture.round,serde_json::to_string(envelope).map_err(|error|RuntimeError::Protocol(error.to_string()))?])?;
        crate::views::commit(tx)
    }
    fn provider_request(
        &self,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        run: Option<&RunId>,
        capture: Option<&str>,
    ) -> Result<(ProviderRequestCapture, RunSnapshot), RuntimeError> {
        if key.0.is_empty() || capture.is_some_and(|id| id.is_empty() || id.len() > 256) {
            return Err(RuntimeError::Protocol("请求读取身份无效".into()));
        }
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        let metadata = sessions::metadata_in(&tx, key)?
            .filter(|metadata| !metadata.deleted && metadata.lifetime == *lifetime)
            .ok_or_else(|| RuntimeError::Protocol("请求读取 stale lifetime".into()))?;
        let row:Option<(String,String,String)>=tx.query_row("SELECT p.capture_json,p.capture_digest,s.snapshot_json FROM provider_requests p JOIN runs r ON r.id=p.run_id JOIN run_snapshots s ON s.run_id=p.run_id WHERE r.session_id=?1 AND r.lifetime=?2 AND (?3 IS NULL OR p.run_id=?3) AND (?4 IS NULL OR p.capture_id=?4) ORDER BY p.rowid DESC LIMIT 1",params![key.0,metadata.lifetime.0,run.map(|run|&run.0),capture],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?))).optional()?;
        let (raw, digest, snapshot) = row.ok_or_else(|| {
            RuntimeError::Protocol(
                "provider request unavailable: 无发送前 capture，不能重建空请求".into(),
            )
        })?;
        if raw.len() > 16 * 1024 * 1024 || format!("{:x}", Sha256::digest(raw.as_bytes())) != digest
        {
            return Err(RuntimeError::Protocol("请求 capture 摘要损坏".into()));
        }
        let captured: ProviderRequestCapture = serde_json::from_str(&raw)
            .map_err(|error| RuntimeError::Protocol(error.to_string()))?;
        let frozen: RunSnapshot = serde_json::from_str(&snapshot)
            .map_err(|error| RuntimeError::Protocol(error.to_string()))?;
        if captured.schema_version != 1
            || sessions::owner_in(&tx, &captured.owner.run_id)? != captured.owner
            || captured.owner.session_key != *key
            || captured.owner.session_lifetime_id != *lifetime
            || captured.input.source.lifetime != *lifetime
            || frozen.tools != captured.input.catalog
            || frozen.tool_catalog_digest
                != format!(
                    "{:x}",
                    Sha256::digest(
                        serde_json::to_vec(&captured.input.catalog)
                            .map_err(|error| RuntimeError::Protocol(error.to_string()))?
                    )
                )
            || frozen.context_token_budget as u64 != captured.input.budget
            || captured.policy_fingerprint
                != format!(
                    "{:x}",
                    Sha256::digest(
                        serde_json::to_vec(&frozen)
                            .map_err(|error| RuntimeError::Protocol(error.to_string()))?
                    )
                )
            || frozen.context_policy_fingerprint.as_deref()
                != Some(&captured.input.source.policy_fingerprint)
            || run.is_some_and(|run| run != &captured.owner.run_id)
            || capture.is_some_and(|id| id != captured.capture_id)
        {
            return Err(RuntimeError::Protocol(
                "请求 capture owner/catalog/policy 损坏".into(),
            ));
        }
        let snapshot = sessions::snapshot_in(&tx, key)?;
        crate::context_projection::projection_in(&tx, &snapshot)?;
        let source_end = usize::try_from(captured.input.source.source_end.0)
            .map_err(|error| RuntimeError::Protocol(error.to_string()))?;
        let prefix = snapshot
            .messages
            .get(..source_end)
            .ok_or_else(|| RuntimeError::Protocol("请求 source revision 越界".into()))?;
        let expected = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(prefix)
                    .map_err(|error| RuntimeError::Protocol(error.to_string()))?
            )
        );
        if captured.input.source.generation > snapshot.projection_generation
            || expected != captured.input.source.prefix_digest
        {
            return Err(RuntimeError::Protocol("请求 source digest 损坏".into()));
        }
        tx.commit()?;
        Ok((captured, frozen))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SessionLifecycle, SessionQuery};
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    fn path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "request-capture-{}-{}.sqlite3",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }
    fn fixture(path: &std::path::Path) -> (RunStore, ProviderRequestCapture, ContextEnvelope) {
        let store = RunStore::open(path).unwrap();
        let metadata = store.create_session(&SessionKey("request".into())).unwrap();
        let frozen:RunSnapshot=serde_json::from_value(serde_json::json!({"tools":[],"route":null,"cwd":".","permission_mode":"risk_approval","sandbox_requested":"native","sandbox_effective":"native","context_read_only":false,"max_tool_calls":8,"config_generation":0,"tool_catalog_digest":format!("{:x}",Sha256::digest(b"[]")),"context_policy_fingerprint":"4096:12:60:85","context_token_budget":4096})).unwrap();
        let Admission::New(run) = store
            .admit_run(
                &RunAdmission {
                    session_key: metadata.key,
                    expected_lifetime: Some(metadata.lifetime),
                    request_id: RequestId::Number(1),
                    input: "请求".into(),
                    mode: AdmissionMode::Queue,
                    plan_execution: None,
                },
                &frozen,
            )
            .unwrap()
        else {
            panic!("new")
        };
        store.try_start_queued(&run.run_id).unwrap();
        let owner = store.run_owner(&run.run_id).unwrap();
        let (snapshot, revision, calibration) = store
            .request_context_source(&owner, "primary", "identity")
            .unwrap();
        let source = ContextSource {
            lifetime: snapshot.lifetime,
            source_start: TranscriptSeq(0),
            source_end: snapshot.revision,
            prefix_digest: format!(
                "{:x}",
                Sha256::digest(serde_json::to_vec(&snapshot.messages).unwrap())
            ),
            generation: snapshot.projection_generation,
            policy_fingerprint: "4096:12:60:85".into(),
            pressure_route: "primary".into(),
            summary_route: "primary".into(),
        };
        let capture = ProviderRequestCapture {
            schema_version: 1,
            capture_id: "one".into(),
            owner,
            round: 1,
            candidate_index: 0,
            snapshot_revision: revision,
            input: ProviderRequestInput {
                purpose: ProviderRequestPurpose::Model,
                source: source.clone(),
                messages: vec![Message::text(Role::User, "材料")],
                tools: vec![],
                catalog: vec![],
                route: "primary".into(),
                provider_identity: "identity".into(),
                images: false,
                tool_calls: true,
                budget: 4096,
                calibration,
            },
            policy_fingerprint: format!(
                "{:x}",
                Sha256::digest(serde_json::to_vec(&frozen).unwrap())
            ),
            request_digest: "a".repeat(64),
        };
        let envelope = ContextEnvelope {
            source,
            route: "primary".into(),
            provider_identity: "identity".into(),
            tool_catalog_digest: format!("{:x}", Sha256::digest(b"[]")),
            stable_tokens: 8,
            history_tokens: 0,
            retrieved_tokens: 0,
            overlay_tokens: 8,
            calibrated_input_tokens: 16,
            output_reserve: 409,
            budget: 4096,
        };
        (store, capture, envelope)
    }
    #[test]
    fn captures_are_atomic_idempotent_exact_and_readback_is_zero_write_on_both_backends() {
        let file = path();
        for target in [std::path::Path::new(":memory:"), file.as_path()] {
            let (store, capture, envelope) = fixture(target);
            let owner = &capture.owner;
            assert!(
                store
                    .provider_request(&owner.session_key, &owner.session_lifetime_id, None, None)
                    .is_err()
            );
            store.record_provider_request(&capture, &envelope).unwrap();
            store.record_provider_request(&capture, &envelope).unwrap();
            let db = store.lock_connection().unwrap();
            let before = db.total_changes();
            let clock: u64 = db
                .query_row(
                    "SELECT revision FROM snapshot_clock WHERE id=1",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            drop(db);
            for _ in 0..3 {
                let (read, _) = store
                    .provider_request(
                        &owner.session_key,
                        &owner.session_lifetime_id,
                        Some(&owner.run_id),
                        Some("one"),
                    )
                    .unwrap();
                assert_eq!(
                    serde_json::to_value(read).unwrap(),
                    serde_json::to_value(&capture).unwrap()
                );
            }
            let db = store.lock_connection().unwrap();
            assert_eq!(db.total_changes(), before);
            assert_eq!(
                clock,
                db.query_row::<u64, _, _>(
                    "SELECT revision FROM snapshot_clock WHERE id=1",
                    [],
                    |row| row.get(0)
                )
                .unwrap()
            );
            drop(db);
            let mut different = capture.clone();
            different.input.messages[0].content = Some("另一输入".into());
            assert!(
                store
                    .record_provider_request(&different, &envelope)
                    .is_err()
            );
            different = capture.clone();
            different.owner.run_generation = RunGeneration(different.owner.run_generation.0 + 1);
            assert!(
                store
                    .record_provider_request(&different, &envelope)
                    .is_err()
            );
            different = capture.clone();
            different.input.catalog.push(ToolSpec {
                name: "forbidden".into(),
                description: "".into(),
                parameters: serde_json::json!({}),
            });
            assert!(
                store
                    .record_provider_request(&different, &envelope)
                    .is_err()
            );
            store.lock_connection().unwrap().execute_batch("CREATE TRIGGER capture_fault BEFORE INSERT ON context_ledgers BEGIN SELECT RAISE(ABORT,'ledger fault'); END").unwrap();
            different = capture.clone();
            different.capture_id = "rollback".into();
            different.round = 2;
            assert!(
                store
                    .record_provider_request(&different, &envelope)
                    .is_err()
            );
            assert!(
                store
                    .provider_request(
                        &owner.session_key,
                        &owner.session_lifetime_id,
                        None,
                        Some("rollback")
                    )
                    .is_err()
            );
            store
                .lock_connection()
                .unwrap()
                .execute(
                    "UPDATE provider_requests SET capture_json='{}' WHERE capture_id='one'",
                    [],
                )
                .unwrap();
            assert!(
                store
                    .provider_request(&owner.session_key, &owner.session_lifetime_id, None, None)
                    .is_err()
            );
        }
        std::fs::remove_file(file).unwrap();
    }
    #[test]
    fn schema19_capture_migration_keeps_legacy_unavailable_and_rolls_back_on_fault() {
        let file = path();
        let (store, capture, _) = fixture(&file);
        let owner = capture.owner;
        store.lock_connection().unwrap().execute_batch("DROP TABLE provider_requests; DELETE FROM schema_migrations WHERE version>=20; CREATE TRIGGER capture_migration_fault BEFORE INSERT ON schema_migrations WHEN NEW.version=20 BEGIN SELECT RAISE(ABORT,'capture migration fault'); END").unwrap();
        drop(store);
        assert!(RunStore::open(&file).is_err());
        let db = rusqlite::Connection::open(&file).unwrap();
        assert_eq!(
            db.query_row::<u64, _, _>("SELECT max(version) FROM schema_migrations", [], |row| row
                .get(0))
                .unwrap(),
            19
        );
        assert_eq!(
            db.query_row::<u64, _, _>(
                "SELECT count(*) FROM sqlite_master WHERE name='provider_requests'",
                [],
                |row| row.get(0)
            )
            .unwrap(),
            0
        );
        assert!(file.with_extension("v19.backup.sqlite3").exists());
        assert!(file.with_extension("v19.backup.verified.json").exists());
        db.execute_batch("DROP TRIGGER capture_migration_fault")
            .unwrap();
        drop(db);
        let store = RunStore::open(&file).unwrap();
        assert_eq!(store.run_owner(&owner.run_id).unwrap(), owner);
        assert!(
            store
                .provider_request(&owner.session_key, &owner.session_lifetime_id, None, None)
                .is_err()
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
