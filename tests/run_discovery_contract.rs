//! 原发现许可的scope、历史读回与预算合同。
use agent_core::*;
use agent_storage::{DiscoveryRepository, RunStore, RuntimeError, SessionLifecycle, SessionQuery};
use rusqlite::{Connection, params};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    root: std::path::PathBuf,
    store: RunStore,
    db: Connection,
    receipt: ToolDiscoveryReceipt,
    snapshot: RunSnapshot,
}
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "ma-discovery-port-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let path = root.join("runtime.sqlite3");
        let store = RunStore::open(&path).unwrap();
        let meta = store
            .create_session(&SessionKey("discovery-port".into()))
            .unwrap();
        let tools = vec![ToolSpec {
            name: "private__lookup".into(),
            description: "私有工具定义".into(),
            parameters: serde_json::json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"],"additionalProperties":false}),
        }];
        let generation = format!("{:x}", Sha256::digest(serde_json::to_vec(&tools).unwrap()));
        let snapshot:RunSnapshot=serde_json::from_value(serde_json::json!({"tools":tools,"cwd":".","permission_mode":"risk_approval","sandbox_requested":"native","sandbox_effective":"native","context_read_only":false,"tool_catalog_digest":generation,"config_generation":0})).unwrap();
        let Admission::New(run) = store
            .admit_run(
                &RunAdmission {
                    workflow_node: None,
                    session_key: meta.key,
                    expected_lifetime: Some(meta.lifetime),
                    request_id: RequestId::Number(1),
                    input: "发现私有工具".into(),
                    mode: AdmissionMode::Queue,
                    plan_execution: None,
                },
                &snapshot,
            )
            .unwrap()
        else {
            panic!("新准入")
        };
        assert!(store.try_start_queued(&run.run_id).unwrap());
        let query = ToolSearchQuery {
            query: "select:private__lookup".into(),
            limit: Some(1),
        };
        let result = ToolSearchIndex::new(snapshot.tools.clone(), generation)
            .unwrap()
            .search(&query)
            .unwrap();
        let receipt = ToolDiscoveryReceipt {
            schema_version: 1,
            operation_id: "discovery-operation".into(),
            owner: store.run_owner(&run.run_id).unwrap(),
            query,
            result,
        };
        let db = Connection::open(path).unwrap();
        db.execute_batch("PRAGMA foreign_keys=ON").unwrap();
        Self {
            root,
            store,
            db,
            receipt,
            snapshot,
        }
    }
    fn foreign(&self) {
        self.db.execute_batch("INSERT INTO organizations VALUES('foreign');INSERT INTO tenants VALUES('foreign','foreign');INSERT INTO projects VALUES('foreign','foreign');INSERT INTO agents VALUES('foreign','foreign','foreign');UPDATE session_scopes SET tenant_id='foreign',project_id='foreign',agent_uid='foreign' WHERE session_id='discovery-port'").unwrap();
    }
    fn clock(&self) -> u64 {
        self.db
            .query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
fn forbidden<T: std::fmt::Debug>(result: &Result<T, RuntimeError>) {
    assert!(
        matches!(
            result,
            Err(RuntimeError::Authorization(AuthorizationError::Forbidden))
        ),
        "必须先在SQL拒绝scope，实际 {result:?}"
    );
}
#[test]
fn discovery_publication_denies_foreign_before_any_permit_or_event() {
    let f = Fixture::new();
    f.foreign();
    let before = f.clock();
    forbidden(&f.store.publish_discovery(&f.receipt));
    assert_eq!(f.clock(), before);
}
#[test]
fn discovered_execution_permission_denies_foreign_before_catalog_and_receipt() {
    let f = Fixture::new();
    f.store.publish_discovery(&f.receipt).unwrap();
    f.foreign();
    let before = f.clock();
    forbidden(&f.store.discovered_tool(
        &f.receipt.owner,
        &f.receipt.result.catalog_generation,
        "private__lookup",
    ));
    assert_eq!(f.clock(), before);
}
#[test]
fn discovery_readback_denies_foreign_before_loading_private_definitions() {
    let f = Fixture::new();
    f.store.publish_discovery(&f.receipt).unwrap();
    f.foreign();
    let before = f.clock();
    forbidden(&f.store.discoveries(&f.receipt.owner.run_id));
    assert_eq!(f.clock(), before);
}
#[test]
fn discovery_cancellation_denies_foreign_without_canonical_cancel_event() {
    let f = Fixture::new();
    f.foreign();
    let before = f.clock();
    forbidden(&f.store.request_cancellation(&f.receipt.owner));
    assert_eq!(f.clock(), before);
}
#[test]
fn retired_discovery_readback_uses_permanent_lifetime_without_current_writer() {
    let f = Fixture::new();
    f.store.publish_discovery(&f.receipt).unwrap();
    f.store
        .finish(&f.receipt.owner.run_id, RunStatus::Completed, None, None)
        .unwrap();
    f.store
        .end_session(
            &f.receipt.owner.session_key,
            &f.receipt.owner.session_lifetime_id,
            false,
        )
        .unwrap();
    let before = f.clock();
    let result = f.store.discoveries(&f.receipt.owner.run_id);
    assert!(
        result.is_ok(),
        "永久归属历史读取不要求当前head/writer，实际 {result:?}"
    );
    assert_eq!(result.unwrap(), vec![f.receipt.clone()]);
    assert_eq!(f.clock(), before);
}
#[test]
fn discovery_saved_frozen_snapshot_has_budget_before_decoding_legal_json() {
    let f = Fixture::new();
    f.store.publish_discovery(&f.receipt).unwrap();
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM run_authorizations", [], |r| r.get(0))
            .unwrap(),
        0
    );
    let mut saved = f.snapshot.clone();
    saved.cwd = "x".repeat(1024 * 1024 + 1);
    f.db.execute(
        "UPDATE run_snapshots SET snapshot_json=?2 WHERE run_id=?1",
        params![
            f.receipt.owner.run_id.0,
            serde_json::to_string(&saved).unwrap()
        ],
    )
    .unwrap();
    let before = f.clock();
    let result = f.store.discovered_tool(
        &f.receipt.owner,
        &f.receipt.result.catalog_generation,
        "private__lookup",
    );
    assert!(
        matches!(
            result,
            Err(RuntimeError::Coordination(CoordinationError::Corrupt))
        ),
        "保存的大JSON应在装入Rust前拒绝，实际 {result:?}"
    );
    assert_eq!(f.clock(), before);
}

use agent_storage_ports::{RunDiscoveryRepository, RunEventRepository};
impl Fixture {
    fn envelope(&self) -> EnterpriseOwnerEnvelope {
        EnterpriseOwnerEnvelope {
            scope: TenantContext::local(),
            owner: self.receipt.owner.clone(),
        }
    }
    fn facts(&self) -> (u64, u64, u64, u64, u64) {
        self.db.query_row("SELECT (SELECT count(*) FROM tool_discoveries),(SELECT count(*) FROM events),(SELECT count(*) FROM event_outbox),(SELECT count(*) FROM event_view_stamps),(SELECT revision FROM snapshot_clock)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).unwrap()
    }
}
#[tokio::test]
async fn native_discovery_all_scope_and_owner_dimensions_fail_before_receipts_or_cancel() {
    let f = Fixture::new();
    let env = f.envelope();
    f.store
        .publish_run_discovery(&env, &f.receipt)
        .await
        .unwrap();
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
        let expected: RepositoryError = if dimension < 4 || dimension == 8 {
            AuthorizationError::Forbidden.into()
        } else {
            AuthorizationError::StaleOwner.into()
        };
        let before = f.facts();
        let mut receipt = f.receipt.clone();
        receipt.owner = bad.owner.clone();
        assert_eq!(
            f.store.publish_run_discovery(&bad, &receipt).await,
            Err(expected),
            "发布维度 {dimension}"
        );
        assert_eq!(
            f.store
                .read_discovered_tool(
                    &bad,
                    &f.receipt.result.catalog_generation,
                    "private__lookup"
                )
                .await,
            Err(expected),
            "许可维度 {dimension}"
        );
        assert_eq!(
            f.store.request_run_cancellation(&bad).await,
            Err(expected),
            "取消维度 {dimension}"
        );
        if dimension < 4 || dimension == 8 {
            assert_eq!(
                f.store
                    .read_run_discoveries(&bad.scope, &bad.owner.run_id)
                    .await,
                Err(expected)
            );
        }
        assert_eq!(f.facts(), before);
    }
    let before = f.facts();
    assert!(
        f.store
            .read_discovered_tool(
                &env,
                &f.receipt.result.catalog_generation,
                "private__lookup"
            )
            .await
            .unwrap()
    );
    assert_eq!(
        f.store
            .read_run_discoveries(&env.scope, &env.owner.run_id)
            .await
            .unwrap(),
        vec![f.receipt.clone()]
    );
    assert_eq!(
        f.store
            .publish_run_discovery(&env, &f.receipt)
            .await
            .unwrap(),
        f.receipt
    );
    assert_eq!(f.facts(), before, "读回和幂等发布不能写clock");
    f.store.request_run_cancellation(&env).await.unwrap();
    let cancelled = f.facts();
    f.store.request_run_cancellation(&env).await.unwrap();
    assert_eq!(f.facts(), cancelled);
    assert_eq!(
        f.store
            .read_discovered_tool(
                &env,
                &f.receipt.result.catalog_generation,
                "private__lookup"
            )
            .await,
        Err(CoordinationError::Conflict.into())
    );
    assert_eq!(
        f.store
            .read_run_discoveries(&env.scope, &env.owner.run_id)
            .await
            .unwrap(),
        vec![f.receipt.clone()]
    );
    assert_eq!(f.facts(), cancelled);
}
#[tokio::test]
async fn native_discovery_and_cancellation_faults_roll_back_original_event_stamp_and_outbox() {
    let f = Fixture::new();
    let env = f.envelope();
    for table in ["events", "event_view_stamps", "event_outbox"] {
        f.db.execute_batch(&format!("CREATE TRIGGER discovery_fault BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT,'发现事务故障'); END")).unwrap();
        let before = f.facts();
        assert_eq!(
            f.store.publish_run_discovery(&env, &f.receipt).await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        assert_eq!(f.facts(), before);
        assert_eq!(
            f.store.request_run_cancellation(&env).await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        assert_eq!(f.facts(), before);
        f.db.execute_batch("DROP TRIGGER discovery_fault").unwrap();
    }
    f.store
        .publish_run_discovery(&env, &f.receipt)
        .await
        .unwrap();
    let events = f
        .store
        .read_run_events(&env.scope, &env.owner.run_id, EventSeq(0), 100)
        .await
        .unwrap();
    assert_eq!(
        events
            .events
            .iter()
            .filter(|e| e.event == "tool_discovered")
            .count(),
        1
    );
}
#[tokio::test]
async fn native_discovery_corrupt_saved_identity_digest_and_sql_budget_fail_without_clock_write() {
    for fault in ["digest", "owner", "operation", "catalog", "bytes", "rows"] {
        let f = Fixture::new();
        let env = f.envelope();
        f.store
            .publish_run_discovery(&env, &f.receipt)
            .await
            .unwrap();
        if fault == "bytes" {
            let raw =
                serde_json::to_string(&serde_json::json!({"padding":"x".repeat(1024*1024+1)}))
                    .unwrap();
            f.db.execute(
                "UPDATE tool_discoveries SET receipt_json=?1,receipt_digest=?2",
                params![raw, format!("{:x}", Sha256::digest(raw.as_bytes()))],
            )
            .unwrap();
        } else if fault == "rows" {
            for index in 0..128 {
                let mut extra = f.receipt.clone();
                extra.operation_id = format!("extra-{index}");
                let raw = serde_json::to_string(&extra).unwrap();
                f.db.execute(
                    "INSERT INTO tool_discoveries VALUES(?1,?2,?3,?4)",
                    params![
                        extra.operation_id,
                        env.owner.run_id.0,
                        raw,
                        format!("{:x}", Sha256::digest(raw.as_bytes()))
                    ],
                )
                .unwrap();
            }
        } else {
            let mut saved = f.receipt.clone();
            match fault {
                "owner" => saved.owner.session_lifetime_id.0 = "wrong".into(),
                "operation" => saved.operation_id = "wrong".into(),
                "catalog" => saved.result.catalog_generation = "0".repeat(64),
                _ => {}
            }
            let raw = serde_json::to_string(&saved).unwrap();
            let digest = if fault == "digest" {
                "0".repeat(64)
            } else {
                format!("{:x}", Sha256::digest(raw.as_bytes()))
            };
            f.db.execute(
                "UPDATE tool_discoveries SET receipt_json=?1,receipt_digest=?2",
                params![raw, digest],
            )
            .unwrap();
        }
        let before = f.facts();
        assert_eq!(
            f.store
                .read_run_discoveries(&env.scope, &env.owner.run_id)
                .await,
            Err(CoordinationError::Corrupt.into()),
            "历史 {fault}"
        );
        assert_eq!(
            f.store
                .read_discovered_tool(
                    &env,
                    &f.receipt.result.catalog_generation,
                    "private__lookup"
                )
                .await,
            Err(CoordinationError::Corrupt.into()),
            "许可 {fault}"
        );
        assert_eq!(f.facts(), before);
        let mut foreign = env.scope.clone();
        foreign.organization_id = "foreign".to_owned().try_into().unwrap();
        assert_eq!(
            f.store
                .read_run_discoveries(&foreign, &env.owner.run_id)
                .await,
            Err(AuthorizationError::Forbidden.into()),
            "外域应先于损坏 {fault}"
        );
    }
}
#[tokio::test]
async fn two_real_connections_publish_one_receipt_and_cancel_event_under_concurrency() {
    let f = Fixture::new();
    let env = f.envelope();
    let second = RunStore::open(&f.root.join("runtime.sqlite3")).unwrap();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    let (a, b) = tokio::join!(
        async {
            barrier.wait().await;
            f.store.publish_run_discovery(&env, &f.receipt).await
        },
        async {
            barrier.wait().await;
            second.publish_run_discovery(&env, &f.receipt).await
        }
    );
    assert_eq!(a.unwrap(), f.receipt);
    assert_eq!(b.unwrap(), f.receipt);
    let (a, b) = tokio::join!(
        async {
            barrier.wait().await;
            f.store.request_run_cancellation(&env).await
        },
        async {
            barrier.wait().await;
            second.request_run_cancellation(&env).await
        }
    );
    a.unwrap();
    b.unwrap();
    let events = f
        .store
        .read_run_events(&env.scope, &env.owner.run_id, EventSeq(0), 100)
        .await
        .unwrap();
    for name in ["tool_discovered", "cancellation_requested"] {
        let matches = events
            .events
            .iter()
            .filter(|e| e.event == name)
            .collect::<Vec<_>>();
        assert_eq!(matches.len(), 1);
        let seq = matches[0].seq.0;
        for table in ["event_view_stamps", "event_outbox"] {
            assert_eq!(
                f.db.query_row::<u64, _, _>(
                    &format!(
                        "SELECT count(*) FROM {table} WHERE run_id=?1 AND {}=?2",
                        if table == "event_view_stamps" {
                            "seq"
                        } else {
                            "event_seq"
                        }
                    ),
                    params![env.owner.run_id.0, seq],
                    |r| r.get(0)
                )
                .unwrap(),
                1
            );
        }
    }
}

impl Fixture {
    async fn native(&mut self) -> EnterpriseOwnerEnvelope {
        use agent_storage_ports::{IdentityReadRepository, RunExecutionRepository};
        self.store
            .finish(&self.receipt.owner.run_id, RunStatus::Completed, None, None)
            .unwrap();
        self.store = self.store.clone().enable_session_writer().unwrap();
        let identity = self
            .store
            .read_identity(
                &TenantContext::local(),
                &"local-subject".to_owned().try_into().unwrap(),
            )
            .await
            .unwrap();
        let authorization = AuthorizationRequest {
            context: RequestContext {
                scope: identity.scope.clone(),
                principal: identity.principal,
                request_id: RequestId::Number(2),
                trace: TraceContext {
                    trace_id: "discovery-native".into(),
                },
            },
            expected_policy: identity.policy.version,
            action: Action::SessionPrompt,
            attributes: AuthorizationAttributes {
                workspace: self.snapshot.cwd.clone(),
                entry_channel: HookChannel::DaemonRpc,
                tool_effect: None,
                timestamp_ms: 0,
                network: NetworkBoundary::None,
            },
        };
        let (Admission::New(_), owner) = self
            .store
            .admit_run_authorized(
                &RunAdmission {
                    workflow_node: None,
                    session_key: self.receipt.owner.session_key.clone(),
                    expected_lifetime: Some(self.receipt.owner.session_lifetime_id.clone()),
                    request_id: RequestId::Number(2),
                    input: "原冻结授权发现".into(),
                    mode: AdmissionMode::Queue,
                    plan_execution: None,
                },
                &self.snapshot,
                &authorization,
                std::sync::Arc::new(agent_policy::PolicyEngine),
            )
            .await
            .unwrap()
        else {
            panic!("新准入")
        };
        let env = EnterpriseOwnerEnvelope {
            scope: identity.scope,
            owner,
        };
        assert!(self.store.start_queued_run(&env).await.unwrap());
        self.receipt.owner = env.owner.clone();
        env
    }
}
#[tokio::test]
async fn native_discovery_revocation_and_writer_expiry_deny_execution_but_preserve_scoped_history()
{
    for fault in ["revoked", "expired"] {
        let mut f = Fixture::new();
        let env = f.native().await;
        f.store
            .publish_run_discovery(&env, &f.receipt)
            .await
            .unwrap();
        let expected: RepositoryError = if fault == "revoked" {
            f.db.execute_batch(
                "UPDATE principals SET revocation_epoch=1 WHERE subject_id='local-subject'",
            )
            .unwrap();
            AuthorizationError::Revoked.into()
        } else {
            f.db.execute_batch("UPDATE session_writer_leases SET expires_at_ms=0")
                .unwrap();
            CoordinationError::StaleLease.into()
        };
        let epoch: u64 =
            f.db.query_row("SELECT epoch FROM session_writer_leases", [], |r| r.get(0))
                .unwrap();
        let before = f.facts();
        let mut later = f.receipt.clone();
        later.operation_id = "late-operation".into();
        assert_eq!(
            f.store.publish_run_discovery(&env, &later).await,
            Err(expected)
        );
        assert_eq!(
            f.store
                .read_discovered_tool(
                    &env,
                    &f.receipt.result.catalog_generation,
                    "private__lookup"
                )
                .await,
            Err(expected)
        );
        assert_eq!(f.store.request_run_cancellation(&env).await, Err(expected));
        assert_eq!(
            f.store
                .read_run_discoveries(&env.scope, &env.owner.run_id)
                .await
                .unwrap(),
            vec![f.receipt.clone()]
        );
        assert_eq!(f.facts(), before);
        assert_eq!(
            f.db.query_row::<u64, _, _>("SELECT epoch FROM session_writer_leases", [], |r| r
                .get(0))
                .unwrap(),
            epoch
        );
    }
}
#[tokio::test]
async fn native_discovery_restarts_after_lifetime_retirement_without_reacquiring_writer() {
    let mut f = Fixture::new();
    let env = f.native().await;
    f.store
        .publish_run_discovery(&env, &f.receipt)
        .await
        .unwrap();
    use agent_storage_ports::RunExecutionRepository;
    f.store
        .commit_run_turn(
            &env,
            &TurnCommit {
                owner: env.owner.clone(),
                status: RunStatus::Completed,
                content: None,
                error: None,
            },
        )
        .await
        .unwrap();
    f.store
        .end_session(
            &env.owner.session_key,
            &env.owner.session_lifetime_id,
            false,
        )
        .unwrap();
    let reopened = RunStore::open(&f.root.join("runtime.sqlite3"))
        .unwrap()
        .enable_session_writer()
        .unwrap();
    let before = f.facts();
    let leases: u64 =
        f.db.query_row("SELECT count(*) FROM session_writer_leases", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(
        reopened
            .read_run_discoveries(&env.scope, &env.owner.run_id)
            .await
            .unwrap(),
        vec![f.receipt.clone()]
    );
    assert_eq!(f.facts(), before);
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT count(*) FROM session_writer_leases", [], |r| r
            .get(0))
            .unwrap(),
        leases
    );
    assert_eq!(
        reopened
            .read_discovered_tool(
                &env,
                &f.receipt.result.catalog_generation,
                "private__lookup"
            )
            .await,
        Err(AuthorizationError::StaleOwner.into())
    );
}
#[tokio::test]
async fn native_discovery_cumulative_bytes_reject_before_insert_and_keep_previous_receipts_readable()
 {
    let f = Fixture::new();
    let env = f.envelope();
    let mut snapshot = f.snapshot.clone();
    snapshot.tools[0].description = "工具".repeat(2000);
    let generation = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&snapshot.tools).unwrap())
    );
    snapshot.tool_catalog_digest = generation.clone();
    f.db.execute(
        "UPDATE run_snapshots SET snapshot_json=?1 WHERE run_id=?2",
        params![
            serde_json::to_string(&snapshot).unwrap(),
            env.owner.run_id.0
        ],
    )
    .unwrap();
    let mut receipt = f.receipt.clone();
    receipt.result = ToolSearchIndex::new(snapshot.tools, generation.clone())
        .unwrap()
        .search(&receipt.query)
        .unwrap();
    assert_eq!(receipt.result.tools.len(), 1);
    let mut saved = Vec::new();
    for index in 0..128 {
        receipt.operation_id = format!("budget-{index}");
        let before = f.facts();
        match f.store.publish_run_discovery(&env, &receipt).await {
            Ok(value) => saved.push(value),
            Err(error) => {
                assert_eq!(error, CoordinationError::InvalidRequest.into());
                assert_eq!(f.facts(), before);
                assert!(!saved.is_empty() && saved.len() < 128);
                assert_eq!(
                    f.store
                        .read_run_discoveries(&env.scope, &env.owner.run_id)
                        .await
                        .unwrap(),
                    saved
                );
                assert!(
                    f.store
                        .read_discovered_tool(&env, &generation, "private__lookup")
                        .await
                        .unwrap()
                );
                return;
            }
        }
    }
    panic!("累计超过1MiB应在INSERT前拒绝");
}

#[tokio::test]
async fn native_discovery_operation_collision_does_not_load_foreign_corrupt_receipt() {
    let f = Fixture::new();
    let env = f.envelope();
    f.store
        .publish_run_discovery(&env, &f.receipt)
        .await
        .unwrap();
    f.foreign();
    f.db.execute_batch(
        "UPDATE tool_discoveries SET receipt_json='{\"foreign\":true}',receipt_digest='bad'",
    )
    .unwrap();
    let meta = f
        .store
        .create_session(&SessionKey("second-local".into()))
        .unwrap();
    let Admission::New(run) = f
        .store
        .admit_run(
            &RunAdmission {
                workflow_node: None,
                session_key: meta.key,
                expected_lifetime: Some(meta.lifetime),
                request_id: RequestId::Number(2),
                input: "本地新许可".into(),
                mode: AdmissionMode::Queue,
                plan_execution: None,
            },
            &f.snapshot,
        )
        .unwrap()
    else {
        panic!("新准入")
    };
    f.store.try_start_queued(&run.run_id).unwrap();
    let local = EnterpriseOwnerEnvelope {
        scope: TenantContext::local(),
        owner: f.store.run_owner(&run.run_id).unwrap(),
    };
    let mut receipt = f.receipt.clone();
    receipt.owner = local.owner.clone();
    let before = f.facts();
    assert_eq!(
        f.store.publish_run_discovery(&local, &receipt).await,
        Err(CoordinationError::Conflict.into())
    );
    assert_eq!(f.facts(), before);
    assert!(
        f.store
            .read_run_discoveries(&local.scope, &local.owner.run_id)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        f.store
            .read_run_discoveries(&local.scope, &env.owner.run_id)
            .await,
        Err(AuthorizationError::Forbidden.into())
    );
    receipt.operation_id = "second-local-operation".into();
    assert_eq!(
        f.store
            .publish_run_discovery(&local, &receipt)
            .await
            .unwrap(),
        receipt
    );
}
