//! 原工具 mutation 的业务失败合同；初始 RED 直接调用原生产 helper。
use agent_core::*;
use agent_storage::{RunStore, RuntimeError, SessionQuery};
use agent_storage_ports::RunToolRepository;
use rusqlite::Connection;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    root: PathBuf,
    store: RunStore,
    db: Connection,
    envelope: EnterpriseOwnerEnvelope,
}
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "ma-tool-port-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let path = root.join("runtime.sqlite3");
        let store = RunStore::open(&path).unwrap();
        let Admission::New(run) = store
            .admit(
                SessionKey("tool-session".into()),
                RequestId::Number(1),
                "原工具事实",
            )
            .unwrap()
        else {
            panic!("new")
        };
        store.try_start_queued(&run.run_id).unwrap();
        let envelope = EnterpriseOwnerEnvelope {
            scope: TenantContext::local(),
            owner: store.run_owner(&run.run_id).unwrap(),
        };
        Self {
            root,
            store,
            db: Connection::open(path).unwrap(),
            envelope,
        }
    }
    fn prepare(&self) {
        let call = ToolCall {
            id: "call-1".into(),
            name: "side_effect".into(),
            arguments: serde_json::json!({}),
        };
        self.store
            .prepare_owned_tool_batch(
                &self.envelope.owner,
                1,
                &[(call, "external_side_effect".into(), "digest".into(), false)],
                &[ToolDescriptor::external()],
            )
            .unwrap();
    }
    fn clock(&self) -> u64 {
        self.db
            .query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap()
    }
    fn foreign(&self) {
        self.db.execute_batch("INSERT INTO organizations VALUES('foreign');INSERT INTO tenants VALUES('foreign','foreign');INSERT INTO projects VALUES('foreign','foreign');INSERT INTO agents VALUES('foreign','foreign','foreign');UPDATE session_scopes SET tenant_id='foreign',project_id='foreign',agent_uid='foreign' WHERE session_id='tool-session';").unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
async fn start(f: &Fixture) -> Result<(), RuntimeError> {
    f.store
        .start_run_tool(&f.envelope, 1, "call-1")
        .await
        .map_err(Into::into)
}
async fn finish_batch(f: &Fixture, round: usize) -> Result<(), RuntimeError> {
    f.store
        .finish_run_tool_batch(&f.envelope, round)
        .await
        .map_err(Into::into)
}
#[tokio::test]
async fn tool_started_cannot_adopt_later_generation_before_external_dispatch() {
    let f = Fixture::new();
    f.prepare();
    f.db.execute("UPDATE runs SET generation=generation+1", [])
        .unwrap();
    let before = f.clock();
    assert!(
        matches!(
            start(&f).await,
            Err(RuntimeError::Authorization(AuthorizationError::StaleOwner))
        ),
        "旧caller不能重读后来owner启动副作用"
    );
    assert_eq!(f.clock(), before);
    assert_eq!(
        f.db.query_row::<String, _, _>("SELECT status FROM tool_executions", [], |r| r.get(0))
            .unwrap(),
        "prepared"
    );
}
#[tokio::test]
async fn tool_started_cannot_write_foreign_legacy_run_without_frozen_authorization() {
    let f = Fixture::new();
    f.prepare();
    f.foreign();
    let before = f.clock();
    assert!(
        matches!(
            start(&f).await,
            Err(RuntimeError::Authorization(AuthorizationError::Forbidden))
        ),
        "无授权legacy run也需原scope核验"
    );
    assert_eq!(f.clock(), before);
}
#[tokio::test]
async fn tool_batch_completion_cannot_manufacture_missing_batch_fact() {
    let f = Fixture::new();
    let before = f.clock();
    assert!(
        matches!(
            finish_batch(&f, 99).await,
            Err(RuntimeError::Coordination(CoordinationError::Conflict))
        ),
        "不存在的batch不能产生完成事件"
    );
    assert_eq!(f.clock(), before);
}
#[tokio::test]
async fn tool_batch_completion_cannot_publish_into_foreign_scope() {
    let f = Fixture::new();
    f.foreign();
    let before = f.clock();
    assert!(
        matches!(
            finish_batch(&f, 99).await,
            Err(RuntimeError::Authorization(AuthorizationError::Forbidden))
        ),
        "scope检查先于missingbatch结果"
    );
    assert_eq!(f.clock(), before);
}

fn completion(content: &str) -> ToolArtifactCompletion {
    use sha2::{Digest, Sha256};
    ToolArtifactCompletion {
        round: 1,
        call_id: "call-1".into(),
        outcome: "success".into(),
        receipt: serde_json::json!({"success":true,"output":content,"output_sha256":format!("{:x}",Sha256::digest(content.as_bytes())),"output_bytes":content.len()}),
    }
}
#[tokio::test]
async fn native_tool_methods_reject_all_scope_and_owner_dimensions_before_any_artifact_or_batch_write()
 {
    let f = Fixture::new();
    f.prepare();
    let before = f.clock();
    let messages = vec![Message::text(Role::User, "invalid exchange")];
    let prepared = [(
        ToolCall {
            id: "call-1".into(),
            name: "side_effect".into(),
            arguments: serde_json::json!({}),
        },
        "external_side_effect".into(),
        {
            use sha2::{Digest, Sha256};
            format!("{:x}", Sha256::digest(b"{}"))
        },
        false,
    )];
    for dimension in 0..8 {
        let mut e = f.envelope.clone();
        match dimension {
            0 => e.scope.organization_id = "foreign".to_owned().try_into().unwrap(),
            1 => e.scope.tenant_id = "foreign".to_owned().try_into().unwrap(),
            2 => e.scope.project_id = "foreign".to_owned().try_into().unwrap(),
            3 => e.scope.agent_uid = "foreign".to_owned().try_into().unwrap(),
            4 => e.owner.session_key.0.push('x'),
            5 => e.owner.session_lifetime_id.0.push('x'),
            6 => e.owner.run_generation.0 += 1,
            _ => e.owner.turn_id.0.push('x'),
        }
        let expected: RepositoryError = if dimension < 4 {
            AuthorizationError::Forbidden.into()
        } else {
            AuthorizationError::StaleOwner.into()
        };
        for result in [
            f.store
                .prepare_run_tool_batch(&e, 1, &prepared, &[ToolDescriptor::external()])
                .await,
            f.store.start_run_tool(&e, 1, "call-1").await,
            f.store.close_run_tool_exchange(&e, 1, &messages).await,
            f.store.finish_run_tool_batch(&e, 1).await,
            f.store.read_tool_artifact_binding(&e).await.map(|_| ()),
            f.store
                .finish_file_tool_output(&e, &completion("result"), "result".into())
                .await
                .map(|_| ()),
        ] {
            assert_eq!(result, Err(expected), "维度 {dimension}");
        }
    }
    assert_eq!(f.clock(), before);
    assert_eq!(
        f.db.query_row::<String, _, _>("SELECT status FROM tool_executions", [], |r| r.get(0))
            .unwrap(),
        "prepared"
    );
    assert!(
        !f.root.join("runtime.artifacts").exists(),
        "scope拒绝先于创建文件stage目录"
    );
}
#[tokio::test]
async fn native_tool_preparation_validates_descriptor_effect_digest_replay_and_duplicate_identity()
{
    use sha2::{Digest, Sha256};
    let f = Fixture::new();
    let before = f.clock();
    let call = ToolCall {
        id: "call-1".into(),
        name: "side_effect".into(),
        arguments: serde_json::json!({}),
    };
    let actual = (
        call,
        "external_side_effect".into(),
        format!("{:x}", Sha256::digest(b"{}")),
        false,
    );
    for dimension in 0..4 {
        let mut calls = vec![actual.clone()];
        match dimension {
            0 => calls[0].1 = "read".into(),
            1 => calls[0].2 = "wrong".into(),
            2 => calls[0].3 = true,
            _ => calls.push(actual.clone()),
        }
        let descriptors = vec![ToolDescriptor::external(); calls.len()];
        assert_eq!(
            f.store
                .prepare_run_tool_batch(&f.envelope, 1, &calls, &descriptors)
                .await,
            Err(CoordinationError::InvalidRequest.into())
        );
        assert_eq!(f.clock(), before);
    }
    f.store
        .prepare_run_tool_batch(&f.envelope, 1, &[actual], &[ToolDescriptor::external()])
        .await
        .unwrap();
    f.store
        .start_run_tool(&f.envelope, 1, "call-1")
        .await
        .unwrap();
    assert_eq!(
        f.store.start_run_tool(&f.envelope, 1, "call-1").await,
        Err(CoordinationError::Conflict.into()),
        "重复Started不能再次进入副作用"
    );
}

#[tokio::test]
async fn native_tool_saved_batch_and_request_byte_budgets_reject_without_mutation() {
    use sha2::{Digest, Sha256};
    let f = Fixture::new();
    let call = ToolCall {
        id: "call-1".into(),
        name: "side_effect".into(),
        arguments: serde_json::json!({}),
    };
    let prepared = [(
        call.clone(),
        "external_side_effect".into(),
        format!("{:x}", Sha256::digest(b"{}")),
        false,
    )];
    let descriptors = [ToolDescriptor::external()];
    f.store
        .prepare_run_tool_batch(&f.envelope, 1, &prepared, &descriptors)
        .await
        .unwrap();
    let before = f.clock();
    for field in ["calls_json", "descriptor_json"] {
        let (old_calls, old_descriptors): (String, String) =
            f.db.query_row(
                "SELECT calls_json,descriptor_json FROM tool_batches",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let large = serde_json::json!([{"payload":"x".repeat(1024 * 1024)}]).to_string();
        f.db.execute(&format!("UPDATE tool_batches SET {field}=?1"), [&large])
            .unwrap();
        assert_eq!(
            f.store
                .prepare_run_tool_batch(&f.envelope, 1, &prepared, &descriptors)
                .await,
            Err(CoordinationError::Corrupt.into())
        );
        f.db.execute(
            "UPDATE tool_batches SET calls_json=?1,descriptor_json=?2",
            rusqlite::params![old_calls, old_descriptors],
        )
        .unwrap();
        assert_eq!(f.clock(), before);
    }
    let mut oversized = prepared.clone();
    oversized[0].0.arguments = serde_json::json!({"payload":"x".repeat(1024 * 1024)});
    oversized[0].2 = format!(
        "{:x}",
        Sha256::digest(oversized[0].0.arguments.to_string().as_bytes())
    );
    assert_eq!(
        f.store
            .prepare_run_tool_batch(&f.envelope, 2, &oversized, &descriptors)
            .await,
        Err(CoordinationError::InvalidRequest.into())
    );
    let messages = [Message::text(Role::User, "x".repeat(4 * 1024 * 1024))];
    assert_eq!(
        f.store
            .close_run_tool_exchange(&f.envelope, 1, &messages)
            .await,
        Err(CoordinationError::InvalidRequest.into())
    );
    assert_eq!(
        f.store.finish_run_tool_batch(&f.envelope, 0).await,
        Err(CoordinationError::InvalidRequest.into())
    );
    assert_eq!(
        f.store.start_run_tool(&f.envelope, 1, "").await,
        Err(CoordinationError::InvalidRequest.into())
    );
    assert_eq!(f.clock(), before);
    assert!(!f.root.join("runtime.artifacts").exists());
}
#[tokio::test]
async fn native_file_tool_completion_preserves_digest_errors_and_original_transaction_rollback_then_replays_once()
 {
    let f = Fixture::new();
    f.prepare();
    f.store
        .start_run_tool(&f.envelope, 1, "call-1")
        .await
        .unwrap();
    let before = f.clock();
    let receipt = completion("result");
    let mut bad = receipt.clone();
    bad.receipt["output_sha256"] = serde_json::json!("0".repeat(64));
    assert_eq!(
        f.store
            .finish_file_tool_output(&f.envelope, &bad, "result".into())
            .await,
        Err(ArtifactError::Corrupt.into())
    );
    assert_eq!(
        f.db.query_row::<String, _, _>("SELECT status FROM tool_executions", [], |r| r.get(0))
            .unwrap(),
        "running"
    );
    f.db.execute_batch("CREATE TRIGGER fail_tool_receipt BEFORE UPDATE OF status ON tool_executions WHEN NEW.status='terminal' BEGIN SELECT RAISE(ABORT,'PRIVATE_TOOL_SQL'); END;").unwrap();
    let error = f
        .store
        .finish_file_tool_output(&f.envelope, &receipt, "result".into())
        .await
        .unwrap_err();
    assert_eq!(error, CoordinationError::StorageUnavailable.into());
    assert!(!error.to_string().contains("PRIVATE"));
    assert_eq!(
        f.db.query_row::<String, _, _>("SELECT status FROM tool_executions", [], |r| r.get(0))
            .unwrap(),
        "running"
    );
    assert_eq!(f.clock(), before);
    f.db.execute_batch("DROP TRIGGER fail_tool_receipt;")
        .unwrap();
    let reference = f
        .store
        .finish_file_tool_output(&f.envelope, &receipt, "result".into())
        .await
        .unwrap();
    let after = f.clock();
    let reopened = RunStore::open(&f.root.join("runtime.sqlite3")).unwrap();
    assert_eq!(
        reopened
            .finish_file_tool_output(&f.envelope, &receipt, "result".into())
            .await
            .unwrap(),
        reference
    );
    assert_eq!(f.clock(), after);
    let mut changed = receipt.clone();
    changed.receipt["output"] = serde_json::json!("forged");
    assert_eq!(
        reopened
            .finish_file_tool_output(&f.envelope, &changed, "result".into())
            .await,
        Err(ArtifactError::Conflict.into())
    );
}
#[tokio::test]
async fn native_tool_closed_exchange_and_completed_batch_share_original_facts_and_idempotent_marker()
 {
    let f = Fixture::new();
    f.prepare();
    f.store
        .start_run_tool(&f.envelope, 1, "call-1")
        .await
        .unwrap();
    f.store
        .finish_file_tool_output(&f.envelope, &completion("result"), "result".into())
        .await
        .unwrap();
    let call = ToolCall {
        id: "call-1".into(),
        name: "side_effect".into(),
        arguments: serde_json::json!({}),
    };
    let messages = vec![
        Message::assistant_tool_calls(vec![call.clone()]),
        Message::tool_result(&call, "result"),
    ];
    assert_eq!(
        f.store.finish_run_tool_batch(&f.envelope, 1).await,
        Err(CoordinationError::Conflict.into()),
        "未闭合exchange不能发布batch完成"
    );
    f.store
        .close_run_tool_exchange(&f.envelope, 1, &messages)
        .await
        .unwrap();
    f.store.finish_run_tool_batch(&f.envelope, 1).await.unwrap();
    let after = f.clock();
    f.store.finish_run_tool_batch(&f.envelope, 1).await.unwrap();
    assert_eq!(f.clock(), after);
    assert_eq!(
        f.db.query_row::<u64, _, _>(
            "SELECT count(*) FROM events WHERE event='tool_batch_completed'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        1
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>(
            "SELECT count(*) FROM transcript_batches WHERE run_id=?1",
            [&f.envelope.owner.run_id.0],
            |r| r.get(0)
        )
        .unwrap(),
        2,
        "原user input与原closed exchange"
    );
}

#[tokio::test]
async fn native_tool_two_original_connections_prepare_one_batch_and_allow_only_one_started_transition()
 {
    use sha2::{Digest, Sha256};
    let f = Fixture::new();
    let second = RunStore::open(&f.root.join("runtime.sqlite3")).unwrap();
    let calls = [(
        ToolCall {
            id: "call-1".into(),
            name: "side_effect".into(),
            arguments: serde_json::json!({}),
        },
        "external_side_effect".into(),
        format!("{:x}", Sha256::digest(b"{}")),
        false,
    )];
    let descriptors = [ToolDescriptor::external()];
    let (a, b) = tokio::join!(
        f.store
            .prepare_run_tool_batch(&f.envelope, 1, &calls, &descriptors),
        second.prepare_run_tool_batch(&f.envelope, 1, &calls, &descriptors)
    );
    a.unwrap();
    b.unwrap();
    assert_eq!(
        f.db.query_row::<u64, _, _>(
            "SELECT count(*) FROM events WHERE event='tool_batch_prepared'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        1
    );
    let after = f.clock();
    second
        .prepare_run_tool_batch(&f.envelope, 1, &calls, &descriptors)
        .await
        .unwrap();
    assert_eq!(f.clock(), after);
    let (a, b) = tokio::join!(
        f.store.start_run_tool(&f.envelope, 1, "call-1"),
        second.start_run_tool(&f.envelope, 1, "call-1")
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert_eq!(
        [a, b].into_iter().find_map(Result::err).unwrap(),
        CoordinationError::Conflict.into()
    );
    let completed = completion("result");
    let (a, b) = tokio::join!(
        f.store
            .finish_file_tool_output(&f.envelope, &completed, "result".into()),
        second.finish_file_tool_output(&f.envelope, &completed, "result".into())
    );
    assert_eq!(a.unwrap(), b.unwrap());
    assert_eq!(
        f.db.query_row::<u64, _, _>(
            "SELECT count(*) FROM tool_executions WHERE status='terminal'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        1
    );
}
#[tokio::test(flavor = "current_thread")]
async fn native_tool_started_waiting_for_real_sqlite_write_lock_keeps_timer_responsive() {
    let f = Fixture::new();
    f.prepare();
    let path = f.root.join("runtime.sqlite3");
    let (tx, rx) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let db = Connection::open(path).unwrap();
        db.execute_batch("BEGIN IMMEDIATE;").unwrap();
        tx.send(()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(300));
        db.execute_batch("COMMIT;").unwrap();
    });
    rx.recv().unwrap();
    let begun = std::time::Instant::now();
    let (result, elapsed) = tokio::join!(f.store.start_run_tool(&f.envelope, 1, "call-1"), async {
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        begun.elapsed()
    });
    holder.join().unwrap();
    result.unwrap();
    assert!(
        elapsed < std::time::Duration::from_millis(150),
        "timer阻塞 {elapsed:?}"
    );
}

#[tokio::test]
async fn native_tool_every_method_rechecks_frozen_revocation_and_expired_proof_without_acquiring_new_epoch()
 {
    use agent_storage_ports::{IdentityReadRepository, RunExecutionRepository};
    use sha2::{Digest, Sha256};
    for fault in ["revoked", "expired"] {
        let root = std::env::temp_dir().join(format!(
            "ma-tool-fence-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let path = root.join("runtime.sqlite3");
        let store = RunStore::open(&path)
            .unwrap()
            .enable_session_writer()
            .unwrap();
        let identity = store
            .read_identity(
                &TenantContext::local(),
                &"local-subject".to_owned().try_into().unwrap(),
            )
            .await
            .unwrap();
        let admission = RunAdmission {
            workflow_node: None,
            plan_execution: None,
            session_key: SessionKey("tool-session".into()),
            expected_lifetime: None,
            request_id: RequestId::Number(1),
            input: "原工具复核".into(),
            mode: AdmissionMode::Queue,
        };
        let snapshot:RunSnapshot=serde_json::from_value(serde_json::json!({"cwd":".","tools":[],"permission_mode":"risk","sandbox_requested":"native","sandbox_effective":"native","context_read_only":false,"config_generation":0})).unwrap();
        let authorization = AuthorizationRequest {
            context: agent_identity::local_context(&identity, admission.request_id.clone())
                .unwrap(),
            expected_policy: identity.policy.version,
            action: Action::SessionPrompt,
            attributes: AuthorizationAttributes {
                workspace: ".".into(),
                entry_channel: HookChannel::DaemonRpc,
                tool_effect: None,
                timestamp_ms: 0,
                network: NetworkBoundary::None,
            },
        };
        let (_, owner) = store
            .admit_run_authorized(
                &admission,
                &snapshot,
                &authorization,
                std::sync::Arc::new(agent_policy::PolicyEngine),
            )
            .await
            .unwrap();
        let e = EnterpriseOwnerEnvelope {
            scope: TenantContext::local(),
            owner,
        };
        assert!(store.start_queued_run(&e).await.unwrap());
        let call = ToolCall {
            id: "call-1".into(),
            name: "side_effect".into(),
            arguments: serde_json::json!({}),
        };
        let prepared = [(
            call.clone(),
            "external_side_effect".into(),
            format!("{:x}", Sha256::digest(b"{}")),
            false,
        )];
        store
            .prepare_run_tool_batch(&e, 1, &prepared, &[ToolDescriptor::external()])
            .await
            .unwrap();
        store.start_run_tool(&e, 1, "call-1").await.unwrap();
        let db = Connection::open(path).unwrap();
        let expected: RepositoryError = if fault == "revoked" {
            db.execute("UPDATE principals SET revocation_epoch=1", [])
                .unwrap();
            AuthorizationError::Revoked.into()
        } else {
            db.execute("UPDATE session_writer_leases SET expires_at_ms=0", [])
                .unwrap();
            CoordinationError::StaleLease.into()
        };
        let before: u64 = db
            .query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
        let epoch: u64 = db
            .query_row("SELECT epoch FROM session_writer_leases", [], |r| r.get(0))
            .unwrap();
        let messages = [
            Message::assistant_tool_calls(vec![call.clone()]),
            Message::tool_result(&call, "result"),
        ];
        for result in [
            store
                .prepare_run_tool_batch(&e, 1, &prepared, &[ToolDescriptor::external()])
                .await,
            store.start_run_tool(&e, 1, "call-1").await,
            store.close_run_tool_exchange(&e, 1, &messages).await,
            store.finish_run_tool_batch(&e, 1).await,
            store.read_tool_artifact_binding(&e).await.map(|_| ()),
            store
                .finish_file_tool_output(&e, &completion("result"), "result".into())
                .await
                .map(|_| ()),
        ] {
            assert_eq!(result, Err(expected), "{fault}");
        }
        assert_eq!(
            db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
                .unwrap(),
            before
        );
        assert_eq!(
            db.query_row::<u64, _, _>("SELECT epoch FROM session_writer_leases", [], |r| r.get(0))
                .unwrap(),
            epoch
        );
        assert!(!root.join("runtime.artifacts").exists());
        drop(store);
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }
}
