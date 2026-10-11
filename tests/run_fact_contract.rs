//! 原 run 只读事实隔离合同；RED 使用真实原同步 repository。
use agent_core::*;
use agent_storage::{RunStore, RuntimeError};
use agent_storage_ports::RunFactRepository;
use rusqlite::Connection;
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
            "ma-run-facts-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let path = root.join("runtime.sqlite3");
        let store = RunStore::open(&path).unwrap();
        let Admission::New(run) = store
            .admit(
                SessionKey("run-facts".into()),
                RequestId::Number(1),
                "私密输入",
            )
            .unwrap()
        else {
            panic!("新准入")
        };
        store.try_start_queued(&run.run_id).unwrap();
        let mut attempt = ProviderAttempt {
            attempt_id: "facts-attempt".into(),
            run_id: run.run_id.clone(),
            round: 1,
            candidate_index: 0,
            provider_profile_id: "facts-profile".into(),
            api_type: ApiType::Ollama,
            model: "frozen-model".into(),
            status: AttemptStatus::Started,
            error_kind: None,
            diagnostic: None,
            retry_after_ms: None,
            stream_committed: false,
            started_at_ms: 1,
            first_event_at_ms: None,
            finished_at_ms: None,
            usage: None,
        };
        store.start_provider_attempt(&attempt).unwrap();
        attempt.status = AttemptStatus::Succeeded;
        attempt.finished_at_ms = Some(2);
        attempt.usage = Some(ProviderUsage {
            input_tokens: Some(7),
            output_tokens: Some(3),
            ..Default::default()
        });
        store.finish_provider_attempt(&attempt).unwrap();
        store
            .prepare_tool_batch(
                &run.run_id,
                1,
                &[(
                    ToolCall {
                        id: "facts-call".into(),
                        name: "read".into(),
                        arguments: serde_json::json!({}),
                    },
                    "read_only".into(),
                    "digest".into(),
                    false,
                )],
            )
            .unwrap();
        store.start_tool(&run.run_id, 1, "facts-call").unwrap();
        store
            .finish_tool(
                &run.run_id,
                1,
                "facts-call",
                "success",
                None,
                &serde_json::json!({"private":"receipt-output"}),
            )
            .unwrap();
        let run = store
            .finish(&run.run_id, RunStatus::Completed, Some("私密输出"), None)
            .unwrap();
        let db = Connection::open(path).unwrap();
        Self {
            root,
            store,
            db,
            run,
        }
    }
    fn foreign(&self) {
        self.db.execute_batch("INSERT INTO organizations VALUES('other-org');INSERT INTO tenants VALUES('other-tenant','other-org');INSERT INTO projects VALUES('other-project','other-tenant');INSERT INTO agents VALUES('other-agent','other-tenant','other-project');UPDATE session_scopes SET tenant_id='other-tenant',project_id='other-project',agent_uid='other-agent' WHERE session_id='run-facts';").unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
async fn read(
    store: &RunStore,
    scope: &TenantContext,
    run: &RunId,
) -> Result<RunRecord, RuntimeError> {
    store
        .read_run_fact(scope, run)
        .await
        .map(|value| value.run)
        .map_err(Into::into)
}

#[tokio::test]
async fn run_fact_readback_never_reveals_foreign_content() {
    let f = Fixture::new();
    f.foreign();
    let result = read(&f.store, &TenantContext::local(), &f.run.run_id).await;
    assert!(
        matches!(
            result,
            Err(RuntimeError::Coordination(CoordinationError::Forbidden))
        ),
        "不能返回越域 run 私密输出：{result:?}"
    );
}

#[test]
fn provider_history_does_not_trust_inner_json_owner_fields() {
    let f = Fixture::new();
    let raw: String =
        f.db.query_row(
            "SELECT data_json FROM provider_attempts WHERE attempt_id='facts-attempt'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let mut value: serde_json::Value = serde_json::from_str(&raw).unwrap();
    value["round"] = serde_json::json!(77);
    f.db.execute(
        "UPDATE provider_attempts SET data_json=?1 WHERE attempt_id='facts-attempt'",
        [value.to_string()],
    )
    .unwrap();
    let result = f.store.provider_attempts(&f.run.run_id);
    assert!(
        result.is_err(),
        "SQL身份和JSON不一致不能被读回当作canonical：{result:?}"
    );
}

#[tokio::test]
async fn all_run_fact_ports_filter_every_scope_dimension_and_absent_before_reading() {
    let f = Fixture::new();
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
                .read_run_fact(&scope, &f.run.run_id)
                .await
                .unwrap_err(),
            CoordinationError::Forbidden
        );
        assert_eq!(
            f.store
                .read_tool_receipts(&scope, &f.run.run_id)
                .await
                .unwrap_err(),
            CoordinationError::Forbidden
        );
        assert_eq!(
            f.store
                .read_provider_history(&scope, &f.run.run_id)
                .await
                .unwrap_err(),
            CoordinationError::Forbidden
        );
    }
    let absent = RunId("absent".into());
    let scope = TenantContext::local();
    assert_eq!(
        f.store.read_run_fact(&scope, &absent).await.unwrap_err(),
        CoordinationError::Forbidden
    );
    assert_eq!(
        f.store
            .read_tool_receipts(&scope, &absent)
            .await
            .unwrap_err(),
        CoordinationError::Forbidden
    );
    assert_eq!(
        f.store
            .read_provider_history(&scope, &absent)
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
async fn run_fact_projection_reuses_original_facts_after_restart_and_retired_lifetime() {
    use agent_storage::{SessionLifecycle, SessionQuery};
    let f = Fixture::new();
    let scope = TenantContext::local();
    let read = f.store.read_run_fact(&scope, &f.run.run_id).await.unwrap();
    assert_eq!(
        serde_json::to_value(&read.run).unwrap(),
        serde_json::to_value(&f.run).unwrap()
    );
    assert_eq!(
        read.view_stamp.as_ref().unwrap().event_seq,
        Some(f.run.last_seq)
    );
    let tools = f
        .store
        .read_tool_receipts(&scope, &f.run.run_id)
        .await
        .unwrap();
    assert_eq!(
        tools[0].receipt.as_ref().unwrap()["private"],
        "receipt-output"
    );
    assert_eq!(
        serde_json::to_value(&tools).unwrap(),
        serde_json::to_value(f.store.tool_receipts(&f.run.run_id).unwrap()).unwrap()
    );
    let provider = f
        .store
        .read_provider_history(&scope, &f.run.run_id)
        .await
        .unwrap();
    assert_eq!(provider.usage.input_tokens, Some(7));
    assert_eq!(provider.usage.output_tokens, Some(3));
    assert_eq!(provider.final_candidate_index, Some(0));
    assert_eq!(
        serde_json::to_value(&provider.attempts).unwrap(),
        serde_json::to_value(f.store.provider_attempts(&f.run.run_id).unwrap()).unwrap()
    );
    let old = f
        .store
        .session_metadata(&f.run.session_id)
        .unwrap()
        .unwrap();
    f.store
        .execute_lifecycle(
            &SessionCommand::End {
                key: f.run.session_id.clone(),
                delete: false,
            },
            Some(&old.lifetime),
            "retire-facts",
        )
        .unwrap();
    let clock: u64 =
        f.db.query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
    let restarted = RunStore::open(&f.root.join("runtime.sqlite3")).unwrap();
    assert_eq!(
        serde_json::to_value(
            restarted
                .read_run_fact(&scope, &f.run.run_id)
                .await
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(read).unwrap()
    );
    assert_eq!(
        serde_json::to_value(
            restarted
                .read_tool_receipts(&scope, &f.run.run_id)
                .await
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(tools).unwrap()
    );
    assert_eq!(
        serde_json::to_value(
            restarted
                .read_provider_history(&scope, &f.run.run_id)
                .await
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(provider).unwrap()
    );
    assert_eq!(
        f.db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap(),
        clock
    );
}

#[tokio::test]
async fn run_readback_rejects_byte_and_row_overflow_and_corrupted_provider_identity() {
    let f = Fixture::new();
    let scope = TenantContext::local();
    let raw: String =
        f.db.query_row(
            "SELECT data_json FROM provider_attempts WHERE attempt_id='facts-attempt'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    for field in ["attempt_id", "run_id", "round", "candidate_index", "status"] {
        let mut value: serde_json::Value = serde_json::from_str(&raw).unwrap();
        value[field] = match field {
            "round" | "candidate_index" => serde_json::json!(77),
            "status" => serde_json::json!("failed"),
            _ => serde_json::json!("foreign"),
        };
        f.db.execute(
            "UPDATE provider_attempts SET data_json=?1 WHERE attempt_id='facts-attempt'",
            [value.to_string()],
        )
        .unwrap();
        assert_eq!(
            f.store
                .read_provider_history(&scope, &f.run.run_id)
                .await
                .unwrap_err(),
            CoordinationError::Corrupt,
            "{field}"
        );
    }
    f.db.execute(
        "UPDATE provider_attempts SET data_json=?1 WHERE attempt_id='facts-attempt'",
        [raw],
    )
    .unwrap();
    f.db.execute(
        "UPDATE runs SET content=?1 WHERE id=?2",
        rusqlite::params!["x".repeat(1024 * 1024), f.run.run_id.0],
    )
    .unwrap();
    assert_eq!(
        f.store
            .read_run_fact(&scope, &f.run.run_id)
            .await
            .unwrap_err(),
        CoordinationError::InvalidRequest
    );
    f.db.execute(
        "UPDATE tool_executions SET receipt_json=?1 WHERE run_id=?2",
        rusqlite::params![
            serde_json::json!({"output":"x".repeat(1024*1024)}).to_string(),
            f.run.run_id.0
        ],
    )
    .unwrap();
    assert_eq!(
        f.store
            .read_tool_receipts(&scope, &f.run.run_id)
            .await
            .unwrap_err(),
        CoordinationError::InvalidRequest
    );
    f.db.execute(
        "UPDATE tool_executions SET receipt_json='{}' WHERE run_id=?1",
        [&f.run.run_id.0],
    )
    .unwrap();
    f.db.execute("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<1000) INSERT INTO tool_executions(run_id,round,call_id,name,status,effect,argument_digest,prepared_at_ms) SELECT ?1,1,'extra-'||i,'read','completed','read_only','digest',0 FROM n",[&f.run.run_id.0]).unwrap();
    assert_eq!(
        f.store
            .read_tool_receipts(&scope, &f.run.run_id)
            .await
            .unwrap_err(),
        CoordinationError::InvalidRequest
    );
}

#[tokio::test]
async fn damaged_stamp_and_database_fault_are_typed_and_never_leak_sql() {
    let f = Fixture::new();
    let scope = TenantContext::local();
    f.db.execute(
        "UPDATE event_view_stamps SET stamp_json='{}' WHERE run_id=?1 AND seq=?2",
        rusqlite::params![f.run.run_id.0, f.run.last_seq.0],
    )
    .unwrap();
    assert_eq!(
        f.store
            .read_run_fact(&scope, &f.run.run_id)
            .await
            .unwrap_err(),
        CoordinationError::Corrupt
    );
    f.db.execute_batch("DROP TABLE provider_attempts;").unwrap();
    let error = f
        .store
        .read_provider_history(&scope, &f.run.run_id)
        .await
        .unwrap_err();
    assert_eq!(error, CoordinationError::StorageUnavailable);
    assert!(!error.to_string().contains("provider_attempts"));
}

#[tokio::test]
async fn run_fact_continuation_link_cannot_reveal_another_scope_or_missing_run() {
    let f = Fixture::new();
    let Admission::New(child) = f
        .store
        .admit(
            SessionKey("other-facts".into()),
            RequestId::Number(2),
            "另一域",
        )
        .unwrap()
    else {
        panic!("新child")
    };
    f.db.execute_batch("INSERT INTO organizations VALUES('other-org');INSERT INTO tenants VALUES('other-tenant','other-org');INSERT INTO projects VALUES('other-project','other-tenant');INSERT INTO agents VALUES('other-agent','other-tenant','other-project');UPDATE session_scopes SET tenant_id='other-tenant',project_id='other-project',agent_uid='other-agent' WHERE session_id='other-facts';").unwrap();
    f.db.execute(
        "INSERT INTO hook_continuations VALUES(?1,?2,'bad-link')",
        rusqlite::params![f.run.run_id.0, child.run_id.0],
    )
    .unwrap();
    for link in [&child.run_id.0, &"missing".to_owned(), &f.run.run_id.0] {
        f.db.execute(
            "UPDATE hook_continuations SET child_run_id=?1 WHERE parent_run_id=?2",
            rusqlite::params![link, f.run.run_id.0],
        )
        .unwrap();
        let read = f
            .store
            .read_run_fact(&TenantContext::local(), &f.run.run_id)
            .await;
        assert!(
            matches!(read, Err(CoordinationError::Corrupt)),
            "损坏 continuation 不能跨域/丢失/自环：{read:?}"
        );
    }
}
