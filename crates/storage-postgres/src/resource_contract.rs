use super::*;
use agent_storage_ports::{
    IdentityReadRepository, RunExecutionRepository, SessionWriterRepository,
};
use sha2::Digest;
use std::sync::Arc;

async fn publication_progress(store: &PgCanonicalStore) -> [i64; 4] {
    let client = store.inner.client.lock().await;
    let row = client.query_one("SELECT (SELECT revision FROM snapshot_clock WHERE slot=1),(SELECT count(*) FROM events),(SELECT count(*) FROM event_view_stamps),(SELECT count(*) FROM event_outbox)", &[]).await.unwrap();
    [row.get(0), row.get(1), row.get(2), row.get(3)]
}

/// 显式隔离数据库：AGENT_RESOURCE_TEST_DB/USER；不操作用户业务库。
#[tokio::test]
#[ignore = "需要显式配置本机 PostgreSQL 16+ 的临时测试数据库"]
async fn postgres_resource_lifecycle_scope_logs_and_transaction_rollback() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let database = std::env::var("AGENT_RESOURCE_TEST_DB").unwrap();
    let user = std::env::var("AGENT_RESOURCE_TEST_USER").unwrap();
    let schema = format!("resource_contract_{}", std::process::id());
    let store = PgCanonicalStore::connect_local(&database, &user, &schema)
        .await
        .unwrap();
    let scope = TenantContext::local();
    let principal = AuthenticatedPrincipal {
        subject_id: "resource-subject".to_owned().try_into().unwrap(),
        kind: SubjectKind::Human,
        revocation_epoch: 0,
    };
    store
        .provision_initial_domain(
            &scope,
            &principal,
            &[EnterpriseRole::Developer],
            &PolicyDocument::local(),
        )
        .await
        .unwrap();
    let identity = store
        .read_identity(&scope, &principal.subject_id)
        .await
        .unwrap();
    let snapshot: RunSnapshot = serde_json::from_value(serde_json::json!({
        "entry_channel":"cli","tools":[],"cwd":".","permission_mode":"risk_approval",
        "sandbox_requested":"native","sandbox_effective":"native","context_read_only":false,
        "context_token_budget":4096,"context_policy_fingerprint":"4096:2:60:85",
        "tool_catalog_digest":format!("{:x}",sha2::Sha256::digest(b"[]")),"config_generation":0
    }))
    .unwrap();
    let admission = RunAdmission {
        workflow_node: None,
        session_key: SessionKey("resource-session".into()),
        expected_lifetime: None,
        request_id: RequestId::Number(1),
        input: "资源合同".into(),
        mode: AdmissionMode::Queue,
        plan_execution: None,
    };
    let authorization = AuthorizationRequest {
        context: RequestContext {
            principal,
            scope: scope.clone(),
            request_id: admission.request_id.clone(),
            trace: TraceContext {
                trace_id: "resource-contract".into(),
            },
        },
        expected_policy: identity.policy.version,
        action: Action::SessionPrompt,
        attributes: AuthorizationAttributes {
            workspace: ".".into(),
            entry_channel: HookChannel::Cli,
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
            Arc::new(agent_policy::PolicyEngine),
        )
        .await
        .unwrap();
    let env = EnterpriseOwnerEnvelope {
        scope: scope.clone(),
        owner: owner.clone(),
    };
    store.start_queued_run(&env).await.unwrap();
    assert!(
        store
            .create_run_resource(&env, "/wrong", "native", "native")
            .await
            .is_err()
    );
    let resource = store
        .create_run_resource(&env, ".", "native", "native")
        .await
        .unwrap();
    assert_eq!(resource.state, "starting");
    assert!(
        store
            .update_run_resource(&env, resource.id, "completed", None, None)
            .await
            .is_err()
    );
    store
        .update_run_resource(
            &env,
            resource.id,
            "running",
            Some("fixed-workload-identity"),
            None,
        )
        .await
        .unwrap();
    for _ in 0..3 {
        store
            .append_run_resource_log(&env, resource.id, &"x".repeat(16384))
            .await
            .unwrap();
    }
    store
        .append_run_resource_log(&env, resource.id, &"x".repeat(16383))
        .await
        .unwrap();
    assert_eq!(
        store
            .append_run_resource_log(&env, resource.id, "你好")
            .await
            .unwrap(),
        4
    );
    assert_eq!(
        store
            .append_run_resource_log(&env, resource.id, "z")
            .await
            .unwrap(),
        5
    );
    assert_eq!(
        store
            .append_run_resource_log(&env, resource.id, "满额")
            .await
            .unwrap(),
        5
    );
    let logs = store
        .read_resource_log_page(
            &scope,
            &owner.session_key,
            &owner.session_lifetime_id,
            resource.id,
            3,
            100,
        )
        .await
        .unwrap();
    assert_eq!(logs.iter().map(|(n, _)| *n).collect::<Vec<_>>(), vec![4, 5]);
    assert_eq!(logs[1].1, "z");
    let mut foreign = scope.clone();
    foreign.tenant_id = "other-tenant".to_owned().try_into().unwrap();
    assert_eq!(
        store
            .read_resource_record(
                &foreign,
                &owner.session_key,
                &owner.session_lifetime_id,
                resource.id
            )
            .await
            .unwrap_err(),
        CoordinationError::Forbidden.into()
    );
    let mut stale = env.clone();
    stale.owner.run_generation.0 += 1;
    assert!(
        store
            .append_run_resource_log(&stale, resource.id, "迟到")
            .await
            .is_err()
    );
    // event失败时资源状态、游标、stamp和outbox必须一起回滚。
    let before = publication_progress(&store).await;
    {
        let client = store.inner.client.lock().await;
        client.batch_execute("CREATE FUNCTION resource_event_fault() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'resource event fault'; END $$; CREATE TRIGGER resource_event_fault BEFORE INSERT ON events FOR EACH ROW EXECUTE FUNCTION resource_event_fault()").await.unwrap();
    }
    assert!(
        store
            .update_run_resource(&env, resource.id, "stopping", None, None)
            .await
            .is_err()
    );
    assert_eq!(publication_progress(&store).await, before);
    assert_eq!(
        store
            .read_resource_record(
                &scope,
                &owner.session_key,
                &owner.session_lifetime_id,
                resource.id
            )
            .await
            .unwrap()
            .state,
        "running"
    );
    {
        let client = store.inner.client.lock().await;
        client
            .batch_execute(
                "DROP TRIGGER resource_event_fault ON events; DROP FUNCTION resource_event_fault()",
            )
            .await
            .unwrap();
    }
    store
        .update_run_resource(&env, resource.id, "orphaned", None, Some("失联"))
        .await
        .unwrap();
    assert!(
        store
            .reconcile_run_resource(&env, resource.id, "stopped", "")
            .await
            .is_err()
    );
    let result = store
        .reconcile_run_resource(&env, resource.id, "stopped", "已核对原进程退出")
        .await
        .unwrap();
    assert_eq!(result.state, "stopped");
    store
        .reconcile_run_resource(&env, resource.id, "stopped", "已核对原进程退出")
        .await
        .unwrap();
    assert!(
        store
            .update_run_resource(&env, resource.id, "running", None, None)
            .await
            .is_err()
    );
    assert!(
        store
            .append_run_resource_log(&env, resource.id, "结算后")
            .await
            .is_err()
    );
    assert_eq!(
        store
            .read_resource_page(
                &scope,
                &owner.session_key,
                &owner.session_lifetime_id,
                ResourceId(0),
                100
            )
            .await
            .unwrap()
            .len(),
        1
    );
    store.release_bound_writers().await.unwrap();
    store.close().await;
    let reopened = PgCanonicalStore::connect_local(&database, &user, &schema)
        .await
        .unwrap();
    let result = reopened
        .read_resource_record(
            &scope,
            &owner.session_key,
            &owner.session_lifetime_id,
            resource.id,
        )
        .await
        .unwrap();
    assert_eq!(result.state, "stopped");
    assert_eq!(result.log_cursor, 5);
    assert_eq!(
        result.process_identity.as_deref(),
        Some("fixed-workload-identity")
    );
    reopened.close().await;
}
