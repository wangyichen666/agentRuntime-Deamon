//! 敏捷局部合同：只在调用者显式创建的临时数据库执行。
use super::*;
use agent_storage_ports::*;
use sha2::{Digest, Sha256};
use std::sync::Arc;

async fn admit(store: &PgCanonicalStore, session: &str) -> EnterpriseOwnerEnvelope {
    let scope = TenantContext::local();
    let principal = store
        .read_identity(&scope, &"local-subject".to_owned().try_into().unwrap())
        .await
        .unwrap();
    let tools = vec![ToolSpec {
        name: "read_file".into(),
        description: "只读文件".into(),
        parameters: serde_json::json!({"type":"object"}),
    }];
    let snapshot: RunSnapshot = serde_json::from_value(serde_json::json!({
        "entry_channel":"cli", "tools":tools,"cwd":".", "permission_mode":"risk_approval",
        "sandbox_requested":"native", "sandbox_effective":"native", "context_read_only":false,
        "context_token_budget":4096,"context_policy_fingerprint":"4096:2:60:85",
        "tool_catalog_digest":format!("{:x}",Sha256::digest(serde_json::to_vec(&tools).unwrap())), "config_generation":0
    }))
    .unwrap();
    let admission = RunAdmission {
        workflow_node: None,
        session_key: SessionKey(session.into()),
        expected_lifetime: None,
        request_id: RequestId::String(session.into()),
        input: "OPENAI_API_KEY=smoke-fixture\n普通任务".into(),
        mode: AdmissionMode::Queue,
        plan_execution: None,
    };
    let authorization = AuthorizationRequest {
        context: RequestContext {
            principal: principal.principal,
            scope: scope.clone(),
            request_id: admission.request_id.clone(),
            trace: TraceContext {
                trace_id: "native-smoke".into(),
            },
        },
        expected_policy: principal.policy.version,
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
    let env = EnterpriseOwnerEnvelope { scope, owner };
    assert!(store.start_queued_run(&env).await.unwrap());
    env
}
async fn complete(store: &PgCanonicalStore, env: &EnterpriseOwnerEnvelope) {
    store
        .stage_run_assistant(env, &Message::text(Role::Assistant, "完成"))
        .await
        .unwrap();
    store
        .commit_run_turn(
            env,
            &TurnCommit {
                owner: env.owner.clone(),
                status: RunStatus::Completed,
                content: Some("完成".into()),
                error: None,
            },
        )
        .await
        .unwrap();
}
#[tokio::test]
#[ignore = "必须显式配置 AGENT_RESOURCE_TEST_DB/USER 的独立临时 PostgreSQL 数据库"]
async fn postgres_native_memory_delegation_recovery_smoke() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let database = std::env::var("AGENT_RESOURCE_TEST_DB").unwrap();
    let user = std::env::var("AGENT_RESOURCE_TEST_USER").unwrap();
    let schema = format!("native_smoke_{}", std::process::id());
    let store = PgCanonicalStore::connect_local(&database, &user, &schema)
        .await
        .unwrap();
    let scope = TenantContext::local();
    store
        .provision_initial_domain(
            &scope,
            &AuthenticatedPrincipal {
                subject_id: "local-subject".to_owned().try_into().unwrap(),
                kind: SubjectKind::Human,
                revocation_epoch: 0,
            },
            &[EnterpriseRole::TenantAdmin],
            &PolicyDocument::local(),
        )
        .await
        .unwrap();
    let cron = store
        .read_identity(&scope, &LOCAL_CRON_SUBJECT.to_owned().try_into().unwrap())
        .await
        .unwrap();
    assert_eq!(cron.principal.kind, SubjectKind::Workload);
    assert_eq!(cron.roles, vec![EnterpriseRole::Developer]);
    let parent = admit(&store, "native-parent").await;
    let visibility = MemoryVisibility {
        lifetime: parent.owner.session_lifetime_id.clone(),
        project: ".".into(),
        allow_confirmed_global: false,
    };
    let entry = MemoryRecord {
        id: "native-memory".into(),
        layer: MemoryLayer::Working,
        scope: MemoryScope::Session(parent.owner.session_lifetime_id.clone()),
        kind: MemoryKind::Explicit,
        content: "优先复用现有接口".into(),
        source: Some(parent.owner.clone()),
        source_message_ids: vec![format!("{}:0", parent.owner.session_lifetime_id.0)],
        event_time: 0,
        created_at: 0,
        updated_at: 0,
        expires_at: None,
        confidence: 80,
        confirmed_by_user: false,
        content_digest: format!("{:x}", Sha256::digest("优先复用现有接口".as_bytes())),
        revision: 1,
    };
    store.store_run_memory(&parent, &entry).await.unwrap();
    store.store_run_memory(&parent, &entry).await.unwrap();
    let memories = store
        .read_memory_candidates(&scope, &parent.owner.session_key, &visibility)
        .await
        .unwrap();
    assert_eq!(memories.len(), 1);
    let evidence = store
        .read_memory_evidence(
            &scope,
            &parent.owner.session_key,
            &visibility,
            &entry.id,
            0,
            16,
        )
        .await
        .unwrap();
    assert_eq!(evidence.sources.len(), 1);
    assert!(!evidence.sources[0].text.contains("smoke-fixture"));
    store
        .record_run_memory_exposures(
            &parent,
            &[MemoryExposure::from(&entry)],
            "context_prepared",
            "smoke",
        )
        .await
        .unwrap();
    let request = DelegationRequest {
        parent_session_id: parent.owner.session_key.clone(),
        parent_run_id: parent.owner.run_id.clone(),
        child_session_id: SessionKey("native-child".into()),
        spawn_key: "child-once".into(),
        task: "查阅原任务".into(),
        context_source_ids: vec!["parent_input".into()],
        tools: vec!["read_file".into()],
        permission_mode: "request_approval".into(),
        cwd: ".".into(),
        max_rounds: 2,
        max_tokens: 1024,
        max_tool_calls: 2,
        deadline_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 60000,
    };
    let child = store
        .admit_child_delegation(&parent, &request)
        .await
        .unwrap();
    let retry = store
        .admit_child_delegation(&parent, &request)
        .await
        .unwrap();
    assert_eq!(retry.child_run_id, child.child_run_id);
    assert_eq!(
        store
            .read_session_delegation(&scope, &child.child_session_id)
            .await
            .unwrap()
            .unwrap()
            .child_run_id,
        child.child_run_id
    );
    let child_env = EnterpriseOwnerEnvelope {
        scope: scope.clone(),
        owner: identities::exact_owner_in(
            &*store.inner.client.lock().await,
            &scope,
            &child.child_run_id,
        )
        .await
        .unwrap(),
    };
    assert!(store.start_queued_run(&child_env).await.unwrap());
    complete(&store, &child_env).await;
    let settled = store
        .read_scoped_delegation(&scope, &parent.owner.run_id, &child.child_run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(settled.status, RunStatus::Completed);
    let reserved = store
        .reserve_child_result(
            &parent,
            &child.child_run_id,
            "delivery-one",
            settled.revision,
        )
        .await
        .unwrap();
    {
        let client = store.inner.client.lock().await;
        client
            .execute(
                "UPDATE delegations SET reservation_expires_at_ms=0 WHERE child_run_id=$1",
                &[&child.child_run_id.0],
            )
            .await
            .unwrap();
    }
    let reclaimed = store
        .reserve_child_result(
            &parent,
            &child.child_run_id,
            "delivery-one",
            reserved.revision,
        )
        .await
        .unwrap();
    assert_eq!(reclaimed.revision, reserved.revision + 2);
    let delivered = store
        .transition_child_result(
            &parent,
            &child.child_run_id,
            "delivery-one",
            reclaimed.revision,
            "delivered",
        )
        .await
        .unwrap();
    assert_eq!(delivered.result_state, "delivered");
    complete(&store, &parent).await;
    store
        .record_run_memory_feedback(&parent, "feedback-one", &entry.id, MemoryFeedback::Helpful)
        .await
        .unwrap();
    assert_eq!(
        store
            .read_memory_assessments(&scope, &parent.owner.session_key, &visibility)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        store
            .forget_run_memory(&parent, &visibility, &entry.id, entry.revision)
            .await
            .unwrap()
    );
    assert!(
        store
            .forget_run_memory(&parent, &visibility, &entry.id, entry.revision)
            .await
            .unwrap()
    );
    store
        .record_run_memory_feedback(&parent, "feedback-one", &entry.id, MemoryFeedback::Helpful)
        .await
        .unwrap();
    assert!(store.store_run_memory(&parent, &entry).await.is_err());
    store.ingest_run_memory(&parent).await.unwrap();
    store.ingest_run_memory(&parent).await.unwrap();
    assert_eq!(
        store
            .read_memory_page(&scope, &parent.owner.session_key, &visibility, None, 10)
            .await
            .unwrap()
            .len(),
        1
    );
    store
        .read_memory_flywheel(
            &scope,
            &parent.owner.session_key,
            &parent.owner.session_lifetime_id,
        )
        .await
        .unwrap();
    let finished = admit(&store, "native-maintain").await;
    complete(&store, &finished).await;
    assert_eq!(store.maintain_memory_turns(&scope, 32).await.unwrap().0, 2);
    let interrupted = admit(&store, "native-recovery").await;
    let foreign_writer = PgCanonicalStore::connect_local(&database, &user, &schema)
        .await
        .unwrap();
    assert_eq!(
        foreign_writer
            .recover_interrupted_work(&scope)
            .await
            .unwrap(),
        0
    );
    foreign_writer.close().await;
    store.release_bound_writers().await.unwrap();
    store.close().await;
    let reopened = PgCanonicalStore::connect_local(&database, &user, &schema)
        .await
        .unwrap();
    assert_eq!(reopened.recover_interrupted_work(&scope).await.unwrap(), 1);
    let recovered = reopened
        .read_run_record(&scope, &interrupted.owner.run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.status, RunStatus::UnknownAfterRestart);
    reopened
        .reconcile_run_outcome(
            &interrupted,
            recovered.last_seq,
            RunStatus::Completed,
            Some("人工确认"),
            "已经查阅执行日志及产物并确认结果",
        )
        .await
        .unwrap();
    assert_eq!(
        reopened.read_storage_health(&scope).await.unwrap()["schema_version"],
        2
    );
    reopened.release_bound_writers().await.unwrap();
    reopened.close().await;
}
