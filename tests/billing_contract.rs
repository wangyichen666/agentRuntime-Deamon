//! 两个真实 adapter 的原准入/Provider事务费用合同；无前端派生费用作为准入依据。
use agent_core::*;
use agent_storage_ports::*;
use sha2::{Digest, Sha256};
use std::sync::Arc;
fn digest(value: &serde_json::Value) -> String {
    format!("{:x}", Sha256::digest(serde_json::to_vec(value).unwrap()))
}
use std::collections::BTreeMap;
fn verifier() -> Arc<dyn AuthorizationVerifier> {
    Arc::new(agent_policy::PolicyEngine)
}
async fn auth<T: IdentityReadRepository>(s: &T, action: Action, id: u64) -> AuthorizationRequest {
    let scope = TenantContext::local();
    let subject = "local-subject".to_owned().try_into().unwrap();
    let identity = s.read_identity(&scope, &subject).await.unwrap();
    AuthorizationRequest {
        context: agent_identity::local_context(&identity, RequestId::Number(id)).unwrap(),
        expected_policy: identity.policy.version,
        action,
        attributes: AuthorizationAttributes {
            workspace: ".".into(),
            entry_channel: HookChannel::DaemonRpc,
            tool_effect: None,
            timestamp_ms: 0,
            network: NetworkBoundary::None,
        },
    }
}
fn policy() -> BillingPolicy {
    let mut p = BillingPolicy {
        version: "prices-1".into(),
        currency: "USD".into(),
        window_start_ms: 0,
        window_end_ms: i64::MAX,
        models: BTreeMap::from([(
            "fixture-model".into(),
            ModelPrice {
                input_per_million: 1_000_000,
                output_per_million: 2_000_000,
                cache_read_per_million: 200_000,
                cache_creation_per_million: 500_000,
            },
        )]),
        tool_call_micros: 5,
        sandbox_per_million_ms: 1_000_000,
        limits: BillingLimits {
            max_rounds: 1,
            max_provider_attempts: 1,
            max_input_tokens: 65536,
            max_output_tokens_per_attempt: 128,
            max_tool_calls: 1,
            max_sandbox_wall_ms: 10,
            max_sandbox_wall_ms_per_resource: 5,
        },
        budgets: vec![],
    };
    let amount = p.maximum_cost().unwrap();
    p.budgets = [
        BillingDimension::Tenant,
        BillingDimension::Project,
        BillingDimension::Agent,
        BillingDimension::Model("fixture-model".into()),
    ]
    .into_iter()
    .map(|dimension| BillingBudget {
        dimension,
        hard_micros: amount,
        soft_micros: 1,
    })
    .collect();
    p
}
fn snapshot(frozen: FrozenBillingPolicy) -> RunSnapshot {
    let mut s:RunSnapshot=serde_json::from_value(serde_json::json!({"entry_channel":"daemon_rpc","tools":[],"cwd":".","permission_mode":"risk","sandbox_requested":"native","sandbox_effective":"native","context_read_only":false,"context_token_budget":4096,"tool_catalog_digest":digest(&serde_json::json!([])),"context_policy_fingerprint":"4096:12:60:85","config_generation":0})).unwrap();
    s.billing_policy = Some(frozen);
    s.route = Some(RouteSnapshot {
        candidates: vec![RouteCandidate {
            profile_id: "fixture-profile".into(),
            api_type: ApiType::OpenaiChat,
            model: "fixture-model".into(),
            base_url_sha256: "a".repeat(64),
        }],
        retry_policy: RetryPolicy::default(),
        timeout_policy: TimeoutPolicy::default(),
        context_policy: ContextPolicySnapshot { token_budget: 4096 },
        config_generation: 0,
    });
    s.max_tool_calls = Some(1);
    s
}
fn admission(id: u64) -> RunAdmission {
    RunAdmission {
        workflow_node: None,
        session_key: SessionKey(format!("billing-{id}")),
        expected_lifetime: None,
        request_id: RequestId::Number(id),
        input: "输入".into(),
        mode: AdmissionMode::Queue,
        plan_execution: None,
    }
}
async fn start_attempt<T: RunRequestRepository + RunAttemptRepository>(
    s: &T,
    env: &EnterpriseOwnerEnvelope,
    snapshot: &RunSnapshot,
    id: &str,
) -> ProviderAttempt {
    let candidate = &snapshot.route.as_ref().unwrap().candidates[0];
    let identity = digest(&serde_json::json!(candidate));
    let source = s
        .read_run_request_source(env, "fixture-model", &identity)
        .await
        .unwrap();
    let context_source = ContextSource {
        lifetime: source.snapshot.lifetime,
        source_start: TranscriptSeq(0),
        source_end: source.snapshot.revision,
        prefix_digest: agent_context::digest(&source.snapshot.messages).unwrap(),
        generation: source.snapshot.projection_generation,
        policy_fingerprint: snapshot.context_policy_fingerprint.clone().unwrap(),
        pressure_route: "fixture-model".into(),
        summary_route: "fixture-model".into(),
    };
    let input = ProviderRequestInput {
        purpose: ProviderRequestPurpose::Model,
        source: context_source,
        messages: vec![Message::text(Role::User, "材料")],
        tools: vec![],
        catalog: vec![],
        route: "fixture-model".into(),
        provider_identity: identity,
        images: true,
        tool_calls: true,
        budget: 4096,
        calibration: source.calibration,
    };
    let assembled = agent_context::assemble_request(&input).unwrap();
    let capture = ProviderRequestCapture {
        schema_version: 1,
        capture_id: id.into(),
        owner: env.owner.clone(),
        round: 1,
        candidate_index: 0,
        snapshot_revision: source.revision,
        input,
        policy_fingerprint: digest(&serde_json::json!(snapshot)),
        request_digest: assembled.request_digest,
    };
    s.capture_run_request(env, &capture, &assembled.envelope)
        .await
        .unwrap();
    let attempt = ProviderAttempt {
        attempt_id: id.into(),
        run_id: env.owner.run_id.clone(),
        round: 1,
        candidate_index: 0,
        provider_profile_id: "fixture-profile".into(),
        api_type: ApiType::OpenaiChat,
        model: "fixture-model".into(),
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
    s.start_run_attempt(env, &attempt).await.unwrap();
    s.start_run_attempt(env, &attempt).await.unwrap();
    attempt
}
async fn contract<T>(s: Arc<T>, competitor: Option<Arc<T>>)
where
    T: BillingRepository
        + IdentityReadRepository
        + AuditReadRepository
        + RunExecutionRepository
        + RunRequestRepository
        + RunAttemptRepository
        + RunTurnRepository
        + RunFactRepository
        + Send
        + Sync
        + 'static,
{
    let scope = TenantContext::local();
    let admin = auth(s.as_ref(), Action::IdentityManage, 0).await;
    let p = policy();
    let reserve = p.maximum_cost().unwrap();
    let frozen = s
        .configure_billing(&admin, &p, "publish-1", verifier())
        .await
        .unwrap();
    assert_eq!(
        s.configure_billing(&admin, &p, "publish-1", verifier())
            .await
            .unwrap(),
        frozen
    );
    let mut conflict = p.clone();
    conflict.currency = "EUR".into();
    assert_eq!(
        s.configure_billing(&admin, &conflict, "publish-1", verifier())
            .await
            .unwrap_err(),
        RepositoryError::Coordination(CoordinationError::Conflict)
    );
    let old_snapshot = snapshot(frozen.clone());
    let request = auth(s.as_ref(), Action::SessionPrompt, 1).await;
    let (_, owner) = s
        .admit_run_authorized(&admission(1), &old_snapshot, &request, verifier())
        .await
        .unwrap();
    let env = EnterpriseOwnerEnvelope {
        scope: scope.clone(),
        owner,
    };
    assert!(
        s.read_billing_accounts(&scope)
            .await
            .unwrap()
            .iter()
            .all(|a| a.reserved_micros == reserve && a.consumed_micros == 0)
    );
    let mut jobs = Vec::new();
    for id in 10..18 {
        let request=auth(s.as_ref(),Action::SessionPrompt,id).await;
        let s = if id%2==0 {competitor.as_ref().unwrap_or(&s).clone()} else{s.clone()};
        let snapshot = old_snapshot.clone();
        jobs.push(tokio::spawn(async move {
            s.admit_run_authorized(&admission(id), &snapshot, &request, verifier())
                .await
        }));
    }
    for j in jobs {
        let error=j.await.unwrap().unwrap_err();
        assert!(matches!(error,RepositoryError::Ledger(LedgerError::QuotaExceeded)|RepositoryError::Coordination(CoordinationError::StorageUnavailable)),"{error:?}");
    }
    // 发布当前新价，不改变在途原run的封存价格；旧op重读也不回滚current。
    let mut next = p.clone();
    next.version = "prices-2".into();
    next.models
        .get_mut("fixture-model")
        .unwrap()
        .input_per_million = 5_000_000;
    let new_reserve = next.maximum_cost().unwrap();
    for b in &mut next.budgets {
        b.hard_micros = new_reserve + reserve + 100;
    }
    let newest = s
        .configure_billing(&admin, &next, "publish-2", verifier())
        .await
        .unwrap();
    assert_eq!(
        s.configure_billing(&admin, &p, "publish-1", verifier())
            .await
            .unwrap(),
        frozen
    );
    assert_eq!(
        s.read_billing_policy(&scope).await.unwrap(),
        Some(newest.clone())
    );
    assert!(s.start_queued_run(&env).await.unwrap());
    let mut attempt = start_attempt(s.as_ref(), &env, &old_snapshot, "provider-one").await;
    attempt.status = AttemptStatus::Succeeded;
    attempt.finished_at_ms = Some(2);
    attempt.usage = Some(ProviderUsage {
        input_tokens: Some(10),
        output_tokens: Some(2),
        cache_read_tokens: Some(3),
        cache_creation_tokens: Some(0),
    });
    s.finish_run_attempt(&env, &attempt).await.unwrap();
    s.finish_run_attempt(&env, &attempt).await.unwrap();
    let mut extra = attempt.clone();
    extra.attempt_id = "over-attempt-limit".into();
    extra.status = AttemptStatus::Started;
    extra.finished_at_ms = None;
    extra.usage = None;
    assert_eq!(
        s.start_run_attempt(&env, &extra).await.unwrap_err(),
        RepositoryError::Ledger(LedgerError::QuotaExceeded)
    );
    s.stage_run_assistant(&env, &Message::text(Role::Assistant, "完成"))
        .await
        .unwrap();
    let terminal = TurnCommit {
        owner: env.owner.clone(),
        status: RunStatus::Completed,
        content: Some("完成".into()),
        error: None,
    };
    s.commit_run_turn(&env, &terminal).await.unwrap();
    s.commit_run_turn(&env, &terminal).await.unwrap();
    let accounts = s.read_billing_accounts(&scope).await.unwrap();
    assert!(
        accounts
            .iter()
            .all(|a| a.reserved_micros == 0 && a.consumed_micros == 12),
        "{accounts:?}"
    );
    let usage = s.read_billing_usage(&scope, 0, 64).await.unwrap();
    assert_eq!(usage.entries.len(), 1);
    assert_eq!(usage.entries[0].cost_micros, Some(12));
    assert_eq!(
        usage.entries[0].policy_digest.as_deref(),
        Some(frozen.digest.as_str())
    );
    let fact = s.read_run_fact(&scope, &env.owner.run_id).await.unwrap();
    assert!(fact.view_stamp.unwrap().terminal);
    assert!(
        s.read_audit_entries(&scope, 0, 64)
            .await
            .unwrap()
            .iter()
            .any(|e| e.command.action == "billing.budget_alert")
    );
    let mut foreign = scope.clone();
    foreign.tenant_id = "foreign".to_owned().try_into().unwrap();
    assert!(
        s.read_billing_usage(&foreign, 0, 64)
            .await
            .unwrap()
            .entries
            .is_empty()
    );
    assert!(s.read_billing_policy(&foreign).await.unwrap().is_none());
    let new_snapshot = snapshot(newest);
    let req = auth(s.as_ref(), Action::SessionPrompt, 2).await;
    let (_, owner) = s
        .admit_run_authorized(&admission(2), &new_snapshot, &req, verifier())
        .await
        .unwrap();
    let unknown = EnterpriseOwnerEnvelope {
        scope: scope.clone(),
        owner,
    };
    assert!(s.start_queued_run(&unknown).await.unwrap());
    start_attempt(s.as_ref(), &unknown, &new_snapshot, "unknown-provider").await;
    let t = TurnCommit {
        owner: unknown.owner.clone(),
        status: RunStatus::UnknownAfterRestart,
        content: None,
        error: Some((-32002, "未知调用".into())),
    };
    s.commit_run_turn(&unknown, &t).await.unwrap();
    let accounts = s.read_billing_accounts(&scope).await.unwrap();
    assert!(
        accounts
            .iter()
            .all(|a| a.reserved_micros == new_reserve && a.consumed_micros == 12)
    );
    let usage = s.read_billing_usage(&scope, 0, 64).await.unwrap();
    assert_eq!(usage.entries.len(), 2);
    assert!(usage.entries[1].outcome_unknown && usage.entries[1].cost_micros.is_none());
}
#[tokio::test]
async fn sqlite_billing_original_transactions() {
    let root = std::env::temp_dir().join(format!("agent-billing-contract-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let s = Arc::new(agent_storage::RunStore::open(&root.join("facts.sqlite3")).unwrap());
    contract(s.clone(),None).await;
    drop(s);
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
#[ignore = "必须显式提供独立AGENT_RESOURCE_TEST_DB/USER"]
async fn postgres_billing_original_transactions() {
    let schema = format!("billing_contract_{}", std::process::id());
    let database = std::env::var("AGENT_RESOURCE_TEST_DB").unwrap();
    let user = std::env::var("AGENT_RESOURCE_TEST_USER").unwrap();
    let s = Arc::new(
        agent_storage_postgres::PgCanonicalStore::connect_local(&database, &user, &schema)
            .await
            .unwrap(),
    );
    s.provision_initial_domain(
        &TenantContext::local(),
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
    let other=Arc::new(agent_storage_postgres::PgCanonicalStore::connect_local(&database,&user,&schema).await.unwrap());
    contract(s.clone(),Some(other.clone())).await;
    match Arc::try_unwrap(other) {Ok(store)=>store.close().await,Err(_)=>panic!("合同连接仍被持有")}
    match Arc::try_unwrap(s) {Ok(store)=>store.close().await,Err(_)=>panic!("主合同连接仍被持有")}

    assert!(
        std::process::Command::new("psql")
            .args([
                "-X",
                "-w",
                "-h",
                "/tmp",
                "-U",
                &user,
                "-d",
                &database,
                "-c",
                &format!("DROP SCHEMA {schema} CASCADE")
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap()
            .success()
    );
}
