//! 同一合同通过两个真实canonical adapter，验证配额与run原子性及审计scope。
use agent_core::*;
use agent_storage_ports::*;
use std::sync::Arc;
fn snapshot() -> RunSnapshot {
    serde_json::from_value(serde_json::json!({"entry_channel":"daemon_rpc","route":null,"tools":[],"cwd":".","permission_mode":"risk","sandbox_requested":"native","sandbox_effective":"native","context_read_only":false,"context_token_budget":4096,"tool_catalog_digest":"empty","config_generation":0})).unwrap()
}
async fn contract<T>(s: &T)
where
    T: QuotaRepository
        + QuotaManagementRepository
        + IdentityReadRepository
        + AuditReadRepository
        + RunExecutionRepository
        + SessionReadRepository,
{
    let scope = TenantContext::local();
    let subject = "local-subject".to_owned().try_into().unwrap();
    let id = s.read_identity(&scope, &subject).await.unwrap();
    let context = agent_identity::local_context(&id, RequestId::Number(1)).unwrap();
    let mut request = AuthorizationRequest {
        context,
        expected_policy: id.policy.version,
        action: Action::AgentConfigure,
        attributes: AuthorizationAttributes {
            workspace: ".".into(),
            entry_channel: HookChannel::DaemonRpc,
            tool_effect: None,
            timestamp_ms: 0,
            network: NetworkBoundary::None,
        },
    };
    let q = QuotaAccount {
        scope: scope.clone(),
        metric: "runs".into(),
        window_start_ms: 0,
        window_end_ms: i64::MAX,
        budget: 1,
        consumed: 0,
        reserved: 0,
        revision: 0,
    };
    s.configure_quota_window(&request, &q, Arc::new(agent_policy::PolicyEngine))
        .await
        .unwrap();
    request.action = Action::SessionPrompt;
    let admission = RunAdmission {
        workflow_node: None,
        session_key: SessionKey("quota-contract".into()),
        expected_lifetime: None,
        request_id: RequestId::Number(1),
        input: "运行".into(),
        mode: AdmissionMode::Queue,
        plan_execution: None,
    };
    let (first, owner) = s
        .admit_run_authorized(
            &admission,
            &snapshot(),
            &request,
            Arc::new(agent_policy::PolicyEngine),
        )
        .await
        .unwrap();
    let env = EnterpriseOwnerEnvelope {
        scope: scope.clone(),
        owner,
    };
    assert!(matches!(first, Admission::New(_)));
    assert_eq!(
        s.read_quota_account(&scope, "runs", 0)
            .await
            .unwrap()
            .reserved,
        1
    );
    assert!(matches!(
        s.admit_run_authorized(
            &admission,
            &snapshot(),
            &request,
            Arc::new(agent_policy::PolicyEngine)
        )
        .await
        .unwrap()
        .0,
        Admission::Existing(_)
    ));
    let mut second = admission.clone();
    second.request_id = RequestId::Number(2);
    request.context.request_id = second.request_id.clone();
    let before = s
        .read_session(&scope, &admission.session_key, HistoryReadMode::Omitted)
        .await
        .unwrap();
    assert_eq!(
        s.admit_run_authorized(
            &second,
            &snapshot(),
            &request,
            Arc::new(agent_policy::PolicyEngine)
        )
        .await
        .unwrap_err(),
        RepositoryError::Ledger(LedgerError::QuotaExceeded)
    );
    let after = s
        .read_session(&scope, &admission.session_key, HistoryReadMode::Omitted)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(before).unwrap(),
        serde_json::to_value(after).unwrap()
    );
    assert!(s.cancel_queued_run(&env).await.unwrap());
    assert!(!s.cancel_queued_run(&env).await.unwrap());
    let quota = s.read_quota_account(&scope, "runs", 0).await.unwrap();
    assert_eq!((quota.reserved, quota.consumed), (0, 0));
    let (_, owner) = s
        .admit_run_authorized(
            &second,
            &snapshot(),
            &request,
            Arc::new(agent_policy::PolicyEngine),
        )
        .await
        .unwrap();
    let env = EnterpriseOwnerEnvelope {
        scope: scope.clone(),
        owner,
    };
    assert!(s.start_queued_run(&env).await.unwrap());
    let terminal = TurnCommit {
        owner: env.owner.clone(),
        status: RunStatus::Failed,
        content: None,
        error: Some((-32003, "本机合同".into())),
    };
    s.commit_run_turn(&env, &terminal).await.unwrap();
    s.commit_run_turn(&env, &terminal).await.unwrap();
    let quota = s.read_quota_account(&scope, "runs", 0).await.unwrap();
    assert_eq!((quota.reserved, quota.consumed), (0, 1));
    let audit = s
        .read_audit_entries(&scope, 0, 64)
        .await
        .unwrap()
        .into_iter()
        .filter(|e| e.command.action != "domain.provision")
        .collect::<Vec<_>>();
    assert_eq!(audit.len(), 5);
    assert_eq!(audit[0].command.action, "quota.configure");
    assert_eq!(audit.last().unwrap().command.outcome, "failed");
    let mut foreign = scope.clone();
    foreign.tenant_id = "foreign".to_owned().try_into().unwrap();
    assert!(
        s.read_audit_entries(&foreign, 0, 64)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(s.read_quota_account(&foreign, "runs", 0).await.is_err());
}
#[tokio::test]
async fn sqlite_quota_admission_terminal_idempotency_and_audit_scope() {
    let root = std::env::temp_dir().join(format!("agent-ledger-contract-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let s = agent_storage::RunStore::open(&root.join("facts.sqlite3")).unwrap();
    contract(&s).await;
    drop(s);
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
#[ignore = "需要显式独立AGENT_RESOURCE_TEST_DB/USER"]
async fn postgres_quota_admission_terminal_idempotency_and_audit_scope() {
    let s = agent_storage_postgres::PgCanonicalStore::connect_local(
        &std::env::var("AGENT_RESOURCE_TEST_DB").unwrap(),
        &std::env::var("AGENT_RESOURCE_TEST_USER").unwrap(),
        &format!("ledger_contract_{}", std::process::id()),
    )
    .await
    .unwrap();
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
    contract(&s).await;
    s.close().await;
}
