use agent_core::*;
use agent_storage_ports::*;
use std::sync::Arc;
async fn auth<T: IdentityReadRepository>(s: &T, action: Action) -> AuthorizationRequest {
    let scope = TenantContext::local();
    let subject = "local-subject".to_owned().try_into().unwrap();
    let identity = s.read_identity(&scope, &subject).await.unwrap();
    AuthorizationRequest {
        context: RequestContext {
            scope,
            principal: identity.principal,
            request_id: RequestId::String("access-contract".into()),
            trace: TraceContext {
                trace_id: "contract".into(),
            },
        },
        expected_policy: identity.policy.version,
        action,
        attributes: AuthorizationAttributes {
            workspace: ".".into(),
            entry_channel: HookChannel::Http,
            tool_effect: None,
            timestamp_ms: 0,
            network: NetworkBoundary::None,
        },
    }
}
fn verifier() -> Arc<dyn AuthorizationVerifier> {
    Arc::new(agent_policy::PolicyEngine)
}
async fn contract<T: AccessRepository + IdentityReadRepository + SessionReadRepository+CredentialRepository+IdentityManagementRepository+AuditReadRepository>(s: &T) {
    let create = auth(s, Action::SessionCreate).await;
    let read = auth(s, Action::AgentRead).await;
    let key = SessionKey("session-directory.jsonl".into());
    let route = s
        .create_directory_session(&create, &key, "runtime-default", "create", verifier())
        .await
        .unwrap();
    assert!(valid_directory_id(&route.external_id));
    assert!(!route.external_id.contains("jsonl"));
    assert_eq!(
        s.create_directory_session(&create, &key, "runtime-default", "create", verifier())
            .await
            .unwrap(),
        route
    );
    assert_eq!(
        s.create_directory_session(&create, &key, "other-runtime", "create", verifier())
            .await
            .unwrap_err(),
        RepositoryError::Coordination(CoordinationError::Conflict)
    );
    assert_eq!(
        s.resolve_directory_session(&read, &route.external_id, verifier())
            .await
            .unwrap(),
        route
    );
    let mut foreign = read.clone();
    foreign.context.scope.agent_uid = "foreign".to_owned().try_into().unwrap();
    assert!(
        s.resolve_directory_session(&foreign, &route.external_id, verifier())
            .await
            .is_err()
    );
    let snapshot = s
        .read_session(&read.context.scope, &key, HistoryReadMode::Canonical)
        .await
        .unwrap();
    assert_eq!(snapshot.session_lifetime_id, route.session_lifetime);
    for i in 0..16 {
        let op = format!("active-{i}");
        let grant = s
            .acquire_access(&read, AccessClass::Request, &op, verifier())
            .await
            .unwrap();
        assert_eq!(
            s.acquire_access(&read, AccessClass::Request, &op, verifier())
                .await
                .unwrap(),
            grant
        );
    }
    assert_eq!(
        s.acquire_access(&read, AccessClass::Request, "overflow", verifier())
            .await
            .unwrap_err(),
        RepositoryError::Ledger(LedgerError::RateLimited)
    );
    s.release_access(&read, "active-0", verifier())
        .await
        .unwrap();
    s.release_access(&read, "active-0", verifier())
        .await
        .unwrap();
    assert_eq!(
        s.acquire_access(&read, AccessClass::Request, "active-0", verifier())
            .await
            .unwrap_err(),
        RepositoryError::Coordination(CoordinationError::StaleLease)
    );
    s.acquire_access(&read, AccessClass::Request, "after-release", verifier())
        .await
        .unwrap();
    for i in 1..16 {
        s.release_access(&read, &format!("active-{i}"), verifier())
            .await
            .unwrap();
    }
    s.release_access(&read, "after-release", verifier())
        .await
        .unwrap();
    let admin=auth(s,Action::IdentityManage).await;
    let subject:SubjectId="service-fixture".to_owned().try_into().unwrap();
    s.manage_identity(&admin,&IdentityCommand::RegisterSubject {principal:AuthenticatedPrincipal {subject_id:subject.clone(),kind:SubjectKind::ServiceAccount,revocation_epoch:0}},"register-service",verifier()).await.unwrap();
    s.manage_identity(&admin,&IdentityCommand::BindRoles {subject_id:subject.clone(),roles:vec![EnterpriseRole::Developer]},"service-roles",verifier()).await.unwrap();
    assert!(s.issue_api_credential(&admin,&subject,901,"ttl-fail",verifier()).await.is_err());
    let issued=s.issue_api_credential(&admin,&subject,300,"issue",verifier()).await.unwrap();
    let token=issued.token.unwrap();assert!(!format!("{token:?}").contains(token.as_str()));
    assert_eq!(s.authenticate_api_credential(&admin.context.scope,&token).await.unwrap(),issued.metadata);
    let replay=s.issue_api_credential(&admin,&subject,300,"issue",verifier()).await.unwrap();assert!(replay.token.is_none());assert_eq!(replay.metadata,issued.metadata);
    assert_eq!(s.issue_api_credential(&admin,&subject,301,"issue",verifier()).await.unwrap_err(),RepositoryError::Coordination(CoordinationError::Conflict));
    let mut forged=token.as_str().to_owned();let last=forged.pop().unwrap();forged.push(if last=='A'{'B'}else{'A'});
    assert!(s.authenticate_api_credential(&admin.context.scope,&SecretToken::new(forged).unwrap()).await.is_err());
    assert!(s.authenticate_api_credential(&foreign.context.scope,&token).await.is_err());
    s.manage_identity(&admin,&IdentityCommand::Revoke {subject_id:subject.clone(),expected_epoch:0},"service-revoke",verifier()).await.unwrap();
    assert_eq!(s.authenticate_api_credential(&admin.context.scope,&token).await.unwrap_err(),RepositoryError::Authorization(AuthorizationError::Revoked));
    let rotated=s.issue_api_credential(&admin,&subject,300,"rotate",verifier()).await.unwrap();let rotated_token=rotated.token.unwrap();assert_eq!(rotated.metadata.principal.revocation_epoch,1);
    assert!(s.authenticate_api_credential(&admin.context.scope,&rotated_token).await.is_ok());
    let disabled=s.revoke_api_credential(&admin,&rotated.metadata.key_id,"credential-revoke",verifier()).await.unwrap();assert!(disabled.disabled);
    assert_eq!(s.revoke_api_credential(&admin,&rotated.metadata.key_id,"credential-revoke",verifier()).await.unwrap(),disabled);
    assert!(s.authenticate_api_credential(&admin.context.scope,&rotated_token).await.is_err());
    s.manage_identity(&admin,&IdentityCommand::BindRoles {subject_id:subject.clone(),roles:vec![EnterpriseRole::Gateway]},"gateway-roles",verifier()).await.unwrap();
    let gateway=s.read_identity(&admin.context.scope,&subject).await.unwrap();
    let mut delegation=admin.clone();delegation.context.principal=gateway.principal;delegation.action=Action::GatewayDelegate;
    let proof=OAuthLoginProof{state_digest:"a".repeat(64),nonce_digest:"b".repeat(64),verifier_digest:"c".repeat(64)};
    assert!(s.oauth_login(&admin,&proof,false,verifier()).await.is_err());
    s.oauth_login(&delegation,&proof,false,verifier()).await.unwrap();
    let mut wrong=proof.clone();wrong.verifier_digest="d".repeat(64);
    assert!(s.oauth_login(&delegation,&wrong,true,verifier()).await.is_err());
    s.oauth_login(&delegation,&proof,true,verifier()).await.unwrap();
    assert!(s.oauth_login(&delegation,&proof,true,verifier()).await.is_err());
    let audits=serde_json::to_string(&s.read_audit_entries(&admin.context.scope,0,64).await.unwrap()).unwrap();assert!(!audits.contains(token.as_str()));assert!(!audits.contains(rotated_token.as_str()));assert!(!audits.contains("argon2id"));
}
#[tokio::test]
async fn sqlite_directory_and_bounded_access() {
    let root = std::env::temp_dir().join(format!("access-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let s = agent_storage::RunStore::open(&root.join("facts.sqlite3")).unwrap();
    contract(&s).await;
    drop(s);
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
#[ignore = "需要显式独立AGENT_RESOURCE_TEST_DB/USER"]
async fn postgres_directory_and_shared_access() {
    let database = std::env::var("AGENT_RESOURCE_TEST_DB").unwrap();
    let user = std::env::var("AGENT_RESOURCE_TEST_USER").unwrap();
    let schema = format!(
        "access_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    let s = agent_storage_postgres::PgCanonicalStore::connect_local(&database, &user, &schema)
        .await
        .unwrap();
    let local = RuntimeIdentity::local();
    s.provision_initial_domain(
        &local.scope,
        &local.principal,
        &[EnterpriseRole::TenantAdmin],
        &PolicyDocument::local(),
    )
    .await
    .unwrap();
    contract(&s).await;
    let other = agent_storage_postgres::PgCanonicalStore::connect_local(&database, &user, &schema)
        .await
        .unwrap();
    let read = auth(&s, Action::AgentRead).await;
    async fn work(
        store: &agent_storage_postgres::PgCanonicalStore,
        prefix: &str,
        read: AuthorizationRequest,
    ) -> usize {
        let mut accepted = 0;
        for i in 0..12 {
            match store
                .acquire_access(
                    &read,
                    AccessClass::Request,
                    &format!("{prefix}-{i}"),
                    verifier(),
                )
                .await
            {
                Ok(_) => accepted += 1,
                Err(RepositoryError::Ledger(LedgerError::RateLimited)) => {}
                Err(error) => panic!("unexpected {error:?}"),
            }
        }
        accepted
    }
    let (a, b) = tokio::join!(work(&s, "a", read.clone()), work(&other, "b", read));
    assert_eq!(a + b, 16, "两个PG连接共享同一主体并发预算");
    other.close().await;
    s.close().await;
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
