//! 同一纵向合同：原身份管理/会话准入/审批CAS，不伪造第二审批状态。
use agent_core::*;
use agent_storage_ports::*;
use std::sync::Arc;
async fn auth<T:IdentityReadRepository>(store:&T,subject:&str,action:Action,id:&str)->AuthorizationRequest{
    let scope=TenantContext::local();let subject=subject.to_owned().try_into().unwrap();
    let identity=store.read_identity(&scope,&subject).await.unwrap();
    AuthorizationRequest {context:RequestContext {scope,principal:identity.principal,request_id:RequestId::String(id.into()),trace:TraceContext {trace_id:format!("test-{id}")}},expected_policy:identity.policy.version,action,
        attributes:AuthorizationAttributes {workspace:".".into(),entry_channel:HookChannel::DaemonRpc,tool_effect:None,timestamp_ms:0,network:NetworkBoundary::None}}
}
fn verifier()->Arc<dyn AuthorizationVerifier>{Arc::new(agent_policy::PolicyEngine)}
async fn contract<T>(s:&T) where T:IdentityReadRepository+IdentityManagementRepository+SessionLifecycleRepository+RunExecutionRepository+RunEventRepository+RunInteractionRepository+AuditReadRepository {
    let scope=TenantContext::local();let admin=auth(s,"local-subject",Action::IdentityManage,"admin").await;
    for name in ["approver_a","approver_b","approver_c"] {
        let command=IdentityCommand::RegisterSubject {principal:AuthenticatedPrincipal {subject_id:name.to_owned().try_into().unwrap(),kind:SubjectKind::Human,revocation_epoch:0}};
        let op=format!("register-{name}");
        let first=s.manage_identity(&admin,&command,&op,verifier()).await.unwrap();
        assert_eq!(s.manage_identity(&admin,&command,&op,verifier()).await.unwrap(),first);
        let roles=IdentityCommand::BindRoles {subject_id:name.to_owned().try_into().unwrap(),roles:vec![EnterpriseRole::Developer]};
        s.manage_identity(&admin,&roles,&format!("roles-{name}"),verifier()).await.unwrap();
        assert_eq!(s.manage_identity(&admin,&roles,&op,verifier()).await.unwrap_err(),RepositoryError::Coordination(CoordinationError::Conflict));
    }
    let mut dual=PolicyDocument::local();dual.version="dual-1".to_owned().try_into().unwrap();dual.dual_approval=true;
    s.manage_identity(&admin,&IdentityCommand::PublishPolicy {policy:dual},"publish-dual",verifier()).await.unwrap();
    let create=auth(s,"local-subject",Action::SessionCreate,"create").await;
    let key=SessionKey("same-name.jsonl".into());let command=SessionCommand::Create {key:key.clone()};
    let meta=s.commit_session_authorized(&create,&command,None,"create",verifier()).await.unwrap();
    assert_eq!(s.commit_session_authorized(&create,&command,None,"create",verifier()).await.unwrap().lifetime,meta.lifetime);
    let stale=AuthorizationRequest {action:Action::SessionCreate,..admin.clone()};
    assert_eq!(s.commit_session_authorized(&stale,&SessionCommand::Create {key:SessionKey("zero-write.jsonl".into())},None,"stale-create",verifier()).await.unwrap_err(),RepositoryError::Authorization(AuthorizationError::StalePolicy));
    let request=auth(s,"local-subject",Action::SessionPrompt,"run").await;
    let snapshot:RunSnapshot=serde_json::from_value(serde_json::json!({"entry_channel":"daemon_rpc","route":null,"tools":[],"cwd":".","permission_mode":"risk","sandbox_requested":"native","sandbox_effective":"native","context_read_only":false,"context_token_budget":4096,"tool_catalog_digest":"empty","config_generation":0})).unwrap();
    let (_,owner)=s.admit_run_authorized(&RunAdmission {session_key:key,expected_lifetime:Some(meta.lifetime),request_id:request.context.request_id.clone(),input:"fixture".into(),mode:AdmissionMode::Queue,plan_execution:None},&snapshot,&request,verifier()).await.unwrap();
    let env=EnterpriseOwnerEnvelope {scope:scope.clone(),owner};s.start_queued_run(&env).await.unwrap();
    let id=InteractionId("dual-approval".into());
    s.append_run_live_event(&env,"approval_required",&serde_json::json!({"approval":{"id":id,"prompt":"风险动作"}})).await.unwrap();
    // 普通发布不追改在途run冻结的双人要求。
    let mut normal=PolicyDocument::local();normal.version="normal-2".to_owned().try_into().unwrap();
    s.manage_identity(&auth(s,"local-subject",Action::IdentityManage,"publish-normal").await,&IdentityCommand::PublishPolicy {policy:normal},"publish-normal",verifier()).await.unwrap();
    assert!(s.read_authorization(&env).await.unwrap().unwrap().policy.dual_approval);
    let requester=auth(s,"local-subject",Action::InteractionRespond,"self").await;
    assert_eq!(s.respond_interaction_authorized(&requester,&env,&id,0,true,verifier()).await.unwrap_err(),RepositoryError::Authorization(AuthorizationError::Forbidden));
    let a=auth(s,"approver_a",Action::InteractionRespond,"vote-a").await;
    let one=s.respond_interaction_authorized(&a,&env,&id,0,true,verifier()).await.unwrap();
    assert_eq!((one.status.as_str(),one.revision),("pending",1));assert!(one.response.is_none());
    assert_eq!(s.respond_interaction_authorized(&a,&env,&id,0,true,verifier()).await.unwrap(),one);
    assert_eq!(s.respond_interaction_authorized(&a,&env,&id,1,true,verifier()).await.unwrap_err(),RepositoryError::Coordination(CoordinationError::Conflict));
    let revoke=IdentityCommand::Revoke {subject_id:"approver_a".to_owned().try_into().unwrap(),expected_epoch:0};
    s.manage_identity(&auth(s,"local-subject",Action::IdentityManage,"revoke").await,&revoke,"revoke-a",verifier()).await.unwrap();
    assert_eq!(s.respond_interaction_authorized(&a,&env,&id,0,true,verifier()).await.unwrap_err(),RepositoryError::Authorization(AuthorizationError::Revoked));
    let b=auth(s,"approver_b",Action::InteractionRespond,"vote-b").await;
    assert_eq!(s.respond_interaction_authorized(&b,&env,&id,0,true,verifier()).await.unwrap_err(),RepositoryError::Coordination(CoordinationError::Conflict));
    let two=s.respond_interaction_authorized(&b,&env,&id,1,true,verifier()).await.unwrap();
    assert_eq!((two.status.as_str(),two.revision),("pending",2),"撤销旧票后仍不足两人");
    let c=auth(s,"approver_c",Action::InteractionRespond,"vote-c").await;
    let final_answer=s.respond_interaction_authorized(&c,&env,&id,2,true,verifier()).await.unwrap();
    assert_eq!((final_answer.status.as_str(),final_answer.revision),("answered",3));
    assert_eq!(final_answer.response,Some(serde_json::json!({"approved":true})));
    let count=s.read_audit_entries(&scope,0,64).await.unwrap().len();
    assert_eq!(s.respond_interaction_authorized(&c,&env,&id,2,true,verifier()).await.unwrap(),final_answer);
    assert_eq!(s.read_audit_entries(&scope,0,64).await.unwrap().len(),count);
    let mut foreign=c.clone();foreign.context.scope.agent_uid="foreign-agent".to_owned().try_into().unwrap();
    assert_eq!(s.respond_interaction_authorized(&foreign,&env,&id,2,true,verifier()).await.unwrap_err(),RepositoryError::Authorization(AuthorizationError::Forbidden));
    s.commit_run_turn(&env,&TurnCommit {owner:env.owner.clone(),status:RunStatus::Failed,content:None,error:Some((-32003,"fixture收敛".into()))}).await.unwrap();
}
#[tokio::test]
async fn sqlite_identity_lifecycle_and_dual_approval(){
    let root=std::env::temp_dir().join(format!("governance-{}",std::process::id()));std::fs::create_dir_all(&root).unwrap();
    let s=agent_storage::RunStore::open(&root.join("facts.sqlite3")).unwrap();contract(&s).await;drop(s);std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
#[ignore="需要显式独立AGENT_RESOURCE_TEST_DB/USER"]
async fn postgres_identity_lifecycle_and_dual_approval(){
    let schema=format!("governance_{}_{}",std::process::id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis());
    let s=agent_storage_postgres::PgCanonicalStore::connect_local(&std::env::var("AGENT_RESOURCE_TEST_DB").unwrap(),&std::env::var("AGENT_RESOURCE_TEST_USER").unwrap(),&schema).await.unwrap();
    let binding=RuntimeIdentity::local();s.provision_initial_domain(&binding.scope,&binding.principal,&[EnterpriseRole::TenantAdmin],&PolicyDocument::local()).await.unwrap();contract(&s).await;s.release_bound_writers().await.unwrap();s.close().await;
}
