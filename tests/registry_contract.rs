//! 两backend的签名发行、发布CAS、session冻结与回滚合同。
use agent_core::*;
use agent_storage_ports::*;
use base64::{Engine,engine::general_purpose::STANDARD};
use ed25519_dalek::{Signer,SigningKey};
use std::{collections::BTreeMap,sync::Arc};
fn key()->SigningKey{SigningKey::from_bytes(&[42;32])}
fn trust()->RegistryTrust{RegistryTrust{version:"roots-v1".into(),roots:vec![RegistryTrustRoot{key_id:"fixture-root".into(),public_key_base64:STANDARD.encode(key().verifying_key().as_bytes()),maximum_trust:PluginTrust::OrganizationReviewed}]}}
fn pack(version:&str,prompt:&str)->SignedAgentPack{
    let content="只使用已获准工具。".as_bytes();let hash=format!("{:x}",sha2::Sha256::digest(content));
    let config=AgentConfig{system_prompt:"默认prompt".into(),models:vec!["fixture-model".into()],tool_allowlist:vec![],skill_refs:vec![ExtensionRef{coordinate:"skill/example".into(),path:"skills/example.md".into(),digest:hash.clone(),trust:PluginTrust::ProjectLocal}],mcp_refs:vec![],hook_refs:vec![],sandbox:AgentSandboxPolicy{backends:vec!["native".into()],network:false,secrets:false},resources:AgentResourceProfile{max_rounds:1,max_tool_calls:1,context_tokens:4096},labels:BTreeMap::new(),secret_refs:BTreeMap::new()};
    let layers=ConfigLayers{defaults:config,organization:AgentConfigPatch::default(),tenant:AgentConfigPatch{system_prompt:Some("租户prompt".into()),..Default::default()},project:AgentConfigPatch{system_prompt:Some(prompt.into()),..Default::default()},agent:AgentConfigPatch::default(),session_override:AgentConfigPatch::default()};
    let manifest=PackManifest{schema_version:1,definition:AgentDefinition{scope:TenantContext::local(),name:"同名Agent".into(),version:version.into(),layers},files:BTreeMap::from([("skills/example.md".into(),hash.clone())]),trust:PluginTrust::ProjectLocal};
    SignedAgentPack{signature_base64:STANDARD.encode(key().sign(&agent_pack::signature_message(&manifest).unwrap()).to_bytes()),manifest,contents:vec![PackFile{path:"skills/example.md".into(),digest:hash,content_base64:STANDARD.encode(content)}],key_id:"fixture-root".into()}
}
use sha2::Digest;
async fn auth<T:IdentityReadRepository>(s:&T,action:Action)->AuthorizationRequest{let scope=TenantContext::local();let subject="local-subject".to_owned().try_into().unwrap();let i=s.read_identity(&scope,&subject).await.unwrap();AuthorizationRequest{context:agent_identity::local_context(&i,RequestId::String("registry-contract".into())).unwrap(),expected_policy:i.policy.version,action,attributes:AuthorizationAttributes{workspace:".".into(),entry_channel:HookChannel::DaemonRpc,tool_effect:None,timestamp_ms:0,network:NetworkBoundary::None}}}
async fn mutate<T:RegistryRepository+IdentityReadRepository>(s:&T,rev:u64,op:&str,c:RegistryCommand)->RegistryReceipt{let a=auth(s,agent_registry::command_action(&c)).await;let m=RegistryMutation{operation_id:op.into(),expected_revision:rev,command:c};s.mutate_registry(&a,&m,Arc::new(agent_policy::PolicyEngine)).await.unwrap()}
async fn create<T:SessionLifecycleRepository+IdentityReadRepository>(s:&T,key:&str)->SessionLifetimeId{let a=auth(s,Action::SessionCreate).await;let r=s.commit_session_authorized(&a,&SessionCommand::Create{key:SessionKey(key.into())},None,key,Arc::new(agent_policy::PolicyEngine)).await.unwrap();r.lifetime}
async fn contract<T:RegistryRepository+SessionLifecycleRepository+IdentityReadRepository>(s:&T){let scope=TenantContext::local();let old=create(s,"old-session").await;
    mutate(s,0,"trust",RegistryCommand::ConfigureTrust{trust:trust()}).await;
    let v1=mutate(s,1,"publish-1",RegistryCommand::Publish{pack:pack("1.0.0","项目覆盖V1")}).await;
    let g1=v1.generation.clone().unwrap();assert_eq!(g1.effective.config.system_prompt,"项目覆盖V1");assert_eq!(g1.effective.sources["system_prompt"],"project");assert_eq!(g1.skill_materials["skill/example"],"只使用已获准工具。");
    assert_eq!(mutate(s,1,"publish-1",RegistryCommand::Publish{pack:pack("1.0.0","项目覆盖V1")}).await,v1);
    let mut bad=pack("1.1.0","坏签名");bad.contents[0].content_base64=STANDARD.encode(b"tampered");let a=auth(s,Action::PluginInstall).await;let m=RegistryMutation{operation_id:"bad".into(),expected_revision:2,command:RegistryCommand::Publish{pack:bad}};assert!(s.mutate_registry(&a,&m,Arc::new(agent_policy::PolicyEngine)).await.is_err());assert_eq!(s.read_registry(&scope).await.unwrap().state.revision,2);
    mutate(s,2,"rollout1",RegistryCommand::Rollout{stable:g1.pack_digest.clone(),canary:None,canary_basis_points:0}).await;
    let life_a=create(s,"session-a").await;assert_eq!(s.read_session_generation(&scope,&SessionKey("session-a".into()),&life_a).await.unwrap(),Some(g1.clone()));assert!(s.read_session_generation(&scope,&SessionKey("old-session".into()),&old).await.unwrap().is_none());
    let g2=mutate(s,3,"publish2",RegistryCommand::Publish{pack:pack("2.0.0","项目覆盖V2")}).await.generation.unwrap();
    mutate(s,4,"canary",RegistryCommand::Rollout{stable:g1.pack_digest.clone(),canary:Some(g2.pack_digest.clone()),canary_basis_points:10000}).await;
    let life_b=create(s,"session-b").await;assert_eq!(s.read_session_generation(&scope,&SessionKey("session-b".into()),&life_b).await.unwrap(),Some(g2.clone()));assert_eq!(s.read_session_generation(&scope,&SessionKey("session-a".into()),&life_a).await.unwrap(),Some(g1.clone()));
    mutate(s,5,"rollback",RegistryCommand::Rollback{digest:g1.pack_digest.clone()}).await;let life_c=create(s,"session-c").await;assert_eq!(s.read_session_generation(&scope,&SessionKey("session-c".into()),&life_c).await.unwrap(),Some(g1));assert_eq!(s.read_session_generation(&scope,&SessionKey("session-b".into()),&life_b).await.unwrap(),Some(g2));
    let foreign=TenantContext{tenant_id:"foreign".to_owned().try_into().unwrap(),..scope};assert!(s.read_registry(&foreign).await.unwrap().generations.is_empty());
}
#[tokio::test]async fn sqlite_registry_signed_generation(){let root=std::env::temp_dir().join(format!("registry-test-{}",std::process::id()));std::fs::create_dir_all(&root).unwrap();let store=agent_storage::RunStore::open(&root.join("facts.sqlite3")).unwrap();contract(&store).await;drop(store);std::fs::remove_dir_all(root).unwrap();}
#[tokio::test]#[ignore="显式自有PG测试库"]async fn postgres_registry_signed_generation(){let database=std::env::var("AGENT_RESOURCE_TEST_DB").unwrap();let user=std::env::var("AGENT_RESOURCE_TEST_USER").unwrap();let schema=format!("registry_contract_{}",std::process::id());let store=agent_storage_postgres::PgCanonicalStore::connect_local(&database,&user,&schema).await.unwrap();store.provision_initial_domain(&TenantContext::local(),&RuntimeIdentity::local().principal,&[EnterpriseRole::TenantAdmin],&PolicyDocument::local()).await.unwrap();contract(&store).await;store.close().await;assert!(std::process::Command::new("psql").args(["-X","-w","-h","/tmp","-U",&user,"-d",&database,"-c",&format!("DROP SCHEMA {schema} CASCADE")]).stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).status().unwrap().success());}
