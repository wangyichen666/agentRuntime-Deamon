//! canonical事务内的共用编排。适配器只提供事实读写，不各自复制状态机。
use agent_core::*;
use serde::{Deserialize,Serialize};
use std::sync::Arc;
#[async_trait::async_trait]
pub trait WorkflowTransaction:Send {
    async fn identity(&mut self,subject:&SubjectId)->Result<IdentityReadback,RepositoryError>;
    async fn definition(&mut self,digest:&str)->Result<Option<FrozenWorkflowDefinition>,RepositoryError>;
    async fn put_definition(&mut self,frozen:&FrozenWorkflowDefinition)->Result<(),RepositoryError>;
    async fn instance(&mut self,id:&str)->Result<Option<WorkflowInstance>,RepositoryError>;
    async fn active_instances(&mut self)->Result<Vec<WorkflowInstance>,RepositoryError>;
    async fn nodes(&mut self,id:&str)->Result<Vec<WorkflowNodeRecord>,RepositoryError>;
    async fn insert_instance(&mut self,instance:&WorkflowInstance,nodes:&[WorkflowNodeRecord])->Result<(),RepositoryError>;
    async fn update_instance(&mut self,instance:&WorkflowInstance,old_revision:u64)->Result<(),RepositoryError>;
    async fn insert_node(&mut self,node:&WorkflowNodeRecord)->Result<(),RepositoryError>;
    async fn update_node(&mut self,node:&WorkflowNodeRecord,old_revision:u64)->Result<(),RepositoryError>;
    async fn receipt(&mut self,actor:&SubjectId,operation:&str)->Result<Option<(String,Reply)>,RepositoryError>;
    async fn put_receipt(&mut self,actor:&SubjectId,operation:&str,digest:&str,reply:&Reply)->Result<(),RepositoryError>;
    async fn event(&mut self,instance:&WorkflowInstance,node:Option<&WorkflowNodeRecord>,kind:&str,now:i64)->Result<(),RepositoryError>;
    async fn audit(&mut self,request:&AuthorizationRequest,operation:&str,digest:&str,now:i64)->Result<(),RepositoryError>;
    /// 原native终态/owner/artifact核验，禁止以客户端结果伪造native成功。
    async fn verify_native(&mut self,node:&WorkflowNodeRecord,result:&WorkflowNodeResult)->Result<(),RepositoryError>;
}
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(tag="type",content="value",rename_all="snake_case")]
pub enum Reply {Definition(FrozenWorkflowDefinition),Instance(WorkflowInstance),Claims(Vec<WorkflowClaim>),Node(WorkflowNodeRecord),Renewed}
pub enum Command<'a>{Message(&'a WorkflowMessage),Publish(&'a WorkflowPublish),Start(&'a WorkflowStart),Control(&'a WorkflowControl),Claim{executor:&'a str,limit:u32},Renew(&'a WorkflowNodeLease),BindSession{lease:&'a WorkflowNodeLease,key:&'a SessionKey},SpawnChild(&'a WorkflowNodeLease),Finish{lease:&'a WorkflowNodeLease,result:&'a WorkflowNodeResult}}
async fn load<T:WorkflowTransaction>(tx:&mut T,scope:&TenantContext,id:&str)->Result<(WorkflowInstance,FrozenWorkflowDefinition,Vec<WorkflowNodeRecord>),RepositoryError>{
    if !crate::valid_id(id){return Err(CoordinationError::InvalidRequest.into())}
    let instance=tx.instance(id).await?.ok_or(CoordinationError::Forbidden)?;let definition=tx.definition(&instance.definition_digest).await?.ok_or(CoordinationError::Corrupt)?;let nodes=tx.nodes(id).await?;crate::validate_state(&instance,&definition,&nodes,scope)?;Ok((instance,definition,nodes))
}
async fn save<T:WorkflowTransaction>(tx:&mut T,instance:&mut WorkflowInstance,original:&WorkflowInstance,nodes:&[WorkflowNodeRecord],old:&[WorkflowNodeRecord],kind:&str,now:i64)->Result<(),RepositoryError>{
    let mut changed=instance!=original;
    for node in nodes {let Some(prior)=old.iter().find(|n|n.key==node.key)else{if node.key.occurrence!=1||node.revision!=0{return Err(CoordinationError::Corrupt.into())}tx.insert_node(node).await?;tx.event(instance,Some(node),"compensation_planned",now).await?;changed=true;continue};if node!=prior {if node.revision<=prior.revision{return Err(CoordinationError::Corrupt.into())}tx.update_node(node,prior.revision).await?;tx.event(instance,Some(node),kind,now).await?;changed=true;}}
    if changed {if instance.revision==original.revision{instance.revision=instance.revision.checked_add(1).ok_or(CoordinationError::Corrupt)?;}instance.updated_at_ms=now;tx.update_instance(instance,original.revision).await?;tx.event(instance,None,kind,now).await?;}Ok(())
}
pub async fn execute<T:WorkflowTransaction>(tx:&mut T,request:&AuthorizationRequest,command:Command<'_>,verifier:Arc<dyn AuthorizationVerifier>,now:i64)->Result<Reply,RepositoryError>{
    let finish_operation=if let Command::Finish{lease,..}=&command{Some(format!("workflow-finish:{}",crate::digest(&serde_json::json!([lease.key,lease.epoch]))?))}else{None};
    let (action,operation,digest)=match &command{
        Command::Publish(c)=>(Action::AgentConfigure,Some(c.operation_id.as_str()),crate::operation_digest("publish",c)?),
        Command::Message(c)=>(Action::SessionPrompt,Some(c.operation_id.as_str()),crate::operation_digest("message",c)?),
        Command::Start(c)=>(Action::SessionPrompt,Some(c.operation_id.as_str()),crate::operation_digest("start",c)?),
        Command::Control(c)=>(Action::SessionManage,Some(c.operation_id.as_str()),crate::operation_digest("control",c)?),
        Command::Finish{lease,result}=>(Action::SessionPrompt,finish_operation.as_deref(),crate::operation_digest("node_result",&(lease,result))?),
        Command::Claim{..}|Command::Renew(_)|Command::SpawnChild(_)|Command::BindSession{..}=>(Action::SessionPrompt,None,String::new())};
    if request.action!=action{return Err(CoordinationError::InvalidRequest.into())}
    let mut checked=request.clone();checked.attributes.timestamp_ms=now;let identity=tx.identity(&request.context.principal.subject_id).await?;verifier.verify(&checked,&identity)?;
    if let Some(op)=operation {if op.is_empty()||op.len()>256{return Err(CoordinationError::InvalidRequest.into())}if let Some((previous,reply))=tx.receipt(&request.context.principal.subject_id,op).await? {if previous!=digest{return Err(CoordinationError::Conflict.into())}return Ok(reply)}}
    let reply=match command {
        Command::Publish(c)=>{let frozen=crate::freeze(&c.definition)?;tx.put_definition(&frozen).await?;Reply::Definition(frozen)},
        Command::Message(c)=>{if !crate::valid_id(&c.message_id)||c.message.trim().is_empty()||c.message.len()>32768{return Err(CoordinationError::InvalidRequest.into())}let frozen=crate::freeze(&crate::message_definition())?;tx.put_definition(&frozen).await?;let start=WorkflowStart{operation_id:c.operation_id.clone(),definition_digest:frozen.digest.clone(),input:serde_json::json!({"message":c.message,"message_id":c.message_id})};let(instance,nodes)=crate::new_instance(&request.context.scope,&request.context.principal,&start,&frozen,now)?;tx.insert_instance(&instance,&nodes).await?;tx.event(&instance,None,"message_submitted",now).await?;Reply::Instance(instance)},
        Command::Start(c)=>{let frozen=tx.definition(&c.definition_digest).await?.ok_or(CoordinationError::Forbidden)?;let(instance,nodes)=crate::new_instance(&request.context.scope,&request.context.principal,c,&frozen,now)?;tx.insert_instance(&instance,&nodes).await?;tx.event(&instance,None,"started",now).await?;Reply::Instance(instance)},
        Command::Control(c)=>{let(mut instance,_,nodes)=load(tx,&request.context.scope,&c.instance_id).await?;let old=instance.clone();crate::control(&mut instance,c,now)?;save(tx,&mut instance,&old,&nodes,&nodes,"controlled",now).await?;Reply::Instance(instance)},
        Command::Claim{executor,limit}=>{if limit==0||limit>32||!crate::valid_id(executor){return Err(CoordinationError::InvalidRequest.into())}let mut claims=Vec::new();for instance in tx.active_instances().await? {if claims.len()>=limit as usize{break}let(mut instance,frozen,mut nodes)=load(tx,&request.context.scope,&instance.id).await?;
            // 在途冻结principal不因普通策略发布被替换，但显式epoch撤销阻止新节点。
            let creator=tx.identity(&instance.creator.subject_id).await?;if creator.principal!=instance.creator{return Err(CoordinationError::Forbidden.into())}
            let old=instance.clone();let old_nodes=nodes.clone();let new=crate::advance(&mut instance,&frozen,&mut nodes,executor,limit as usize-claims.len(),now)?;instance.updated_at_ms=now;instance.revision=instance.revision.checked_add(1).ok_or(CoordinationError::Corrupt)?;save(tx,&mut instance,&old,&nodes,&old_nodes,"advanced",now).await?;claims.extend(new);}
            Reply::Claims(claims)},
        Command::SpawnChild(lease)=>{if lease.key.scope!=request.context.scope{return Err(CoordinationError::Forbidden.into())}let(mut instance,frozen,mut nodes)=load(tx,&request.context.scope,&lease.key.instance_id).await?;let original=instance.clone();let old_nodes=nodes.clone();let node=nodes.iter_mut().find(|n|n.key==lease.key).ok_or(CoordinationError::Forbidden)?;crate::lease_fence(node,lease,now)?;if node.dispatch_started||node.status!=WorkflowNodeStatus::Claimed{return Err(CoordinationError::Conflict.into())}let def=frozen.definition.nodes.iter().find(|d|d.id==node.key.node_id).ok_or(CoordinationError::Corrupt)?;let WorkflowNodeKind::SubWorkflow{definition_digest,input}=&def.kind else{return Err(CoordinationError::InvalidRequest.into())};let child_def=tx.definition(definition_digest).await?.ok_or(CoordinationError::Forbidden)?;
            let mut ancestor=instance.clone();let mut depth=0;while let Some(parent)=ancestor.parent{depth+=1;if depth>=8{return Err(CoordinationError::InvalidRequest.into())}ancestor=tx.instance(&parent.instance_id).await?.ok_or(CoordinationError::Corrupt)?;}let start=WorkflowStart{operation_id:lease.operation_id.clone(),definition_digest:definition_digest.clone(),input:input.clone()};let(mut child,child_nodes)=crate::new_instance(&request.context.scope,&instance.creator,&start,&child_def,now)?;child.parent=Some(node.key.clone());tx.insert_instance(&child,&child_nodes).await?;tx.event(&child,None,"child_started",now).await?;node.child_workflow_id=Some(child.id.clone());node.dispatch_started=true;node.status=WorkflowNodeStatus::WaitingNative;node.revision+=1;node.executor_id=None;node.expires_at_ms=0;save(tx,&mut instance,&original,&nodes,&old_nodes,"child_linked",now).await?;Reply::Instance(child)},
        Command::BindSession{lease,key}=>{if lease.key.scope!=request.context.scope||request.context.principal.kind!=SubjectKind::Workload||key.0.is_empty()||key.0.len()>1024||key.0.contains('/')||key.0.contains('\\')||key.0.contains(".."){return Err(CoordinationError::InvalidRequest.into())}let(mut instance,_,mut nodes)=load(tx,&request.context.scope,&lease.key.instance_id).await?;let old=instance.clone();let old_nodes=nodes.clone();let node=nodes.iter_mut().find(|n|n.key==lease.key).ok_or(CoordinationError::Forbidden)?;crate::lease_fence(node,lease,now)?;if node.dispatch_started||node.status!=WorkflowNodeStatus::Claimed{return Err(CoordinationError::Conflict.into())}if let Some(fixed)=&node.native_session_key{if fixed!=key{return Err(CoordinationError::Conflict.into())}}else{node.native_session_key=Some(key.clone());node.revision+=1;}let result=node.clone();save(tx,&mut instance,&old,&nodes,&old_nodes,"native_session_bound",now).await?;Reply::Node(result)},
        Command::Renew(lease)=>{if lease.key.scope!=request.context.scope{return Err(CoordinationError::Forbidden.into())}let(mut instance,_,mut nodes)=load(tx,&request.context.scope,&lease.key.instance_id).await?;let old=instance.clone();let old_nodes=nodes.clone();let node=nodes.iter_mut().find(|n|n.key==lease.key).ok_or(CoordinationError::Forbidden)?;crate::renew(node,lease,now)?;save(tx,&mut instance,&old,&nodes,&old_nodes,"lease_renewed",now).await?;Reply::Renewed},
        Command::Finish{lease,result}=>{if lease.key.scope!=request.context.scope{return Err(CoordinationError::Forbidden.into())}let(mut instance,frozen,mut nodes)=load(tx,&request.context.scope,&lease.key.instance_id).await?;let old=instance.clone();let old_nodes=nodes.clone();let node=nodes.iter_mut().find(|n|n.key==lease.key).ok_or(CoordinationError::Forbidden)?;crate::lease_fence(node,lease,now)?;tx.verify_native(node,result).await?;let definition=frozen.definition.nodes.iter().find(|n|n.id==lease.key.node_id).ok_or(CoordinationError::Corrupt)?;crate::finish(definition,node,lease,result,now)?;let result=node.clone();crate::settle(&mut instance,&frozen,&mut nodes)?;save(tx,&mut instance,&old,&nodes,&old_nodes,"node_result",now).await?;Reply::Node(result)}
    };
    if let Some(op)=operation{tx.put_receipt(&request.context.principal.subject_id,op,&digest,&reply).await?;tx.audit(request,op,&digest,now).await?;}Ok(reply)
}
