//! 有界、受管DAG调度；原native run是执行与审批事实源。
use crate::DaemonState;
use agent_core::*;
use agent_daemon_protocol::JsonRpcRequest;
use std::{sync::Arc,time::Duration};
use anyhow::Result;
use serde_json::json;
use tokio::task::JoinSet;
fn local_identity()->RuntimeIdentity{RuntimeIdentity::local_workflow()}
async fn authorize(state:&Arc<DaemonState>,identity:&RuntimeIdentity,action:Action,id:&str)->Result<AuthorizationRequest>{crate::handlers::AUTHENTICATED_IDENTITY.scope(identity.clone(),state.authorize_local_request(RequestId::String(id.into()),action)).await.map_err(Into::into)}
pub(crate) async fn start(state:Arc<DaemonState>)->Result<()>{
    let identity=if state.scope()==TenantContext::local(){local_identity()}else if std::env::var_os("AGENT_WORKFLOW_IDENTITY").is_some(){crate::runtime::read_configured_identity("AGENT_WORKFLOW_IDENTITY",local_identity())?}else{return Ok(())};
    anyhow::ensure!(identity.scope==state.scope()&&identity.principal.kind==SubjectKind::Workload&&identity.principal.subject_id!=state.runtime_identity.principal.subject_id,"工作流必须绑定同域独立workload");
    let read=state.identity_store.read_identity(&identity.scope,&identity.principal.subject_id).await?;anyhow::ensure!(read.principal==identity.principal,"工作流workload身份无效");
    let executor=format!("wf-{}-{}",std::process::id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_nanos());
    let owner=state.clone();anyhow::ensure!(state.spawn_owned(async move{
        let mut tick=tokio::time::interval(Duration::from_secs(1));tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);let mut workers=JoinSet::new();
        loop{tokio::select!{_=owner.shutdown.cancelled()=>break,
            _=tick.tick(),if workers.len()<32=>{let result=async{let auth=authorize(&owner,&identity,Action::SessionPrompt,"workflow-claim").await?;owner.workflow_store.claim_workflow_nodes(&auth,&executor,(32-workers.len()).min(8) as u32,Arc::new(agent_policy::PolicyEngine)).await.map_err(anyhow::Error::from)}.await;match result{Ok(claims)=>for claim in claims{let state=owner.clone();let identity=identity.clone();workers.spawn(async move{execute(state,identity,claim).await});},Err(_)=>tracing::warn!("工作流领取未确认；不重放已派发节点")}},
            result=workers.join_next(),if !workers.is_empty()=>{if !matches!(result,Some(Ok(Ok(())))){tracing::warn!("工作流执行未确认；读取canonical证据收敛");}}
        }}while let Some(result)=workers.join_next().await{if !matches!(result,Ok(Ok(()))){tracing::warn!("工作流排空未确认，保留原事实");}}
    }).await,"daemon停止准入，不能启动工作流调度");Ok(())
}
async fn execute(state:Arc<DaemonState>,identity:RuntimeIdentity,claim:WorkflowClaim)->Result<()>{
    let lease=claim.lease.clone();let creator=RuntimeIdentity{scope:claim.instance.scope.clone(),principal:claim.instance.creator.clone()};
    if claim.node.status==WorkflowNodeStatus::Claimed {
        match &claim.definition.kind {
            WorkflowNodeKind::SubWorkflow{..}=>{let auth=authorize(&state,&identity,Action::SessionPrompt,&lease.operation_id).await?;state.workflow_store.start_workflow_child(&auth,&lease,Arc::new(agent_policy::PolicyEngine)).await?;return Ok(())},
            WorkflowNodeKind::AgentRun{..}|WorkflowNodeKind::ToolCall{..}|WorkflowNodeKind::Approval{..}|WorkflowNodeKind::Webhook{..}=>{
                let key=claim.node.native_session_key.clone().unwrap_or_else(||SessionKey(state.session.session_key_for_operation(&lease.operation_id)));let workload_auth=authorize(&state,&identity,Action::SessionPrompt,&lease.operation_id).await?;state.workflow_store.bind_workflow_session(&workload_auth,&lease,&key,Arc::new(agent_policy::PolicyEngine)).await?;let auth=authorize(&state,&creator,Action::SessionCreate,&lease.operation_id).await?;
                if state.session_control_store.read_session_metadata(&state.scope(),&key).await?.is_none(){state.lifecycle_store.commit_session_authorized(&auth,&SessionCommand::Create{key:key.clone()},None,&format!("{}:session",lease.operation_id),Arc::new(agent_policy::PolicyEngine)).await?;}
                let request=JsonRpcRequest::new(RequestId::String(lease.operation_id.clone()),"chat.send",json!({"session_id":key.0,"message":agent_workflow::native_input_for(&claim.definition,&claim.instance.input)?,"entry_channel":"daemon_rpc"}));
                let(frames,mut receiver)=crate::frames::frame_channel();let mut overflow=frames.overflow_receiver();let drain=async move{while crate::frames::next_frame(&mut receiver,&mut overflow).await?.is_some(){}Ok::<(),crate::frames::FrameSendError>(())};
                let chat=crate::handlers::AUTHENTICATED_IDENTITY.scope(creator.clone(),crate::handlers::WORKFLOW_LEASE.scope(lease.clone(),crate::handlers::WORKFLOW_DIRECTIVE.scope(WorkflowNativeDirective{key:lease.key.clone(),definition:claim.definition.clone()},state.clone().handle_chat_send(request,frames))));
                let joined=async{let(_,drained)=tokio::join!(chat,drain);drained.map_err(|_|anyhow::anyhow!("内部工作流输出超出预算"))};tokio::pin!(joined);let mut renew=tokio::time::interval(Duration::from_secs(10));renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop{tokio::select!{result=&mut joined=>{result?;break},_=renew.tick()=>{let auth=authorize(&state,&identity,Action::SessionPrompt,&lease.operation_id).await?;state.workflow_store.renew_workflow_node(&auth,&lease,Arc::new(agent_policy::PolicyEngine)).await?;let read=state.workflow_store.read_workflow(&state.scope(),&lease.key.instance_id,0,1).await?;if let Some(node)=read.nodes.iter().find(|n|n.key==lease.key){let now=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis();if read.instance.status==WorkflowStatus::Cancelling||node.deadline_ms.is_some_and(|d|d>=0&&now>=d as u128){propagate_cancel(&state,&identity,&creator,node).await?;}}}}}
            },
            _=>return Err(anyhow::anyhow!("节点需要原native工具执行器")),
        }
    }
    let read=state.workflow_store.read_workflow(&state.scope(),&lease.key.instance_id,0,1).await?;let node=read.nodes.iter().find(|n|n.key==lease.key).ok_or_else(||anyhow::anyhow!("工作流节点不可访问"))?;
    if read.instance.status==WorkflowStatus::Cancelling {propagate_cancel(&state,&identity,&creator,node).await?;}
    let result=if let Some(run)=&node.native_run_id {let run=state.queue_store.read_run_record(&state.scope(),run).await?.ok_or_else(||anyhow::anyhow!("原run不可访问"))?;match run.status{RunStatus::Completed=>WorkflowNodeResult::Completed{output:agent_workflow::native_output(&run.content.unwrap_or_default())?,artifacts:artifact_refs(&state,&run.run_id).await?},RunStatus::UnknownAfterRestart=>WorkflowNodeResult::Unknown{evidence:"原run保留未知副作用证据，禁止重放".into()},RunStatus::Cancelled if read.instance.status==WorkflowStatus::Cancelling=>WorkflowNodeResult::Cancelled,RunStatus::Failed|RunStatus::Cancelled=>WorkflowNodeResult::Failed{code:"native_terminal".into(),evidence:"原run确认终态，外部副作用保留原收据".into(),confirmed_no_effect:false},_=>return Ok(())}}
    else if let Some(child)=&node.child_workflow_id{let child=state.workflow_store.read_workflow(&state.scope(),child,0,1).await?.instance;match child.status{WorkflowStatus::Completed=>WorkflowNodeResult::Completed{output:json!({"task_id":child.task_id,"status":"completed"}),artifacts:vec![]},WorkflowStatus::Cancelled if read.instance.status==WorkflowStatus::Cancelling=>WorkflowNodeResult::Cancelled,WorkflowStatus::Failed=>WorkflowNodeResult::Failed{code:"child_failed".into(),evidence:"子工作流原事实失败".into(),confirmed_no_effect:false},WorkflowStatus::Unknown=>WorkflowNodeResult::Unknown{evidence:"子流程存在未知副作用".into()},_=>return Ok(())}}else{return Err(anyhow::anyhow!("尚未关联原执行事实"))};
    let auth=authorize(&state,&identity,Action::SessionPrompt,&lease.operation_id).await?;state.workflow_store.finish_workflow_node(&auth,&lease,&result,Arc::new(agent_policy::PolicyEngine)).await?;Ok(())
}

async fn propagate_cancel(state:&Arc<DaemonState>,workload:&RuntimeIdentity,creator:&RuntimeIdentity,node:&WorkflowNodeRecord)->Result<()>{
 if let (Some(run),Some(session))=(&node.native_run_id,&node.native_session_key){let _=authorize(state,creator,Action::RunCancel,"workflow-cancel-child").await?;let owner=state.run_fact_store.read_exact_run_owner(&state.scope(),run).await?;crate::handlers::AUTHENTICATED_IDENTITY.scope(creator.clone(),state.cancel_one(agent_daemon_protocol::params::CancelParams{exact_owner:Some(owner),request_id:None,run_id:Some(run.clone()),session_id:Some(session.0.clone())})).await.map_err(|_|anyhow::anyhow!("原run取消未确认"))?;}
 if let Some(child)=&node.child_workflow_id {let instance=state.workflow_store.read_workflow(&state.scope(),child,0,1).await?.instance;if matches!(instance.status,WorkflowStatus::Running|WorkflowStatus::Paused){let auth=authorize(state,workload,Action::SessionManage,"workflow-cancel-child").await?;state.workflow_store.control_workflow(&auth,&WorkflowControl{operation_id:format!("workflow-parent-cancel:{}:{}:{}",node.key.instance_id,child,instance.revision),instance_id:child.clone(),expected_revision:instance.revision,action:WorkflowControlAction::Cancel},Arc::new(agent_policy::PolicyEngine)).await?;}}Ok(())
}

async fn artifact_refs(state:&Arc<DaemonState>,run:&RunId)->Result<Vec<WorkflowArtifactRef>>{let owner=state.run_fact_store.read_exact_run_owner(&state.scope(),run).await?;let receipts=state.run_fact_store.read_tool_receipts(&state.scope(),run).await?;let mut refs=Vec::new();for receipt in receipts{if refs.len()>=16{break}if receipt.status!="terminal"||receipt.outcome.as_deref()!=Some("success"){continue}if let (Some(reference),Some(raw))=(receipt.artifact_ref,receipt.receipt){let identity=ArtifactIdentity{sha256:raw.get("output_sha256").and_then(serde_json::Value::as_str).ok_or_else(||anyhow::anyhow!("原artifact缺内容digest"))?.into(),bytes:raw.get("output_bytes").and_then(serde_json::Value::as_u64).ok_or_else(||anyhow::anyhow!("原artifact缺字节数"))?};anyhow::ensure!(identity.valid(),"原artifact内容身份无效");refs.push(WorkflowArtifactRef{owner:EnterpriseOwnerEnvelope{scope:state.scope(),owner:owner.clone()},round:u32::try_from(receipt.round)?,call_id:receipt.call_id,reference,identity});}}Ok(refs)}
