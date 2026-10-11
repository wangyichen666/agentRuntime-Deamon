//! 纯DAG/重试/补偿规则；执行、lease与事件由canonical适配器和daemon拥有。
#![forbid(unsafe_code)]
use agent_core::*;
use std::collections::{BTreeMap,BTreeSet};
pub use agent_pack::valid_digest;
pub fn digest(value:&serde_json::Value)->Result<String,CoordinationError>{fn sorted(v:&serde_json::Value,depth:usize)->Result<serde_json::Value,CoordinationError>{if depth>32{return Err(CoordinationError::InvalidRequest)}Ok(match v{serde_json::Value::Object(map)=>{let ordered=map.iter().collect::<BTreeMap<_,_>>();serde_json::Value::Object(ordered.into_iter().map(|(k,v)|Ok((k.clone(),sorted(v,depth+1)?))).collect::<Result<_,CoordinationError>>()?)},serde_json::Value::Array(a)=>serde_json::Value::Array(a.iter().map(|v|sorted(v,depth+1)).collect::<Result<_,_>>()?),_=>v.clone()})}let bytes=serde_json::to_vec(&sorted(value,0)?).map_err(|_|CoordinationError::InvalidRequest)?;if bytes.len()>1024*1024{return Err(CoordinationError::InvalidRequest)}use sha2::Digest;Ok(format!("{:x}",sha2::Sha256::digest(bytes)))}
pub fn valid_id(id:&str)->bool {!id.is_empty()&&id.len()<=128&&id.bytes().all(|v|v.is_ascii_alphanumeric()||matches!(v,b'-'|b'_'))}
pub fn freeze(definition:&WorkflowDefinition)->Result<FrozenWorkflowDefinition,CoordinationError>{
    if definition.schema_version!=1 || definition.name.is_empty() || definition.name.len()>128 || semver::Version::parse(&definition.version).is_err() || definition.nodes.is_empty() || definition.nodes.len()>256 || serde_json::to_vec(definition).map_err(|_|CoordinationError::InvalidRequest)?.len()>1024*1024{return Err(CoordinationError::InvalidRequest)}
    let mut ids=BTreeMap::new();for n in &definition.nodes {if !valid_id(&n.id)||ids.insert(n.id.as_str(),n).is_some()||n.dependencies.len()>256||!(1..=8).contains(&n.max_attempts)||!(100..=86400000).contains(&n.timeout_ms)||serde_json::to_vec(&n.input).map_err(|_|CoordinationError::InvalidRequest)?.len()>65536{return Err(CoordinationError::InvalidRequest)}
        match &n.kind {
            WorkflowNodeKind::AgentRun{message}|WorkflowNodeKind::Approval{prompt:message} if message.is_empty() || message.len()>32768=>return Err(CoordinationError::InvalidRequest),
            WorkflowNodeKind::ToolCall{call} if call.name.is_empty() || call.name.len()>256 || serde_json::to_vec(&call.arguments).map_err(|_|CoordinationError::InvalidRequest)?.len()>65536=>return Err(CoordinationError::InvalidRequest),
            WorkflowNodeKind::Condition{path,..} if path.len()>16 || path.iter().any(|p|p.is_empty()||p.len()>128)=>return Err(CoordinationError::InvalidRequest),
            WorkflowNodeKind::WaitUntil{timestamp_ms} if *timestamp_ms<0=>return Err(CoordinationError::InvalidRequest),
            WorkflowNodeKind::Webhook{url,idempotency_key,..} if !url.starts_with("https://") || url.len()>2048 || idempotency_key.as_ref().is_some_and(|v|v.is_empty()||v.len()>256)=>return Err(CoordinationError::InvalidRequest),
            WorkflowNodeKind::SubWorkflow{definition_digest,..} if !valid_digest(definition_digest)=>return Err(CoordinationError::InvalidRequest),_=>{}
        }
        if matches!(n.retry,WorkflowRetryClass::Pure) && !is_pure(&n.kind) || matches!(&n.retry,WorkflowRetryClass::Idempotent{key} if key.is_empty()||key.len()>256){return Err(CoordinationError::InvalidRequest)}
    }
    let mut finished=BTreeSet::new();let mut compensators=BTreeSet::new();
    for n in &definition.nodes {let mut deps=BTreeSet::new();if n.dependencies.iter().any(|d|d==&n.id||!ids.contains_key(d.as_str())||!deps.insert(d)){return Err(CoordinationError::InvalidRequest)}if let Some(c)=&n.compensation {if c==&n.id||!ids.contains_key(c.as_str())||!compensators.insert(c){return Err(CoordinationError::InvalidRequest)}}}
    for id in &compensators {if ids.get(id.as_str()).is_some_and(|n|n.compensation.is_some()){return Err(CoordinationError::InvalidRequest)}}
    while finished.len()<ids.len(){let before=finished.len();for n in &definition.nodes {if n.dependencies.iter().all(|d|finished.contains(d.as_str())){finished.insert(n.id.as_str());}}if finished.len()==before{return Err(CoordinationError::InvalidRequest)}}
    Ok(FrozenWorkflowDefinition{digest:digest(&serde_json::json!(definition))?,definition:definition.clone()})
}
pub fn is_pure(kind:&WorkflowNodeKind)->bool {matches!(kind,WorkflowNodeKind::Condition{..}|WorkflowNodeKind::Parallel|WorkflowNodeKind::Join|WorkflowNodeKind::WaitUntil{..})}
pub fn terminal(status:WorkflowNodeStatus)->bool {matches!(status,WorkflowNodeStatus::Completed|WorkflowNodeStatus::Failed|WorkflowNodeStatus::Unknown|WorkflowNodeStatus::Cancelled|WorkflowNodeStatus::Compensated|WorkflowNodeStatus::Skipped)}
pub fn ready(def:&WorkflowNodeDefinition,nodes:&[WorkflowNodeRecord])->bool {def.dependencies.iter().all(|d|nodes.iter().any(|n|n.key.node_id==*d&&n.key.occurrence==0&&matches!(n.status,WorkflowNodeStatus::Completed|WorkflowNodeStatus::Skipped)))}
pub fn pure_result(kind:&WorkflowNodeKind,input:&serde_json::Value,now_ms:i64)->Result<Option<serde_json::Value>,CoordinationError>{Ok(match kind {
    WorkflowNodeKind::Condition{path,equals}=>{let value=path.iter().try_fold(input,|v,k|v.get(k));Some(serde_json::json!({"condition":value==Some(equals)}))},
    WorkflowNodeKind::Parallel|WorkflowNodeKind::Join=>Some(serde_json::json!({"joined":true})),
    WorkflowNodeKind::WaitUntil{timestamp_ms}=>if now_ms>=*timestamp_ms {Some(serde_json::json!({"reached_at_ms":now_ms}))}else{None},_=>return Err(CoordinationError::InvalidRequest)
})}
/// 结果未知不能因为retry class或lease超时重放；只有明确无副作用失败可进入下一attempt。
pub fn may_retry(def:&WorkflowNodeDefinition,node:&WorkflowNodeRecord,no_effect:bool)->bool {no_effect&&node.attempt<def.max_attempts&&matches!(def.retry,WorkflowRetryClass::Pure|WorkflowRetryClass::Idempotent{..})}
pub fn expire(node:&mut WorkflowNodeRecord,now:i64){if node.expires_at_ms>now || node.status!=WorkflowNodeStatus::Claimed{return}node.executor_id=None;node.expires_at_ms=0;if !node.dispatch_started{node.attempt=node.attempt.saturating_sub(1);}node.status=if node.dispatch_started{WorkflowNodeStatus::Unknown}else{WorkflowNodeStatus::Pending};if node.dispatch_started{node.evidence=Some("lease过期且派发已提交；必须读取native receipt或证据化reconcile，禁止重放".into());}}
pub fn aggregate(nodes:&[WorkflowNodeRecord],current:WorkflowStatus)->WorkflowStatus {if nodes.iter().any(|n|n.status==WorkflowNodeStatus::Unknown){return WorkflowStatus::Unknown}if nodes.iter().any(|n|n.status==WorkflowNodeStatus::Failed){return WorkflowStatus::Failed}if nodes.iter().all(|n|matches!(n.status,WorkflowNodeStatus::Completed|WorkflowNodeStatus::Skipped|WorkflowNodeStatus::Compensated)){return WorkflowStatus::Completed}if current==WorkflowStatus::Cancelling && nodes.iter().all(|n|terminal(n.status)){return WorkflowStatus::Cancelled}current}
pub fn false_branch(def:&WorkflowNodeDefinition,nodes:&[WorkflowNodeRecord],frozen:&FrozenWorkflowDefinition)->bool {if matches!(def.kind,WorkflowNodeKind::Join){return false}def.dependencies.iter().any(|d|nodes.iter().any(|n|n.key.node_id==*d&&n.key.occurrence==0&&(n.status==WorkflowNodeStatus::Skipped || frozen.definition.nodes.iter().any(|d|d.id==n.key.node_id&&matches!(d.kind,WorkflowNodeKind::Condition{..}))&&n.output.as_ref().is_some_and(|v|v.get("condition")==Some(&serde_json::Value::Bool(false))))))}
pub fn new_instance(scope:&TenantContext,creator:&AuthenticatedPrincipal,command:&WorkflowStart,frozen:&FrozenWorkflowDefinition,now:i64)->Result<(WorkflowInstance,Vec<WorkflowNodeRecord>),CoordinationError>{
    if command.operation_id.is_empty() || command.operation_id.len()>256 || command.definition_digest!=frozen.digest || serde_json::to_vec(&command.input).map_err(|_|CoordinationError::InvalidRequest)?.len()>65536{return Err(CoordinationError::InvalidRequest)}
    let id=format!("wf-{}",digest(&serde_json::json!([scope,creator.subject_id,command.operation_id]))?);let input_digest=digest(&command.input)?;
    let instance=WorkflowInstance{id:id.clone(),scope:scope.clone(),definition_digest:frozen.digest.clone(),input:command.input.clone(),input_digest,creator:creator.clone(),status:WorkflowStatus::Running,revision:0,created_at_ms:now,updated_at_ms:now,parent:None,task_id:id.clone(),failure_origin:None};
    let compensators=frozen.definition.nodes.iter().filter_map(|n|n.compensation.as_ref()).collect::<BTreeSet<_>>();let mut records=Vec::new();
    for def in &frozen.definition.nodes {let input_digest=digest(&serde_json::json!([instance.input,def.input]))?;records.push(WorkflowNodeRecord{key:WorkflowNodeKey{scope:scope.clone(),instance_id:id.clone(),node_id:def.id.clone(),occurrence:0},status:if compensators.contains(&def.id){WorkflowNodeStatus::Skipped}else{WorkflowNodeStatus::Pending},input_digest,attempt:0,revision:0,lease_epoch:0,executor_id:None,expires_at_ms:0,deadline_ms:None,native_run_id:None,native_session_key:None,child_workflow_id:None,output:None,artifacts:vec![],evidence:None,dispatch_started:false,compensation_order:None});}
    Ok((instance,records))
}
pub fn lease_fence(node:&WorkflowNodeRecord,lease:&WorkflowNodeLease,now:i64)->Result<(),CoordinationError>{if node.key!=lease.key || node.lease_epoch!=lease.epoch || node.executor_id.as_deref()!=Some(&lease.executor_id) || node.expires_at_ms<=now || !matches!(node.status,WorkflowNodeStatus::Claimed|WorkflowNodeStatus::WaitingNative|WorkflowNodeStatus::Compensating){return Err(CoordinationError::StaleLease)}Ok(())}
pub fn claim_node(instance:&WorkflowInstance,def:&WorkflowNodeDefinition,node:&mut WorkflowNodeRecord,executor:&str,now:i64)->Result<WorkflowClaim,CoordinationError>{
    if !valid_id(executor) || (instance.status!=WorkflowStatus::Running&&!(instance.status==WorkflowStatus::Cancelling&&node.status==WorkflowNodeStatus::WaitingNative)&&!(instance.status==WorkflowStatus::Compensating&&node.key.occurrence>0)) || !matches!(node.status,WorkflowNodeStatus::Pending|WorkflowNodeStatus::WaitingNative) || node.expires_at_ms>now{return Err(CoordinationError::Conflict)}
    node.lease_epoch=node.lease_epoch.checked_add(1).filter(|v|*v<=i64::MAX as u64).ok_or(CoordinationError::InvalidRequest)?;node.executor_id=Some(executor.into());node.expires_at_ms=now.checked_add(30000).ok_or(CoordinationError::InvalidRequest)?;
    if node.status==WorkflowNodeStatus::Pending {if node.attempt>=def.max_attempts{return Err(CoordinationError::InvalidRequest)}node.attempt+=1;node.status=WorkflowNodeStatus::Claimed;node.deadline_ms=Some(now.checked_add(def.timeout_ms as i64).ok_or(CoordinationError::InvalidRequest)?);}
    node.revision=node.revision.checked_add(1).ok_or(CoordinationError::InvalidRequest)?;
    let operation_id=format!("workflow-native:{}:{}:{}:{}",node.key.instance_id,node.key.node_id,node.key.occurrence,node.attempt);
    Ok(WorkflowClaim{lease:WorkflowNodeLease{key:node.key.clone(),epoch:node.lease_epoch,executor_id:executor.into(),operation_id},instance:instance.clone(),definition:def.clone(),node:node.clone()})
}
pub fn control(instance:&mut WorkflowInstance,c:&WorkflowControl,now:i64)->Result<(),CoordinationError>{
    if c.operation_id.is_empty()||c.operation_id.len()>256||instance.id!=c.instance_id||instance.revision!=c.expected_revision{return Err(CoordinationError::Conflict)}
    instance.status=match(c.action.clone(),instance.status){(WorkflowControlAction::Pause,WorkflowStatus::Running)=>WorkflowStatus::Paused,(WorkflowControlAction::Resume,WorkflowStatus::Paused)=>WorkflowStatus::Running,(WorkflowControlAction::Cancel,WorkflowStatus::Running|WorkflowStatus::Paused)=>WorkflowStatus::Cancelling,_=>return Err(CoordinationError::Conflict)};instance.revision=instance.revision.checked_add(1).ok_or(CoordinationError::InvalidRequest)?;instance.updated_at_ms=now;Ok(())
}

/// 两个适配器共享此调度规则；数据库负责原事务、时钟、作用域和CAS。
pub fn advance(instance:&mut WorkflowInstance,frozen:&FrozenWorkflowDefinition,nodes:&mut Vec<WorkflowNodeRecord>,executor:&str,limit:usize,now:i64)->Result<Vec<WorkflowClaim>,CoordinationError>{
    if limit==0||limit>32||!valid_id(executor){return Err(CoordinationError::InvalidRequest)}
    let mut claims=Vec::new();
    for node in nodes.iter_mut(){let before=node.clone();expire(node,now);if node!=&before{node.revision=node.revision.checked_add(1).ok_or(CoordinationError::InvalidRequest)?;}}
    settle(instance,frozen,nodes)?;
    if instance.status==WorkflowStatus::Cancelling {for node in nodes.iter_mut(){if matches!(node.status,WorkflowNodeStatus::Pending|WorkflowNodeStatus::WaitingUntil){node.status=WorkflowNodeStatus::Cancelled;node.revision+=1;}}}
    if instance.status==WorkflowStatus::Running {
        // 按拓扑依赖推进纯节点，最多nodes.len()轮，不阻塞线程等待WaitUntil。
        for _ in 0..nodes.len(){let mut changed=false;for def in &frozen.definition.nodes {
            let i=nodes.iter().position(|n|n.key.node_id==def.id&&n.key.occurrence==0).ok_or(CoordinationError::Corrupt)?;
            if nodes[i].status!=WorkflowNodeStatus::Pending || !ready(def,nodes){continue}
            if false_branch(def,nodes,frozen){nodes[i].status=WorkflowNodeStatus::Skipped;nodes[i].revision+=1;changed=true;continue}
            if is_pure(&def.kind){let input=serde_json::json!({"workflow":instance.input,"node":def.input,"dependencies":def.dependencies.iter().map(|d|(d.clone(),nodes.iter().find(|n|n.key.node_id==*d&&n.key.occurrence==0).and_then(|n|n.output.clone()).unwrap_or_default())).collect::<BTreeMap<_,_>>()});if let Some(output)=pure_result(&def.kind,&input,now)?{nodes[i].status=WorkflowNodeStatus::Completed;nodes[i].output=Some(output);nodes[i].attempt=1;nodes[i].revision+=1;changed=true;}}
        }if !changed{break}}
    }
    if matches!(instance.status,WorkflowStatus::Running|WorkflowStatus::Cancelling){
        for def in &frozen.definition.nodes {
            if claims.len()>=limit{break}let i=nodes.iter().position(|n|n.key.node_id==def.id&&n.key.occurrence==0).ok_or(CoordinationError::Corrupt)?;
            if is_pure(&def.kind) || !matches!(nodes[i].status,WorkflowNodeStatus::Pending|WorkflowNodeStatus::WaitingNative) || nodes[i].expires_at_ms>now || (instance.status==WorkflowStatus::Cancelling&&nodes[i].status==WorkflowNodeStatus::Pending) || (instance.status!=WorkflowStatus::Cancelling&&!ready(def,nodes)){continue}
            claims.push(claim_node(instance,def,&mut nodes[i],executor,now)?);
        }
    }
    if instance.status==WorkflowStatus::Compensating{let mut ordered=nodes.iter().filter_map(|n|n.compensation_order.map(|order|(order,n.key.clone()))).collect::<Vec<_>>();ordered.sort_by_key(|v|v.0);for(_,key) in ordered{let i=nodes.iter().position(|n|n.key==key).ok_or(CoordinationError::Corrupt)?;if nodes[i].status==WorkflowNodeStatus::Compensated{continue}if claims.len()<limit&&matches!(nodes[i].status,WorkflowNodeStatus::Pending|WorkflowNodeStatus::WaitingNative)&&nodes[i].expires_at_ms<=now{let def=frozen.definition.nodes.iter().find(|d|d.id==key.node_id).ok_or(CoordinationError::Corrupt)?;if is_pure(&def.kind){if let Some(output)=pure_result(&def.kind,&serde_json::json!({"workflow":instance.input,"node":def.input}),now)?{nodes[i].status=WorkflowNodeStatus::Compensated;nodes[i].output=Some(output);nodes[i].revision+=1;}}else{claims.push(claim_node(instance,def,&mut nodes[i],executor,now)?);}}break;}}
    settle(instance,frozen,nodes)?;Ok(claims)
}
pub fn finish(def:&WorkflowNodeDefinition,node:&mut WorkflowNodeRecord,lease:&WorkflowNodeLease,result:&WorkflowNodeResult,now:i64)->Result<(),CoordinationError>{
    lease_fence(node,lease,now)?;
    match result {
        WorkflowNodeResult::Completed{output,artifacts}=>{if serde_json::to_vec(output).map_err(|_|CoordinationError::InvalidRequest)?.len()>8192 || artifacts.len()>16{return Err(CoordinationError::InvalidRequest)}node.output=Some(output.clone());node.artifacts=artifacts.clone();node.status=if node.key.occurrence>0{WorkflowNodeStatus::Compensated}else{WorkflowNodeStatus::Completed};},
        WorkflowNodeResult::Failed{code,evidence,confirmed_no_effect}=>{if code.is_empty()||code.len()>128||evidence.is_empty()||evidence.len()>2048{return Err(CoordinationError::InvalidRequest)}node.evidence=Some(format!("{code}: {evidence}"));node.status=if may_retry(def,node,*confirmed_no_effect){WorkflowNodeStatus::Pending}else{WorkflowNodeStatus::Failed};},
        WorkflowNodeResult::Cancelled=>{node.status=WorkflowNodeStatus::Cancelled;},
        WorkflowNodeResult::Unknown{evidence}=>{if evidence.is_empty()||evidence.len()>2048{return Err(CoordinationError::InvalidRequest)}node.evidence=Some(evidence.clone());node.status=WorkflowNodeStatus::Unknown;},
        // Native链接只能由原run准入事务写入，不能用独立finish制造派发收据。
        WorkflowNodeResult::Native{..}=>return Err(CoordinationError::InvalidRequest),
        WorkflowNodeResult::SubWorkflow{instance_id}=>{if !valid_id(instance_id){return Err(CoordinationError::InvalidRequest)}node.child_workflow_id=Some(instance_id.clone());node.dispatch_started=true;node.status=WorkflowNodeStatus::WaitingNative;},
    }
    node.executor_id=None;node.expires_at_ms=0;node.revision=node.revision.checked_add(1).ok_or(CoordinationError::InvalidRequest)?;Ok(())
}
pub fn validate_state(instance:&WorkflowInstance,frozen:&FrozenWorkflowDefinition,nodes:&[WorkflowNodeRecord],scope:&TenantContext)->Result<(),CoordinationError>{
    if instance.scope!=*scope||instance.definition_digest!=frozen.digest||digest(&instance.input)?!=instance.input_digest||freeze(&frozen.definition)?.digest!=frozen.digest||nodes.len()>512{return Err(CoordinationError::Corrupt)}
    let mut keys=BTreeSet::new();for n in nodes{let def=frozen.definition.nodes.iter().find(|d|d.id==n.key.node_id).ok_or(CoordinationError::Corrupt)?;if n.key.scope!=*scope||n.key.instance_id!=instance.id||!keys.insert((n.key.node_id.clone(),n.key.occurrence))||n.input_digest!=digest(&serde_json::json!([instance.input,def.input]))?||n.lease_epoch>i64::MAX as u64||n.revision>i64::MAX as u64{return Err(CoordinationError::Corrupt)}}
    if frozen.definition.nodes.iter().any(|d|!keys.contains(&(d.id.clone(),0))){return Err(CoordinationError::Corrupt)}Ok(())
}
pub fn operation_digest<T:serde::Serialize>(kind:&str,command:&T)->Result<String,CoordinationError>{digest(&serde_json::json!([kind,command]))}
pub fn renew(node:&mut WorkflowNodeRecord,lease:&WorkflowNodeLease,now:i64)->Result<(),CoordinationError>{lease_fence(node,lease,now)?;node.expires_at_ms=now.checked_add(30000).ok_or(CoordinationError::InvalidRequest)?;node.revision=node.revision.checked_add(1).ok_or(CoordinationError::InvalidRequest)?;Ok(())}

pub mod persistence;
pub fn native_session_key(key:&WorkflowNodeKey)->Result<SessionKey,CoordinationError>{Ok(SessionKey(format!("session-{}.jsonl",digest(&serde_json::json!(key))?)))}
pub fn native_input(def:&WorkflowNodeDefinition)->Result<String,CoordinationError>{match &def.kind{WorkflowNodeKind::AgentRun{message}=>Ok(message.clone()),WorkflowNodeKind::Approval{prompt}=>Ok(prompt.clone()),WorkflowNodeKind::ToolCall{call}=>Ok(format!("workflow tool {}",serde_json::to_string(call).map_err(|_|CoordinationError::InvalidRequest)?)),WorkflowNodeKind::Webhook{..}=>Ok(format!("workflow webhook {}",digest(&serde_json::json!(def.kind))?)),_=>Err(CoordinationError::InvalidRequest)}}
pub fn bind_native(instance:&WorkflowInstance,frozen:&FrozenWorkflowDefinition,node:&mut WorkflowNodeRecord,lease:&WorkflowNodeLease,admission:&RunAdmission,principal:&AuthenticatedPrincipal,snapshot:&RunSnapshot,run:Option<&RunId>,now:i64)->Result<(),CoordinationError>{
    lease_fence(node,lease,now)?;let def=frozen.definition.nodes.iter().find(|d|d.id==node.key.node_id).ok_or(CoordinationError::Corrupt)?;
    if snapshot.workflow.as_ref()!=Some(&WorkflowNativeDirective{key:node.key.clone(),definition:def.clone()})||!matches!(instance.status,WorkflowStatus::Running|WorkflowStatus::Compensating)||instance.creator!=*principal||node.status!=WorkflowNodeStatus::Claimed||node.dispatch_started||node.native_run_id.is_some()||admission.plan_execution.is_some()||admission.input!=native_input_for(def,&instance.input)?||node.native_session_key.as_ref()!=Some(&admission.session_key)||serde_json::to_value(&admission.request_id).map_err(|_|CoordinationError::InvalidRequest)?!=serde_json::json!(lease.operation_id)||lease.operation_id!=format!("workflow-native:{}:{}:{}:{}",node.key.instance_id,node.key.node_id,node.key.occurrence,node.attempt){return Err(CoordinationError::Conflict)}
    if let Some(run)=run {node.native_run_id=Some(run.clone());node.native_session_key=Some(admission.session_key.clone());node.dispatch_started=true;node.status=WorkflowNodeStatus::WaitingNative;node.revision=node.revision.checked_add(1).ok_or(CoordinationError::Corrupt)?;}Ok(())
}
pub fn native_output(content:&str)->Result<serde_json::Value,CoordinationError>{let mut end=content.len().min(4096);while !content.is_char_boundary(end){end-=1}Ok(serde_json::json!({"content":&content[..end],"content_bytes":content.len(),"content_digest":digest(&serde_json::json!(content))?}))}

pub fn message_definition()->WorkflowDefinition{WorkflowDefinition{schema_version:1,name:"builtin-a2a-message".into(),version:"1.0.0".into(),nodes:vec![WorkflowNodeDefinition{id:"agent".into(),dependencies:vec![],kind:WorkflowNodeKind::AgentRun{message:"${workflow.message}".into()},input:serde_json::json!({}),retry:WorkflowRetryClass::Never,max_attempts:1,timeout_ms:240000,compensation:None}]}}
pub fn native_input_for(def:&WorkflowNodeDefinition,input:&serde_json::Value)->Result<String,CoordinationError>{if matches!(&def.kind,WorkflowNodeKind::AgentRun{message} if message=="${workflow.message}"){let message=input.get("message").and_then(|v|v.as_str()).filter(|m|!m.trim().is_empty()&&m.len()<=32768).ok_or(CoordinationError::InvalidRequest)?;Ok(message.into())}else{native_input(def)}}

/// 仅补偿已确认完成的节点；原输出和原run收据不被改写。未知节点需要证据化reconcile。
pub fn settle(instance:&mut WorkflowInstance,frozen:&FrozenWorkflowDefinition,nodes:&mut Vec<WorkflowNodeRecord>)->Result<(),CoordinationError>{
 if nodes.iter().any(|n|n.status==WorkflowNodeStatus::Unknown){instance.status=WorkflowStatus::Unknown;return Ok(())}
 if instance.status==WorkflowStatus::Compensating {let compensations=nodes.iter().filter(|n|n.key.occurrence>0).collect::<Vec<_>>();if compensations.iter().any(|n|matches!(n.status,WorkflowNodeStatus::Failed|WorkflowNodeStatus::Cancelled)){instance.status=WorkflowStatus::Failed;}else if compensations.iter().all(|n|n.status==WorkflowNodeStatus::Compensated){instance.status=instance.failure_origin.unwrap_or(WorkflowStatus::Failed);}return Ok(())}
 if nodes.iter().any(|n|n.status==WorkflowNodeStatus::Failed)&&!matches!(instance.status,WorkflowStatus::Failed|WorkflowStatus::Cancelled){instance.failure_origin=Some(WorkflowStatus::Failed);instance.status=WorkflowStatus::Cancelling;}
 if instance.status==WorkflowStatus::Cancelling && nodes.iter().all(|n|terminal(n.status)) {
  let mut order=Vec::new();let mut seen=BTreeSet::new();while seen.len()<frozen.definition.nodes.len(){let before=seen.len();for def in &frozen.definition.nodes{if !seen.contains(&def.id)&&def.dependencies.iter().all(|d|seen.contains(d)){seen.insert(def.id.clone());order.push(def);}}if before==seen.len(){return Err(CoordinationError::Corrupt)}}
  for original in order.into_iter().rev(){if let Some(target)=&original.compensation{if nodes.iter().any(|n|n.key.node_id==original.id&&n.key.occurrence==0&&n.status==WorkflowNodeStatus::Completed){if nodes.iter().any(|n|n.key.node_id==*target&&n.key.occurrence==1){continue}let mut compensation=nodes.iter().find(|n|n.key.node_id==*target&&n.key.occurrence==0).ok_or(CoordinationError::Corrupt)?.clone();compensation.key.occurrence=1;compensation.status=WorkflowNodeStatus::Pending;compensation.compensation_order=Some(nodes.iter().filter(|n|n.key.occurrence>0).count() as u32);nodes.push(compensation);}}}
  instance.status=if nodes.iter().any(|n|n.key.occurrence>0){WorkflowStatus::Compensating}else{instance.failure_origin.unwrap_or(WorkflowStatus::Cancelled)};
 }else if instance.status==WorkflowStatus::Running&&nodes.iter().all(|n|matches!(n.status,WorkflowNodeStatus::Completed|WorkflowNodeStatus::Skipped)){instance.status=WorkflowStatus::Completed;}Ok(())
}
