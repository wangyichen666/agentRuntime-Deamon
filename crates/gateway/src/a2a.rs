//! A2A 0.3 JSON-RPC/SSE投影；task事实全部来自原workflow端口。
use super::*;
use serde::Deserialize;
use agent_core::{WorkflowReadback,WorkflowStatus,WorkflowNodeStatus};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Request {jsonrpc:String,id:Value,method:String,params:Value}
#[derive(Deserialize)]
#[serde(rename_all="camelCase",deny_unknown_fields)]
struct Send {message:Message,#[serde(default)] configuration:Option<Configuration>,#[serde(default)] metadata:Option<Value>}
#[derive(Deserialize)]
#[serde(rename_all="camelCase",deny_unknown_fields)]
struct Message {#[serde(default)] kind:Option<String>,role:String,parts:Vec<Part>,message_id:String,#[serde(default)] context_id:Option<String>,#[serde(default)] task_id:Option<String>,#[serde(default)] metadata:Option<Value>,#[serde(default)] reference_task_ids:Vec<String>}
#[derive(Deserialize)]
#[serde(tag="kind",rename_all="snake_case",deny_unknown_fields)]
enum Part {Text{text:String,#[serde(default)] metadata:Option<Value>}}
#[derive(Deserialize,Default)]
#[serde(rename_all="camelCase",deny_unknown_fields)]
struct Configuration {#[serde(default)] accepted_output_modes:Vec<String>,#[serde(default)] history_length:Option<u32>,#[serde(default)] blocking:bool,#[serde(default)] push_notification_config:Option<Value>}
#[derive(Deserialize)]
#[serde(rename_all="camelCase",deny_unknown_fields)]
struct TaskQuery {id:String,#[serde(default)] history_length:Option<u32>,#[serde(default)] metadata:Option<Value>}
fn safe_id(id:&str)->bool{!id.is_empty()&&id.len()<=128&&id.bytes().all(|c|c.is_ascii_alphanumeric()||matches!(c,b'-'|b'_'))}
fn reply(id:&Value,result:Value)->Value{json!({"jsonrpc":"2.0","id":id,"result":result})}
fn error(id:&Value,code:i64,message:&str)->Value{json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})}
fn status(instance:&agent_core::WorkflowInstance)->Value{json!({"state":match instance.status{WorkflowStatus::Running|WorkflowStatus::Paused|WorkflowStatus::Cancelling|WorkflowStatus::Compensating=>"working",WorkflowStatus::Cancelled=>"canceled",WorkflowStatus::Completed=>"completed",WorkflowStatus::Failed=>"failed",WorkflowStatus::Unknown=>"unknown"}})}
fn final_state(status:WorkflowStatus)->bool{matches!(status,WorkflowStatus::Cancelled|WorkflowStatus::Completed|WorkflowStatus::Failed|WorkflowStatus::Unknown)}
fn projection(read:&WorkflowReadback,history:u32)->Value{
 let id=&read.instance.task_id;let mut messages=Vec::new();if let Some(text)=read.instance.input.get("message").and_then(Value::as_str){messages.push(json!({"kind":"message","role":"user","messageId":read.instance.input.get("message_id"),"taskId":id,"contextId":id,"parts":[{"kind":"text","text":text}]}));}
 let mut artifacts=Vec::new();for node in &read.nodes {if matches!(node.status,WorkflowNodeStatus::Completed|WorkflowNodeStatus::Compensated){if let Some(output)=&node.output{let content=output.get("content").and_then(Value::as_str).unwrap_or("");if !content.is_empty(){let artifact_id=format!("{}-{}",node.key.node_id,node.key.occurrence);artifacts.push(json!({"artifactId":artifact_id,"name":"已确认输出","parts":[{"kind":"text","text":content}],"metadata":{"contentDigest":output.get("content_digest"),"contentBytes":output.get("content_bytes")}}));messages.push(json!({"kind":"message","role":"agent","messageId":format!("{}-{}",id,node.key.node_id),"taskId":id,"contextId":id,"parts":[{"kind":"text","text":content}]}));}}}}
 if messages.len()>history as usize {messages.drain(..messages.len()-history as usize);}
 json!({"kind":"task","id":id,"contextId":id,"status":status(&read.instance),"history":messages,"artifacts":artifacts,"metadata":{"revision":read.instance.revision,"eventCursor":read.next_cursor,"paused":read.instance.status==WorkflowStatus::Paused,"cancelPending":read.instance.status==WorkflowStatus::Cancelling,"stateHistory":read.events.iter().map(|e|json!({"cursor":e.cursor,"kind":e.kind,"revision":e.revision,"digest":e.digest,"timestampMs":e.timestamp_ms})).collect::<Vec<_>>()}})
}
async fn read(client:&DaemonClient,id:&str)->std::result::Result<WorkflowReadback,()>{if !safe_id(id){return Err(())}let value=client.request_result("workflow.read",json!({"id":id,"after":0,"limit":128})).await.map_err(|_|())?;serde_json::from_value(value).map_err(|_|())}
pub(super) async fn card(ScopeState(state):ScopeState)->Response{
 let Some(url)=&state.a2a_url else{return api_error(StatusCode::SERVICE_UNAVAILABLE,"需要配置受信任的A2A公开URL")};let mut card=json!({"protocolVersion":"0.3.0","name":"my-agent","description":"持久企业任务与原生审批","url":url,"preferredTransport":"JSONRPC","version":env!("CARGO_PKG_VERSION"),"capabilities":{"streaming":true,"pushNotifications":false,"stateTransitionHistory":true},"defaultInputModes":["text/plain"],"defaultOutputModes":["text/plain"],"skills":[{"id":"agent-task","name":"持久Agent任务","description":"以用户文本启动原native AgentRun，按稳定task id读回与取消","tags":["workflow","agent"],"inputModes":["text/plain"],"outputModes":["text/plain"]}]});
 if state.oidc.is_some()||state.bearer_token.is_some(){card["securitySchemes"]=json!({"bearer":{"type":"http","scheme":"bearer"}});card["security"]=json!([{"bearer":[]}]);}let mut response=Json(card).into_response();response.headers_mut().insert(axum::http::header::CACHE_CONTROL,axum::http::HeaderValue::from_static("no-store"));response
}
pub(super) async fn rpc(ScopeState(state):ScopeState,headers:HeaderMap,Json(request):Json<Request>)->Response{
 let id=&request.id;if request.jsonrpc!="2.0"||!(id.is_string()||id.is_i64()||id.is_u64())||id.as_str().is_some_and(|s|s.len()>128){return Json(error(id,-32600,"请求格式无效")).into_response()}
 if !is_authorized(&headers,state.bearer_token.as_deref()){return api_error(StatusCode::UNAUTHORIZED,"身份认证失败")}
 let client=match state.client_for(None).await{Ok((_,client))=>client,Err(_)=>return Json(error(id,-32603,"执行端点不可用")).into_response()};
 match request.method.as_str(){
  "message/send"|"message/stream"=>{
   let send:Send=match serde_json::from_value(request.params){Ok(v)=>v,Err(_)=>return Json(error(id,-32005,"仅支持用户text/plain输入")).into_response()};let config=send.configuration.unwrap_or_default();let message=send.message;
   if message.role!="user"||message.kind.as_deref().is_some_and(|k|k!="message")||!safe_id(&message.message_id)||message.parts.is_empty()||message.parts.len()>32||message.context_id.is_some()||!message.reference_task_ids.is_empty()||message.metadata.as_ref().is_some_and(|v|v!=&json!({}))||send.metadata.as_ref().is_some_and(|v|v!=&json!({}))||config.history_length.is_some_and(|n|n>128){return Json(error(id,-32602,"消息边界无效；不接受系统角色、跨任务引用或配置覆盖")).into_response()}
   if message.task_id.is_some(){return Json(error(id,-32004,"追加输入请使用原interaction的owner与revision接口")).into_response()}
   if config.push_notification_config.is_some(){return Json(error(id,-32003,"未提供push通知能力")).into_response()}
   if config.accepted_output_modes.iter().any(|m|m!="text/plain"){return Json(error(id,-32005,"仅支持text/plain输出")).into_response()}
   let mut text=String::new();for part in message.parts {match part{Part::Text{text:part,metadata}=>{if metadata.as_ref().is_some_and(|v|v!=&json!({})){return Json(error(id,-32602,"不接受part配置覆盖")).into_response()}if text.len().saturating_add(part.len()).saturating_add(1)>32768{return Json(error(id,-32602,"文本超过预算")).into_response()}if !text.is_empty(){text.push('\n')}text.push_str(&part)}}}if text.trim().is_empty(){return Json(error(id,-32602,"文本为空")).into_response()}
   let value=match client.request_result("workflow.message",json!({"operation_id":format!("a2a:{}",message.message_id),"message_id":message.message_id,"message":text})).await{Ok(v)=>v,Err(_)=>return Json(error(id,-32603,"提交未获确认；使用同messageId重读")).into_response()};let Some(task)=value.get("task_id").and_then(Value::as_str).map(str::to_owned) else{return Json(error(id,-32603,"原任务响应无效")).into_response()};let mut current=match read(&client,&task).await{Ok(v)=>v,Err(_)=>return Json(error(id,-32603,"读回未确认")).into_response()};let history=config.history_length.unwrap_or(32);
   if request.method=="message/stream"{return stream_response(client,request.id,current,history)}
   if config.blocking{let deadline=tokio::time::Instant::now()+std::time::Duration::from_secs(25);while !final_state(current.instance.status)&&tokio::time::Instant::now()<deadline{tokio::time::sleep(std::time::Duration::from_millis(500)).await;current=match read(&client,&task).await{Ok(v)=>v,Err(_)=>break};}}
   Json(reply(id,projection(&current,history))).into_response()
  },
  "tasks/get"|"tasks/cancel"=>{let query:TaskQuery=match serde_json::from_value(request.params){Ok(v)=>v,Err(_)=>return Json(error(id,-32602,"任务参数无效")).into_response()};if !safe_id(&query.id)||query.history_length.is_some_and(|n|n>128)||query.metadata.as_ref().is_some_and(|v|v!=&json!({})){return Json(error(id,-32602,"任务边界无效")).into_response()}let mut current=match read(&client,&query.id).await{Ok(v)=>v,Err(_)=>return Json(error(id,-32001,"任务不可访问")).into_response()};if request.method=="tasks/cancel" {if final_state(current.instance.status){return Json(error(id,-32002,"任务已终止，不能取消")).into_response()}if current.instance.status!=WorkflowStatus::Cancelling{let command=json!({"operation_id":format!("a2a-cancel:{}:{}",query.id,current.instance.revision),"instance_id":query.id,"expected_revision":current.instance.revision,"action":"cancel"});if client.request_result("workflow.control",command).await.is_err(){return Json(error(id,-32603,"取消未获确认；先读回原任务")).into_response()}current=match read(&client,&query.id).await{Ok(v)=>v,Err(_)=>return Json(error(id,-32603,"取消读回未确认")).into_response()};}}
   Json(reply(id,projection(&current,query.history_length.unwrap_or(32)))).into_response()
  },_=>Json(error(id,-32601,"方法不存在")).into_response()
 }
}
#[derive(Clone)]
struct StreamState {client:DaemonClient,id:Value,current:WorkflowReadback,history:u32,first:bool,done:bool,deadline:tokio::time::Instant}
fn stream_response(client:DaemonClient,id:Value,current:WorkflowReadback,history:u32)->Response{
 let initial=StreamState{client,id,current,history,first:true,done:false,deadline:tokio::time::Instant::now()+std::time::Duration::from_secs(230)};
 let stream=futures_util::stream::unfold(initial,|mut s|async move{if s.done{return None}if s.first{s.first=false;s.done=final_state(s.current.instance.status);let event=axum::response::sse::Event::default().data(reply(&s.id,projection(&s.current,s.history)).to_string()).id(s.current.next_cursor.to_string());return Some((Ok::<_,Infallible>(event),s))}
 loop{if tokio::time::Instant::now()>=s.deadline{s.done=true;return Some((Ok(axum::response::sse::Event::default().data(error(&s.id,-32603,"流预算已到；任务继续，使用tasks/get读回").to_string())),s))}tokio::time::sleep(std::time::Duration::from_millis(500)).await;let next=match read(&s.client,&s.current.instance.id).await{Ok(v)=>v,Err(_)=>{s.done=true;return Some((Ok(axum::response::sse::Event::default().data(error(&s.id,-32603,"读回未确认，原任务保持持久状态").to_string())),s))}};if next.instance.revision==s.current.instance.revision&&next.next_cursor==s.current.next_cursor{continue}s.current=next;s.done=final_state(s.current.instance.status);let update=json!({"kind":"status-update","taskId":s.current.instance.task_id,"contextId":s.current.instance.task_id,"status":status(&s.current.instance),"final":s.done,"metadata":{"revision":s.current.instance.revision,"eventCursor":s.current.next_cursor}});return Some((Ok(axum::response::sse::Event::default().data(reply(&s.id,update).to_string()).id(s.current.next_cursor.to_string())),s))}});
 axum::response::Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::new().interval(std::time::Duration::from_secs(5))).into_response()
}
