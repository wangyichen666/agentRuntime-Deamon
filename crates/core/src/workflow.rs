//! 工作流是长流程编排事实，只引用原native run/tool/interaction/artifact，不复制它们的终态。
use crate::*;
use serde::{Deserialize,Serialize};
use serde_json::Value;
#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
#[serde(tag="kind",rename_all="snake_case",deny_unknown_fields)]
pub enum WorkflowNodeKind {
    AgentRun {message:String},ToolCall {call:ToolCall},Approval {prompt:String},
    Condition {path:Vec<String>,equals:Value},Parallel,Join,WaitUntil {timestamp_ms:i64},
    Webhook {url:String,body:Value,idempotency_key:Option<String>},
    SubWorkflow {definition_digest:String,input:Value},
}
#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
#[serde(tag="class",rename_all="snake_case",deny_unknown_fields)]
pub enum WorkflowRetryClass {Pure,Idempotent {key:String},Never}
#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowNodeDefinition {
    pub id:String,pub dependencies:Vec<String>,pub kind:WorkflowNodeKind,pub input:Value,
    pub retry:WorkflowRetryClass,pub max_attempts:u32,pub timeout_ms:u64,pub compensation:Option<String>,
}
#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowDefinition {pub schema_version:u32,pub name:String,pub version:String,pub nodes:Vec<WorkflowNodeDefinition>}
#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenWorkflowDefinition {pub digest:String,pub definition:WorkflowDefinition}
#[derive(Clone,Copy,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="snake_case")]
pub enum WorkflowStatus {Running,Paused,Cancelling,Compensating,Cancelled,Completed,Failed,Unknown}
#[derive(Clone,Copy,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="snake_case")]
pub enum WorkflowNodeStatus {Pending,Claimed,WaitingNative,WaitingUntil,Completed,Failed,Unknown,Cancelled,Compensating,Compensated,Skipped}
#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowInstance {
    pub id:String,pub scope:TenantContext,pub definition_digest:String,pub input:Value,pub input_digest:String,
    pub creator:AuthenticatedPrincipal,pub status:WorkflowStatus,pub revision:u64,pub created_at_ms:i64,pub updated_at_ms:i64,
    pub parent:Option<WorkflowNodeKey>,pub task_id:String,
    #[serde(default,skip_serializing_if="Option::is_none")] pub failure_origin:Option<WorkflowStatus>,
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowNodeKey {pub scope:TenantContext,pub instance_id:String,pub node_id:String,pub occurrence:u32}
#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowNodeRecord {
    pub key:WorkflowNodeKey,pub status:WorkflowNodeStatus,pub input_digest:String,pub attempt:u32,
    pub revision:u64,pub lease_epoch:u64,pub executor_id:Option<String>,pub expires_at_ms:i64,
    pub deadline_ms:Option<i64>,pub native_run_id:Option<RunId>,pub native_session_key:Option<SessionKey>,
    pub child_workflow_id:Option<String>,pub output:Option<Value>,pub artifacts:Vec<WorkflowArtifactRef>,
    pub evidence:Option<String>,pub dispatch_started:bool,
    #[serde(default,skip_serializing_if="Option::is_none")] pub compensation_order:Option<u32>,
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowNodeLease {pub key:WorkflowNodeKey,pub epoch:u64,pub executor_id:String,pub operation_id:String}
#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowClaim {pub lease:WorkflowNodeLease,pub instance:WorkflowInstance,pub definition:WorkflowNodeDefinition,pub node:WorkflowNodeRecord}
#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowEvent {pub cursor:u64,pub instance_id:String,pub node_id:Option<String>,pub kind:String,pub revision:u64,pub digest:String,pub timestamp_ms:i64}
#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowReadback {pub instance:WorkflowInstance,pub definition:FrozenWorkflowDefinition,pub nodes:Vec<WorkflowNodeRecord>,pub events:Vec<WorkflowEvent>,pub next_cursor:u64}
#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowPublish {pub operation_id:String,pub definition:WorkflowDefinition}
#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowStart {pub operation_id:String,pub definition_digest:String,pub input:Value}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="snake_case")]
pub enum WorkflowControlAction {Pause,Resume,Cancel}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowControl {pub operation_id:String,pub instance_id:String,pub expected_revision:u64,pub action:WorkflowControlAction}
#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
#[serde(tag="outcome",rename_all="snake_case",deny_unknown_fields)]
pub enum WorkflowNodeResult {
    Completed {output:Value,artifacts:Vec<WorkflowArtifactRef>},
    Failed {code:String,evidence:String,confirmed_no_effect:bool},
    Unknown {evidence:String},Cancelled,
    Native {run_id:RunId,session_key:SessionKey},SubWorkflow {instance_id:String},
}

#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowArtifactRef {pub owner:EnterpriseOwnerEnvelope,pub round:u32,pub call_id:String,pub reference:String,pub identity:ArtifactIdentity}
#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowNativeDirective {pub key:WorkflowNodeKey,pub definition:WorkflowNodeDefinition}
#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowMessage {pub operation_id:String,pub message_id:String,pub message:String}
