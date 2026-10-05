use serde::Deserialize;
use serde_json::Value;

use crate::{JsonRpcRequest, ProtocolError, params::*};

/// 别名只映射同一 handler，不能创建另一份状态 owner。
pub fn canonical_method(method: &str) -> &str {
    match method {
        "sessions.create" => "session.new",
        "sessions.delete" => "session.delete",
        "sessions.clear" | "sessions.reset" => "session.clear",
        "sessions.fork" => "session.fork",
        "sessions.compact" => "session.compact",
        "sessions.list" => "session.list",
        "sessions.read" => "session.load",
        "sessions.resume" => "session.resume",
        "runs.send" => "chat.send",
        "runs.read" => "run.read",
        "runs.events" => "run.events",
        "runs.cancel" => "agent.cancel",
        "interactions.list" => "interaction.list",
        "interactions.read" => "interaction.read",
        "interactions.respond" => "interaction.respond",
        "interactions.reject" => "interaction.reject",
        _ => method,
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyParams {}

fn validate<T: for<'de> Deserialize<'de>>(
    value: &mut Value,
    fields: &[&str],
    strict: bool,
) -> Result<(), ProtocolError> {
    if !strict && let Some(object) = value.as_object_mut() {
        object.retain(|key, _| fields.contains(&key.as_str()));
    }
    serde_json::from_value::<T>(value.clone())
        .map(|_| ())
        .map_err(|error| ProtocolError::InvalidParams(error.to_string()))
}

pub fn normalize_request(mut request: JsonRpcRequest) -> Result<JsonRpcRequest, ProtocolError> {
    if let Some(version) = request.protocol_version
        && version != 1
    {
        return Err(ProtocolError::UnsupportedProtocolVersion(version));
    }
    request.method = canonical_method(&request.method).to_owned();
    if request.protocol_version.is_none()
        && request.method == "session.new"
        && request.params.is_null()
    {
        request.params = serde_json::json!({});
    }
    let strict = request.protocol_version == Some(1);
    macro_rules! check {
        ($ty:ty, $($field:literal),* $(,)?) => {
            validate::<$ty>(&mut request.params, &[$($field),*], strict)?
        };
    }
    match request.method.as_str() {
        "chat.send" => check!(
            ChatSendParams,
            "message",
            "session_id",
            "admission_mode",
            "context_read_only",
            "sandbox"
        ),
        "artifacts.read" => check!(
            ArtifactReadParams,
            "session_id",
            "run_id",
            "artifact_ref",
            "offset",
            "limit"
        ),
        "resources.reconcile" => check!(
            ResourceReconcileParams,
            "session_id",
            "resource_id",
            "owner_run_id",
            "terminal_state",
            "evidence"
        ),
        "resources.list" | "resources.read" | "resources.logs" | "resources.wait"
        | "resources.stop" => check!(
            ResourceParams,
            "session_id",
            "resource_id",
            "owner_run_id",
            "after_cursor",
            "limit",
            "timeout_ms"
        ),
        "memory.store" => check!(
            MemoryStoreParams,
            "session_id",
            "owner_run_id",
            "operation_id",
            "content",
            "scope",
            "confirmed_by_user",
            "ttl_days"
        ),
        "memory.recall" | "memory.list" | "memory.scope" => {
            check!(MemoryReadParams, "session_id", "query", "after_id", "limit")
        }
        "memory.forget" => check!(
            MemoryForgetParams,
            "session_id",
            "owner_run_id",
            "memory_id",
            "revision"
        ),
        "runtime.doctor" => check!(EmptyParams,),
        "memory.feedback" => check!(
            MemoryFeedbackParams,
            "session_id",
            "owner_run_id",
            "operation_id",
            "memory_id",
            "feedback"
        ),
        "memory.flywheel" => check!(MemoryReadParams, "session_id", "query", "after_id", "limit"),
        "memory.evidence" => check!(
            MemoryEvidenceParams,
            "session_id",
            "memory_id",
            "after_source",
            "limit"
        ),
        "session.compact" => check!(
            SessionCompactParams,
            "session_id",
            "owner_run_id",
            "operation_id",
            "expected_revision"
        ),
        "session.new" => check!(SessionCreateParams, "session_id", "operation_id"),
        "session.delete" | "session.clear" => {
            check!(SessionEndParams, "session_id", "operation_id")
        }
        "session.fork" => check!(
            SessionForkParams,
            "session_id",
            "target_session_id",
            "operation_id",
            "expected_revision"
        ),
        "session.load" => check!(SessionSelectorParams, "session_id"),
        "session.resume" | "session.trace" | "interaction.list" | "queue.list" => {
            check!(SessionResumeParams, "session_id")
        }
        "session.load_page" | "session.trace_page" => {
            check!(SessionPageParams, "session_id", "offset", "limit")
        }
        "agent.subscribe" => check!(SubscribeParams, "request_id", "session_id", "after_seq"),
        "agent.cancel" => check!(CancelParams, "request_id", "run_id", "session_id"),
        "approval.respond" | "interaction.respond" | "interaction.reject" => check!(
            ApprovalRespondParams,
            "approval_id",
            "interaction_id",
            "approved",
            "session_id",
            "owner_run_id",
            "revision"
        ),
        "interaction.read" => check!(InteractionReadParams, "interaction_id"),
        "run.read" | "run.tools" | "run.audit" | "run.provider_attempts" => {
            check!(RunReadParams, "run_id")
        }
        "run.events" => check!(RunEventsParams, "run_id", "after_seq", "limit"),
        "run.reconcile" => check!(
            RunReconcileParams,
            "session_id",
            "run_id",
            "expected_last_seq",
            "status",
            "content",
            "evidence"
        ),
        "queue.read" | "queue.remove" => check!(QueueItemParams, "session_id", "run_id"),
        "spawn_subagent" => check!(
            SpawnSubagentParams,
            "parent_session_id",
            "parent_run_id",
            "spawn_key",
            "task",
            "context_source_ids",
            "tools",
            "max_rounds",
            "max_tokens",
            "max_tool_calls",
            "timeout_ms"
        ),
        "list_subagents" => check!(SubagentListParams, "root_run_id"),
        "read_subagent" | "cancel_subagent" => {
            check!(SubagentScopeParams, "parent_run_id", "child_run_id")
        }
        "wait_subagents" => check!(
            WaitSubagentsParams,
            "parent_run_id",
            "child_run_ids",
            "timeout_ms",
            "after_seq"
        ),
        "subagent.result.reserve" | "subagent.result.release" | "subagent.result.commit" => check!(
            SubagentResultParams,
            "parent_run_id",
            "child_run_id",
            "owner",
            "revision"
        ),
        "permissions.set" => check!(PermissionModeParams, "mode"),
        "models.use" => check!(ModelUseParams, "profile_id"),
        "models.save" => {
            if strict {
                check!(ModelSaveParams<ModelProfileParams>, "profile", "activate");
            } else {
                check!(ModelSaveParams<Value>, "profile", "activate");
            }
        }
        "slash.execute" => check!(SlashExecuteParams, "line", "session_id"),
        "session.list" => {
            if !strict && request.params.is_null() {
                request.params = serde_json::json!({});
            }
            check!(SessionListParams, "after_id", "limit");
        }
        "permissions.get" | "models.list" | "daemon.stop" | "ping" => {
            if !strict && request.params.is_null() {
                request.params = serde_json::json!({});
            }
            check!(EmptyParams,);
        }
        _ => {}
    }
    Ok(request)
}
