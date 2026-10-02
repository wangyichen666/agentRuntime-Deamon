use agent_core::{Message, PendingApprovalInfo, RequestId, Role, SessionInfo, SessionStatus};
use serde_json::json;

#[test]
fn pre_workspace_jsonl_message_loads_without_new_fields() {
    let value = json!({"role":"user", "content":"旧数据", "tool_call_id":null, "name":null});
    let message: Message = serde_json::from_value(value).unwrap();
    assert_eq!(message, Message::text(Role::User, "旧数据"));
    assert!(message.thinking.is_none());
    assert!(message.image_urls.is_empty());
    assert!(message.tool_calls.is_empty());
}

#[test]
fn legacy_session_list_preserves_defaults_and_public_id() {
    let value = json!({"id":"旧-session.jsonl", "path":"/tmp/session.jsonl", "active":false,
        "message_count":3, "modified_at":null, "preview":"原始消息"});
    let info: SessionInfo = serde_json::from_value(value).unwrap();
    assert_eq!(info.id, "旧-session.jsonl");
    assert_eq!(info.status, SessionStatus::Idle);
    assert_eq!(info.active_requests, 0);
    assert!(info.updated_at.is_none());
    assert!(
        serde_json::to_value(info)
            .unwrap()
            .get("session_lifetime_id")
            .is_none()
    );
}

#[test]
fn pending_approval_and_request_ids_remain_compatible() {
    let approval: PendingApprovalInfo = serde_json::from_value(json!({
        "id":"approval-1", "request_id":"原请求", "prompt":"允许写入？"
    }))
    .unwrap();
    assert_eq!(approval.request_id, RequestId::String("原请求".into()));
    assert_eq!(
        serde_json::from_value::<RequestId>(json!(12)).unwrap(),
        RequestId::Number(12)
    );
    assert!(serde_json::from_value::<RequestId>(json!(-1)).is_err());
    assert!(serde_json::from_value::<RequestId>(json!(null)).is_err());
}
