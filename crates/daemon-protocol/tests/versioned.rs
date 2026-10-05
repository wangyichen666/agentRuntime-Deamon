use agent_daemon_protocol::{
    JsonRpcRequest, ProtocolError, RequestId, decode_request, normalize_request,
};
use serde_json::json;

#[test]
fn v1_rejects_unknown_envelope_and_params_before_dispatch() {
    assert!(matches!(decode_request(br#"{"jsonrpc":"2.0","protocol_version":1,"id":1,"method":"runs.read","extra":true,"params":{"run_id":"run-1"}}"#), Err(ProtocolError::InvalidParams(_))));
    let mut request = JsonRpcRequest::new(
        RequestId::Number(1),
        "runs.send",
        json!({"message":"输入","session_id":"s","silent_typo":true}),
    );
    request.protocol_version = Some(1);
    assert!(matches!(
        normalize_request(request),
        Err(ProtocolError::InvalidParams(_))
    ));
    let params = json!({"profile":{"api_type":"ollama","model":"mock","base_url":"http://localhost:11434","unknown":true}});
    let legacy = JsonRpcRequest::new(RequestId::Number(2), "models.save", params);
    assert!(normalize_request(legacy.clone()).is_ok());
    let mut strict = legacy;
    strict.protocol_version = Some(1);
    assert!(matches!(
        normalize_request(strict),
        Err(ProtocolError::InvalidParams(_))
    ));
}

#[test]
fn legacy_extensions_are_discarded_and_share_the_same_command() {
    let request = JsonRpcRequest::new(
        RequestId::Number(1),
        "runs.send",
        json!({"message":"输入","extension":true}),
    );
    let normalized = normalize_request(request).unwrap();
    assert_eq!(normalized.method, "chat.send");
    assert_eq!(normalized.params, json!({"message":"输入"}));
}

#[test]
fn unknown_protocol_version_fails_closed_for_wire_and_internal_callers() {
    assert!(matches!(
        decode_request(br#"{"jsonrpc":"2.0","protocol_version":2,"id":1,"method":"runs.send"}"#),
        Err(ProtocolError::UnsupportedProtocolVersion(2))
    ));
    let mut request = JsonRpcRequest::new(RequestId::Number(1), "session.new", json!({}));
    request.protocol_version = Some(99);
    assert!(matches!(
        normalize_request(request),
        Err(ProtocolError::UnsupportedProtocolVersion(99))
    ));
}

#[test]
fn versioned_response_and_event_envelopes_reject_unknown_fields() {
    use agent_daemon_protocol::decode_server_frame_for_version;
    for bytes in [
        br#"{"frame":"response","jsonrpc":"2.0","id":1,"result":{},"extension":true}"#.as_slice(),
        br#"{"frame":"response","jsonrpc":"2.0","id":1,"error":{"code":-1,"message":"error","extension":true}}"#.as_slice(),
        br#"{"frame":"event","jsonrpc":"2.0","request_id":1,"event":"turn_started","data":{},"extension":true}"#.as_slice(),
    ] {
        assert!(decode_server_frame_for_version(bytes, 0).is_ok());
        assert!(matches!(decode_server_frame_for_version(bytes, 1), Err(ProtocolError::InvalidParams(_))));
    }
}

#[test]
fn versioned_aliases_preserve_ids_and_interaction_owner_fields() {
    for (method, target, params) in [
        ("sessions.create", "session.new", json!({})),
        ("sessions.list", "session.list", json!({})),
        ("sessions.read", "session.load", json!({"session_id":"s"})),
        ("runs.read", "run.read", json!({"run_id":"r"})),
        (
            "runs.events",
            "run.events",
            json!({"run_id":"r","after_seq":4,"limit":10}),
        ),
        (
            "runs.cancel",
            "agent.cancel",
            json!({"session_id":"s","run_id":"r"}),
        ),
        (
            "interactions.respond",
            "interaction.respond",
            json!({"interaction_id":"i","session_id":"s","owner_run_id":"r","revision":2,"approved":true}),
        ),
    ] {
        let mut request =
            JsonRpcRequest::new(RequestId::String("稳定请求".into()), method, params.clone());
        request.protocol_version = Some(1);
        let normalized = normalize_request(request).unwrap();
        assert_eq!(normalized.id, RequestId::String("稳定请求".into()));
        assert_eq!(normalized.method, target);
        assert_eq!(normalized.params, params);
    }
}

#[test]
fn memory_feedback_requires_typed_user_vote_and_strict_fields() {
    let params = json!({"session_id":"s","owner_run_id":"r","operation_id":"vote","memory_id":"m","feedback":"helpful"});
    let mut request = JsonRpcRequest::new(RequestId::Number(1), "memory.feedback", params);
    request.protocol_version = Some(1);
    assert!(normalize_request(request.clone()).is_ok());
    request.params["feedback"] = json!("model_succeeded");
    assert!(normalize_request(request.clone()).is_err());
    request.params["feedback"] = json!("incorrect");
    request.params["confidence"] = json!(100);
    assert!(normalize_request(request).is_err());
}

#[test]
fn delegation_context_is_optional_and_strictly_typed() {
    let mut request = JsonRpcRequest::new(
        RequestId::Number(1),
        "spawn_subagent",
        json!({"parent_session_id":"s","parent_run_id":"r","spawn_key":"k","task":"核实"}),
    );
    request.protocol_version = Some(1);
    assert!(normalize_request(request.clone()).is_ok());
    request.params["context_source_ids"] = json!(["parent_input"]);
    assert!(normalize_request(request.clone()).is_ok());
    request.params["context_source_ids"] = json!("全部历史");
    assert!(normalize_request(request.clone()).is_err());
    request.params["context_source_ids"] = json!([]);
    request.params["inherit_all"] = json!(true);
    assert!(normalize_request(request).is_err());
}
