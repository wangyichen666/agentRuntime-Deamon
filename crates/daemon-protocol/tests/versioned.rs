use agent_daemon_protocol::{
    JsonRpcRequest, ProtocolError, RequestId, decode_request, normalize_request,
};
use serde_json::json;

#[test]
fn native_compact_readback_binds_source_kind_owner_and_terminal_strictly() {
    let mut value = json!({"kind":"compact","run_id":"r","turn_id":"t","session_id":"s","request_id":"compact:op","status":"cancelled","last_seq":3,"content":null,"error_code":-32800,"error_message":"已取消","compact":{"schema_version":1,"owner":{"session_key":"s","session_lifetime_id":"life","run_id":"r","run_generation":1,"turn_id":"t"},"operation_id":"op","source":{"lifetime":"life","source_start":0,"source_end":8,"prefix_digest":"digest","generation":0,"policy_fingerprint":"policy","pressure_route":"model","summary_route":"model"},"outcome":"cancelled","result_generation":0}});
    assert!(agent_daemon_protocol::decode_run_readback(value.clone()).is_ok());
    for (path, wrong) in [("kind", json!("chat")), ("status", json!("completed"))] {
        let mut invalid = value.clone();
        invalid[path] = wrong;
        assert!(agent_daemon_protocol::decode_run_readback(invalid).is_err());
    }
    value["compact"]["owner"]["run_id"] = json!("old-run");
    assert!(agent_daemon_protocol::decode_run_readback(value.clone()).is_err());
    value["compact"]["owner"]["run_id"] = json!("r");
    value["compact"]["ignored_owner"] = json!("old");
    assert!(agent_daemon_protocol::decode_run_readback(value.clone()).is_err());
    value.as_object_mut().unwrap().remove("compact");
    assert!(agent_daemon_protocol::decode_run_readback(value).is_err());
}

#[test]
fn run_readback_validates_additive_continuation_identity_and_remains_strict() {
    let mut value = json!({"run_id":"r","turn_id":"t","session_id":"s","request_id":1,"status":"completed","last_seq":2,"content":"完成","error_code":null,"error_message":null,"snapshot":null,"continuation_parent_run_id":null,"continuation_run_id":"child"});
    assert_eq!(
        agent_daemon_protocol::decode_run_readback(value.clone())
            .unwrap()
            .run_id
            .0,
        "r"
    );
    value["continuation_run_id"] = json!({"run_id":"child"});
    assert!(agent_daemon_protocol::decode_run_readback(value.clone()).is_err());
    value["continuation_run_id"] = json!("");
    assert!(agent_daemon_protocol::decode_run_readback(value.clone()).is_err());
    value["continuation_run_id"] = json!("child");
    value["ignored_owner"] = json!("stale");
    assert!(agent_daemon_protocol::decode_run_readback(value).is_err());
}

#[test]
fn plan_execution_is_typed_exact_and_text_adapter_preserves_identity() {
    let mut request = JsonRpcRequest::new(
        RequestId::Number(1),
        "chat.send",
        json!({"session_id":"s","message":"/plan execute lifetime plan-id 7 digest operation"}),
    );
    request.protocol_version = Some(1);
    let normalized = normalize_request(request).unwrap();
    assert_eq!(normalized.params["expected_lifetime"], "lifetime");
    assert_eq!(
        normalized.params["plan_execution"],
        json!({"plan_id":"plan-id","revision":7,"content_digest":"digest","operation_id":"operation"})
    );
    assert_eq!(normalized.params["admission_mode"], "reject_if_busy");
    assert_eq!(
        normalize_request(normalized.clone()).unwrap().params,
        normalized.params
    );
    let mut invalid = normalized;
    invalid.params["plan_execution"]["extra_lifetime"] = json!("ignored?");
    assert!(normalize_request(invalid).is_err());
    let missing = JsonRpcRequest::new(
        RequestId::Number(2),
        "chat.send",
        json!({"message":"/plan execute lifetime plan-id 7 digest operation"}),
    );
    assert!(normalize_request(missing).is_err());
}

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

#[test]
fn provider_request_readback_is_strict_and_binds_owner_source_policy_digest_and_budget() {
    let base = serde_json::json!({"schema_version":1,"capture_id":"one","owner":{"session_key":"s","session_lifetime_id":"a","run_id":"r","run_generation":1,"turn_id":"t"},"round":1,"candidate_index":0,"snapshot_revision":5,"envelope":{"source":{"lifetime":"a","source_start":0,"source_end":1,"prefix_digest":"a".repeat(64),"generation":0,"policy_fingerprint":"4096:12:60:85"},"route":"model","provider_identity":"identity","tool_catalog_digest":"b".repeat(64),"stable_tokens":8,"history_tokens":0,"retrieved_tokens":0,"overlay_tokens":8,"calibrated_input_tokens":16,"output_reserve":409,"budget":4096},"policy_fingerprint":"c".repeat(64),"request_digest":"d".repeat(64),"message_digest":"e".repeat(64),"provider_tools_digest":"f".repeat(64),"images":false,"tool_calls":true,"replayability":"captured","omissions":[]});
    assert!(agent_daemon_protocol::decode_provider_request(base.clone()).is_ok());
    let mut bad = base.clone();
    bad["envelope"]["source"]["lifetime"] = serde_json::json!("old");
    assert!(agent_daemon_protocol::decode_provider_request(bad).is_err());
    let mut bad = base.clone();
    bad["envelope"]["source"]["lifetime_alias"] = serde_json::json!("old");
    assert!(agent_daemon_protocol::decode_provider_request(bad).is_err());
    let mut bad = base.clone();
    bad["owner_generation"] = serde_json::json!(7);
    assert!(agent_daemon_protocol::decode_provider_request(bad).is_err());
    let mut bad = base.clone();
    bad["policy_fingerprint"] = serde_json::json!("not-a-digest");
    assert!(agent_daemon_protocol::decode_provider_request(bad).is_err());
    let mut bad = base;
    bad["envelope"]["calibrated_input_tokens"] = serde_json::json!(4096);
    assert!(agent_daemon_protocol::decode_provider_request(bad).is_err());
}
