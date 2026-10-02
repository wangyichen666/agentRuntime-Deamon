use agent_core::{EventSeq, RunId};
use agent_daemon_protocol::{
    EventFrame, EventKind, JsonRpcRequest, JsonRpcResponse, ProtocolError, RequestId, ServerFrame,
    decode_request, decode_server_frame, encode_frame,
};
use serde_json::json;

#[test]
fn request_encoding_is_byte_compatible_with_existing_clients() {
    let request = JsonRpcRequest::new(RequestId::Number(7), "chat.send", json!({"message":"你好"}));
    assert_eq!(encode_frame(&request).unwrap(), "{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"chat.send\",\"params\":{\"message\":\"你好\"}}\n".as_bytes());
}

#[test]
fn v0_legacy_request_extensions_remain_accepted_until_versioned_migration() {
    // Wave 0 固定兼容行为；严格 versioned DTO 在 Wave 1 引入。
    let request =
        decode_request(br#"{"jsonrpc":"2.0","id":1,"method":"run.read","future_extension":true}"#)
            .unwrap();
    assert_eq!(request.params, serde_json::Value::Null);
}

#[test]
fn failure_data_and_resync_cursor_preserve_the_existing_wire_shape() {
    let response = JsonRpcResponse::failure_data(
        RequestId::String("订阅".into()),
        -32001,
        "需要读回",
        json!({"kind":"resync_required", "cursor":42, "next_method":"run.events"}),
    );
    assert_eq!(
        serde_json::to_value(&response).unwrap(),
        json!({
            "jsonrpc":"2.0", "id":"订阅", "error":{"code":-32001,"message":"需要读回",
            "data":{"kind":"resync_required","cursor":42,"next_method":"run.events"}}
        })
    );
    let frame = ServerFrame::Response(response);
    assert_eq!(
        decode_server_frame(&encode_frame(&frame).unwrap()).unwrap(),
        frame
    );
}

#[test]
fn event_readback_uses_core_ids_without_publishing_internal_lifetime() {
    let mut event = EventFrame::new(
        RequestId::Number(1),
        EventKind::TextDelta,
        json!({"delta":"原文"}),
    );
    event.run_id = Some(RunId("run-1".into()));
    event.seq = Some(EventSeq(42));
    let value = serde_json::to_value(&event).unwrap();
    assert_eq!(value["run_id"], "run-1");
    assert_eq!(value["seq"], 42);
    assert!(value.get("session_lifetime_id").is_none());
    let frame = ServerFrame::Event(event);
    assert_eq!(
        decode_server_frame(&encode_frame(&frame).unwrap()).unwrap(),
        frame
    );
}

#[test]
fn malformed_frames_and_unknown_jsonrpc_versions_fail_closed() {
    assert!(matches!(
        decode_request(b"{"),
        Err(ProtocolError::InvalidJson(_))
    ));
    assert!(matches!(
        decode_request(br#"{"jsonrpc":"3.0","id":1,"method":"chat.send"}"#),
        Err(ProtocolError::UnsupportedVersion(_))
    ));
    assert!(matches!(
        decode_server_frame(br#"{"frame":"response","jsonrpc":"3.0","id":1,"result":{}}"#),
        Err(ProtocolError::UnsupportedVersion(_))
    ));
    assert!(matches!(
        decode_server_frame(br#"{"frame":"future"}"#),
        Err(ProtocolError::InvalidJson(_))
    ));
}
