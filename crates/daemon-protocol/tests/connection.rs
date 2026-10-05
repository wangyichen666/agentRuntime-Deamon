use agent_daemon_protocol::*;
use serde_json::json;
fn strict(method: &str, params: serde_json::Value) -> JsonRpcRequest {
    let mut request = JsonRpcRequest::new(RequestId::Number(1), method, params);
    request.protocol_version = Some(1);
    request
}
#[test]
fn capabilities_are_intersected_by_schema_and_bound_to_the_connection() {
    let mut connection = ConnectionState::default();
    assert!(
        connection
            .accept(strict("sessions.create", json!({})))
            .is_err()
    );
    let offer = InitializeParams {
        protocol_versions: vec![9, 1],
        capabilities: vec![
            Capability {
                name: "session".into(),
                schema_version: 1,
            },
            Capability {
                name: "memory".into(),
                schema_version: 99,
            },
            Capability {
                name: "future-extension".into(),
                schema_version: 1,
            },
        ],
    };
    let ConnectionRequest::Initialized(result) = connection
        .accept(strict(
            "connection.initialize",
            serde_json::to_value(&offer).unwrap(),
        ))
        .unwrap()
    else {
        panic!("初始化");
    };
    assert_eq!(
        result.capabilities,
        vec![Capability {
            name: "session".into(),
            schema_version: 1
        }]
    );
    result.validate_offer(&offer).unwrap();
    assert!(matches!(
        connection
            .accept(strict(
                "sessions.read",
                json!({"session_id":"s","history_mode":"omitted"})
            ))
            .unwrap(),
        ConnectionRequest::Dispatch(_)
    ));
    assert!(
        connection
            .accept(strict(
                "memory.store",
                json!({"session_id":"s","owner_run_id":"r","operation_id":"o","content":"禁止"})
            ))
            .is_err()
    );
    assert!(
        connection
            .accept(strict("slash.execute", json!({"line":"/memory s"})))
            .is_err(),
        "slash 不能绕过未协商的能力"
    );
    assert!(
        connection
            .accept(strict("arbitrary.rpc", json!({})))
            .is_err()
    );
    assert!(
        connection
            .accept(JsonRpcRequest::new(
                RequestId::Number(2),
                "sessions.create",
                json!({})
            ))
            .is_err(),
        "协商后不能降级为 legacy"
    );
    let changed = InitializeParams::default();
    assert!(
        connection
            .accept(strict(
                "connection.initialize",
                serde_json::to_value(changed).unwrap()
            ))
            .is_err()
    );
    let mut fresh = ConnectionState::default();
    assert!(
        fresh.accept(strict("sessions.read", json!({}))).is_err(),
        "重连必须重新协商"
    );
}
#[test]
fn malformed_initialization_does_not_publish_a_capability_snapshot() {
    let mut connection = ConnectionState::default();
    for params in [
        json!({"protocol_versions":[1],"capabilities":[],"unknown":true}),
        json!({"protocol_versions":[1,1],"capabilities":[]}),
        json!({"protocol_versions":[9],"capabilities":[]}),
        json!({"protocol_versions":[1],"capabilities":[{"name":"session","schema_version":1},{"name":"session","schema_version":1}]}),
    ] {
        assert!(
            connection
                .accept(strict("connection.initialize", params))
                .is_err()
        );
        assert!(
            connection
                .accept(strict("sessions.create", json!({})))
                .is_err()
        );
    }
    assert!(
        connection
            .accept(strict(
                "connection.initialize",
                serde_json::to_value(InitializeParams::default()).unwrap()
            ))
            .is_ok()
    );
}
#[test]
fn all_destructive_mutations_require_lifetime_and_legacy_aliases_stay_centralized() {
    for (method, params) in [
        (
            "sessions.delete",
            json!({"session_id":"s","operation_id":"d"}),
        ),
        (
            "sessions.clear",
            json!({"session_id":"s","operation_id":"c"}),
        ),
        (
            "sessions.fork",
            json!({"session_id":"s","target_session_id":"t","operation_id":"f","expected_revision":0}),
        ),
    ] {
        assert!(normalize_request(strict(method, params.clone())).is_err());
        let mut expected = params.clone();
        expected["expected_lifetime"] = json!("lifetime");
        assert!(normalize_request(strict(method, expected)).is_ok());
        assert!(
            normalize_request(JsonRpcRequest::new(RequestId::Number(1), method, params)).is_err()
        );
    }
}

#[test]
fn model_only_read_is_an_explicit_pure_projection_and_illegal_combinations_fail() {
    let normalized = normalize_request(strict(
        "sessions.read",
        json!({"session_id":"s","read_model_only":true}),
    ))
    .unwrap();
    assert_eq!(
        normalized.params,
        json!({"session_id":"s","history_mode":"model"})
    );
    assert!(
        normalize_request(strict(
            "sessions.read",
            json!({"session_id":"s","read_model_only":true,"history_mode":"canonical"})
        ))
        .is_err()
    );
    assert!(
        normalize_request(strict(
            "sessions.read",
            json!({"session_id":"s","read_model_only":"yes"})
        ))
        .is_err()
    );
}
#[test]
fn response_rejects_conflicting_success_failure_and_nested_snapshot_fields() {
    for bytes in [
        br#"{"frame":"response","jsonrpc":"2.0","id":1}"#.as_slice(),
        br#"{"frame":"response","jsonrpc":"2.0","id":1,"result":{},"error":{"code":-1,"message":"e"}}"#.as_slice(),
    ] { assert!(decode_server_frame_for_version(bytes,1).is_err()); }
    let terminal = json!({"run_id":"run-1","turn_id":"turn-1","session_id":"s","request_id":1,"status":"completed","last_seq":1,"content":null,"error_code":null,"error_message":null,"unknown":true});
    assert!(serde_json::from_value::<RunRecord>(terminal).is_err());
}
