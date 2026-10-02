use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use agent_daemon_client::{ClientError, DaemonClient};
use agent_daemon_protocol::{
    EventSeq, JsonRpcResponse, RequestId, RunId, ServerFrame, decode_request, encode_frame,
};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;

static NEXT: AtomicU64 = AtomicU64::new(0);
fn socket() -> PathBuf {
    std::env::temp_dir().join(format!(
        "agent-client-{}-{}.sock",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

#[tokio::test]
async fn reconnect_uses_cursor_readback_without_replaying_mutation() {
    let socket = socket();
    let listener = UnixListener::bind(&socket).unwrap();
    let server = tokio::spawn(async move {
        let (first, _) = listener.accept().await.unwrap();
        let (reader, first_writer) = first.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = Vec::new();
        reader.read_until(b'\n', &mut line).await.unwrap();
        let mutation = decode_request(&line).unwrap();
        assert_eq!(mutation.method, "runs.send");
        assert_eq!(mutation.protocol_version, Some(1));
        drop(first_writer);
        drop(reader); // 响应丢失：不证明业务是否已提交。
        let (second, _) = listener.accept().await.unwrap();
        let (reader, mut writer) = second.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = Vec::new();
        reader.read_until(b'\n', &mut line).await.unwrap();
        let readback = decode_request(&line).unwrap();
        assert_eq!(readback.method, "runs.events");
        assert_eq!(readback.params["after_seq"], 4);
        assert_ne!(readback.id, mutation.id);
        writer
            .write_all(
                &encode_frame(&ServerFrame::Response(JsonRpcResponse::success(
                    readback.id,
                    json!({"events":[
                        {"run_id":"run-1","seq":5,"event":"terminal","data":{"status":"completed"}}
                    ]}),
                )))
                .unwrap(),
            )
            .await
            .unwrap();
    });
    let client = DaemonClient::connect_unix(&socket).await.unwrap();
    assert!(matches!(
        client
            .request_result("runs.send", json!({"session_id":"s","message":"输入"}))
            .await,
        Err(ClientError::Disconnected)
    ));
    let recovered = client.reconnect().await.unwrap();
    let page = recovered
        .read_events(&RunId("run-1".into()), EventSeq(4), 10)
        .await
        .unwrap();
    assert_eq!(page.cursor, EventSeq(5));
    assert!(!page.has_more);
    assert_eq!(page.events[0].data["status"], "completed");
    server.await.unwrap();
    std::fs::remove_file(socket).unwrap();
}

#[tokio::test]
async fn readback_rejects_wrong_owner_and_missing_event_sequence() {
    for (owner, seq) in [("other-run", 5), ("run-1", 6)] {
        let socket = socket();
        let listener = UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (reader, mut writer) = stream.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = Vec::new();
            reader.read_until(b'\n', &mut line).await.unwrap();
            let request = decode_request(&line).unwrap();
            let response = JsonRpcResponse::success(
                request.id,
                json!({"events":[{"run_id":owner,"seq":seq,"event":"terminal","data":{}}]}),
            );
            writer
                .write_all(&encode_frame(&ServerFrame::Response(response)).unwrap())
                .await
                .unwrap();
        });
        let client = DaemonClient::connect_unix(&socket).await.unwrap();
        assert!(matches!(
            client
                .read_events(&RunId("run-1".into()), EventSeq(4), 10)
                .await,
            Err(ClientError::Protocol(_))
        ));
        server.await.unwrap();
        std::fs::remove_file(socket).unwrap();
    }
}

#[tokio::test]
async fn run_readback_rejects_a_different_run_owner() {
    let socket = socket();
    let listener = UnixListener::bind(&socket).unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = Vec::new();
        reader.read_until(b'\n', &mut line).await.unwrap();
        let request = decode_request(&line).unwrap();
        let response = JsonRpcResponse::success(
            request.id,
            json!({
                "run_id":"other-run", "turn_id":"turn-2", "session_id":"s",
                "request_id":1, "status":"completed", "last_seq":2,
                "content":null, "error_code":null, "error_message":null
            }),
        );
        writer
            .write_all(&encode_frame(&ServerFrame::Response(response)).unwrap())
            .await
            .unwrap();
    });
    let client = DaemonClient::connect_unix(&socket).await.unwrap();
    assert!(matches!(
        client.read_run(&RunId("run-1".into())).await,
        Err(ClientError::Protocol(_))
    ));
    server.await.unwrap();
    std::fs::remove_file(socket).unwrap();
}

#[tokio::test]
async fn resync_required_keeps_typed_snapshot_and_cursor() {
    let socket = socket();
    let listener = UnixListener::bind(&socket).unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = Vec::new();
        reader.read_until(b'\n', &mut line).await.unwrap();
        let request = decode_request(&line).unwrap();
        let response = JsonRpcResponse::failure_data(
            request.id,
            -32001,
            "读回缺口",
            json!({"kind":"resync_required","cursor":9,"snapshot":{"status":"running"}}),
        );
        writer
            .write_all(&encode_frame(&ServerFrame::Response(response)).unwrap())
            .await
            .unwrap();
    });
    let client = DaemonClient::connect_unix(&socket).await.unwrap();
    let error = client
        .request_result(
            "agent.subscribe",
            json!({"request_id":RequestId::Number(1)}),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, ClientError::ResyncRequired {cursor:9, snapshot} if snapshot["status"] == "running")
    );
    server.await.unwrap();
    std::fs::remove_file(socket).unwrap();
}
