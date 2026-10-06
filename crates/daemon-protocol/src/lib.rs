//! 兼容 JSON-RPC wire；不拥有会话或运行状态。
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::print_stdout)
)]

pub mod acp;
mod readback;
pub use readback::{
    decode_provider_request, decode_run_readback, decode_session_page, decode_session_readback,
    decode_view_sync,
};
mod connection;
pub use agent_core::{
    HistoryReadMode, ProviderRequestReadback, SessionKey, SessionReadback, SnapshotRevision,
};
pub use agent_core::{ViewDecision, ViewInput, ViewReduction, ViewStamp, ViewState, reduce_view};
pub use connection::*;
mod methods;
pub mod params;
pub use agent_core::{
    EventSeq, Message, PendingApprovalInfo, RequestId, RunId, RunRecord, StoredEvent,
};
pub use methods::{canonical_method, normalize_request};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

pub const JSONRPC_VERSION: &str = "2.0";
pub const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_version: Option<u16>,
    pub id: RequestId,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

impl JsonRpcRequest {
    pub fn new(id: RequestId, method: impl Into<String>, params: Value) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_owned(),
            protocol_version: None,
            id,
            method: method.into(),
            params,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: RequestId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

impl JsonRpcResponse {
    pub fn success(id: RequestId, result: Value) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_owned(),
            id,
            result: Some(result),
            error: None,
        }
    }

    pub fn failure(id: RequestId, code: i64, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_owned(),
            id,
            result: None,
            error: Some(RpcError {
                code,
                message: message.into(),
                data: None,
            }),
        }
    }

    pub fn failure_data(id: RequestId, code: i64, message: impl Into<String>, data: Value) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_owned(),
            id,
            result: None,
            error: Some(RpcError {
                code,
                message: message.into(),
                data: Some(data),
            }),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    ViewResynced,
    RunStarted,
    CompactTerminal,
    TurnStarted,
    ThinkingDelta,
    ThinkingFinished,
    TextDelta,
    ToolStarted,
    ToolFinished,
    ApprovalRequired,
    TurnCompleted,
    DelegationSpawned,
    DelegationTerminal,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct EventFrame {
    pub jsonrpc: String,
    pub request_id: RequestId,
    pub event: EventKind,
    pub data: Value,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub run_id: Option<RunId>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub seq: Option<EventSeq>,
}

impl EventFrame {
    pub fn new(request_id: RequestId, event: EventKind, data: Value) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_owned(),
            request_id,
            event,
            data,
            run_id: None,
            seq: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "frame", rename_all = "snake_case")]
pub enum ServerFrame {
    Event(EventFrame),
    Response(JsonRpcResponse),
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("不支持的 daemon 协议版本：{0}")]
    UnsupportedProtocolVersion(u16),
    #[error("RPC 参数无效：{0}")]
    InvalidParams(String),
    #[error("协议帧大小 {actual} 字节，超过 {limit} 字节限制")]
    FrameTooLarge { actual: usize, limit: usize },
    #[error("JSON-RPC 帧不是合法 JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
    #[error("不支持的 JSON-RPC 版本: {0}")]
    UnsupportedVersion(String),
}

pub fn encode_frame<T: Serialize>(frame: &T) -> Result<Vec<u8>, ProtocolError> {
    let mut bytes = serde_json::to_vec(frame)?;
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge {
            actual: bytes.len(),
            limit: MAX_FRAME_BYTES,
        });
    }
    bytes.push(b'\n');
    Ok(bytes)
}

pub fn decode_request(bytes: &[u8]) -> Result<JsonRpcRequest, ProtocolError> {
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge {
            actual: bytes.len(),
            limit: MAX_FRAME_BYTES,
        });
    }
    let request: JsonRpcRequest = serde_json::from_slice(bytes)?;
    if let Some(version) = request.protocol_version {
        if version != 1 {
            return Err(ProtocolError::UnsupportedProtocolVersion(version));
        }
        let raw: Value = serde_json::from_slice(bytes)?;
        if let Some(fields) = raw.as_object() {
            for field in fields.keys() {
                if !["jsonrpc", "protocol_version", "id", "method", "params"]
                    .contains(&field.as_str())
                {
                    return Err(ProtocolError::InvalidParams(format!(
                        "未知 envelope 字段：{field}"
                    )));
                }
            }
        }
    }
    if request.jsonrpc != JSONRPC_VERSION {
        return Err(ProtocolError::UnsupportedVersion(request.jsonrpc));
    }
    Ok(request)
}

pub fn decode_server_frame(bytes: &[u8]) -> Result<ServerFrame, ProtocolError> {
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge {
            actual: bytes.len(),
            limit: MAX_FRAME_BYTES,
        });
    }
    let frame: ServerFrame = serde_json::from_slice(bytes)?;
    let version = match &frame {
        ServerFrame::Event(event) => &event.jsonrpc,
        ServerFrame::Response(response) => &response.jsonrpc,
    };
    if version != JSONRPC_VERSION {
        return Err(ProtocolError::UnsupportedVersion(version.clone()));
    }
    Ok(frame)
}

/// v0 保持兼容；v1 对已知 response/event envelope 字段严格校验。
pub fn decode_server_frame_for_version(
    bytes: &[u8],
    version: u16,
) -> Result<ServerFrame, ProtocolError> {
    if version > 1 {
        return Err(ProtocolError::UnsupportedProtocolVersion(version));
    }
    let frame = decode_server_frame(bytes)?;
    if version == 1 {
        let value: Value = serde_json::from_slice(bytes)?;
        if let ServerFrame::Response(response) = &frame {
            if response.result.is_some() == response.error.is_some() {
                return Err(ProtocolError::InvalidParams(
                    "response 必须且只能有 result 或 error".into(),
                ));
            }
        }
        let allowed: &[&str] = match &frame {
            ServerFrame::Response(_) => &["frame", "jsonrpc", "id", "result", "error"],
            ServerFrame::Event(_) => &[
                "frame",
                "jsonrpc",
                "request_id",
                "event",
                "data",
                "run_id",
                "seq",
            ],
        };
        if let Some(object) = value.as_object() {
            for field in object.keys() {
                if !allowed.contains(&field.as_str()) {
                    return Err(ProtocolError::InvalidParams(format!(
                        "未知响应字段：{field}"
                    )));
                }
            }
        }
        if let Some(error) = value.get("error").and_then(Value::as_object) {
            for field in error.keys() {
                if !["code", "message", "data"].contains(&field.as_str()) {
                    return Err(ProtocolError::InvalidParams(format!(
                        "未知错误字段：{field}"
                    )));
                }
            }
        }
    }
    Ok(frame)
}

pub fn server_frame_request_id(frame: &ServerFrame) -> &RequestId {
    match frame {
        ServerFrame::Event(event) => &event.request_id,
        ServerFrame::Response(response) => &response.id,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn request_round_trip_uses_newline_delimited_json() {
        let request = JsonRpcRequest::new(
            RequestId::Number(7),
            "chat.send",
            json!({"message": "你好"}),
        );
        let encoded = encode_frame(&request).unwrap();

        assert_eq!(encoded.last(), Some(&b'\n'));
        assert_eq!(decode_request(&encoded).unwrap(), request);
    }

    #[test]
    fn server_frame_round_trip_preserves_request_id() {
        let frame = ServerFrame::Response(JsonRpcResponse::success(
            RequestId::String("abc".to_owned()),
            json!({"ok": true}),
        ));
        let encoded = encode_frame(&frame).unwrap();
        let decoded = decode_server_frame(&encoded).unwrap();

        assert_eq!(
            server_frame_request_id(&decoded),
            &RequestId::String("abc".to_owned())
        );
    }

    #[test]
    fn rejects_oversized_frames() {
        let bytes = vec![b'x'; MAX_FRAME_BYTES + 1];
        assert!(matches!(
            decode_request(&bytes),
            Err(ProtocolError::FrameTooLarge { .. })
        ));
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RecoverySnapshot {
    pub session_id: String,
    pub messages: Vec<Message>,
    pub pending_approvals: Vec<PendingApprovalInfo>,
    pub active_requests: Vec<RequestId>,
}

pub mod slash;
