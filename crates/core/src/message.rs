use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Message {
    pub role: Role,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub image_urls: Vec<String>,
}

impl Message {
    pub fn text(role: Role, content: impl Into<String>) -> Self {
        Self {
            role,
            content: Some(content.into()),
            thinking: None,
            tool_calls: Vec::new(),
            tool_call_id: None,
            name: None,
            image_urls: Vec::new(),
        }
    }

    pub fn assistant_tool_calls(calls: Vec<ToolCall>) -> Self {
        Self {
            role: Role::Assistant,
            content: None,
            thinking: None,
            tool_calls: calls,
            tool_call_id: None,
            name: None,
            image_urls: Vec::new(),
        }
    }

    pub fn tool_result(call: &ToolCall, content: impl Into<String>) -> Self {
        Self {
            role: Role::Tool,
            content: Some(content.into()),
            thinking: None,
            tool_calls: Vec::new(),
            tool_call_id: Some(call.id.clone()),
            name: Some(call.name.clone()),
            image_urls: Vec::new(),
        }
    }

    pub fn user_with_images(content: impl Into<String>, image_urls: Vec<String>) -> Self {
        Self {
            role: Role::User,
            content: Some(content.into()),
            thinking: None,
            tool_calls: Vec::new(),
            tool_call_id: None,
            name: None,
            image_urls,
        }
    }

    pub fn assistant_with_thinking(content: impl Into<String>, thinking: Option<String>) -> Self {
        Self {
            role: Role::Assistant,
            content: Some(content.into()),
            thinking,
            tool_calls: Vec::new(),
            tool_call_id: None,
            name: None,
            image_urls: Vec::new(),
        }
    }

    pub fn assistant_tool_calls_with_thinking(
        calls: Vec<ToolCall>,
        thinking: Option<String>,
    ) -> Self {
        if thinking.is_none() {
            return Self::assistant_tool_calls(calls);
        }
        Self {
            role: Role::Assistant,
            content: None,
            thinking,
            tool_calls: calls,
            tool_call_id: None,
            name: None,
            image_urls: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolCall {
    /// Agent 内部 execution identity。provider 出站前必须还原 wire id，禁止原样发送。
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}
