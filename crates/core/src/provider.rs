use crate::{DomainError, RetryPolicy};
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum ApiType {
    OpenaiChat,
    AnthropicMessages,
    Ollama,
}

impl ApiType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OpenaiChat => "openai-chat",
            Self::AnthropicMessages => "anthropic-messages",
            Self::Ollama => "ollama",
        }
    }
}
impl fmt::Display for ApiType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
impl FromStr for ApiType {
    type Err = DomainError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "openai-chat" => Ok(Self::OpenaiChat),
            "anthropic-messages" => Ok(Self::AnthropicMessages),
            "ollama" => Ok(Self::Ollama),
            _ => Err(DomainError::InvalidApiType),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeoutPhase {
    Connect,
    FirstEvent,
    StreamIdle,
    Overall,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "phase", rename_all = "snake_case")]
pub enum ProviderErrorKind {
    Auth,
    AccessDenied,
    RateLimit,
    QuotaExceeded,
    InvalidRequest,
    ContextOverflow,
    ContentPolicy,
    Timeout(TimeoutPhase),
    Transport,
    Server,
    Protocol,
    EmptyCompletion,
    ReasoningOnly,
    OutputTruncated,
    Cancelled,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProviderDiagnostic {
    pub http_status: Option<u16>,
    pub upstream_code: Option<String>,
    pub request_id: Option<String>,
    pub retry_after_ms: Option<u64>,
    pub redacted_message: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RouteCandidate {
    pub profile_id: String,
    pub api_type: ApiType,
    pub model: String,
    pub base_url_sha256: String,
}

impl RouteCandidate {
    pub fn circuit_key(&self) -> String {
        format!("{}:{}", self.profile_id, self.model)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TimeoutPolicy {
    pub connect_ms: u64,
    pub first_event_ms: u64,
    pub stream_idle_ms: u64,
    pub overall_ms: u64,
}

impl Default for TimeoutPolicy {
    fn default() -> Self {
        Self {
            connect_ms: 10_000,
            first_event_ms: 20_000,
            stream_idle_ms: 30_000,
            overall_ms: 300_000,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ContextPolicySnapshot {
    pub token_budget: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RouteSnapshot {
    pub candidates: Vec<RouteCandidate>,
    pub retry_policy: RetryPolicy,
    pub timeout_policy: TimeoutPolicy,
    pub context_policy: ContextPolicySnapshot,
    pub config_generation: u64,
}
