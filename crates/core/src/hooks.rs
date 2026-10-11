//! Hook 的允许效果与失败语义；执行、配置和持久化属于各自 owner。
use crate::{ExactOwner, SessionKey, SessionLifetimeId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookChannel {
    #[default]
    DaemonRpc,
    Cli,
    Tui,
    Acp,
    WebSocket,
    Http,
    Continuation,
    Subagent,
    Cron,
}
impl HookChannel {
    pub fn key(self) -> &'static str {
        match self {
            Self::DaemonRpc => "daemon_rpc",
            Self::Cli => "cli",
            Self::Tui => "tui",
            Self::Acp => "acp",
            Self::WebSocket => "web_socket",
            Self::Http => "http",
            Self::Continuation => "continuation",
            Self::Subagent => "subagent",
            Self::Cron => "cron",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookEvent {
    UserPromptSubmit,
    PreTool,
    PostTool,
    ToolError,
    ApprovalRequested,
    ApprovalResolved,
    SubagentStart,
    SubagentEnd,
    AssistantReply,
    AfterTurn,
    Stop,
    SessionStart,
    SessionEnd,
    Notification,
    PreCompact,
    PostCompact,
}

impl HookEvent {
    pub const ALL: [Self; 16] = [
        Self::UserPromptSubmit,
        Self::PreTool,
        Self::PostTool,
        Self::ToolError,
        Self::ApprovalRequested,
        Self::ApprovalResolved,
        Self::SubagentStart,
        Self::SubagentEnd,
        Self::AssistantReply,
        Self::AfterTurn,
        Self::Stop,
        Self::SessionStart,
        Self::SessionEnd,
        Self::Notification,
        Self::PreCompact,
        Self::PostCompact,
    ];

    pub fn controls(self) -> bool {
        matches!(
            self,
            Self::UserPromptSubmit | Self::PreTool | Self::PreCompact | Self::Stop
        )
    }

    pub fn permits(self, effect: &HookEffect) -> bool {
        match effect {
            HookEffect::Observe {} => true,
            HookEffect::Allow {} | HookEffect::Deny { .. } => matches!(
                self,
                Self::UserPromptSubmit | Self::PreTool | Self::PreCompact
            ),
            HookEffect::Continue { prompt } => {
                self == Self::Stop && !prompt.trim().is_empty() && prompt.len() <= 4096
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "effect", rename_all = "snake_case", deny_unknown_fields)]
pub enum HookEffect {
    Observe {},
    Allow {},
    Deny { reason: String },
    Continue { prompt: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookFailure {
    Untrusted,
    InvalidConfig,
    SpawnFailed,
    Timeout,
    NonzeroExit,
    OutputLimit,
    InvalidOutput,
    InvalidEffect,
    Cancelled,
    UnknownAfterRestart,
    BudgetExceeded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookStatus {
    Running,
    Succeeded,
    Failed,
    UnknownAfterRestart,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookPayload {
    pub schema_version: u16,
    pub event: HookEvent,
    pub session_key: SessionKey,
    pub session_lifetime_id: SessionLifetimeId,
    pub owner: Option<ExactOwner>,
    pub cwd: String,
    pub channel: String,
    pub permission_mode: String,
    pub route_digest: Option<String>,
    pub data: serde_json::Value,
    pub operation_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookOutcome {
    pub payload: HookPayload,
    pub status: HookStatus,
    pub effect: Option<HookEffect>,
    pub failure: Option<HookFailure>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HookClaim {
    New,
    Existing(Box<HookOutcome>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookPublication {
    pub session_key: SessionKey,
    pub session_lifetime_id: SessionLifetimeId,
    pub event: HookEvent,
    pub operation_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookReadback {
    pub schema_version: u16,
    pub session_key: SessionKey,
    pub session_lifetime_id: SessionLifetimeId,
    pub snapshot_revision: crate::SnapshotRevision,
    pub outcomes: Vec<HookOutcome>,
    pub cursor: u64,
    pub has_more: bool,
}

pub fn validate_hook_outcome(outcome: &HookOutcome) -> Result<(), crate::CoordinationError> {
    let payload = &outcome.payload;
    let owner_valid = payload.owner.as_ref().map_or_else(
        || {
            matches!(
                payload.event,
                HookEvent::SessionStart | HookEvent::SessionEnd | HookEvent::Notification
            )
        },
        |owner| {
            owner.session_key == payload.session_key
                && owner.session_lifetime_id == payload.session_lifetime_id
        },
    );
    let state_valid = match outcome.status {
        HookStatus::Running => outcome.effect.is_none() && outcome.failure.is_none(),
        HookStatus::Succeeded => {
            outcome.failure.is_none()
                && outcome
                    .effect
                    .as_ref()
                    .is_some_and(|effect| payload.event.permits(effect))
        }
        HookStatus::Failed => {
            outcome.effect.is_none()
                && outcome
                    .failure
                    .is_some_and(|failure| failure != HookFailure::UnknownAfterRestart)
        }
        HookStatus::UnknownAfterRestart => {
            outcome.effect.is_none() && outcome.failure == Some(HookFailure::UnknownAfterRestart)
        }
    };
    if payload.schema_version != 1
        || payload.operation_id.is_empty()
        || payload.operation_id.len() > 256
        || serde_json::to_vec(payload)
            .map_err(|_| crate::CoordinationError::InvalidRequest)?
            .len()
            > 16384
        || !owner_valid
        || !state_valid
    {
        return Err(crate::CoordinationError::InvalidRequest);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn inventory_controls_are_explicit_and_observers_cannot_continue_or_deny() {
        assert_eq!(HookEvent::ALL.len(), 16);
        for event in HookEvent::ALL {
            assert!(event.permits(&HookEffect::Observe {}));
            assert_eq!(
                event.permits(&HookEffect::Deny {
                    reason: "拒绝".into()
                }),
                matches!(
                    event,
                    HookEvent::UserPromptSubmit | HookEvent::PreTool | HookEvent::PreCompact
                )
            );
            assert_eq!(
                event.permits(&HookEffect::Continue {
                    prompt: "继续".into()
                }),
                event == HookEvent::Stop
            );
        }
        assert!(!HookEvent::Stop.permits(&HookEffect::Continue {
            prompt: String::new()
        }));
        assert!(
            serde_json::from_value::<HookEffect>(
                serde_json::json!({"effect":"allow","approved":true})
            )
            .is_err()
        );
    }
}
