use crate::DomainError;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Queued,
    Running,
    WaitingInteraction,
    Completed,
    Failed,
    Cancelled,
    UnknownAfterRestart,
}
impl RunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::WaitingInteraction => "waiting_interaction",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::UnknownAfterRestart => "unknown_after_restart",
        }
    }
    pub fn parse(value: &str) -> Result<Self, DomainError> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "waiting_interaction" => Ok(Self::WaitingInteraction),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "unknown_after_restart" => Ok(Self::UnknownAfterRestart),
            _ => Err(DomainError::UnknownRunStatus(value.to_owned())),
        }
    }
    pub fn terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::UnknownAfterRestart
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_run_statuses_keep_their_wire_names_and_terminal_meaning() {
        for (status, name, terminal) in [
            (RunStatus::Queued, "queued", false),
            (RunStatus::Running, "running", false),
            (RunStatus::WaitingInteraction, "waiting_interaction", false),
            (RunStatus::Completed, "completed", true),
            (RunStatus::Failed, "failed", true),
            (RunStatus::Cancelled, "cancelled", true),
            (
                RunStatus::UnknownAfterRestart,
                "unknown_after_restart",
                true,
            ),
        ] {
            assert_eq!(status.as_str(), name);
            assert_eq!(RunStatus::parse(name).unwrap(), status);
            assert_eq!(serde_json::to_value(status).unwrap(), name);
            assert_eq!(status.terminal(), terminal);
        }
        assert_eq!(
            RunStatus::parse("future"),
            Err(DomainError::UnknownRunStatus("future".into()))
        );
    }
}
