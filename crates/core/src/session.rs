use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::PathBuf;

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    #[default]
    Idle,
    Running,
    Waiting,
}

impl fmt::Display for SessionStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Idle => "idle",
            Self::Running => "running",
            Self::Waiting => "waiting",
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SessionInfo {
    pub id: String,
    pub path: PathBuf,
    pub active: bool,
    pub message_count: usize,
    pub modified_at: Option<u64>,
    pub preview: Option<String>,
    #[serde(default)]
    pub status: SessionStatus,
    #[serde(default)]
    pub active_requests: usize,
    #[serde(default)]
    pub updated_at: Option<u64>,
}
