use crate::{ExactOwner, SessionLifetimeId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "scope", content = "identity", rename_all = "snake_case")]
pub enum MemoryScope {
    Session(SessionLifetimeId),
    Project(String),
    Global,
    Legacy(String),
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryLayer {
    Working,
    Episode,
    Semantic,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryKind {
    TurnSummary,
    Fact,
    StandingPreference,
    Decision,
    FailureLesson,
    Explicit,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemoryRecord {
    pub id: String,
    pub layer: MemoryLayer,
    pub scope: MemoryScope,
    pub kind: MemoryKind,
    pub content: String,
    pub source: Option<ExactOwner>,
    pub source_message_ids: Vec<String>,
    pub event_time: u64,
    pub created_at: u64,
    pub updated_at: u64,
    pub expires_at: Option<u64>,
    pub confidence: u8,
    pub confirmed_by_user: bool,
    pub content_digest: String,
    pub revision: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemoryVisibility {
    pub lifetime: SessionLifetimeId,
    pub project: String,
    pub allow_confirmed_global: bool,
}
impl MemoryVisibility {
    pub fn allows(&self, entry: &MemoryRecord, now: u64) -> bool {
        entry.expires_at.is_none_or(|expiry| expiry > now)
            && match &entry.scope {
                MemoryScope::Session(lifetime) => lifetime == &self.lifetime,
                MemoryScope::Project(project) => project == &self.project,
                MemoryScope::Global => {
                    self.allow_confirmed_global
                        && entry.confirmed_by_user
                        && entry.layer == MemoryLayer::Semantic
                }
                MemoryScope::Legacy(_) => false,
            }
    }
}
