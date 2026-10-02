use crate::{Message, ProjectionGeneration, SessionKey, SessionLifetimeId, TranscriptSeq};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionSnapshot {
    pub session_key: SessionKey,
    pub lifetime: SessionLifetimeId,
    pub revision: TranscriptSeq,
    pub projection_generation: ProjectionGeneration,
    pub messages: Vec<Message>,
    pub batch_ranges: Vec<(u64, u64)>,
    pub prefix_digest: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionMetadata {
    pub key: SessionKey,
    pub lifetime: SessionLifetimeId,
    pub deleted: bool,
    pub legacy_imported: bool,
    pub revision: TranscriptSeq,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionCommand {
    Create {
        key: SessionKey,
    },
    End {
        key: SessionKey,
        delete: bool,
    },
    Fork {
        source: SessionKey,
        target: SessionKey,
        revision: TranscriptSeq,
    },
}
