use crate::{ExactOwner, Message, ProjectionGeneration, SessionLifetimeId, TranscriptSeq};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextSource {
    pub lifetime: SessionLifetimeId,
    #[serde(default = "source_start")]
    pub source_start: TranscriptSeq,
    pub source_end: TranscriptSeq,
    pub prefix_digest: String,
    pub generation: ProjectionGeneration,
    pub policy_fingerprint: String,
    #[serde(default)]
    pub pressure_route: String,
    #[serde(default)]
    pub summary_route: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactIntent {
    pub operation: String,
    pub owner: ExactOwner,
    pub source: ContextSource,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContextProjection {
    pub source_end: TranscriptSeq,
    pub generation: ProjectionGeneration,
    pub messages: Vec<Message>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContextEnvelope {
    pub source: ContextSource,
    pub route: String,
    #[serde(default)]
    pub provider_identity: String,
    pub tool_catalog_digest: String,
    pub stable_tokens: u64,
    pub history_tokens: u64,
    pub retrieved_tokens: u64,
    pub overlay_tokens: u64,
    #[serde(default)]
    pub calibrated_input_tokens: u64,
    pub output_reserve: u64,
    pub budget: u64,
}

fn source_start() -> TranscriptSeq {
    TranscriptSeq(0)
}
