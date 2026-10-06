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
#[serde(deny_unknown_fields)]
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
    /// 结算工作并释放 runtime；保留 canonical history/lifetime。
    Close {
        key: SessionKey,
    },
    Fork {
        source: SessionKey,
        target: SessionKey,
        revision: TranscriptSeq,
    },
}

/// 恢复投影游标；由数据库事务推进，跨 lifetime 不回绕。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SnapshotRevision(pub u64);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryReadMode {
    #[default]
    Canonical,
    Model,
    Omitted,
}

/// Repository 的一致读模型；daemon 只附加兼容展示，不另读缓存拼装事实。
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionReadback {
    pub schema_version: u16,
    pub session_id: SessionKey,
    pub session_lifetime_id: SessionLifetimeId,
    pub snapshot_revision: SnapshotRevision,
    pub metadata_revision: SnapshotRevision,
    pub metadata: SessionMetadata,
    pub transcript_revision: TranscriptSeq,
    pub projection_generation: ProjectionGeneration,
    pub history_mode: HistoryReadMode,
    pub omitted: Vec<String>,
    pub messages: Vec<Message>,
    /// 由 canonical batch 的原生 turn/seq 派生；不额外保存历史。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub message_ids: Vec<String>,
    pub batch_ranges: Vec<(u64, u64)>,
    pub active_owner: Option<crate::ExactOwner>,
    #[serde(default)]
    pub run_owners: Vec<crate::ExactOwner>,
    pub active_runs: Vec<crate::RunRecord>,
    pub queue_rows: Vec<crate::QueuedMessage>,
    pub queue_cursor: Option<i64>,
    pub pending_interactions: Vec<crate::InteractionRecord>,
    pub last_durable_terminal: Option<crate::RunRecord>,
    pub current_plan: Option<crate::PlanSnapshot>,
    pub plan_digest: Option<String>,
    pub context_usage: Option<crate::ContextEnvelope>,
}

impl SessionReadback {
    /// 相同 session 的旧读回不能覆盖更晚的 durable/live 投影。
    pub fn supersedes(&self, current: &Self) -> bool {
        let baseline = crate::reduce_view(
            None,
            crate::ViewInput::Readback {
                snapshot: Box::new(current.clone()),
            },
        );
        crate::reduce_view(
            baseline.state,
            crate::ViewInput::Readback {
                snapshot: Box::new(self.clone()),
            },
        )
        .decision
            == crate::ViewDecision::Accepted
    }
}
