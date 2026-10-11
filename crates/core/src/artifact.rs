//! Artifact 的内容身份与可分类错误；不含路径、对象存储凭据或 I/O。
use serde::{Deserialize, Serialize};
pub const MAX_ARTIFACT_BYTES: u64 = 64 * 1024 * 1024;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactIdentity {
    pub sha256: String,
    pub bytes: u64,
}
impl ArtifactIdentity {
    pub fn valid(&self) -> bool {
        self.bytes <= MAX_ARTIFACT_BYTES
            && self.sha256.len() == 64
            && self
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactError {
    #[error("Artifact 存储不可用")]
    StorageUnavailable,
    #[error("Artifact 内容身份校验失败")]
    Corrupt,
    #[error("Artifact 目录或文件身份已变化")]
    Conflict,
    #[error("Artifact 引用或大小无效")]
    InvalidReference,
    #[error("Artifact 不允许链接或越界访问")]
    Forbidden,
    #[error("Artifact 原生 writer 凭据已失效")]
    StaleWriter,
}
impl ArtifactError {
    pub fn rpc_code(self) -> i64 {
        match self {
            Self::StorageUnavailable => -32060,
            Self::Corrupt => -32061,
            Self::Conflict => -32062,
            Self::InvalidReference => -32602,
            Self::Forbidden => -32041,
            Self::StaleWriter => -32063,
        }
    }
    pub fn http_status(self) -> u16 {
        match self {
            Self::StorageUnavailable => 503,
            Self::Corrupt => 500,
            Self::Conflict | Self::StaleWriter => 409,
            Self::InvalidReference => 400,
            Self::Forbidden => 403,
        }
    }
    pub fn recovery_action(self) -> &'static str {
        match self {
            Self::StorageUnavailable => "restore_artifact_store",
            Self::Corrupt => "verify_and_restore_object",
            Self::Conflict => "reconcile_object_identity",
            Self::InvalidReference => "correct_reference",
            Self::Forbidden => "review_object_access",
            Self::StaleWriter => "stop_old_writer_and_readback",
        }
    }
}

/// 原工具收据的引用发布输入；不持有另一套工具状态或对象凭据。
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolArtifactCompletion {
    pub round: usize,
    pub call_id: String,
    pub outcome: String,
    pub receipt: serde_json::Value,
}

/// 对象的持久发布证据；工具终态仍由原 tool receipt 保存。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectArtifactReservation {
    pub publication_id: String,
    pub envelope: crate::EnterpriseOwnerEnvelope,
    pub round: usize,
    pub call_id: String,
    pub identity: ArtifactIdentity,
    pub store_id: String,
    pub completion_digest: String,
    pub published: bool,
}
impl ObjectArtifactReservation {
    pub fn reference(&self) -> String {
        format!("artifact:v1:{}", self.publication_id)
    }
    pub fn valid(&self) -> bool {
        self.identity.valid()
            && self.round <= i64::MAX as usize
            && !self.call_id.is_empty()
            && self.call_id.len() <= 1024
            && [
                &self.publication_id,
                &self.store_id,
                &self.completion_digest,
            ]
            .iter()
            .all(|s| {
                ArtifactIdentity {
                    sha256: (*s).clone(),
                    bytes: 0,
                }
                .valid()
            })
    }
}
#[derive(Debug, PartialEq, Eq)]
pub struct ArtifactPage {
    pub identity: ArtifactIdentity,
    pub data: Vec<u8>,
}
