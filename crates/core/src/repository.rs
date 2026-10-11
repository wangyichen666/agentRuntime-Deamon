//! 存储端口的纯领域错误；不携带 SQL、数据库驱动或文件路径。
use crate::{ArtifactError, AuthorizationError, CoordinationError, LedgerError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(
    tag = "domain",
    content = "kind",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum RepositoryError {
    #[error(transparent)]
    Ledger(#[from] LedgerError),
    #[error(transparent)]
    Artifact(#[from] ArtifactError),
    #[error(transparent)]
    Authorization(#[from] AuthorizationError),
    #[error(transparent)]
    Coordination(#[from] CoordinationError),
}
impl RepositoryError {
    pub fn rpc_code(self) -> i64 {
        match self {
            Self::Ledger(error) => error.rpc_code(),
            Self::Artifact(error) => error.rpc_code(),
            Self::Authorization(error) => error.rpc_code(),
            Self::Coordination(error) => error.rpc_code(),
        }
    }
    pub fn http_status(self) -> u16 {
        match self {
            Self::Ledger(error) => error.http_status(),
            Self::Artifact(error) => error.http_status(),
            Self::Authorization(error) => error.http_status(),
            Self::Coordination(error) => error.http_status(),
        }
    }
    pub fn retryable(self) -> bool {
        match self {
            Self::Ledger(_) => false,
            Self::Artifact(_) => false,
            Self::Authorization(_) => false,
            Self::Coordination(error) => error.retryable(),
        }
    }
    pub fn recovery_action(self) -> &'static str {
        match self {
            Self::Ledger(error) => error.recovery_action(),
            Self::Artifact(error) => error.recovery_action(),
            Self::Authorization(error) => error.recovery_action(),
            Self::Coordination(error) => error.recovery_action(),
        }
    }
}
