//! 共享领域值；不依赖入口、执行内核、daemon 或具体存储。
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::print_stdout)
)]

mod attempt;
mod context;
mod control;
mod identity;
mod memory;
mod message;
mod provider;
mod resource;
mod retry;
mod run;
mod session;
mod tool;
mod transcript;
mod turn;
mod wire;

pub use attempt::*;
pub use context::*;
pub use control::*;
pub use identity::*;
pub use memory::*;
pub use message::{Message, Role, ToolCall, ToolSpec};
pub use provider::*;
pub use resource::*;
pub use retry::*;
pub use run::RunStatus;
pub use session::{SessionInfo, SessionStatus};
pub use tool::*;
pub use transcript::*;
pub use turn::*;
pub use wire::{PendingApprovalInfo, RequestId};

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DomainError {
    #[error("API_TYPE 必须是 openai-chat、anthropic-messages 或 ollama")]
    InvalidApiType,
    #[error("过期 owner：{0:?} 不匹配")]
    StaleOwner(OwnerDimension),
    #[error("未知 run 状态：{0}")]
    UnknownRunStatus(String),
    #[error("{0} generation 已耗尽，不能回绕复用身份")]
    GenerationExhausted(&'static str),
}
