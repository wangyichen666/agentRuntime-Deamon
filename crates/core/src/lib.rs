//! 共享领域值；不依赖入口、执行内核、daemon 或具体存储。
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::print_stdout)
)]

mod access;
mod artifact;
mod attempt;
mod context;
mod control;
mod delegation;
pub use artifact::*;
mod coordination;
mod enterprise;
mod hooks;
mod identity;
mod ledger;
mod billing;
mod registry;
mod workflow;
pub use workflow::*;
pub use registry::*;
mod memory;
mod message;
mod plan;
mod provider;
mod provider_request;
mod repository;
mod resource;
mod retry;
mod run;
mod session;
mod tool;
mod tool_search;
mod transcript;
mod turn;
mod view;
mod wire;

pub use access::*;
pub use attempt::*;
pub use context::*;
pub use control::*;
pub use coordination::*;
pub use delegation::*;
pub use enterprise::*;
pub use hooks::*;
pub use identity::*;
pub use ledger::*;
pub use billing::*;
pub use memory::*;
pub use message::{Message, Role, ToolCall, ToolSpec};
pub use plan::*;
pub use provider::*;
pub use provider_request::*;
pub use repository::*;
pub use resource::*;
pub use retry::*;
pub use run::RunOutcomeUnknown;
pub use run::{RunStatus, normalize_run_terminal_status};
pub use session::{SessionInfo, SessionStatus};
pub use tool::*;
pub use tool_search::*;
pub use transcript::*;
pub use turn::*;
pub use view::*;
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

mod writer;
pub use writer::*;
