#![forbid(unsafe_code)]
//! 共享入口恢复与协议调用；不持有业务状态或服务端 owner。
pub mod recovery;
pub mod rpc;
pub mod web;
