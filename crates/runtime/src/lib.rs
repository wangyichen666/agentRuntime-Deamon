#![forbid(unsafe_code)]
//! 执行引擎与 Provider/工具；持久事实由 repository 端口唯一持有。
pub mod config;
pub mod context;
pub mod cron;
pub mod hooks;
pub mod loop_engine;
pub mod mcp;
pub mod mcp_http;
pub mod memory;
pub mod plan;
pub mod provider;
pub mod safety;
pub mod secrets;
pub mod session;
pub mod skills;
pub mod tool_calls;
pub mod tools;
