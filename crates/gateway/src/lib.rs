#![forbid(unsafe_code)]
//! HTTP/WS 与 Web 资源适配；host 只提供进程组合和初始配置。
pub mod host;
mod rate_limit;
mod scope_routes;
pub mod serve;
