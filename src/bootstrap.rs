//! 进程组合与首次配置；不持有会话、运行终态或上下文状态。
use agent_acp::editor::run_acp_server;
use agent_daemon::lifecycle::DaemonStatus;
use agent_daemon::runtime::build_daemon_state;
use agent_daemon::server::run_unix_server;
use agent_entry_support::rpc::request_result;
use agent_gateway::serve::run_http_server_optional;
use agent_runtime::config;
use std::fmt::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use agent_daemon_client::DaemonClient;
use anyhow::{Context, Result};
use serde_json::{Value, json};

use agent_daemon::lifecycle::RuntimePaths;
use agent_runtime::config::{ConfigStore, ProfileSummary};
use agent_runtime::provider::ProviderProfile;

pub async fn connect_workspace(workspace: &Path) -> Result<DaemonClient> {
    let paths = RuntimePaths::for_workspace(workspace)?;
    paths.ensure_daemon(workspace).await?;
    Ok(DaemonClient::connect_unix(&paths.socket).await?)
}

pub fn config_path() -> PathBuf {
    ConfigStore::default().path().to_path_buf()
}

pub fn active_model(fallback: &str) -> String {
    ConfigStore::default()
        .active_profile()
        .ok()
        .flatten()
        .map(|profile| profile.model)
        .unwrap_or_else(|| fallback.to_owned())
}

pub fn list_models() -> Result<Value> {
    let store = ConfigStore::default();
    let config = store.load()?;
    let mut profiles = config
        .profiles
        .iter()
        .map(ProfileSummary::from_profile)
        .collect::<Vec<_>>();
    let active_id = config
        .active_profile
        .clone()
        .or_else(|| ProviderProfile::from_env().ok().map(|profile| profile.id));
    if profiles.is_empty()
        && let Ok(profile) = ProviderProfile::from_env()
    {
        profiles.push(ProfileSummary::from_profile(&profile));
    }
    Ok(json!({
        "active_id":active_id,"profiles":profiles,"config_path":store.path(),
        "providers":[
            {"api_type":"openai-chat","label":"OpenAI 兼容"},
            {"api_type":"anthropic-messages","label":"Anthropic Messages"},
            {"api_type":"ollama","label":"Ollama 本地模型"}
        ]
    }))
}

pub fn save_configuration(value: Value, activate: bool) -> Result<Value> {
    let store = ConfigStore::default();
    let profile: ProviderProfile = serde_json::from_value(value).context("模型配置格式无效")?;
    let config = store.upsert(profile, activate)?;
    let active_id = config.active_profile.context("保存后没有活动模型配置")?;
    let active = config
        .profiles
        .iter()
        .find(|profile| profile.id == active_id)
        .context("活动模型配置不存在")?;
    Ok(json!({"changed":true,"active_id":active.id,
        "profile":ProfileSummary::from_profile(active),"config_path":store.path()}))
}

pub fn activate_configuration(profile_id: &str) -> Result<Value> {
    let store = ConfigStore::default();
    let profile = store.activate(profile_id)?;
    Ok(json!({"changed":true,"active_id":profile.id,
        "profile":ProfileSummary::from_profile(&profile),"config_path":store.path()}))
}

pub fn load_persisted_environment() {
    let store = ConfigStore::default();
    let Ok(Some(profile)) = store.active_profile() else {
        return;
    };
    // main 在启动 Tokio runtime 前调用，避免运行中并发修改进程环境。
    set_if_missing("API_TYPE", profile.api_type.as_str());
    set_if_missing("OPENAI_BASE_URL", &profile.base_url);
    set_if_missing("MODEL_NAME", &profile.model);
    if let Some(api_key) = profile.api_key.as_deref() {
        set_if_missing("OPENAI_API_KEY", api_key);
    }
}

fn set_if_missing(name: &str, value: &str) {
    if std::env::var_os(name).is_none() {
        // SAFETY: called once during process startup before any worker thread is spawned.
        unsafe { std::env::set_var(name, value) };
    }
}

/// 根 binary 唯一进程组合实现；适配库只依赖 GatewayHost 接口。
pub struct BootstrapHost;
#[async_trait::async_trait]
impl agent_gateway::host::GatewayHost for BootstrapHost {
    async fn connect_workspace(&self, workspace: &Path) -> Result<DaemonClient> {
        connect_workspace(workspace).await
    }
    fn active_model(&self, fallback: &str) -> String {
        active_model(fallback)
    }
    fn config_path(&self) -> PathBuf {
        config_path()
    }
    fn list_models(&self) -> Result<Value> {
        list_models()
    }
    fn save_configuration(&self, value: Value, activate: bool) -> Result<Value> {
        save_configuration(value, activate)
    }
    fn activate_configuration(&self, profile_id: &str) -> Result<Value> {
        activate_configuration(profile_id)
    }
}
async fn run_editor_command(workspace: &Path, acp_v2: bool) -> Result<()> {
    config::validate_environment()?;
    let paths = RuntimePaths::for_workspace(workspace)?;
    paths.ensure_daemon(workspace).await?;
    let client = agent_daemon_client::DaemonClient::connect_unix(&paths.socket).await?;
    if acp_v2 {
        agent_acp::editor_v2::run_acp_v2_server(client, workspace.to_path_buf()).await
    } else {
        run_acp_server(client, workspace.to_path_buf()).await
    }
}

async fn run_serve_command(workspace: &Path, bind: SocketAddr) -> Result<()> {
    let bearer_token = std::env::var("MY_AGENT_API_TOKEN")
        .ok()
        .filter(|value| !value.trim().is_empty());
    if !bind.ip().is_loopback() && bearer_token.is_none() {
        anyhow::bail!("非回环地址 {bind} 必须设置 MY_AGENT_API_TOKEN；建议默认使用 127.0.0.1:8787");
    }
    let client = if config::check_environment().is_empty() {
        let paths = RuntimePaths::for_workspace(workspace)?;
        paths.ensure_daemon(workspace).await?;
        Some(agent_daemon_client::DaemonClient::connect_unix(&paths.socket).await?)
    } else {
        None
    };
    let model = std::env::var("MODEL_NAME").unwrap_or_else(|_| "未配置".to_owned());
    run_http_server_optional(
        client,
        bind,
        model,
        bearer_token,
        workspace.to_path_buf(),
        std::sync::Arc::new(BootstrapHost),
    )
    .await
}

async fn run_daemon_command(workspace: &Path) -> Result<()> {
    config::validate_environment()?;
    let paths = RuntimePaths::for_workspace(workspace)?;
    if matches!(paths.status().await, DaemonStatus::Ready { .. }) {
        anyhow::bail!("该工作区的 daemon 已在运行");
    }
    let state = build_daemon_state(workspace).await?;
    run_unix_server(state, &paths, workspace).await
}

async fn run_status_command(workspace: &Path) -> Result<String> {
    let mut output = String::new();
    let paths = RuntimePaths::for_workspace(workspace)?;
    match paths.status().await {
        DaemonStatus::Ready { pid } => writeln!(
            &mut output,
            "ready · pid={pid} · socket={} · log={} · sessions={}",
            paths.socket.display(),
            paths.log.display(),
            workspace.join(".my-agent").display()
        )?,
        DaemonStatus::Starting { pid } => writeln!(
            &mut output,
            "starting · pid={pid:?} · log={} · sessions={}",
            paths.log.display(),
            workspace.join(".my-agent").display()
        )?,
        DaemonStatus::Stale { pid } => writeln!(
            &mut output,
            "stale · pid={pid:?} · log={} · 可再次运行 `my-agent` 自动清理并重启",
            paths.log.display()
        )?,
        DaemonStatus::Stopped => writeln!(&mut output, "stopped · log={}", paths.log.display())?,
    }
    Ok(output)
}

async fn run_stop_command(workspace: &Path) -> Result<String> {
    let mut output = String::new();
    let paths = RuntimePaths::for_workspace(workspace)?;
    match paths.status().await {
        DaemonStatus::Ready { .. } => {
            let client = agent_daemon_client::DaemonClient::connect_unix(&paths.socket).await?;
            request_result(&client, "daemon.stop", json!({})).await?;
            writeln!(
                &mut output,
                "已请求 daemon 优雅停止；正在执行的 turn 不会被超时强杀。"
            )?
        }
        DaemonStatus::Stale { .. } => {
            paths.cleanup().await;
            writeln!(&mut output, "已清理失效的 daemon 运行标记。")?
        }
        DaemonStatus::Starting { pid } => writeln!(
            &mut output,
            "daemon 正在启动（pid={pid:?}），请稍后重试 stop。"
        )?,
        DaemonStatus::Stopped => writeln!(&mut output, "daemon 未运行。")?,
    }
    Ok(output)
}

#[async_trait::async_trait]
impl agent_cli::command::CommandHost for BootstrapHost {
    async fn connect_workspace(&self, workspace: &Path) -> Result<DaemonClient> {
        connect_workspace(workspace).await
    }
    fn validate_environment(&self) -> Result<()> {
        config::validate_environment()
    }
    async fn run_editor(&self, workspace: &Path, v2: bool) -> Result<()> {
        run_editor_command(workspace, v2).await
    }
    async fn run_server(&self, workspace: &Path, bind: SocketAddr) -> Result<()> {
        run_serve_command(workspace, bind).await
    }
    async fn run_daemon(&self, workspace: &Path) -> Result<()> {
        run_daemon_command(workspace).await
    }
    async fn status_message(&self, workspace: &Path) -> Result<String> {
        run_status_command(workspace).await
    }
    async fn stop_message(&self, workspace: &Path) -> Result<String> {
        run_stop_command(workspace).await
    }
    async fn offline_sessions(&self, workspace: &Path) -> Result<Vec<agent_core::SessionInfo>> {
        agent_daemon::maintenance::legacy_session_listing(workspace).await
    }
    fn config_path(&self) -> PathBuf {
        config_path()
    }
    fn config_issues(&self) -> Vec<(String, String)> {
        config::check_environment()
            .into_iter()
            .map(|issue| (issue.variable.into(), issue.message))
            .collect()
    }
    fn saved_models(&self) -> Result<(Option<String>, Vec<agent_cli::command::SavedModel>)> {
        let file = ConfigStore::default().load()?;
        let profiles = file
            .profiles
            .iter()
            .map(ProfileSummary::from_profile)
            .map(|p| agent_cli::command::SavedModel {
                id: p.id,
                name: p.name,
                api_type: p.api_type.to_string(),
                model: p.model,
                has_api_key: p.has_api_key,
            })
            .collect();
        Ok((file.active_profile, profiles))
    }
    fn log_path(&self, workspace: &Path) -> Result<PathBuf> {
        Ok(RuntimePaths::for_workspace(workspace)?.log)
    }
    fn evaluate(&self) -> Result<(Value, bool)> {
        let report = agent_memory::evaluate_builtin(|text| {
            agent_context::TokenEstimator
                .messages(&[agent_core::Message::text(agent_core::Role::System, text)])
        })?;
        let context = agent_context::evaluate_builtin()?;
        let passed = report.is_passed()
            && context["passed"] == context["total"]
            && context["total"].as_u64().is_some_and(|n| n > 0);
        let mut output = serde_json::to_value(report)?;
        output["context"] = context;
        Ok((output, passed))
    }
}
