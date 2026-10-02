//! 进程组合与首次配置；不持有会话、运行终态或上下文状态。
use std::path::{Path, PathBuf};

use agent_daemon_client::DaemonClient;
use anyhow::{Context, Result};
use serde_json::{Value, json};

use crate::config::{ConfigStore, ProfileSummary};
use crate::daemon::lifecycle::RuntimePaths;
use crate::provider::ProviderProfile;

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
