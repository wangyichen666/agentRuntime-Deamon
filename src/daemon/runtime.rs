use crate::storage::MaintenanceRepository;
use std::path::Path;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::Result;

use super::DaemonState;
use super::approval::ApprovalBroker;
use super::delegation_tool::DelegationTool;
use super::lifecycle::RuntimePaths;
use crate::config::ConfigStore;
use crate::context::{ContextConfig, ContextManager};
use crate::cron::{AgentCronRunner, CronManager, CronStore, UnattendedApproval};
use crate::daemon::protocol::JsonRpcRequest;
use crate::loop_engine::LoopEngine;
use crate::mcp::McpManager;
use crate::memory::{MemoryEvidenceTool, MemoryStore, RecallMemoryTool, RememberTool};
use crate::plan::{PlanStore, PlanTool};
use crate::provider::{Provider, ProviderManager, ProviderProfile};
use crate::safety::SafetyPolicy;
use crate::session::SessionStore;
use crate::skills::SkillLibrary;
use crate::storage::{RunStore, SessionQuery, TranscriptStore};
use crate::tools::{EditFileTool, ExecTool, ReadFileTool, ToolRegistry, WriteFileTool};

pub async fn build_daemon_state(workspace: &Path) -> Result<Arc<DaemonState>> {
    let config_store = ConfigStore::default();
    let profile = config_store
        .active_profile()?
        .or_else(|| ProviderProfile::from_env().ok())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "没有可用的模型配置；请运行 `myagent config check` 或先在 Web 设置中保存模型"
            )
        })?;
    let provider_manager = Arc::new(ProviderManager::new(profile)?);
    let configured = config_store.load()?;
    let fallback_profiles = configured
        .fallback_profile_ids
        .iter()
        .filter_map(|id| {
            configured
                .profiles
                .iter()
                .find(|profile| &profile.id == id)
                .cloned()
        })
        .collect();
    provider_manager.set_fallbacks(fallback_profiles)?;
    let provider: Arc<dyn Provider> = provider_manager.clone();
    let approvals = ApprovalBroker::new();
    let safety = Arc::new(SafetyPolicy::new(workspace, Arc::new(approvals.clone()))?);
    let mut tools = ToolRegistry::new();
    tools.register(ReadFileTool::from_env(safety.clone())?);
    tools.register(ExecTool::new(safety.clone()));
    tools.register(WriteFileTool::new(safety.clone()));
    tools.register(EditFileTool::new(safety.clone()));
    let memory = Arc::new(MemoryStore::from_env(workspace));
    tools.register(RememberTool::new(memory.clone()));
    tools.register(RecallMemoryTool::new(memory.clone()));
    tools.register(MemoryEvidenceTool);
    let delegation_daemon = Arc::new(OnceLock::new());
    tools.register(super::resources::BackgroundTool::new(
        delegation_daemon.clone(),
    ));
    for name in [
        "resource_list",
        "resource_status",
        "resource_logs",
        "resource_wait",
        "resource_stop",
    ] {
        tools.register(super::resources::ResourceTool::new(
            name,
            delegation_daemon.clone(),
        ));
    }
    let context_config = ContextConfig::from_env()?;
    let run_store = Arc::new(RunStore::open(
        &workspace.join(".my-agent/runtime.sqlite3"),
    )?);
    memory.migrate_legacy(&run_store).await?;
    let plan = Arc::new(PlanStore::from_env(workspace, &*run_store).await?);
    tools.register(PlanTool::new(plan.clone()));
    tools.register(DelegationTool::new("sub_agent", delegation_daemon.clone()));
    tools.register(DelegationTool::new(
        "spawn_subagent",
        delegation_daemon.clone(),
    ));
    tools.register(DelegationTool::new(
        "wait_subagents",
        delegation_daemon.clone(),
    ));
    tools.register(DelegationTool::new(
        "cancel_subagent",
        delegation_daemon.clone(),
    ));
    tools.register(DelegationTool::new(
        "list_subagents",
        delegation_daemon.clone(),
    ));
    let skills = SkillLibrary::from_env(workspace);
    skills.refresh().await?;
    let mcp = Arc::new(McpManager::new(workspace, safety.clone()));
    mcp.reload().await;
    tools.register_dynamic_source(mcp.clone());

    let cron_safety = Arc::new(SafetyPolicy::new(workspace, Arc::new(UnattendedApproval))?);
    let mut cron_tools = ToolRegistry::new();
    cron_tools.register(ReadFileTool::from_env(cron_safety.clone())?);
    cron_tools.register(ExecTool::new(cron_safety.clone()));
    cron_tools.register(WriteFileTool::new(cron_safety.clone()));
    cron_tools.register(EditFileTool::new(cron_safety));
    cron_tools.register(RememberTool::new(memory.clone()));
    cron_tools.register(RecallMemoryTool::new(memory));
    cron_tools.register(MemoryEvidenceTool);
    let cron_store = Arc::new(CronStore::load_best_effort(workspace).await);
    let heartbeat = env_bool("HEARTBEAT_ENABLED", false)
        .then(|| Duration::from_secs(env_u64("HEARTBEAT_INTERVAL_SECS", 300)));
    let cron_runner = Arc::new(AgentCronRunner::new(
        provider.clone(),
        cron_tools,
        workspace.to_path_buf(),
        context_config.clone(),
    ));
    let cron = Arc::new(CronManager::new(
        cron_store,
        cron_runner,
        Duration::from_secs(env_u64("CRON_TICK_SECONDS", 1).max(1)),
        Duration::from_secs(env_u64("CRON_STAGGER_SECONDS", 2)),
        heartbeat,
    ));
    let context = ContextManager::new_with_skills(
        provider.clone(),
        workspace,
        context_config,
        plan.clone(),
        skills.clone(),
    )?;
    let session = Arc::new(SessionStore::from_env(workspace));
    let recovered = run_store.recover()?;
    let collected = run_store.gc_artifacts(std::time::Duration::from_secs(86400))?;
    tracing::debug!(collected, "启动时回收未引用 artifact");
    if recovered > 0 {
        tracing::warn!(recovered, "将重启时未确认的 run 标为 unknown_after_restart");
    }
    for old in session.list_sessions().await? {
        if run_store
            .session_metadata(&agent_core::SessionKey(old.id.clone()))?
            .is_some_and(|m| m.deleted || m.legacy_imported)
        {
            continue;
        }
        let legacy = session.open_known_session(&old.id)?;
        let messages = legacy.load().await?;
        run_store.import_legacy(&agent_core::SessionKey(old.id), &messages)?;
    }
    let history = if run_store
        .session_metadata(&agent_core::SessionKey(session.current_id_sync()))?
        .is_some_and(|m| m.legacy_imported)
    {
        Vec::new()
    } else {
        session.load().await?
    };
    plan.migrate_legacy(
        &*run_store,
        &agent_core::SessionKey(session.current_id_sync()),
    )
    .await?;
    let engine = Arc::new(LoopEngine::new(provider, tools, context, session.clone()));
    let state = Arc::new(
        DaemonState::new_with_services_and_log_path_and_safety_and_provider(
            engine,
            history,
            session,
            approvals,
            Some(skills),
            Some(cron.clone()),
            Some(mcp),
            RuntimePaths::for_workspace(workspace)?.log,
            Some(safety),
            Some(provider_manager),
            config_store,
            run_store,
        )?,
    );
    let _ = delegation_daemon.set(Arc::downgrade(&state));
    for queued in state.run_store.recoverable_queued()? {
        let message = state
            .run_store
            .queued_message(&queued.session_id.0, &queued.run_id)?
            .ok_or_else(|| anyhow::anyhow!("queued run {} 缺少队列项", queued.run_id.0))?
            .message;
        let state = state.clone();
        let owner = state.clone();
        let accepted = state
            .spawn_owned(async move {
                let (frames, mut receiver) = super::frames::frame_channel();
                let request = JsonRpcRequest::new(
                    queued.request_id,
                    "chat.send",
                    serde_json::json!({"session_id": queued.session_id.0, "message": message}),
                );
                let process = owner.handle_request(request, frames);
                let drain = async { while receiver.recv().await.is_some() {} };
                tokio::join!(process, drain);
            })
            .await;
        debug_assert!(accepted, "daemon 尚未进入 shutdown");
    }
    cron.start(state.shutdown.clone()).await;
    Ok(state)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name).ok().map_or(default, |value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}
