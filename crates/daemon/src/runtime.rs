use std::path::Path;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::Result;

use super::DaemonState;
use super::approval::ApprovalBroker;
use super::delegation_tool::DelegationTool;
use super::lifecycle::RuntimePaths;
use agent_daemon_protocol::JsonRpcRequest;
use agent_runtime::config::ConfigStore;
use agent_runtime::context::{ContextConfig, ContextManager};
use agent_runtime::cron::{AgentCronRunner, CronManager, CronStore, UnattendedApproval};
use agent_runtime::loop_engine::LoopEngine;
use agent_runtime::mcp::McpManager;
use agent_runtime::memory::{MemoryEvidenceTool, MemoryStore, RecallMemoryTool, RememberTool};
use agent_runtime::plan::{PlanStore, PlanTool};
use agent_runtime::provider::{Provider, ProviderManager, ProviderProfile};
use agent_runtime::safety::SafetyPolicy;
use agent_runtime::session::SessionStore;
use agent_runtime::skills::SkillLibrary;
use agent_runtime::tools::{EditFileTool, ExecTool, ReadFileTool, ToolRegistry, WriteFileTool};
use agent_storage::RunStore;

pub async fn build_daemon_state(workspace: &Path) -> Result<Arc<DaemonState>> {
    let runtime_identity = read_runtime_identity()?;
    let oidc = agent_identity::OidcVerifier::from_environment()?.map(Arc::new);
    anyhow::ensure!(
        oidc.as_ref()
            .is_none_or(|v| v.scope() == &runtime_identity.scope),
        "OIDC与运行域绑定不一致"
    );
    let scope = runtime_identity.scope.clone();
    let local = scope == agent_core::TenantContext::local();
    let cron_identity = if local {
        Some(agent_core::RuntimeIdentity::local_cron())
    } else if std::env::var_os("AGENT_CRON_IDENTITY").is_some() {
        let binding = read_configured_identity(
            "AGENT_CRON_IDENTITY",
            agent_core::RuntimeIdentity::local_cron(),
        )?;
        anyhow::ensure!(
            binding.scope == scope
                && binding.principal.kind == agent_core::SubjectKind::Workload
                && binding.principal.subject_id != runtime_identity.principal.subject_id,
            "Cron必须绑定同域独立workload"
        );
        Some(binding)
    } else {
        None
    };
    let cron_enabled = cron_identity.is_some();
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
    let backend = open_runtime_backend(workspace, &runtime_identity).await?;
    let repositories = backend.repositories;
    let writers = backend.writers;
    // 在任何 canonical legacy 导入和恢复修改前获得原 session 的 writer。
    let recovered = repositories
        .daemon_maintenance_store
        .recover_interrupted_work(&scope)
        .await?;
    // 启动期间也续租；配置/对象探针不应让已取得的 writer 在 listener 开放前过期。
    let mut startup_renewals = tokio::task::JoinSet::new();
    if let Some(store) = writers.clone() {
        startup_renewals.spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(3));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                if store.renew_bound_writers().await.is_err() {
                    tracing::warn!("启动 writer 续租未确认，开放 listener 前将再次核验");
                }
            }
        });
    }
    if local {
        memory
            .migrate_legacy(backend.legacy_memory.as_ref())
            .await?;
    }
    let plan = Arc::new(if local {
        PlanStore::from_env(workspace, repositories.plan_control_store.as_ref()).await?
    } else {
        PlanStore::memory_only()
    });
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
    let artifacts = artifact_service(
        backend.artifact_publications,
        backend.file_objects,
        scope.clone(),
    )
    .await?;
    let cron_runner = Arc::new(
        AgentCronRunner::new(
            provider.clone(),
            cron_tools,
            workspace.to_path_buf(),
            context_config.clone(),
        )
        .with_identity_binding(
            cron_identity.unwrap_or_else(agent_core::RuntimeIdentity::local_cron),
        )?
        .with_canonical_ports(
            agent_runtime::cron::CronRepositories {
                execution: repositories.run_execution_store.clone(),
                identity: repositories.identity_store.clone(),
                attempts: repositories.run_attempt_store.clone(),
                tools: repositories.run_tool_store.clone(),
                turns: repositories.run_turn_store.clone(),
                requests: repositories.run_request_store.clone(),
                discoveries: repositories.run_discovery_store.clone(),
                memories: repositories.memory_store.clone(),
                hook_execution: repositories.hook_execution_store.clone(),
                verifier: Arc::new(agent_policy::PolicyEngine),
            },
            artifacts.clone(),
        )
        .with_provider_manager(provider_manager.clone())
        .with_hooks(
            Arc::new(agent_runtime::hooks::HookDispatcher::load(workspace)),
            Arc::new(super::DeferredHookTaskOwner {
                state: delegation_daemon.clone(),
            }),
        ),
    );
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
    let collected = if local {
        repositories
            .daemon_maintenance_store
            .collect_local_artifacts(&scope, 86400000)
            .await?
    } else {
        0
    };
    tracing::debug!(collected, "启动时回收未引用 artifact");
    if recovered > 0 {
        tracing::warn!(recovered, "将重启时未确认的 run 标为 unknown_after_restart");
    }
    if local {
        for old in session.list_sessions().await? {
            let key = agent_core::SessionKey(old.id.clone());
            let metadata = repositories
                .session_control_store
                .read_session_metadata(&agent_core::TenantContext::local(), &key)
                .await?;
            if metadata
                .as_ref()
                .is_some_and(|m| m.deleted || m.legacy_imported)
            {
                continue;
            }
            let legacy = session.open_known_session(&old.id)?;
            let messages = legacy.load().await?;
            repositories
                .session_control_store
                .import_legacy_session(
                    &agent_core::TenantContext::local(),
                    &key,
                    metadata.as_ref().map(|m| &m.lifetime),
                    &messages,
                )
                .await?;
        }
    }
    let history = if !local
        || repositories
            .session_control_store
            .read_session_metadata(
                &agent_core::TenantContext::local(),
                &agent_core::SessionKey(session.current_id_sync()),
            )
            .await?
            .is_some_and(|m| m.legacy_imported)
    {
        Vec::new()
    } else {
        session.load().await?
    };
    if local {
        plan.migrate_legacy_with_ports(
            repositories.plan_control_store.as_ref(),
            repositories.session_control_store.as_ref(),
            &agent_core::SessionKey(session.current_id_sync()),
        )
        .await?;
    }

    let engine = Arc::new(
        LoopEngine::new(provider, tools, context, session.clone())
            .with_artifact_service(artifacts.clone()),
    );
    let state = Arc::new(
        DaemonState::bootstrap_scoped(
            engine,
            history,
            session,
            approvals,
            Some(skills),
            cron_enabled.then(|| cron.clone()),
            Some(mcp),
            RuntimePaths::for_workspace(workspace)?.log,
            Some(safety),
            Some(provider_manager),
            config_store,
            repositories,
            runtime_identity,
        )
        .await?
        .with_session_writers(writers)
        .with_artifact_service(artifacts)
        .with_oidc(oidc)
        .with_hooks(workspace),
    );
    // listener 尚未开放，有限恢复积压分批排空；已 claim 的 unknown 不重执行。
    while !state
        .hook_observation_store
        .read_pending_hooks(&scope)
        .await?
        .is_empty()
    {
        state.dispatch_hook_publications().await?;
        tokio::task::yield_now().await;
    }
    let _ = delegation_daemon.set(Arc::downgrade(&state));
    let mut cursor = None;
    loop {
        let page = state
            .queue_store
            .read_recoverable_queue(&scope, cursor, 32)
            .await?;
        for run_id in page.runs {
            let queued = state
                .queue_store
                .read_run_record(&scope, &run_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("恢复run缺少事实"))?;
            let message = state
                .queue_store
                .read_queued_message(&scope, &queued.session_id, &queued.run_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("恢复run缺少队列项"))?
                .message;
            let state = state.clone();
            let owner = state.clone();
            let accepted = state
                .spawn_owned(async move {
                    let (frames, mut receiver) = super::frames::frame_channel();
                    let request = JsonRpcRequest::new(
                        queued.request_id,
                        "chat.send",
                        serde_json::json!({"session_id":queued.session_id.0,"message":message}),
                    );
                    let process = owner.handle_request(request, frames);
                    let drain = async { while receiver.recv().await.is_some() {} };
                    tokio::join!(process, drain);
                })
                .await;
            if !accepted {
                return Err(anyhow::anyhow!("恢复时daemon已停止准入"));
            }
        }
        if !page.has_more {
            break;
        }
        if page.cursor == cursor {
            return Err(anyhow::anyhow!("队列恢复游标未推进"));
        }
        cursor = page.cursor;
        tokio::task::yield_now().await;
    }
    startup_renewals.abort_all();
    while startup_renewals.join_next().await.is_some() {}
    if let Some(store) = &state.session_writers {
        store.renew_bound_writers().await?;
    }
    if cron_enabled {
        cron.start(state.shutdown.clone()).await;
    }
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

// 启动时显式选择，错误配置不得静默退回 Local 文件后端。
async fn artifact_service(
    store: Arc<dyn agent_storage_ports::ArtifactPublicationRepository>,
    file_objects: Arc<dyn agent_storage_ports::ArtifactStore>,
    scope: agent_core::TenantContext,
) -> Result<Option<Arc<agent_artifact::ArtifactService>>> {
    let backend = match std::env::var("AGENT_ARTIFACT_BACKEND") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(_) => anyhow::bail!("artifact backend 配置无效"),
    };
    let objects: Arc<dyn agent_storage_ports::ArtifactStore> = match backend.as_str() {
        "file-v1" => {
            anyhow::ensure!(
                scope == agent_core::TenantContext::local(),
                "企业artifact必须使用受治理对象存储"
            );
            file_objects
        }
        "s3" => {
            let required =
                |name| std::env::var(name).map_err(|_| anyhow::anyhow!("缺少 S3 启动配置 {name}"));
            let ca = match std::env::var("AGENT_S3_ROOT_CA") {
                Ok(path) => Some(
                    tokio::fs::read(path)
                        .await
                        .map_err(|_| anyhow::anyhow!("无法读取 S3 CA"))?,
                ),
                Err(std::env::VarError::NotPresent) => None,
                Err(_) => anyhow::bail!("S3 CA 配置无效"),
            };
            Arc::new(
                agent_artifact::S3ArtifactStore::new(agent_artifact::S3ArtifactConfig {
                    endpoint: required("AGENT_S3_ENDPOINT")?,
                    bucket: required("AGENT_S3_BUCKET")?,
                    region: required("AGENT_S3_REGION")?,
                    prefix: required("AGENT_S3_PREFIX")?,
                    access_key: required("AGENT_S3_ACCESS_KEY")?,
                    secret_key: required("AGENT_S3_SECRET_KEY")?,
                    root_ca_pem: ca,
                })
                .await?,
            )
        }
        _ => anyhow::bail!("artifact backend 配置无效"),
    };
    Ok(Some(Arc::new(agent_artifact::ArtifactService::new(
        store, objects, scope,
    ))))
}

/// 启动句柄集合；数据库事实全部来自 repositories 的同一个 adapter。
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StorageCutover {
    version: u32,
    state: String,
    migration_id: String,
    source_digest: String,
    target: StorageTarget,
}
#[derive(serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct StorageTarget {
    host: String,
    port: u16,
    database: String,
    schema: String,
}
fn read_cutover(workspace: &Path) -> Result<Option<StorageCutover>> {
    use std::io::Read;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let path = workspace.join(".my-agent/storage-cutover.json");
    let file = match std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
        .open(path)
    {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => anyhow::bail!("存储切换marker不可读取"),
    };
    anyhow::ensure!(
        file.metadata()?.is_file() && file.metadata()?.permissions().mode() & 0o077 == 0,
        "切换marker权限无效"
    );
    let mut bytes = Vec::new();
    file.take(1024 * 1024 + 1).read_to_end(&mut bytes)?;
    anyhow::ensure!(bytes.len() <= 1024 * 1024, "切换marker超预算");
    let marker: StorageCutover = serde_json::from_slice(&bytes)?;
    anyhow::ensure!(
        marker.version == 1 && matches!(marker.state.as_str(), "postgres" | "rolled_back"),
        "切换marker版本/状态无效"
    );
    Ok(Some(marker))
}
struct RuntimeBackend {
    repositories: crate::DaemonRepositories,
    writers: Option<Arc<dyn agent_storage_ports::SessionWriterRepository>>,
    legacy_memory: Arc<dyn agent_storage_ports::LegacyMemoryImportRepository>,
    artifact_publications: Arc<dyn agent_storage_ports::ArtifactPublicationRepository>,
    file_objects: Arc<dyn agent_storage_ports::ArtifactStore>,
}
fn read_runtime_identity() -> Result<agent_core::RuntimeIdentity> {
    let binding = read_configured_identity(
        "AGENT_RUNTIME_IDENTITY",
        agent_core::RuntimeIdentity::local(),
    )?;
    anyhow::ensure!(
        binding.scope != agent_core::TenantContext::local()
            || binding == agent_core::RuntimeIdentity::local(),
        "Local保留域不能改绑主体"
    );
    Ok(binding)
}
pub(crate) fn read_configured_identity(
    name: &str,
    default: agent_core::RuntimeIdentity,
) -> Result<agent_core::RuntimeIdentity> {
    use std::io::Read;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let path = match std::env::var_os(name) {
        None => return Ok(default),
        Some(path) => std::path::PathBuf::from(path),
    };
    anyhow::ensure!(path.is_absolute(), "运行身份必须使用受保护的绝对配置路径");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
        .open(path)
        .map_err(|_| anyhow::anyhow!("运行身份配置不可读取"))?;
    let metadata = file.metadata()?;
    anyhow::ensure!(
        metadata.is_file() && metadata.permissions().mode() & 0o077 == 0,
        "运行身份配置权限无效"
    );
    let mut bytes = Vec::new();
    file.take(65537).read_to_end(&mut bytes)?;
    anyhow::ensure!(bytes.len() <= 65536, "运行身份配置超预算");
    let identity: agent_core::RuntimeIdentity =
        serde_json::from_slice(&bytes).map_err(|_| anyhow::anyhow!("运行身份配置格式无效"))?;
    Ok(identity)
}

async fn open_runtime_backend(
    workspace: &Path,
    binding: &agent_core::RuntimeIdentity,
) -> Result<RuntimeBackend> {
    use agent_storage_ports::IdentityReadRepository;
    let selected = match std::env::var("AGENT_STORAGE_BACKEND") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => "sqlite".into(),
        Err(_) => anyhow::bail!("存储后端配置无效"),
    };
    let cutover = read_cutover(workspace)?;
    let writer = match std::env::var("AGENT_SESSION_WRITER") {
        Ok(value) if value == "1" => true,
        Ok(value) if value == "0" => false,
        Err(std::env::VarError::NotPresent) => {
            matches!(selected.as_str(), "postgres-local" | "postgres")
        }
        _ => anyhow::bail!("session writer 租约配置无效"),
    };
    match selected.as_str() {
        "sqlite" => {
            anyhow::ensure!(
                binding.scope == agent_core::TenantContext::local(),
                "企业运行身份必须使用Postgres事实源"
            );
            anyhow::ensure!(
                cutover.as_ref().is_none_or(|m| m.state == "rolled_back"),
                "事实源已切换到Postgres；必须先完成离线回滚"
            );
            let store = RunStore::open(&workspace.join(".my-agent/runtime.sqlite3"))?;
            let store = Arc::new(if writer {
                store.enable_session_writer()?
            } else {
                store
            });
            Ok(RuntimeBackend {
                repositories: crate::DaemonRepositories::from_sqlite(store.clone()),
                writers: writer.then(|| {
                    store.clone() as Arc<dyn agent_storage_ports::SessionWriterRepository>
                }),
                legacy_memory: store.clone(),
                artifact_publications: store.clone(),
                file_objects: store,
            })
        }
        "postgres-local" | "postgres" => {
            anyhow::ensure!(writer, "Postgres 启动必须启用 session writer");
            tokio::fs::create_dir_all(workspace.join(".my-agent")).await?;
            let required = |name| {
                std::env::var(name).map_err(|_| anyhow::anyhow!("缺少 Postgres 启动配置 {name}"))
            };
            let remote = if selected == "postgres" {
                Some(agent_storage_postgres::PgConnectionConfig::from_env()?)
            } else {
                None
            };
            let database = remote
                .as_ref()
                .map(|c| c.database.clone())
                .map_or_else(|| required("AGENT_PUBLICATION_DB"), Ok)?;
            let user = remote
                .as_ref()
                .map(|c| c.user.clone())
                .map_or_else(|| required("AGENT_PUBLICATION_USER"), Ok)?;
            let schema = remote
                .as_ref()
                .map(|c| c.schema.clone())
                .map_or_else(|| required("AGENT_PUBLICATION_SCHEMA"), Ok)?;
            use sha2::{Digest, Sha256};
            // 远程host/port也属于事实源身份，不能跨数据库回收文件。
            let identity = StorageTarget {
                host: remote
                    .as_ref()
                    .map_or_else(|| "/tmp".into(), |c| c.host.clone()),
                port: remote.as_ref().map_or(5432, |c| c.port),
                database: database.clone(),
                schema: schema.clone(),
            };
            let directory_id = if let Some(c) = &remote {
                format!(
                    "{:x}",
                    Sha256::digest(serde_json::to_vec(&(&database, &schema, &c.host, c.port))?)
                )
            } else {
                format!(
                    "{:x}",
                    Sha256::digest(serde_json::to_vec(&(&database, &schema))?)
                )
            };
            let store = match remote {
                Some(config) => {
                    agent_storage_postgres::PgCanonicalStore::connect_remote(&config).await?
                }
                None => {
                    agent_storage_postgres::PgCanonicalStore::connect_local(
                        &database, &user, &schema,
                    )
                    .await?
                }
            };
            let artifact_path = if let Some(marker) = cutover {
                anyhow::ensure!(
                    marker.state == "postgres" && marker.target == identity,
                    "切换目标与启动事实源不匹配"
                );
                store
                    .validate_cutover_receipt(&marker.migration_id, &marker.source_digest)
                    .await?;
                workspace.join(".my-agent/runtime.artifacts")
            } else {
                workspace.join(format!(".my-agent/runtime.pg.{directory_id}.artifacts"))
            };
            let store = store.with_local_file_artifacts(artifact_path).await?;
            let scope = binding.scope.clone();
            let subject = binding.principal.subject_id.clone();
            match store.read_identity(&scope, &subject).await {
                Ok(identity) => {
                    agent_identity::bound_context(
                        binding,
                        &identity,
                        agent_core::RequestId::String("startup".into()),
                    )?;
                }
                Err(agent_core::RepositoryError::Authorization(
                    agent_core::AuthorizationError::Forbidden,
                )) if scope == agent_core::TenantContext::local() => {
                    // 仅初始化空 Local 域；现有域缺主体、撤权或策略错误均不得覆盖。
                    store
                        .provision_initial_domain(
                            &scope,
                            &agent_core::AuthenticatedPrincipal {
                                subject_id: subject,
                                kind: agent_core::SubjectKind::Human,
                                revocation_epoch: 0,
                            },
                            &[agent_core::EnterpriseRole::TenantAdmin],
                            &agent_core::PolicyDocument::local(),
                        )
                        .await?;
                }
                Err(error) => return Err(error.into()),
            }
            if scope == agent_core::TenantContext::local() {
                let cron = store
                    .read_identity(
                        &scope,
                        &agent_core::LOCAL_CRON_SUBJECT.to_owned().try_into()?,
                    )
                    .await?;
                anyhow::ensure!(
                    cron.principal.kind == agent_core::SubjectKind::Workload,
                    "Postgres Local Cron 主体类型无效"
                );
            }
            let store = Arc::new(store);
            Ok(RuntimeBackend {
                repositories: crate::DaemonRepositories::from_backend(
                    store.clone(),
                    #[cfg(any(test, feature = "test-support"))]
                    None,
                    #[cfg(any(test, feature = "test-support"))]
                    None,
                ),
                writers: Some(store.clone()),
                legacy_memory: store.clone(),
                artifact_publications: store.clone(),
                file_objects: store,
            })
        }
        _ => anyhow::bail!(
            "AGENT_STORAGE_BACKEND 仅支持 sqlite、postgres-local 或 postgres（严格TLS）"
        ),
    }
}
