#![forbid(unsafe_code)]
pub mod approval;
mod compact;
mod context_readback;
pub mod delegation_tool;
mod frames;
pub mod handlers;
pub mod lifecycle;
mod repositories;
mod workflow;
pub mod resources;
pub use repositories::DaemonRepositories;
pub mod runtime;
pub mod server;

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::{Mutex, Notify, broadcast};
use tokio::task::JoinSet;

use self::approval::ApprovalBroker;
use agent_daemon_protocol::{EventFrame, EventKind, JsonRpcResponse, RequestId, ServerFrame};
use agent_runtime::config::ConfigStore;
use agent_runtime::cron::CronManager;
use agent_runtime::loop_engine::{CancellationToken, LoopEngine};
use agent_runtime::mcp::McpManager;
use agent_runtime::provider::{FrozenRoute, Message, ProviderManager};
use agent_runtime::safety::SafetyPolicy;
use agent_runtime::session::SessionStore;
use agent_runtime::skills::SkillLibrary;
use agent_storage::SessionLifecycle;
use agent_storage::{EventSeq, RunId, RunStore, SessionQuery, TranscriptStore};

#[cfg(any(test, feature = "test-support"))]
use agent_storage::{ControlRepository, MaintenanceRepository};

pub struct DaemonState {
    pub(crate) workflow_store: Arc<dyn agent_storage_ports::WorkflowRepository>,
    pub(crate) registry_store: Arc<dyn agent_storage_ports::RegistryRepository>,
    pub(crate) billing_store: Arc<dyn agent_storage_ports::BillingRepository>,
    pub(crate) credential_store: Arc<dyn agent_storage_ports::CredentialRepository>,
    pub(crate) access_store: Arc<dyn agent_storage_ports::AccessRepository>,
    pub(crate) oidc: Option<Arc<agent_identity::OidcVerifier>>,
    pub(crate) runtime_identity: agent_core::RuntimeIdentity,
    pub(crate) session_supervisor: SessionSupervisor,
    pub(crate) run_coordinator: RunCoordinator,
    pub(crate) session: Arc<SessionStore>,
    pub(crate) approvals: ApprovalBroker,
    pub(crate) safety: Option<Arc<SafetyPolicy>>,
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) run_store: Option<Arc<dyn ControlRepository>>,
    pub(crate) daemon_maintenance_store: Arc<dyn agent_storage_ports::DaemonMaintenanceRepository>,
    pub(crate) observation_store: Arc<dyn agent_storage_ports::RuntimeObservationRepository>,
    pub(crate) plan_control_store: Arc<dyn agent_storage_ports::PlanControlRepository>,
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) maintenance_store: Option<Arc<dyn MaintenanceRepository>>,
    pub(crate) publication_store: Arc<dyn agent_storage_ports::PublicationRepository>,
    pub(crate) readback_store: Arc<dyn agent_storage_ports::SessionReadRepository>,
    pub(crate) audit_store: Arc<dyn agent_storage_ports::AuditReadRepository>,
    pub(crate) quota_store: Arc<dyn agent_storage_ports::QuotaRepository>,
    pub(crate) quota_management: Arc<dyn agent_storage_ports::QuotaManagementRepository>,
    pub(crate) identity_management: Arc<dyn agent_storage_ports::IdentityManagementRepository>,
    pub(crate) identity_store: Arc<dyn agent_storage_ports::IdentityReadRepository>,
    pub(crate) event_store: Arc<dyn agent_storage_ports::RunEventRepository>,
    pub(crate) queue_store: Arc<dyn agent_storage_ports::RunQueueRepository>,
    pub(crate) run_fact_store: Arc<dyn agent_storage_ports::RunFactRepository>,
    pub(crate) run_execution_store: Arc<dyn agent_storage_ports::RunExecutionRepository>,
    pub(crate) run_request_store: Arc<dyn agent_storage_ports::RunContextRepository>,
    pub(crate) delegation_store: Arc<dyn agent_storage_ports::RunDelegationRepository>,
    pub(crate) interaction_store: Arc<dyn agent_storage_ports::RunInteractionRepository>,
    pub(crate) resource_store: Arc<dyn agent_storage_ports::RunResourceRepository>,
    pub(crate) memory_maintenance: Arc<dyn agent_storage_ports::MemoryMaintenanceRepository>,
    pub(crate) memory_store: Arc<dyn agent_storage_ports::ScopedMemoryRepository>,
    pub(crate) hook_continuation_store: Arc<dyn agent_storage_ports::HookContinuationRepository>,
    pub(crate) hook_execution_store: Arc<dyn agent_storage_ports::HookExecutionRepository>,
    pub(crate) storage_work_drain: Arc<dyn agent_storage_ports::StorageWorkDrainRepository>,
    pub(crate) hook_observation_store: Arc<dyn agent_storage_ports::HookObservationRepository>,
    pub(crate) run_discovery_store: Arc<dyn agent_storage_ports::RunDiscoveryRepository>,
    pub(crate) run_turn_store: Arc<dyn agent_storage_ports::RunTurnRepository>,
    pub(crate) run_tool_store: Arc<dyn agent_storage_ports::RunToolRepository>,
    pub(crate) run_attempt_store: Arc<dyn agent_storage_ports::RunAttemptRepository>,
    pub(crate) session_control_store: Arc<dyn agent_storage_ports::SessionControlRepository>,
    pub(crate) lifecycle_store: Arc<dyn agent_storage_ports::SessionLifecycleRepository>,
    pub(crate) session_writers: Option<Arc<dyn agent_storage_ports::SessionWriterRepository>>,
    pub(crate) artifact_service: Option<Arc<agent_artifact::ArtifactService>>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) resource_tokens: Mutex<HashMap<agent_core::ResourceId, CancellationToken>>,
    pub(crate) skills: Option<SkillLibrary>,
    pub(crate) cron: Option<Arc<CronManager>>,
    pub(crate) mcp: Option<Arc<McpManager>>,
    pub(crate) provider_manager: Option<Arc<ProviderManager>>,
    pub(crate) config_store: ConfigStore,
    pub(crate) daemon_log_path: PathBuf,
    pub(crate) hooks: Arc<agent_runtime::hooks::HookDispatcher>,
}

/// 会话实例、writer 与生命周期屏障的唯一运行时 owner。
pub(crate) struct SessionSupervisor {
    pub(crate) legacy_session_id: Mutex<String>,
    pub(crate) default_session: Arc<SessionRuntime>,
    pub(crate) sessions: Mutex<HashMap<String, Arc<SessionRuntime>>>,
    pub(crate) control_lock: Mutex<()>,
}
/// 准入后的活跃任务、冻结能力和排队唤醒的唯一协调 owner。
pub(crate) struct RunCoordinator {
    pub(crate) effect_tasks: Mutex<JoinSet<()>>,
    pub(crate) hook_dispatch_lock: Mutex<()>,

    pub(crate) active: Mutex<HashMap<ActiveKey, ActiveRequest>>,
    pub(crate) request_tasks: Mutex<JoinSet<()>>,
    pub(crate) queue_notify: Notify,
    pub(crate) frozen_routes: Mutex<HashMap<RunId, FrozenRoute>>,
    pub(crate) frozen_engines: Mutex<HashMap<RunId, LoopEngine>>,
}

pub(crate) struct SessionRuntime {
    pub(crate) id: String,
    pub(crate) engine: Arc<LoopEngine>,
    pub(crate) lifetime: agent_core::SessionLifetimeId,
    pub(crate) writer: Arc<Mutex<()>>,
    pub(crate) store: Arc<SessionStore>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ActiveKey {
    pub(crate) session_id: String,
    pub(crate) request_id: RequestId,
}

const ACTIVE_REPLAY_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) enum ActiveRequestUpdate {
    Event {
        kind: EventKind,
        data: Value,
        run_id: Option<RunId>,
        seq: Option<EventSeq>,
    },
    Terminal {
        response: Result<Value, (i64, String)>,
        view: Option<Box<agent_core::ViewStamp>>,
    },
}

impl ActiveRequestUpdate {
    pub(crate) fn to_frame(&self, request_id: RequestId) -> ServerFrame {
        match self {
            Self::Event {
                kind,
                data,
                run_id,
                seq,
            } => {
                let mut frame = EventFrame::new(request_id, kind.clone(), data.clone());
                frame.run_id = run_id.clone();
                frame.seq = *seq;
                ServerFrame::Event(frame)
            }
            Self::Terminal { response, view } => {
                let mut frame = match response {
                    Ok(result) => JsonRpcResponse::success(request_id, result.clone()),
                    Err((code, message)) => {
                        JsonRpcResponse::failure(request_id, *code, message.clone())
                    }
                };
                if let Some(view) = view {
                    if let Some(result) = &mut frame.result {
                        result["_my_agent_view"] = serde_json::json!(view);
                    } else if let Some(error) = &mut frame.error {
                        error.data = Some(serde_json::json!({"_my_agent_view":view}));
                    }
                }
                ServerFrame::Response(frame)
            }
        }
    }

    fn replay_bytes(&self) -> usize {
        match self {
            Self::Event { data, .. } => data.to_string().len().saturating_add(64),
            Self::Terminal {
                response: Ok(result),
                ..
            } => result.to_string().len().saturating_add(64),
            Self::Terminal {
                response: Err((_, message)),
                ..
            } => message.len().saturating_add(64),
        }
    }
}

pub(crate) struct ActiveRequest {
    pub(crate) cancellation: CancellationToken,
    pub(crate) run_id: RunId,
    pub(crate) storage_error: Option<String>,
    updates: broadcast::Sender<ActiveRequestUpdate>,
    replay: VecDeque<ActiveRequestUpdate>,
    replay_bytes: usize,
    origin: Option<frames::FrameSender>,
}

impl ActiveRequest {
    pub(crate) fn new(cancellation: CancellationToken, run_id: RunId) -> Self {
        let (updates, _) = broadcast::channel(1024);
        Self {
            cancellation,
            run_id,
            storage_error: None,
            updates,
            replay: VecDeque::new(),
            replay_bytes: 0,
            origin: None,
        }
    }

    pub(crate) fn with_origin(mut self, origin: frames::FrameSender) -> Self {
        self.origin = Some(origin);
        self
    }

    pub(crate) fn publish_external(&mut self, request_id: RequestId, update: ActiveRequestUpdate) {
        self.publish(update.clone());
        if let Some(origin) = &self.origin {
            let _ = origin.send(update.to_frame(request_id));
        }
    }

    pub(crate) fn publish(&mut self, update: ActiveRequestUpdate) {
        let bytes = update.replay_bytes();
        self.replay.push_back(update.clone());
        self.replay_bytes = self.replay_bytes.saturating_add(bytes);
        while self.replay_bytes > ACTIVE_REPLAY_BYTES {
            let Some(removed) = self.replay.pop_front() else {
                break;
            };
            self.replay_bytes = self.replay_bytes.saturating_sub(removed.replay_bytes());
        }
        let _ = self.updates.send(update);
    }

    pub(crate) fn subscribe(
        &self,
    ) -> (
        Vec<ActiveRequestUpdate>,
        broadcast::Receiver<ActiveRequestUpdate>,
    ) {
        (
            self.replay.iter().cloned().collect(),
            self.updates.subscribe(),
        )
    }
}

impl DaemonState {
    #[cfg(any(test, feature = "test-support"))]
    pub fn new(
        engine: Arc<LoopEngine>,
        history: Vec<Message>,
        session: Arc<SessionStore>,
        approvals: ApprovalBroker,
    ) -> Self {
        Self::new_with_skills(engine, history, session, approvals, None)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn new_with_skills(
        engine: Arc<LoopEngine>,
        history: Vec<Message>,
        session: Arc<SessionStore>,
        approvals: ApprovalBroker,
        skills: Option<SkillLibrary>,
    ) -> Self {
        Self::new_with_services(engine, history, session, approvals, skills, None, None)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn new_with_services(
        engine: Arc<LoopEngine>,
        history: Vec<Message>,
        session: Arc<SessionStore>,
        approvals: ApprovalBroker,
        skills: Option<SkillLibrary>,
        cron: Option<Arc<CronManager>>,
        mcp: Option<Arc<McpManager>>,
    ) -> Self {
        let daemon_log_path = session
            .path_for_session(&session.current_id_sync())
            .with_file_name("daemon.log");
        Self::new_with_services_and_log_path(
            engine,
            history,
            session,
            approvals,
            skills,
            cron,
            mcp,
            daemon_log_path,
        )
    }

    #[cfg(any(test, feature = "test-support"))]
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_services_and_log_path(
        engine: Arc<LoopEngine>,
        history: Vec<Message>,
        session: Arc<SessionStore>,
        approvals: ApprovalBroker,
        skills: Option<SkillLibrary>,
        cron: Option<Arc<CronManager>>,
        mcp: Option<Arc<McpManager>>,
        daemon_log_path: PathBuf,
    ) -> Self {
        Self::new_with_services_and_log_path_and_safety(
            engine,
            history,
            session,
            approvals,
            skills,
            cron,
            mcp,
            daemon_log_path,
            None,
        )
    }

    #[cfg(any(test, feature = "test-support"))]
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_services_and_log_path_and_safety(
        engine: Arc<LoopEngine>,
        history: Vec<Message>,
        session: Arc<SessionStore>,
        approvals: ApprovalBroker,
        skills: Option<SkillLibrary>,
        cron: Option<Arc<CronManager>>,
        mcp: Option<Arc<McpManager>>,
        daemon_log_path: PathBuf,
        safety: Option<Arc<SafetyPolicy>>,
    ) -> Self {
        let run_store = Arc::new(
            RunStore::open(
                &session
                    .path_for_session(&session.current_id_sync())
                    .with_extension("sqlite3"),
            )
            .expect("测试 SQLite run store"),
        );
        Self::new_with_services_and_log_path_and_safety_and_provider(
            engine,
            history,
            session,
            approvals,
            skills,
            cron,
            mcp,
            daemon_log_path,
            safety,
            None,
            ConfigStore::default(),
            run_store,
        )
        .expect("测试 session repository")
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_services_and_log_path_and_safety_and_provider(
        engine: Arc<LoopEngine>,
        history: Vec<Message>,
        session: Arc<SessionStore>,
        approvals: ApprovalBroker,
        skills: Option<SkillLibrary>,
        cron: Option<Arc<CronManager>>,
        mcp: Option<Arc<McpManager>>,
        daemon_log_path: PathBuf,
        safety: Option<Arc<SafetyPolicy>>,
        provider_manager: Option<Arc<ProviderManager>>,
        config_store: ConfigStore,
        run_store: Arc<RunStore>,
    ) -> anyhow::Result<Self> {
        let original_key = agent_core::SessionKey(session.current_id_sync());
        run_store.import_legacy(&original_key, &history)?;
        let selected = run_store.preferred_session()?.filter(|key| {
            run_store
                .session_metadata(key)
                .ok()
                .flatten()
                .is_some_and(|m| !m.deleted)
        });
        let default_session_id = if let Some(key) = selected {
            key.0
        } else if run_store
            .session_metadata(&original_key)?
            .is_some_and(|m| !m.deleted)
        {
            original_key.0
        } else if let Some(meta) = run_store.session_metadata_list()?.into_iter().next() {
            meta.key.0
        } else {
            let (key, _) = session.create_isolated_session()?;
            run_store.create_session(&agent_core::SessionKey(key.clone()))?;
            key
        };
        let session = Arc::new(session.open_known_session(&default_session_id)?);
        run_store.set_preferred_session(&agent_core::SessionKey(default_session_id.clone()))?;
        let snapshot =
            run_store.session_snapshot(&agent_core::SessionKey(default_session_id.clone()))?;
        let session = Arc::new(session.with_trace_lifetime(&snapshot.lifetime)?);
        let engine = Arc::new(engine.for_session(session.clone()));
        Ok(Self::assemble_prepared(
            engine,
            session,
            default_session_id,
            snapshot.lifetime,
            approvals,
            skills,
            cron,
            mcp,
            daemon_log_path,
            safety,
            provider_manager,
            config_store,
            DaemonRepositories::from_sqlite(run_store),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn assemble_prepared(
        engine: Arc<LoopEngine>,
        session: Arc<SessionStore>,
        default_session_id: String,
        lifetime: agent_core::SessionLifetimeId,
        approvals: ApprovalBroker,
        skills: Option<SkillLibrary>,
        cron: Option<Arc<CronManager>>,
        mcp: Option<Arc<McpManager>>,
        daemon_log_path: PathBuf,
        safety: Option<Arc<SafetyPolicy>>,
        provider_manager: Option<Arc<ProviderManager>>,
        config_store: ConfigStore,
        repositories: DaemonRepositories,
    ) -> Self {
        let default_session = Arc::new(SessionRuntime {
            id: default_session_id.clone(),
            engine,
            lifetime,
            writer: Arc::new(Mutex::new(())),
            store: session.clone(),
        });
        Self {
            runtime_identity: agent_core::RuntimeIdentity::local(),
            oidc: None,
            session,
            session_supervisor: SessionSupervisor {
                legacy_session_id: Mutex::new(default_session_id),
                default_session,
                sessions: Mutex::new(HashMap::new()),
                control_lock: Mutex::new(()),
            },
            approvals,
            safety,
            run_coordinator: RunCoordinator {
                effect_tasks: Mutex::new(JoinSet::new()),
                hook_dispatch_lock: Mutex::new(()),

                active: Mutex::new(HashMap::new()),
                request_tasks: Mutex::new(JoinSet::new()),
                queue_notify: Notify::new(),
                frozen_routes: Mutex::new(HashMap::new()),
                frozen_engines: Mutex::new(HashMap::new()),
            },
            daemon_maintenance_store: repositories.daemon_maintenance_store,
            observation_store: repositories.observation_store,
            plan_control_store: repositories.plan_control_store,
            #[cfg(any(test, feature = "test-support"))]
            maintenance_store: repositories.legacy_maintenance,
            publication_store: repositories.publication_store,
            readback_store: repositories.readback_store,
            audit_store: repositories.audit_store,
            quota_store: repositories.quota_store,
            quota_management: repositories.quota_management,
            identity_store: repositories.identity_store,
            identity_management: repositories.identity_management,
            access_store: repositories.access_store,
            workflow_store:repositories.workflow_store,
            registry_store:repositories.registry_store,
            billing_store:repositories.billing_store,
            credential_store: repositories.credential_store,
            event_store: repositories.event_store,
            session_control_store: repositories.session_control_store,
            lifecycle_store: repositories.lifecycle_store,
            queue_store: repositories.queue_store,
            run_fact_store: repositories.run_fact_store,
            run_execution_store: repositories.run_execution_store,
            run_attempt_store: repositories.run_attempt_store,
            run_tool_store: repositories.run_tool_store,
            run_turn_store: repositories.run_turn_store,
            run_request_store: repositories.run_request_store,
            delegation_store: repositories.delegation_store,
            interaction_store: repositories.interaction_store,
            resource_store: repositories.resource_store,
            memory_maintenance: repositories.memory_maintenance,
            memory_store: repositories.memory_store,
            hook_continuation_store: repositories.hook_continuation_store,
            hook_execution_store: repositories.hook_execution_store,
            storage_work_drain: repositories.storage_work_drain,
            hook_observation_store: repositories.hook_observation_store,
            run_discovery_store: repositories.run_discovery_store,
            #[cfg(any(test, feature = "test-support"))]
            run_store: repositories.legacy_control,
            session_writers: None,
            artifact_service: None,
            shutdown: CancellationToken::new(),
            resource_tokens: Mutex::new(HashMap::new()),
            skills,
            cron,
            mcp,
            provider_manager,
            config_store,
            daemon_log_path,
            hooks: Arc::new(agent_runtime::hooks::HookDispatcher::default()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn bootstrap_with_services(
        engine: Arc<LoopEngine>,
        history: Vec<Message>,
        session: Arc<SessionStore>,
        approvals: ApprovalBroker,
        skills: Option<SkillLibrary>,
        cron: Option<Arc<CronManager>>,
        mcp: Option<Arc<McpManager>>,
        daemon_log_path: PathBuf,
        safety: Option<Arc<SafetyPolicy>>,
        provider_manager: Option<Arc<ProviderManager>>,
        config_store: ConfigStore,
        run_store: Arc<RunStore>,
    ) -> anyhow::Result<Self> {
        Self::bootstrap_with_repositories(
            engine,
            history,
            session,
            approvals,
            skills,
            cron,
            mcp,
            daemon_log_path,
            safety,
            provider_manager,
            config_store,
            DaemonRepositories::from_sqlite(run_store),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    /// 当前 Local 启动流程，后端通过完整领域端口注入；租户路由另由企业启动层提供。
    pub async fn bootstrap_with_repositories(
        engine: Arc<LoopEngine>,
        history: Vec<Message>,
        session: Arc<SessionStore>,
        approvals: ApprovalBroker,
        skills: Option<SkillLibrary>,
        cron: Option<Arc<CronManager>>,
        mcp: Option<Arc<McpManager>>,
        daemon_log_path: PathBuf,
        safety: Option<Arc<SafetyPolicy>>,
        provider_manager: Option<Arc<ProviderManager>>,
        config_store: ConfigStore,
        repositories: DaemonRepositories,
    ) -> anyhow::Result<Self> {
        Self::bootstrap_scoped(
            engine,
            history,
            session,
            approvals,
            skills,
            cron,
            mcp,
            daemon_log_path,
            safety,
            provider_manager,
            config_store,
            repositories,
            agent_core::RuntimeIdentity::local(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn bootstrap_scoped(
        engine: Arc<LoopEngine>,
        history: Vec<Message>,
        session: Arc<SessionStore>,
        approvals: ApprovalBroker,
        skills: Option<SkillLibrary>,
        cron: Option<Arc<CronManager>>,
        mcp: Option<Arc<McpManager>>,
        daemon_log_path: PathBuf,
        safety: Option<Arc<SafetyPolicy>>,
        provider_manager: Option<Arc<ProviderManager>>,
        config_store: ConfigStore,
        repositories: DaemonRepositories,
        runtime_identity: agent_core::RuntimeIdentity,
    ) -> anyhow::Result<Self> {
        let scope = runtime_identity.scope.clone();
        let local = scope == agent_core::TenantContext::local();
        let original_key = if local {
            agent_core::SessionKey(session.current_id_sync())
        } else {
            let verifier = agent_identity::OidcVerifier::from_environment()?;
            let endpoint = verifier.as_ref().map_or("runtime-default", |v|v.runtime_endpoint());
            let binding = serde_json::to_string(&("runtime-default-session",&scope,endpoint))?;
            agent_core::SessionKey(session.session_key_for_operation(&binding))
        };
        let original = repositories
            .session_control_store
            .read_session_metadata(&scope, &original_key)
            .await?;
        if scope == agent_core::TenantContext::local()
            && original.as_ref().is_none_or(|meta| !meta.deleted)
        {
            repositories
                .session_control_store
                .import_legacy_session(
                    &scope,
                    &original_key,
                    original.as_ref().map(|meta| &meta.lifetime),
                    &history,
                )
                .await?;
        }
        let original = repositories
            .session_control_store
            .read_session_metadata(&scope, &original_key)
            .await?;
        let selected = if local { repositories
            .session_control_store
            .read_selected_session(&scope)
            .await? } else {None};
        let fallback = if local { repositories.readback_store.list_sessions(&scope,None,1).await?.into_iter().next() } else {None};
        let key = if let Some(key) = selected {
            key
        } else if original.as_ref().is_some_and(|meta| !meta.deleted) {
            original_key
        } else if let Some(entry) = fallback {
            entry.metadata.key
        } else {
            let key = if local {
                let (id, _) = session.create_isolated_session()?;
                agent_core::SessionKey(id)
            } else {original_key.clone()};
            let identity = repositories
                .identity_store
                .read_identity(&scope, &runtime_identity.principal.subject_id)
                .await?;
            let context = agent_identity::bound_context(
                &runtime_identity,
                &identity,
                agent_core::RequestId::String(format!("bootstrap:{}", key.0)),
            )?;
            repositories
                .lifecycle_store
                .commit_session_authorized(
                    &agent_core::AuthorizationRequest {
                        context,
                        expected_policy: identity.policy.version,
                        action: agent_core::Action::SessionCreate,
                        attributes: agent_core::AuthorizationAttributes {
                            workspace: safety.as_ref().map_or_else(
                                || ".".into(),
                                |s| s.workspace().to_string_lossy().into_owned(),
                            ),
                            entry_channel: agent_core::HookChannel::DaemonRpc,
                            tool_effect: None,
                            timestamp_ms: 0,
                            network: agent_core::NetworkBoundary::None,
                        },
                    },
                    &agent_core::SessionCommand::Create { key: key.clone() },
                    None,
                    &format!("bootstrap:{}", key.0),
                    Arc::new(agent_policy::PolicyEngine),
                )
                .await?;
            key
        };
        let read = repositories
            .readback_store
            .read_session(&scope, &key, agent_core::HistoryReadMode::Omitted)
            .await?;
        if local { repositories
            .session_control_store
            .select_session(&scope, &key, &read.session_lifetime_id)
            .await?; }
        let session = Arc::new(
            session
                .open_known_session(&key.0)?
                .with_trace_lifetime(&read.session_lifetime_id)?,
        );
        let engine = Arc::new(engine.for_session(session.clone()));
        let mut state = Self::assemble_prepared(
            engine,
            session,
            key.0,
            read.session_lifetime_id,
            approvals,
            skills,
            cron,
            mcp,
            daemon_log_path,
            safety,
            provider_manager,
            config_store,
            repositories,
        );
        state.runtime_identity = runtime_identity;
        Ok(state)
    }

    pub(crate) fn scope(&self) -> agent_core::TenantContext {
        self.runtime_identity.scope.clone()
    }

    pub(crate) fn with_oidc(mut self, verifier: Option<Arc<agent_identity::OidcVerifier>>) -> Self {
        self.oidc = verifier;
        self
    }

    pub(crate) fn request_identity(&self) -> agent_core::RuntimeIdentity {
        handlers::AUTHENTICATED_IDENTITY
            .try_with(Clone::clone)
            .unwrap_or_else(|_| self.runtime_identity.clone())
    }

    pub(crate) fn with_session_writers(
        mut self,
        writers: Option<Arc<dyn agent_storage_ports::SessionWriterRepository>>,
    ) -> Self {
        self.session_writers = writers;
        self
    }

    pub(crate) fn with_artifact_service(
        mut self,
        service: Option<Arc<agent_artifact::ArtifactService>>,
    ) -> Self {
        self.artifact_service = service;
        self
    }

    pub(crate) fn with_hooks(mut self, workspace: &std::path::Path) -> Self {
        self.hooks = Arc::new(agent_runtime::hooks::HookDispatcher::load(workspace));
        self
    }

    pub(crate) async fn dispatch_hook_publications(self: &Arc<Self>) -> anyhow::Result<()> {
        let _dispatch = self.run_coordinator.hook_dispatch_lock.lock().await;
        for publication in self
            .hook_observation_store
            .read_pending_hooks(&self.scope())
            .await?
        {
            if !self.hooks.configured(publication.event) {
                self.hook_observation_store
                    .acknowledge_pending_hook(&self.scope(), &publication)
                    .await?;
                continue;
            }
            let generation=self.registry_store.read_session_generation(&self.scope(),&publication.session_key,&publication.session_lifetime_id).await?;
            if let Some(g)=generation {if !self.hooks.matches_registry_refs(&g.effective.config.hook_refs) {self.hook_observation_store.acknowledge_pending_hook(&self.scope(),&publication).await?;continue;}}
            let payload = agent_core::HookPayload {
                schema_version: 1,
                event: publication.event,
                session_key: publication.session_key.clone(),
                session_lifetime_id: publication.session_lifetime_id.clone(),
                owner: None,
                cwd: self.safety.as_ref().map_or_else(String::new, |s| {
                    s.workspace().to_string_lossy().into_owned()
                }),
                channel: "daemon_rpc".into(),
                permission_mode: "observation".into(),
                route_digest: None,
                data: serde_json::json!({"published":true}),
                operation_id: publication.operation_id.clone(),
            };
            if let Err(error) = self
                .hooks
                .invoke_managed(self.hook_context(None), payload, &self.shutdown)
                .await
            {
                // 存储/owner失败不代表交付，不ACK尚未确认的原publication。
                return Err(error);
            }
            self.hook_observation_store
                .acknowledge_pending_hook(&self.scope(), &publication)
                .await?;
        }
        Ok(())
    }

    pub(crate) fn hook_context(
        self: &Arc<Self>,
        envelope: Option<agent_core::EnterpriseOwnerEnvelope>,
    ) -> agent_runtime::hooks::HookExecutionContext {
        agent_runtime::hooks::HookExecutionContext {
            store: self.hook_execution_store.clone(),
            scope: envelope
                .as_ref()
                .map_or_else(|| self.scope(), |e| e.scope.clone()),
            envelope,
            tasks: self.clone(),
        }
    }
    pub(crate) fn hook_scope(
        self: &Arc<Self>,
        envelope: &agent_core::EnterpriseOwnerEnvelope,
    ) -> agent_runtime::hooks::HookScope {
        agent_runtime::hooks::HookScope {
            dispatcher: self.hooks.clone(),
            execution: self.hook_context(Some(envelope.clone())),
        }
    }
    pub(crate) async fn join_effects(&self) {
        let mut tasks = self.run_coordinator.effect_tasks.lock().await;
        while let Some(result) = tasks.join_next().await {
            if let Err(error) = result {
                tracing::error!(%error,"受管副作用任务异常结束");
            }
        }
    }
    pub async fn has_active_turns(&self) -> bool {
        !self.run_coordinator.active.lock().await.is_empty()
    }

    pub(crate) async fn spawn_owned<F>(&self, future: F) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if self.shutdown.is_cancelled() {
            return false;
        }
        let mut tasks = tokio::select! {
            tasks=self.run_coordinator.request_tasks.lock()=>tasks,
            _=self.shutdown.cancelled()=>return false
        };
        if self.shutdown.is_cancelled() {
            return false;
        }
        while let Some(result) = tasks.try_join_next() {
            if let Err(error) = result {
                tracing::error!(%error, "受管 daemon 任务异常结束");
            }
        }
        if tasks.len() >= 256 {
            return false;
        }
        tasks.spawn(Box::pin(future));
        true
    }

    pub(crate) async fn join_owned(&self, grace: std::time::Duration) -> usize {
        let mut tasks = self.run_coordinator.request_tasks.lock().await;
        let joined = async {
            while let Some(result) = tasks.join_next().await {
                if let Err(error) = result {
                    tracing::error!(%error, "受管 daemon 任务异常结束");
                }
            }
        };
        if tokio::time::timeout(grace, joined).await.is_ok() {
            return 0;
        }
        let aborted = tasks.len();
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        aborted
    }

    pub async fn has_persistent_background_work(&self) -> bool {
        if !self.resource_tokens.lock().await.is_empty() {
            return true;
        }
        match &self.cron {
            Some(cron) => cron.keeps_daemon_alive().await,
            None => false,
        }
    }

    pub async fn join_background(&self) {
        if let Some(cron) = &self.cron {
            cron.join().await;
        }
        if let Some(mcp) = &self.mcp {
            mcp.shutdown().await;
        }
    }
}
pub mod maintenance;

#[async_trait::async_trait]
impl agent_runtime::hooks::HookTaskOwner for DaemonState {
    async fn spawn_hook_job(
        &self,
        job: std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    ) -> bool {
        self.spawn_effect_job(job).await
    }
}

impl DaemonState {
    pub(crate) async fn spawn_effect_job(
        &self,
        job: std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    ) -> bool {
        if self.shutdown.is_cancelled() {
            return false;
        }
        let mut tasks = tokio::select! {tasks=self.run_coordinator.effect_tasks.lock()=>tasks,_=self.shutdown.cancelled()=>return false};
        if self.shutdown.is_cancelled() {
            return false;
        }
        while let Some(result) = tasks.try_join_next() {
            if let Err(error) = result {
                tracing::error!(%error,"受管副作用任务异常结束");
            }
        }
        tasks.spawn(job);
        true
    }
}

/// Cron只保存daemon弱引用的任务委派器，Hook结果仍写入原事实源。
pub(crate) struct DeferredHookTaskOwner {
    pub(crate) state: Arc<std::sync::OnceLock<std::sync::Weak<DaemonState>>>,
}
#[async_trait::async_trait]
impl agent_runtime::hooks::HookTaskOwner for DeferredHookTaskOwner {
    async fn spawn_hook_job(
        &self,
        job: std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    ) -> bool {
        match self.state.get().and_then(std::sync::Weak::upgrade) {
            Some(state) => state.spawn_effect_job(job).await,
            None => false,
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
impl DaemonState {
    fn legacy_run_store(&self) -> &Arc<dyn ControlRepository> {
        self.run_store.as_ref().expect("此兼容测试需要 SQLite 后端")
    }
    fn legacy_maintenance_store(&self) -> &Arc<dyn MaintenanceRepository> {
        self.maintenance_store
            .as_ref()
            .expect("此兼容测试需要 SQLite 后端")
    }
}
