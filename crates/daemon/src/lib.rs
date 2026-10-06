#![forbid(unsafe_code)]
pub mod approval;
mod compact;
mod context_readback;
pub mod delegation_tool;
mod frames;
pub mod handlers;
pub mod lifecycle;
pub mod resources;
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
use agent_storage::{
    ControlRepository, EventSeq, MaintenanceRepository, RunId, RunStore, SessionQuery,
    TranscriptStore,
};

pub struct DaemonState {
    pub(crate) session_supervisor: SessionSupervisor,
    pub(crate) run_coordinator: RunCoordinator,
    pub(crate) session: Arc<SessionStore>,
    pub(crate) approvals: ApprovalBroker,
    pub(crate) safety: Option<Arc<SafetyPolicy>>,
    pub(crate) run_store: Arc<dyn ControlRepository>,
    pub(crate) maintenance_store: Arc<dyn MaintenanceRepository>,
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
        let default_session = Arc::new(SessionRuntime {
            id: default_session_id.clone(),
            engine,
            lifetime: snapshot.lifetime,
            writer: Arc::new(Mutex::new(())),
            store: session.clone(),
        });
        Ok(Self {
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
                active: Mutex::new(HashMap::new()),
                request_tasks: Mutex::new(JoinSet::new()),
                queue_notify: Notify::new(),
                frozen_routes: Mutex::new(HashMap::new()),
                frozen_engines: Mutex::new(HashMap::new()),
            },
            maintenance_store: run_store.clone(),
            run_store,
            shutdown: CancellationToken::new(),
            resource_tokens: Mutex::new(HashMap::new()),
            skills,
            cron,
            mcp,
            provider_manager,
            config_store,
            daemon_log_path,
            hooks: Arc::new(agent_runtime::hooks::HookDispatcher::default()),
        })
    }

    pub(crate) fn with_hooks(mut self, workspace: &std::path::Path) -> Self {
        self.hooks = Arc::new(agent_runtime::hooks::HookDispatcher::load(workspace));
        self
    }

    pub(crate) async fn dispatch_hook_publications(&self) -> anyhow::Result<()> {
        for publication in self.run_store.hook_publications()? {
            if !self.hooks.configured(publication.event) {
                self.run_store
                    .acknowledge_hook_publication(&publication.operation_id)?;
                continue;
            }
            let payload = agent_core::HookPayload {
                schema_version: 1,
                event: publication.event,
                session_key: publication.session_key,
                session_lifetime_id: publication.session_lifetime_id,
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
                .invoke(&*self.run_store, payload, &self.shutdown)
                .await
            {
                tracing::warn!(%error,"lifecycle hook 观察失败");
            }
            self.run_store
                .acknowledge_hook_publication(&publication.operation_id)?;
        }
        Ok(())
    }

    pub async fn has_active_turns(&self) -> bool {
        !self.run_coordinator.active.lock().await.is_empty()
    }

    pub(crate) async fn spawn_owned<F>(&self, future: F) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut tasks = self.run_coordinator.request_tasks.lock().await;
        if self.shutdown.is_cancelled() {
            return false;
        }
        while let Some(result) = tasks.try_join_next() {
            if let Err(error) = result {
                tracing::error!(%error, "受管 daemon 任务异常结束");
            }
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
