use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ::cron::Schedule;
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, warn};

use crate::context::{ContextConfig, ContextManager};
use crate::loop_engine::{CancellationToken, LoopEngine};
use crate::plan::PlanStore;
use crate::provider::{Provider, ProviderManager};
use crate::safety::Approval;
use crate::session::SessionStore;
use crate::tools::ToolRegistry;

const MAX_HISTORY: usize = 20;
const CRON_SYSTEM_PROMPT: &str = "你正在执行个人 Agent 的无人值守定时任务。使用全新独立历史，只完成给定 prompt。所有工具仍受安全策略约束；任何需要人工审批的动作都会被拒绝，必须改用安全方案或清晰报告失败。";
static NEXT_JOB_ID: AtomicU64 = AtomicU64::new(0);
static NEXT_CRON_RUN_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScheduleSpec {
    Interval { seconds: u64 },
    Cron { expression: String },
}

impl ScheduleSpec {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Interval { seconds } if *seconds == 0 => bail!("interval 秒数必须大于 0"),
            Self::Interval { .. } => Ok(()),
            Self::Cron { expression } => parse_five_field_cron(expression).map(|_| ()),
        }
    }

    fn next_after(&self, after: u64) -> Result<u64> {
        match self {
            Self::Interval { seconds } => Ok(after.saturating_add(*seconds)),
            Self::Cron { expression } => {
                let schedule = parse_five_field_cron(expression)?;
                let time = UNIX_EPOCH
                    .checked_add(Duration::from_secs(after))
                    .context("cron 基准时间溢出")?;
                let datetime: DateTime<Utc> = time.into();
                schedule
                    .after(&datetime)
                    .next()
                    .map(|next| u64::try_from(next.timestamp()).unwrap_or(u64::MAX))
                    .context("cron 表达式没有下一次触发时间")
            }
        }
    }

    pub fn display(&self) -> String {
        match self {
            Self::Interval { seconds } => format!("interval={seconds}s"),
            Self::Cron { expression } => format!("cron={expression}"),
        }
    }
}

fn parse_five_field_cron(expression: &str) -> Result<Schedule> {
    if expression.split_whitespace().count() != 5 {
        bail!("cron 表达式必须正好是 5 段：分 时 日 月 周");
    }
    Schedule::from_str(&format!("0 {expression}"))
        .with_context(|| format!("非法 cron 表达式：{expression}"))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CronJob {
    pub id: String,
    pub name: String,
    pub schedule: ScheduleSpec,
    pub prompt: String,
    pub enabled: bool,
    pub max_retries: u32,
    pub backoff_seconds: u64,
    pub next_run_at: u64,
    #[serde(default)]
    pub history: Vec<CronRunRecord>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CronRunRecord {
    pub started_at: u64,
    pub finished_at: u64,
    pub attempts: u32,
    pub success: bool,
    pub result: String,
}

#[derive(Clone, Default, Deserialize, Serialize)]
struct CronState {
    jobs: Vec<CronJob>,
}

pub struct CronStore {
    path: PathBuf,
    state: RwLock<CronState>,
    mutation_lock: Mutex<()>,
}

impl CronStore {
    pub async fn load(workspace: &Path) -> Result<Self> {
        let path = workspace.join(".my-agent/cron.json");
        let state = if path.exists() {
            let bytes = tokio::fs::read(&path)
                .await
                .with_context(|| format!("读取 cron store 失败：{}", path.display()))?;
            serde_json::from_slice(&bytes)
                .with_context(|| format!("解析 cron store 失败：{}", path.display()))?
        } else {
            CronState::default()
        };
        for job in &state.jobs {
            job.schedule.validate()?;
        }
        Ok(Self {
            path,
            state: RwLock::new(state),
            mutation_lock: Mutex::new(()),
        })
    }

    pub async fn load_best_effort(workspace: &Path) -> Self {
        match Self::load(workspace).await {
            Ok(store) => store,
            Err(error) => {
                warn!(%error, "cron.json 无效，已禁用现有任务并使用空 store");
                Self {
                    path: workspace.join(".my-agent/cron.json"),
                    state: RwLock::new(CronState::default()),
                    mutation_lock: Mutex::new(()),
                }
            }
        }
    }

    pub async fn list(&self) -> Vec<CronJob> {
        self.state.read().await.jobs.clone()
    }

    pub async fn add(
        &self,
        name: String,
        schedule: ScheduleSpec,
        prompt: String,
        max_retries: u32,
        backoff_seconds: u64,
    ) -> Result<CronJob> {
        let _mutation = self.mutation_lock.lock().await;
        if name.trim().is_empty() || prompt.trim().is_empty() {
            bail!("cron name 与 prompt 不能为空");
        }
        schedule.validate()?;
        let now = unix_now()?;
        let job = CronJob {
            id: format!("cron-{now}-{}", NEXT_JOB_ID.fetch_add(1, Ordering::SeqCst)),
            name,
            next_run_at: schedule.next_after(now)?,
            schedule,
            prompt,
            enabled: true,
            max_retries,
            backoff_seconds,
            history: Vec::new(),
        };
        let mut next = self.state.read().await.clone();
        if next.jobs.iter().any(|item| item.name == job.name) {
            bail!("cron 名称已存在：{}", job.name);
        }
        next.jobs.push(job.clone());
        self.commit(next).await?;
        Ok(job)
    }

    pub async fn set_enabled(&self, id_or_name: &str, enabled: bool) -> Result<CronJob> {
        let _mutation = self.mutation_lock.lock().await;
        let mut next = self.state.read().await.clone();
        let job = find_job_mut(&mut next.jobs, id_or_name)?;
        job.enabled = enabled;
        if enabled {
            job.next_run_at = job.schedule.next_after(unix_now()?)?;
        }
        let output = job.clone();
        self.commit(next).await?;
        Ok(output)
    }

    pub async fn remove(&self, id_or_name: &str) -> Result<CronJob> {
        let _mutation = self.mutation_lock.lock().await;
        let mut next = self.state.read().await.clone();
        let index = next
            .jobs
            .iter()
            .position(|job| job.id == id_or_name || job.name == id_or_name)
            .with_context(|| format!("找不到 cron：{id_or_name}"))?;
        let removed = next.jobs.remove(index);
        self.commit(next).await?;
        Ok(removed)
    }

    pub async fn get(&self, id_or_name: &str) -> Result<CronJob> {
        self.state
            .read()
            .await
            .jobs
            .iter()
            .find(|job| job.id == id_or_name || job.name == id_or_name)
            .cloned()
            .with_context(|| format!("找不到 cron：{id_or_name}"))
    }

    async fn take_due(&self, now: u64) -> Result<Vec<CronJob>> {
        let _mutation = self.mutation_lock.lock().await;
        let mut next = self.state.read().await.clone();
        let mut due = Vec::new();
        for job in &mut next.jobs {
            if job.enabled && job.next_run_at <= now {
                due.push(job.clone());
                job.next_run_at = job.schedule.next_after(now)?;
            }
        }
        if !due.is_empty() {
            self.commit(next).await?;
        }
        Ok(due)
    }

    async fn record(&self, job_id: &str, record: CronRunRecord) -> Result<()> {
        let _mutation = self.mutation_lock.lock().await;
        let mut next = self.state.read().await.clone();
        let job = find_job_mut(&mut next.jobs, job_id)?;
        job.history.push(record);
        if job.history.len() > MAX_HISTORY {
            let remove = job.history.len() - MAX_HISTORY;
            job.history.drain(..remove);
        }
        self.commit(next).await
    }

    pub async fn has_enabled_jobs(&self) -> bool {
        self.state.read().await.jobs.iter().any(|job| job.enabled)
    }

    async fn commit(&self, next: CronState) -> Result<()> {
        persist_state(&self.path, &next).await?;
        *self.state.write().await = next;
        Ok(())
    }
}

fn find_job_mut<'a>(jobs: &'a mut [CronJob], id_or_name: &str) -> Result<&'a mut CronJob> {
    jobs.iter_mut()
        .find(|job| job.id == id_or_name || job.name == id_or_name)
        .with_context(|| format!("找不到 cron：{id_or_name}"))
}

async fn persist_state(path: &Path, state: &CronState) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("创建 cron 目录失败：{}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(state).context("序列化 cron store 失败")?;
    let temporary = path.with_file_name(format!(".cron.json.tmp-{}", std::process::id()));
    tokio::fs::write(&temporary, bytes)
        .await
        .with_context(|| format!("写入临时 cron store 失败：{}", temporary.display()))?;
    tokio::fs::rename(&temporary, path)
        .await
        .with_context(|| format!("提交 cron store 失败：{}", path.display()))
}

#[async_trait]
pub trait ScheduledJobRunner: Send + Sync {
    async fn run(&self, job: &CronJob) -> Result<String>;
}

#[derive(Clone)]
pub struct CronRepositories {
    pub execution: Arc<dyn agent_storage_ports::RunExecutionRepository>,
    pub identity: Arc<dyn agent_storage_ports::IdentityReadRepository>,
    pub attempts: Arc<dyn agent_storage_ports::RunAttemptRepository>,
    pub tools: Arc<dyn agent_storage_ports::RunToolRepository>,
    pub turns: Arc<dyn agent_storage_ports::RunTurnRepository>,
    pub requests: Arc<dyn agent_storage_ports::RunContextRepository>,
    pub discoveries: Arc<dyn agent_storage_ports::RunDiscoveryRepository>,
    pub memories: Arc<dyn agent_storage_ports::ScopedMemoryRepository>,
    pub hook_execution: Arc<dyn agent_storage_ports::HookExecutionRepository>,
    pub verifier: Arc<dyn agent_core::AuthorizationVerifier>,
}
pub struct AgentCronRunner {
    identity_binding: agent_core::RuntimeIdentity,
    provider: Arc<dyn Provider>,
    tools: ToolRegistry,
    workspace: PathBuf,
    context_config: ContextConfig,
    timeout: Duration,
    repository: Option<CronRepositories>,
    artifacts: Option<Arc<agent_artifact::ArtifactService>>,
    provider_manager: Option<Arc<ProviderManager>>,
    hooks: Option<(
        Arc<crate::hooks::HookDispatcher>,
        Arc<dyn crate::hooks::HookTaskOwner>,
    )>,
}

impl AgentCronRunner {
    pub fn new(
        provider: Arc<dyn Provider>,
        tools: ToolRegistry,
        workspace: PathBuf,
        context_config: ContextConfig,
    ) -> Self {
        let timeout = std::env::var("CRON_RUN_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|seconds| *seconds > 0)
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(600));
        Self {
            identity_binding: agent_core::RuntimeIdentity::local_cron(),
            provider,
            tools,
            workspace,
            context_config,
            timeout,
            repository: None,
            artifacts: None,
            provider_manager: None,
            hooks: None,
        }
    }
    /// 原 daemon 的事实源、writer proof 与 artifact backend；独立历史仍属于新 native session。
    pub fn with_canonical_execution<T>(
        self,
        repository: Arc<T>,
        artifacts: Option<Arc<agent_artifact::ArtifactService>>,
        verifier: Arc<dyn agent_core::AuthorizationVerifier>,
    ) -> Self
    where
        T: agent_storage_ports::RunExecutionRepository
            + agent_storage_ports::IdentityReadRepository
            + agent_storage_ports::RunAttemptRepository
            + agent_storage_ports::RunToolRepository
            + agent_storage_ports::RunTurnRepository
            + agent_storage_ports::RunContextRepository
            + agent_storage_ports::ScopedMemoryRepository
            + agent_storage_ports::HookExecutionRepository
            + agent_storage_ports::RunDiscoveryRepository
            + 'static,
    {
        self.with_canonical_ports(
            CronRepositories {
                execution: repository.clone(),
                identity: repository.clone(),
                attempts: repository.clone(),
                tools: repository.clone(),
                turns: repository.clone(),
                requests: repository.clone(),
                memories: repository.clone(),
                hook_execution: repository.clone(),
                discoveries: repository,
                verifier,
            },
            artifacts,
        )
    }

    pub fn with_canonical_ports(
        mut self,
        repositories: CronRepositories,
        artifacts: Option<Arc<agent_artifact::ArtifactService>>,
    ) -> Self {
        self.repository = Some(repositories);
        self.artifacts = artifacts;
        self
    }
    /// 仅由启动组合提供独立Cron workload；job参数不包含主体或scope。
    pub fn with_identity_binding(mut self, binding: agent_core::RuntimeIdentity) -> Result<Self> {
        if binding.principal.kind != agent_core::SubjectKind::Workload {
            return Err(agent_storage::RuntimeError::Authorization(
                agent_core::AuthorizationError::Unauthenticated,
            )
            .into());
        }
        self.identity_binding = binding;
        Ok(self)
    }
    pub fn with_hooks(
        mut self,
        dispatcher: Arc<crate::hooks::HookDispatcher>,
        tasks: Arc<dyn crate::hooks::HookTaskOwner>,
    ) -> Self {
        self.hooks = Some((dispatcher, tasks));
        self
    }
    fn hook_scope(
        &self,
        ports: &CronRepositories,
        envelope: &agent_core::EnterpriseOwnerEnvelope,
    ) -> Option<crate::hooks::HookScope> {
        self.hooks
            .as_ref()
            .map(|(dispatcher, tasks)| crate::hooks::HookScope {
                dispatcher: dispatcher.clone(),
                execution: crate::hooks::HookExecutionContext {
                    store: ports.hook_execution.clone(),
                    scope: envelope.scope.clone(),
                    envelope: Some(envelope.clone()),
                    tasks: tasks.clone(),
                },
            })
    }
    async fn observe_terminal(
        &self,
        ports: &CronRepositories,
        envelope: &agent_core::EnterpriseOwnerEnvelope,
        status: agent_core::RunStatus,
        content: Option<&str>,
        cancel: &CancellationToken,
    ) {
        let Some(scope) = self.hook_scope(ports, envelope) else {
            return;
        };
        let observation=crate::hooks::with_scope(scope,async {
            if let Some(content)=content {use sha2::{Digest,Sha256};crate::hooks::current(agent_core::HookEvent::AssistantReply,"reply",serde_json::json!({"content_bytes":content.len(),"content_sha256":format!("{:x}",Sha256::digest(content.as_bytes()))}),cancel).await?;}
            crate::hooks::current(agent_core::HookEvent::AfterTurn,"terminal",serde_json::json!({"status":status}),cancel).await?;
            crate::hooks::current(agent_core::HookEvent::Notification,"terminal",serde_json::json!({"kind":"run_terminal","status":status}),cancel).await?;
            crate::hooks::current(agent_core::HookEvent::Stop,"stop",serde_json::json!({"status":status,"continuation_allowed":false}),cancel).await?;
            Ok::<(),anyhow::Error>(())
        }).await;
        if let Err(error) = observation {
            tracing::warn!(%error,"Cron终态Hook观察失败，保留原run终态");
        }
    }
    pub fn with_provider_manager(mut self, manager: Arc<ProviderManager>) -> Self {
        self.provider_manager = Some(manager);
        self
    }
}

#[async_trait]
impl ScheduledJobRunner for AgentCronRunner {
    async fn run(&self, job: &CronJob) -> Result<String> {
        use agent_core::*;
        use agent_storage::RuntimeError;
        let ports = self.repository.as_ref().ok_or(RuntimeError::Authorization(
            AuthorizationError::Unauthenticated,
        ))?;
        let route = self
            .provider_manager
            .as_ref()
            .map(|manager| manager.freeze(self.context_config.token_budget));
        let provider = route
            .as_ref()
            .map(|route| route.primary())
            .unwrap_or_else(|| self.provider.clone());
        let context = ContextManager::with_system_prompt(
            provider.clone(),
            &self.workspace,
            self.context_config.clone(),
            Arc::new(PlanStore::memory_only()),
            CRON_SYSTEM_PROMPT,
        )?;
        let occurrence = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("系统时间早于 UNIX_EPOCH")?
            .as_nanos();
        let key = agent_core::SessionKey(format!(
            "session-cron-{}-{occurrence}-{}.jsonl",
            job.id,
            NEXT_CRON_RUN_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let tools = self.tools.frozen()?;
        let specs = tools.specs();
        let snapshot: agent_core::RunSnapshot = serde_json::from_value(serde_json::json!({
            "entry_channel":HookChannel::Cron,
            "artifact_store_id":self.artifacts.as_ref().map(|s|s.store_id()),
            "tools":specs,"route":route.as_ref().map(|r|&r.snapshot),"cwd":self.workspace.to_string_lossy(),"permission_mode":"risk_approval",
            "sandbox_requested":"native","sandbox_effective":"native","context_read_only":false,"max_tool_calls":null,
            "context_token_budget":context.token_budget(),"context_policy_fingerprint":context.fingerprint(),"config_generation":route.as_ref().map_or(0,|r|r.snapshot.config_generation),
            "tool_catalog_digest":format!("{:x}",sha2::Sha256::digest(serde_json::to_vec(&specs)?))
        }))?;
        let identity = ports
            .identity
            .read_identity(
                &self.identity_binding.scope,
                &self.identity_binding.principal.subject_id,
            )
            .await
            .map_err(RuntimeError::from)?;
        if identity.scope != self.identity_binding.scope
            || identity.principal.kind != SubjectKind::Workload
            || identity.principal.subject_id != self.identity_binding.principal.subject_id
            || (self.identity_binding.scope != TenantContext::local()
                && identity.principal != self.identity_binding.principal)
        {
            return Err(RuntimeError::Authorization(AuthorizationError::Unauthenticated).into());
        }
        let request_id = RequestId::String(format!("cron:{}", key.0));
        let authorization = AuthorizationRequest {
            context: RequestContext {
                scope: identity.scope.clone(),
                principal: identity.principal.clone(),
                request_id: request_id.clone(),
                trace: TraceContext {
                    trace_id: format!("cron:{}", key.0),
                },
            },
            expected_policy: identity.policy.version,
            action: Action::SessionPrompt,
            attributes: AuthorizationAttributes {
                workspace: snapshot.cwd.clone(),
                entry_channel: HookChannel::Cron,
                tool_effect: None,
                timestamp_ms: 0,
                network: NetworkBoundary::None,
            },
        };
        let (admitted, owner) = ports
            .execution
            .admit_workload_run(
                &RunAdmission {
                    workflow_node: None,
                    session_key: key.clone(),
                    expected_lifetime: None,
                    request_id,
                    input: job.prompt.clone(),
                    mode: AdmissionMode::RejectIfBusy,
                    plan_execution: None,
                },
                &snapshot,
                &authorization,
                ports.verifier.clone(),
            )
            .await
            .map_err(RuntimeError::from)?;
        let run = match admitted {
            Admission::New(run) | Admission::Existing(run) => run,
        };
        let envelope = EnterpriseOwnerEnvelope {
            scope: identity.scope,
            owner,
        };
        if !ports
            .execution
            .start_queued_run(&envelope)
            .await
            .map_err(RuntimeError::from)?
        {
            return Err(RuntimeError::Coordination(CoordinationError::Conflict).into());
        }
        let session = Arc::new(SessionStore::from_env(&self.workspace).open_known_session(&key.0)?);
        let session = Arc::new(session.with_trace_lifetime(&envelope.owner.session_lifetime_id)?);
        let runner = LoopEngine::new(provider, tools, context, session)
            .with_artifact_service(self.artifacts.clone());
        let runner = match route {
            Some(route) => runner.with_route(route),
            None => runner,
        };
        let mut history = ports
            .requests
            .read_run_request_source(&envelope, "default", "initial-cron-history")
            .await
            .map_err(RuntimeError::from)?
            .snapshot
            .messages;
        let cancellation = CancellationToken::new();
        let execute = crate::loop_engine::with_native_execution(
            envelope.clone(),
            crate::loop_engine::NativeExecutionPorts {
                attempts: ports.attempts.clone(),
                tools: ports.tools.clone(),
                turns: ports.turns.clone(),
                requests: ports.requests.clone(),
                discoveries: ports.discoveries.clone(),
                memories: ports.memories.clone(),
            },
            async {
                if let Some(scope) = self.hook_scope(ports, &envelope) {
                    crate::hooks::with_scope(
                        scope,
                        runner.run_turn_with_events(
                            &mut history,
                            job.prompt.clone(),
                            None,
                            cancellation.clone(),
                        ),
                    )
                    .await
                } else {
                    runner
                        .run_turn_with_events(
                            &mut history,
                            job.prompt.clone(),
                            None,
                            cancellation.clone(),
                        )
                        .await
                }
            },
        );
        let result = tokio::time::timeout(self.timeout, execute).await;
        match result {
            Ok(Ok(content)) => {
                let settled = ports
                    .execution
                    .commit_run_turn(
                        &envelope,
                        &TurnCommit {
                            owner: envelope.owner.clone(),
                            status: RunStatus::Completed,
                            content: Some(content.clone()),
                            error: None,
                        },
                    )
                    .await
                    .map_err(RuntimeError::from)?;
                if settled.status != RunStatus::Completed {
                    return Err(RunOutcomeUnknown { run_id: run.run_id }.into());
                }
                self.observe_terminal(
                    ports,
                    &envelope,
                    settled.status,
                    Some(&content),
                    &cancellation,
                )
                .await;
                Ok(content)
            }
            Ok(Err(error)) => {
                let settled = ports
                    .execution
                    .commit_run_turn(
                        &envelope,
                        &TurnCommit {
                            owner: envelope.owner.clone(),
                            status: RunStatus::Failed,
                            content: None,
                            error: Some((-32002, error.to_string())),
                        },
                    )
                    .await
                    .map_err(RuntimeError::from)?;
                if settled.status != RunStatus::Failed {
                    return Err(RunOutcomeUnknown { run_id: run.run_id }.into());
                }
                self.observe_terminal(ports, &envelope, settled.status, None, &cancellation)
                    .await;
                Err(error)
            }
            Err(_) => {
                cancellation.cancel();
                ports
                    .execution
                    .commit_run_turn(
                        &envelope,
                        &TurnCommit {
                            owner: envelope.owner.clone(),
                            status: RunStatus::UnknownAfterRestart,
                            content: None,
                            error: Some((-32002, "cron 超时，执行结果待核实".into())),
                        },
                    )
                    .await
                    .map_err(RuntimeError::from)?;
                self.observe_terminal(
                    ports,
                    &envelope,
                    RunStatus::UnknownAfterRestart,
                    None,
                    &cancellation,
                )
                .await;
                Err(RunOutcomeUnknown { run_id: run.run_id }.into())
            }
        }
    }
}

pub struct CronManager {
    store: Arc<CronStore>,
    runner: Arc<dyn ScheduledJobRunner>,
    tick: Duration,
    stagger: Duration,
    heartbeat: Option<Duration>,
    running: Mutex<HashSet<String>>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl CronManager {
    pub fn new(
        store: Arc<CronStore>,
        runner: Arc<dyn ScheduledJobRunner>,
        tick: Duration,
        stagger: Duration,
        heartbeat: Option<Duration>,
    ) -> Self {
        Self {
            store,
            runner,
            tick,
            stagger,
            heartbeat,
            running: Mutex::new(HashSet::new()),
            task: Mutex::new(None),
        }
    }

    pub async fn start(self: &Arc<Self>, shutdown: CancellationToken) {
        let mut task = self.task.lock().await;
        if task.is_some() {
            return;
        }
        let manager = self.clone();
        *task = Some(tokio::spawn(async move {
            let mut tick = tokio::time::interval(manager.tick);
            let mut last_heartbeat = tokio::time::Instant::now();
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tick.tick() => {
                        if let Err(error) = manager.run_due(unix_now().unwrap_or(0)).await {
                            warn!(%error, "cron 调度 tick 失败");
                        }
                        if manager.heartbeat.is_some_and(|period| last_heartbeat.elapsed() >= period) {
                            manager.heartbeat_once().await;
                            last_heartbeat = tokio::time::Instant::now();
                        }
                    }
                }
            }
        }));
    }

    pub async fn join(&self) {
        if let Some(mut task) = self.task.lock().await.take() {
            match tokio::time::timeout(Duration::from_secs(5), &mut task).await {
                Ok(Err(error)) if !error.is_cancelled() => warn!(%error, "cron 调度任务异常终止"),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    warn!("cron 调度任务关闭超时，已中止");
                }
                _ => {}
            }
        }
    }

    pub fn store(&self) -> &Arc<CronStore> {
        &self.store
    }

    pub async fn run_now(&self, id_or_name: &str) -> Result<CronRunRecord> {
        let job = self.store.get(id_or_name).await?;
        self.execute_job(job).await
    }

    async fn run_due(&self, now: u64) -> Result<()> {
        let due = self.store.take_due(now).await?;
        for (index, job) in due.into_iter().enumerate() {
            if index > 0 && !self.stagger.is_zero() {
                tokio::time::sleep(self.stagger).await;
            }
            if let Err(error) = self.execute_job(job).await {
                warn!(%error, "记录 cron 执行结果失败");
            }
        }
        Ok(())
    }

    async fn execute_job(&self, job: CronJob) -> Result<CronRunRecord> {
        {
            let mut running = self.running.lock().await;
            if !running.insert(job.id.clone()) {
                bail!("cron 已在运行：{}", job.name);
            }
        }
        let started_at = unix_now()?;
        let mut attempts = 0_u32;
        let final_result = loop {
            attempts = attempts.saturating_add(1);
            match self.runner.run(&job).await {
                Ok(result) => break (true, result),
                Err(error) if cron_retry_requires_reconcile(&error) => {
                    break (false, format!("{error:#}"));
                }
                Err(error) if attempts <= job.max_retries => {
                    let exponent = attempts.saturating_sub(1).min(20);
                    let delay = job.backoff_seconds.saturating_mul(1_u64 << exponent);
                    if delay > 0 {
                        tokio::time::sleep(Duration::from_secs(delay)).await;
                    }
                    debug!(job = %job.name, attempts, %error, "cron 任务准备重试");
                }
                Err(error) => break (false, format!("{error:#}")),
            }
        };
        self.running.lock().await.remove(&job.id);
        let record = CronRunRecord {
            started_at,
            finished_at: unix_now()?,
            attempts,
            success: final_result.0,
            result: final_result.1,
        };
        self.store.record(&job.id, record.clone()).await?;
        Ok(record)
    }

    async fn heartbeat_once(&self) {
        let jobs = self.store.list().await;
        debug!(jobs = jobs.len(), "heartbeat：cron store 自检完成");
    }

    pub async fn keeps_daemon_alive(&self) -> bool {
        self.heartbeat.is_some()
            || !self.running.lock().await.is_empty()
            || self.store.has_enabled_jobs().await
    }
}

fn cron_retry_requires_reconcile(error: &anyhow::Error) -> bool {
    if error
        .downcast_ref::<agent_core::RunOutcomeUnknown>()
        .is_some()
    {
        return true;
    }
    matches!(
        error.downcast_ref::<agent_storage::RuntimeError>(),
        Some(
            agent_storage::RuntimeError::Authorization(_)
                | agent_storage::RuntimeError::Coordination(_)
                | agent_storage::RuntimeError::Persistence(_)
                | agent_storage::RuntimeError::Artifact(_)
        )
    )
}

pub struct UnattendedApproval;

#[async_trait]
impl Approval for UnattendedApproval {
    async fn request(&self, prompt: &str) -> Result<bool> {
        warn!(prompt, "无人值守 cron 动作需要审批，已安全拒绝");
        Ok(false)
    }
}

fn unix_now() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("系统时间早于 UNIX_EPOCH")
        .map(|duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use crate::provider::{Message, Response, ToolSpec};
    use agent_storage_ports::SessionReadRepository;

    use super::*;

    struct FakeRunner {
        failures: usize,
        calls: AtomicUsize,
    }

    struct TextProvider;

    #[async_trait]
    impl Provider for TextProvider {
        async fn chat(&self, _messages: &[Message], _tools: &[ToolSpec]) -> Result<Response> {
            Ok(Response::Text("scheduled-result".to_owned()))
        }
    }

    struct RejectedRunner;

    #[async_trait]
    impl ScheduledJobRunner for RejectedRunner {
        async fn run(&self, _job: &CronJob) -> Result<String> {
            if !UnattendedApproval.request("danger").await? {
                bail!("无人值守审批已拒绝");
            }
            Ok("unexpected".to_owned())
        }
    }

    #[async_trait]
    impl ScheduledJobRunner for FakeRunner {
        async fn run(&self, _job: &CronJob) -> Result<String> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call < self.failures {
                bail!("planned failure")
            }
            Ok("done".to_owned())
        }
    }

    fn workspace(name: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("my-agent-cron-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[tokio::test]
    async fn persists_due_job_and_records_success() {
        let workspace = workspace("persist");
        let store = Arc::new(CronStore::load(&workspace).await.unwrap());
        let job = store
            .add(
                "fast".to_owned(),
                ScheduleSpec::Interval { seconds: 5 },
                "run".to_owned(),
                0,
                0,
            )
            .await
            .unwrap();
        let runner = Arc::new(FakeRunner {
            failures: 0,
            calls: AtomicUsize::new(0),
        });
        let manager = CronManager::new(
            store.clone(),
            runner,
            Duration::from_secs(1),
            Duration::ZERO,
            None,
        );
        manager.run_due(job.next_run_at).await.unwrap();

        let reloaded = CronStore::load(&workspace).await.unwrap();
        let jobs = reloaded.list().await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].history.len(), 1);
        assert!(jobs[0].history[0].success);
        let _ = std::fs::remove_dir_all(workspace);
    }

    #[tokio::test]
    async fn stops_after_finite_retries() {
        let workspace = workspace("retry");
        let store = Arc::new(CronStore::load(&workspace).await.unwrap());
        let job = store
            .add(
                "failure".to_owned(),
                ScheduleSpec::Interval { seconds: 5 },
                "fail".to_owned(),
                2,
                0,
            )
            .await
            .unwrap();
        let runner = Arc::new(FakeRunner {
            failures: usize::MAX,
            calls: AtomicUsize::new(0),
        });
        let manager = CronManager::new(
            store,
            runner.clone(),
            Duration::from_secs(1),
            Duration::ZERO,
            None,
        );
        let record = manager.run_now(&job.id).await.unwrap();
        assert!(!record.success);
        assert_eq!(record.attempts, 3);
        assert_eq!(runner.calls.load(Ordering::SeqCst), 3);
        let _ = std::fs::remove_dir_all(workspace);
    }

    #[tokio::test]
    async fn unattended_approval_always_refuses() {
        assert!(!UnattendedApproval.request("danger").await.unwrap());
    }

    #[tokio::test]
    async fn agent_runner_uses_an_independent_persisted_session() {
        let workspace = workspace("independent-session");
        let store = CronStore::load(&workspace).await.unwrap();
        let job = store
            .add(
                "session".to_owned(),
                ScheduleSpec::Interval { seconds: 5 },
                "run".to_owned(),
                0,
                0,
            )
            .await
            .unwrap();
        let canonical = Arc::new(
            agent_storage::RunStore::open(&workspace.join(".my-agent/runtime.sqlite3"))
                .unwrap()
                .enable_session_writer()
                .unwrap(),
        );
        let runner = AgentCronRunner::new(
            Arc::new(TextProvider),
            ToolRegistry::new(),
            workspace.clone(),
            ContextConfig {
                token_budget: 1_000_000,
                recent_messages: 100,
                mild_compression_percent: 60,
                strong_compression_percent: 85,
                summary_chunk_tokens: 100_000,
            },
        )
        .with_canonical_execution(canonical, None, Arc::new(agent_policy::PolicyEngine));
        assert_eq!(runner.run(&job).await.unwrap(), "scheduled-result");
        use agent_storage::{RunStore, SessionQuery};
        let repository = RunStore::open(&workspace.join(".my-agent/runtime.sqlite3")).unwrap();
        let sessions = repository.session_metadata_list().unwrap();
        assert_eq!(sessions.len(), 1);
        let snapshot = repository
            .read_session(
                &agent_core::TenantContext::local(),
                &sessions[0].key,
                agent_core::HistoryReadMode::Canonical,
            )
            .await
            .unwrap();
        assert_eq!(snapshot.messages.len(), 2);
        assert_eq!(
            snapshot.messages[1].content.as_deref(),
            Some("scheduled-result")
        );
        assert!(!workspace.join(".my-agent/cron-sessions").exists());
        assert!(!workspace.join(".my-agent/session.jsonl").exists());
        let _ = std::fs::remove_dir_all(workspace);
    }

    #[tokio::test]
    async fn unattended_rejection_is_recorded_as_failure() {
        let workspace = workspace("approval-record");
        let store = Arc::new(CronStore::load(&workspace).await.unwrap());
        let job = store
            .add(
                "danger".to_owned(),
                ScheduleSpec::Interval { seconds: 5 },
                "danger".to_owned(),
                0,
                0,
            )
            .await
            .unwrap();
        let manager = CronManager::new(
            store.clone(),
            Arc::new(RejectedRunner),
            Duration::from_secs(1),
            Duration::ZERO,
            None,
        );
        let record = manager.run_now(&job.id).await.unwrap();
        assert!(!record.success);
        assert!(record.result.contains("无人值守审批已拒绝"));
        assert_eq!(store.get(&job.id).await.unwrap().history.len(), 1);
        let _ = std::fs::remove_dir_all(workspace);
    }

    #[test]
    fn accepts_standard_five_field_cron() {
        assert!(parse_five_field_cron("0 9 * * 1-5").is_ok());
        assert!(parse_five_field_cron("0 9 * *").is_err());
    }
    struct AmbiguousProvider {
        calls: AtomicUsize,
    }
    #[async_trait]
    impl Provider for AmbiguousProvider {
        async fn chat(&self, _messages: &[Message], _tools: &[ToolSpec]) -> Result<Response> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(Response::ToolCalls(vec![agent_core::ToolCall {
                    id: "effect-once".into(),
                    name: "write_file".into(),
                    arguments: serde_json::json!({}),
                }]))
            } else {
                Ok(Response::Text("文本无法证明副作用结果".into()))
            }
        }
    }
    struct AmbiguousEffect {
        path: PathBuf,
        calls: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl crate::tools::Tool for AmbiguousEffect {
        fn name(&self) -> &str {
            "write_file"
        }
        fn description(&self) -> &str {
            "写入测试文件后模拟外部结果丢失"
        }
        fn parameters(&self) -> serde_json::Value {
            serde_json::json!({"type":"object","properties":{},"additionalProperties":false})
        }
        async fn execute(&self, _args: serde_json::Value) -> Result<String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            std::fs::write(&self.path, "只执行一次")?;
            Err(agent_core::ToolOutcomeUnknown("受控丢失确认".into()).into())
        }
    }
    #[tokio::test]
    async fn native_cron_unknown_side_effect_is_not_success_and_is_not_retried() {
        let workspace = workspace("canonical-unknown");
        let repository = Arc::new(
            agent_storage::RunStore::open(&workspace.join(".my-agent/runtime.sqlite3"))
                .unwrap()
                .enable_session_writer()
                .unwrap(),
        );
        let store = Arc::new(CronStore::load(&workspace).await.unwrap());
        let job = store
            .add(
                "unknown".into(),
                ScheduleSpec::Interval { seconds: 86400 },
                "执行一次".into(),
                3,
                0,
            )
            .await
            .unwrap();
        let provider = Arc::new(AmbiguousProvider {
            calls: AtomicUsize::new(0),
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let mut tools = ToolRegistry::new();
        tools.register(AmbiguousEffect {
            path: workspace.join("only-once.txt"),
            calls: calls.clone(),
        });
        let runner = Arc::new(
            AgentCronRunner::new(
                provider.clone(),
                tools,
                workspace.clone(),
                ContextConfig {
                    token_budget: 1_000_000,
                    recent_messages: 100,
                    mild_compression_percent: 60,
                    strong_compression_percent: 85,
                    summary_chunk_tokens: 100_000,
                },
            )
            .with_canonical_execution(
                repository.clone(),
                None,
                Arc::new(agent_policy::PolicyEngine),
            ),
        );
        let manager = CronManager::new(store, runner, Duration::from_secs(1), Duration::ZERO, None);
        let record = manager.run_now(&job.id).await.unwrap();
        assert!(!record.success);
        assert_eq!(record.attempts, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            std::fs::read_to_string(workspace.join("only-once.txt")).unwrap(),
            "只执行一次"
        );
        use agent_storage::SessionQuery;
        let sessions = repository.session_metadata_list().unwrap();
        assert_eq!(sessions.len(), 1);
        let db = rusqlite::Connection::open(workspace.join(".my-agent/runtime.sqlite3")).unwrap();
        let status: String = db
            .query_row("SELECT status FROM runs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(status, "unknown_after_restart");
        let outcome: String = db
            .query_row("SELECT outcome FROM tool_executions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(outcome, "outcome_unknown");
        let run: String = db
            .query_row("SELECT id FROM runs", [], |r| r.get(0))
            .unwrap();
        let native = repository
            .read_run(&agent_core::RunId(run))
            .unwrap()
            .unwrap();
        assert_eq!(native.status, agent_core::RunStatus::UnknownAfterRestart);
        assert!(provider.calls.load(Ordering::SeqCst) <= 2);
        drop(db);
        std::fs::remove_dir_all(workspace).unwrap();
    }
    #[tokio::test]
    async fn immediate_repetitions_of_one_job_have_distinct_native_occurrences() {
        let workspace = workspace("distinct-occurrences");
        let store = CronStore::load(&workspace).await.unwrap();
        let job = store
            .add(
                "fast".into(),
                ScheduleSpec::Interval { seconds: 86400 },
                "独立历史".into(),
                0,
                0,
            )
            .await
            .unwrap();
        let canonical = Arc::new(
            agent_storage::RunStore::open(&workspace.join(".my-agent/runtime.sqlite3"))
                .unwrap()
                .enable_session_writer()
                .unwrap(),
        );
        let runner = AgentCronRunner::new(
            Arc::new(TextProvider),
            ToolRegistry::new(),
            workspace.clone(),
            ContextConfig {
                token_budget: 1_000_000,
                recent_messages: 100,
                mild_compression_percent: 60,
                strong_compression_percent: 85,
                summary_chunk_tokens: 100_000,
            },
        )
        .with_canonical_execution(canonical, None, Arc::new(agent_policy::PolicyEngine));
        runner.run(&job).await.unwrap();
        runner.run(&job).await.unwrap();
        use agent_storage::SessionQuery;
        let repository =
            agent_storage::RunStore::open(&workspace.join(".my-agent/runtime.sqlite3")).unwrap();
        let sessions = repository.session_metadata_list().unwrap();
        assert_eq!(sessions.len(), 2);
        assert_ne!(sessions[0].key, sessions[1].key);
        for session in sessions {
            assert_eq!(
                repository
                    .read_session(
                        &agent_core::TenantContext::local(),
                        &session.key,
                        agent_core::HistoryReadMode::Canonical
                    )
                    .await
                    .unwrap()
                    .messages
                    .len(),
                2
            );
        }
        std::fs::remove_dir_all(workspace).unwrap();
    }
    struct AdvancingOwnerProvider {
        path: PathBuf,
    }
    #[async_trait]
    impl Provider for AdvancingOwnerProvider {
        async fn chat(&self, _: &[Message], _: &[ToolSpec]) -> Result<Response> {
            let db = rusqlite::Connection::open(&self.path)?;
            db.execute(
                "UPDATE runs SET generation=generation+1 WHERE status='running'",
                [],
            )?;
            Ok(Response::Text("迟到结果不能结算新generation".into()))
        }
    }
    fn native_runner(
        repository: Arc<agent_storage::RunStore>,
        provider: Arc<dyn Provider>,
        workspace: PathBuf,
    ) -> AgentCronRunner {
        AgentCronRunner::new(
            provider,
            ToolRegistry::new(),
            workspace,
            ContextConfig {
                token_budget: 1_000_000,
                recent_messages: 100,
                mild_compression_percent: 60,
                strong_compression_percent: 85,
                summary_chunk_tokens: 100_000,
            },
        )
        .with_canonical_execution(repository, None, Arc::new(agent_policy::PolicyEngine))
    }
    #[tokio::test]
    async fn native_cron_late_result_cannot_finalize_a_later_owner_generation() {
        let workspace = workspace("late-owner");
        let path = workspace.join(".my-agent/runtime.sqlite3");
        let repository = Arc::new(
            agent_storage::RunStore::open(&path)
                .unwrap()
                .enable_session_writer()
                .unwrap(),
        );
        let store = CronStore::load(&workspace).await.unwrap();
        let job = store
            .add(
                "late".into(),
                ScheduleSpec::Interval { seconds: 86400 },
                "执行".into(),
                0,
                0,
            )
            .await
            .unwrap();
        let runner = native_runner(
            repository.clone(),
            Arc::new(AdvancingOwnerProvider { path: path.clone() }),
            workspace.clone(),
        );
        assert!(runner.run(&job).await.is_err());
        let db = rusqlite::Connection::open(path).unwrap();
        let (status, terminals): (String, u64) = db
            .query_row(
                "SELECT status,(SELECT count(*) FROM events WHERE event='terminal') FROM runs",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "running", "旧执行体不能用重取owner结算新generation");
        assert_eq!(terminals, 0);
        db.execute_batch("UPDATE runs SET generation=generation-1")
            .unwrap();
        drop(db);
        drop(runner);
        drop(repository);
        let _ = std::fs::remove_dir_all(workspace);
    }
    #[tokio::test]
    async fn native_cron_has_its_own_frozen_workload_authorization() {
        let workspace = workspace("workload-identity");
        let path = workspace.join(".my-agent/runtime.sqlite3");
        let repository = Arc::new(
            agent_storage::RunStore::open(&path)
                .unwrap()
                .enable_session_writer()
                .unwrap(),
        );
        let store = CronStore::load(&workspace).await.unwrap();
        let job = store
            .add(
                "workload".into(),
                ScheduleSpec::Interval { seconds: 86400 },
                "执行".into(),
                0,
                0,
            )
            .await
            .unwrap();
        let runner = native_runner(
            repository.clone(),
            Arc::new(TextProvider),
            workspace.clone(),
        );
        assert_eq!(runner.run(&job).await.unwrap(), "scheduled-result");
        let db = rusqlite::Connection::open(path).unwrap();
        let count: u64 = db
            .query_row(
                "SELECT count(*) FROM run_authorizations WHERE subject_id='local-cron-workload'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "不能借Local人类身份或缺失授权代表后台workload");
        drop(db);
        drop(runner);
        drop(repository);
        let _ = std::fs::remove_dir_all(workspace);
    }
    #[tokio::test(flavor = "current_thread")]
    async fn native_cron_real_sqlite_write_lock_does_not_block_runtime_timer() {
        let workspace = workspace("async-admission");
        let path = workspace.join(".my-agent/runtime.sqlite3");
        let repository = Arc::new(
            agent_storage::RunStore::open(&path)
                .unwrap()
                .enable_session_writer()
                .unwrap(),
        );
        let store = CronStore::load(&workspace).await.unwrap();
        let job = store
            .add(
                "async".into(),
                ScheduleSpec::Interval { seconds: 86400 },
                "执行".into(),
                0,
                0,
            )
            .await
            .unwrap();
        let runner = native_runner(
            repository.clone(),
            Arc::new(TextProvider),
            workspace.clone(),
        );
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let holder = std::thread::spawn(move || {
            let db = rusqlite::Connection::open(path).unwrap();
            db.execute_batch("BEGIN IMMEDIATE").unwrap();
            tx.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(300));
            db.execute_batch("COMMIT").unwrap();
        });
        rx.recv().unwrap();
        let started = std::time::Instant::now();
        let (result, timer) = tokio::join!(runner.run(&job), async {
            tokio::time::sleep(Duration::from_millis(25)).await;
            started.elapsed()
        });
        holder.join().unwrap();
        result.unwrap();
        assert!(
            timer < Duration::from_millis(150),
            "timer被同步Cron数据库调用阻塞 {timer:?}"
        );
        drop(runner);
        drop(repository);
        let _ = std::fs::remove_dir_all(workspace);
    }
}
