//! 受信 hook 配置、单一子进程调度和 exact run task scope。
use crate::loop_engine::CancellationToken;
use agent_core::*;
use agent_storage::ControlRepository;
use anyhow::{Result, bail};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct HookCommand {
    event: HookEvent,
    executable: PathBuf,
    #[serde(default = "timeout_ms")]
    timeout_ms: u64,
}
fn timeout_ms() -> u64 {
    1000
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HookConfig {
    schema_version: u16,
    hooks: Vec<HookCommand>,
}

#[derive(Clone, Default)]
pub struct HookDispatcher {
    commands: BTreeMap<HookEvent, HookCommand>,
    config_failure: Option<HookFailure>,
    workspace: PathBuf,
    config_directory: PathBuf,
    uid: u32,
    fingerprint: String,
}

impl HookDispatcher {
    pub fn load(workspace: &Path) -> Self {
        match std::env::var_os("MY_AGENT_HOOKS_CONFIG") {
            Some(path) => Self::load_path(workspace, &PathBuf::from(path)),
            None => Self {
                workspace: workspace.to_path_buf(),
                ..Self::default()
            },
        }
    }
    fn load_path(workspace: &Path, file: &Path) -> Self {
        let mut dispatcher = Self {
            workspace: workspace.to_path_buf(),
            config_directory: file.parent().map_or_else(PathBuf::new, Path::to_path_buf),
            ..Self::default()
        };
        if !file.is_absolute() {
            dispatcher.config_failure = Some(HookFailure::Untrusted);
            return dispatcher;
        }
        if std::fs::symlink_metadata(file).is_err() {
            dispatcher.config_failure = Some(HookFailure::Untrusted);
            return dispatcher;
        }
        match dispatcher.load_config(file) {
            Ok(commands) => dispatcher.commands = commands,
            Err(failure) => dispatcher.config_failure = Some(failure),
        }
        dispatcher
    }
    fn load_config(
        &mut self,
        file: &Path,
    ) -> std::result::Result<BTreeMap<HookEvent, HookCommand>, HookFailure> {
        let identity = std::process::Command::new("/usr/bin/id")
            .arg("-u")
            .env_clear()
            .output()
            .map_err(|_| HookFailure::Untrusted)?;
        self.uid = std::str::from_utf8(&identity.stdout)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .ok_or(HookFailure::Untrusted)?;
        trusted(&self.workspace, self.uid, false, false)?;
        trusted(&self.config_directory, self.uid, true, false)?;
        trusted(file, self.uid, true, false)?;
        let metadata = std::fs::symlink_metadata(file).map_err(|_| HookFailure::InvalidConfig)?;
        if metadata.len() > 16384 {
            return Err(HookFailure::InvalidConfig);
        }
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        let opened = options.open(file).map_err(|_| HookFailure::Untrusted)?;
        let opened_metadata = opened.metadata().map_err(|_| HookFailure::Untrusted)?;
        if !opened_metadata.is_file() {
            return Err(HookFailure::Untrusted);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if metadata.dev() != opened_metadata.dev()
                || metadata.ino() != opened_metadata.ino()
                || opened_metadata.nlink() != 1
            {
                return Err(HookFailure::Untrusted);
            }
        }
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut std::io::Read::take(opened, 16385), &mut bytes)
            .map_err(|_| HookFailure::InvalidConfig)?;
        if bytes.len() > 16384 {
            return Err(HookFailure::InvalidConfig);
        }
        self.fingerprint = format!("{:x}", Sha256::digest(&bytes));
        let config: HookConfig =
            serde_json::from_slice(&bytes).map_err(|_| HookFailure::InvalidConfig)?;
        if config.schema_version != 1 || config.hooks.len() > 16 {
            return Err(HookFailure::InvalidConfig);
        }
        let mut commands = BTreeMap::new();
        for command in config.hooks {
            if !command.executable.is_absolute()
                || !(10..=5000).contains(&command.timeout_ms)
                || commands.contains_key(&command.event)
            {
                return Err(HookFailure::InvalidConfig);
            }
            self.validate_executable(&command.executable)?;
            commands.insert(command.event, command);
        }
        Ok(commands)
    }
    fn validate_executable(&self, path: &Path) -> std::result::Result<(), HookFailure> {
        // 可执行物必须位于受信私有配置目录，禁止 workspace 中任意脚本与 shell command 字符串。
        if path.parent() != Some(self.config_directory.as_path()) {
            return Err(HookFailure::Untrusted);
        }
        trusted(&self.workspace, self.uid, false, false)?;
        trusted(
            path.parent().ok_or(HookFailure::Untrusted)?,
            self.uid,
            true,
            false,
        )?;
        trusted(path, self.uid, true, true)
    }
    pub fn matches_registry_refs(&self,refs:&[ExtensionRef])->bool {refs.iter().any(|r|r.digest==self.fingerprint && r.trust>=PluginTrust::OrganizationReviewed)}
    pub fn configured(&self, event: HookEvent) -> bool {
        self.config_failure.is_some() || self.commands.contains_key(&event)
    }

    pub async fn invoke(
        &self,
        store: &dyn ControlRepository,
        mut payload: HookPayload,
        cancel: &CancellationToken,
    ) -> Result<HookEffect> {
        if !self.configured(payload.event) {
            return Ok(HookEffect::Observe {});
        }
        payload.data["hook_config_digest"] = serde_json::json!(self.fingerprint);
        if let HookClaim::Existing(outcome) = store.claim_hook(&payload)? {
            return effect_result(&outcome);
        }
        let mut claim = ClaimGuard {
            store,
            payload: payload.clone(),
            armed: true,
        };
        let result = match self.config_failure {
            Some(failure) => Err(failure),
            None => match self.commands.get(&payload.event) {
                Some(command) => {
                    let mut started = false;
                    self.execute(command, &payload, cancel, &mut started).await
                }
                None => Ok(HookEffect::Observe {}),
            },
        };
        let outcome = make_outcome(payload, result);
        let settled = store.settle_hook(&outcome)?;
        claim.armed = false;
        effect_result(&settled)
    }

    pub async fn invoke_managed(
        &self,
        context: HookExecutionContext,
        mut payload: HookPayload,
        parent: &CancellationToken,
    ) -> Result<HookEffect> {
        if !self.configured(payload.event) {
            return Ok(HookEffect::Observe {});
        }
        if !payload.data.is_object() {
            bail!("hook数据必须是对象");
        }
        payload.data["hook_config_digest"] = serde_json::json!(self.fingerprint);
        let HookExecutionContext {
            store,
            scope,
            envelope,
            tasks,
        } = context;
        let dispatcher = self.clone();
        let cancellation = CancellationToken::new();
        if parent.is_cancelled() {
            cancellation.cancel();
        }
        let _cancel_on_drop = CancelHookOnDrop(cancellation.clone());
        let actor_cancel = cancellation.clone();
        let (completed, result) = tokio::sync::oneshot::channel();
        let job = async move {
            let run = async {
                // await不能在取消时detach：已接受的claim收敛后才判定能否启动进程。
                let claim = store
                    .claim_hook_effect(&scope, envelope.as_ref(), &payload)
                    .await?;
                if let HookClaim::Existing(outcome) = claim {
                    return effect_result(&outcome);
                }
                let mut started = false;
                let execution = if actor_cancel.is_cancelled() {
                    Err(HookFailure::Cancelled)
                } else {
                    match dispatcher.config_failure {
                        Some(failure) => Err(failure),
                        None => match dispatcher.commands.get(&payload.event) {
                            Some(command) => {
                                dispatcher
                                    .execute(command, &payload, &actor_cancel, &mut started)
                                    .await
                            }
                            None => Ok(HookEffect::Observe {}),
                        },
                    }
                };
                let uncertain = started
                    && matches!(
                        execution,
                        Err(HookFailure::Cancelled | HookFailure::Timeout)
                    );
                let mut outcome = make_outcome(payload, execution);
                if uncertain {
                    outcome.status = HookStatus::UnknownAfterRestart;
                    outcome.effect = None;
                    outcome.failure = Some(HookFailure::UnknownAfterRestart);
                }
                let settled = store
                    .settle_hook_effect(&scope, envelope.as_ref(), &outcome)
                    .await?;
                effect_result(&settled)
            };
            let _ = completed.send(run.await);
        };
        if !tasks.spawn_hook_job(Box::pin(job)).await {
            bail!("hook执行owner已停止");
        }
        let mut result = result;
        tokio::select! {
            result=&mut result=>result.map_err(|_|anyhow::anyhow!("hook执行owner未返回结果"))?,
            _=parent.cancelled()=>{cancellation.cancel();result.await.map_err(|_|anyhow::anyhow!("hook取消结果未确认"))?}
        }
    }

    async fn execute(
        &self,
        command: &HookCommand,
        payload: &HookPayload,
        cancel: &CancellationToken,
        started: &mut bool,
    ) -> std::result::Result<HookEffect, HookFailure> {
        self.validate_executable(&command.executable)?;
        if cancel.is_cancelled() {
            return Err(HookFailure::Cancelled);
        }
        let mut process = tokio::process::Command::new(&command.executable);
        process
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("LANG", "C.UTF-8")
            .current_dir(&self.workspace)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        process.process_group(0);
        let mut child = process.spawn().map_err(|_| HookFailure::SpawnFailed)?;
        *started = true;
        let group = ProcessGroup(child.id());
        let mut input = child.stdin.take().ok_or(HookFailure::SpawnFailed)?;
        let stdout = child.stdout.take().ok_or(HookFailure::SpawnFailed)?;
        let stderr = child.stderr.take().ok_or(HookFailure::SpawnFailed)?;
        let bytes = serde_json::to_vec(payload).map_err(|_| HookFailure::InvalidOutput)?;
        let operation = async {
            let send = async move {
                input
                    .write_all(&bytes)
                    .await
                    .map_err(|_| HookFailure::SpawnFailed)?;
                input
                    .shutdown()
                    .await
                    .map_err(|_| HookFailure::SpawnFailed)?;
                drop(input);
                Ok::<(), HookFailure>(())
            };
            let wait = async { child.wait().await.map_err(|_| HookFailure::SpawnFailed) };
            let (_, status, out, _) =
                tokio::try_join!(send, wait, bounded_output(stdout), bounded_output(stderr))?;
            if !status.success() {
                return Err(HookFailure::NonzeroExit);
            }
            serde_json::from_slice::<HookEffect>(&out).map_err(|_| HookFailure::InvalidOutput)
        };
        let result = tokio::select! {
            outcome=tokio::time::timeout(std::time::Duration::from_millis(command.timeout_ms),operation)=>outcome.unwrap_or(Err(HookFailure::Timeout)),
            _=cancel.cancelled()=>Err(HookFailure::Cancelled),
        };
        drop(group);
        if result.is_err() {
            let _ = child.kill().await;
        }
        let _ = child.wait().await;
        result
    }
}

fn make_outcome(
    payload: HookPayload,
    result: std::result::Result<HookEffect, HookFailure>,
) -> HookOutcome {
    match result {
        Ok(HookEffect::Continue { .. }) if payload.data["continuation_allowed"] == false => {
            HookOutcome {
                payload,
                status: HookStatus::Failed,
                effect: None,
                failure: Some(HookFailure::BudgetExceeded),
            }
        }
        Ok(effect) if payload.event.permits(&effect) => HookOutcome {
            payload,
            status: HookStatus::Succeeded,
            effect: Some(effect),
            failure: None,
        },
        Ok(_) => HookOutcome {
            payload,
            status: HookStatus::Failed,
            effect: None,
            failure: Some(HookFailure::InvalidEffect),
        },
        Err(failure) => HookOutcome {
            payload,
            status: HookStatus::Failed,
            effect: None,
            failure: Some(failure),
        },
    }
}

fn effect_result(outcome: &HookOutcome) -> Result<HookEffect> {
    if outcome.status != HookStatus::Succeeded {
        tracing::warn!(event=?outcome.payload.event,failure=?outcome.failure,"hook 失败");
        if outcome.payload.event.controls() {
            bail!("控制 hook 失败: {:?}", outcome.failure);
        }
        return Ok(HookEffect::Observe {});
    }
    match &outcome.effect {
        Some(HookEffect::Deny { .. }) => bail!("控制 hook 拒绝本次操作"),
        Some(effect) => Ok(effect.clone()),
        None => bail!("hook 成功结果缺 effect"),
    }
}

async fn bounded_output(
    reader: impl tokio::io::AsyncRead + Unpin,
) -> std::result::Result<Vec<u8>, HookFailure> {
    let mut bytes = Vec::new();
    reader
        .take(8193)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| HookFailure::SpawnFailed)?;
    if bytes.len() > 8192 {
        return Err(HookFailure::OutputLimit);
    }
    Ok(bytes)
}

struct ProcessGroup(Option<u32>);
struct ClaimGuard<'a> {
    store: &'a dyn ControlRepository,
    payload: HookPayload,
    armed: bool,
}
impl Drop for ClaimGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            let outcome = HookOutcome {
                payload: self.payload.clone(),
                status: HookStatus::Failed,
                effect: None,
                failure: Some(HookFailure::Cancelled),
            };
            if let Err(error) = self.store.settle_hook(&outcome) {
                tracing::error!(%error,"hook 取消结算失败");
            }
        }
    }
}
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        if let Some(pid) = self.0 {
            crate::tools::terminate_process_group(pid);
        }
    }
}

#[cfg(unix)]
fn trusted(
    path: &Path,
    uid: u32,
    private: bool,
    executable: bool,
) -> std::result::Result<(), HookFailure> {
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::symlink_metadata(path).map_err(|_| HookFailure::Untrusted)?;
    if meta.file_type().is_symlink()
        || meta.uid() != uid
        || meta.mode() & if private { 0o077 } else { 0o022 } != 0
        || (executable && (!meta.is_file() || meta.mode() & 0o100 == 0 || meta.nlink() != 1))
    {
        return Err(HookFailure::Untrusted);
    }
    Ok(())
}
#[cfg(not(unix))]
fn trusted(
    _path: &Path,
    _uid: u32,
    _private: bool,
    _executable: bool,
) -> std::result::Result<(), HookFailure> {
    Err(HookFailure::Untrusted)
}

/// 只拥有执行句柄；领域结果仍属于原SQL HookOutcome。
#[async_trait::async_trait]
pub trait HookTaskOwner: Send + Sync {
    async fn spawn_hook_job(
        &self,
        job: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>,
    ) -> bool;
}
#[derive(Clone)]
pub struct HookExecutionContext {
    pub store: Arc<dyn agent_storage_ports::HookExecutionRepository>,
    pub scope: TenantContext,
    pub envelope: Option<EnterpriseOwnerEnvelope>,
    pub tasks: Arc<dyn HookTaskOwner>,
}
struct CancelHookOnDrop(CancellationToken);
impl Drop for CancelHookOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
#[derive(Clone)]
pub struct HookScope {
    pub dispatcher: Arc<HookDispatcher>,
    pub execution: HookExecutionContext,
}
tokio::task_local! {static ACTIVE_HOOKS:HookScope;}
pub async fn with_scope<F: std::future::Future>(scope: HookScope, future: F) -> F::Output {
    ACTIVE_HOOKS.scope(scope, future).await
}
pub async fn current(
    event: HookEvent,
    suffix: &str,
    mut data: serde_json::Value,
    cancel: &CancellationToken,
) -> Result<HookEffect> {
    let Ok(scope) = ACTIVE_HOOKS.try_with(Clone::clone) else {
        return Ok(HookEffect::Observe {});
    };
    if !scope.dispatcher.configured(event) {
        return Ok(HookEffect::Observe {});
    }
    let envelope = scope
        .execution
        .envelope
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("run hook缺固定准入proof"))?;
    let owner = &envelope.owner;
    if matches!(
        event,
        HookEvent::ApprovalRequested | HookEvent::ApprovalResolved
    ) {
        let id = data["interaction_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("hook 缺 interaction 身份"))?;
        let record = scope
            .execution
            .store
            .read_hook_interaction(envelope, &InteractionId(id.into()))
            .await?
            .ok_or_else(|| anyhow::anyhow!("hook interaction 未持久发布"))?;
        if record.owner_run_id != owner.run_id || record.session_id != owner.session_key {
            bail!("hook interaction owner 冲突");
        }
        if event == HookEvent::ApprovalResolved && record.status == "pending" {
            return Ok(HookEffect::Observe {});
        }
        data["interaction_revision"] = serde_json::json!(record.revision);
        data["interaction_status"] = serde_json::json!(record.status);
        data["approved"] = serde_json::json!(
            record
                .response
                .as_ref()
                .and_then(|response| response["approved"].as_bool())
        );
    }
    let snapshot = scope.execution.store.read_hook_snapshot(envelope).await?;
    if let Some(g)=&snapshot.installed_generation {
        if g.effective.config.hook_refs.is_empty() {return Ok(HookEffect::Observe {});}
        anyhow::ensure!(g.effective.config.hook_refs.iter().any(|r|r.digest==scope.dispatcher.fingerprint && r.trust>=PluginTrust::OrganizationReviewed),"当前Hook不属于冻结发行代");
    }
    let route_digest = snapshot
        .route
        .as_ref()
        .map(|route| serde_json::to_vec(route).map(|raw| format!("{:x}", Sha256::digest(raw))))
        .transpose()?;
    let payload = HookPayload {
        schema_version: 1,
        event,
        session_key: owner.session_key.clone(),
        session_lifetime_id: owner.session_lifetime_id.clone(),
        owner: Some(owner.clone()),
        cwd: snapshot.cwd,
        channel: snapshot.entry_channel.key().into(),
        permission_mode: snapshot.permission_mode,
        route_digest,
        data,
        operation_id: format!(
            "hook:{event:?}:{}:{}",
            owner.run_id.0,
            if suffix.len() > 128 {
                format!("{:x}", Sha256::digest(suffix.as_bytes()))
            } else {
                suffix.into()
            }
        ),
    };
    scope
        .dispatcher
        .invoke_managed(scope.execution.clone(), payload, cancel)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_storage::{HookRepository, RunStore, SessionLifecycle, SessionQuery};
    use std::os::unix::fs::PermissionsExt;
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    fn fixture(
        script: &str,
        event: HookEvent,
        timeout: u64,
    ) -> (PathBuf, HookDispatcher, RunStore, HookPayload) {
        let root = std::env::temp_dir().join(format!(
            "hook-process-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(root.join(".my-agent")).unwrap();
        let root = std::fs::canonicalize(root).unwrap();
        let directory = root.join(".my-agent");
        let executable = directory.join("hook");
        std::fs::write(
            &executable,
            format!("#!/bin/sh\ncat >/dev/null\n{script}\n"),
        )
        .unwrap();
        let config = directory.join("hooks.json");
        std::fs::write(&config,serde_json::to_vec(&serde_json::json!({"schema_version":1,"hooks":[{"event":event,"executable":executable,"timeout_ms":timeout}]})).unwrap()).unwrap();
        for (path, mode) in [(&directory, 0o700), (&executable, 0o700), (&config, 0o600)] {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
        }
        let dispatcher = HookDispatcher::load_path(&root, &config);
        let store = RunStore::open(Path::new(":memory:")).unwrap();
        let meta = store.create_session(&SessionKey("test".into())).unwrap();
        let Admission::New(run) = store
            .admit(
                meta.key.clone(),
                agent_daemon_protocol::RequestId::Number(1),
                "input",
            )
            .unwrap()
        else {
            panic!("new")
        };
        store.try_start_queued(&run.run_id).unwrap();
        let payload = HookPayload {
            schema_version: 1,
            event,
            session_key: meta.key,
            session_lifetime_id: meta.lifetime,
            owner: Some(store.run_owner(&run.run_id).unwrap()),
            cwd: root.to_string_lossy().into_owned(),
            channel: "test".into(),
            permission_mode: "risk".into(),
            route_digest: None,
            data: serde_json::json!({}),
            operation_id: "test".into(),
        };
        (root, dispatcher, store, payload)
    }
    #[tokio::test]
    async fn timeout_nonzero_output_limit_and_invalid_effect_have_typed_results() {
        for (script, failure) in [
            ("sleep 10", HookFailure::Timeout),
            ("exit 7", HookFailure::NonzeroExit),
            ("head -c 9000 /dev/zero", HookFailure::OutputLimit),
            (
                "printf '{\"effect\":\"continue\",\"prompt\":\"禁止\"}'",
                HookFailure::InvalidEffect,
            ),
        ] {
            let (root, dispatcher, store, payload) = fixture(
                script,
                HookEvent::PostTool,
                if failure == HookFailure::Timeout {
                    100
                } else {
                    5000
                },
            );
            assert_eq!(
                dispatcher
                    .invoke(&store, payload.clone(), &CancellationToken::new())
                    .await
                    .unwrap(),
                HookEffect::Observe {}
            );
            let outcomes = store
                .hook_outcomes(&payload.session_key, &payload.session_lifetime_id)
                .unwrap();
            assert_eq!(outcomes[0].failure, Some(failure), "{outcomes:?}");
            assert_eq!(
                dispatcher
                    .invoke(&store, payload, &CancellationToken::new())
                    .await
                    .unwrap(),
                HookEffect::Observe {}
            );
            std::fs::remove_dir_all(root).unwrap();
        }
    }
    #[tokio::test]
    async fn control_cancel_untrusted_config_and_descendant_stop_fail_closed() {
        let (root, dispatcher, store, payload) =
            fixture("printf '{\"effect\":\"allow\"}'", HookEvent::PreTool, 1000);
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        assert!(
            dispatcher
                .invoke(&store, payload.clone(), &cancellation)
                .await
                .is_err()
        );
        assert_eq!(
            store
                .hook_outcomes(&payload.session_key, &payload.session_lifetime_id)
                .unwrap()[0]
                .failure,
            Some(HookFailure::Cancelled)
        );
        std::fs::set_permissions(
            root.join(".my-agent/hooks.json"),
            std::fs::Permissions::from_mode(0o666),
        )
        .unwrap();
        let untrusted = HookDispatcher::load_path(&root, &root.join(".my-agent/hooks.json"));
        let mut denied = payload;
        denied.operation_id = "untrusted".into();
        assert!(
            untrusted
                .invoke(&store, denied.clone(), &CancellationToken::new())
                .await
                .is_err()
        );
        assert_eq!(
            store
                .hook_outcomes(&denied.session_key, &denied.session_lifetime_id)
                .unwrap()[1]
                .failure,
            Some(HookFailure::Untrusted)
        );
        std::fs::remove_dir_all(root).unwrap();
        let (root, dispatcher, store, mut payload) = fixture(
            "printf '{\"effect\":\"continue\",\"prompt\":\"继续\"}'",
            HookEvent::Stop,
            5000,
        );
        payload.data = serde_json::json!({"continuation_allowed":false});
        assert!(
            dispatcher
                .invoke(&store, payload.clone(), &CancellationToken::new())
                .await
                .is_err()
        );
        assert_eq!(
            store
                .hook_outcomes(&payload.session_key, &payload.session_lifetime_id)
                .unwrap()[0]
                .failure,
            Some(HookFailure::BudgetExceeded)
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn dropping_an_inflight_hook_settles_cancelled_without_replaying_it() {
        let (root, dispatcher, store, payload) = fixture("sleep 10", HookEvent::PreTool, 5000);
        let store = Arc::new(store);
        let child_store = store.clone();
        let request = payload.clone();
        let task = tokio::spawn(async move {
            dispatcher
                .invoke(&*child_store, request, &CancellationToken::new())
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if store
                    .hook_outcomes(&payload.session_key, &payload.session_lifetime_id)
                    .unwrap()
                    .first()
                    .is_some_and(|record| record.status == HookStatus::Running)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let outcomes = store
            .hook_outcomes(&payload.session_key, &payload.session_lifetime_id)
            .unwrap();
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].status, HookStatus::Failed);
        assert_eq!(outcomes[0].failure, Some(HookFailure::Cancelled));
        assert!(
            matches!(store.claim_hook(&outcomes[0].payload).unwrap(), HookClaim::Existing(record) if record.failure == Some(HookFailure::Cancelled))
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
