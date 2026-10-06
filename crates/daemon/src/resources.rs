//! 后台进程由 daemon 监督；SQLite 是状态 owner，进程句柄仅用于证明当前启动身份。
use super::DaemonState;
use agent_core::{ExactOwner, ResourceId, ResourceRecord};
use agent_runtime::{loop_engine::CancellationToken, tools::Tool};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde_json::{Value, json};
use std::{
    process::Stdio,
    sync::{Arc, OnceLock, Weak},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
};

struct ProcessLease(Option<u32>);
impl ProcessLease {
    fn disarm(&mut self) {
        self.0 = None;
    }
}
impl Drop for ProcessLease {
    fn drop(&mut self) {
        if let Some(pid) = self.0 {
            kill_group(pid);
        }
    }
}
impl DaemonState {
    pub(crate) async fn start_background(
        self: &Arc<Self>,
        owner: ExactOwner,
        command: String,
    ) -> Result<ResourceRecord> {
        let snapshot = self
            .run_store
            .run_snapshot(&owner.run_id)?
            .context("后台资源缺少冻结 RunSnapshot")?;
        if snapshot.sandbox_effective != "native" {
            bail!("后台执行暂不支持强隔离资源；拒绝启动 Native 进程");
        }
        let record = self.run_store.create_resource(
            &owner,
            &snapshot.cwd,
            &snapshot.sandbox_requested,
            &snapshot.sandbox_effective,
        )?;
        let mut process = Command::new("/bin/sh");
        process
            .arg("-c")
            .arg(command)
            .current_dir(&snapshot.cwd)
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("LANG", "C.UTF-8")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        process.process_group(0);
        let mut child = match process.spawn() {
            Ok(child) => child,
            Err(error) => {
                self.run_store.update_resource(
                    &owner,
                    record.id,
                    "failed",
                    None,
                    Some("spawn failed"),
                )?;
                return Err(error.into());
            }
        };
        let pid = child.id().context("后台进程缺少 PID")?;
        let mut lease = ProcessLease(Some(pid));
        let identity = format!(
            "daemon-start:{}:{}:{}",
            owner.session_lifetime_id.0, record.id.0, pid
        );
        if let Err(error) =
            self.run_store
                .update_resource(&owner, record.id, "running", Some(&identity), None)
        {
            kill_group(pid);
            let _ = child.wait().await;
            return Err(error.into());
        }
        let token = CancellationToken::new();
        self.resource_tokens
            .lock()
            .await
            .insert(record.id, token.clone());
        let stdout = child.stdout.take().context("缺少 stdout")?;
        let stderr = child.stderr.take().context("缺少 stderr")?;
        let daemon = self.clone();
        let id = record.id;
        let admitted=self.spawn_owned(async move {
            let out=drain(stdout,daemon.clone(),owner.clone(),id,"stdout");
            let err=drain(stderr,daemon.clone(),owner.clone(),id,"stderr");
            let wait=async {
                tokio::select! {
                    status=child.wait()=>status.map(|s|if s.success(){("completed","exit 0")}else{("failed","nonzero exit")}),
                    _=token.cancelled()=>{kill_group(pid);let _=child.wait().await;Ok(("stopped","exact resource stop"))},
                    _=daemon.shutdown.cancelled()=>{kill_group(pid);let _=child.wait().await;Ok(("stopped","daemon shutdown"))},
                }
            };
            let (status,outcome_out,outcome_err)=tokio::join!(wait,out,err);
            lease.disarm();
            let (state,reason)=status.unwrap_or(("failed","wait failed"));
            if outcome_out.is_err()||outcome_err.is_err(){tracing::warn!(resource_id=id.0,"后台日志持久化失败");}
            if let Err(error)=daemon.run_store.update_resource(&owner,id,state,None,Some(reason)){tracing::warn!(resource_id=id.0,error=%error,"后台资源结算被拒绝");}
            daemon.resource_tokens.lock().await.remove(&id);daemon.run_coordinator.queue_notify.notify_waiters();
        }).await;
        if !admitted {
            self.resource_tokens.lock().await.remove(&id);
            self.run_store.update_resource(
                &record.owner,
                id,
                "orphaned",
                None,
                Some("supervisor shutdown"),
            )?;
            bail!("daemon 正在关闭");
        }
        Ok(self.run_store.read_resource(id)?)
    }
    pub(crate) async fn stop_resource(
        &self,
        owner: &ExactOwner,
        id: ResourceId,
    ) -> Result<ResourceRecord> {
        let record = self.run_store.read_resource(id)?;
        record.owner.fence(owner)?;
        if let Some(token) = self.resource_tokens.lock().await.get(&id).cloned() {
            self.run_store
                .update_resource(owner, id, "stopping", None, None)?;
            token.cancel();
        } else if matches!(record.state.as_str(), "starting" | "running" | "stopping") {
            return Ok(self.run_store.update_resource(
                owner,
                id,
                "orphaned",
                None,
                Some("无法证明当前进程身份，未发送 kill"),
            )?);
        }
        Ok(self.run_store.read_resource(id)?)
    }
    pub(crate) async fn wait_resource(
        &self,
        id: ResourceId,
        timeout: Duration,
    ) -> Result<ResourceRecord> {
        let deadline = tokio::time::Instant::now() + timeout.min(Duration::from_secs(60));
        loop {
            let notified = self.run_coordinator.queue_notify.notified();
            let record = self.run_store.read_resource(id)?;
            if !matches!(record.state.as_str(), "starting" | "running" | "stopping") {
                return Ok(record);
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Ok(self.run_store.read_resource(id)?);
            }
        }
    }
}
async fn drain(
    mut reader: impl AsyncRead + Unpin,
    daemon: Arc<DaemonState>,
    owner: ExactOwner,
    id: ResourceId,
    label: &str,
) -> Result<()> {
    let mut chunk = [0u8; 4096];
    let mut error = None;
    loop {
        let count = reader.read(&mut chunk).await?;
        if count == 0 {
            break;
        }
        if error.is_none() {
            if let Err(failure) = daemon.run_store.append_resource_log(
                &owner,
                id,
                &format!("[{label}] {}", String::from_utf8_lossy(&chunk[..count])),
            ) {
                error = Some(failure);
            } else {
                daemon.run_coordinator.queue_notify.notify_waiters();
            }
        }
    }
    if let Some(error) = error {
        return Err(error.into());
    }
    Ok(())
}
#[cfg(unix)]
fn kill_group(pid: u32) {
    agent_sandbox::terminate_process_group(pid);
}
#[cfg(not(unix))]
fn kill_group(_pid: u32) {}

pub struct BackgroundTool {
    daemon: Arc<OnceLock<Weak<DaemonState>>>,
}

pub struct ResourceTool {
    name: &'static str,
    daemon: Arc<OnceLock<Weak<DaemonState>>>,
}
impl ResourceTool {
    pub fn new(name: &'static str, daemon: Arc<OnceLock<Weak<DaemonState>>>) -> Self {
        Self { name, daemon }
    }
}
#[async_trait]
impl Tool for ResourceTool {
    fn name(&self) -> &str {
        self.name
    }
    fn description(&self) -> &str {
        "查询、等待或停止本会话的受管资源；等待超时不终止资源"
    }
    fn parameters(&self) -> Value {
        if self.name == "resource_list" {
            return json!({"type":"object","properties":{},"additionalProperties":false});
        }
        json!({"type":"object","properties":{"resource_id":{"type":"integer","minimum":1},"after_cursor":{"type":"integer","minimum":0},"timeout_ms":{"type":"integer","minimum":0,"maximum":60000}},"required":["resource_id"],"additionalProperties":false})
    }
    fn is_read_only(&self) -> bool {
        self.name != "resource_stop"
    }
    async fn preflight(&self, args: &Value) -> Result<()> {
        let daemon = self
            .daemon
            .get()
            .and_then(Weak::upgrade)
            .context("资源 daemon 尚未就绪")?;
        let (_, owner) =
            agent_runtime::loop_engine::current_session_repository().context("要求 exact owner")?;
        if self.name != "resource_list" {
            let id = ResourceId(
                args.get("resource_id")
                    .and_then(Value::as_u64)
                    .context("缺少 resource_id")?,
            );
            let resource = daemon.run_store.read_resource(id)?;
            if resource.owner.session_key != owner.session_key
                || resource.owner.session_lifetime_id != owner.session_lifetime_id
            {
                bail!("资源不属于当前 session lifetime");
            }
        }
        if self.name == "resource_stop" {
            daemon
                .safety
                .as_ref()
                .context("缺少 safety policy")?
                .authorize_external_action("停止 exact 后台资源", args)
                .await?;
        }
        Ok(())
    }
    async fn execute(&self, args: Value) -> Result<String> {
        self.preflight(&args).await?;
        let daemon = self
            .daemon
            .get()
            .and_then(Weak::upgrade)
            .context("资源 daemon 尚未就绪")?;
        let (_, owner) =
            agent_runtime::loop_engine::current_session_repository().context("要求 exact owner")?;
        if self.name == "resource_list" {
            return Ok(serde_json::to_string(&daemon.run_store.list_resources(
                &owner.session_lifetime_id,
                ResourceId(0),
                100,
            )?)?);
        }
        let id = ResourceId(
            args.get("resource_id")
                .and_then(Value::as_u64)
                .context("缺少 resource_id")?,
        );
        let resource = daemon.run_store.read_resource(id)?;
        let result = match self.name {
            "resource_status" => serde_json::to_value(resource)?,
            "resource_logs" => serde_json::to_value(
                daemon.run_store.resource_logs(
                    id,
                    args.get("after_cursor")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    20,
                )?,
            )?,
            "resource_wait" => serde_json::to_value(
                daemon
                    .wait_resource(
                        id,
                        Duration::from_millis(
                            args.get("timeout_ms")
                                .and_then(Value::as_u64)
                                .unwrap_or(1000)
                                .min(60000),
                        ),
                    )
                    .await?,
            )?,
            "resource_stop" => {
                serde_json::to_value(daemon.stop_resource(&resource.owner, id).await?)?
            }
            _ => bail!("未知资源工具"),
        };
        Ok(result.to_string())
    }
}
impl BackgroundTool {
    pub fn new(daemon: Arc<OnceLock<Weak<DaemonState>>>) -> Self {
        Self { daemon }
    }
}
#[async_trait]
impl Tool for BackgroundTool {
    fn name(&self) -> &str {
        "background_exec"
    }
    fn description(&self) -> &str {
        "启动受 daemon 监督的 Native 后台进程；返回持久资源 ID。Native 是软边界。"
    }
    fn parameters(&self) -> Value {
        json!({"type":"object","properties":{"command":{"type":"string"}},"required":["command"],"additionalProperties":false})
    }
    fn descriptor(&self, _: &Value) -> Result<agent_core::ToolDescriptor> {
        let mut d = agent_core::ToolDescriptor::external();
        d.background = true;
        Ok(d)
    }
    async fn preflight(&self, args: &Value) -> Result<()> {
        let daemon = self
            .daemon
            .get()
            .and_then(Weak::upgrade)
            .context("后台 daemon 尚未就绪")?;
        let (_, owner) =
            agent_runtime::loop_engine::current_session_repository().context("要求 exact owner")?;
        let snapshot = daemon
            .run_store
            .run_snapshot(&owner.run_id)?
            .context("缺少 RunSnapshot")?;
        if snapshot.sandbox_effective != "native" {
            bail!("此后台执行接口只支持明确的 Native backend");
        }
        daemon
            .safety
            .as_ref()
            .context("缺少 safety policy")?
            .authorize_command(
                args.get("command")
                    .and_then(Value::as_str)
                    .context("缺少 command")?,
            )
            .await
    }
    async fn execute(&self, args: Value) -> Result<String> {
        self.preflight(&args).await?;
        let daemon = self
            .daemon
            .get()
            .and_then(Weak::upgrade)
            .context("后台 daemon 尚未就绪")?;
        let (_, owner) =
            agent_runtime::loop_engine::current_session_repository().context("要求 exact owner")?;
        Ok(serde_json::to_string(
            &daemon
                .start_background(
                    owner,
                    args.get("command")
                        .and_then(Value::as_str)
                        .context("缺少 command")?
                        .into(),
                )
                .await?,
        )?)
    }
}
