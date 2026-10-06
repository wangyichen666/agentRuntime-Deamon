#![forbid(unsafe_code)]
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;

#[async_trait]
pub trait ToolCancellation: Send + Sync {
    fn is_cancelled(&self) -> bool;
    async fn cancelled(&self);
}

const MAX_OUTPUT_BYTES: usize = 64 * 1024;

use agent_core::ResourceId;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxBackend {
    Native,
    Docker,
}

pub struct ExecRequest {
    pub command: String,
    pub shell: PathBuf,
    pub cwd: PathBuf,
    pub timeout: Duration,
    pub requested: SandboxBackend,
    pub owner: Option<agent_core::ExactOwner>,
    pub docker_image: Option<String>,
}

pub struct ExecResult {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub requested: SandboxBackend,
    pub effective: SandboxBackend,
}

#[derive(Default)]
pub struct ResourceManager {
    next_id: AtomicU64,
    processes: Mutex<HashMap<ResourceId, u32>>,
}

impl ResourceManager {
    pub(crate) fn register_process(self: &Arc<Self>, pid: u32) -> ResourceRegistration {
        let id = ResourceId(self.next_id.fetch_add(1, Ordering::Relaxed) + 1);
        self.processes
            .lock()
            .unwrap_or_else(|poison| {
                tracing::error!("资源锁损坏，恢复已登记进程用于清理");
                poison.into_inner()
            })
            .insert(id, pid);
        ResourceRegistration {
            manager: self.clone(),
            id,
            pid: Some(pid),
        }
    }

    #[allow(dead_code)]
    pub fn list(&self) -> Vec<ResourceId> {
        self.processes
            .lock()
            .unwrap_or_else(|poison| {
                tracing::error!("资源锁损坏，恢复已登记进程用于清理");
                poison.into_inner()
            })
            .keys()
            .copied()
            .collect()
    }

    #[allow(dead_code)]
    pub fn stop(&self, id: ResourceId) -> bool {
        let pid = self
            .processes
            .lock()
            .unwrap_or_else(|poison| {
                tracing::error!("资源锁损坏，恢复已登记进程用于清理");
                poison.into_inner()
            })
            .get(&id)
            .copied();
        if let Some(pid) = pid {
            terminate_process_group(pid);
            true
        } else {
            false
        }
    }

    pub fn stop_all(&self) {
        let pids = self
            .processes
            .lock()
            .unwrap_or_else(|poison| {
                tracing::error!("资源锁损坏，恢复已登记进程用于清理");
                poison.into_inner()
            })
            .values()
            .copied()
            .collect::<Vec<_>>();
        for pid in pids {
            terminate_process_group(pid);
        }
    }
}

pub(crate) struct ResourceRegistration {
    manager: Arc<ResourceManager>,
    id: ResourceId,
    pid: Option<u32>,
}

impl ResourceRegistration {
    #[allow(dead_code)]
    pub fn id(&self) -> ResourceId {
        self.id
    }

    fn disarm(&mut self) {
        self.pid = None;
    }
}

impl Drop for ResourceRegistration {
    fn drop(&mut self) {
        if let Some(pid) = self.pid {
            terminate_process_group(pid);
        }
        self.manager
            .processes
            .lock()
            .unwrap_or_else(|poison| {
                tracing::error!("资源锁损坏，恢复已登记进程用于清理");
                poison.into_inner()
            })
            .remove(&self.id);
    }
}

#[async_trait]
pub trait Sandbox: Send + Sync {
    async fn execute(
        &self,
        request: ExecRequest,
        cancellation: &dyn ToolCancellation,
    ) -> Result<ExecResult>;
    fn stop_all(&self);
}

pub struct NativeSandbox {
    resources: Arc<ResourceManager>,
}

impl Default for NativeSandbox {
    fn default() -> Self {
        Self::new()
    }
}

impl NativeSandbox {
    pub fn new() -> Self {
        Self {
            resources: Arc::new(ResourceManager::default()),
        }
    }
    pub async fn docker_available() -> bool {
        let image = std::env::var("MY_AGENT_DOCKER_IMAGE").unwrap_or_else(|_| "alpine:3.21".into());
        match tokio::time::timeout(
            Duration::from_secs(3),
            Command::new("docker")
                .args(["image", "inspect", &image])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true)
                .status(),
        )
        .await
        {
            Ok(Ok(status)) => status.success(),
            _ => false,
        }
    }
    async fn execute_docker(
        &self,
        request: ExecRequest,
        cancellation: &dyn ToolCancellation,
    ) -> Result<ExecResult> {
        let owner = request
            .owner
            .as_ref()
            .context("Docker 执行要求 exact owner")?;
        if !Self::docker_available().await {
            bail!("Docker backend 或预装镜像不可用；拒绝强隔离请求");
        }
        let cwd = std::fs::canonicalize(&request.cwd)?;
        if cwd.to_string_lossy().contains(',') {
            bail!("Docker mount 路径包含不支持的分隔符");
        }
        let name = format!(
            "myagent-{}-{}-{}",
            owner.session_lifetime_id.0,
            std::process::id(),
            self.resources.next_id.fetch_add(1, Ordering::Relaxed)
        );
        let image = request
            .docker_image
            .clone()
            .unwrap_or_else(|| "alpine:3.21".into());
        let args = vec![
            "docker".to_owned(),
            "run".into(),
            "--pull=never".into(),
            "--rm".into(),
            "--name".into(),
            name.clone(),
            "--network=none".into(),
            "--read-only".into(),
            "--user=65534:65534".into(),
            "--pids-limit=64".into(),
            "--memory=512m".into(),
            "--cpus=1".into(),
            "--cap-drop=ALL".into(),
            "--security-opt=no-new-privileges".into(),
            "--tmpfs=/tmp:rw,nosuid,nodev,size=67108864".into(),
            "--mount".into(),
            format!("type=bind,src={},dst=/workspace,readonly", cwd.display()),
            "--tmpfs=/workspace/.my-agent:ro,nosuid,nodev,size=1048576".into(),
            "--workdir=/workspace".into(),
            image,
            "/bin/sh".into(),
            "-c".into(),
            request.command,
        ];
        let mut guard = DockerLease(Some(name));
        let result = Box::pin(
            self.execute(
                ExecRequest {
                    command: args
                        .iter()
                        .map(|s| shell_quote(s))
                        .collect::<Vec<_>>()
                        .join(" "),
                    shell: PathBuf::from("/bin/sh"),
                    cwd,
                    timeout: request.timeout,
                    requested: SandboxBackend::Native,
                    owner: request.owner,
                    docker_image: request.docker_image,
                },
                cancellation,
            ),
        )
        .await;
        guard.cleanup().await;
        result.map(|mut r| {
            r.requested = SandboxBackend::Docker;
            r.effective = SandboxBackend::Docker;
            r
        })
    }
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}
struct DockerLease(Option<String>);
impl DockerLease {
    async fn cleanup(&mut self) {
        if let Some(name) = self.0.take() {
            remove_container(name).await;
        }
    }
}
async fn remove_container(name: String) {
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        Command::new("docker")
            .args(["rm", "--force", &name])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .status(),
    )
    .await;
}
impl Drop for DockerLease {
    fn drop(&mut self) {
        if let Some(name) = self.0.take() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(remove_container(name));
            }
        }
    }
}

#[async_trait]
impl Sandbox for NativeSandbox {
    async fn execute(
        &self,
        request: ExecRequest,
        cancellation: &dyn ToolCancellation,
    ) -> Result<ExecResult> {
        if !matches!(request.requested, SandboxBackend::Native) {
            return self.execute_docker(request, cancellation).await;
        }
        if cancellation.is_cancelled() {
            bail!("命令执行已取消");
        }
        let mut command = Command::new(&request.shell);
        command
            .arg("-c")
            .arg(&request.command)
            .current_dir(&request.cwd)
            .env_clear()
            .env("PWD", &request.cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for key in [
            "PATH",
            "HOME",
            "TMPDIR",
            "LANG",
            "LC_ALL",
            "TERM",
            "CARGO_HOME",
            "RUSTUP_HOME",
        ] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command
            .spawn()
            .with_context(|| format!("执行命令失败: {}", request.command))?;
        let mut guard = child.id().map(|pid| self.resources.register_process(pid));
        let stdout = child.stdout.take().context("stdout 管道不存在")?;
        let stderr = child.stderr.take().context("stderr 管道不存在")?;
        let out_task = tokio::spawn(capture_bounded(stdout));
        let err_task = tokio::spawn(capture_bounded(stderr));
        let status = tokio::select! {
            result = child.wait() => result.context("等待命令结束失败")?,
            _ = cancellation.cancelled() => {
                if let Some(pid) = child.id() { terminate_process_group(pid); }
                let _ = child.wait().await;
                if let Some(guard) = guard.as_mut() { guard.disarm(); }
                let _ = out_task.await;
                let _ = err_task.await;
                bail!("命令执行已取消: {}", request.command);
            },
            _ = tokio::time::sleep(request.timeout) => {
                if let Some(pid) = child.id() { terminate_process_group(pid); }
                let _ = child.wait().await;
                if let Some(guard) = guard.as_mut() { guard.disarm(); }
                let _ = out_task.await;
                let _ = err_task.await;
                bail!("命令执行超时（{} 秒）: {}", request.timeout.as_secs(), request.command);
            }
        };
        if let Some(guard) = guard.as_mut() {
            guard.disarm();
        }
        let stdout = out_task.await.context("读取 stdout 任务失败")??;
        let stderr = err_task.await.context("读取 stderr 任务失败")??;
        Ok(ExecResult {
            exit_code: status.code().unwrap_or(-1),
            stdout,
            stderr,
            requested: request.requested,
            effective: SandboxBackend::Native,
        })
    }

    fn stop_all(&self) {
        self.resources.stop_all();
    }
}

async fn capture_bounded(mut reader: impl AsyncRead + Unpin) -> Result<String> {
    let mut kept = Vec::with_capacity(MAX_OUTPUT_BYTES);
    let mut total = 0u64;
    let mut chunk = [0u8; 8192];
    loop {
        let count = reader.read(&mut chunk).await?;
        if count == 0 {
            break;
        }
        total += count as u64;
        let remaining = MAX_OUTPUT_BYTES.saturating_sub(kept.len());
        kept.extend_from_slice(&chunk[..count.min(remaining)]);
    }
    let mut output = String::from_utf8_lossy(&kept).into_owned();
    if total > MAX_OUTPUT_BYTES as u64 {
        output.push_str(&format!("\n...[输出已截断，原始大小 {total} 字节]"));
    }
    Ok(output)
}

#[cfg(unix)]
pub fn terminate_process_group(pid: u32) {
    if let Ok(group) = i32::try_from(pid)
        && let Some(group) = rustix::process::Pid::from_raw(group)
    {
        let _ = rustix::process::kill_process_group(group, rustix::process::Signal::KILL);
    }
}

#[cfg(not(unix))]
pub fn terminate_process_group(_pid: u32) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_registration_can_be_queried_and_removed() {
        let manager = Arc::new(ResourceManager::default());
        let mut registration = manager.register_process(u32::MAX);
        assert_eq!(manager.list(), vec![registration.id]);
        assert!(manager.stop(registration.id));
        registration.disarm();
        drop(registration);
        assert!(manager.list().is_empty());
    }
}
