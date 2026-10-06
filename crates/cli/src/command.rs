//! binary 命令解析与终端展示；通过 CommandHost 接入进程组合。
use crate::cli::{print_session_list, print_sessions, run_chat, run_repl};
use crate::tui::run_tui;
use agent_core::SessionInfo;
use agent_daemon_client::DaemonClient;
use agent_entry_support::rpc::request_result;
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde_json::{Value, json};
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

pub struct SavedModel {
    pub id: String,
    pub name: String,
    pub api_type: String,
    pub model: String,
    pub has_api_key: bool,
}
#[async_trait::async_trait]
pub trait CommandHost: Send + Sync {
    async fn connect_workspace(&self, workspace: &Path) -> Result<DaemonClient>;
    fn validate_environment(&self) -> Result<()>;
    async fn run_editor(&self, workspace: &Path, v2: bool) -> Result<()>;
    async fn run_server(&self, workspace: &Path, bind: SocketAddr) -> Result<()>;
    async fn run_daemon(&self, workspace: &Path) -> Result<()>;
    async fn status_message(&self, workspace: &Path) -> Result<String>;
    async fn stop_message(&self, workspace: &Path) -> Result<String>;
    async fn offline_sessions(&self, workspace: &Path) -> Result<Vec<SessionInfo>>;
    fn config_path(&self) -> PathBuf;
    fn config_issues(&self) -> Vec<(String, String)>;
    fn saved_models(&self) -> Result<(Option<String>, Vec<SavedModel>)>;
    fn log_path(&self, workspace: &Path) -> Result<PathBuf>;
    fn evaluate(&self) -> Result<(Value, bool)>;
}
fn write_message(message: &str) -> Result<()> {
    write!(std::io::stdout().lock(), "{message}")?;
    Ok(())
}

#[derive(Parser)]
#[command(
    name = "my-agent",
    version,
    about = "个人使用的轻量 Rust AI 编码 Agent"
)]
struct Cli {
    #[arg(long, global = true, default_value = ".", help = "Agent 工作区")]
    workspace: PathBuf,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "进入交互对话；也可直接附带一次性问题")]
    Chat {
        #[arg(trailing_var_arg = true)]
        prompt: Vec<String>,
    },
    #[command(about = "启动终端 TUI 对话界面")]
    Tui,
    #[command(about = "启动本地 OpenAI 兼容 HTTP API")]
    Serve {
        #[arg(long, default_value = "127.0.0.1:8787", help = "HTTP 监听地址")]
        bind: SocketAddr,
    },
    #[command(about = "启动编辑器 stdio JSON-RPC 适配器")]
    Editor {
        #[arg(long, help = "显式启用 ACP v2 连接协商")]
        acp_v2: bool,
    },
    #[command(about = "在前台运行内部 daemon", hide = true)]
    Daemon,
    #[command(about = "查看当前工作区 daemon 状态")]
    Status,
    #[command(about = "读取 daemon 的存储、恢复与凭据来源诊断")]
    Doctor,
    #[command(subcommand, about = "记忆数据飞轮：报告、用户反馈与离线质量评测")]
    Flywheel(FlywheelCommand),
    #[command(about = "优雅停止当前工作区 daemon")]
    Stop,
    #[command(about = "列出当前工作区会话")]
    Sessions {
        #[arg(long, help = "通过离线 maintenance 读取旧会话清单")]
        offline: bool,
    },
    #[command(about = "查看当前工作区 daemon 日志")]
    Logs {
        #[arg(long, default_value_t = 100, help = "显示最近多少行")]
        lines: usize,
        #[arg(long, help = "只显示指定 session_id 的日志")]
        session: Option<String>,
        #[arg(long, help = "只显示指定 request id 的日志，例如 2 或 task-1")]
        request: Option<String>,
    },
    #[command(subcommand, about = "配置诊断")]
    Config(ConfigCommand),
}

#[derive(Subcommand)]
enum ConfigCommand {
    #[command(about = "检查必需与可选环境变量")]
    Check,
    #[command(about = "显示全局模型配置文件路径")]
    Path,
    #[command(about = "列出已保存模型配置（不会显示 API key）")]
    List,
}

#[derive(Subcommand)]
enum FlywheelCommand {
    #[command(about = "读取当前会话的曝光、评价和摄入诊断")]
    Report {
        #[arg(long)]
        session: String,
    },
    #[command(about = "评价报告中已曝光的记忆，运行结束后提交")]
    Feedback {
        #[arg(long)]
        session: String,
        #[arg(long)]
        run: String,
        #[arg(long)]
        memory: String,
        #[arg(long)]
        operation: String,
        #[arg(long, value_enum)]
        vote: FeedbackVote,
    },
    #[command(about = "离线执行固定质量评测，不调用模型；失败退出非零")]
    Evaluate,
}
#[derive(Clone, clap::ValueEnum)]
enum FeedbackVote {
    Helpful,
    Irrelevant,
    Incorrect,
    Outdated,
}

pub async fn run(host: &dyn CommandHost) -> Result<()> {
    let cli = Cli::parse();
    let workspace = canonical_workspace(&cli.workspace)?;
    let command = cli.command.unwrap_or(Command::Tui);
    match command {
        Command::Chat { prompt } => run_chat_command(host, &workspace, prompt).await,
        Command::Tui => run_tui_command(host, &workspace).await,
        Command::Serve { bind } => host.run_server(&workspace, bind).await,
        Command::Editor { acp_v2 } => host.run_editor(&workspace, acp_v2).await,
        Command::Daemon => host.run_daemon(&workspace).await,
        Command::Status => write_message(&host.status_message(&workspace).await?),
        Command::Doctor => {
            let client = host.connect_workspace(&workspace).await?;
            writeln!(
                std::io::stdout().lock(),
                "{}",
                serde_json::to_string_pretty(
                    &request_result(&client, "runtime.doctor", json!({})).await?
                )?
            )?;
            Ok(())
        }
        Command::Flywheel(command) => {
            let result = match command {
                FlywheelCommand::Evaluate => {
                    let (output, passed) = host.evaluate()?;
                    writeln!(
                        std::io::stdout().lock(),
                        "{}",
                        serde_json::to_string_pretty(&output)?
                    )?;
                    anyhow::ensure!(passed, "记忆/上下文质量门禁未通过");
                    return Ok(());
                }
                FlywheelCommand::Report { session } => {
                    let client = host.connect_workspace(&workspace).await?;
                    request_result(&client, "memory.flywheel", json!({"session_id":session}))
                        .await?
                }
                FlywheelCommand::Feedback {
                    session,
                    run,
                    memory,
                    operation,
                    vote,
                } => {
                    let feedback = match vote {
                        FeedbackVote::Helpful => agent_core::MemoryFeedback::Helpful,
                        FeedbackVote::Irrelevant => agent_core::MemoryFeedback::Irrelevant,
                        FeedbackVote::Incorrect => agent_core::MemoryFeedback::Incorrect,
                        FeedbackVote::Outdated => agent_core::MemoryFeedback::Outdated,
                    };
                    let client = host.connect_workspace(&workspace).await?;
                    request_result(&client,"memory.feedback",json!({"session_id":session,"owner_run_id":run,"memory_id":memory,"operation_id":operation,"feedback":feedback})).await?
                }
            };
            writeln!(
                std::io::stdout().lock(),
                "{}",
                serde_json::to_string_pretty(&result)?
            )?;
            Ok(())
        }
        Command::Stop => write_message(&host.stop_message(&workspace).await?),
        Command::Sessions { offline } => run_sessions_command(host, &workspace, offline).await,
        Command::Logs {
            lines,
            session,
            request,
        } => {
            run_logs_command(
                host,
                &workspace,
                lines,
                session.as_deref(),
                request.as_deref(),
            )
            .await
        }
        Command::Config(ConfigCommand::Check) => run_config_check(host),
        Command::Config(ConfigCommand::Path) => {
            writeln!(std::io::stdout().lock(), "{}", host.config_path().display())?;
            Ok(())
        }
        Command::Config(ConfigCommand::List) => run_config_list(host),
    }
}

async fn run_chat_command(
    host: &dyn CommandHost,
    workspace: &Path,
    prompt: Vec<String>,
) -> Result<()> {
    host.validate_environment()?;
    let client = host.connect_workspace(workspace).await?;
    let snapshot = agent_entry_support::recovery::start_new_session(&client).await?;
    let mut session_id = snapshot.session_id;
    if prompt.is_empty() {
        run_repl(&client, &mut session_id).await
    } else {
        run_chat(&client, &prompt.join(" "), &session_id).await
    }
}

async fn run_tui_command(host: &dyn CommandHost, workspace: &Path) -> Result<()> {
    host.validate_environment()?;
    let client = host.connect_workspace(workspace).await?;
    run_tui(client, workspace).await
}

async fn run_sessions_command(
    host: &dyn CommandHost,
    workspace: &Path,
    offline: bool,
) -> Result<()> {
    if offline {
        let sessions = host.offline_sessions(workspace).await?;
        writeln!(
            std::io::stdout().lock(),
            "离线 maintenance：以下为旧格式会话快照："
        )?;
        print_session_list(&sessions)?;
        return Ok(());
    }
    let client = host.connect_workspace(workspace).await?;
    print_sessions(&client).await
}

async fn run_logs_command(
    host: &dyn CommandHost,
    workspace: &Path,
    lines: usize,
    session: Option<&str>,
    request: Option<&str>,
) -> Result<()> {
    if lines == 0 {
        anyhow::bail!("--lines 必须大于 0");
    }
    let log = host.log_path(workspace)?;
    let content = match tokio::fs::read_to_string(&log).await {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            writeln!(
                std::io::stdout().lock(),
                "暂无 daemon 日志：{}",
                log.display()
            )?;
            return Ok(());
        }
        Err(error) => {
            return Err(error).with_context(|| format!("读取 daemon 日志失败: {}", log.display()));
        }
    };
    let recent = content
        .lines()
        .filter(|line| {
            session.is_none_or(|session| line.contains(&format!("session_id={session}")))
                && request.is_none_or(|request| log_line_matches_request(line, request))
        })
        .rev()
        .take(lines)
        .collect::<Vec<_>>();
    for line in recent.into_iter().rev() {
        writeln!(std::io::stdout().lock(), "{line}")?;
    }
    Ok(())
}

fn log_line_matches_request(line: &str, request: &str) -> bool {
    if request.starts_with("Number(") || request.starts_with("String(") {
        return line.contains(&format!("request_id={request}"));
    }
    line.contains(&format!("request_id=Number({request})"))
        || line.contains(&format!("request_id=String(\"{request}\")"))
        || line.contains(&format!("request_id={request}"))
}

fn run_config_check(host: &dyn CommandHost) -> Result<()> {
    let issues = host.config_issues();
    if issues.is_empty() {
        writeln!(
            std::io::stdout().lock(),
            "配置检查通过。API 密钥已设置（值不会显示）。\n全局配置文件：{}",
            host.config_path().display()
        )?;
        return Ok(());
    }
    writeln!(
        std::io::stdout().lock(),
        "配置检查发现 {} 个问题：",
        issues.len()
    )?;
    for issue in &issues {
        writeln!(std::io::stdout().lock(), "- {}：{}", issue.0, issue.1)?;
    }
    anyhow::bail!(
        "配置尚未就绪。可运行 `myagent serve` 打开 Web 设置保存模型，或修正以上环境变量；配置文件：{}",
        host.config_path().display()
    )
}

fn run_config_list(host: &dyn CommandHost) -> Result<()> {
    let (active, profiles) = host.saved_models()?;
    let config_path = host.config_path();
    if profiles.is_empty() {
        writeln!(
            std::io::stdout().lock(),
            "暂无已保存模型配置。\n配置文件：{}",
            config_path.display()
        )?;
        return Ok(());
    }
    writeln!(
        std::io::stdout().lock(),
        "全局配置文件：{}",
        config_path.display()
    )?;
    for profile in profiles {
        let marker = if active.as_deref() == Some(profile.id.as_str()) {
            "*"
        } else {
            " "
        };
        writeln!(
            std::io::stdout().lock(),
            "{marker} {} · {} · {} · key={}",
            profile.name,
            profile.api_type,
            profile.model,
            profile.has_api_key
        )?;
    }
    Ok(())
}

fn canonical_workspace(path: &Path) -> Result<PathBuf> {
    std::fs::canonicalize(path).with_context(|| format!("无法访问工作区：{}", path.display()))
}

#[cfg(test)]
mod main_tests {
    use super::log_line_matches_request;

    #[test]
    fn filters_numeric_and_string_request_ids_without_partial_matches() {
        let numeric = "agent_turn{request_id=Number(2)}: 开始 ReAct 轮次";
        let string = "agent_turn{request_id=String(\"task-2\")}: 开始 ReAct 轮次";

        assert!(log_line_matches_request(numeric, "2"));
        assert!(log_line_matches_request(numeric, "Number(2)"));
        assert!(log_line_matches_request(string, "task-2"));
        assert!(!log_line_matches_request(numeric, "1"));
    }
}
