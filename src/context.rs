use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use tracing::warn;

use crate::plan::PlanStore;
#[cfg(test)]
use crate::provider::ToolSpec;
use crate::provider::{Message, Provider, Response, Role};
use crate::skills::SkillLibrary;
use crate::tool_calls::collect_complete_provider_response;

const DEFAULT_SYSTEM_PROMPT: &str = "你是一个个人 AI 编码 Agent。先理解任务，再按需调用工具；工具失败时根据错误调整方案，先修正根因再重试，不要在同一失败上无变化地循环；任务完成后给出简洁、可核验的最终回答。面对“给我写一个前端”这类未指定技术栈的请求，优先沿用当前项目已有技术栈；没有现有栈时默认创建可直接打开的原生 HTML/CSS/JavaScript 页面，并明确说明这个假设。开始写入嵌套路径前确保父目录存在，优先使用工具提供的目录创建能力；如果工具报告路径不存在，立即创建目录并重试一次。面对需要三个或更多步骤的复杂任务，先调用 plan 的 set 制定计划，开始和完成每一步时用 update 更新状态，必要时用 add 调整；简单单步任务不要使用 plan，避免形式主义。若动态上下文提供了命中的技能正文，应把它作为当前任务的工作方法；未命中的技能只有索引，不要假装已读取其正文。";
const DEFAULT_TOKEN_BUDGET: usize = 32_000;
const DEFAULT_RECENT_MESSAGES: usize = 12;
const MAX_PROJECT_RULE_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug)]
pub struct ContextConfig {
    pub token_budget: usize,
    pub recent_messages: usize,
    pub mild_compression_percent: usize,
    pub strong_compression_percent: usize,
    pub summary_chunk_tokens: usize,
}

impl ContextConfig {
    pub fn from_env() -> Result<Self> {
        let token_budget = optional_usize("CONTEXT_TOKEN_BUDGET", DEFAULT_TOKEN_BUDGET)?;
        let recent_messages = optional_usize("CONTEXT_RECENT_MESSAGES", DEFAULT_RECENT_MESSAGES)?;
        let mild_compression_percent = optional_usize("CONTEXT_MILD_PERCENT", 60)?;
        let strong_compression_percent = optional_usize("CONTEXT_STRONG_PERCENT", 85)?;
        if token_budget < 256 {
            bail!("CONTEXT_TOKEN_BUDGET 不能小于 256");
        }
        if recent_messages == 0 {
            bail!("CONTEXT_RECENT_MESSAGES 不能为 0");
        }
        if mild_compression_percent == 0
            || mild_compression_percent >= strong_compression_percent
            || strong_compression_percent > 100
        {
            bail!("压缩阈值必须满足 0 < CONTEXT_MILD_PERCENT < CONTEXT_STRONG_PERCENT <= 100");
        }
        Ok(Self {
            token_budget,
            recent_messages,
            mild_compression_percent,
            strong_compression_percent,
            summary_chunk_tokens: (token_budget / 3).max(128),
        })
    }
}

#[derive(Clone)]
pub struct ContextManager {
    provider: Arc<dyn Provider>,
    workspace: PathBuf,
    system_prompt: String,
    project_rules: Option<String>,
    config: ContextConfig,
    plan: Arc<PlanStore>,
    skills: SkillLibrary,
}

impl ContextManager {
    pub fn token_budget(&self) -> usize {
        self.config.token_budget
    }

    pub fn with_provider(&self, provider: Arc<dyn Provider>) -> Self {
        let mut copy = self.clone();
        copy.provider = provider;
        copy
    }

    pub fn with_token_budget(&self, token_budget: usize) -> Self {
        let mut copy = self.clone();
        copy.config.token_budget = token_budget;
        copy
    }

    #[cfg(test)]
    pub fn new(
        provider: Arc<dyn Provider>,
        workspace: impl AsRef<Path>,
        config: ContextConfig,
        plan: Arc<PlanStore>,
    ) -> Result<Self> {
        Self::with_system_prompt(provider, workspace, config, plan, DEFAULT_SYSTEM_PROMPT)
    }

    pub fn new_with_skills(
        provider: Arc<dyn Provider>,
        workspace: impl AsRef<Path>,
        config: ContextConfig,
        plan: Arc<PlanStore>,
        skills: SkillLibrary,
    ) -> Result<Self> {
        Self::build(
            provider,
            workspace,
            config,
            plan,
            DEFAULT_SYSTEM_PROMPT,
            Some(skills),
        )
    }

    pub fn with_system_prompt(
        provider: Arc<dyn Provider>,
        workspace: impl AsRef<Path>,
        config: ContextConfig,
        plan: Arc<PlanStore>,
        system_prompt: impl Into<String>,
    ) -> Result<Self> {
        Self::build(provider, workspace, config, plan, system_prompt, None)
    }

    fn build(
        provider: Arc<dyn Provider>,
        workspace: impl AsRef<Path>,
        config: ContextConfig,
        plan: Arc<PlanStore>,
        system_prompt: impl Into<String>,
        skills: Option<SkillLibrary>,
    ) -> Result<Self> {
        let workspace = std::fs::canonicalize(workspace.as_ref())
            .with_context(|| format!("无法解析上下文工作区: {}", workspace.as_ref().display()))?;
        let project_rules = load_project_rules(&workspace)?;
        let skills = skills.unwrap_or_else(|| SkillLibrary::from_env(&workspace));
        Ok(Self {
            provider,
            workspace,
            system_prompt: system_prompt.into(),
            project_rules,
            config,
            plan,
            skills,
        })
    }

    pub fn with_frozen_policy(&self, fingerprint: &str) -> Result<Self> {
        let values = fingerprint
            .split(':')
            .map(str::parse::<usize>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if values.len() != 4
            || values[0] < 256
            || values[1] == 0
            || values[2] == 0
            || values[2] >= values[3]
            || values[3] > 100
        {
            bail!("冻结的 context policy 无效");
        }
        let mut next = self.clone();
        next.config = ContextConfig {
            token_budget: values[0],
            recent_messages: values[1],
            mild_compression_percent: values[2],
            strong_compression_percent: values[3],
            summary_chunk_tokens: (values[0] / 3).max(128),
        };
        Ok(next)
    }
    pub fn fingerprint(&self) -> String {
        format!(
            "{}:{}:{}:{}",
            self.config.token_budget,
            self.config.recent_messages,
            self.config.mild_compression_percent,
            self.config.strong_compression_percent
        )
    }
    pub fn should_compact(&self, tokens: usize) -> bool {
        tokens
            > self
                .config
                .token_budget
                .saturating_mul(self.config.mild_compression_percent)
                / 100
    }
    pub async fn compact_candidate<F: std::future::Future<Output = ()>>(
        &self,
        history: &[Message],
        cancelled: F,
        force: bool,
    ) -> Result<Option<(Vec<Message>, String)>> {
        let old = history.to_vec();
        let mut projected = old.clone();
        let mut manager = self.clone();
        if force {
            manager.config.recent_messages = 2;
        }
        let prune_before = projected
            .len()
            .saturating_sub(manager.config.recent_messages)
            .min(agent_context::current_turn_start(&projected));
        for message in &mut projected[..prune_before] {
            message.image_urls.clear();
            if message.role == Role::Tool
                && message
                    .content
                    .as_ref()
                    .is_some_and(|s| s.chars().count() > 4096)
            {
                message.content = Some(format!(
                    "{}\n[投影已裁剪；原始结果保留在 transcript 与 artifact 中]",
                    message
                        .content
                        .as_deref()
                        .unwrap_or_default()
                        .chars()
                        .take(2048)
                        .collect::<String>()
                ));
            }
        }
        let pruned = projected.clone();
        tokio::pin!(cancelled);
        let result = tokio::select! {
            biased;
            _=&mut cancelled=>Err(agent_context::ContextError::Cancelled.into()),
            _=tokio::time::sleep(std::time::Duration::from_secs(30))=>Err(anyhow::anyhow!("上下文摘要超时")),
            result=manager.compress_history(&mut projected,CompressionMode::Strong)=>result,
        };
        match result {
            Ok(()) if agent_context::validate_candidate(&old, &projected).is_ok() => {
                Ok(Some((projected, "summary".into())))
            }
            Ok(()) => Ok(None),
            Err(ref error)
                if error
                    .downcast_ref::<agent_context::ContextError>()
                    .is_none_or(|cause| {
                        !matches!(cause, agent_context::ContextError::Cancelled)
                    })
                    && agent_context::validate_candidate(&old, &pruned).is_ok() =>
            {
                Ok(Some((pruned, "prune_only/partial".into())))
            }
            Err(error) => Err(error),
        }
    }
    #[cfg(test)]
    pub async fn prepare(
        &self,
        history: &mut Vec<Message>,
        tools: &[ToolSpec],
    ) -> Result<Vec<Message>> {
        let before = self.compose(history).await;
        let total = agent_context::TokenEstimator.request(&before, tools)?;
        let mode = if total
            > self
                .config
                .token_budget
                .saturating_mul(self.config.strong_compression_percent)
                / 100
        {
            Some(CompressionMode::Strong)
        } else if total
            > self
                .config
                .token_budget
                .saturating_mul(self.config.mild_compression_percent)
                / 100
        {
            Some(CompressionMode::Mild)
        } else {
            None
        };
        if let Some(mode) = mode {
            self.compress_history(history, mode).await?;
        }
        Ok(self.compose(history).await)
    }

    pub(crate) async fn compose(&self, history: &[Message]) -> Vec<Message> {
        let mut messages = Vec::with_capacity(history.len() + 3);
        messages.push(Message::text(Role::System, &self.system_prompt));
        if let Some(rules) = &self.project_rules {
            messages.push(Message::text(
                Role::System,
                format!("项目规则（AGENTS.md）：\n{rules}"),
            ));
        }
        let overlay_start = history
            .iter()
            .rposition(|m| m.role == Role::User)
            .unwrap_or(history.len());
        messages.extend_from_slice(&history[..overlay_start]);
        if let Some(retrieved) = self.retrieved_context(history).await {
            messages.push(Message::text(
                Role::System,
                format!("[retrieved_context] {retrieved}"),
            ));
        }
        messages.extend_from_slice(&history[overlay_start..]);
        messages.push(Message::text(
            Role::System,
            format!("[turn_overlay] {}", self.dynamic_environment().await),
        ));
        messages
    }

    async fn dynamic_environment(&self) -> String {
        let branch = tokio::process::Command::new("git")
            .arg("branch")
            .arg("--show-current")
            .current_dir(&self.workspace)
            .output()
            .await
            .ok()
            .filter(|output: &std::process::Output| output.status.success())
            .map(|output: std::process::Output| {
                String::from_utf8_lossy(&output.stdout).trim().to_owned()
            })
            .filter(|branch: &String| !branch.is_empty())
            .unwrap_or_else(|| "（非 Git 仓库或 detached HEAD）".to_owned());
        format!(
            "动态环境：cwd={}；git_branch={branch}",
            self.workspace.display()
        )
    }
    async fn retrieved_context(&self, history: &[Message]) -> Option<String> {
        let mut environment = String::new();
        if let Some(plan) = self.plan.context_block().await {
            environment.push('\n');
            environment.push_str(&plan);
        }
        let query = history
            .iter()
            .rev()
            .find(|message: &&Message| message.role == Role::User)
            .and_then(|message: &Message| message.content.as_deref())
            .unwrap_or_default();
        if let Some(skills) = self.skills.context_for(query).await {
            environment.push('\n');
            environment.push_str(&skills);
        }
        (!environment.is_empty()).then_some(environment)
    }

    async fn compress_history(
        &self,
        history: &mut Vec<Message>,
        mode: CompressionMode,
    ) -> Result<()> {
        if history.len() <= self.config.recent_messages {
            warn!(
                messages = history.len(),
                "上下文过大，但没有可压缩的旧消息；保留当前输入"
            );
            return Ok(());
        }

        let compressible = history.len() - self.config.recent_messages;
        let mut split = match mode {
            #[cfg(test)]
            CompressionMode::Mild => compressible.div_ceil(3).max(1),
            CompressionMode::Strong => compressible,
        };
        split = split.min(agent_context::current_turn_start(history));
        while split > 0
            && history
                .get(split)
                .is_some_and(|item: &Message| item.role == Role::Tool)
        {
            split -= 1;
        }
        if split == 0 {
            return Ok(());
        }

        let old = history[..split].to_vec();
        let serialized = serde_json::to_string(&old).context("序列化待压缩历史失败")?;
        let summary = self.summarize_text(serialized, 0).await?;
        let anchor_budget = (self.config.token_budget / 8)
            .min(2048)
            .min(estimate_messages(&old) / 4);
        let anchors = agent_context::retained_user_inputs(&old, anchor_budget).unwrap_or_default();
        let recent = history.split_off(split);
        history.clear();
        history.push(Message::text(
            Role::System,
            format!("{anchors}此前对话摘要：\n{summary}"),
        ));
        history.extend(recent);
        Ok(())
    }

    /// 上游明确报告上下文溢出时，仅在本轮请求副本上压缩一次。
    pub async fn compact_for_overflow(&self, messages: &[Message]) -> Result<Option<Vec<Message>>> {
        if messages.len() <= 2 {
            return Ok(None);
        }
        let mut compacted = messages.to_vec();
        self.compress_history(&mut compacted, CompressionMode::Strong)
            .await?;
        if estimate_messages(&compacted) < estimate_messages(messages) {
            Ok(Some(compacted))
        } else {
            Ok(None)
        }
    }

    fn summarize_text<'a>(
        &'a self,
        text: String,
        depth: usize,
    ) -> futures_util::future::BoxFuture<'a, Result<String>> {
        Box::pin(async move {
            if depth >= 8 {
                return Ok(truncate_chars(
                    &text,
                    self.config.summary_chunk_tokens.saturating_mul(3),
                ));
            }
            if estimate_text_tokens(&text) <= self.config.summary_chunk_tokens {
                return self.request_summary(&text).await;
            }

            let chunks = split_for_token_budget(&text, self.config.summary_chunk_tokens);
            let mut summaries = Vec::with_capacity(chunks.len());
            for chunk in chunks {
                summaries.push(self.request_summary(&chunk).await?);
            }
            let merged = summaries.join("\n\n");
            self.summarize_text(merged, depth + 1).await
        })
    }

    async fn request_summary(&self, source: &str) -> Result<String> {
        let messages = [
            Message::text(
                Role::System,
                "把给定的早期对话压缩成简短事实摘要。按用户目标、明确约束、已完成工作、当前进度、关键决策、文件/符号、真实工具结果、未解决失败、计划与下一步组织 checkpoint；区分未知与事实，不要虚构。已有 checkpoint 应增量更新，不嵌套引用全文。只输出摘要。",
            ),
            Message::text(Role::User, source),
        ];
        match collect_complete_provider_response(self.provider.as_ref(), &messages, &[]).await? {
            Response::Text(summary) if !summary.trim().is_empty() => Ok(summary),
            Response::Text(_) => bail!("模型返回空压缩摘要"),
            Response::ToolCalls(_) => bail!("模型在上下文压缩时返回了工具调用"),
            Response::ToolAssemblyFailed(error) => bail!(
                "模型在上下文压缩时产生无效工具调用（{}）：{}",
                error.code,
                error.message
            ),
        }
    }
}

#[derive(Clone, Copy)]
enum CompressionMode {
    #[cfg(test)]
    Mild,
    Strong,
}

pub fn estimate_messages(messages: &[Message]) -> usize {
    agent_context::TokenEstimator.messages(messages)
}
pub fn estimate_text_tokens(text: &str) -> usize {
    agent_context::TokenEstimator.text(text)
}
fn split_for_token_budget(text: &str, budget: usize) -> Vec<String> {
    let char_limit = budget.saturating_mul(3).max(1);
    let mut chunks = Vec::new();
    let mut current = String::new();
    for character in text.chars() {
        current.push(character);
        if current.chars().count() >= char_limit || estimate_text_tokens(&current) >= budget {
            chunks.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    let mut output = text.chars().take(max_chars).collect::<String>();
    if text.chars().count() > max_chars {
        output.push_str("…[摘要已截断]");
    }
    output
}

fn load_project_rules(workspace: &Path) -> Result<Option<String>> {
    let path = workspace.join("AGENTS.md");
    if !path.exists() {
        return Ok(None);
    }
    let bytes =
        std::fs::read(&path).with_context(|| format!("读取项目规则失败: {}", path.display()))?;
    let limited = if bytes.len() > MAX_PROJECT_RULE_BYTES {
        &bytes[..MAX_PROJECT_RULE_BYTES]
    } else {
        &bytes
    };
    Ok(Some(String::from_utf8_lossy(limited).into_owned()))
}

fn optional_usize(name: &str, default: usize) -> Result<usize> {
    match env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .with_context(|| format!("环境变量 {name} 必须是正整数")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("读取环境变量 {name} 失败")),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::plan::{PlanStatus, PlanStep};

    struct SummaryProvider {
        responses: Mutex<VecDeque<Response>>,
        calls: Mutex<usize>,
    }

    #[async_trait]
    impl Provider for SummaryProvider {
        async fn chat(&self, _messages: &[Message], _tools: &[ToolSpec]) -> Result<Response> {
            *self.calls.lock().unwrap() += 1;
            Ok(self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Response::Text("更短摘要".to_owned())))
        }
    }

    #[test]
    fn token_estimate_treats_chinese_more_densely_than_ascii() {
        assert_eq!(estimate_text_tokens("中文中文中文"), 4);
        assert_eq!(estimate_text_tokens("abcdefghijkl"), 3);
    }

    #[tokio::test]
    async fn compresses_old_messages_and_keeps_recent_messages() {
        let provider = Arc::new(SummaryProvider {
            responses: Mutex::new(VecDeque::from([Response::Text(
                "目标和决策摘要".to_owned(),
            )])),
            calls: Mutex::new(0),
        });
        let config = ContextConfig {
            token_budget: 256,
            recent_messages: 2,
            mild_compression_percent: 1,
            strong_compression_percent: 2,
            summary_chunk_tokens: 2_000,
        };
        let manager = ContextManager::new(
            provider.clone(),
            std::env::current_dir().unwrap(),
            config,
            Arc::new(PlanStore::memory_only()),
        )
        .unwrap();
        let mut history = vec![
            Message::text(Role::User, "旧问题"),
            Message::text(Role::Assistant, "旧回答"),
            Message::text(Role::User, "新问题"),
            Message::text(Role::Assistant, "新回答"),
        ];

        let prepared = manager.prepare(&mut history, &[]).await.unwrap();

        assert_eq!(history.len(), 3);
        assert!(
            history[0]
                .content
                .as_deref()
                .unwrap()
                .contains("目标和决策摘要")
        );
        assert_eq!(history[1].content.as_deref(), Some("新问题"));
        assert_eq!(history[2].content.as_deref(), Some("新回答"));
        assert_eq!(*provider.calls.lock().unwrap(), 1);
        assert_eq!(prepared.first().unwrap().role, Role::System);
        assert!(
            prepared
                .last()
                .unwrap()
                .content
                .as_deref()
                .unwrap()
                .contains("cwd=")
        );
    }

    #[tokio::test]
    async fn mild_gate_only_summarizes_the_oldest_slice() {
        let provider = Arc::new(SummaryProvider {
            responses: Mutex::new(VecDeque::from([Response::Text("温和摘要".to_owned())])),
            calls: Mutex::new(0),
        });
        let manager = ContextManager::new(
            provider.clone(),
            std::env::current_dir().unwrap(),
            ContextConfig {
                token_budget: 100_000,
                recent_messages: 2,
                mild_compression_percent: 1,
                strong_compression_percent: 90,
                summary_chunk_tokens: 100_000,
            },
            Arc::new(PlanStore::memory_only()),
        )
        .unwrap();
        let mut history = (0..8)
            .map(|index: usize| {
                Message::text(Role::User, format!("消息-{index}-{}", "x".repeat(1_000)))
            })
            .collect::<Vec<Message>>();

        manager.prepare(&mut history, &[]).await.unwrap();

        assert_eq!(history.len(), 7);
        assert!(history[0].content.as_deref().unwrap().contains("温和摘要"));
        assert!(
            history[1]
                .content
                .as_deref()
                .unwrap()
                .starts_with("消息-2-")
        );
        assert!(
            history[6]
                .content
                .as_deref()
                .unwrap()
                .starts_with("消息-7-")
        );
        assert_eq!(*provider.calls.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn injects_current_plan_into_retrieved_partition_before_overlay() {
        let provider = Arc::new(SummaryProvider {
            responses: Mutex::new(VecDeque::new()),
            calls: Mutex::new(0),
        });
        let plan = Arc::new(PlanStore::memory_only());
        plan.set(vec![PlanStep {
            id: "1".to_owned(),
            description: "实现功能".to_owned(),
            status: PlanStatus::InProgress,
        }])
        .await
        .unwrap();
        let manager = ContextManager::new(
            provider,
            std::env::current_dir().unwrap(),
            ContextConfig {
                token_budget: 10_000,
                recent_messages: 12,
                mild_compression_percent: 60,
                strong_compression_percent: 85,
                summary_chunk_tokens: 2_000,
            },
            plan,
        )
        .unwrap();

        let prepared = manager.prepare(&mut Vec::new(), &[]).await.unwrap();
        let last = prepared
            .iter()
            .find_map(|m| {
                m.content
                    .as_deref()
                    .filter(|s| s.starts_with("[retrieved_context]"))
            })
            .unwrap();
        assert!(last.contains("当前任务计划"));
        assert!(last.contains("[>] 1 · 实现功能"));
    }

    #[tokio::test]
    async fn orders_context_from_stable_prefix_to_dynamic_suffix() {
        let workspace =
            std::env::temp_dir().join(format!("my-agent-context-order-{}", std::process::id()));
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(workspace.join("AGENTS.md"), "# 固定项目规则\n始终先测试").unwrap();
        let provider = Arc::new(SummaryProvider {
            responses: Mutex::new(VecDeque::new()),
            calls: Mutex::new(0),
        });
        let manager = ContextManager::new(
            provider,
            &workspace,
            ContextConfig {
                token_budget: 10_000,
                recent_messages: 12,
                mild_compression_percent: 60,
                strong_compression_percent: 85,
                summary_chunk_tokens: 2_000,
            },
            Arc::new(PlanStore::memory_only()),
        )
        .unwrap();
        let mut history = vec![Message::text(Role::User, "本轮问题")];

        let prepared = manager.prepare(&mut history, &[]).await.unwrap();

        assert_eq!(prepared.len(), 4);
        assert!(
            prepared[0]
                .content
                .as_deref()
                .unwrap()
                .contains("编码 Agent")
        );
        assert!(
            prepared[1]
                .content
                .as_deref()
                .unwrap()
                .contains("固定项目规则")
        );
        assert_eq!(prepared[2].content.as_deref(), Some("本轮问题"));
        assert!(
            prepared[3]
                .content
                .as_deref()
                .unwrap()
                .starts_with("[turn_overlay] 动态环境")
        );
        std::fs::remove_dir_all(workspace).unwrap();
    }

    #[tokio::test]
    async fn injects_only_bodies_of_skills_matching_the_user_request() {
        let workspace =
            std::env::temp_dir().join(format!("my-agent-skill-context-{}", std::process::id()));
        let skills_dir = workspace.join(".my-agent/skills");
        std::fs::create_dir_all(&skills_dir).unwrap();
        std::fs::write(
            skills_dir.join("rust-testing.md"),
            "---\nname: rust-testing\nversion: 1.0.0\ndescription: 用 cargo test 验证 Rust 项目\nkeywords: [Rust, 单元测试]\nscope: [rust]\n---\n# Rust 测试\n\n## 流程\nONLY_MATCHED_BODY\n",
        )
        .unwrap();
        let provider = Arc::new(SummaryProvider {
            responses: Mutex::new(VecDeque::new()),
            calls: Mutex::new(0),
        });
        let manager = ContextManager::new(
            provider,
            &workspace,
            ContextConfig {
                token_budget: 10_000,
                recent_messages: 12,
                mild_compression_percent: 60,
                strong_compression_percent: 85,
                summary_chunk_tokens: 2_000,
            },
            Arc::new(PlanStore::memory_only()),
        )
        .unwrap();

        let matched = manager
            .prepare(
                &mut vec![Message::text(Role::User, "请为 Rust 模块补单元测试")],
                &[],
            )
            .await
            .unwrap();
        let unmatched = manager
            .prepare(&mut vec![Message::text(Role::User, "查询今天的天气")], &[])
            .await
            .unwrap();

        let matched_dynamic = matched
            .iter()
            .find_map(|m| {
                m.content
                    .as_deref()
                    .filter(|s| s.starts_with("[retrieved_context]"))
            })
            .unwrap();
        assert!(matched_dynamic.contains("可用技能索引"));
        assert!(matched_dynamic.contains("ONLY_MATCHED_BODY"));
        let unmatched_dynamic = unmatched
            .iter()
            .find_map(|m| {
                m.content
                    .as_deref()
                    .filter(|s| s.starts_with("[retrieved_context]"))
            })
            .unwrap();
        assert!(unmatched_dynamic.contains("rust-testing@1.0.0"));
        assert!(!unmatched_dynamic.contains("ONLY_MATCHED_BODY"));
        std::fs::remove_dir_all(workspace).unwrap();
    }
    #[tokio::test]
    async fn compaction_keeps_entire_current_turn_and_retains_old_user_evidence() {
        let provider = Arc::new(SummaryProvider {
            responses: Mutex::new(VecDeque::from([
                Response::Text("历史结果摘要".into()),
                Response::Text("模型没有重述用户约束".into()),
            ])),
            calls: Mutex::new(0),
        });
        let manager = ContextManager::new(
            provider.clone(),
            std::env::current_dir().unwrap(),
            ContextConfig {
                token_budget: 32000,
                recent_messages: 2,
                mild_compression_percent: 60,
                strong_compression_percent: 85,
                summary_chunk_tokens: 100000,
            },
            Arc::new(PlanStore::memory_only()),
        )
        .unwrap();
        let mut history = vec![
            Message::text(Role::User, "禁止上传，必须保持中文"),
            Message::text(Role::Assistant, "历史工作".repeat(1000)),
            Message::text(Role::User, "继续当前任务"),
        ];
        for index in 0..8 {
            let call = crate::provider::ToolCall {
                id: format!("call-{index}"),
                name: "read_file".into(),
                arguments: serde_json::json!({"path":"src/context.rs"}),
            };
            history.push(Message::assistant_tool_calls(vec![call.clone()]));
            history.push(Message::tool_result(&call, "当前工具证据😀".repeat(1000)));
        }
        let (candidate, _) = manager
            .compact_candidate(&history, std::future::pending(), true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&candidate[1..], &history[2..]);
        assert!(
            candidate[0]
                .content
                .as_deref()
                .unwrap()
                .contains("禁止上传，必须保持中文")
        );
        assert_eq!(*provider.calls.lock().unwrap(), 1);
        assert!(agent_context::validate_candidate(&history, &candidate).is_ok());
        let current_only = history[2..].to_vec();
        assert!(
            manager
                .compact_candidate(&current_only, std::future::pending(), true)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(*provider.calls.lock().unwrap(), 1);
        let mut next = candidate;
        next.push(Message::text(Role::User, "开始下一轮"));
        next.push(Message::text(Role::Assistant, "下一轮答复"));
        let (second, _) = manager
            .compact_candidate(&next, std::future::pending(), true)
            .await
            .unwrap()
            .unwrap();
        assert!(
            second[0]
                .content
                .as_deref()
                .unwrap()
                .contains("禁止上传，必须保持中文")
        );
        assert_eq!(&second[1..], &next[next.len() - 2..]);
        assert_eq!(*provider.calls.lock().unwrap(), 2);
    }

    struct TruncatedSummary;
    #[async_trait]
    impl Provider for TruncatedSummary {
        async fn chat(&self, _: &[Message], _: &[ToolSpec]) -> Result<Response> {
            Ok(Response::Text("partial".into()))
        }
        async fn chat_stream(
            &self,
            _: &[Message],
            _: &[ToolSpec],
            events: tokio::sync::mpsc::UnboundedSender<crate::provider::ProviderEvent>,
        ) -> Result<()> {
            for event in [
                crate::provider::ProviderEvent::TextDelta("partial".into()),
                crate::provider::ProviderEvent::OutputTruncated,
                crate::provider::ProviderEvent::ProtocolDone,
            ] {
                let _ = events.send(event);
            }
            Ok(())
        }
    }
    #[tokio::test]
    async fn truncated_summary_and_cancelled_candidate_are_never_installed() {
        let manager = ContextManager::new(
            Arc::new(TruncatedSummary),
            std::env::current_dir().unwrap(),
            ContextConfig {
                token_budget: 32000,
                recent_messages: 2,
                mild_compression_percent: 60,
                strong_compression_percent: 85,
                summary_chunk_tokens: 10000,
            },
            Arc::new(PlanStore::memory_only()),
        )
        .unwrap();
        assert!(manager.request_summary("source").await.is_err());
        let history = vec![
            Message::text(Role::User, "original".repeat(100)),
            Message::text(Role::Assistant, "result"),
            Message::text(Role::User, "latest"),
            Message::text(Role::Assistant, "tail"),
        ];
        let original = history.clone();
        let cancelled = manager
            .compact_candidate(&history, std::future::ready(()), true)
            .await
            .unwrap_err();
        assert!(matches!(
            cancelled.downcast_ref::<agent_context::ContextError>(),
            Some(agent_context::ContextError::Cancelled)
        ));
        assert_eq!(history, original);
    }
}
