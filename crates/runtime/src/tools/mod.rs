mod edit;
mod exec;
mod read;
mod search;
pub(crate) use agent_sandbox::terminate_process_group;
mod write;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;

use crate::provider::{Message, ToolSpec};

pub use agent_sandbox::{ExecRequest, NativeSandbox, Sandbox, SandboxBackend};
pub use edit::EditFileTool;
pub use exec::ExecTool;
pub use read::ReadFileTool;
pub use write::WriteFileTool;

pub use agent_sandbox::ToolCancellation;

#[derive(Debug, Error)]
pub enum ToolAdmissionError {
    #[error("未知工具: {0}")]
    UnknownTool(String),
    #[error("工具 {tool} 参数校验失败: {message}")]
    InvalidArguments { tool: String, message: String },
}

impl ToolAdmissionError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::UnknownTool(_) => "unknown_tool",
            Self::InvalidArguments { .. } => "invalid_arguments",
        }
    }
}

#[async_trait]
pub trait Tool: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn parameters(&self) -> Value;
    fn is_read_only(&self) -> bool {
        false
    }
    fn descriptor(&self, _args: &Value) -> Result<agent_core::ToolDescriptor> {
        Ok(if self.is_read_only() {
            agent_core::ToolDescriptor::read()
        } else {
            agent_core::ToolDescriptor::external()
        })
    }
    async fn execute(&self, args: Value) -> Result<String>;
    async fn preflight(&self, _args: &Value) -> Result<()> {
        Ok(())
    }

    fn stop_resources(&self) {}

    async fn execute_rich(&self, args: Value) -> Result<ToolOutput> {
        self.execute(args).await.map(ToolOutput::text)
    }

    async fn execute_rich_with_cancellation(
        &self,
        args: Value,
        _cancellation: &dyn ToolCancellation,
    ) -> Result<ToolOutput> {
        self.execute_rich(args).await
    }
}

pub trait DynamicToolSource: Send + Sync {
    fn specs(&self) -> Vec<ToolSpec>;
    fn get(&self, name: &str) -> Option<Arc<dyn Tool>>;
}

pub struct ToolOutput {
    pub content: String,
    pub transient_messages: Vec<Message>,
}

impl ToolOutput {
    pub fn text(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            transient_messages: Vec::new(),
        }
    }
}

#[derive(Clone, Default)]
pub struct ToolRegistry {
    tools: HashMap<String, Arc<dyn Tool>>,
    dynamic_sources: Vec<Arc<dyn DynamicToolSource>>,
    search: Option<Arc<search::SearchCatalog>>,
}

impl ToolRegistry {
    pub fn descriptor(
        &self,
        call: &crate::provider::ToolCall,
    ) -> Result<agent_core::ToolDescriptor> {
        match self.target(&call.name, &call.arguments)? {
            Some((tool, args)) => tool.descriptor(&args),
            None => Ok(agent_core::ToolDescriptor::read()),
        }
    }
    pub fn scheduled_waves(&self, calls: &[crate::provider::ToolCall]) -> Result<Vec<Vec<usize>>> {
        let descriptors = calls
            .iter()
            .map(|call| self.descriptor(call))
            .collect::<Result<Vec<_>>>()?;
        let mut waves = Vec::new();
        let mut current: Vec<usize> = Vec::new();
        for (index, descriptor) in descriptors.iter().enumerate() {
            if current.len() >= 8
                || current
                    .iter()
                    .any(|i| descriptor.conflicts(&descriptors[*i]))
            {
                waves.push(std::mem::take(&mut current));
            }
            current.push(index);
            if descriptor.effect == agent_core::ToolEffect::External {
                waves.push(std::mem::take(&mut current));
            }
        }
        if !current.is_empty() {
            waves.push(current);
        }
        Ok(waves)
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<T>(&mut self, tool: T)
    where
        T: Tool + 'static,
    {
        self.tools.insert(tool.name().to_owned(), Arc::new(tool));
    }

    pub fn stop_resources(&self) {
        for tool in self.tools.values() {
            tool.stop_resources();
        }
    }

    pub fn register_dynamic_source<T>(&mut self, source: Arc<T>)
    where
        T: DynamicToolSource + 'static,
    {
        self.dynamic_sources.push(source);
    }

    pub fn subset<'a>(&self, names: impl IntoIterator<Item = &'a str>) -> Result<Self> {
        let mut subset = Self::new();
        for name in names {
            let Some(tool) = self.tools.get(name) else {
                bail!("不可用的子 Agent 工具: {name}");
            };
            subset.tools.insert(name.to_owned(), tool.clone());
        }
        if self.search.is_some() {
            let specs = self
                .specs()
                .into_iter()
                .filter(|spec| subset.tools.contains_key(&spec.name))
                .collect();
            subset.search = Some(Arc::new(search::SearchCatalog::new(specs)?));
            Ok(subset)
        } else {
            Ok(subset)
        }
    }

    pub fn frozen(&self) -> Result<Self> {
        if self.search.is_some() {
            return Ok(self.clone());
        }
        let mut frozen = Self::new();
        let specs = self.specs();
        for spec in &specs {
            anyhow::ensure!(spec.name != "tool_search", "保留工具名冲突：tool_search");
            let tool = self
                .resolve(&spec.name)
                .ok_or_else(|| anyhow::anyhow!("工具 catalog 在冻结期间变化：{}", spec.name))?;
            anyhow::ensure!(
                tool.name() == spec.name,
                "工具 canonical name 与 descriptor 不一致"
            );
            anyhow::ensure!(
                tool.description() == spec.description && tool.parameters() == spec.parameters,
                "工具 descriptor 在冻结期间变化：{}",
                spec.name
            );
            anyhow::ensure!(
                !frozen.tools.contains_key(&spec.name),
                "工具 catalog 重名：{}",
                spec.name
            );
            frozen.tools.insert(spec.name.clone(), tool);
        }
        frozen.search = Some(Arc::new(search::SearchCatalog::new(specs)?));
        Ok(frozen)
    }

    pub fn provider_specs(&self) -> Vec<ToolSpec> {
        if self.search.is_none() {
            return self.specs();
        }
        let catalog = self.specs();
        let deferred = catalog
            .iter()
            .any(|spec| !search::CORE_TOOLS.contains(&spec.name.as_str()));
        let mut specs = catalog
            .into_iter()
            .filter(|spec| search::CORE_TOOLS.contains(&spec.name.as_str()))
            .collect::<Vec<_>>();
        if deferred {
            specs.push(search::provider_spec());
        }
        specs
    }

    pub fn specs(&self) -> Vec<ToolSpec> {
        if let Some(search) = &self.search {
            return (*search.specs).clone();
        }
        let mut specs = self
            .tools
            .values()
            .map(|tool| ToolSpec {
                name: tool.name().to_owned(),
                description: tool.description().to_owned(),
                parameters: tool.parameters(),
            })
            .collect::<Vec<ToolSpec>>();
        specs.extend(
            self.dynamic_sources
                .iter()
                .flat_map(|source| source.specs()),
        );
        specs.sort_by(|left: &ToolSpec, right: &ToolSpec| left.name.cmp(&right.name));
        specs
    }

    #[allow(dead_code)]
    pub async fn execute(&self, name: &str, args: Value) -> Result<ToolOutput> {
        self.admit(name, &args)?;
        let tool = self
            .resolve(name)
            .ok_or_else(|| ToolAdmissionError::UnknownTool(name.to_owned()))?;
        tool.execute_rich(args).await
    }

    pub async fn execute_with_cancellation(
        &self,
        name: &str,
        args: Value,
        cancellation: &dyn ToolCancellation,
    ) -> Result<ToolOutput> {
        self.admit(name, &args)?;
        match self.target(name, &args)? {
            Some((tool, arguments)) => {
                tool.execute_rich_with_cancellation(arguments, cancellation)
                    .await
            }
            None => {
                let search = self
                    .search
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("未冻结的检索目录"))?;
                let query = serde_json::from_value(args)?;
                search.execute(query, cancellation).await
            }
        }
    }

    pub async fn preflight_all(
        &self,
        calls: &[crate::provider::ToolCall],
        round: usize,
    ) -> Result<()> {
        anyhow::ensure!(calls.len() <= 64, "tool batch 超过 64 调用预算");
        anyhow::ensure!(
            calls
                .iter()
                .all(|call| call.arguments.to_string().len() <= 65536),
            "tool 参数超过 64 KiB 预算"
        );
        self.admit_all(calls)?;
        self.scheduled_waves(calls)?;
        for call in calls {
            if let Some((tool, args)) = self.target(&call.name, &call.arguments)? {
                crate::safety::with_action_identity(
                    crate::safety::action_identity(round, call),
                    tool.preflight(&args),
                )
                .await?;
            }
        }
        Ok(())
    }

    pub fn admit_all(&self, calls: &[crate::provider::ToolCall]) -> Result<(), ToolAdmissionError> {
        for call in calls {
            self.admit(&call.name, &call.arguments)?;
        }
        Ok(())
    }

    fn admit(&self, name: &str, args: &Value) -> Result<(), ToolAdmissionError> {
        let invalid = |message: String| ToolAdmissionError::InvalidArguments {
            tool: name.into(),
            message,
        };
        let target = self
            .target(name, args)
            .map_err(|e| invalid(e.to_string()))?;
        let schema = if let Some((tool, _)) = &target {
            self.specs()
                .into_iter()
                .find(|spec| spec.name == tool.name())
                .map(|spec| spec.parameters)
                .ok_or_else(|| ToolAdmissionError::UnknownTool(tool.name().into()))?
        } else {
            search::provider_spec().parameters
        };
        let arguments = target.as_ref().map_or(args, |(_, arguments)| arguments);
        validate_value(&schema, arguments, "$args").map_err(invalid)
    }

    fn target(&self, name: &str, args: &Value) -> Result<Option<(Arc<dyn Tool>, Value)>> {
        let target_name = name;
        let mut arguments = args.clone();
        if let Some(search) = &self.search {
            if name == "tool_search" {
                match serde_json::from_value::<agent_core::ToolSearchRequest>(args.clone())? {
                    agent_core::ToolSearchRequest::Search(_) => return Ok(None),
                    agent_core::ToolSearchRequest::Invoke(invoke) => {
                        let (repository, owner) = crate::loop_engine::current_session_repository()
                            .ok_or_else(|| anyhow::anyhow!("嵌套工具缺 canonical owner"))?;
                        anyhow::ensure!(
                            repository.discovered_tool(&owner, &search.generation, &invoke.name)?,
                            "工具尚未在当前 run/generation 发现"
                        );
                        let tool = self
                            .resolve(&invoke.name)
                            .ok_or_else(|| anyhow::anyhow!("冻结目录缺工具 {}", invoke.name))?;
                        return Ok(Some((tool, invoke.arguments)));
                    }
                }
            }
            anyhow::ensure!(
                search::CORE_TOOLS.contains(&name),
                "隐藏工具禁止 direct 调用：{name}"
            );
        }
        let tool = self
            .resolve(target_name)
            .ok_or_else(|| ToolAdmissionError::UnknownTool(target_name.into()))?;
        Ok(Some((tool, std::mem::take(&mut arguments))))
    }

    fn resolve(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.tools.get(name).cloned().or_else(|| {
            self.dynamic_sources
                .iter()
                .find_map(|source| source.get(name))
        })
    }
}

fn validate_value(schema: &Value, value: &Value, path: &str) -> Result<(), String> {
    let validator =
        jsonschema::validator_for(schema).map_err(|e| format!("{path} schema 无效: {e}"))?;
    if let Some(error) = validator.iter_errors(value).next() {
        return Err(format!("{path} {}: {error}", error.instance_path));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct ChangingTool {
        generation: Arc<std::sync::atomic::AtomicUsize>,
        name: &'static str,
    }
    #[async_trait]
    impl Tool for ChangingTool {
        fn name(&self) -> &str {
            self.name
        }
        fn description(&self) -> &str {
            "版本化 descriptor"
        }
        fn parameters(&self) -> Value {
            json!({"type":"integer","const":self.generation.load(std::sync::atomic::Ordering::SeqCst)})
        }
        async fn execute(&self, args: Value) -> Result<String> {
            Ok(args.to_string())
        }
    }
    #[test]
    fn frozen_schema_survives_reload_and_hidden_direct_or_unowned_invokes_fail_closed() {
        let generation = Arc::new(std::sync::atomic::AtomicUsize::new(1));
        let mut registry = ToolRegistry::new();
        registry.register(ChangingTool {
            generation: generation.clone(),
            name: "read_file",
        });
        registry.register(ChangingTool {
            generation: generation.clone(),
            name: "deferred",
        });
        let frozen = registry.frozen().unwrap();
        assert_eq!(
            frozen
                .provider_specs()
                .iter()
                .map(|spec| spec.name.as_str())
                .collect::<Vec<_>>(),
            vec!["read_file", "tool_search"]
        );
        let call = |name: &str, arguments| crate::provider::ToolCall {
            id: "id".into(),
            name: name.into(),
            arguments,
        };
        assert!(frozen.admit_all(&[call("read_file", json!(1))]).is_ok());
        generation.store(2, std::sync::atomic::Ordering::SeqCst);
        assert!(frozen.admit_all(&[call("read_file", json!(1))]).is_ok());
        assert!(frozen.admit_all(&[call("read_file", json!(2))]).is_err());
        assert!(frozen.admit_all(&[call("deferred", json!(1))]).is_err());
        assert!(
            frozen
                .admit_all(&[call(
                    "tool_search",
                    json!({"name":"deferred","arguments":1})
                )])
                .is_err()
        );
        let narrowed = frozen.subset(["read_file"]).unwrap();
        assert!(narrowed.admit_all(&[call("read_file", json!(1))]).is_ok());
        assert_eq!(narrowed.provider_specs().len(), 1);
        assert!(narrowed.specs().iter().all(|spec| spec.name != "deferred"));
        assert_ne!(
            registry.frozen().unwrap().search.unwrap().generation,
            frozen.search.as_ref().unwrap().generation
        );
        registry.register(ChangingTool {
            generation,
            name: "tool_search",
        });
        assert!(registry.frozen().is_err());
    }

    struct DescribedTool;
    #[async_trait]
    impl Tool for DescribedTool {
        fn name(&self) -> &str {
            "described"
        }
        fn description(&self) -> &str {
            "test"
        }
        fn parameters(&self) -> Value {
            json!({"type":"object"})
        }
        fn descriptor(&self, args: &Value) -> Result<agent_core::ToolDescriptor> {
            let mut d = agent_core::ToolDescriptor::read();
            if args["fence"] == true {
                return Ok(agent_core::ToolDescriptor::external());
            }
            d.resources.push(agent_core::ResourceAccess {
                key: args["key"].as_str().unwrap().into(),
                mode: if args["write"] == true {
                    agent_core::AccessMode::Write
                } else {
                    agent_core::AccessMode::Read
                },
            });
            Ok(d)
        }
        async fn execute(&self, _: Value) -> Result<String> {
            Ok("ok".into())
        }
    }
    #[test]
    fn scheduling_parallelizes_independent_resources_and_fences_conflicting_writes() {
        let mut registry = ToolRegistry::new();
        registry.register(DescribedTool);
        let calls = [
            json!({"key":"a","write":true}),
            json!({"key":"b","write":true}),
            json!({"key":"a","write":false}),
            json!({"fence":true}),
            json!({"key":"c"}),
        ]
        .into_iter()
        .enumerate()
        .map(|(id, arguments)| crate::provider::ToolCall {
            id: id.to_string(),
            name: "described".into(),
            arguments,
        })
        .collect::<Vec<_>>();
        assert_eq!(
            registry.scheduled_waves(&calls).unwrap(),
            vec![vec![0, 1], vec![2], vec![3], vec![4]]
        );
        assert!(
            validate_value(
                &json!({"type":"array","items":{"type":"integer","minimum":1},"maxItems":2}),
                &json!([0]),
                "args"
            )
            .is_err()
        );
        assert!(
            validate_value(
                &json!({"enum":["native","docker"]}),
                &json!("silent-fallback"),
                "args"
            )
            .is_err()
        );
    }

    #[test]
    fn validator_is_permissive_unless_extra_fields_are_explicitly_forbidden() {
        let permissive = json!({
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"]
        });
        assert!(validate_value(&permissive, &json!({"path": "a", "extra": 1}), "$args").is_ok());

        let strict = json!({
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
            "additionalProperties": false
        });
        assert!(validate_value(&strict, &json!({"path": "a", "extra": 1}), "$args").is_err());
        assert!(validate_value(&strict, &json!({}), "$args").is_err());
        assert!(validate_value(&strict, &json!({"path": 42}), "$args").is_err());
    }
}
