mod edit;
mod exec;
mod read;
mod sandbox;
mod write;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;

use crate::provider::{Message, ToolSpec};

pub use edit::EditFileTool;
pub use exec::ExecTool;
pub use read::ReadFileTool;
pub use sandbox::{ExecRequest, NativeSandbox, Sandbox, SandboxBackend};
pub use write::WriteFileTool;

#[async_trait]
pub trait ToolCancellation: Send + Sync {
    fn is_cancelled(&self) -> bool;
    async fn cancelled(&self);
}

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
}

impl ToolRegistry {
    pub fn descriptor(
        &self,
        call: &crate::provider::ToolCall,
    ) -> Result<agent_core::ToolDescriptor> {
        self.resolve(&call.name)
            .ok_or_else(|| anyhow::anyhow!("未知工具 {}", call.name))?
            .descriptor(&call.arguments)
    }
    pub fn scheduled_waves(&self, calls: &[crate::provider::ToolCall]) -> Result<Vec<Vec<usize>>> {
        let descriptors = calls
            .iter()
            .map(|call| {
                self.resolve(&call.name)
                    .ok_or_else(|| anyhow::anyhow!("未知工具 {}", call.name))?
                    .descriptor(&call.arguments)
            })
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
        Ok(subset)
    }

    pub fn frozen(&self) -> Result<Self> {
        let mut frozen = Self::new();
        for spec in self.specs() {
            let tool = self
                .resolve(&spec.name)
                .ok_or_else(|| anyhow::anyhow!("工具 catalog 在冻结期间变化：{}", spec.name))?;
            anyhow::ensure!(
                !frozen.tools.contains_key(&spec.name),
                "工具 catalog 重名：{}",
                spec.name
            );
            frozen.tools.insert(spec.name, tool);
        }
        Ok(frozen)
    }

    pub fn specs(&self) -> Vec<ToolSpec> {
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
        let tool = self
            .resolve(name)
            .ok_or_else(|| ToolAdmissionError::UnknownTool(name.to_owned()))?;
        tool.execute_rich_with_cancellation(args, cancellation)
            .await
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
            let tool = self
                .resolve(&call.name)
                .ok_or_else(|| ToolAdmissionError::UnknownTool(call.name.clone()))?;
            crate::safety::with_action_identity(
                crate::safety::action_identity(round, call),
                tool.preflight(&call.arguments),
            )
            .await?;
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
        let Some(tool) = self.resolve(name) else {
            return Err(ToolAdmissionError::UnknownTool(name.to_owned()));
        };
        validate_value(&tool.parameters(), args, "$args").map_err(|message| {
            ToolAdmissionError::InvalidArguments {
                tool: name.to_owned(),
                message,
            }
        })
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
