//! 能力只约束当前物理连接的协议面，不授予 session 或资源权限。
use crate::{JsonRpcRequest, ProtocolError, normalize_request};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capability {
    pub name: String,
    pub schema_version: u16,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitializeParams {
    pub protocol_versions: Vec<u16>,
    pub capabilities: Vec<Capability>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NegotiatedCapabilities {
    pub protocol_version: u16,
    pub capabilities: Vec<Capability>,
}

pub fn supported_capabilities() -> Vec<Capability> {
    [
        "session",
        "runs",
        "interactions",
        "context",
        "memory",
        "resources",
        "artifacts",
        "delegation",
        "configuration",
        "maintenance",
    ]
    .into_iter()
    .map(|name| Capability {
        name: name.into(),
        schema_version: 1,
    })
    .collect()
}
impl Default for InitializeParams {
    fn default() -> Self {
        Self {
            protocol_versions: vec![1],
            capabilities: supported_capabilities(),
        }
    }
}
impl InitializeParams {
    pub fn negotiate(&self) -> Result<NegotiatedCapabilities, ProtocolError> {
        if self.protocol_versions.is_empty()
            || self.protocol_versions.len() > 8
            || self.capabilities.len() > 32
        {
            return Err(ProtocolError::InvalidParams(
                "能力协商超过预算或无版本".into(),
            ));
        }
        let versions = self.protocol_versions.iter().collect::<BTreeSet<_>>();
        let names = self
            .capabilities
            .iter()
            .map(|c| &c.name)
            .collect::<BTreeSet<_>>();
        if versions.len() != self.protocol_versions.len()
            || names.len() != self.capabilities.len()
            || self
                .capabilities
                .iter()
                .any(|c| c.name.is_empty() || c.name.len() > 64 || c.schema_version == 0)
        {
            return Err(ProtocolError::InvalidParams("能力或版本重复/无效".into()));
        }
        if !self.protocol_versions.contains(&1) {
            return Err(ProtocolError::InvalidParams("没有共同协议版本".into()));
        }
        let capabilities = supported_capabilities()
            .into_iter()
            .filter(|c| self.capabilities.contains(c))
            .collect();
        Ok(NegotiatedCapabilities {
            protocol_version: 1,
            capabilities,
        })
    }
}
impl NegotiatedCapabilities {
    pub fn validate_offer(&self, offer: &InitializeParams) -> Result<(), ProtocolError> {
        offer.negotiate()?;
        let names = self
            .capabilities
            .iter()
            .map(|c| &c.name)
            .collect::<BTreeSet<_>>();
        if !offer.protocol_versions.contains(&self.protocol_version)
            || self.protocol_version != 1
            || names.len() != self.capabilities.len()
            || self
                .capabilities
                .iter()
                .any(|c| !offer.capabilities.contains(c) || !supported_capabilities().contains(c))
        {
            return Err(ProtocolError::InvalidParams(
                "服务端能力交集与本次 offer 不一致".into(),
            ));
        }
        Ok(())
    }
    pub fn permits(&self, method: &str) -> bool {
        if method == "slash.execute"
            && supported_capabilities()
                .iter()
                .any(|c| !self.capabilities.contains(c))
        {
            return false;
        }
        required_capability(method).is_some_and(|name| {
            self.capabilities
                .iter()
                .any(|c| c.name == name && c.schema_version == 1)
        })
    }
}
fn required_capability(method: &str) -> Option<&'static str> {
    match method {
        "ping" | "session.new" | "session.load" | "session.load_page" | "session.list"
        | "session.resume" | "session.clear" | "session.delete" | "session.fork"
        | "session.trace" | "session.trace_page" | "session.close" => Some("session"),
        "sessions.plan.readback" | "sessions.plan.discard" | "hooks.readback" | "views.reduce" => {
            Some("session")
        }
        "chat.send"
        | "run.read"
        | "run.events"
        | "run.discovery"
        | "run.tools"
        | "run.audit"
        | "run.provider_attempts"
        | "run.reconcile"
        | "agent.cancel"
        | "agent.subscribe"
        | "queue.list"
        | "queue.read"
        | "queue.remove" => Some("runs"),
        "approval.respond"
        | "interaction.respond"
        | "interaction.reject"
        | "interaction.read"
        | "interaction.list" => Some("interactions"),
        "session.compact" | "compact.start" | "context.readback" => Some("context"),
        "memory.store" | "memory.recall" | "memory.list" | "memory.forget" | "memory.scope"
        | "memory.feedback" | "memory.flywheel" | "memory.evidence" => Some("memory"),
        "resources.list"
        | "resources.read"
        | "resources.logs"
        | "resources.wait"
        | "resources.stop"
        | "resources.reconcile" => Some("resources"),
        "artifacts.read" => Some("artifacts"),
        "spawn_subagent"
        | "list_subagents"
        | "read_subagent"
        | "wait_subagents"
        | "cancel_subagent"
        | "subagent.result.reserve"
        | "subagent.result.release"
        | "subagent.result.commit" => Some("delegation"),
        "permissions.get" | "permissions.set" | "models.list" | "models.use" | "models.save" => {
            Some("configuration")
        }
        "runtime.doctor" | "daemon.stop" | "slash.execute" => Some("maintenance"),
        _ => None,
    }
}

#[derive(Default)]
pub struct ConnectionState {
    negotiated: Option<NegotiatedCapabilities>,
    legacy: bool,
}
pub enum ConnectionRequest {
    Initialized(NegotiatedCapabilities),
    Dispatch(JsonRpcRequest),
}
impl ConnectionState {
    /// 调度 callback 前串行执行；失败不修改连接协商，也不产生业务 mutation。
    pub fn accept(&mut self, request: JsonRpcRequest) -> Result<ConnectionRequest, ProtocolError> {
        let request = normalize_request(request)?;
        if request.method == "connection.initialize" {
            if self.legacy {
                return Err(ProtocolError::InvalidParams(
                    "legacy 连接需重连后协商".into(),
                ));
            }
            let offer: InitializeParams = serde_json::from_value(request.params)?;
            let result = offer.negotiate()?;
            if self.negotiated.as_ref().is_some_and(|old| old != &result) {
                return Err(ProtocolError::InvalidParams(
                    "当前连接能力不可热变更；请重连".into(),
                ));
            }
            self.negotiated = Some(result.clone());
            return Ok(ConnectionRequest::Initialized(result));
        }
        if let Some(negotiated) = &self.negotiated {
            if request.protocol_version != Some(negotiated.protocol_version)
                || !negotiated.permits(&request.method)
                || (request.method == "chat.send"
                    && request
                        .params
                        .get("plan_execution")
                        .is_some_and(|v| !v.is_null())
                    && !negotiated.permits("sessions.plan.readback"))
            {
                return Err(ProtocolError::InvalidParams(
                    "方法/版本/schema 能力未协商".into(),
                ));
            }
        } else if request.protocol_version.is_some() {
            return Err(ProtocolError::InvalidParams(
                "versioned 连接必须先 connection.initialize".into(),
            ));
        } else {
            self.legacy = true;
        }
        Ok(ConnectionRequest::Dispatch(request))
    }
}
