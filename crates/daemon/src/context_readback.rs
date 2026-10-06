//! 纯读取已发送材料；不构造 runtime、Provider 或 MCP。
use super::DaemonState;
use agent_core::*;
use agent_daemon_protocol::params::ContextReadParams;
use serde_json::Value;

impl DaemonState {
    pub(super) fn context_readback(
        &self,
        params: ContextReadParams,
    ) -> Result<Value, (i64, String)> {
        let fail = |error: agent_storage::RuntimeError| (-32001, error.to_string());
        let (capture, frozen) = self
            .run_store
            .provider_request(
                &SessionKey(params.session_id),
                &params.expected_lifetime,
                params.run_id.as_ref(),
                params.capture_id.as_deref(),
            )
            .map_err(fail)?;
        let assembled = agent_context::assemble_request(&capture.input)
            .map_err(|error| (-32001, error.to_string()))?;
        if assembled.request_digest != capture.request_digest {
            return Err((-32001, "Provider 请求重建摘要不一致".into()));
        }
        let mut omissions = assembled.omissions;
        let (mut messages, tools) = if params.local_diagnostics {
            if self
                .safety
                .as_ref()
                .is_some_and(|safety| safety.mode().key() != frozen.permission_mode)
            {
                return Err((-32001, "当前权限与 capture 不一致，完整诊断不可用".into()));
            }
            let mut secrets = std::env::vars()
                .filter(|(name, _)| {
                    let name = name.to_ascii_uppercase();
                    ["KEY", "TOKEN", "PASSWORD", "SECRET"]
                        .iter()
                        .any(|marker| name.contains(marker))
                })
                .map(|(_, value)| value)
                .collect::<Vec<_>>();
            // 直接只读配置，避免 ConfigStore::load 的旧凭据迁移写入。
            match std::fs::read(self.config_store.path()) {
                Ok(bytes) => {
                    let config: agent_runtime::config::ConfigFile = serde_json::from_slice(&bytes)
                        .map_err(|_| (-32001, "配置损坏，完整诊断不可用".into()))?;
                    for profile in config.profiles {
                        if let Some(reference) = profile.api_key {
                            secrets
                                .push(agent_runtime::secrets::resolve(&reference).map_err(
                                    |_| (-32001, "凭据无法核对，完整诊断不可用".into()),
                                )?);
                        }
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return Err((-32001, "配置读取失败，完整诊断不可用".into())),
            }
            omissions.push("diagnostic:secrets_memory_media_and_tool_arguments_redacted".into());
            let tools = assembled
                .tools
                .into_iter()
                .map(|mut tool| {
                    // schema 的 default/examples 同样保守脱敏。
                    agent_context::diagnostic_value(&mut tool.parameters, &secrets);
                    tool.description = agent_context::diagnostic_messages(
                        &[Message::text(Role::System, tool.description)],
                        &secrets,
                    )
                    .into_iter()
                    .next()
                    .and_then(|message| message.content)
                    .unwrap_or_default();
                    tool
                })
                .collect();
            (
                Some(agent_context::diagnostic_messages(
                    &assembled.messages,
                    &secrets,
                )),
                Some(tools),
            )
        } else {
            (None, None)
        };
        if capture.input.purpose == ProviderRequestPurpose::CompactSummary {
            if let Some(messages) = messages.as_mut() {
                for message in messages {
                    if message.role == Role::User {
                        message.content = Some("[摘要源原文已脱敏；见源摘要]".into());
                    }
                }
            }
        }
        let readback = ProviderRequestReadback {
            purpose: capture.input.purpose,
            schema_version: 1,
            capture_id: capture.capture_id,
            owner: capture.owner,
            round: capture.round,
            candidate_index: capture.candidate_index,
            snapshot_revision: capture.snapshot_revision,
            envelope: assembled.envelope,
            policy_fingerprint: capture.policy_fingerprint,
            request_digest: capture.request_digest,
            message_digest: assembled.message_digest,
            provider_tools_digest: assembled.provider_tools_digest,
            images: capture.input.images,
            tool_calls: capture.input.tool_calls,
            replayability: "captured".into(),
            omissions,
            messages,
            tools,
        };
        let value = serde_json::to_value(readback).map_err(|error| (-32603, error.to_string()))?;
        if serde_json::to_vec(&value)
            .map_err(|error| (-32603, error.to_string()))?
            .len()
            > agent_daemon_protocol::MAX_FRAME_BYTES - 256 * 1024
        {
            return Err((-32001, "完整诊断超出响应预算，请读取摘要".into()));
        }
        Ok(value)
    }
}
