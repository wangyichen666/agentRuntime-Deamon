//! 实际 Provider 调用与只读 capture 重建共用的纯函数。
use crate::{ContextError, TokenEstimator};
use agent_core::*;
use sha2::{Digest, Sha256};

pub use agent_core::AssembledProviderRequest as AssembledRequest;

pub fn value_digest(value: &serde_json::Value) -> Result<String, ContextError> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(value)?)))
}

pub fn assemble_request(input: &ProviderRequestInput) -> Result<AssembledRequest, ContextError> {
    if input.budget < 256
        || input.messages.len() > 32768
        || input.catalog.len() > 8192
        || input.tools.len() > 8192
        || input.source.policy_fingerprint.is_empty()
        || input.provider_identity.is_empty()
        || input.source.source_start != TranscriptSeq(0)
        || serde_json::to_vec(input)?.len() > 16 * 1024 * 1024
    {
        return Err(ContextError::InvalidRequest);
    }
    let mut messages = input.messages.clone();
    let mut omissions = vec!["dynamic_environment:non_replayable_current;captured_at_send".into()];
    for message in &mut messages {
        if message
            .content
            .as_deref()
            .is_some_and(|text| text.starts_with("[retrieved_context]"))
        {
            omissions.push("plan_skill:captured_versions;non_replayable_current".into());
        }
        if message
            .content
            .as_deref()
            .is_some_and(|text| text.starts_with("[retrieved_memory]"))
        {
            omissions.push("memory:captured_visibility;non_replayable_current".into());
        }
        if message.thinking.take().is_some() {
            omissions.push("stored_thinking:not_sent".into());
        }
        if !input.images && !message.image_urls.is_empty() {
            let note = format!(
                "[当前 provider 不支持图片，已省略 {} 个图片内容块]",
                message.image_urls.len()
            );
            message.content = Some(match message.content.take() {
                Some(content) if !content.is_empty() => format!("{content}\n{note}"),
                _ => note,
            });
            message.image_urls.clear();
            omissions.push("media:unsupported_provider".into());
        }
    }
    let tools = if input.tool_calls {
        input.tools.clone()
    } else {
        omissions.push("tools:unsupported_provider".into());
        Vec::new()
    };
    let meter = TokenEstimator;
    let mut stable_tokens = meter.request(&[], &tools)? as u64;
    let (mut history_tokens, mut retrieved_tokens, mut overlay_tokens) = (0u64, 0u64, 0u64);
    let turn_start = current_turn_start(&messages);
    for (index, message) in messages.iter().enumerate() {
        let count = meter.messages(std::slice::from_ref(message)) as u64;
        let text = message.content.as_deref().unwrap_or_default();
        if message.role == Role::System && text.starts_with("[retrieved_") {
            retrieved_tokens = retrieved_tokens.saturating_add(count);
        } else if message.role == Role::System
            && (index == 0 || text.starts_with("项目规则（AGENTS.md）"))
        {
            stable_tokens = stable_tokens.saturating_add(count);
        } else if index >= turn_start
            || (message.role == Role::System && text.starts_with("[turn_overlay]"))
        {
            overlay_tokens = overlay_tokens.saturating_add(count);
        } else {
            history_tokens = history_tokens.saturating_add(count);
        }
    }
    let estimated = stable_tokens
        .saturating_add(history_tokens)
        .saturating_add(retrieved_tokens)
        .saturating_add(overlay_tokens);
    let calibrated = input.calibration.map_or(estimated, |(actual, prior)| {
        estimated.saturating_add(actual.saturating_sub(prior))
    });
    let output_reserve = (input.budget / 10).clamp(1, 4096);
    if calibrated.saturating_add(output_reserve) > input.budget {
        return Err(ContextError::RequestBudget);
    }
    let catalog_digest = value_digest(&serde_json::to_value(&input.catalog)?)?;
    let message_digest = crate::digest(&messages)?;
    let provider_tools_digest = value_digest(&serde_json::to_value(&tools)?)?;
    let envelope = ContextEnvelope {
        source: input.source.clone(),
        route: input.route.clone(),
        provider_identity: input.provider_identity.clone(),
        tool_catalog_digest: catalog_digest,
        stable_tokens,
        history_tokens,
        retrieved_tokens,
        overlay_tokens,
        calibrated_input_tokens: calibrated,
        output_reserve,
        budget: input.budget,
    };
    let mut digest_material = serde_json::json!({"messages":messages,"tools":tools,"envelope":envelope,"images":input.images,"tool_calls":input.tool_calls});
    if input.purpose == ProviderRequestPurpose::CompactSummary {
        digest_material["purpose"] = serde_json::json!(input.purpose);
        omissions.push("summary_source:diagnostic_digest_only".into());
    }
    let request_digest = value_digest(&digest_material)?;
    omissions.sort();
    omissions.dedup();
    Ok(AssembledRequest {
        messages,
        tools,
        envelope,
        request_digest,
        message_digest,
        provider_tools_digest,
        omissions,
    })
}

/// 完整诊断只暴露结构与已清理文本；memory、媒体和工具参数始终省略。
/// 凭证可能出现在任意自由文本，保守屏蔽整行，避免只替换标签。
pub fn diagnostic_messages(messages: &[Message], secrets: &[String]) -> Vec<Message> {
    messages
        .iter()
        .cloned()
        .map(|mut message| {
            message.thinking = None;
            message.image_urls.clear();
            message.tool_calls.clear();
            message.tool_call_id = None;
            message.name = None;
            if let Some(text) = message.content.take() {
                message.content = Some(
                    if message.role == Role::Tool
                        || text.starts_with("[retrieved_memory]")
                        || text.contains("[context_checkpoint]")
                    {
                        "[retrieved_memory] [已脱敏；不返回授权 memory 原文]".into()
                    } else {
                        text.lines()
                            .map(|line| {
                                let lower = line.to_ascii_lowercase();
                                if [
                                    "bearer ",
                                    "api_key",
                                    "api-key",
                                    "apikey",
                                    "token=",
                                    "token:",
                                    "password",
                                    "secret",
                                    "authorization",
                                    "sk-",
                                ]
                                .iter()
                                .any(|marker| lower.contains(marker))
                                    || secrets
                                        .iter()
                                        .any(|secret| !secret.is_empty() && line.contains(secret))
                                {
                                    "[已脱敏]"
                                } else {
                                    line
                                }
                            })
                            .collect::<Vec<_>>()
                            .join("\n")
                    },
                );
            }
            message
        })
        .collect()
}

/// schema/examples 与模型名称同样执行保守脱敏，不修改 capture 原文或摘要。
pub fn diagnostic_value(value: &mut serde_json::Value, secrets: &[String]) {
    match value {
        serde_json::Value::String(text) => {
            if let Some(content) =
                diagnostic_messages(&[Message::text(Role::System, &*text)], secrets)
                    .into_iter()
                    .next()
                    .and_then(|message| message.content)
            {
                *text = content;
            }
        }
        serde_json::Value::Object(fields) => {
            for (key, value) in fields {
                if [
                    "api_key",
                    "api-key",
                    "password",
                    "authorization",
                    "bearer",
                    "secret",
                    "token",
                ]
                .contains(&key.to_ascii_lowercase().as_str())
                {
                    *value = serde_json::Value::String("[已脱敏]".into());
                } else {
                    diagnostic_value(value, secrets);
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                diagnostic_value(value, secrets);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn input() -> ProviderRequestInput {
        ProviderRequestInput {
            purpose: ProviderRequestPurpose::Model,
            source: ContextSource {
                lifetime: SessionLifetimeId("life".into()),
                source_start: TranscriptSeq(0),
                source_end: TranscriptSeq(1),
                prefix_digest: crate::digest(&[]).unwrap(),
                generation: ProjectionGeneration(3),
                policy_fingerprint: "4096:12:60:85".into(),
                pressure_route: "primary".into(),
                summary_route: "primary".into(),
            },
            messages: vec![
                Message::text(Role::System, "stable"),
                Message::text(Role::Assistant, "旧历史"),
                Message::text(Role::System, "[retrieved_context] canonical plan/skill"),
                Message::user_with_images("任务", vec!["private-image".into()]),
                Message::text(Role::System, "[turn_overlay] branch=captured"),
            ],
            tools: vec![ToolSpec {
                name: "read_file".into(),
                description: "read".into(),
                parameters: serde_json::json!({"type":"object"}),
            }],
            catalog: vec![],
            route: "primary".into(),
            provider_identity: "provider-sha".into(),
            images: true,
            tool_calls: true,
            budget: 4096,
            calibration: None,
        }
    }
    #[test]
    fn captured_request_is_deterministic_partitioned_and_remeasured_for_fallback() {
        let primary = input();
        let request = assemble_request(&primary).unwrap();
        assert_eq!(
            request.request_digest,
            assemble_request(&primary).unwrap().request_digest
        );
        assert!(
            request.envelope.history_tokens > 0
                && request.envelope.retrieved_tokens > 0
                && request.envelope.overlay_tokens > 0
        );
        let mut fallback = primary.clone();
        fallback.route = "fallback".into();
        fallback.provider_identity = "alternate".into();
        fallback.images = false;
        fallback.tool_calls = false;
        let rebuilt = assemble_request(&fallback).unwrap();
        assert!(rebuilt.tools.is_empty());
        assert!(
            rebuilt
                .messages
                .iter()
                .all(|message| message.image_urls.is_empty())
        );
        assert!(
            rebuilt.envelope.calibrated_input_tokens < request.envelope.calibrated_input_tokens
        );
        assert_ne!(rebuilt.request_digest, request.request_digest);
        fallback.calibration = Some((3000, 100));
        assert_eq!(
            assemble_request(&fallback)
                .unwrap()
                .envelope
                .calibrated_input_tokens,
            rebuilt.envelope.calibrated_input_tokens + 2900
        );
        fallback.budget = 256;
        assert!(matches!(
            assemble_request(&fallback),
            Err(ContextError::RequestBudget)
        ));
        fallback = primary.clone();
        fallback.source.generation = ProjectionGeneration(4);
        assert_ne!(
            assemble_request(&fallback).unwrap().request_digest,
            request.request_digest
        );
        fallback = primary.clone();
        fallback.catalog = primary.tools.clone();
        assert_ne!(
            assemble_request(&fallback).unwrap().request_digest,
            request.request_digest
        );
        let mut skill_changed = primary.clone();
        skill_changed.messages[2].content = Some("[retrieved_context] 技能定义已经更新".into());
        assert_ne!(
            assemble_request(&skill_changed).unwrap().request_digest,
            request.request_digest
        );
        let mut summary = primary.clone();
        summary.purpose = ProviderRequestPurpose::CompactSummary;
        assert_ne!(
            assemble_request(&summary).unwrap().request_digest,
            request.request_digest
        );
        assert!(
            assemble_request(&summary)
                .unwrap()
                .omissions
                .iter()
                .any(|reason| reason.starts_with("summary_source:"))
        );
        fallback.source.policy_fingerprint = "8192:12:60:85".into();
        assert_ne!(
            assemble_request(&fallback).unwrap().request_digest,
            request.request_digest
        );
    }
    #[test]
    fn diagnostic_redacts_memory_credentials_media_and_arbitrary_schema_values() {
        let messages = vec![
            Message::text(Role::User, "Bearer confidential"),
            Message::text(Role::System, "[retrieved_memory] authorized-body"),
            Message::text(Role::Assistant, "opaque-value"),
        ];
        let clean = diagnostic_messages(&messages, &["opaque-value".into()]);
        let raw = serde_json::to_string(&clean).unwrap();
        assert!(
            !raw.contains("confidential")
                && !raw.contains("authorized-body")
                && !raw.contains("opaque-value")
        );
        let mut schema = serde_json::json!({"type":"object","properties":{"api_key":{"default":"raw-key"},"normal":{"example":"opaque-value"}},"description":"safe"});
        diagnostic_value(&mut schema, &["opaque-value".into()]);
        assert!(
            !schema.to_string().contains("raw-key") && !schema.to_string().contains("opaque-value")
        );
        assert_eq!(schema["description"], "safe");
    }
}
