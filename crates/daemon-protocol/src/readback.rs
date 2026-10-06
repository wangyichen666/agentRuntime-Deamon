use crate::{ProtocolError, SessionReadback};
use serde_json::Value;

pub fn decode_provider_request(
    value: Value,
) -> Result<agent_core::ProviderRequestReadback, ProtocolError> {
    let result: agent_core::ProviderRequestReadback = serde_json::from_value(value)?;
    let digest = |text: &str| {
        text.len() == 64
            && text
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    };
    if result.schema_version != 1
        || result.capture_id.is_empty()
        || result.capture_id.len() > 256
        || result.round == 0
        || result.owner.run_generation.0 == 0
        || result.owner.run_id.0.is_empty()
        || result.owner.turn_id.0.is_empty()
        || result.owner.session_key.0.is_empty()
        || result.owner.session_lifetime_id != result.envelope.source.lifetime
        || result.replayability != "captured"
        || !digest(&result.policy_fingerprint)
        || !digest(&result.request_digest)
        || !digest(&result.message_digest)
        || !digest(&result.provider_tools_digest)
        || !digest(&result.envelope.tool_catalog_digest)
        || !digest(&result.envelope.source.prefix_digest)
        || result
            .envelope
            .calibrated_input_tokens
            .saturating_add(result.envelope.output_reserve)
            > result.envelope.budget
        || result.messages.is_some() != result.tools.is_some()
    {
        return Err(ProtocolError::InvalidParams(
            "Provider 请求读回身份、摘要或预算无效".into(),
        ));
    }
    Ok(result)
}

/// 兼容展示字段集中移除；其余 schema 字段必须严格匹配 canonical DTO。
pub fn decode_session_readback(mut value: Value) -> Result<SessionReadback, ProtocolError> {
    if let Some(object) = value.as_object_mut() {
        for field in [
            "active_requests",
            "pending_approvals",
            "status",
            "workspace",
            "capability_generation",
            "created",
            "resumed",
        ] {
            object.remove(field);
        }
    }
    let snapshot: SessionReadback = serde_json::from_value(value)?;
    if snapshot.schema_version != 1 {
        return Err(ProtocolError::InvalidParams(
            "未知 SessionSnapshot schema".into(),
        ));
    }
    if !snapshot.message_ids.is_empty()
        && (snapshot.message_ids.len() != snapshot.messages.len()
            || snapshot
                .message_ids
                .iter()
                .any(|id| id.is_empty() || id.len() > 512)
            || snapshot
                .message_ids
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != snapshot.message_ids.len())
    {
        return Err(ProtocolError::InvalidParams(
            "canonical message ids 不一致".into(),
        ));
    }
    if snapshot.metadata.key != snapshot.session_id
        || snapshot.metadata.lifetime != snapshot.session_lifetime_id
        || snapshot.metadata.deleted
        || snapshot.metadata.revision != snapshot.transcript_revision
        || snapshot.metadata_revision > snapshot.snapshot_revision
        || snapshot.run_owners.len() > 66
        || snapshot.run_owners.iter().any(|owner| {
            owner.session_key != snapshot.session_id
                || owner.session_lifetime_id != snapshot.session_lifetime_id
                || owner.run_generation.0 == 0
                || !snapshot
                    .active_runs
                    .iter()
                    .chain(snapshot.last_durable_terminal.iter())
                    .any(|run| run.run_id == owner.run_id && run.turn_id == owner.turn_id)
        })
        || snapshot
            .run_owners
            .iter()
            .map(|owner| &owner.run_id)
            .collect::<std::collections::HashSet<_>>()
            .len()
            != snapshot.run_owners.len()
        || snapshot.active_owner.as_ref().is_some_and(|owner| {
            owner.session_key != snapshot.session_id
                || owner.session_lifetime_id != snapshot.session_lifetime_id
                || !snapshot.active_runs.iter().any(|r| {
                    r.run_id == owner.run_id
                        && matches!(
                            r.status,
                            agent_core::RunStatus::Running
                                | agent_core::RunStatus::WaitingInteraction
                        )
                })
        })
        || snapshot
            .active_runs
            .iter()
            .any(|r| r.session_id != snapshot.session_id || r.status.terminal())
        || snapshot
            .last_durable_terminal
            .as_ref()
            .is_some_and(|r| r.session_id != snapshot.session_id || !r.status.terminal())
    {
        return Err(ProtocolError::InvalidParams(
            "SessionSnapshot owner/terminal 不一致".into(),
        ));
    }
    let mut last = 0;
    for (index, row) in snapshot.queue_rows.iter().enumerate() {
        if row.id <= last
            || row.position != (index + 1) as i64
            || row.session_id != snapshot.session_id
            || row.status != "queued"
            || !snapshot
                .active_runs
                .iter()
                .any(|r| r.run_id == row.run_id && r.status == agent_core::RunStatus::Queued)
        {
            return Err(ProtocolError::InvalidParams(
                "SessionSnapshot queue rows 不一致".into(),
            ));
        }
        last = row.id;
    }
    if snapshot.queue_cursor != snapshot.queue_rows.last().map(|r| r.id) {
        return Err(ProtocolError::InvalidParams(
            "SessionSnapshot queue cursor 不一致".into(),
        ));
    }
    match snapshot.history_mode {
        crate::HistoryReadMode::Canonical
            if snapshot.messages.len() as u64 != snapshot.transcript_revision.0 =>
        {
            return Err(ProtocolError::InvalidParams(
                "SessionSnapshot 历史 revision 不一致".into(),
            ));
        }
        crate::HistoryReadMode::Omitted
            if !snapshot.messages.is_empty()
                || !snapshot.omitted.iter().any(|field| field == "messages") =>
        {
            return Err(ProtocolError::InvalidParams(
                "SessionSnapshot 稀疏标记不一致".into(),
            ));
        }
        _ => {}
    }
    Ok(snapshot)
}

/// run.read 的兼容附加快照与 continuation 身份独立校验；其他未知字段仍拒绝。
pub fn decode_run_readback(mut value: Value) -> Result<agent_core::RunRecord, ProtocolError> {
    let view = value
        .as_object_mut()
        .and_then(|object| object.remove("_my_agent_view"))
        .map(serde_json::from_value::<agent_core::ViewStamp>)
        .transpose()?;
    if let Some(decision) = value
        .as_object_mut()
        .and_then(|object| object.remove("_my_agent_view_decision"))
    {
        if decision != "ignored" {
            return Err(ProtocolError::InvalidParams("未知展示决策".into()));
        }
    }
    let projection = value
        .as_object_mut()
        .and_then(|object| object.remove("projection_generation"))
        .map(serde_json::from_value::<agent_core::ProjectionGeneration>)
        .transpose()?;
    let transcript = value
        .as_object_mut()
        .and_then(|object| object.remove("transcript_revision"))
        .map(serde_json::from_value::<agent_core::TranscriptSeq>)
        .transpose()?;
    let compact = value
        .as_object_mut()
        .and_then(|object| object.remove("compact"))
        .map(serde_json::from_value::<agent_core::CompactRunReadback>)
        .transpose()?;
    if let Some(object) = value.as_object_mut() {
        if let Some(snapshot) = object.remove("snapshot") {
            if !snapshot.is_null() {
                serde_json::from_value::<agent_core::RunSnapshot>(snapshot)?;
            }
        }
        for field in ["continuation_parent_run_id", "continuation_run_id"] {
            if let Some(identity) = object.remove(field) {
                let run: Option<agent_core::RunId> = serde_json::from_value(identity)?;
                if run.as_ref().is_some_and(|run| run.0.is_empty()) {
                    return Err(ProtocolError::InvalidParams(
                        "continuation run 身份为空".into(),
                    ));
                }
            }
        }
    }
    let run: agent_core::RunRecord = serde_json::from_value(value)?;
    if view.as_ref().is_some_and(|stamp| {
        !stamp.valid()
            || stamp.owner.as_ref().is_none_or(|owner| {
                owner.run_id != run.run_id
                    || owner.turn_id != run.turn_id
                    || owner.session_key != run.session_id
            })
            || stamp.event_seq != Some(run.last_seq)
    }) {
        return Err(ProtocolError::InvalidParams("run 展示版本身份冲突".into()));
    }
    if let Some(compact) = compact {
        use agent_core::{CompactRunOutcome as O, RunStatus as S};
        let status = match compact.outcome {
            O::Started => S::Running,
            O::Committed | O::NoGain => S::Completed,
            O::Rejected | O::Failed => S::Failed,
            O::Cancelled => S::Cancelled,
            O::Unknown => S::UnknownAfterRestart,
        };
        if projection.is_some_and(|generation| {
            generation
                != compact
                    .result_generation
                    .unwrap_or(compact.source.generation)
        }) || transcript.is_some_and(|revision| revision != compact.source.source_end)
        {
            return Err(ProtocolError::InvalidParams(
                "compact 附加版本与 receipt 不一致".into(),
            ));
        }
        if compact.schema_version != 1
            || run.kind != agent_core::RunKind::Compact
            || compact.owner.run_id != run.run_id
            || compact.owner.turn_id != run.turn_id
            || compact.owner.session_key != run.session_id
            || compact.owner.session_lifetime_id != compact.source.lifetime
            || view
                .as_ref()
                .is_some_and(|stamp| stamp.owner.as_ref() != Some(&compact.owner))
            || status != run.status
        {
            return Err(ProtocolError::InvalidParams(
                "compact readback 身份或终态冲突".into(),
            ));
        }
    } else if projection.is_some() || transcript.is_some() {
        return Err(ProtocolError::InvalidParams(
            "普通 run 不接受 compact 版本字段".into(),
        ));
    } else if run.kind == agent_core::RunKind::Compact {
        return Err(ProtocolError::InvalidParams(
            "compact run 缺 source/receipt".into(),
        ));
    }
    Ok(run)
}

/// 分页仅补充同一事务版本的展示片段，不能冒充完整 transcript。
pub fn decode_session_page(mut value: Value) -> Result<(SessionReadback, String), ProtocolError> {
    let bad = || ProtocolError::InvalidParams("SessionPage cursor/revision 不一致".into());
    let offset = value
        .get("offset")
        .and_then(Value::as_u64)
        .ok_or_else(bad)?;
    let cursor = value
        .get("cursor")
        .and_then(Value::as_u64)
        .ok_or_else(bad)?;
    let total = value
        .get("total_messages")
        .and_then(Value::as_u64)
        .ok_or_else(bad)?;
    let limit = value.get("limit").and_then(Value::as_u64).ok_or_else(bad)?;
    let messages: Vec<agent_core::Message> =
        serde_json::from_value(value.get("messages").cloned().ok_or_else(bad)?)?;
    if offset.checked_add(messages.len() as u64) != Some(cursor)
        || cursor > total
        || !(1..=1000).contains(&limit)
        || value.get("transcript_revision").and_then(Value::as_u64) != Some(total)
        || value.get("history_mode").and_then(Value::as_str) != Some("canonical")
        || value.get("has_more").and_then(Value::as_bool).is_none()
    {
        return Err(bad());
    }
    if let Some(ids) = value.get("message_ids") {
        let ids: Vec<String> = serde_json::from_value(ids.clone())?;
        if !ids.is_empty()
            && (ids.len() != messages.len()
                || ids.iter().any(|id| id.is_empty() || id.len() > 512)
                || ids.iter().collect::<std::collections::BTreeSet<_>>().len() != ids.len())
        {
            return Err(bad());
        }
    }
    let object = value.as_object_mut().ok_or_else(bad)?;
    object.remove("message_ids");
    for field in ["offset", "cursor", "total_messages", "limit", "has_more"] {
        object.remove(field);
    }
    object.insert("messages".into(), serde_json::json!([]));
    object.insert("history_mode".into(), serde_json::json!("omitted"));
    object.insert(
        "omitted".into(),
        serde_json::json!(["messages", "batch_ranges"]),
    );
    Ok((
        decode_session_readback(value)?,
        format!("{offset}:{cursor}:{total}"),
    ))
}

/// 本地展示 resync 通知携带已归约的事务快照，不代表业务终态。
pub fn decode_view_sync(
    data: Value,
) -> Result<(crate::RecoverySnapshot, Option<agent_core::RunRecord>), ProtocolError> {
    let value = data
        .get("snapshot")
        .cloned()
        .ok_or_else(|| ProtocolError::InvalidParams("resync 缺快照".into()))?;
    let (canonical, _) = decode_session_page(value.clone())?;
    let snapshot: crate::RecoverySnapshot = serde_json::from_value(value)?;
    if snapshot.session_id != canonical.session_id.0
        || snapshot.active_requests
            != canonical
                .active_runs
                .iter()
                .map(|run| run.request_id.clone())
                .collect::<Vec<_>>()
    {
        return Err(ProtocolError::InvalidParams("resync 活动身份不一致".into()));
    }
    let terminal: Option<agent_core::RunRecord> =
        serde_json::from_value(data.get("run").cloned().unwrap_or(Value::Null))?;
    if terminal.as_ref().is_some_and(|run| {
        !run.status.terminal()
            || canonical.last_durable_terminal.as_ref().is_none_or(|fact| {
                serde_json::to_value(fact).ok() != serde_json::to_value(run).ok()
            })
    }) {
        return Err(ProtocolError::InvalidParams(
            "resync terminal 缺事务证明".into(),
        ));
    }
    Ok((snapshot, terminal))
}
