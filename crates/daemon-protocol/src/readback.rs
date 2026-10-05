use crate::{ProtocolError, SessionReadback};
use serde_json::Value;

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
    if snapshot.metadata.key != snapshot.session_id
        || snapshot.metadata.lifetime != snapshot.session_lifetime_id
        || snapshot.metadata.deleted
        || snapshot.metadata.revision != snapshot.transcript_revision
        || snapshot.metadata_revision > snapshot.snapshot_revision
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

/// run.read 的兼容附加快照有独立 typed schema，不作为 RunRecord 的未知字段吞掉。
pub fn decode_run_readback(mut value: Value) -> Result<agent_core::RunRecord, ProtocolError> {
    if let Some(object) = value.as_object_mut() {
        if let Some(snapshot) = object.remove("snapshot") {
            if !snapshot.is_null() {
                serde_json::from_value::<agent_core::RunSnapshot>(snapshot)?;
            }
        }
    }
    Ok(serde_json::from_value(value)?)
}
