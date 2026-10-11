use crate::{CoordinationError, ExactOwner, ResourceId};
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourceRecord {
    pub id: ResourceId,
    pub owner: ExactOwner,
    pub state: String,
    pub cwd: String,
    pub sandbox_requested: String,
    pub sandbox_effective: String,
    pub process_identity: Option<String>,
    pub log_cursor: u64,
    pub terminal_reason: Option<String>,
}

/// Local 与 PostgreSQL 共用原后台资源状态规则；失联只收敛为 orphaned。
pub fn valid_resource_state(state: &str) -> bool {
    matches!(
        state,
        "starting" | "running" | "stopping" | "completed" | "failed" | "stopped" | "orphaned"
    )
}

pub fn resource_transition_allowed(current: &str, next: &str) -> bool {
    match current {
        "starting" => matches!(next, "running" | "failed" | "orphaned"),
        "running" => matches!(
            next,
            "stopping" | "completed" | "failed" | "stopped" | "orphaned"
        ),
        "stopping" => matches!(next, "stopped" | "completed" | "failed" | "orphaned"),
        state if valid_resource_state(state) => state == next,
        _ => false,
    }
}

/// 日志永久最多 64 KiB；保留完整 UTF-8 字符，满额后沿用已有游标。
pub fn bounded_resource_log(
    content: &str,
    stored_bytes: u64,
) -> Result<Option<&str>, CoordinationError> {
    let remaining = 65536u64
        .checked_sub(stored_bytes)
        .ok_or(CoordinationError::Corrupt)?;
    if remaining == 0 {
        return Ok(None);
    }
    let mut end = content.len().min(remaining as usize);
    while !content.is_char_boundary(end) {
        end -= 1;
    }
    if end == 0 {
        return Ok(None);
    }
    Ok(Some(&content[..end]))
}

pub fn resource_reconcile_reason(state: &str, evidence: &str) -> Result<String, CoordinationError> {
    let prefix = "manual reconcile: ";
    if !matches!(state, "completed" | "failed" | "stopped")
        || evidence.trim().is_empty()
        || evidence.len() > 4096 - prefix.len()
    {
        return Err(CoordinationError::InvalidRequest);
    }
    Ok(format!("{prefix}{evidence}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_terminal_and_orphaned_cannot_restart() {
        for terminal in ["completed", "failed", "stopped", "orphaned"] {
            assert!(resource_transition_allowed(terminal, terminal));
            for active in ["starting", "running", "stopping"] {
                assert!(!resource_transition_allowed(terminal, active));
            }
        }
        assert!(resource_transition_allowed("running", "orphaned"));
        assert!(!resource_transition_allowed("starting", "completed"));
        assert!(!resource_transition_allowed("invalid", "invalid"));
    }

    #[test]
    fn resource_log_budget_preserves_unicode_and_detects_corruption() {
        assert_eq!(bounded_resource_log("你好", 65533), Ok(Some("你")));
        assert_eq!(bounded_resource_log("你好", 65534), Ok(None));
        assert_eq!(bounded_resource_log("", 0), Ok(None));
        assert_eq!(bounded_resource_log("x", 65536), Ok(None));
        assert_eq!(
            bounded_resource_log("x", 65537),
            Err(CoordinationError::Corrupt)
        );
    }

    #[test]
    fn resource_reconcile_requires_bounded_evidence_and_terminal() {
        assert!(resource_reconcile_reason("running", "已核验").is_err());
        assert!(resource_reconcile_reason("stopped", "  ").is_err());
        assert!(resource_reconcile_reason("stopped", &"x".repeat(4096)).is_err());
        assert_eq!(
            resource_reconcile_reason("stopped", "已核验"),
            Ok("manual reconcile: 已核验".into())
        );
    }
}
