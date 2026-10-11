//! 审计链和配额领域值；共享封存格式由存储端口实现。
use crate::{CoordinationError, RunId, SubjectId, TenantContext};
use serde::{Deserialize, Serialize};
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
pub enum LedgerError {
    #[error("访问速率或并发预算已耗尽")]
    RateLimited,
    #[error("配额已耗尽")]
    QuotaExceeded,
    #[error("配额窗口已关闭")]
    WindowClosed,
    #[error(transparent)]
    Coordination(#[from] CoordinationError),
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditCommand {
    pub scope: TenantContext,
    pub operation_id: String,
    pub subject_id: SubjectId,
    pub action: String,
    pub resource: serde_json::Value,
    pub outcome: String,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditEntry {
    pub seq: u64,
    pub command: AuditCommand,
    pub previous_digest: String,
    pub digest: String,
    pub created_at_ms: i64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaAccount {
    pub scope: TenantContext,
    pub metric: String,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub budget: u64,
    pub consumed: u64,
    pub reserved: u64,
    pub revision: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaState {
    Reserved,
    Consumed,
    Released,
}
impl QuotaState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reserved => "reserved",
            Self::Consumed => "consumed",
            Self::Released => "released",
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaReservation {
    pub scope: TenantContext,
    pub operation_id: String,
    pub metric: String,
    pub window_start_ms: i64,
    pub amount: u64,
    pub run_id: RunId,
    pub state: QuotaState,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl LedgerError {
    pub fn rpc_code(self) -> i64 {
        match self {
            Self::RateLimited => -32046,
            Self::QuotaExceeded => -32044,
            Self::WindowClosed => -32045,
            Self::Coordination(e) => e.rpc_code(),
        }
    }
    pub fn http_status(self) -> u16 {
        match self {
            Self::QuotaExceeded | Self::WindowClosed | Self::RateLimited => 429,
            Self::Coordination(e) => e.http_status(),
        }
    }
    pub fn recovery_action(self) -> &'static str {
        match self {
            Self::RateLimited => "wait_for_access_budget",
            Self::QuotaExceeded => "increase_budget_or_wait",
            Self::WindowClosed => "configure_quota_window",
            Self::Coordination(e) => e.recovery_action(),
        }
    }
}
pub fn quota_metric_valid(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}
/// 两后端使用同一检查，避免饱和算术使配额超卖。
pub fn check_quota_reservation(account: &QuotaAccount, amount: u64) -> Result<(), LedgerError> {
    if amount == 0
        || amount > i64::MAX as u64
        || !quota_metric_valid(&account.metric)
        || account.window_start_ms >= account.window_end_ms
    {
        return Err(CoordinationError::InvalidRequest.into());
    }
    if account
        .consumed
        .checked_add(account.reserved)
        .and_then(|n| n.checked_add(amount))
        .is_none_or(|n| n > account.budget)
    {
        return Err(LedgerError::QuotaExceeded);
    }
    Ok(())
}
