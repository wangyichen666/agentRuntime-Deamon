//! 发布协调领域状态；不会创建 session/run 的第二事实。
use crate::{
    EnterpriseOwnerEnvelope, EventSeq, RunId, SessionKey, SessionLifetimeId, TenantContext,
    ViewStamp,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryLeaseKey {
    pub scope: TenantContext,
    pub session_key: SessionKey,
    pub session_lifetime_id: SessionLifetimeId,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryLease {
    pub key: DeliveryLeaseKey,
    pub owner_id: String,
    pub epoch: u64,
    pub expires_at_ms: i64,
}
impl DeliveryLease {
    pub fn acquire(
        previous: Option<&Self>,
        key: &DeliveryLeaseKey,
        owner_id: &str,
        now: i64,
        ttl_ms: i64,
    ) -> Result<Self, CoordinationError> {
        if owner_id.is_empty()
            || owner_id.len() > 128
            || key.session_key.0.is_empty()
            || key.session_key.0.len() > 1024
            || key.session_lifetime_id.0.is_empty()
            || key.session_lifetime_id.0.len() > 128
            || !(100..=60_000).contains(&ttl_ms)
        {
            return Err(CoordinationError::InvalidRequest);
        }
        let epoch = match previous {
            Some(previous) if previous.key != *key => return Err(CoordinationError::Forbidden),
            Some(previous) if previous.expires_at_ms > now && previous.owner_id != owner_id => {
                return Err(CoordinationError::LeaseBusy);
            }
            Some(previous) if previous.expires_at_ms > now && previous.owner_id == owner_id => {
                previous.epoch
            }
            Some(previous) => previous
                .epoch
                .checked_add(1)
                .filter(|e| *e <= i64::MAX as u64)
                .ok_or(CoordinationError::Conflict)?,
            None => 1,
        };
        Ok(Self {
            key: key.clone(),
            owner_id: owner_id.into(),
            epoch,
            expires_at_ms: now
                .checked_add(ttl_ms)
                .ok_or(CoordinationError::InvalidRequest)?,
        })
    }
    pub fn fence(&self, candidate: &Self, now: i64) -> Result<(), CoordinationError> {
        if self.key != candidate.key
            || self.owner_id != candidate.owner_id
            || self.epoch != candidate.epoch
            || self.expires_at_ms <= now
        {
            return Err(CoordinationError::StaleLease);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxEvent {
    pub envelope: EnterpriseOwnerEnvelope,
    pub event_seq: EventSeq,
    pub event: String,
    pub data: serde_json::Value,
    pub stamp: ViewStamp,
}
impl OutboxEvent {
    pub fn lease_key(&self) -> DeliveryLeaseKey {
        DeliveryLeaseKey {
            scope: self.envelope.scope.clone(),
            session_key: self.envelope.owner.session_key.clone(),
            session_lifetime_id: self.envelope.owner.session_lifetime_id.clone(),
        }
    }
    pub fn valid(&self) -> bool {
        !self.event.is_empty()
            && self.event.len() <= 128
            && self.event_seq.0 > 0
            && self.data.is_object()
            && self.stamp.valid()
            && self.stamp.owner.as_ref() == Some(&self.envelope.owner)
            && self.stamp.event_seq == Some(self.event_seq)
    }
}
/// 原SQL行的身份投影；仅核列与封存JSON，不保存另一份发布状态。
#[derive(Debug)]
pub struct OutboxRowBinding {
    pub lease_key: DeliveryLeaseKey,
    pub run_id: RunId,
    pub event_seq: EventSeq,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxRecord {
    pub cursor: u64,
    pub event_id: String,
    pub event: OutboxEvent,
    pub payload_digest: String,
    pub delivered: bool,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxSummary {
    pub cursor: u64,
    pub event_id: String,
    pub envelope: EnterpriseOwnerEnvelope,
    pub event_seq: EventSeq,
    pub event: String,
    pub payload_digest: String,
    pub delivered: bool,
}
impl From<OutboxRecord> for OutboxSummary {
    fn from(record: OutboxRecord) -> Self {
        Self {
            cursor: record.cursor,
            event_id: record.event_id,
            envelope: record.event.envelope,
            event_seq: record.event.event_seq,
            event: record.event.event,
            payload_digest: record.payload_digest,
            delivered: record.delivered,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxReadback {
    pub records: Vec<OutboxSummary>,
    pub cursor: u64,
    pub has_more: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
pub enum CoordinationError {
    #[error("存储服务不可用")]
    StorageUnavailable,
    #[error("持久操作内容冲突")]
    Conflict,
    #[error("租约正由另一 owner 持有")]
    LeaseBusy,
    #[error("租约 epoch 已过期")]
    StaleLease,
    #[error("无权访问此域")]
    Forbidden,
    #[error("协调请求无效或超出预算")]
    InvalidRequest,
    #[error("持久协调记录校验失败")]
    Corrupt,
    #[error("数据库 schema 比当前程序新")]
    SchemaTooNew,
}
impl CoordinationError {
    pub fn from_rpc_code(code: i64) -> Option<Self> {
        match code {
            -32041 => Some(Self::Forbidden),
            -32050 => Some(Self::Conflict),
            -32051 => Some(Self::LeaseBusy),
            -32052 => Some(Self::StaleLease),
            -32053 => Some(Self::StorageUnavailable),
            -32054 => Some(Self::Corrupt),
            -32055 => Some(Self::SchemaTooNew),
            // -32602 是共享 JSON-RPC 参数错误，不能猜测其领域。
            _ => None,
        }
    }
    pub fn http_status(self) -> u16 {
        match self {
            Self::InvalidRequest => 400,
            Self::Forbidden => 403,
            Self::Conflict | Self::StaleLease => 409,
            Self::LeaseBusy => 429,
            Self::StorageUnavailable => 503,
            Self::Corrupt | Self::SchemaTooNew => 500,
        }
    }
    pub fn rpc_code(self) -> i64 {
        match self {
            Self::InvalidRequest => -32602,
            Self::Forbidden => -32041,
            Self::Conflict => -32050,
            Self::LeaseBusy => -32051,
            Self::StaleLease => -32052,
            Self::StorageUnavailable => -32053,
            Self::Corrupt => -32054,
            Self::SchemaTooNew => -32055,
        }
    }
    pub fn retryable(self) -> bool {
        matches!(self, Self::LeaseBusy | Self::StorageUnavailable)
    }
    pub fn recovery_action(self) -> &'static str {
        match self {
            Self::InvalidRequest => "correct_request",
            Self::Forbidden => "contact_administrator",
            Self::Conflict => "read_existing_operation",
            Self::LeaseBusy => "wait_for_owner",
            Self::StaleLease => "stop_old_writer_and_readback",
            Self::StorageUnavailable => "restore_storage_then_readback",
            Self::Corrupt => "verify_and_restore_backup",
            Self::SchemaTooNew => "use_compatible_binary",
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lease_epoch_fences_takeover_and_expired_same_owner_without_wraparound() {
        let key = DeliveryLeaseKey {
            scope: TenantContext::local(),
            session_key: SessionKey("s".into()),
            session_lifetime_id: SessionLifetimeId("life".into()),
        };
        let first = DeliveryLease::acquire(None, &key, "worker-1", 100, 100).unwrap();
        assert_eq!(
            DeliveryLease::acquire(Some(&first), &key, "worker-2", 199, 100),
            Err(CoordinationError::LeaseBusy)
        );
        let renewed = DeliveryLease::acquire(Some(&first), &key, "worker-1", 199, 100).unwrap();
        assert_eq!(renewed.epoch, 1);
        assert!(renewed.fence(&first, 200).is_ok());
        let takeover = DeliveryLease::acquire(Some(&renewed), &key, "worker-2", 299, 100).unwrap();
        assert_eq!(takeover.epoch, 2);
        assert_eq!(
            takeover.fence(&first, 299),
            Err(CoordinationError::StaleLease)
        );
        let mut exhausted = takeover;
        exhausted.epoch = i64::MAX as u64;
        assert_eq!(
            DeliveryLease::acquire(Some(&exhausted), &key, "worker-2", 400, 100),
            Err(CoordinationError::Conflict)
        );
    }
}
