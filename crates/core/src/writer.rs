//! Session native writer 的租约凭据；与发布 delivery lease 使用不同类型和持久 owner。
use crate::*;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionWriterLeaseKey {
    pub scope: TenantContext,
    pub session_key: SessionKey,
    pub session_lifetime_id: SessionLifetimeId,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionWriterLease {
    pub key: SessionWriterLeaseKey,
    pub owner_id: String,
    pub epoch: u64,
    pub expires_at_ms: i64,
}
impl SessionWriterLeaseKey {
    fn coordination_key(&self) -> DeliveryLeaseKey {
        DeliveryLeaseKey {
            scope: self.scope.clone(),
            session_key: self.session_key.clone(),
            session_lifetime_id: self.session_lifetime_id.clone(),
        }
    }
}
impl SessionWriterLease {
    fn coordination_lease(&self) -> DeliveryLease {
        DeliveryLease {
            key: self.key.coordination_key(),
            owner_id: self.owner_id.clone(),
            epoch: self.epoch,
            expires_at_ms: self.expires_at_ms,
        }
    }
    pub fn acquire(
        previous: Option<&Self>,
        key: &SessionWriterLeaseKey,
        owner: &str,
        now: i64,
        ttl_ms: i64,
    ) -> Result<Self, CoordinationError> {
        let previous = previous.map(Self::coordination_lease);
        let granted = DeliveryLease::acquire(
            previous.as_ref(),
            &key.coordination_key(),
            owner,
            now,
            ttl_ms,
        )?;
        Ok(Self {
            key: key.clone(),
            owner_id: granted.owner_id,
            epoch: granted.epoch,
            expires_at_ms: granted.expires_at_ms,
        })
    }
    pub fn fence(&self, candidate: &Self, now: i64) -> Result<(), CoordinationError> {
        self.coordination_lease()
            .fence(&candidate.coordination_lease(), now)
    }
}
