use serde::{Deserialize, Serialize};

use crate::DomainError;

macro_rules! string_id {
    ($($name:ident),+ $(,)?) => {$(
        #[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);
    )+};
}

string_id!(
    SessionKey,
    SessionLifetimeId,
    RunId,
    TurnId,
    MessageId,
    InteractionId
);

macro_rules! sequence {
    ($($name:ident),+ $(,)?) => {$(
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub u64);
    )+};
}

sequence!(
    EventSeq,
    TranscriptSeq,
    ResourceId,
    RunGeneration,
    ProjectionGeneration
);

impl RunGeneration {
    pub fn successor(self) -> Result<Self, DomainError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(DomainError::GenerationExhausted("run"))
    }
}

impl ProjectionGeneration {
    pub fn successor(self) -> Result<Self, DomainError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(DomainError::GenerationExhausted("projection"))
    }
}

/// 后续 repository 必须在事务或持久写锁内调用 fence。
/// 此值不分配 lifetime、不持有 permit，也不证明调用方具有会话授权。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactOwner {
    pub session_key: SessionKey,
    pub session_lifetime_id: SessionLifetimeId,
    pub run_id: RunId,
    pub run_generation: RunGeneration,
    pub turn_id: TurnId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OwnerDimension {
    SessionKey,
    SessionLifetime,
    Run,
    RunGeneration,
    Turn,
}

impl ExactOwner {
    /// `self` 为当前事实的 owner，`candidate` 为迟到写入/回调携带的 owner。
    pub fn fence(&self, candidate: &Self) -> Result<(), DomainError> {
        let dimensions = [
            (
                self.session_key == candidate.session_key,
                OwnerDimension::SessionKey,
            ),
            (
                self.session_lifetime_id == candidate.session_lifetime_id,
                OwnerDimension::SessionLifetime,
            ),
            (self.run_id == candidate.run_id, OwnerDimension::Run),
            (
                self.run_generation == candidate.run_generation,
                OwnerDimension::RunGeneration,
            ),
            (self.turn_id == candidate.turn_id, OwnerDimension::Turn),
        ];
        for (matches, dimension) in dimensions {
            if !matches {
                return Err(DomainError::StaleOwner(dimension));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owner() -> ExactOwner {
        ExactOwner {
            session_key: SessionKey("公开会话".into()),
            session_lifetime_id: SessionLifetimeId("内部生命周期-1".into()),
            run_id: RunId("run-1".into()),
            run_generation: RunGeneration(1),
            turn_id: TurnId("turn-1".into()),
        }
    }

    #[test]
    fn exact_owner_accepts_only_all_matching_dimensions() {
        let current = owner();
        assert_eq!(current.fence(&current), Ok(()));
        type OwnerChange = (OwnerDimension, fn(&mut ExactOwner));
        let changes: [OwnerChange; 5] = [
            (OwnerDimension::SessionKey, |o| o.session_key.0.push('2')),
            (OwnerDimension::SessionLifetime, |o| {
                o.session_lifetime_id.0.push('2')
            }),
            (OwnerDimension::Run, |o| o.run_id.0.push('2')),
            (OwnerDimension::RunGeneration, |o| o.run_generation.0 += 1),
            (OwnerDimension::Turn, |o| o.turn_id.0.push('2')),
        ];
        for (dimension, change) in changes {
            let mut stale = current.clone();
            change(&mut stale);
            assert_eq!(
                current.fence(&stale),
                Err(DomainError::StaleOwner(dimension))
            );
        }
    }

    #[test]
    fn reused_public_key_does_not_authorize_old_lifetime() {
        let stale = owner();
        let mut current = stale.clone();
        current.session_lifetime_id = SessionLifetimeId("重建生命周期".into());
        assert_eq!(
            current.fence(&stale),
            Err(DomainError::StaleOwner(OwnerDimension::SessionLifetime))
        );
    }

    #[test]
    fn generations_never_wrap_and_reauthorize_stale_work() {
        assert_eq!(RunGeneration(1).successor(), Ok(RunGeneration(2)));
        assert_eq!(
            ProjectionGeneration(1).successor(),
            Ok(ProjectionGeneration(2))
        );
        assert!(RunGeneration(u64::MAX).successor().is_err());
        assert!(ProjectionGeneration(u64::MAX).successor().is_err());
    }

    #[test]
    fn exact_owner_round_trip_preserves_internal_identity() {
        let current = owner();
        let value = serde_json::to_value(&current).unwrap();
        assert_eq!(
            serde_json::from_value::<ExactOwner>(value.clone()).unwrap(),
            current
        );
        let mut unknown = value;
        unknown["unexpected_owner"] = serde_json::json!(true);
        assert!(serde_json::from_value::<ExactOwner>(unknown).is_err());
    }

    #[test]
    fn legacy_ids_keep_transparent_wire_representation() {
        assert_eq!(
            serde_json::to_value(SessionKey("session.jsonl".into())).unwrap(),
            "session.jsonl"
        );
        assert_eq!(
            serde_json::to_value(RunId("run-1".into())).unwrap(),
            "run-1"
        );
        assert_eq!(serde_json::to_value(EventSeq(7)).unwrap(), 7);
        assert_eq!(serde_json::to_value(ResourceId(7)).unwrap(), 7);
    }
}
