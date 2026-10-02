use serde::{Deserialize, Serialize};
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolEffect {
    Read,
    Write,
    External,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AccessMode {
    Read,
    Write,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourceAccess {
    pub key: String,
    pub mode: AccessMode,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayPolicy {
    Safe,
    ReceiptOnly,
    Never,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolDescriptor {
    pub effect: ToolEffect,
    pub resources: Vec<ResourceAccess>,
    pub approval_required: bool,
    pub replay: ReplayPolicy,
    pub cancellation_join: bool,
    pub background: bool,
    pub output_budget: usize,
}
impl ToolDescriptor {
    pub fn read() -> Self {
        Self {
            effect: ToolEffect::Read,
            resources: vec![],
            approval_required: false,
            replay: ReplayPolicy::Safe,
            cancellation_join: true,
            background: false,
            output_budget: 65536,
        }
    }
    pub fn external() -> Self {
        Self {
            effect: ToolEffect::External,
            resources: vec![],
            approval_required: true,
            replay: ReplayPolicy::Never,
            cancellation_join: true,
            background: false,
            output_budget: 65536,
        }
    }
    pub fn conflicts(&self, other: &Self) -> bool {
        self.effect == ToolEffect::External
            || other.effect == ToolEffect::External
            || self.resources.iter().any(|a| {
                other.resources.iter().any(|b| {
                    a.key == b.key && (a.mode == AccessMode::Write || b.mode == AccessMode::Write)
                })
            })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("外部副作用结果未知：{0}；禁止自动重放")]
pub struct ToolOutcomeUnknown(pub String);
