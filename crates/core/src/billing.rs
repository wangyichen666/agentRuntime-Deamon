//! 用量/费用是原 Provider、tool、resource 事实的事务内账本，不拥有执行状态。
use crate::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelPrice {
    /// 每百万 token 的货币微单位；避免浮点价格和汇率污染准入。
    pub input_per_million: u64,
    pub output_per_million: u64,
    pub cache_read_per_million: u64,
    pub cache_creation_per_million: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BillingLimits {
    pub max_rounds: u32,
    pub max_provider_attempts: u32,
    pub max_input_tokens: u64,
    pub max_output_tokens_per_attempt: u64,
    pub max_tool_calls: u64,
    pub max_sandbox_wall_ms: u64,
    pub max_sandbox_wall_ms_per_resource: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", content = "model", rename_all = "snake_case")]
pub enum BillingDimension {
    Tenant,
    Project,
    Agent,
    Model(String),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BillingBudget {
    pub dimension: BillingDimension,
    pub hard_micros: u64,
    pub soft_micros: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BillingPolicy {
    pub version: String,
    pub currency: String,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub models: BTreeMap<String, ModelPrice>,
    pub tool_call_micros: u64,
    pub sandbox_per_million_ms: u64,
    pub limits: BillingLimits,
    pub budgets: Vec<BillingBudget>,
}
fn bounded(value: u64) -> bool {
    value <= i64::MAX as u64
}
impl BillingPolicy {
    pub fn validate(&self) -> Result<(), LedgerError> {
        let limits = &self.limits;
        let mut dimensions = BTreeSet::new();
        if self.version.is_empty() || self.version.len()>128 || self.currency.len()!=3 || !self.currency.bytes().all(|v|v.is_ascii_uppercase())
            || self.window_start_ms>=self.window_end_ms || self.models.is_empty() || self.models.len()>64
            || !(1..=64).contains(&limits.max_rounds) || !(1..=256).contains(&limits.max_provider_attempts) || !(1..=16_777_216).contains(&limits.max_input_tokens)
            || !(1..=32768).contains(&limits.max_output_tokens_per_attempt) || limits.max_tool_calls>1024 || limits.max_sandbox_wall_ms>86_400_000
            || limits.max_sandbox_wall_ms_per_resource>limits.max_sandbox_wall_ms
            || !bounded(self.tool_call_micros) || !bounded(self.sandbox_per_million_ms) || self.budgets.is_empty() || self.budgets.len()>67
            || self.models.iter().any(|(model,p)|model.is_empty() || model.len()>256 || [p.input_per_million,p.output_per_million,p.cache_read_per_million,p.cache_creation_per_million].into_iter().any(|v|!bounded(v)))
            || self.budgets.iter().any(|b|!bounded(b.hard_micros) || b.soft_micros>b.hard_micros || !dimensions.insert(b.dimension.clone()) || matches!(&b.dimension,BillingDimension::Model(model) if !self.models.contains_key(model))) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        if ![
            BillingDimension::Tenant,
            BillingDimension::Project,
            BillingDimension::Agent,
        ]
        .into_iter()
        .all(|d| dimensions.contains(&d))
            || self
                .models
                .keys()
                .any(|m| !dimensions.contains(&BillingDimension::Model(m.clone())))
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        self.maximum_cost()?;
        Ok(())
    }
    pub fn maximum_cost(&self) -> Result<u64, LedgerError> {
        let input = self
            .models
            .values()
            .map(|p| {
                p.input_per_million
                    .saturating_add(p.cache_read_per_million)
                    .saturating_add(p.cache_creation_per_million)
            })
            .max()
            .unwrap_or(0);
        let output = self
            .models
            .values()
            .map(|p| p.output_per_million)
            .max()
            .unwrap_or(0);
        let cost = micros(self.limits.max_input_tokens,input)? as u128
            + micros(self.limits.max_output_tokens_per_attempt.checked_mul(self.limits.max_provider_attempts.into()).ok_or(CoordinationError::InvalidRequest)?,output)? as u128
            + self.tool_call_micros as u128*self.limits.max_tool_calls as u128
            + micros(self.limits.max_sandbox_wall_ms,self.sandbox_per_million_ms)? as u128
            // 分项、分attempt向上取整的最坏误差也预占。
            + if input>0 || output>0 {self.limits.max_provider_attempts as u128*4} else {0};
        u64::try_from(cost)
            .ok()
            .filter(|v| bounded(*v))
            .ok_or(CoordinationError::InvalidRequest.into())
    }
    pub fn provider_cost(
        &self,
        model: &str,
        api: ApiType,
        usage: &ProviderUsage,
    ) -> Result<Option<u64>, LedgerError> {
        let p = self.models.get(model).ok_or(CoordinationError::Forbidden)?;
        let (Some(input), Some(output)) = (usage.input_tokens, usage.output_tokens) else {
            return Ok(None);
        };
        let read = usage.cache_read_tokens.unwrap_or(0);
        let create = usage.cache_creation_tokens.unwrap_or(0);
        let uncached = if api == ApiType::OpenaiChat {
            input.checked_sub(read).ok_or(CoordinationError::Corrupt)?
        } else {
            input
        };
        let sum = micros(uncached, p.input_per_million)? as u128
            + micros(output, p.output_per_million)? as u128
            + micros(read, p.cache_read_per_million)? as u128
            + micros(create, p.cache_creation_per_million)? as u128;
        Ok(Some(
            u64::try_from(sum)
                .ok()
                .filter(|v| bounded(*v))
                .ok_or(CoordinationError::InvalidRequest)?,
        ))
    }
}
pub fn micros(units: u64, per_million: u64) -> Result<u64, LedgerError> {
    let amount = (units as u128 * per_million as u128).div_ceil(1_000_000);
    u64::try_from(amount)
        .ok()
        .filter(|v| bounded(*v))
        .ok_or(CoordinationError::InvalidRequest.into())
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenBillingPolicy {
    pub digest: String,
    pub policy: BillingPolicy,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageCharge {
    pub seq: u64,
    pub scope: TenantContext,
    pub owner: ExactOwner,
    pub operation_id: String,
    pub model: Option<String>,
    pub kind: String,
    pub units: serde_json::Value,
    pub policy_digest: Option<String>,
    pub currency: Option<String>,
    pub cost_micros: Option<u64>,
    pub outcome_unknown: bool,
    pub created_at_ms: i64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsagePage {
    pub entries: Vec<UsageCharge>,
    pub next_seq: u64,
    pub has_more: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BillingAccount {
    pub dimension: BillingDimension,
    pub currency: String,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub hard_micros: u64,
    pub soft_micros: u64,
    pub reserved_micros: u64,
    pub consumed_micros: u64,
    pub revision: u64,
}
