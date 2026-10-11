//! 进程内预检；跨进程预算仍由 canonical AccessRepository 同事务仲裁。
use std::{collections::HashMap, time::{Duration, Instant}};
use agent_core::{SubjectId, TenantContext};

struct Bucket { tokens: f64, updated: Instant, used: Instant }
#[derive(Default)]
pub(crate) struct RateLimiter { buckets: HashMap<String, Bucket> }
impl RateLimiter {
    pub(crate) fn admit(&mut self, scope: &TenantContext, subject: &SubjectId, endpoint: &str, now: Instant) -> bool {
        let tenant = serde_json::to_string(&(&scope.organization_id, &scope.tenant_id)).expect("标识可序列化");
        let subject = serde_json::to_string(&(tenant.clone(), subject)).expect("标识可序列化");
        let endpoint = serde_json::to_string(&(tenant.clone(), endpoint)).expect("标识可序列化");
        let dimensions = [(format!("t:{tenant}"), 600.0), (format!("s:{subject}"), 120.0), (format!("e:{endpoint}"), 300.0)];
        self.buckets.retain(|_, b| now.saturating_duration_since(b.used) < Duration::from_secs(3600));
        let missing = dimensions.iter().filter(|(key, _)| !self.buckets.contains_key(key)).count();
        if self.buckets.len() + missing > 8192 { return false; }
        let available: Vec<f64> = dimensions.iter().map(|(key, capacity)| {
            self.buckets.get(key).map_or(*capacity, |bucket| {
                (bucket.tokens + now.saturating_duration_since(bucket.updated).as_secs_f64() * capacity / 60.0).min(*capacity)
            })
        }).collect();
        if available.iter().any(|tokens| *tokens < 1.0) { return false; }
        for ((key, _), tokens) in dimensions.into_iter().zip(available) {
            self.buckets.insert(key, Bucket { tokens: tokens - 1.0, updated: now, used: now });
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn subject_budget_refills_and_failed_admission_consumes_nothing() {
        let scope = TenantContext::local();
        let subject = serde_json::from_value(serde_json::json!("fixture")).unwrap();
        let mut limiter = RateLimiter::default();
        let now = Instant::now();
        for _ in 0..120 { assert!(limiter.admit(&scope, &subject, "/rpc", now)); }
        assert!(!limiter.admit(&scope, &subject, "/rpc", now));
        assert!(limiter.admit(&scope, &subject, "/rpc", now + Duration::from_millis(501)));
        assert!(!limiter.admit(&scope, &subject, "/rpc", now + Duration::from_millis(501)));
        let other = serde_json::from_value(serde_json::json!("other")).unwrap();
        assert!(limiter.admit(&scope, &other, "/rpc", now));
    }
}
