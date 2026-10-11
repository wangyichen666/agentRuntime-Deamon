//! Access Plane只持久化路由、认证和有界租约，不拥有run/session终态。
use crate::*;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectorySession {
    pub external_id: String,
    pub scope: TenantContext,
    pub session_key: SessionKey,
    pub session_lifetime: SessionLifetimeId,
    pub runtime_endpoint: String,
    pub created_by: SubjectId,
    pub operation_id: String,
}
pub fn valid_directory_id(value: &str) -> bool {
    value.starts_with("ds-")
        && (35..=39).contains(&value.len())
        && value[3..]
            .bytes()
            .all(|v| v.is_ascii_hexdigit() || v == b'-')
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessClass {
    Request,
    WebSocket,
}
impl AccessClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::WebSocket => "web_socket",
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessLease {
    pub operation_id: String,
    pub scope: TenantContext,
    pub subject_id: SubjectId,
    pub revocation_epoch: u64,
    pub class: AccessClass,
    pub expires_at_ms: i64,
}
/// 三层预算由可信服务端固定。拒绝不会生成run，租约使用数据库时钟。
pub const ACCESS_RATE_LIMITS: [u64; 3] = [600, 120, 300];
pub const ACCESS_CONCURRENCY_LIMITS: [u64; 3] = [128, 16, 64];

/// Gateway只使用固定路由名；无查询参数或任意高基数键。
pub fn valid_access_endpoint(endpoint:&str)->bool {
    matches!(endpoint,"legacy"|"/ws"|"/auth/login"|"/auth/callback"|"/api/identity"|"/enterprise/sessions"|"/enterprise/rpc"|"/enterprise/credentials/issue"|"/enterprise/credentials/revoke"|"/enterprise/billing/configure"|"/enterprise/billing"|"/enterprise/billing/usage"|"/enterprise/billing/accounts"|"/enterprise/registry"|"/enterprise/registry/generation"|"/enterprise/workflows"|"/enterprise/workflows/read"|"/enterprise/workflows/control"|"/a2a"|"/.well-known/agent-card.json")
}
/// 登录事务只保存随机值的摘要；明文 state/nonce/verifier 仅在浏览器受保护 cookie。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OAuthLoginProof {
    pub state_digest: String,
    pub nonce_digest: String,
    pub verifier_digest: String,
}
impl OAuthLoginProof {
    pub fn valid(&self) -> bool {
        [&self.state_digest, &self.nonce_digest, &self.verifier_digest].into_iter()
            .all(|s| s.len() == 64 && s.bytes().all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)))
    }
}

#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct SecretToken(String);
impl SecretToken {
    pub fn new(value: String) -> Result<Self, AuthorizationError> {
        if value.is_empty()
            || value.len() > 16384
            || value
                .bytes()
                .any(|v| v.is_ascii_control() || v.is_ascii_whitespace())
        {
            return Err(AuthorizationError::Unauthenticated);
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl std::fmt::Debug for SecretToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretToken([已脱敏])")
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiCredentialMetadata {
    pub key_id: String,
    pub scope: TenantContext,
    pub principal: AuthenticatedPrincipal,
    pub expires_at_ms: i64,
    pub disabled: bool,
}
#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IssuedApiCredential {
    pub metadata: ApiCredentialMetadata,
    /// 首次响应之后不重放明文；未知发行结果须撤销后用新operation发行。
    pub token: Option<SecretToken>,
}
