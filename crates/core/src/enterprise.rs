//! 企业身份与授权领域值；不含传输或数据库逻辑。
use crate::{ExactOwner, HookChannel, RequestId, ToolEffect};
use serde::{Deserialize, Serialize};

macro_rules! opaque_ids {
    ($($name:ident),+ $(,)?) => {$(
        #[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);
        impl $name {
            pub fn as_str(&self) -> &str { &self.0 }
        }
        impl TryFrom<String> for $name {
            type Error = AuthorizationError;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                if value.is_empty() || value.len() > 128 || !value.bytes().all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_')) {
                    return Err(AuthorizationError::ProtocolViolation);
                }
                Ok(Self(value))
            }
        }
        impl From<$name> for String { fn from(value: $name) -> Self { value.0 } }
    )+};
}
opaque_ids!(
    OrganizationId,
    TenantId,
    ProjectId,
    SubjectId,
    ServiceAccountId,
    AgentUid,
    PolicyVersion
);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantContext {
    pub organization_id: OrganizationId,
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub agent_uid: AgentUid,
}
impl TenantContext {
    pub fn local() -> Self {
        Self {
            organization_id: OrganizationId("local-organization".into()),
            tenant_id: TenantId("local-tenant".into()),
            project_id: ProjectId("local-project".into()),
            agent_uid: AgentUid("local-agent".into()),
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubjectKind {
    Human,
    ServiceAccount,
    Workload,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthenticatedPrincipal {
    pub subject_id: SubjectId,
    pub kind: SubjectKind,
    pub revocation_epoch: u64,
}
/// 运行实例的受信任启动绑定；使用面DTO不能修改它。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeIdentity {
    pub scope: TenantContext,
    pub principal: AuthenticatedPrincipal,
}
impl RuntimeIdentity {
    pub fn local_workflow() -> Self {Self{scope:TenantContext::local(),principal:AuthenticatedPrincipal{subject_id:SubjectId("local-workflow-workload".into()),kind:SubjectKind::Workload,revocation_epoch:0}}}
    pub fn local_cron() -> Self {
        Self {
            scope: TenantContext::local(),
            principal: AuthenticatedPrincipal {
                subject_id: SubjectId(LOCAL_CRON_SUBJECT.into()),
                kind: SubjectKind::Workload,
                revocation_epoch: 0,
            },
        }
    }
    pub fn local() -> Self {
        Self {
            scope: TenantContext::local(),
            principal: AuthenticatedPrincipal {
                subject_id: SubjectId("local-subject".into()),
                kind: SubjectKind::Human,
                revocation_epoch: 0,
            },
        }
    }
}
pub fn session_command_action(command: &crate::SessionCommand) -> Action {
    match command {
        crate::SessionCommand::Create { .. } | crate::SessionCommand::Fork { .. } => {
            Action::SessionCreate
        }
        crate::SessionCommand::End { .. } | crate::SessionCommand::Close { .. } => {
            Action::SessionManage
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraceContext {
    pub trace_id: String,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestContext {
    pub principal: AuthenticatedPrincipal,
    pub scope: TenantContext,
    pub request_id: RequestId,
    pub trace: TraceContext,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnterpriseRole {
    Gateway,
    OrgAdmin,
    TenantAdmin,
    ProjectAdmin,
    AgentOperator,
    Developer,
    Auditor,
    Viewer,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    GatewayDelegate,
    IdentityManage,
    AgentRead,
    AgentConfigure,
    SessionCreate,
    SessionPrompt,
    SessionManage,
    RunCancel,
    InteractionRespond,
    ArtifactRead,
    SecretUse,
    PluginInstall,
    AuditExport,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum IdentityCommand {
    RegisterSubject {
        principal: AuthenticatedPrincipal,
    },
    BindRoles {
        subject_id: SubjectId,
        roles: Vec<EnterpriseRole>,
    },
    PublishPolicy {
        policy: PolicyDocument,
    },
    Revoke {
        subject_id: SubjectId,
        expected_epoch: u64,
    },
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityManagementReceipt {
    pub operation_id: String,
    pub command_digest: String,
    pub resulting_identity: IdentityReadback,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkBoundary {
    None,
    Public,
    Private,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationAttributes {
    pub workspace: String,
    pub entry_channel: HookChannel,
    pub tool_effect: Option<ToolEffect>,
    pub timestamp_ms: i64,
    pub network: NetworkBoundary,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PolicyConditions {
    pub tenant_id: Option<TenantId>,
    pub project_id: Option<ProjectId>,
    pub agent_uid: Option<AgentUid>,
    pub workspace: Option<String>,
    pub entry_channels: Option<Vec<HookChannel>>,
    pub tool_effects: Option<Vec<ToolEffect>>,
    pub not_before_ms: Option<i64>,
    pub expires_at_ms: Option<i64>,
    pub networks: Option<Vec<NetworkBoundary>>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyDocument {
    pub version: PolicyVersion,
    pub hard_denies: Vec<Action>,
    pub conditions: PolicyConditions,
    /// 风险审批要求两名非请求者批准；缺省不改变旧序列化摘要。
    #[serde(default, skip_serializing_if = "is_false")]
    pub dual_approval: bool,
}
fn is_false(value: &bool) -> bool {
    !*value
}
impl PolicyDocument {
    pub fn local() -> Self {
        Self {
            version: PolicyVersion("local-policy-1".into()),
            hard_denies: vec![],
            conditions: PolicyConditions::default(),
            dual_approval: false,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityReadback {
    pub scope: TenantContext,
    pub principal: AuthenticatedPrincipal,
    pub roles: Vec<EnterpriseRole>,
    pub policy: PolicyDocument,
    pub policy_digest: String,
    pub enterprise_ready: bool,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationRequest {
    pub context: RequestContext,
    pub expected_policy: PolicyVersion,
    pub action: Action,
    pub attributes: AuthorizationAttributes,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnterpriseOwnerEnvelope {
    pub scope: TenantContext,
    pub owner: ExactOwner,
}
impl EnterpriseOwnerEnvelope {
    pub fn fence(&self, candidate: &Self) -> Result<(), AuthorizationError> {
        if self.scope != candidate.scope {
            return Err(AuthorizationError::Forbidden);
        }
        self.owner
            .fence(&candidate.owner)
            .map_err(|_| AuthorizationError::StaleOwner)
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationSnapshot {
    pub envelope: EnterpriseOwnerEnvelope,
    pub context: RequestContext,
    pub roles: Vec<EnterpriseRole>,
    pub policy: PolicyDocument,
    pub policy_digest: String,
    pub attributes: AuthorizationAttributes,
    pub action: Action,
    pub run_snapshot_digest: String,
}
/// 准入事务中的同步纯判定端口；实现不得进行 I/O、回调仓储或等待。
pub trait AuthorizationVerifier: Send + Sync {
    fn verify(
        &self,
        request: &AuthorizationRequest,
        identity: &IdentityReadback,
    ) -> Result<(), AuthorizationError>;
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizationError {
    #[error("身份认证失败")]
    Unauthenticated,
    #[error("无权访问此资源")]
    Forbidden,
    #[error("主体权限已撤销")]
    Revoked,
    #[error("策略版本已变化")]
    StalePolicy,
    #[error("操作 owner 已过期")]
    StaleOwner,
    #[error("授权请求格式无效")]
    ProtocolViolation,
}
impl AuthorizationError {
    pub fn http_status(self) -> u16 {
        match self {
            Self::Unauthenticated => 401,
            Self::Forbidden | Self::Revoked => 403,
            Self::StalePolicy | Self::StaleOwner => 409,
            Self::ProtocolViolation => 400,
        }
    }
    pub fn rpc_code(self) -> i64 {
        match self {
            Self::Unauthenticated => -32040,
            Self::Forbidden | Self::Revoked => -32041,
            Self::StalePolicy => -32042,
            Self::StaleOwner => -32043,
            Self::ProtocolViolation => -32602,
        }
    }
    pub fn recovery_action(self) -> &'static str {
        match self {
            Self::Unauthenticated => "authenticate",
            Self::Forbidden | Self::Revoked => "contact_administrator",
            Self::StalePolicy => "refresh_policy",
            Self::StaleOwner => "refresh_owner",
            Self::ProtocolViolation => "correct_request",
        }
    }
}

/// Local daemon固定选择的无人值守主体；不能从job或请求参数覆写。
pub const LOCAL_CRON_SUBJECT: &str = "local-cron-workload";

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn opaque_ids_reject_empty_paths_and_oversized_slugs() {
        for invalid in [
            String::new(),
            "../tenant".into(),
            "租户显示名".into(),
            "a".repeat(129),
        ] {
            assert!(TenantId::try_from(invalid.clone()).is_err());
            assert!(
                serde_json::from_value::<TenantId>(serde_json::Value::String(invalid)).is_err()
            );
        }
        assert_eq!(
            TenantId::try_from("tenant-001".to_owned())
                .unwrap()
                .as_str(),
            "tenant-001"
        );
    }
    #[test]
    fn enterprise_owner_checks_scope_before_all_exact_owner_dimensions() {
        let owner = EnterpriseOwnerEnvelope {
            scope: TenantContext::local(),
            owner: ExactOwner {
                session_key: crate::SessionKey("s".into()),
                session_lifetime_id: crate::SessionLifetimeId("life".into()),
                run_id: crate::RunId("run-1".into()),
                run_generation: crate::RunGeneration(1),
                turn_id: crate::TurnId("turn-1".into()),
            },
        };
        assert_eq!(owner.fence(&owner), Ok(()));
        let mut candidate = owner.clone();
        candidate.scope.agent_uid = "another-agent".to_owned().try_into().unwrap();
        assert_eq!(owner.fence(&candidate), Err(AuthorizationError::Forbidden));
        candidate = owner.clone();
        candidate.owner.session_lifetime_id.0.push('2');
        assert_eq!(owner.fence(&candidate), Err(AuthorizationError::StaleOwner));
    }
}

/// 纯策略条件匹配，普通准入与内部续跑/委派共用；时间由数据库提供。
pub fn authorization_conditions_match(
    request: &AuthorizationRequest,
    policy: &PolicyDocument,
) -> bool {
    let c = &policy.conditions;
    let a = &request.attributes;
    let scope = &request.context.scope;
    !policy.hard_denies.contains(&request.action)
        && c.tenant_id.as_ref().is_none_or(|v| *v == scope.tenant_id)
        && c.project_id.as_ref().is_none_or(|v| *v == scope.project_id)
        && c.agent_uid.as_ref().is_none_or(|v| *v == scope.agent_uid)
        && c.workspace.as_ref().is_none_or(|v| *v == a.workspace)
        && c.entry_channels
            .as_ref()
            .is_none_or(|v| v.contains(&a.entry_channel))
        && c.tool_effects
            .as_ref()
            .is_none_or(|v| a.tool_effect.is_some_and(|effect| v.contains(&effect)))
        && c.not_before_ms.is_none_or(|v| a.timestamp_ms >= v)
        && c.expires_at_ms.is_none_or(|v| a.timestamp_ms < v)
        && c.networks.as_ref().is_none_or(|v| v.contains(&a.network))
}
