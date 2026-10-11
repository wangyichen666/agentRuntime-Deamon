//! 类型化 RBAC 与条件约束；拒绝优先，不替代工具 hard deny 或 sandbox。
#![forbid(unsafe_code)]
use agent_core::*;

pub struct PolicyEngine;
impl AuthorizationVerifier for PolicyEngine {
    fn verify(
        &self,
        request: &AuthorizationRequest,
        identity: &IdentityReadback,
    ) -> Result<(), AuthorizationError> {
        if request.context.scope != identity.scope
            || request.context.principal.subject_id != identity.principal.subject_id
            || request.context.principal.kind != identity.principal.kind
        {
            return Err(AuthorizationError::Forbidden);
        }
        if request.context.principal.revocation_epoch != identity.principal.revocation_epoch {
            return Err(AuthorizationError::Revoked);
        }
        if request.expected_policy != identity.policy.version {
            return Err(AuthorizationError::StalePolicy);
        }
        if !authorization_conditions_match(request, &identity.policy) {
            return Err(AuthorizationError::Forbidden);
        }
        if identity
            .roles
            .iter()
            .any(|role| permits(*role, request.action))
        {
            Ok(())
        } else {
            Err(AuthorizationError::Forbidden)
        }
    }
}
fn permits(role: EnterpriseRole, action: Action) -> bool {
    use Action::*;
    use EnterpriseRole::*;
    if action == GatewayDelegate {
        return role == Gateway;
    }
    match role {
        Gateway => matches!(action, AgentRead),
        OrgAdmin | TenantAdmin => true,
        _ if action == IdentityManage => false,
        ProjectAdmin => action != AuditExport,
        AgentOperator => matches!(
            action,
            AgentRead
                | AgentConfigure
                | SessionCreate
                | SessionPrompt
                | RunCancel
                | InteractionRespond
                | ArtifactRead
        ),
        Developer => matches!(
            action,
            AgentRead
                | SessionCreate
                | SessionPrompt
                | RunCancel
                | InteractionRespond
                | ArtifactRead
        ),
        Auditor => matches!(action, AgentRead | ArtifactRead | AuditExport),
        Viewer => matches!(action, AgentRead | ArtifactRead),
    }
}
/// 未知 RPC 没有动作映射，控制面在调用业务 handler 前拒绝。
pub fn action_for_rpc(method: &str) -> Option<Action> {
    match method {
        "workflow.publish" => Some(Action::AgentConfigure),
        "workflow.message" => Some(Action::SessionPrompt),
        "workflow.start" => Some(Action::SessionPrompt),
        "workflow.control" => Some(Action::SessionManage),
        "workflow.read" => Some(Action::AgentRead),
        "registry.mutate" => Some(Action::PluginInstall),
        "registry.readback" | "registry.generation" => Some(Action::AgentRead),
        "identity.manage" => Some(Action::IdentityManage),
        "billing.configure" | "credential.issue" | "credential.revoke" => Some(Action::IdentityManage),
        "oauth.begin" | "oauth.consume" => Some(Action::GatewayDelegate),
        "credential.status" => Some(Action::AgentRead),
        "runtime.endpoint" => Some(Action::AgentRead),
        "audit.readback" => Some(Action::AuditExport),
        "quota.readback" => Some(Action::AgentRead),
        "billing.readback" | "billing.accounts" | "billing.usage" => Some(Action::AgentRead),
        "quota.configure" => Some(Action::AgentConfigure),
        "chat.send" | "compact.start" | "session.compact" | "spawn_subagent" => {
            Some(Action::SessionPrompt)
        }
        "session.new" | "session.fork" => Some(Action::SessionCreate),
        "directory.create" => Some(Action::SessionCreate),
        "directory.find" | "directory.resolve" | "access.acquire" | "access.release" => Some(Action::AgentRead),
        "session.clear"
        | "session.delete"
        | "session.close"
        | "session.resume"
        | "sessions.plan.discard" => Some(Action::SessionManage),
        "agent.cancel" | "queue.remove" | "resources.stop" | "cancel_subagent" => {
            Some(Action::RunCancel)
        }
        "approval.respond" | "interaction.respond" | "interaction.reject" => {
            Some(Action::InteractionRespond)
        }
        "artifacts.read" => Some(Action::ArtifactRead),
        "agent.subscribe"
        | "context.readback"
        | "hooks.readback"
        | "identity.readback"
        | "outbox.readback"
        | "interaction.list"
        | "interaction.read"
        | "memory.evidence"
        | "memory.list"
        | "memory.scope"
        | "models.list"
        | "permissions.get"
        | "ping"
        | "queue.list"
        | "queue.read"
        | "resources.list"
        | "resources.logs"
        | "resources.read"
        | "resources.wait"
        | "run.audit"
        | "run.authorization"
        | "run.discovery"
        | "run.events"
        | "run.provider_attempts"
        | "run.read"
        | "run.tools"
        | "runtime.doctor"
        | "session.list"
        | "session.load"
        | "session.load_page"
        | "session.trace"
        | "session.trace_page"
        | "sessions.plan.readback"
        | "views.reduce" => Some(Action::AgentRead),
        "connection.initialize"
        | "daemon.stop"
        | "list_subagents"
        | "memory.feedback"
        | "memory.flywheel"
        | "memory.forget"
        | "memory.recall"
        | "memory.store"
        | "models.save"
        | "models.use"
        | "permissions.set"
        | "read_subagent"
        | "resources.reconcile"
        | "run.reconcile"
        | "slash.execute"
        | "subagent.result.commit"
        | "subagent.result.release"
        | "subagent.result.reserve"
        | "wait_subagents" => Some(Action::AgentConfigure),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn pair() -> (AuthorizationRequest, IdentityReadback) {
        let identity = IdentityReadback {
            scope: TenantContext::local(),
            principal: AuthenticatedPrincipal {
                subject_id: "local-subject".to_owned().try_into().unwrap(),
                kind: SubjectKind::Human,
                revocation_epoch: 0,
            },
            roles: vec![EnterpriseRole::TenantAdmin],
            policy: PolicyDocument::local(),
            policy_digest: String::new(),
            enterprise_ready: false,
        };
        let request = AuthorizationRequest {
            context: RequestContext {
                principal: identity.principal.clone(),
                scope: identity.scope.clone(),
                request_id: RequestId::Number(1),
                trace: TraceContext {
                    trace_id: "trace-1".into(),
                },
            },
            expected_policy: identity.policy.version.clone(),
            action: Action::SessionPrompt,
            attributes: AuthorizationAttributes {
                workspace: "/work".into(),
                entry_channel: HookChannel::DaemonRpc,
                tool_effect: Some(ToolEffect::Read),
                timestamp_ms: 100,
                network: NetworkBoundary::None,
            },
        };
        (request, identity)
    }
    #[test]
    fn hard_deny_and_every_condition_override_admin_roles() {
        let (request, identity) = pair();
        assert_eq!(PolicyEngine.verify(&request, &identity), Ok(()));
        type Change = fn(&mut IdentityReadback);
        let changes: [Change; 10] = [
            |i| i.policy.hard_denies.push(Action::SessionPrompt),
            |i| i.policy.conditions.tenant_id = Some("other".to_owned().try_into().unwrap()),
            |i| i.policy.conditions.project_id = Some("other".to_owned().try_into().unwrap()),
            |i| i.policy.conditions.agent_uid = Some("other".to_owned().try_into().unwrap()),
            |i| i.policy.conditions.workspace = Some("/foreign".into()),
            |i| i.policy.conditions.entry_channels = Some(vec![HookChannel::Http]),
            |i| i.policy.conditions.tool_effects = Some(vec![ToolEffect::External]),
            |i| i.policy.conditions.not_before_ms = Some(101),
            |i| i.policy.conditions.expires_at_ms = Some(100),
            |i| i.policy.conditions.networks = Some(vec![NetworkBoundary::Public]),
        ];
        for change in changes {
            let mut denied = identity.clone();
            change(&mut denied);
            assert_eq!(
                PolicyEngine.verify(&request, &denied),
                Err(AuthorizationError::Forbidden)
            );
        }
    }
    #[test]
    fn channel_never_grants_roles_and_versions_and_revocation_are_typed() {
        let (mut request, mut identity) = pair();
        identity.roles = vec![EnterpriseRole::Viewer];
        for channel in [
            HookChannel::DaemonRpc,
            HookChannel::Cli,
            HookChannel::Http,
            HookChannel::Acp,
        ] {
            request.attributes.entry_channel = channel;
            assert_eq!(
                PolicyEngine.verify(&request, &identity),
                Err(AuthorizationError::Forbidden)
            );
        }
        identity.principal.revocation_epoch = 1;
        assert_eq!(
            PolicyEngine.verify(&request, &identity),
            Err(AuthorizationError::Revoked)
        );
        identity.principal.revocation_epoch = 0;
        identity.policy.version = "policy-2".to_owned().try_into().unwrap();
        assert_eq!(
            PolicyEngine.verify(&request, &identity),
            Err(AuthorizationError::StalePolicy)
        );
        request.context.scope.tenant_id = "foreign".to_owned().try_into().unwrap();
        assert_eq!(
            PolicyEngine.verify(&request, &identity),
            Err(AuthorizationError::Forbidden)
        );
    }
}
