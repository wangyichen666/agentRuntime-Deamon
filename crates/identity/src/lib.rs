//! 可信传输主体与持久身份的绑定；请求参数不能选择主体或租户。
#![forbid(unsafe_code)]
use agent_core::*;
pub mod api_key;
pub mod oidc;
pub mod oauth;
pub use oidc::{OidcConfig, OidcVerifier, VerifiedToken};

/// 核对运营方绑定的完整scope/principal；不根据输入channel或客户端slug授予权限。
pub fn bound_context(
    binding: &RuntimeIdentity,
    identity: &IdentityReadback,
    request_id: RequestId,
) -> Result<RequestContext, AuthorizationError> {
    if binding.scope == TenantContext::local() && *binding == RuntimeIdentity::local() {
        return local_context(identity, request_id);
    }
    if identity.scope != binding.scope || identity.principal != binding.principal {
        return Err(AuthorizationError::Unauthenticated);
    }
    let trace_id = request_trace(&request_id);
    Ok(RequestContext {
        scope: binding.scope.clone(),
        principal: binding.principal.clone(),
        request_id,
        trace: TraceContext { trace_id },
    })
}

fn request_trace(request_id: &RequestId) -> String {
    use sha2::{Digest, Sha256};
    format!(
        "request-{:x}",
        Sha256::digest(serde_json::to_vec(request_id).unwrap_or_default())
    )
}

/// 仅用于本地 daemon 已建立的工作区连接；远程 Enterprise 入口尚未启用。
pub fn local_context(
    identity: &IdentityReadback,
    request_id: RequestId,
) -> Result<RequestContext, AuthorizationError> {
    if identity.scope != TenantContext::local()
        || identity.principal.subject_id.as_str() != "local-subject"
        || identity.principal.kind != SubjectKind::Human
        || identity.enterprise_ready
    {
        return Err(AuthorizationError::Unauthenticated);
    }
    let trace_id = match &request_id {
        RequestId::Number(n) => format!("local-{n}"),
        RequestId::String(s) => format!("local-{s}"),
    };
    Ok(RequestContext {
        principal: identity.principal.clone(),
        scope: identity.scope.clone(),
        request_id,
        trace: TraceContext { trace_id },
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn local_transport_does_not_accept_foreign_or_workload_identity() {
        let mut identity = IdentityReadback {
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
        assert!(local_context(&identity, RequestId::Number(1)).is_ok());
        identity.scope.tenant_id = "another-tenant".to_owned().try_into().unwrap();
        assert_eq!(
            local_context(&identity, RequestId::Number(1)),
            Err(AuthorizationError::Unauthenticated)
        );
        identity.scope = TenantContext::local();
        identity.principal.kind = SubjectKind::Workload;
        assert_eq!(
            local_context(&identity, RequestId::Number(1)),
            Err(AuthorizationError::Unauthenticated)
        );
    }
}
