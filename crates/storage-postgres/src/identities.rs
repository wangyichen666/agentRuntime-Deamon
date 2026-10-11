//! PostgreSQL身份查询、固定授权及run永久owner绑定；查询先完整域再解析正文。
use crate::canonical::{PgCanonicalStore, field, permanent_life, set_scope, unsigned};
use crate::{SCOPE, now, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::IdentityReadRepository;
use sha2::{Digest, Sha256};
use tokio_postgres::{GenericClient, Transaction};
fn digest(raw: &str) -> String {
    format!("{:x}", Sha256::digest(raw.as_bytes()))
}
pub(crate) async fn identity_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    subject: &SubjectId,
) -> Result<IdentityReadback, RepositoryError> {
    let mut values = scope_values(scope);
    values.push(subject.as_str().into());
    let row=db.query_opt("SELECT p.kind,p.revocation_epoch,h.version,CASE WHEN octet_length(v.document_json)<=1048576 THEN v.document_json END FROM principals p JOIN agents a ON a.organization_id=p.organization_id AND a.tenant_id=p.tenant_id JOIN identity_policy_heads h ON (h.organization_id,h.tenant_id,h.project_id,h.agent_uid)=(a.organization_id,a.tenant_id,a.project_id,a.uid) JOIN identity_policies v ON (v.organization_id,v.tenant_id,v.project_id,v.agent_uid,v.version)=(h.organization_id,h.tenant_id,h.project_id,h.agent_uid,h.version) WHERE p.organization_id=$1 AND p.tenant_id=$2 AND a.project_id=$3 AND a.uid=$4 AND p.subject_id=$5 FOR SHARE OF p,h",&parameters(&values)).await.map_err(unavailable)?.ok_or(AuthorizationError::Forbidden)?;
    let raw = field(&row, 3)?;
    let policy: PolicyDocument =
        serde_json::from_str(&raw).map_err(|_| CoordinationError::Corrupt)?;
    if policy.version.as_str() != row.try_get::<_, String>(2).map_err(unavailable)? {
        return Err(CoordinationError::Corrupt.into());
    }
    let kind: SubjectKind = serde_json::from_value(serde_json::Value::String(
        row.try_get(0).map_err(unavailable)?,
    ))
    .map_err(|_| CoordinationError::Corrupt)?;
    let rows=db.query(&format!("SELECT CASE WHEN octet_length(role)<=128 THEN role END FROM role_bindings WHERE {SCOPE} AND subject_id=$5 ORDER BY role LIMIT 17 FOR SHARE"),&parameters(&values)).await.map_err(unavailable)?;
    if rows.len() > 16 {
        return Err(CoordinationError::Corrupt.into());
    }
    let mut roles = Vec::new();
    for row in rows {
        roles.push(
            serde_json::from_value(serde_json::Value::String(field(&row, 0)?))
                .map_err(|_| CoordinationError::Corrupt)?,
        );
    }
    Ok(IdentityReadback {
        scope: scope.clone(),
        principal: AuthenticatedPrincipal {
            subject_id: subject.clone(),
            kind,
            revocation_epoch: unsigned(row.try_get(1).map_err(unavailable)?)?,
        },
        roles,
        policy,
        policy_digest: digest(&raw),
        enterprise_ready: false,
    })
}
pub(crate) async fn exact_owner_in<C: GenericClient + Sync>(
    db: &C,
    scope: &TenantContext,
    run: &RunId,
) -> Result<ExactOwner, CoordinationError> {
    let mut values = scope_values(scope);
    values.push(run.0.clone());
    let row=db.query_opt("SELECT CASE WHEN octet_length(r.session_id) BETWEEN 1 AND 1024 THEN r.session_id END,CASE WHEN octet_length(r.lifetime) BETWEEN 1 AND 128 THEN r.lifetime END,r.generation,CASE WHEN octet_length(t.id) BETWEEN 1 AND 1024 THEN t.id END FROM runs r JOIN turns t ON (t.organization_id,t.tenant_id,t.project_id,t.agent_uid,t.run_id)=(r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id) WHERE r.organization_id=$1 AND r.tenant_id=$2 AND r.project_id=$3 AND r.agent_uid=$4 AND r.id=$5",&parameters(&values)).await.map_err(unavailable)?.ok_or(CoordinationError::Forbidden)?;
    let owner = ExactOwner {
        session_key: SessionKey(field(&row, 0)?),
        session_lifetime_id: SessionLifetimeId(field(&row, 1)?),
        run_id: run.clone(),
        run_generation: RunGeneration(unsigned(row.try_get(2).map_err(unavailable)?)?),
        turn_id: TurnId(field(&row, 3)?),
    };
    permanent_life(
        db,
        &SessionWriterLeaseKey {
            scope: scope.clone(),
            session_key: owner.session_key.clone(),
            session_lifetime_id: owner.session_lifetime_id.clone(),
        },
    )
    .await?;
    Ok(owner)
}
pub(crate) async fn saved_authorization_in(
    db: &Transaction<'_>,
    envelope: &EnterpriseOwnerEnvelope,
) -> Result<Option<AuthorizationSnapshot>, RepositoryError> {
    let actual = exact_owner_in(db, &envelope.scope, &envelope.owner.run_id).await?;
    actual
        .fence(&envelope.owner)
        .map_err(|_| AuthorizationError::StaleOwner)?;
    let mut values = scope_values(&envelope.scope);
    values.push(envelope.owner.run_id.0.clone());
    let row=db.query_opt(&format!("SELECT CASE WHEN octet_length(snapshot_json)<=65536 THEN snapshot_json END,CASE WHEN octet_length(digest)=64 THEN digest END,CASE WHEN octet_length(subject_id) BETWEEN 1 AND 128 THEN subject_id END,revocation_epoch FROM run_authorizations WHERE {SCOPE} AND run_id=$5"),&parameters(&values)).await.map_err(unavailable)?;
    let Some(row) = row else { return Ok(None) };
    let raw = field(&row, 0)?;
    if digest(&raw) != field(&row, 1)? {
        return Err(CoordinationError::Corrupt.into());
    }
    let snapshot: AuthorizationSnapshot =
        serde_json::from_str(&raw).map_err(|_| CoordinationError::Corrupt)?;
    envelope.fence(&snapshot.envelope)?;
    if snapshot.context.scope != envelope.scope
        || snapshot.context.principal.subject_id.as_str() != field(&row, 2)?
        || snapshot.context.principal.revocation_epoch
            != unsigned(row.try_get(3).map_err(unavailable)?)?
    {
        return Err(CoordinationError::Corrupt.into());
    }
    let frozen=db.query_opt(&format!("SELECT CASE WHEN octet_length(snapshot_json)<=1048576 THEN snapshot_json END FROM run_snapshots WHERE {SCOPE} AND run_id=$5"),&parameters(&values)).await.map_err(unavailable)?.ok_or(CoordinationError::Corrupt)?;
    let settings = field(&frozen, 0)?;
    if digest(&settings) != snapshot.run_snapshot_digest {
        return Err(CoordinationError::Corrupt.into());
    }
    Ok(Some(snapshot))
}
pub(crate) async fn authorization_in(
    db: &Transaction<'_>,
    envelope: &EnterpriseOwnerEnvelope,
) -> Result<Option<AuthorizationSnapshot>, RepositoryError> {
    let Some(snapshot) = saved_authorization_in(db, envelope).await? else {
        return Ok(None);
    };
    // 普通策略/角色发布只作用于新准入。旧run继续使用不可变快照，显式
    // principal epoch撤销才阻止执行；共享行锁与撤销事务串行，避免检查后竞态。
    let principal = &snapshot.context.principal;
    let row = db.query_opt(
        "SELECT kind,revocation_epoch FROM principals WHERE organization_id=$1 AND tenant_id=$2 AND subject_id=$3 FOR SHARE",
        &[&envelope.scope.organization_id.as_str(), &envelope.scope.tenant_id.as_str(), &principal.subject_id.as_str()],
    ).await.map_err(unavailable)?.ok_or(AuthorizationError::Revoked)?;
    let kind: SubjectKind = serde_json::from_value(serde_json::Value::String(
        row.try_get(0).map_err(unavailable)?,
    ))
    .map_err(|_| CoordinationError::Corrupt)?;
    if kind != principal.kind
        || unsigned(row.try_get(1).map_err(unavailable)?)? != principal.revocation_epoch
    {
        return Err(AuthorizationError::Revoked.into());
    }
    let mut values = scope_values(&envelope.scope);
    values.push(snapshot.policy.version.as_str().into());
    let row=db.query_opt(&format!("SELECT CASE WHEN octet_length(document_json)<=1048576 THEN document_json END FROM identity_policies WHERE {SCOPE} AND version=$5"),&parameters(&values)).await.map_err(unavailable)?.ok_or(CoordinationError::Corrupt)?;
    let raw = field(&row, 0)?;
    let policy: PolicyDocument =
        serde_json::from_str(&raw).map_err(|_| CoordinationError::Corrupt)?;
    if digest(&raw) != snapshot.policy_digest || policy != snapshot.policy {
        return Err(CoordinationError::Corrupt.into());
    }
    Ok(Some(snapshot))
}

impl PgCanonicalStore {
    pub(crate) async fn fence_envelope_in(
        db: &Transaction<'_>,
        proof: &SessionWriterLease,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<AuthorizationSnapshot, RepositoryError> {
        if proof.key.scope != envelope.scope
            || proof.key.session_key != envelope.owner.session_key
            || proof.key.session_lifetime_id != envelope.owner.session_lifetime_id
        {
            return Err(AuthorizationError::Forbidden.into());
        }
        Self::fence_in(db, proof).await?;
        authorization_in(db, envelope)
            .await?
            .ok_or_else(|| AuthorizationError::Forbidden.into())
    }
    /// 仅空域初始化。调用者是受信任的数据库管理入口；运行RPC不暴露此方法。
    pub async fn provision_initial_domain(
        &self,
        scope: &TenantContext,
        principal: &AuthenticatedPrincipal,
        roles: &[EnterpriseRole],
        policy: &PolicyDocument,
    ) -> Result<(), RepositoryError> {
        if roles.is_empty() || roles.len() > 16 || principal.revocation_epoch != 0 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let raw = serde_json::to_string(policy).map_err(|_| CoordinationError::InvalidRequest)?;
        if raw.len() > 1048576 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let values = scope_values(scope);
        // 并发初始化在同域advisory lock下序列化；不覆盖现有主体、角色或策略。
        let key = serde_json::to_string(scope).map_err(|_| CoordinationError::InvalidRequest)?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,40))",
            &[&key],
        )
        .await
        .map_err(unavailable)?;
        let exists:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM agents WHERE organization_id=$1 AND tenant_id=$2 AND project_id=$3 AND uid=$4)",&parameters(&values)).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
        if exists {
            return Err(CoordinationError::Conflict.into());
        }
        tx.execute(
            "INSERT INTO organizations VALUES($1) ON CONFLICT DO NOTHING",
            &[&scope.organization_id.as_str()],
        )
        .await
        .map_err(unavailable)?;
        tx.execute(
            "INSERT INTO tenants VALUES($1,$2) ON CONFLICT DO NOTHING",
            &[&scope.organization_id.as_str(), &scope.tenant_id.as_str()],
        )
        .await
        .map_err(unavailable)?;
        tx.execute(
            "INSERT INTO projects VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
            &[
                &scope.organization_id.as_str(),
                &scope.tenant_id.as_str(),
                &scope.project_id.as_str(),
            ],
        )
        .await
        .map_err(unavailable)?;
        tx.execute(
            "INSERT INTO agents VALUES($1,$2,$3,$4)",
            &parameters(&values),
        )
        .await
        .map_err(unavailable)?;
        let mut params = values.clone();
        params.push(principal.subject_id.as_str().into());
        let kind = serde_json::to_value(principal.kind)
            .map_err(|_| CoordinationError::InvalidRequest)?
            .as_str()
            .ok_or(CoordinationError::InvalidRequest)?
            .to_owned();
        tx.execute(
            "INSERT INTO principals VALUES($1,$2,$3,$4,0) ON CONFLICT DO NOTHING",
            &[
                &scope.organization_id.as_str(),
                &scope.tenant_id.as_str(),
                &principal.subject_id.as_str(),
                &kind,
            ],
        )
        .await
        .map_err(unavailable)?;
        let current=tx.query_one("SELECT kind,revocation_epoch FROM principals WHERE organization_id=$1 AND tenant_id=$2 AND subject_id=$3 FOR UPDATE",&[&scope.organization_id.as_str(),&scope.tenant_id.as_str(),&principal.subject_id.as_str()]).await.map_err(unavailable)?;
        if current.try_get::<_, String>(0).map_err(unavailable)? != kind
            || current.try_get::<_, i64>(1).map_err(unavailable)? != 0
        {
            return Err(CoordinationError::Conflict.into());
        }
        for role in roles {
            let mut roleparams = params.clone();
            roleparams.push(
                serde_json::to_value(role)
                    .map_err(|_| CoordinationError::InvalidRequest)?
                    .as_str()
                    .ok_or(CoordinationError::InvalidRequest)?
                    .to_owned(),
            );
            tx.execute(
                "INSERT INTO role_bindings VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING",
                &parameters(&roleparams),
            )
            .await
            .map_err(unavailable)?;
        }
        // 显式初始化 Local 域时，Cron 使用独立 workload 主体，不能借用 human 权限。
        if *scope == TenantContext::local()
            && principal.subject_id.as_str() == "local-subject"
            && principal.kind == SubjectKind::Human
            && *policy == PolicyDocument::local()
        {
            tx.execute(
                "INSERT INTO principals VALUES($1,$2,$3,'workload',0)",
                &[
                    &scope.organization_id.as_str(),
                    &scope.tenant_id.as_str(),
                    &LOCAL_CRON_SUBJECT,
                ],
            )
            .await
            .map_err(unavailable)?;
            let mut cron = values.clone();
            cron.extend([LOCAL_CRON_SUBJECT.into(), "developer".into()]);
            tx.execute(
                "INSERT INTO role_bindings VALUES($1,$2,$3,$4,$5,$6)",
                &parameters(&cron),
            )
            .await
            .map_err(unavailable)?;
        }
        if *scope==TenantContext::local() {tx.execute("INSERT INTO principals VALUES($1,$2,'local-workflow-workload','workload',0) ON CONFLICT DO NOTHING",&[&scope.organization_id.as_str(),&scope.tenant_id.as_str()]).await.map_err(unavailable)?;let mut workflow=values.clone();workflow.extend(["local-workflow-workload".into(),"developer".into()]);tx.execute("INSERT INTO role_bindings VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING",&parameters(&workflow)).await.map_err(unavailable)?;}
        let mut params = values;
        params.extend([policy.version.as_str().into(), raw]);
        tx.execute(
            "INSERT INTO identity_policies VALUES($1,$2,$3,$4,$5,$6)",
            &parameters(&params),
        )
        .await
        .map_err(unavailable)?;
        tx.execute(
            "INSERT INTO identity_policy_heads VALUES($1,$2,$3,$4,$5)",
            &parameters(&params[..5]),
        )
        .await
        .map_err(unavailable)?;
        crate::ledgers::append_audit_in(
            &tx,
            &AuditCommand {
                scope: scope.clone(),
                operation_id: format!("provision:{}", scope.agent_uid.as_str()),
                subject_id: principal.subject_id.clone(),
                action: "domain.provision".into(),
                resource: serde_json::json!({"agent_uid":scope.agent_uid}),
                outcome: "created".into(),
            },
            now(&tx).await?,
        )
        .await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }
}
#[async_trait::async_trait]
impl IdentityReadRepository for PgCanonicalStore {
    async fn read_identity(
        &self,
        scope: &TenantContext,
        subject: &SubjectId,
    ) -> Result<IdentityReadback, RepositoryError> {
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let value = identity_in(&tx, scope, subject).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(value)
    }
    async fn read_authorization(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<Option<AuthorizationSnapshot>, RepositoryError> {
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &envelope.scope).await?;
        let value = authorization_in(&tx, envelope).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(value)
    }
}

/// 内部派生运行只继承父能力，并复核当前主体、完整策略与新准入时间条件。
pub(crate) struct InheritedAuthorization {
    pub snapshot: AuthorizationSnapshot,
    pub channel: HookChannel,
    pub create_session: bool,
}
impl AuthorizationVerifier for InheritedAuthorization {
    fn verify(
        &self,
        request: &AuthorizationRequest,
        identity: &IdentityReadback,
    ) -> Result<(), AuthorizationError> {
        if !(request.action == Action::SessionPrompt
            || self.create_session && request.action == Action::SessionCreate)
            || request.context.scope != self.snapshot.envelope.scope
            || identity.scope != self.snapshot.envelope.scope
            || request.context.principal != self.snapshot.context.principal
            || identity.principal != self.snapshot.context.principal
            || identity.roles != self.snapshot.roles
            || identity.policy != self.snapshot.policy
            || identity.policy_digest != self.snapshot.policy_digest
            || request.expected_policy != self.snapshot.policy.version
            || request.attributes.entry_channel != self.channel
            || request.attributes.workspace != self.snapshot.attributes.workspace
            || request.attributes.tool_effect != self.snapshot.attributes.tool_effect
            || request.attributes.network != self.snapshot.attributes.network
            || !authorization_conditions_match(request, &identity.policy)
        {
            return Err(AuthorizationError::Forbidden);
        }
        Ok(())
    }
}

#[cfg(test)]
mod frozen_policy_tests {
    use super::*;
    use agent_storage_ports::RunExecutionRepository;
    use std::sync::Arc;
    #[tokio::test]
    #[ignore = "需要独立AGENT_RESOURCE_TEST_DB/USER"]
    async fn ordinary_publish_preserves_run_but_explicit_revocation_fences() {
        let schema = format!(
            "frozen_policy_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        let store = PgCanonicalStore::connect_local(
            &std::env::var("AGENT_RESOURCE_TEST_DB").unwrap(),
            &std::env::var("AGENT_RESOURCE_TEST_USER").unwrap(),
            &schema,
        )
        .await
        .unwrap();
        let binding = RuntimeIdentity::local();
        store
            .provision_initial_domain(
                &binding.scope,
                &binding.principal,
                &[EnterpriseRole::TenantAdmin],
                &PolicyDocument::local(),
            )
            .await
            .unwrap();
        let identity = store
            .read_identity(&binding.scope, &binding.principal.subject_id)
            .await
            .unwrap();
        let mut request = AuthorizationRequest {
            context: RequestContext {
                principal: binding.principal.clone(),
                scope: binding.scope.clone(),
                request_id: RequestId::Number(1),
                trace: TraceContext {
                    trace_id: "frozen-policy-test".into(),
                },
            },
            expected_policy: identity.policy.version.clone(),
            action: Action::SessionPrompt,
            attributes: AuthorizationAttributes {
                workspace: ".".into(),
                entry_channel: HookChannel::DaemonRpc,
                tool_effect: None,
                timestamp_ms: 0,
                network: NetworkBoundary::None,
            },
        };
        let admission = RunAdmission {
            workflow_node: None,
            session_key: SessionKey("same-name.jsonl".into()),
            expected_lifetime: None,
            request_id: request.context.request_id.clone(),
            input: "fixture".into(),
            mode: AdmissionMode::Queue,
            plan_execution: None,
        };
        let settings:RunSnapshot=serde_json::from_value(serde_json::json!({"entry_channel":"daemon_rpc","route":null,"tools":[],"cwd":".","permission_mode":"risk","sandbox_requested":"native","sandbox_effective":"native","context_read_only":false,"context_token_budget":4096,"tool_catalog_digest":"empty","config_generation":0})).unwrap();
        let (_, owner) = store
            .admit_run_authorized(
                &admission,
                &settings,
                &request,
                Arc::new(agent_policy::PolicyEngine),
            )
            .await
            .unwrap();
        let env = EnterpriseOwnerEnvelope {
            scope: binding.scope.clone(),
            owner,
        };
        let original = store.read_authorization(&env).await.unwrap().unwrap();
        let mut policy = PolicyDocument::local();
        policy.version = "changed-policy".to_owned().try_into().unwrap();
        policy.hard_denies.push(Action::SessionPrompt);
        {
            let mut client = store.inner.client.lock().await;
            let tx = client.transaction().await.unwrap();
            set_scope(&tx, &binding.scope).await.unwrap();
            tx.execute("INSERT INTO identity_policies SELECT organization_id,tenant_id,project_id,agent_uid,$1,$2 FROM identity_policy_heads",&[&policy.version.as_str(),&serde_json::to_string(&policy).unwrap()]).await.unwrap();
            tx.execute(
                "UPDATE identity_policy_heads SET version=$1",
                &[&policy.version.as_str()],
            )
            .await
            .unwrap();
            tx.execute(
                "DELETE FROM role_bindings WHERE subject_id=$1",
                &[&binding.principal.subject_id.as_str()],
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();
        }
        assert_eq!(
            store.read_authorization(&env).await.unwrap(),
            Some(original)
        );
        assert!(store.start_queued_run(&env).await.unwrap());
        request.expected_policy = policy.version;
        request.context.request_id = RequestId::Number(2);
        let mut next = admission.clone();
        next.request_id = request.context.request_id.clone();
        assert_eq!(
            store
                .admit_run_authorized(
                    &next,
                    &settings,
                    &request,
                    Arc::new(agent_policy::PolicyEngine)
                )
                .await
                .unwrap_err(),
            RepositoryError::Authorization(AuthorizationError::Forbidden)
        );
        {
            let mut client = store.inner.client.lock().await;
            let tx = client.transaction().await.unwrap();
            set_scope(&tx, &binding.scope).await.unwrap();
            tx.execute(
                "UPDATE principals SET revocation_epoch=revocation_epoch+1 WHERE subject_id=$1",
                &[&binding.principal.subject_id.as_str()],
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();
        }
        assert_eq!(
            store.read_authorization(&env).await.unwrap_err(),
            RepositoryError::Authorization(AuthorizationError::Revoked)
        );
        let before = store
            .inner
            .client
            .lock()
            .await
            .query_one("SELECT count(*) FROM events", &[])
            .await
            .unwrap()
            .get::<_, i64>(0);
        assert_eq!(
            store
                .commit_run_turn(
                    &env,
                    &TurnCommit {
                        owner: env.owner.clone(),
                        status: RunStatus::Failed,
                        content: None,
                        error: Some((-32003, "fixture".into()))
                    }
                )
                .await
                .unwrap_err(),
            RepositoryError::Authorization(AuthorizationError::Revoked)
        );
        assert_eq!(
            store
                .inner
                .client
                .lock()
                .await
                .query_one("SELECT count(*) FROM events", &[])
                .await
                .unwrap()
                .get::<_, i64>(0),
            before
        );
        store
            .inner
            .client
            .lock()
            .await
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .await
            .unwrap();
        store.close().await;
    }
}
