//! 身份与 run 授权共用既有连接和事务，不持有第二份执行状态。
use crate::{RunStore, RuntimeError, sessions};
use agent_core::*;
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use sha2::{Digest, Sha256};

pub trait IdentityRepository: Send + Sync {
    fn identity_readback(
        &self,
        scope: &TenantContext,
        subject: &SubjectId,
    ) -> Result<IdentityReadback, RuntimeError>;
    fn authorization_readback(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<Option<AuthorizationSnapshot>, RuntimeError>;
}

#[async_trait::async_trait]
impl agent_storage_ports::IdentityReadRepository for RunStore {
    async fn read_identity(
        &self,
        scope: &TenantContext,
        subject: &SubjectId,
    ) -> Result<IdentityReadback, RepositoryError> {
        let (scope, subject) = (scope.clone(), subject.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .identity_readback(&scope, &subject)
                .map_err(RuntimeError::repository_error)
        })
        .await
    }
    async fn read_authorization(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<Option<AuthorizationSnapshot>, RepositoryError> {
        let envelope = envelope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .authorization_readback(&envelope)
                .map_err(RuntimeError::repository_error)
        })
        .await
    }
}
impl IdentityRepository for RunStore {
    fn identity_readback(
        &self,
        scope: &TenantContext,
        subject: &SubjectId,
    ) -> Result<IdentityReadback, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let tx = connection.transaction()?;
        identity_in(&tx, scope, subject)
    }
    fn authorization_readback(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<Option<AuthorizationSnapshot>, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let tx = connection.transaction()?;
        check_session_scope_in(&tx, &envelope.scope, &envelope.owner.session_key)?;
        authorization_owner_in(&tx, envelope)?;
        let row = tx.query_row("SELECT snapshot_json,digest FROM run_authorizations WHERE tenant_id=?1 AND project_id=?2 AND agent_uid=?3 AND run_id=?4",params![envelope.scope.tenant_id.as_str(),envelope.scope.project_id.as_str(),envelope.scope.agent_uid.as_str(),envelope.owner.run_id.0],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?))).optional()?;
        row.map(|(raw, digest)| {
            let snapshot: AuthorizationSnapshot =
                serde_json::from_str(&raw).map_err(|_| AuthorizationError::Forbidden)?;
            if hash(raw.as_bytes()) != digest {
                return Err(AuthorizationError::Forbidden.into());
            }
            envelope.fence(&snapshot.envelope)?;
            check_snapshot_in(&tx, &snapshot)?;
            Ok(snapshot)
        })
        .transpose()
    }
}

fn authorization_owner_in(
    db: &Connection,
    envelope: &EnterpriseOwnerEnvelope,
) -> Result<(), RuntimeError> {
    let key = &envelope.owner.session_key;
    let scope = &envelope.scope;
    let row = db.query_row("SELECT r.lifetime,r.generation,t.id,h.lifetime,h.deleted FROM runs r JOIN turns t ON t.run_id=r.id JOIN session_heads h ON h.session_id=r.session_id JOIN session_scopes s ON s.session_id=r.session_id JOIN tenants tenant ON tenant.id=s.tenant_id WHERE tenant.organization_id=?1 AND s.tenant_id=?2 AND s.project_id=?3 AND s.agent_uid=?4 AND r.session_id=?5 AND r.id=?6",params![scope.organization_id.as_str(),scope.tenant_id.as_str(),scope.project_id.as_str(),scope.agent_uid.as_str(),key.0,envelope.owner.run_id.0],|r|Ok((r.get::<_,String>(0)?,r.get::<_,u64>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?,r.get::<_,bool>(4)?))).optional()?.ok_or(AuthorizationError::Forbidden)?;
    let actual = ExactOwner {
        session_key: key.clone(),
        session_lifetime_id: SessionLifetimeId(row.0),
        run_id: envelope.owner.run_id.clone(),
        run_generation: RunGeneration(row.1),
        turn_id: TurnId(row.2),
    };
    actual
        .fence(&envelope.owner)
        .map_err(|_| AuthorizationError::StaleOwner)?;
    if row.4 || row.3 != actual.session_lifetime_id.0 {
        return Err(AuthorizationError::StaleOwner.into());
    }
    sessions::lifetime_owner_in(db, key, &actual.session_lifetime_id)?;
    crate::writer::fence_in(db, key, &actual.session_lifetime_id)
}
pub(crate) fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn encoded<T: Serialize>(value: &T) -> Result<String, RuntimeError> {
    serde_json::to_string(value).map_err(|e| RuntimeError::Protocol(e.to_string()))
}
pub(crate) fn identity_in(
    db: &Connection,
    scope: &TenantContext,
    subject: &SubjectId,
) -> Result<IdentityReadback, RuntimeError> {
    let row = db.query_row("SELECT p.kind,p.revocation_epoch,h.version,v.document_json FROM principals p JOIN tenants t ON t.id=p.tenant_id JOIN agents a ON a.tenant_id=p.tenant_id JOIN identity_policy_heads h ON h.tenant_id=a.tenant_id AND h.project_id=a.project_id AND h.agent_uid=a.uid JOIN identity_policies v ON v.tenant_id=h.tenant_id AND v.project_id=h.project_id AND v.agent_uid=h.agent_uid AND v.version=h.version WHERE t.organization_id=?1 AND p.tenant_id=?2 AND a.project_id=?3 AND a.uid=?4 AND p.subject_id=?5",params![scope.organization_id.as_str(),scope.tenant_id.as_str(),scope.project_id.as_str(),scope.agent_uid.as_str(),subject.as_str()],|r|Ok((r.get::<_,String>(0)?,r.get::<_,u64>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?))).optional()?.ok_or(AuthorizationError::Forbidden)?;
    let kind: SubjectKind = serde_json::from_value(serde_json::Value::String(row.0))
        .map_err(|_| AuthorizationError::Forbidden)?;
    let policy: PolicyDocument =
        serde_json::from_str(&row.3).map_err(|_| AuthorizationError::Forbidden)?;
    if policy.version.as_str() != row.2 {
        return Err(AuthorizationError::Forbidden.into());
    }
    let mut stmt = db.prepare("SELECT role FROM role_bindings WHERE tenant_id=?1 AND project_id=?2 AND agent_uid=?3 AND subject_id=?4 ORDER BY role")?;
    let roles = stmt
        .query_map(
            params![
                scope.tenant_id.as_str(),
                scope.project_id.as_str(),
                scope.agent_uid.as_str(),
                subject.as_str()
            ],
            |r| r.get::<_, String>(0),
        )?
        .map(|r| {
            serde_json::from_value::<EnterpriseRole>(serde_json::Value::String(r?))
                .map_err(|_| RuntimeError::Authorization(AuthorizationError::Forbidden))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let policy_digest = hash(encoded(&policy)?.as_bytes());
    Ok(IdentityReadback {
        scope: scope.clone(),
        principal: AuthenticatedPrincipal {
            subject_id: subject.clone(),
            kind,
            revocation_epoch: row.1,
        },
        roles,
        policy,
        policy_digest,
        enterprise_ready: false,
    })
}
pub(crate) fn check_run_scope_in(
    db: &Connection,
    scope: &TenantContext,
    run: &RunId,
) -> Result<(), RuntimeError> {
    let visible = db.query_row("SELECT 1 FROM runs r JOIN session_scopes s ON s.session_id=r.session_id JOIN tenants t ON t.id=s.tenant_id WHERE t.organization_id=?1 AND s.tenant_id=?2 AND s.project_id=?3 AND s.agent_uid=?4 AND r.id=?5",params![scope.organization_id.as_str(),scope.tenant_id.as_str(),scope.project_id.as_str(),scope.agent_uid.as_str(),run.0],|row|row.get::<_,i64>(0)).optional()?;
    if visible.is_none() {
        return Err(AuthorizationError::Forbidden.into());
    }
    Ok(())
}
pub(crate) fn check_session_scope_in(
    db: &Connection,
    scope: &TenantContext,
    session: &SessionKey,
) -> Result<(), RuntimeError> {
    let found = db.query_row("SELECT 1 FROM session_scopes s JOIN tenants t ON t.id=s.tenant_id WHERE t.organization_id=?1 AND s.tenant_id=?2 AND s.project_id=?3 AND s.agent_uid=?4 AND s.session_id=?5",params![scope.organization_id.as_str(),scope.tenant_id.as_str(),scope.project_id.as_str(),scope.agent_uid.as_str(),session.0],|r|r.get::<_,i64>(0)).optional()?;
    if found.is_none() {
        return Err(AuthorizationError::Forbidden.into());
    }
    Ok(())
}
pub(crate) fn authorize_in(
    db: &Connection,
    request: &AuthorizationRequest,
    verifier: &dyn AuthorizationVerifier,
) -> Result<IdentityReadback, RuntimeError> {
    if request.action != Action::SessionPrompt {
        return Err(AuthorizationError::Forbidden.into());
    }
    let identity = identity_in(
        db,
        &request.context.scope,
        &request.context.principal.subject_id,
    )?;
    verifier.verify(request, &identity)?;
    // 此切片只允许 Local。全资源 tenant 查询闭合前不得用此路径创建远程租户。
    if request.context.scope != TenantContext::local() {
        return Err(AuthorizationError::Forbidden.into());
    }
    Ok(identity)
}
pub(crate) fn freeze_in(
    db: &Connection,
    run: &RunId,
    snapshot: &RunSnapshot,
    request: &AuthorizationRequest,
    identity: &IdentityReadback,
) -> Result<(), RuntimeError> {
    let owner = sessions::owner_in(db, run)?;
    check_session_scope_in(db, &request.context.scope, &owner.session_key)?;
    let frozen = AuthorizationSnapshot {
        envelope: EnterpriseOwnerEnvelope {
            scope: request.context.scope.clone(),
            owner,
        },
        context: request.context.clone(),
        roles: identity.roles.clone(),
        policy: identity.policy.clone(),
        policy_digest: identity.policy_digest.clone(),
        attributes: request.attributes.clone(),
        action: request.action,
        run_snapshot_digest: hash(encoded(snapshot)?.as_bytes()),
    };
    let payload = encoded(&frozen)?;
    db.execute(
        "INSERT INTO run_authorizations VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
        params![
            run.0,
            frozen.envelope.scope.tenant_id.as_str(),
            frozen.envelope.scope.project_id.as_str(),
            frozen.envelope.scope.agent_uid.as_str(),
            frozen.context.principal.subject_id.as_str(),
            frozen.context.principal.revocation_epoch,
            payload,
            hash(payload.as_bytes())
        ],
    )?;
    Ok(())
}
fn check_snapshot_in(
    db: &Connection,
    snapshot: &AuthorizationSnapshot,
) -> Result<(), RuntimeError> {
    check_session_scope_in(
        db,
        &snapshot.envelope.scope,
        &snapshot.envelope.owner.session_key,
    )?;
    if snapshot.context.scope != snapshot.envelope.scope
        || hash(encoded(&snapshot.policy)?.as_bytes()) != snapshot.policy_digest
    {
        return Err(AuthorizationError::Forbidden.into());
    }
    let epoch = db
        .query_row(
            "SELECT revocation_epoch FROM principals WHERE tenant_id=?1 AND subject_id=?2",
            params![
                snapshot.envelope.scope.tenant_id.as_str(),
                snapshot.context.principal.subject_id.as_str()
            ],
            |r| r.get::<_, u64>(0),
        )
        .optional()?
        .ok_or(AuthorizationError::Forbidden)?;
    if epoch != snapshot.context.principal.revocation_epoch {
        return Err(AuthorizationError::Revoked.into());
    }
    let bytes: u64 = db.query_row(
        "SELECT length(CAST(snapshot_json AS BLOB)) FROM run_snapshots WHERE run_id=?1",
        [&snapshot.envelope.owner.run_id.0],
        |row| row.get(0),
    )?;
    if bytes > 1024 * 1024 {
        return Err(CoordinationError::Corrupt.into());
    }
    let raw: String = db.query_row(
        "SELECT snapshot_json FROM run_snapshots WHERE run_id=?1",
        params![snapshot.envelope.owner.run_id.0],
        |r| r.get(0),
    )?;
    if hash(raw.as_bytes()) != snapshot.run_snapshot_digest {
        return Err(AuthorizationError::Forbidden.into());
    }
    Ok(())
}
pub(crate) fn authorization_fence_in(
    db: &Connection,
    owner: &ExactOwner,
) -> Result<(), RuntimeError> {
    let row = db
        .query_row(
            "SELECT snapshot_json,digest FROM run_authorizations WHERE run_id=?1",
            params![owner.run_id.0],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )
        .optional()?;
    if let Some((raw, digest)) = row {
        if hash(raw.as_bytes()) != digest {
            return Err(AuthorizationError::Forbidden.into());
        }
        let snapshot: AuthorizationSnapshot =
            serde_json::from_str(&raw).map_err(|_| AuthorizationError::Forbidden)?;
        snapshot
            .envelope
            .owner
            .fence(owner)
            .map_err(|_| AuthorizationError::StaleOwner)?;
        check_snapshot_in(db, &snapshot)?;
    }
    Ok(())
}
pub(crate) fn check_existing_in(
    db: &Connection,
    run: &RunId,
    request: &AuthorizationRequest,
) -> Result<(), RuntimeError> {
    let raw = db.query_row("SELECT snapshot_json FROM run_authorizations WHERE tenant_id=?1 AND project_id=?2 AND agent_uid=?3 AND subject_id=?4 AND run_id=?5",params![request.context.scope.tenant_id.as_str(),request.context.scope.project_id.as_str(),request.context.scope.agent_uid.as_str(),request.context.principal.subject_id.as_str(),run.0],|r|r.get::<_,String>(0)).optional()?;
    let Some(raw) = raw else {
        // 升级前 Local 历史不伪造授权；重读幂等结果仍需本次可信 Local 准入授权。
        let has_authorization = db
            .query_row(
                "SELECT 1 FROM run_authorizations WHERE run_id=?1",
                params![run.0],
                |r| r.get::<_, i64>(0),
            )
            .optional()?
            .is_some();
        if has_authorization
            || request.context.scope != TenantContext::local()
            || request.context.principal.subject_id.as_str() != "local-subject"
            || request.context.principal.kind != SubjectKind::Human
        {
            return Err(AuthorizationError::Forbidden.into());
        }
        return Ok(());
    };
    let snapshot: AuthorizationSnapshot =
        serde_json::from_str(&raw).map_err(|_| AuthorizationError::Forbidden)?;
    if snapshot.context.principal != request.context.principal
        || snapshot.context.scope != request.context.scope
    {
        return Err(AuthorizationError::Forbidden.into());
    }
    authorization_fence_in(db, &sessions::owner_in(db, run)?)
}

/// 子任务只继承已核验父授权和受限原快照，不从模型/入口参数授予主体。
pub(crate) fn inherit_authorization_in(
    db: &Connection,
    parent: &RunId,
    child: &RunId,
    snapshot: &RunSnapshot,
) -> Result<(), RuntimeError> {
    let parent_owner = sessions::owner_in(db, parent)?;
    sessions::fence_in(db, &parent_owner)?;
    let raw = db
        .query_row(
            "SELECT snapshot_json FROM run_authorizations WHERE run_id=?1",
            params![parent.0],
            |r| r.get::<_, String>(0),
        )
        .optional()?;
    let Some(raw) = raw else {
        return Ok(());
    };
    let frozen: AuthorizationSnapshot =
        serde_json::from_str(&raw).map_err(|_| AuthorizationError::Forbidden)?;
    if frozen.context.scope != TenantContext::local() || frozen.attributes.workspace != snapshot.cwd
    {
        return Err(AuthorizationError::Forbidden.into());
    }
    let request_id = crate::read_run_in(db, &child.0)?
        .ok_or(AuthorizationError::Forbidden)?
        .request_id;
    let mut context = frozen.context;
    context.request_id = request_id;
    let request = AuthorizationRequest {
        context: context.clone(),
        expected_policy: frozen.policy.version.clone(),
        action: Action::SessionPrompt,
        attributes: frozen.attributes,
    };
    let identity = IdentityReadback {
        scope: context.scope,
        principal: context.principal,
        roles: frozen.roles,
        policy: frozen.policy,
        policy_digest: frozen.policy_digest,
        enterprise_ready: false,
    };
    freeze_in(db, child, snapshot, &request, &identity)
}

#[cfg(test)]
pub(crate) fn remove_v21_for_fixture(db: &Connection) {
    db.execute_batch("DELETE FROM role_bindings WHERE tenant_id='local-tenant' AND subject_id='local-cron-workload'; DELETE FROM principals WHERE tenant_id='local-tenant' AND subject_id='local-cron-workload'; DELETE FROM schema_migrations WHERE version=27; DROP TRIGGER native_event_provenance_insert; DROP TRIGGER immutable_event_provenance_update; DROP TRIGGER immutable_event_stamp_delete; ALTER TABLE events DROP COLUMN view_provenance; DELETE FROM schema_migrations WHERE version=26; DROP TRIGGER bind_lifetime_insert; DROP TRIGGER bind_lifetime_update; DROP TRIGGER immutable_run_incarnation; DROP TRIGGER immutable_lifetime_owner_update; DROP TRIGGER immutable_lifetime_owner_delete; DROP TABLE session_lifetime_owners; DELETE FROM schema_migrations WHERE version=25; DROP TABLE session_writer_leases; DROP INDEX session_scope_writer_key; DROP INDEX tenant_writer_domain; DELETE FROM schema_migrations WHERE version=24; DROP TRIGGER artifact_publication_immutable; DROP TRIGGER artifact_publication_monotonic; DROP TABLE artifact_publications; DELETE FROM schema_migrations WHERE version=23; DROP TRIGGER outbox_immutable; DROP TABLE publication_inbox; DROP TABLE event_outbox; DROP TABLE delivery_leases; DELETE FROM schema_migrations WHERE version=22;").unwrap();
    db.execute_batch("DROP TRIGGER local_session_scope; DROP TRIGGER immutable_policy; DROP TRIGGER immutable_run_authorization; DROP TABLE run_authorizations; DROP TABLE session_scopes; DROP TABLE identity_policy_heads; DROP TABLE identity_policies; DROP TABLE role_bindings; DROP TABLE principals; DROP TABLE agents; DROP TABLE projects; DROP TABLE tenants; DROP TABLE organizations; DELETE FROM schema_migrations WHERE version=21;").unwrap();
}

#[cfg(test)]
mod async_contract {
    use super::*;
    use agent_storage_ports::IdentityReadRepository;
    #[tokio::test(flavor = "current_thread")]
    async fn identity_async_port_yields_while_original_connection_is_occupied() {
        let root = std::env::temp_dir().join(format!("ma-identity-adapter-{}", std::process::id()));
        let store = RunStore::open(&root.join("runtime.sqlite3")).unwrap();
        let occupied = store.clone();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let holder = std::thread::spawn(move || {
            let db = occupied.lock_connection().unwrap();
            db.query_row("SELECT count(*) FROM principals", [], |r| {
                r.get::<_, u64>(0)
            })
            .unwrap();
            ready_tx.send(()).unwrap();
            // 有界失败保护；GREEN 在 timer 到期后主动释放，不用 sleep 猜竞争。
            let _ = release_rx.recv_timeout(std::time::Duration::from_secs(5));
            drop(db);
        });
        ready_rx.await.unwrap();
        let scope = TenantContext::local();
        let subject: SubjectId = "local-subject".to_owned().try_into().unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_millis(100), async {
            store.read_identity(&scope, &subject).await
        })
        .await;
        let _ = release_tx.send(());
        holder.join().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while store.blocking_budget.available_permits() != 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(store.read_identity(&scope, &subject).await.is_ok());
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
        assert!(
            result.is_err(),
            "中心身份查询必须让 Tokio 调度 timeout，而不是占用线程直到原连接释放：{result:?}"
        );
    }
}

/// 原事务已核execution时，读取同一不可变授权供审批/配置消费；不查询当前policy head。
pub(crate) fn authorization_snapshot_in(
    db: &Connection,
    env: &EnterpriseOwnerEnvelope,
) -> Result<Option<AuthorizationSnapshot>, RuntimeError> {
    check_session_scope_in(db, &env.scope, &env.owner.session_key)?;
    authorization_owner_in(db, env)?;
    let raw:Option<Option<String>>=db.query_row("SELECT CASE WHEN length(CAST(snapshot_json AS BLOB))<=65536 THEN snapshot_json END FROM run_authorizations WHERE tenant_id=?1 AND project_id=?2 AND agent_uid=?3 AND run_id=?4",params![env.scope.tenant_id.as_str(),env.scope.project_id.as_str(),env.scope.agent_uid.as_str(),env.owner.run_id.0],|r|r.get(0)).optional()?;
    raw.map(|raw| {
        let snapshot: AuthorizationSnapshot =
            serde_json::from_str(&raw.ok_or(CoordinationError::Corrupt)?)
                .map_err(|_| CoordinationError::Corrupt)?;
        env.fence(&snapshot.envelope)?;
        check_snapshot_in(db, &snapshot)?;
        Ok(snapshot)
    })
    .transpose()
}
