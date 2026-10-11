use crate::{RunStore, RuntimeError, enterprise, now_ms, sessions};
use agent_core::*;
use agent_storage_ports::AccessRepository;
use rusqlite::{Connection, OptionalExtension, params};
use std::sync::Arc;

fn authorize(
    db: &Connection,
    request: &AuthorizationRequest,
    action: Action,
    verifier: &dyn AuthorizationVerifier,
) -> Result<(), RuntimeError> {
    if request.action != action {
        return Err(AuthorizationError::Forbidden.into());
    }
    let identity = enterprise::identity_in(
        db,
        &request.context.scope,
        &request.context.principal.subject_id,
    )?;
    let mut checked = request.clone();
    checked.attributes.timestamp_ms = now_ms();
    verifier.verify(&checked, &identity)?;
    Ok(())
}
fn operation_valid(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256
}
fn route(
    db: &Connection,
    scope: &TenantContext,
    external: Option<&str>,
    actor: &SubjectId,
    operation: &str,
) -> Result<Option<DirectorySession>, RuntimeError> {
    let row=db.query_row("SELECT external_id,session_id,lifetime,runtime_endpoint,created_by,operation_id FROM session_directory WHERE organization_id=?1 AND tenant_id=?2 AND project_id=?3 AND agent_uid=?4 AND ((?5 IS NOT NULL AND external_id=?5) OR (?5 IS NULL AND created_by=?6 AND operation_id=?7))",params![scope.organization_id.as_str(),scope.tenant_id.as_str(),scope.project_id.as_str(),scope.agent_uid.as_str(),external,actor.as_str(),operation],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?,r.get::<_,String>(4)?,r.get::<_,String>(5)?))).optional()?;
    row.map(|(id, key, life, endpoint, actor, op)| {
        Ok(DirectorySession {
            external_id: id,
            scope: scope.clone(),
            session_key: SessionKey(key),
            session_lifetime: SessionLifetimeId(life),
            runtime_endpoint: endpoint,
            created_by: actor.try_into().map_err(|_| CoordinationError::Corrupt)?,
            operation_id: op,
        })
    })
    .transpose()
}
#[async_trait::async_trait]
impl AccessRepository for RunStore {
    async fn find_directory_session(&self,request:&AuthorizationRequest,operation:&str,verifier:Arc<dyn AuthorizationVerifier>) -> Result<Option<DirectorySession>,RepositoryError> {
        if !operation_valid(operation) {return Err(CoordinationError::InvalidRequest.into());}
        let (request,operation)=(request.clone(),operation.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(),move|store| {
            let execute=||->Result<Option<DirectorySession>,RuntimeError> {
                let db=store.lock_connection()?;
                authorize(&db,&request,Action::AgentRead,verifier.as_ref())?;
                route(&db,&request.context.scope,None,&request.context.principal.subject_id,&operation)
            };
            execute().map_err(RuntimeError::repository_error)
        }).await
    }
    async fn oauth_login(&self, request: &AuthorizationRequest, proof: &OAuthLoginProof, consume: bool, verifier: Arc<dyn AuthorizationVerifier>) -> Result<(), RepositoryError> {
        if !proof.valid() { return Err(CoordinationError::InvalidRequest.into()); }
        let (request, proof) = (request.clone(), proof.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            let execute = || -> Result<(), RuntimeError> {
                let mut db = store.lock_connection()?;
                let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                authorize(&tx, &request, Action::GatewayDelegate, verifier.as_ref())?;
                let scope = &request.context.scope;
                let principal = &request.context.principal;
                let now = now_ms();
                tx.execute("DELETE FROM oauth_logins WHERE organization_id=?1 AND tenant_id=?2 AND project_id=?3 AND agent_uid=?4 AND expires_at_ms<=?5", params![scope.organization_id.as_str(),scope.tenant_id.as_str(),scope.project_id.as_str(),scope.agent_uid.as_str(),now])?;
                if consume {
                    let updated = tx.execute("UPDATE oauth_logins SET consumed=1 WHERE organization_id=?1 AND tenant_id=?2 AND project_id=?3 AND agent_uid=?4 AND state_digest=?5 AND nonce_digest=?6 AND verifier_digest=?7 AND actor_id=?8 AND actor_epoch=?9 AND consumed=0 AND expires_at_ms>?10", params![scope.organization_id.as_str(),scope.tenant_id.as_str(),scope.project_id.as_str(),scope.agent_uid.as_str(),proof.state_digest,proof.nonce_digest,proof.verifier_digest,principal.subject_id.as_str(),principal.revocation_epoch,now])?;
                    if updated != 1 { return Err(AuthorizationError::Unauthenticated.into()); }
                } else {
                    let count: i64 = tx.query_row("SELECT count(*) FROM oauth_logins WHERE organization_id=?1 AND tenant_id=?2 AND project_id=?3 AND agent_uid=?4", params![scope.organization_id.as_str(),scope.tenant_id.as_str(),scope.project_id.as_str(),scope.agent_uid.as_str()], |row| row.get(0))?;
                    if count >= 1024 { return Err(LedgerError::RateLimited.into()); }
                    // 随机 state 的碰撞/重放拒绝，客户端应新建一次登录，不复用旧授权码。
                    tx.execute("INSERT INTO oauth_logins VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,0)", params![scope.organization_id.as_str(),scope.tenant_id.as_str(),scope.project_id.as_str(),scope.agent_uid.as_str(),proof.state_digest,proof.nonce_digest,proof.verifier_digest,principal.subject_id.as_str(),principal.revocation_epoch,now+300_000])?;
                }
                tx.commit()?;
                Ok(())
            };
            execute().map_err(RuntimeError::repository_error)
        }).await
    }
    async fn create_directory_session(
        &self,
        request: &AuthorizationRequest,
        key: &SessionKey,
        endpoint: &str,
        operation: &str,
        verifier: Arc<dyn AuthorizationVerifier>,
    ) -> Result<DirectorySession, RepositoryError> {
        if !operation_valid(operation)
            || key.0.is_empty()
            || key.0.len() > 1024
            || endpoint.is_empty()
            || endpoint.len() > 128
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (request, key, endpoint, operation) = (
            request.clone(),
            key.clone(),
            endpoint.to_owned(),
            operation.to_owned(),
        );
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            let run = || -> Result<DirectorySession, RuntimeError> {
                let mut db = store.lock_connection()?;
                let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                authorize(&tx, &request, Action::SessionCreate, verifier.as_ref())?;
                let s = &request.context.scope;
                let actor = &request.context.principal.subject_id;
                if let Some(prior) = route(&tx, s, None, actor, &operation)? {
                    if prior.session_key != key || prior.runtime_endpoint != endpoint {
                        return Err(CoordinationError::Conflict.into());
                    }
                    return Ok(prior);
                }
                let meta = sessions::create_in(&tx, &key)?;
                let external: String =
                    tx.query_row("SELECT 'ds-'||lower(hex(randomblob(16)))", [], |r| r.get(0))?;
                tx.execute(
                    "INSERT INTO session_directory VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                    params![
                        s.organization_id.as_str(),
                        s.tenant_id.as_str(),
                        s.project_id.as_str(),
                        s.agent_uid.as_str(),
                        external,
                        key.0,
                        meta.lifetime.0,
                        endpoint,
                        actor.as_str(),
                        operation
                    ],
                )?;
                let result =
                    route(&tx, s, Some(&external), actor, "")?.ok_or(CoordinationError::Corrupt)?;
                crate::ledgers::append_audit_in(
                    &tx,
                    &AuditCommand {
                        scope: s.clone(),
                        operation_id: format!(
                            "directory:{}",
                            enterprise::hash(format!("{}:{operation}",actor.as_str()).as_bytes())
                        ),
                        subject_id: actor.clone(),
                        action: "directory.create".into(),
                        resource: serde_json::json!({"external_id":external}),
                        outcome: "committed".into(),
                    },
                    now_ms(),
                )?;
                tx.commit()?;
                Ok(result)
            };
            run().map_err(RuntimeError::repository_error)
        })
        .await
    }
    async fn resolve_directory_session(
        &self,
        request: &AuthorizationRequest,
        external: &str,
        verifier: Arc<dyn AuthorizationVerifier>,
    ) -> Result<DirectorySession, RepositoryError> {
        if !valid_directory_id(external) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (request, external) = (request.clone(), external.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            let run = || -> Result<DirectorySession, RuntimeError> {
                let db = store.lock_connection()?;
                authorize(&db, &request, Action::AgentRead, verifier.as_ref())?;
                let result = route(
                    &db,
                    &request.context.scope,
                    Some(&external),
                    &request.context.principal.subject_id,
                    "",
                )?
                .ok_or(AuthorizationError::Forbidden)?;
                let head = super::sessions::scoped_metadata_in(&db, &request.context.scope, &result.session_key)?;
                if head.lifetime != result.session_lifetime {
                    return Err(AuthorizationError::Forbidden.into());
                }
                Ok(result)
            };
            run().map_err(RuntimeError::repository_error)
        })
        .await
    }
    async fn acquire_access_at(
        &self,
        request: &AuthorizationRequest,
        class: AccessClass,
        endpoint:&str,
        operation: &str,
        verifier: Arc<dyn AuthorizationVerifier>,
    ) -> Result<AccessLease, RepositoryError> {
        if !operation_valid(operation) || !valid_access_endpoint(endpoint) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (request, operation,endpoint) = (request.clone(), operation.to_owned(),endpoint.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(),move|store|{
            let run=||->Result<AccessLease,RuntimeError>{
                let mut db=store.lock_connection()?;let tx=db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                authorize(&tx,&request,Action::AgentRead,verifier.as_ref())?;
                let s=&request.context.scope;let actor=&request.context.principal;let at=now_ms();let window=at-at.rem_euclid(60000);
                let old:Option<(String,String,String,String,u64,String,i64,String)>=tx.query_row("SELECT project_id,agent_uid,operation_id,subject_id,revocation_epoch,class,expires_at_ms,endpoint FROM access_leases WHERE organization_id=?1 AND tenant_id=?2 AND operation_id=?3",params![s.organization_id.as_str(),s.tenant_id.as_str(),operation],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?))).optional()?;
                if let Some((project,agent,_,subject,epoch,old_class,expires,old_endpoint))=old {
                    if project!=s.project_id.as_str()||agent!=s.agent_uid.as_str()||subject!=actor.subject_id.as_str()||epoch!=actor.revocation_epoch||old_class!=class.as_str()||old_endpoint!=endpoint{return Err(CoordinationError::Conflict.into())}
                    if expires<=at {return Err(CoordinationError::StaleLease.into())}
                    return Ok(AccessLease {operation_id:operation,scope:s.clone(),subject_id:actor.subject_id.clone(),revocation_epoch:epoch,class,expires_at_ms:expires});
                }
                tx.execute("DELETE FROM access_leases WHERE organization_id=?1 AND tenant_id=?2 AND expires_at_ms<=?3",params![s.organization_id.as_str(),s.tenant_id.as_str(),at])?;
                tx.execute("DELETE FROM access_counters WHERE organization_id=?1 AND tenant_id=?2 AND window_start_ms<?3",params![s.organization_id.as_str(),s.tenant_id.as_str(),window-3600000])?;
                let cardinality:u64=tx.query_row("SELECT count(*) FROM access_counters WHERE organization_id=?1 AND tenant_id=?2",params![s.organization_id.as_str(),s.tenant_id.as_str()],|r|r.get(0))?;
                if cardinality>=4096 {return Err(LedgerError::RateLimited.into())}
                let buckets=["tenant".to_owned(),format!("subject:{}",actor.subject_id.as_str()),format!("endpoint:{endpoint}")];
                for (index,bucket) in buckets.iter().enumerate() {
                    let count:Option<u64>=tx.query_row("SELECT count FROM access_counters WHERE organization_id=?1 AND tenant_id=?2 AND bucket=?3 AND window_start_ms=?4",params![s.organization_id.as_str(),s.tenant_id.as_str(),bucket,window],|r|r.get(0)).optional()?;
                    if count.unwrap_or(0)>=ACCESS_RATE_LIMITS[index] {return Err(LedgerError::RateLimited.into())}
                    let active:u64=tx.query_row("SELECT count(*) FROM access_leases WHERE organization_id=?1 AND tenant_id=?2 AND expires_at_ms>?3 AND (?4=0 OR (?4=1 AND subject_id=?5) OR (?4=2 AND endpoint=?6))",params![s.organization_id.as_str(),s.tenant_id.as_str(),at,index as i64,actor.subject_id.as_str(),endpoint],|r|r.get(0))?;
                    if active>=ACCESS_CONCURRENCY_LIMITS[index] {return Err(LedgerError::RateLimited.into())}
                }
                let expires=at.checked_add(300000).ok_or(CoordinationError::Corrupt)?;
                for bucket in buckets {tx.execute("INSERT INTO access_counters VALUES(?1,?2,?3,?4,1) ON CONFLICT(organization_id,tenant_id,bucket) DO UPDATE SET count=CASE WHEN window_start_ms=excluded.window_start_ms THEN count+1 ELSE 1 END,window_start_ms=excluded.window_start_ms",params![s.organization_id.as_str(),s.tenant_id.as_str(),bucket,window])?;}
                tx.execute("INSERT INTO access_leases VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",params![s.organization_id.as_str(),s.tenant_id.as_str(),s.project_id.as_str(),s.agent_uid.as_str(),operation,actor.subject_id.as_str(),actor.revocation_epoch,class.as_str(),expires,endpoint])?;
                let result=AccessLease {operation_id:operation,scope:s.clone(),subject_id:actor.subject_id.clone(),revocation_epoch:actor.revocation_epoch,class,expires_at_ms:expires};tx.commit()?;Ok(result)
            };run().map_err(RuntimeError::repository_error)
        }).await
    }
    async fn release_access(
        &self,
        request: &AuthorizationRequest,
        operation: &str,
        verifier: Arc<dyn AuthorizationVerifier>,
    ) -> Result<(), RepositoryError> {
        if !operation_valid(operation) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (request, operation) = (request.clone(), operation.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(),move|store|{
            let run=||->Result<(),RuntimeError>{let mut db=store.lock_connection()?;let tx=db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;authorize(&tx,&request,Action::AgentRead,verifier.as_ref())?;
                let s=&request.context.scope;tx.execute("UPDATE access_leases SET expires_at_ms=0 WHERE organization_id=?1 AND tenant_id=?2 AND project_id=?3 AND agent_uid=?4 AND operation_id=?5 AND subject_id=?6 AND revocation_epoch=?7",params![s.organization_id.as_str(),s.tenant_id.as_str(),s.project_id.as_str(),s.agent_uid.as_str(),operation,request.context.principal.subject_id.as_str(),request.context.principal.revocation_epoch])?;tx.commit()?;Ok(())};run().map_err(RuntimeError::repository_error)
        }).await
    }
}
