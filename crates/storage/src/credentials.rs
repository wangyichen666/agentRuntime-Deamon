use crate::{RunStore, RuntimeError, enterprise, now_ms};
use agent_core::*;
use agent_storage_ports::CredentialRepository;
use rusqlite::{Connection, OptionalExtension, params};
use std::sync::Arc;
fn authorize(
    db: &Connection,
    request: &AuthorizationRequest,
    verifier: &dyn AuthorizationVerifier,
) -> Result<(), RuntimeError> {
    if request.action != Action::IdentityManage {
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
fn read(
    db: &Connection,
    s: &TenantContext,
    id: Option<&str>,
    actor: &SubjectId,
    operation: &str,
) -> Result<Option<(ApiCredentialMetadata, String, String)>, RuntimeError> {
    let row=db.query_row("SELECT key_id,subject_id,subject_kind,revocation_epoch,expires_at_ms,disabled,CASE WHEN length(password_hash)<=256 THEN password_hash END,request_digest FROM api_credentials WHERE organization_id=?1 AND tenant_id=?2 AND project_id=?3 AND agent_uid=?4 AND ((?5 IS NOT NULL AND key_id=?5) OR (?5 IS NULL AND created_by=?6 AND operation_id=?7))",params![s.organization_id.as_str(),s.tenant_id.as_str(),s.project_id.as_str(),s.agent_uid.as_str(),id,actor.as_str(),operation],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,u64>(3)?,r.get::<_,i64>(4)?,r.get::<_,bool>(5)?,r.get::<_,Option<String>>(6)?,r.get::<_,String>(7)?))).optional()?;
    row.map(
        |(id, subject, kind, epoch, expiry, disabled, hash, digest)| {
            Ok((
                ApiCredentialMetadata {
                    key_id: id,
                    scope: s.clone(),
                    principal: AuthenticatedPrincipal {
                        subject_id: subject.try_into().map_err(|_| CoordinationError::Corrupt)?,
                        kind: serde_json::from_value(serde_json::Value::String(kind))
                            .map_err(|_| CoordinationError::Corrupt)?,
                        revocation_epoch: epoch,
                    },
                    expires_at_ms: expiry,
                    disabled,
                },
                hash.ok_or(CoordinationError::Corrupt)?,
                digest,
            ))
        },
    )
    .transpose()
}
fn valid_op(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256
}
#[async_trait::async_trait]
impl CredentialRepository for RunStore {
    async fn issue_api_credential(
        &self,
        request: &AuthorizationRequest,
        subject: &SubjectId,
        ttl_secs: u64,
        operation: &str,
        verifier: Arc<dyn AuthorizationVerifier>,
    ) -> Result<IssuedApiCredential, RepositoryError> {
        if !valid_op(operation) || !(60..=86400).contains(&ttl_secs) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (request, subject, operation) =
            (request.clone(), subject.clone(), operation.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(),move|store|{
            let run=||->Result<IssuedApiCredential,RuntimeError>{
                let mut db=store.lock_connection()?;let tx=db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;authorize(&tx,&request,verifier.as_ref())?;
                let s=&request.context.scope;let actor=&request.context.principal.subject_id;let digest=enterprise::hash(serde_json::to_string(&(&subject,ttl_secs)).map_err(|_|CoordinationError::InvalidRequest)?.as_bytes());
                if let Some((meta,_,prior))=read(&tx,s,None,actor,&operation)? {if prior!=digest{return Err(CoordinationError::Conflict.into())}return Ok(IssuedApiCredential {metadata:meta,token:None})}
                let target=enterprise::identity_in(&tx,s,&subject)?;
                if target.principal.kind!=SubjectKind::Human&&ttl_secs>900{return Err(CoordinationError::InvalidRequest.into())}
                let (id,secret,hash)=agent_identity::api_key::generate()?;
                let expires=now_ms().checked_add((ttl_secs*1000) as i64).ok_or(CoordinationError::InvalidRequest)?;
                let kind=serde_json::to_value(&target.principal.kind).map_err(|_|CoordinationError::Corrupt)?;let kind=kind.as_str().ok_or(CoordinationError::Corrupt)?;
                tx.execute("INSERT INTO api_credentials VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,0,?10,?11,?12,?13)",params![s.organization_id.as_str(),s.tenant_id.as_str(),s.project_id.as_str(),s.agent_uid.as_str(),id,subject.as_str(),kind,target.principal.revocation_epoch,expires,hash,actor.as_str(),operation,digest])?;
                let (meta,_,_)=read(&tx,s,Some(&id),actor,"")?.ok_or(CoordinationError::Corrupt)?;
                crate::ledgers::append_audit_in(&tx,&AuditCommand {scope:s.clone(),operation_id:format!("credential.issue:{}",enterprise::hash(format!("{}:{operation}",actor.as_str()).as_bytes())),subject_id:actor.clone(),action:"credential.issue".into(),resource:serde_json::json!({"key_id":id,"subject_id":subject,"expires_at_ms":expires}),outcome:"committed".into()},now_ms())?;
                tx.commit()?;Ok(IssuedApiCredential {metadata:meta,token:Some(SecretToken::new(format!("ak.{id}.{secret}"))?)})
            };run().map_err(RuntimeError::repository_error)
        }).await
    }
    async fn revoke_api_credential(
        &self,
        request: &AuthorizationRequest,
        key_id: &str,
        operation: &str,
        verifier: Arc<dyn AuthorizationVerifier>,
    ) -> Result<ApiCredentialMetadata, RepositoryError> {
        if !valid_op(operation)
            || key_id.len() != 32
            || !key_id.bytes().all(|v| v.is_ascii_hexdigit())
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (request, id, operation) = (request.clone(), key_id.to_owned(), operation.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(),move|store|{
            let run=||->Result<ApiCredentialMetadata,RuntimeError>{
                let mut db=store.lock_connection()?;let tx=db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;authorize(&tx,&request,verifier.as_ref())?;let s=&request.context.scope;let actor=&request.context.principal.subject_id;
                let prior:Option<(String,String,Option<String>)>=tx.query_row("SELECT key_id,actor_id,CASE WHEN length(result_json)<=4096 THEN result_json END FROM credential_revocations WHERE organization_id=?1 AND tenant_id=?2 AND project_id=?3 AND agent_uid=?4 AND operation_id=?5",params![s.organization_id.as_str(),s.tenant_id.as_str(),s.project_id.as_str(),s.agent_uid.as_str(),operation],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
                if let Some((key,old_actor,raw))=prior {if key!=id||old_actor!=actor.as_str(){return Err(CoordinationError::Conflict.into())}return serde_json::from_str(&raw.ok_or(CoordinationError::Corrupt)?).map_err(|_|CoordinationError::Corrupt.into())}
                let (mut meta,_,_)=read(&tx,s,Some(&id),actor,"")?.ok_or(AuthorizationError::Forbidden)?;
                if !meta.disabled {
                    let current=enterprise::identity_in(&tx,s,&meta.principal.subject_id)?;
                    let epoch=current.principal.revocation_epoch.checked_add(1).filter(|e|*e<=i64::MAX as u64).ok_or(CoordinationError::Conflict)?;
                    tx.execute("UPDATE principals SET revocation_epoch=?3 WHERE tenant_id=?1 AND subject_id=?2",params![s.tenant_id.as_str(),meta.principal.subject_id.as_str(),epoch])?;
                    tx.execute("UPDATE api_credentials SET disabled=1 WHERE organization_id=?1 AND tenant_id=?2 AND project_id=?3 AND agent_uid=?4 AND key_id=?5",params![s.organization_id.as_str(),s.tenant_id.as_str(),s.project_id.as_str(),s.agent_uid.as_str(),id])?;meta.disabled=true;
                }
                let raw=serde_json::to_string(&meta).map_err(|_|CoordinationError::Corrupt)?;
                tx.execute("INSERT INTO credential_revocations VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",params![s.organization_id.as_str(),s.tenant_id.as_str(),s.project_id.as_str(),s.agent_uid.as_str(),id,operation,actor.as_str(),raw])?;
                crate::ledgers::append_audit_in(&tx,&AuditCommand {scope:s.clone(),operation_id:format!("credential.revoke:{}",enterprise::hash(format!("{}:{operation}",actor.as_str()).as_bytes())),subject_id:actor.clone(),action:"credential.revoke".into(),resource:serde_json::json!({"key_id":id,"subject_id":meta.principal.subject_id}),outcome:"committed".into()},now_ms())?;
                tx.commit()?;Ok(meta)
            };run().map_err(RuntimeError::repository_error)
        }).await
    }
    async fn authenticate_api_credential(
        &self,
        scope: &TenantContext,
        token: &SecretToken,
    ) -> Result<ApiCredentialMetadata, RepositoryError> {
        let (id, secret) = agent_identity::api_key::parse(token.as_str())?;
        let (s, id, secret) = (scope.clone(), id.to_owned(), secret.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            let run = || -> Result<ApiCredentialMetadata, RuntimeError> {
                let mut db = store.lock_connection()?;
                let tx = db.transaction()?;
                let placeholder = "local-subject"
                    .to_owned()
                    .try_into()
                    .map_err(|_| CoordinationError::Corrupt)?;
                let (meta, hash, _) = read(&tx, &s, Some(&id), &placeholder, "")?
                    .ok_or(AuthorizationError::Unauthenticated)?;
                if meta.disabled || meta.expires_at_ms <= now_ms() {
                    return Err(AuthorizationError::Unauthenticated.into());
                }
                let identity = enterprise::identity_in(&tx, &s, &meta.principal.subject_id)?;
                if identity.principal != meta.principal {
                    return Err(AuthorizationError::Revoked.into());
                }
                agent_identity::api_key::verify(&secret, &hash)?;
                tx.commit()?;
                Ok(meta)
            };
            run().map_err(RuntimeError::repository_error)
        })
        .await
    }
}
