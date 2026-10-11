use crate::canonical::{field, integer, set_scope, unsigned};
use crate::{PgCanonicalStore, SCOPE, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::CredentialRepository;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio_postgres::Transaction;
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
async fn authorize(
    tx: &Transaction<'_>,
    request: &AuthorizationRequest,
    verifier: &dyn AuthorizationVerifier,
) -> Result<(), RepositoryError> {
    if request.action != Action::IdentityManage {
        return Err(AuthorizationError::Forbidden.into());
    }
    let identity = crate::identities::identity_in(
        tx,
        &request.context.scope,
        &request.context.principal.subject_id,
    )
    .await?;
    let mut checked = request.clone();
    checked.attributes.timestamp_ms = crate::now(tx).await?;
    verifier.verify(&checked, &identity)?;
    Ok(())
}
async fn read(
    tx: &Transaction<'_>,
    s: &TenantContext,
    id: Option<&str>,
    actor: &SubjectId,
    operation: &str,
) -> Result<Option<(ApiCredentialMetadata, String, String)>, RepositoryError> {
    let mut values = scope_values(s);
    values.extend([
        id.unwrap_or_default().into(),
        actor.as_str().into(),
        operation.into(),
    ]);
    tx.query_opt(&format!("SELECT key_id,subject_id,subject_kind,revocation_epoch,expires_at_ms,disabled,CASE WHEN octet_length(password_hash)<=256 THEN password_hash END,request_digest FROM api_credentials WHERE {SCOPE} AND (($5<>'' AND key_id=$5) OR ($5='' AND created_by=$6 AND operation_id=$7)) FOR SHARE"),&parameters(&values)).await.map_err(unavailable)?.map(|r|Ok((ApiCredentialMetadata {key_id:field(&r,0)?,scope:s.clone(),principal:AuthenticatedPrincipal {subject_id:field(&r,1)?.try_into().map_err(|_|CoordinationError::Corrupt)?,kind:serde_json::from_value(serde_json::Value::String(field(&r,2)?)).map_err(|_|CoordinationError::Corrupt)?,revocation_epoch:unsigned(r.try_get(3).map_err(unavailable)?)?},expires_at_ms:r.try_get(4).map_err(unavailable)?,disabled:r.try_get(5).map_err(unavailable)?},field(&r,6)?,field(&r,7)?))).transpose()
}
async fn management_lock(tx: &Transaction<'_>, s: &TenantContext) -> Result<(), RepositoryError> {
    let key = serde_json::to_string(&(&s.organization_id, &s.tenant_id))
        .map_err(|_| CoordinationError::InvalidRequest)?;
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,41))",
        &[&key],
    )
    .await
    .map_err(unavailable)?;
    Ok(())
}
fn valid_op(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256
}
#[async_trait::async_trait]
impl CredentialRepository for PgCanonicalStore {
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
        let permit = self
            .credential_budget
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| CoordinationError::StorageUnavailable)?;
        let s = &request.context.scope;
        let actor = &request.context.principal.subject_id;
        let mut db = self.inner.client.lock().await;
        let tx = db.transaction().await.map_err(unavailable)?;
        set_scope(&tx, s).await?;
        management_lock(&tx, s).await?;
        authorize(&tx, request, verifier.as_ref()).await?;
        let request_digest = digest(
            serde_json::to_string(&(subject, ttl_secs))
                .map_err(|_| CoordinationError::InvalidRequest)?
                .as_bytes(),
        );
        if let Some((meta, _, old)) = read(&tx, s, None, actor, operation).await? {
            if old != request_digest {
                return Err(CoordinationError::Conflict.into());
            }
            return Ok(IssuedApiCredential {
                metadata: meta,
                token: None,
            });
        }
        let target = crate::identities::identity_in(&tx, s, subject).await?;
        if target.principal.kind != SubjectKind::Human && ttl_secs > 900 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (id, secret, hash) = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            agent_identity::api_key::generate()
        })
        .await
        .map_err(|_| CoordinationError::StorageUnavailable)??;
        let expires = crate::now(&tx)
            .await?
            .checked_add((ttl_secs * 1000) as i64)
            .ok_or(CoordinationError::InvalidRequest)?;
        let kind =
            serde_json::to_value(&target.principal.kind).map_err(|_| CoordinationError::Corrupt)?;
        let mut values = scope_values(s);
        values.extend([
            id.clone(),
            subject.as_str().into(),
            kind.as_str().ok_or(CoordinationError::Corrupt)?.into(),
            integer(target.principal.revocation_epoch)?.to_string(),
            expires.to_string(),
            hash,
            actor.as_str().into(),
            operation.into(),
            request_digest,
        ]);
        tx.execute("INSERT INTO api_credentials VALUES($1,$2,$3,$4,$5,$6,$7,$8::text::bigint,$9::text::bigint,FALSE,$10,$11,$12,$13)",&parameters(&values)).await.map_err(unavailable)?;
        let (metadata, _, _) = read(&tx, s, Some(&id), actor, "")
            .await?
            .ok_or(CoordinationError::Corrupt)?;
        crate::ledgers::append_audit_in(&tx,&AuditCommand {scope:s.clone(),operation_id:format!("credential.issue:{}",digest(format!("{}:{operation}",actor.as_str()).as_bytes())),subject_id:actor.clone(),action:"credential.issue".into(),resource:serde_json::json!({"key_id":id,"subject_id":subject,"expires_at_ms":expires}),outcome:"committed".into()},crate::now(&tx).await?).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(IssuedApiCredential {
            metadata,
            token: Some(SecretToken::new(format!("ak.{id}.{secret}"))?),
        })
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
        let s = &request.context.scope;
        let actor = &request.context.principal.subject_id;
        let mut db = self.inner.client.lock().await;
        let tx = db.transaction().await.map_err(unavailable)?;
        set_scope(&tx, s).await?;
        management_lock(&tx, s).await?;
        authorize(&tx, request, verifier.as_ref()).await?;
        let mut values = scope_values(s);
        values.push(operation.into());
        if let Some(r)=tx.query_opt(&format!("SELECT key_id,actor_id,CASE WHEN octet_length(result_json)<=4096 THEN result_json END FROM credential_revocations WHERE {SCOPE} AND operation_id=$5"),&parameters(&values)).await.map_err(unavailable)? {
            if field(&r,0)?!=key_id||field(&r,1)?!=actor.as_str(){return Err(CoordinationError::Conflict.into())}return serde_json::from_str(&field(&r,2)?).map_err(|_|CoordinationError::Corrupt.into());
        }
        let (mut meta, _, _) = read(&tx, s, Some(key_id), actor, "")
            .await?
            .ok_or(AuthorizationError::Forbidden)?;
        if !meta.disabled {
            let target = crate::identities::identity_in(&tx, s, &meta.principal.subject_id).await?;
            let epoch = target
                .principal
                .revocation_epoch
                .checked_add(1)
                .filter(|e| *e <= i64::MAX as u64)
                .ok_or(CoordinationError::Conflict)?;
            tx.execute("UPDATE principals SET revocation_epoch=$4 WHERE organization_id=$1 AND tenant_id=$2 AND subject_id=$3",&[&s.organization_id.as_str(),&s.tenant_id.as_str(),&meta.principal.subject_id.as_str(),&integer(epoch)?]).await.map_err(unavailable)?;
            let mut binding = scope_values(s);
            binding.push(key_id.into());
            tx.execute(
                &format!("UPDATE api_credentials SET disabled=TRUE WHERE {SCOPE} AND key_id=$5"),
                &parameters(&binding),
            )
            .await
            .map_err(unavailable)?;
            meta.disabled = true;
        }
        let mut binding = scope_values(s);
        binding.extend([
            key_id.into(),
            operation.into(),
            actor.as_str().into(),
            serde_json::to_string(&meta).map_err(|_| CoordinationError::Corrupt)?,
        ]);
        tx.execute(
            "INSERT INTO credential_revocations VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
            &parameters(&binding),
        )
        .await
        .map_err(unavailable)?;
        crate::ledgers::append_audit_in(&tx,&AuditCommand {scope:s.clone(),operation_id:format!("credential.revoke:{}",digest(format!("{}:{operation}",actor.as_str()).as_bytes())),subject_id:actor.clone(),action:"credential.revoke".into(),resource:serde_json::json!({"key_id":key_id,"subject_id":meta.principal.subject_id}),outcome:"committed".into()},crate::now(&tx).await?).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(meta)
    }
    async fn authenticate_api_credential(
        &self,
        scope: &TenantContext,
        token: &SecretToken,
    ) -> Result<ApiCredentialMetadata, RepositoryError> {
        let (id, secret) = agent_identity::api_key::parse(token.as_str())?;
        let permit = self
            .credential_budget
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| CoordinationError::StorageUnavailable)?;
        let mut db = self.inner.client.lock().await;
        let tx = db.transaction().await.map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let placeholder = RuntimeIdentity::local().principal.subject_id;
        let (meta, hash, _) = read(&tx, scope, Some(id), &placeholder, "")
            .await?
            .ok_or(AuthorizationError::Unauthenticated)?;
        if meta.disabled || meta.expires_at_ms <= crate::now(&tx).await? {
            return Err(AuthorizationError::Unauthenticated.into());
        }
        let identity =
            crate::identities::identity_in(&tx, scope, &meta.principal.subject_id).await?;
        if identity.principal != meta.principal {
            return Err(AuthorizationError::Revoked.into());
        }
        let secret = secret.to_owned();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            agent_identity::api_key::verify(&secret, &hash)
        })
        .await
        .map_err(|_| CoordinationError::StorageUnavailable)??;
        tx.commit().await.map_err(unavailable)?;
        Ok(meta)
    }
}
