use crate::canonical::{field, integer, set_scope, unsigned};
use crate::{PgCanonicalStore, SCOPE, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::AccessRepository;
use std::sync::Arc;
use tokio_postgres::Transaction;
async fn authorize(
    tx: &Transaction<'_>,
    request: &AuthorizationRequest,
    action: Action,
    verifier: &dyn AuthorizationVerifier,
) -> Result<(), RepositoryError> {
    if request.action != action {
        return Err(AuthorizationError::Forbidden.into());
    }
    let identity = crate::identities::identity_in(
        tx,
        &request.context.scope,
        &request.context.principal.subject_id,
    )
    .await?;
    let mut current = request.clone();
    current.attributes.timestamp_ms = crate::now(tx).await?;
    verifier.verify(&current, &identity)?;
    Ok(())
}
async fn route(
    tx: &Transaction<'_>,
    scope: &TenantContext,
    external: Option<&str>,
    actor: &SubjectId,
    operation: &str,
) -> Result<Option<DirectorySession>, RepositoryError> {
    let mut values = scope_values(scope);
    values.extend([
        external.unwrap_or_default().into(),
        actor.as_str().into(),
        operation.into(),
    ]);
    tx.query_opt(&format!("SELECT external_id,session_id,lifetime,runtime_endpoint,created_by,operation_id FROM session_directory WHERE {SCOPE} AND (($5<>'' AND external_id=$5) OR ($5='' AND created_by=$6 AND operation_id=$7))"),&parameters(&values)).await.map_err(unavailable)?.map(|r|Ok(DirectorySession {external_id:field(&r,0)?,scope:scope.clone(),session_key:SessionKey(field(&r,1)?),session_lifetime:SessionLifetimeId(field(&r,2)?),runtime_endpoint:field(&r,3)?,created_by:field(&r,4)?.try_into().map_err(|_|CoordinationError::Corrupt)?,operation_id:field(&r,5)?})).transpose()
}
fn operation_valid(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256
}
#[async_trait::async_trait]
impl AccessRepository for PgCanonicalStore {
    async fn find_directory_session(&self,request:&AuthorizationRequest,operation:&str,verifier:Arc<dyn AuthorizationVerifier>) -> Result<Option<DirectorySession>,RepositoryError> {
        if !operation_valid(operation) {return Err(CoordinationError::InvalidRequest.into());}
        let mut db=self.inner.client.lock().await;
        let tx=db.transaction().await.map_err(unavailable)?;
        set_scope(&tx,&request.context.scope).await?;
        authorize(&tx,request,Action::AgentRead,verifier.as_ref()).await?;
        let result=route(&tx,&request.context.scope,None,&request.context.principal.subject_id,operation).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
    async fn oauth_login(&self, request: &AuthorizationRequest, proof: &OAuthLoginProof, consume: bool, verifier: Arc<dyn AuthorizationVerifier>) -> Result<(), RepositoryError> {
        if !proof.valid() { return Err(CoordinationError::InvalidRequest.into()); }
        let mut db = self.inner.client.lock().await;
        let tx = db.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &request.context.scope).await?;
        authorize(&tx, request, Action::GatewayDelegate, verifier.as_ref()).await?;
        let scope = &request.context.scope;
        tx.execute("SELECT pg_advisory_xact_lock(hashtextextended($1,62))", &[&serde_json::to_string(&(&scope.organization_id,&scope.tenant_id)).map_err(|_|CoordinationError::Corrupt)?]).await.map_err(unavailable)?;
        let now = crate::now(&tx).await?;
        tx.execute("DELETE FROM oauth_logins WHERE organization_id=$1 AND tenant_id=$2 AND expires_at_ms<=$3", &[&scope.organization_id.as_str(),&scope.tenant_id.as_str(),&now]).await.map_err(unavailable)?;
        let mut values = scope_values(scope);
        values.extend([proof.state_digest.clone(),proof.nonce_digest.clone(),proof.verifier_digest.clone(),request.context.principal.subject_id.as_str().into(),request.context.principal.revocation_epoch.to_string(),(now+300_000).to_string()]);
        if consume {
            values[9] = now.to_string();
            let changed = tx.execute(&format!("UPDATE oauth_logins SET consumed=true WHERE {SCOPE} AND state_digest=$5 AND nonce_digest=$6 AND verifier_digest=$7 AND actor_id=$8 AND actor_epoch=$9::text::bigint AND consumed=false AND expires_at_ms>$10::text::bigint"), &parameters(&values)).await.map_err(unavailable)?;
            if changed != 1 { return Err(AuthorizationError::Unauthenticated.into()); }
        } else {
            let count: i64 = tx.query_one("SELECT count(*) FROM oauth_logins WHERE organization_id=$1 AND tenant_id=$2", &[&scope.organization_id.as_str(),&scope.tenant_id.as_str()]).await.map_err(unavailable)?.get(0);
            if count >= 1024 { return Err(LedgerError::RateLimited.into()); }
            tx.execute("INSERT INTO oauth_logins VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::text::bigint,$10::text::bigint,false)", &parameters(&values)).await.map_err(unavailable)?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(())
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
        let s = &request.context.scope;
        let actor = &request.context.principal.subject_id;
        let mut db = self.inner.client.lock().await;
        let tx = db.transaction().await.map_err(unavailable)?;
        set_scope(&tx, s).await?;
        let lock = serde_json::to_string(&(s, actor, operation))
            .map_err(|_| CoordinationError::InvalidRequest)?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,60))",
            &[&lock],
        )
        .await
        .map_err(unavailable)?;
        authorize(&tx, request, Action::SessionCreate, verifier.as_ref()).await?;
        if let Some(prior) = route(&tx, s, None, actor, operation).await? {
            if prior.session_key != *key || prior.runtime_endpoint != endpoint {
                return Err(CoordinationError::Conflict.into());
            }
            return Ok(prior);
        }
        let meta = crate::sessions::create_in(&tx, s, key).await?;
        let external: String = tx
            .query_one("SELECT 'ds-'||gen_random_uuid()::text", &[])
            .await
            .map_err(unavailable)?
            .try_get(0)
            .map_err(unavailable)?;
        let mut values = scope_values(s);
        values.extend([
            external.clone(),
            key.0.clone(),
            meta.lifetime.0.clone(),
            endpoint.into(),
            actor.as_str().into(),
            operation.into(),
        ]);
        tx.execute(
            "INSERT INTO session_directory VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
            &parameters(&values),
        )
        .await
        .map_err(unavailable)?;
        let result = route(&tx, s, Some(&external), actor, "")
            .await?
            .ok_or(CoordinationError::Corrupt)?;
        use sha2::{Digest, Sha256};
        crate::ledgers::append_audit_in(
            &tx,
            &AuditCommand {
                scope: s.clone(),
                operation_id: format!("directory:{:x}", Sha256::digest(format!("{}:{operation}",actor.as_str()).as_bytes())),
                subject_id: actor.clone(),
                action: "directory.create".into(),
                resource: serde_json::json!({"external_id":external}),
                outcome: "committed".into(),
            },
            crate::now(&tx).await?,
        )
        .await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
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
        let mut db = self.inner.client.lock().await;
        let tx = db.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &request.context.scope).await?;
        authorize(&tx, request, Action::AgentRead, verifier.as_ref()).await?;
        let result = route(
            &tx,
            &request.context.scope,
            Some(external),
            &request.context.principal.subject_id,
            "",
        )
        .await?
        .ok_or(AuthorizationError::Forbidden)?;
        let head = crate::sessions::metadata_in(&tx, &request.context.scope, &result.session_key, true).await?
            .ok_or(AuthorizationError::Forbidden)?;
        if head.deleted || head.lifetime != result.session_lifetime {
            return Err(AuthorizationError::Forbidden.into());
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
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
        let s = &request.context.scope;
        let actor = &request.context.principal;
        let mut db = self.inner.client.lock().await;
        let tx = db.transaction().await.map_err(unavailable)?;
        set_scope(&tx, s).await?;
        let lock = serde_json::to_string(&(&s.organization_id, &s.tenant_id))
            .map_err(|_| CoordinationError::InvalidRequest)?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,61))",
            &[&lock],
        )
        .await
        .map_err(unavailable)?;
        authorize(&tx, request, Action::AgentRead, verifier.as_ref()).await?;
        let at = crate::now(&tx).await?;
        let window = at - at.rem_euclid(60000);
        let tenant = [s.organization_id.as_str(), s.tenant_id.as_str()];
        if let Some(row)=tx.query_opt("SELECT project_id,agent_uid,subject_id,revocation_epoch,class,expires_at_ms,endpoint FROM access_leases WHERE organization_id=$1 AND tenant_id=$2 AND operation_id=$3",&[&tenant[0],&tenant[1],&operation]).await.map_err(unavailable)? {
            let epoch=unsigned(row.try_get(3).map_err(unavailable)?)?;let expires:i64=row.try_get(5).map_err(unavailable)?;
            if field(&row,0)?!=s.project_id.as_str()||field(&row,1)?!=s.agent_uid.as_str()||field(&row,2)?!=actor.subject_id.as_str()||epoch!=actor.revocation_epoch||field(&row,4)?!=class.as_str()||field(&row,6)?!=endpoint{return Err(CoordinationError::Conflict.into())}
            if expires<=at{return Err(CoordinationError::StaleLease.into())}
            return Ok(AccessLease {operation_id:operation.into(),scope:s.clone(),subject_id:actor.subject_id.clone(),revocation_epoch:epoch,class,expires_at_ms:expires});
        }
        tx.execute("DELETE FROM access_leases WHERE organization_id=$1 AND tenant_id=$2 AND expires_at_ms<=$3",&[&tenant[0],&tenant[1],&at]).await.map_err(unavailable)?;
        tx.execute("DELETE FROM access_counters WHERE organization_id=$1 AND tenant_id=$2 AND window_start_ms<$3",&[&tenant[0],&tenant[1],&(window-3600000)]).await.map_err(unavailable)?;
        let cardinality:i64=tx.query_one("SELECT count(*)::bigint FROM access_counters WHERE organization_id=$1 AND tenant_id=$2",&[&tenant[0],&tenant[1]]).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
        if cardinality >= 4096 {
            return Err(LedgerError::RateLimited.into());
        }
        let buckets = [
            "tenant".to_owned(),
            format!("subject:{}", actor.subject_id.as_str()),
            format!("endpoint:{endpoint}"),
        ];
        for (index, bucket) in buckets.iter().enumerate() {
            let count=tx.query_opt("SELECT count FROM access_counters WHERE organization_id=$1 AND tenant_id=$2 AND bucket=$3 AND window_start_ms=$4",&[&tenant[0],&tenant[1],bucket,&window]).await.map_err(unavailable)?.map(|r|r.try_get::<_,i64>(0).map_err(unavailable)).transpose()?.unwrap_or(0);
            if unsigned(count)? >= ACCESS_RATE_LIMITS[index] {
                return Err(LedgerError::RateLimited.into());
            }
            let active:i64=tx.query_one("SELECT count(*)::bigint FROM access_leases WHERE organization_id=$1 AND tenant_id=$2 AND expires_at_ms>$3 AND ($4=0 OR ($4=1 AND subject_id=$5) OR ($4=2 AND endpoint=$6))",&[&tenant[0],&tenant[1],&at,&(index as i32),&actor.subject_id.as_str(),&endpoint]).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
            if unsigned(active)? >= ACCESS_CONCURRENCY_LIMITS[index] {
                return Err(LedgerError::RateLimited.into());
            }
        }
        let expires = at.checked_add(300000).ok_or(CoordinationError::Corrupt)?;
        for bucket in buckets {
            tx.execute("INSERT INTO access_counters VALUES($1,$2,$3,$4,1) ON CONFLICT(organization_id,tenant_id,bucket) DO UPDATE SET count=CASE WHEN access_counters.window_start_ms=excluded.window_start_ms THEN access_counters.count+1 ELSE 1 END,window_start_ms=excluded.window_start_ms",&[&tenant[0],&tenant[1],&bucket,&window]).await.map_err(unavailable)?;
        }
        let mut values = scope_values(s);
        values.extend([
            operation.into(),
            actor.subject_id.as_str().into(),
            integer(actor.revocation_epoch)?.to_string(),
            class.as_str().into(),
            expires.to_string(),
            endpoint.into(),
        ]);
        tx.execute("INSERT INTO access_leases VALUES($1,$2,$3,$4,$5,$6,$7::text::bigint,$8,$9::text::bigint,$10)",&parameters(&values)).await.map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(AccessLease {
            operation_id: operation.into(),
            scope: s.clone(),
            subject_id: actor.subject_id.clone(),
            revocation_epoch: actor.revocation_epoch,
            class,
            expires_at_ms: expires,
        })
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
        let s = &request.context.scope;
        let mut db = self.inner.client.lock().await;
        let tx = db.transaction().await.map_err(unavailable)?;
        set_scope(&tx, s).await?;
        authorize(&tx, request, Action::AgentRead, verifier.as_ref()).await?;
        let mut values = scope_values(s);
        values.extend([
            operation.into(),
            request.context.principal.subject_id.as_str().into(),
            integer(request.context.principal.revocation_epoch)?.to_string(),
        ]);
        tx.execute(&format!("UPDATE access_leases SET expires_at_ms=0 WHERE {SCOPE} AND operation_id=$5 AND subject_id=$6 AND revocation_epoch=$7::text::bigint"),&parameters(&values)).await.map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }
}
