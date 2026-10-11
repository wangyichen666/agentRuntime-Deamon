//! 受治理身份管理，与原主体、策略、角色表和审计共用事务。
use crate::canonical::{set_scope, unsigned};
use crate::identities::identity_in;
use crate::sessions::encode;
use crate::{PgCanonicalStore, SCOPE, parameters, scope_values, unavailable};
use agent_core::*;
use sha2::{Digest, Sha256};
#[async_trait::async_trait]
impl agent_storage_ports::IdentityManagementRepository for PgCanonicalStore {
    async fn manage_identity(
        &self,
        request: &AuthorizationRequest,
        command: &IdentityCommand,
        operation: &str,
        verifier: std::sync::Arc<dyn AuthorizationVerifier>,
    ) -> Result<IdentityManagementReceipt, RepositoryError> {
        if request.action != Action::IdentityManage || operation.is_empty() || operation.len() > 256
        {
            return Err(AuthorizationError::Forbidden.into());
        }
        let raw = encode(command, 1048576)?;
        let digest = format!("{:x}", Sha256::digest(raw.as_bytes()));
        let s = &request.context.scope;
        let actor = &request.context.principal.subject_id;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, s).await?;
        // 先序列化租户身份写入，再锁actor/head/target；防互相撤销的反向锁序。
        let tenant = encode(&(&s.organization_id, &s.tenant_id), 4096)?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,41))",
            &[&tenant],
        )
        .await
        .map_err(unavailable)?;
        let identity = identity_in(&tx, s, actor).await?;
        let mut checked = request.clone();
        checked.attributes.timestamp_ms = crate::now(&tx).await?;
        verifier.verify(&checked, &identity)?;
        let mut v = scope_values(s);
        v.push(operation.into());
        if let Some(saved)=tx.query_opt(&format!("SELECT command_digest,actor_id,CASE WHEN octet_length(result_json)<=65536 THEN result_json END FROM identity_management_receipts WHERE {SCOPE} AND operation_id=$5"),&parameters(&v)).await.map_err(unavailable)? {
            if saved.try_get::<_,String>(0).map_err(unavailable)?!=digest || saved.try_get::<_,String>(1).map_err(unavailable)?!=actor.as_str(){return Err(CoordinationError::Conflict.into());}
            let result=serde_json::from_str(&crate::canonical::field(&saved,2)?).map_err(|_|CoordinationError::Corrupt)?;
            tx.commit().await.map_err(unavailable)?;return Ok(result);
        }
        let subject = match command {
            IdentityCommand::RegisterSubject { principal } => {
                if principal.revocation_epoch != 0 {
                    return Err(CoordinationError::InvalidRequest.into());
                }
                let kind = serde_json::to_value(principal.kind)
                    .map_err(|_| CoordinationError::InvalidRequest)?;
                let kind = kind.as_str().ok_or(CoordinationError::InvalidRequest)?;
                let changed = tx
                    .execute(
                        "INSERT INTO principals VALUES($1,$2,$3,$4,0) ON CONFLICT DO NOTHING",
                        &[
                            &s.organization_id.as_str(),
                            &s.tenant_id.as_str(),
                            &principal.subject_id.as_str(),
                            &kind,
                        ],
                    )
                    .await
                    .map_err(unavailable)?;
                if changed != 1 {
                    return Err(CoordinationError::Conflict.into());
                }
                &principal.subject_id
            }
            IdentityCommand::BindRoles { subject_id, roles } => {
                if roles.len() > 16
                    || roles
                        .iter()
                        .enumerate()
                        .any(|(i, r)| roles[..i].contains(r))
                {
                    return Err(CoordinationError::InvalidRequest.into());
                }
                identity_in(&tx, s, subject_id).await?;
                let mut p = scope_values(s);
                p.push(subject_id.as_str().into());
                tx.execute(
                    &format!("DELETE FROM role_bindings WHERE {SCOPE} AND subject_id=$5"),
                    &parameters(&p),
                )
                .await
                .map_err(unavailable)?;
                for role in roles {
                    let kind = serde_json::to_value(role)
                        .map_err(|_| CoordinationError::InvalidRequest)?;
                    let mut p = p.clone();
                    p.push(
                        kind.as_str()
                            .ok_or(CoordinationError::InvalidRequest)?
                            .into(),
                    );
                    tx.execute(
                        "INSERT INTO role_bindings VALUES($1,$2,$3,$4,$5,$6)",
                        &parameters(&p),
                    )
                    .await
                    .map_err(unavailable)?;
                }
                subject_id
            }
            IdentityCommand::PublishPolicy { policy } => {
                let raw = encode(policy, 1048576)?;
                let mut p = scope_values(s);
                p.extend([policy.version.as_str().into(), raw.clone()]);
                if let Some(old)=tx.query_opt(&format!("SELECT CASE WHEN octet_length(document_json)<=1048576 THEN document_json END FROM identity_policies WHERE {SCOPE} AND version=$5"),&parameters(&p[..5])).await.map_err(unavailable)? {
                    if crate::canonical::field(&old,0)?!=raw {return Err(CoordinationError::Conflict.into());}
                }else{tx.execute("INSERT INTO identity_policies VALUES($1,$2,$3,$4,$5,$6)",&parameters(&p)).await.map_err(unavailable)?;}
                tx.execute(
                    &format!("UPDATE identity_policy_heads SET version=$5 WHERE {SCOPE}"),
                    &parameters(&p[..5]),
                )
                .await
                .map_err(unavailable)?;
                actor
            }
            IdentityCommand::Revoke {
                subject_id,
                expected_epoch,
            } => {
                let next = expected_epoch
                    .checked_add(1)
                    .filter(|n| *n <= i64::MAX as u64)
                    .ok_or(CoordinationError::InvalidRequest)?;
                let changed=tx.execute("UPDATE principals SET revocation_epoch=$5 WHERE organization_id=$1 AND tenant_id=$2 AND subject_id=$3 AND revocation_epoch=$4",&[&s.organization_id.as_str(),&s.tenant_id.as_str(),&subject_id.as_str(),&crate::canonical::integer(*expected_epoch)?,&crate::canonical::integer(next)?]).await.map_err(unavailable)?;
                if changed != 1 {
                    return Err(CoordinationError::Conflict.into());
                }
                subject_id
            }
        };
        let result = IdentityManagementReceipt {
            operation_id: operation.into(),
            command_digest: digest.clone(),
            resulting_identity: identity_in(&tx, s, subject).await?,
        };
        let result_json = encode(&result, 65536)?;
        v.extend([digest, actor.as_str().into(), result_json]);
        tx.execute(
            "INSERT INTO identity_management_receipts VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
            &parameters(&v),
        )
        .await
        .map_err(unavailable)?;
        crate::ledgers::append_audit_in(&tx,&AuditCommand {scope:s.clone(),operation_id:format!("identity:{:x}",Sha256::digest(operation.as_bytes())),subject_id:actor.clone(),action:"identity.manage".into(),resource:serde_json::json!({"command_digest":result.command_digest,"subject_id":subject,"policy_version":result.resulting_identity.policy.version,"epoch":result.resulting_identity.principal.revocation_epoch}),outcome:"committed".into()},crate::now(&tx).await?).await?;
        // 读取确保epoch有界，未确认commit时不执行第二次副作用。
        unsigned(crate::canonical::integer(
            result.resulting_identity.principal.revocation_epoch,
        )?)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
}
