//! 管理命令复用原身份表；授权、幂等回执和审计同一IMMEDIATE事务。
use crate::{RunStore, RuntimeError, enterprise};
use agent_core::*;
use rusqlite::{OptionalExtension, params};
use std::sync::Arc;
fn encoded(value: &impl serde::Serialize) -> Result<String, RuntimeError> {
    serde_json::to_string(value).map_err(|_| CoordinationError::InvalidRequest.into())
}
impl RunStore {
    fn manage_identity_in(
        &self,
        request: &AuthorizationRequest,
        command: &IdentityCommand,
        operation: &str,
        verifier: &dyn AuthorizationVerifier,
    ) -> Result<IdentityManagementReceipt, RuntimeError> {
        if request.action != Action::IdentityManage || operation.is_empty() || operation.len() > 256
        {
            return Err(AuthorizationError::Forbidden.into());
        }
        let raw = encoded(command)?;
        if raw.len() > 1048576 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let digest = enterprise::hash(raw.as_bytes());
        let s = &request.context.scope;
        let actor = &request.context.principal.subject_id;
        let mut connection = self.lock_connection()?;
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let identity = enterprise::identity_in(&tx, s, actor)?;
        let mut checked = request.clone();
        checked.attributes.timestamp_ms = crate::now_ms();
        verifier.verify(&checked, &identity)?;
        let saved:Option<(String,String,Option<String>)>=tx.query_row("SELECT command_digest,actor_id,CASE WHEN length(CAST(result_json AS BLOB))<=65536 THEN result_json END FROM identity_management_receipts WHERE organization_id=?1 AND tenant_id=?2 AND project_id=?3 AND agent_uid=?4 AND operation_id=?5",params![s.organization_id.as_str(),s.tenant_id.as_str(),s.project_id.as_str(),s.agent_uid.as_str(),operation],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        if let Some((old, old_actor, result)) = saved {
            if old != digest || old_actor != actor.as_str() {
                return Err(CoordinationError::Conflict.into());
            }
            return serde_json::from_str(&result.ok_or(CoordinationError::Corrupt)?)
                .map_err(|_| CoordinationError::Corrupt.into());
        }
        let subject = match command {
            IdentityCommand::RegisterSubject { principal } => {
                if principal.revocation_epoch != 0 {
                    return Err(CoordinationError::InvalidRequest.into());
                }
                let kind = encoded(&principal.kind)?;
                let kind = kind.trim_matches('"');
                tx.execute(
                    "INSERT INTO principals VALUES(?1,?2,?3,0)",
                    params![s.tenant_id.as_str(), principal.subject_id.as_str(), kind],
                )
                .map_err(|_| CoordinationError::Conflict)?;
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
                enterprise::identity_in(&tx, s, subject_id)?;
                tx.execute("DELETE FROM role_bindings WHERE tenant_id=?1 AND project_id=?2 AND agent_uid=?3 AND subject_id=?4",params![s.tenant_id.as_str(),s.project_id.as_str(),s.agent_uid.as_str(),subject_id.as_str()])?;
                for role in roles {
                    let raw = encoded(role)?;
                    tx.execute(
                        "INSERT INTO role_bindings VALUES(?1,?2,?3,?4,?5)",
                        params![
                            s.tenant_id.as_str(),
                            s.project_id.as_str(),
                            s.agent_uid.as_str(),
                            subject_id.as_str(),
                            raw.trim_matches('"')
                        ],
                    )?;
                }
                subject_id
            }
            IdentityCommand::PublishPolicy { policy } => {
                let raw = encoded(policy)?;
                let prior:Option<String>=tx.query_row("SELECT document_json FROM identity_policies WHERE tenant_id=?1 AND project_id=?2 AND agent_uid=?3 AND version=?4",params![s.tenant_id.as_str(),s.project_id.as_str(),s.agent_uid.as_str(),policy.version.as_str()],|r|r.get(0)).optional()?;
                if let Some(old) = prior {
                    if old != raw {
                        return Err(CoordinationError::Conflict.into());
                    }
                } else {
                    tx.execute(
                        "INSERT INTO identity_policies VALUES(?1,?2,?3,?4,?5)",
                        params![
                            s.tenant_id.as_str(),
                            s.project_id.as_str(),
                            s.agent_uid.as_str(),
                            policy.version.as_str(),
                            raw
                        ],
                    )?;
                }
                tx.execute("UPDATE identity_policy_heads SET version=?4 WHERE tenant_id=?1 AND project_id=?2 AND agent_uid=?3",params![s.tenant_id.as_str(),s.project_id.as_str(),s.agent_uid.as_str(),policy.version.as_str()])?;
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
                let changed=tx.execute("UPDATE principals SET revocation_epoch=?4 WHERE tenant_id=?1 AND subject_id=?2 AND revocation_epoch=?3",params![s.tenant_id.as_str(),subject_id.as_str(),expected_epoch,next])?;
                if changed != 1 {
                    return Err(CoordinationError::Conflict.into());
                }
                subject_id
            }
        };
        let result = IdentityManagementReceipt {
            operation_id: operation.into(),
            command_digest: digest.clone(),
            resulting_identity: enterprise::identity_in(&tx, s, subject)?,
        };
        let result_json = encoded(&result)?;
        if result_json.len() > 65536 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        tx.execute(
            "INSERT INTO identity_management_receipts VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                s.organization_id.as_str(),
                s.tenant_id.as_str(),
                s.project_id.as_str(),
                s.agent_uid.as_str(),
                operation,
                digest,
                actor.as_str(),
                result_json
            ],
        )?;
        crate::ledgers::append_audit_in(
            &tx,
            &AuditCommand {
                scope: s.clone(),
                operation_id: format!("identity:{}", enterprise::hash(operation.as_bytes())),
                subject_id: actor.clone(),
                action: "identity.manage".into(),
                resource: serde_json::json!({"command_digest":result.command_digest,"subject_id":subject,"policy_version":result.resulting_identity.policy.version,"epoch":result.resulting_identity.principal.revocation_epoch}),
                outcome: "committed".into(),
            },
            crate::now_ms(),
        )?;
        tx.commit()?;
        Ok(result)
    }
}
#[async_trait::async_trait]
impl agent_storage_ports::IdentityManagementRepository for RunStore {
    async fn manage_identity(
        &self,
        request: &AuthorizationRequest,
        command: &IdentityCommand,
        operation: &str,
        verifier: Arc<dyn AuthorizationVerifier>,
    ) -> Result<IdentityManagementReceipt, RepositoryError> {
        if !crate::execution::within_json_budget(command, 1048576)
            || !crate::execution::within_json_budget(request, 65536)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (request, command, operation) =
            (request.clone(), command.clone(), operation.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .manage_identity_in(&request, &command, &operation, verifier.as_ref())
                .map_err(RuntimeError::repository_error)
        })
        .await
    }
}
