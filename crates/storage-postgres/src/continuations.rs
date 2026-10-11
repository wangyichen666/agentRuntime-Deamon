//! Stop 续跑复用原准入事务，Hook证据与永久关系均来自 canonical DB。
use crate::canonical::{PgCanonicalStore, field, set_scope};
use crate::executions::{AdmissionPurpose, record_in};
use crate::facts::frozen_in;
use crate::hooks::saved_in;
use crate::identities::exact_owner_in;
use crate::sessions::encode;
use crate::{SCOPE, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::{HookContinuationRepository, IdentityReadRepository};
use std::sync::Arc;
use tokio_postgres::Transaction;

fn inputs(operation: &str, prompt: &str) -> Result<(), CoordinationError> {
    if operation.is_empty()
        || operation.len() > 256
        || prompt.trim().is_empty()
        || prompt.len() > 4096
    {
        return Err(CoordinationError::InvalidRequest);
    }
    Ok(())
}
pub(crate) async fn validate_in(
    db: &Transaction<'_>,
    parent: &EnterpriseOwnerEnvelope,
    operation: &str,
    prompt: &str,
    snapshot: &RunSnapshot,
) -> Result<(), RepositoryError> {
    inputs(operation, prompt)?;
    let mut frozen = frozen_in(db, &parent.scope, &parent.owner.run_id)
        .await?
        .ok_or(CoordinationError::Corrupt)?;
    frozen.entry_channel = HookChannel::Continuation;
    // 执行指令仅属于原工作流节点，派生 run 保留冻结授权但不重复执行父节点。
    frozen.workflow = None;
    if encode(&frozen, 1048576)? != encode(snapshot, 1048576)? {
        return Err(AuthorizationError::Forbidden.into());
    }
    let record = record_in(db, &parent.scope, &parent.owner.run_id).await?;
    if record.status != RunStatus::Completed
        || relation_in(db, &parent.scope, &parent.owner.run_id, true)
            .await?
            .is_some()
    {
        return Err(CoordinationError::Conflict.into());
    }
    let mut values = scope_values(&parent.scope);
    values.push(parent.owner.run_id.0.clone());
    if let Some(row)=db.query_opt(&format!("SELECT CASE WHEN octet_length(operation_id) BETWEEN 1 AND 256 THEN operation_id END FROM hook_continuations WHERE {SCOPE} AND parent_run_id=$5"),&parameters(&values)).await.map_err(unavailable)?{
        if field(&row,0)?!=operation{return Err(CoordinationError::Conflict.into());}
    }
    let outcome = saved_in(
        db,
        &parent.scope,
        &parent.owner.session_key,
        &parent.owner.session_lifetime_id,
        operation,
    )
    .await?
    .ok_or(CoordinationError::Conflict)?;
    if outcome.payload.data["continuation_allowed"] == false {
        return Err(CoordinationError::Corrupt.into());
    }
    if outcome.payload.owner.as_ref() != Some(&parent.owner)
        || outcome.payload.event != HookEvent::Stop
        || outcome.status != HookStatus::Succeeded
        || outcome.effect
            != Some(HookEffect::Continue {
                prompt: prompt.into(),
            })
    {
        return Err(CoordinationError::Conflict.into());
    }
    Ok(())
}

/// 唯一关系decoder，RunFact和Hook RPC共用，不从进程内缓存推导。
pub(crate) async fn relation_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    run: &RunId,
    parent: bool,
) -> Result<Option<RunId>, CoordinationError> {
    let owner = exact_owner_in(db, scope, run).await?;
    let (selected, filter) = if parent {
        ("parent_run_id", "child_run_id")
    } else {
        ("child_run_id", "parent_run_id")
    };
    let mut values = scope_values(scope);
    values.push(run.0.clone());
    let row=db.query_opt(&format!("SELECT CASE WHEN octet_length({selected}) BETWEEN 1 AND 1024 THEN {selected} END,CASE WHEN octet_length(operation_id) BETWEEN 1 AND 256 THEN operation_id END FROM hook_continuations WHERE {SCOPE} AND {filter}=$5"),&parameters(&values)).await.map_err(unavailable)?;
    let Some(row) = row else { return Ok(None) };
    let related = RunId(field(&row, 0)?);
    field(&row, 1)?;
    let actual = exact_owner_in(db, scope, &related).await.map_err(|error| {
        if error == CoordinationError::Forbidden {
            CoordinationError::Corrupt
        } else {
            error
        }
    })?;
    if related == *run
        || actual.session_key != owner.session_key
        || actual.session_lifetime_id != owner.session_lifetime_id
    {
        return Err(CoordinationError::Corrupt);
    }
    Ok(Some(related))
}

#[async_trait::async_trait]
impl HookContinuationRepository for PgCanonicalStore {
    async fn admit_stop_continuation(
        &self,
        parent: &EnterpriseOwnerEnvelope,
        operation: &str,
        prompt: &str,
        snapshot: &RunSnapshot,
    ) -> Result<Admission, RepositoryError> {
        inputs(operation, prompt)?;
        let frozen = self
            .read_authorization(parent)
            .await?
            .ok_or(AuthorizationError::Forbidden)?;
        let mut request = AuthorizationRequest {
            context: frozen.context.clone(),
            expected_policy: frozen.policy.version.clone(),
            action: Action::SessionPrompt,
            attributes: frozen.attributes.clone(),
        };
        request.context.request_id =
            RequestId::String(format!("continuation:{}", parent.owner.run_id.0));
        request.attributes.entry_channel = HookChannel::Continuation;
        let mut snapshot = snapshot.clone();
        snapshot.entry_channel = HookChannel::Continuation;
        // 执行指令仅属于原工作流节点，派生 run 保留冻结授权但不重复执行父节点。
        snapshot.workflow = None;
        let admission = RunAdmission {
            workflow_node: None,
            session_key: parent.owner.session_key.clone(),
            expected_lifetime: Some(parent.owner.session_lifetime_id.clone()),
            request_id: request.context.request_id.clone(),
            input: prompt.into(),
            mode: AdmissionMode::Queue,
            plan_execution: None,
        };
        self.admit_in(
            &admission,
            &snapshot,
            &request,
            Arc::new(crate::identities::InheritedAuthorization {
                snapshot: frozen,
                channel: HookChannel::Continuation,
                create_session: false,
            }),
            AdmissionPurpose::Continuation { parent, operation },
        )
        .await
        .map(|(admission, _)| admission)
    }
    async fn read_hook_parent(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Option<RunId>, RepositoryError> {
        self.read_continuation(scope, run, true).await
    }
    async fn read_hook_child(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Option<RunId>, RepositoryError> {
        self.read_continuation(scope, run, false).await
    }
}
impl PgCanonicalStore {
    async fn read_continuation(
        &self,
        scope: &TenantContext,
        run: &RunId,
        parent: bool,
    ) -> Result<Option<RunId>, RepositoryError> {
        if run.0.is_empty() || run.0.len() > 1024 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let result = relation_in(&tx, scope, run, parent).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
}
