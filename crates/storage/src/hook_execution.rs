//! 原Hook执行的同连接端口，scope/frozen proof在原事务内复核。
use crate::{RunStore, RuntimeError, enterprise, execution, hooks, sessions};
use agent_core::*;
use rusqlite::{Connection, OptionalExtension, params};
pub(crate) fn check_hook_proof_in(
    db: &Connection,
    payload: &HookPayload,
    scope: &TenantContext,
    envelope: Option<&EnterpriseOwnerEnvelope>,
) -> Result<(), RuntimeError> {
    enterprise::check_session_scope_in(db, scope, &payload.session_key)?;
    sessions::lifetime_owner_in(db, &payload.session_key, &payload.session_lifetime_id)?;
    if let Some(owner) = &payload.owner {
        let local = EnterpriseOwnerEnvelope {
            scope: scope.clone(),
            owner: owner.clone(),
        };
        let proof = envelope.unwrap_or(&local);
        if proof.scope != *scope {
            return Err(AuthorizationError::Forbidden.into());
        }
        proof
            .owner
            .fence(owner)
            .map_err(|_| AuthorizationError::StaleOwner)?;
        execution::check_execution_in(db, proof)?;
    } else if envelope.is_some() {
        return Err(CoordinationError::InvalidRequest.into());
    }
    Ok(())
}
pub(crate) fn saved_outcome_in(
    db: &Connection,
    key: &SessionKey,
    life: &SessionLifetimeId,
    operation: &str,
) -> Result<Option<HookOutcome>, RuntimeError> {
    let saved:Option<Option<String>>=db.query_row("SELECT CASE WHEN typeof(outcome_json)='text' AND length(CAST(outcome_json AS BLOB)) BETWEEN 1 AND 49152 THEN outcome_json END FROM hook_outcomes WHERE operation_id=?1 AND session_id=?2 AND lifetime=?3",params![operation,key.0,life.0],|r|r.get(0)).optional()?;
    let Some(raw) = saved else { return Ok(None) };
    let raw = raw.ok_or(CoordinationError::Corrupt)?;
    let outcome: HookOutcome =
        serde_json::from_str(&raw).map_err(|_| CoordinationError::Corrupt)?;
    hooks::validate_outcome(&outcome).map_err(|_| CoordinationError::Corrupt)?;
    if outcome.payload.session_key != *key
        || outcome.payload.session_lifetime_id != *life
        || outcome.payload.operation_id != operation
    {
        return Err(CoordinationError::Corrupt.into());
    }
    if let Some(owner) = &outcome.payload.owner {
        let bound:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM runs r JOIN turns t ON t.run_id=r.id WHERE r.id=?1 AND r.session_id=?2 AND r.lifetime=?3 AND r.generation=?4 AND t.id=?5)",params![owner.run_id.0,key.0,life.0,owner.run_generation.0,owner.turn_id.0],|r|r.get(0))?;
        if !bound {
            return Err(CoordinationError::Corrupt.into());
        }
    }
    Ok(Some(outcome))
}
#[async_trait::async_trait]
impl agent_storage_ports::HookExecutionRepository for RunStore {
    async fn read_hook_snapshot(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<RunSnapshot, RepositoryError> {
        agent_storage_ports::RunContextRepository::read_execution_settings(self, envelope).await
    }
    async fn read_hook_interaction(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        id: &InteractionId,
    ) -> Result<Option<InteractionRecord>, RepositoryError> {
        if !execution::valid_owner(&envelope.owner) || id.0.is_empty() || id.0.len() > 1024 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, id) = (envelope.clone(), id.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(),move|store|{
            let read=||->Result<Option<InteractionRecord>,RuntimeError>{
            let mut db=store.lock_connection()?;let tx=db.transaction()?;execution::check_execution_in(&tx,&envelope)?;
            let sizes:Option<(u64,u64,u64)>=tx.query_row("SELECT length(CAST(prompt AS BLOB)),length(CAST(payload_json AS BLOB)),COALESCE(length(CAST(response_json AS BLOB)),0) FROM interactions WHERE id=?1 AND run_id=?2 AND lifetime=?3 AND generation=?4",params![id.0,envelope.owner.run_id.0,envelope.owner.session_lifetime_id.0,envelope.owner.run_generation.0],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
            let Some((prompt,payload,response))=sizes else{return Ok(None)};
            if prompt>16384||payload>64*1024||response>64*1024{return Err(CoordinationError::Corrupt.into());}
            let record=crate::read_interaction_in(&tx,&id.0).map_err(|_|CoordinationError::Corrupt)?;
            if record.as_ref().is_none_or(|r|r.session_id!=envelope.owner.session_key||r.owner_run_id!=envelope.owner.run_id){return Err(CoordinationError::Corrupt.into());}
            tx.commit()?;Ok(record)
            };read().map_err(RuntimeError::repository_error)
        }).await
    }
    async fn claim_hook_effect(
        &self,
        scope: &TenantContext,
        envelope: Option<&EnterpriseOwnerEnvelope>,
        payload: &HookPayload,
    ) -> Result<HookClaim, RepositoryError> {
        if !execution::within_json_budget(payload, 16384) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, envelope, payload) = (scope.clone(), envelope.cloned(), payload.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .claim_hook_scoped(&payload, &scope, envelope.as_ref())
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn settle_hook_effect(
        &self,
        scope: &TenantContext,
        envelope: Option<&EnterpriseOwnerEnvelope>,
        outcome: &HookOutcome,
    ) -> Result<HookOutcome, RepositoryError> {
        if !execution::within_json_budget(outcome, 49152) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, envelope, outcome) = (scope.clone(), envelope.cloned(), outcome.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .settle_hook_scoped(&outcome, &scope, envelope.as_ref(), true)
                .map_err(execution::execution_error)
        })
        .await
    }
}
#[async_trait::async_trait]
impl agent_storage_ports::StorageWorkDrainRepository for RunStore {
    async fn drain_admitted_work(&self) -> Result<(), CoordinationError> {
        let permits = self
            .blocking_budget
            .clone()
            .acquire_many_owned(4)
            .await
            .map_err(|_| CoordinationError::StorageUnavailable)?;
        drop(permits);
        Ok(())
    }
}
