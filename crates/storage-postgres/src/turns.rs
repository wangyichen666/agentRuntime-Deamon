//! 原计划 stage、助手输出及 canonical transcript；运行终态由 execution 事务消费。
use crate::canonical::{PgCanonicalStore, field, set_scope, unsigned};
use crate::events::{append_in, seal_in};
use crate::plans::{document_in, identity_matches, validate_document, write_document};
use crate::sessions::{encode, insert_batch, metadata_in};
use crate::{SCOPE, now, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::{RunTurnRepository, stage_plan_document};
use tokio_postgres::Transaction;

pub(crate) async fn execution_in(
    db: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    proof: &SessionWriterLease,
) -> Result<(), RepositoryError> {
    PgCanonicalStore::fence_envelope_in(db, proof, env).await?;
    let mut values = scope_values(&env.scope);
    values.push(env.owner.run_id.0.clone());
    let state: String = db
        .query_one(
            &format!("SELECT status FROM runs WHERE {SCOPE} AND id=$5 FOR UPDATE"),
            &parameters(&values),
        )
        .await
        .map_err(unavailable)?
        .try_get(0)
        .map_err(unavailable)?;
    if !matches!(state.as_str(), "running" | "waiting_interaction") {
        return Err(CoordinationError::Conflict.into());
    }
    Ok(())
}
pub(crate) async fn read_plan_in(
    db: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
) -> Result<RunPlanReadback, CoordinationError> {
    let mut values = scope_values(&env.scope);
    values.push(env.owner.run_id.0.clone());
    let stage=db.query_opt(&format!("SELECT revision,CASE WHEN octet_length(data_json)<=1048576 THEN data_json END FROM turn_plan_stages WHERE {SCOPE} AND run_id=$5"),&parameters(&values)).await.map_err(unavailable)?;
    values[4] = env.owner.session_lifetime_id.0.clone();
    let current=db.query_opt(&format!("SELECT revision,CASE WHEN octet_length(data_json)<=1048576 THEN data_json END FROM session_plans WHERE {SCOPE} AND lifetime=$5"),&parameters(&values)).await.map_err(unavailable)?;
    let has_legacy_plan = if let Some(row) = &current {
        let value: serde_json::Value =
            serde_json::from_str(&field(row, 1)?).map_err(|_| CoordinationError::Corrupt)?;
        if value.get("plan_id").is_none() {
            let definition: PlanDefinition =
                serde_json::from_value(value).map_err(|_| CoordinationError::Corrupt)?;
            definition
                .canonical_bytes()
                .map_err(|_| CoordinationError::Corrupt)?;
            true
        } else {
            false
        }
    } else {
        false
    };
    let snapshot = if let Some(row) = stage.as_ref().or(current.as_ref()) {
        let revision = unsigned(row.try_get(0).map_err(unavailable)?)?;
        let value: serde_json::Value =
            serde_json::from_str(&field(row, 1)?).map_err(|_| CoordinationError::Corrupt)?;
        if value.get("plan_id").is_some() {
            let doc: PlanDocument =
                serde_json::from_value(value.clone()).map_err(|_| CoordinationError::Corrupt)?;
            validate_document(&doc)?;
            if doc.revision != revision {
                return Err(CoordinationError::Corrupt);
            }
        } else {
            let doc: PlanDefinition =
                serde_json::from_value(value.clone()).map_err(|_| CoordinationError::Corrupt)?;
            doc.canonical_bytes()
                .map_err(|_| CoordinationError::Corrupt)?;
        }
        PlanSnapshot { revision, value }
    } else {
        PlanSnapshot {
            revision: 0,
            value: serde_json::json!({"steps":[]}),
        }
    };
    Ok(RunPlanReadback {
        snapshot,
        has_legacy_plan,
    })
}
impl PgCanonicalStore {
    pub(crate) async fn execution_proof(
        &self,
        env: &EnterpriseOwnerEnvelope,
    ) -> Result<SessionWriterLease, CoordinationError> {
        self.bound_proof(&SessionWriterLeaseKey {
            scope: env.scope.clone(),
            session_key: env.owner.session_key.clone(),
            session_lifetime_id: env.owner.session_lifetime_id.clone(),
        })
        .await
    }
}
#[async_trait::async_trait]
impl RunTurnRepository for PgCanonicalStore {
    async fn read_run_plan(
        &self,
        env: &EnterpriseOwnerEnvelope,
    ) -> Result<RunPlanReadback, RepositoryError> {
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        Self::fence_envelope_in(&tx, &proof, env).await?;
        let readback = read_plan_in(&tx, env).await?;
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(readback)
    }
    async fn stage_run_plan(
        &self,
        env: &EnterpriseOwnerEnvelope,
        expected: u64,
        value: &serde_json::Value,
    ) -> Result<PlanSnapshot, RepositoryError> {
        if expected > i64::MAX as u64 || !value.is_object() {
            return Err(CoordinationError::InvalidRequest.into());
        }
        encode(value, 512 * 1024)?;
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        execution_in(&tx, env, &proof).await?;
        let current = read_plan_in(&tx, env).await?.snapshot;
        if current.revision != expected {
            return Err(CoordinationError::Conflict.into());
        }
        let document = stage_plan_document(&env.owner, &current, value)?;
        let next = PlanSnapshot {
            revision: document.revision,
            value: serde_json::to_value(&document)
                .map_err(|_| CoordinationError::InvalidRequest)?,
        };
        if current.value != next.value {
            let mut base = scope_values(&env.scope);
            base.push(env.owner.session_lifetime_id.0.clone());
            let row = tx
                .query_opt(
                    &format!("SELECT revision FROM session_plans WHERE {SCOPE} AND lifetime=$5"),
                    &parameters(&base),
                )
                .await
                .map_err(unavailable)?;
            let base_revision = if let Some(row) = row {
                unsigned(row.try_get(0).map_err(unavailable)?)?
            } else {
                0
            };
            let mut values = scope_values(&env.scope);
            values.extend([
                env.owner.run_id.0.clone(),
                base_revision.to_string(),
                next.revision.to_string(),
                encode(&document, 1048576)?,
            ]);
            tx.execute("INSERT INTO turn_plan_stages VALUES($1,$2,$3,$4,$5,$6::text::bigint,$7::text::bigint,$8) ON CONFLICT(organization_id,tenant_id,project_id,agent_uid,run_id) DO UPDATE SET revision=excluded.revision,data_json=excluded.data_json",&parameters(&values)).await.map_err(unavailable)?;
        }
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(next)
    }
    async fn start_run_plan(&self, env: &EnterpriseOwnerEnvelope) -> Result<(), RepositoryError> {
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        execution_in(&tx, env, &proof).await?;
        let mut values = scope_values(&env.scope);
        values.push(env.owner.run_id.0.clone());
        let state: String = tx
            .query_one(
                &format!("SELECT status FROM runs WHERE {SCOPE} AND id=$5"),
                &parameters(&values),
            )
            .await
            .map_err(unavailable)?
            .try_get(0)
            .map_err(unavailable)?;
        if state != "running" {
            return Err(CoordinationError::Conflict.into());
        }
        let rows=tx.query(&format!("SELECT CASE WHEN octet_length(receipt_json)<=65536 THEN receipt_json END,CASE WHEN octet_length(operation_id)<=128 THEN operation_id END,CASE WHEN octet_length(session_id)<=1024 THEN session_id END,CASE WHEN octet_length(lifetime)<=128 THEN lifetime END,CASE WHEN octet_length(identity_json)<=16384 THEN identity_json END FROM plan_decisions WHERE {SCOPE} AND run_id=$5 AND decision='execute' LIMIT 2"),&parameters(&values)).await.map_err(unavailable)?;
        if rows.len() > 1 {
            return Err(CoordinationError::Corrupt.into());
        }
        if let Some(row) = rows.first() {
            let receipt: PlanDecisionReceipt =
                serde_json::from_str(&field(row, 0)?).map_err(|_| CoordinationError::Corrupt)?;
            let identity: PlanExecution =
                serde_json::from_str(&field(row, 4)?).map_err(|_| CoordinationError::Corrupt)?;
            if receipt.run_id.as_ref() != Some(&env.owner.run_id)
                || receipt.decision != PlanDecision::Execute
                || receipt.session_key != env.owner.session_key
                || receipt.lifetime != env.owner.session_lifetime_id
                || receipt.identity != identity
                || identity.operation_id != field(row, 1)?
                || field(row, 2)? != env.owner.session_key.0
                || field(row, 3)? != env.owner.session_lifetime_id.0
            {
                return Err(CoordinationError::Corrupt.into());
            }
            let mut document = document_in(&tx, &env.scope, &env.owner.session_lifetime_id).await?;
            identity_matches(&document, &identity)?;
            if document.review != PlanReviewStatus::PendingExecution {
                return Err(CoordinationError::Conflict.into());
            }
            document.review = PlanReviewStatus::Executing;
            write_document(&tx, &env.scope, &env.owner.session_lifetime_id, &document).await?;
        }
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }
    async fn reject_run_tool_batch(
        &self,
        env: &EnterpriseOwnerEnvelope,
        round: usize,
        calls: &[ToolCall],
        reason: &str,
    ) -> Result<Vec<Message>, RepositoryError> {
        if round == 0
            || round > i64::MAX as usize
            || calls.is_empty()
            || calls.len() > 256
            || reason.is_empty()
            || reason.len() > 65536
            || reason.len().saturating_mul(calls.len()).saturating_mul(12) > 2 * 1024 * 1024
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let calls_raw = encode(calls, 1048576)?;
        let mut ids = std::collections::BTreeSet::new();
        if calls.iter().any(|call| {
            call.id.is_empty()
                || call.id.len() > 1024
                || call.name.is_empty()
                || call.name.len() > 1024
                || !ids.insert(&call.id)
        }) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let mut messages = vec![Message::assistant_tool_calls(calls.to_vec())];
        let output =
            serde_json::json!({"outcome":"not_executed","reason":reason,"batch_rejected":true})
                .to_string();
        for call in calls {
            messages.push(Message::tool_result(call, output.clone()));
        }
        encode(&messages, 4 * 1024 * 1024 - 8192)?;
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        execution_in(&tx, env, &proof).await?;
        let mut values = scope_values(&env.scope);
        values.extend([env.owner.run_id.0.clone(), round.to_string(), calls_raw]);
        tx.execute("INSERT INTO tool_batches(organization_id,tenant_id,project_id,agent_uid,run_id,round,calls_json,state) VALUES($1,$2,$3,$4,$5,$6::text::bigint,$7,'closed')",&parameters(&values)).await.map_err(unavailable)?;
        let time = now(&tx).await?;
        let receipt = encode(
            &serde_json::json!({"output":output,"success":false,"batch_rejected":true}),
            1048576,
        )?;
        for call in calls {
            let mut values = scope_values(&env.scope);
            values.extend([
                env.owner.run_id.0.clone(),
                env.owner.session_lifetime_id.0.clone(),
                env.owner.run_generation.0.to_string(),
                round.to_string(),
                call.id.clone(),
                call.name.clone(),
                time.to_string(),
                receipt.clone(),
            ]);
            tx.execute("INSERT INTO tool_executions(organization_id,tenant_id,project_id,agent_uid,run_id,lifetime,generation,round,call_id,name,status,effect,argument_digest,prepared_at_ms,finished_at_ms,outcome,receipt_json) VALUES($1,$2,$3,$4,$5,$6,$7::text::bigint,$8::text::bigint,$9,$10,'terminal','not_executed','',$11::text::bigint,$11::text::bigint,'not_executed',$12)",&parameters(&values)).await.map_err(unavailable)?;
        }
        let meta = metadata_in(&tx, &env.scope, &env.owner.session_key, true)
            .await?
            .ok_or(CoordinationError::Forbidden)?;
        insert_batch(
            &tx,
            &env.scope,
            &meta,
            &format!("tool-batch:{round}"),
            Some(&env.owner.run_id),
            &messages,
        )
        .await?;
        append_in(
            &tx,
            env,
            "tool_batch_rejected",
            &serde_json::json!({"round":round,"reason":reason}),
        )
        .await?;
        seal_in(&tx, env).await?;
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(messages)
    }
    async fn stage_run_assistant(
        &self,
        env: &EnterpriseOwnerEnvelope,
        message: &Message,
    ) -> Result<(), RepositoryError> {
        if message.role != Role::Assistant
            || !message.tool_calls.is_empty()
            || message.tool_call_id.is_some()
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let raw = encode(message, 4 * 1024 * 1024 - 8192)?;
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        execution_in(&tx, env, &proof).await?;
        let mut values = scope_values(&env.scope);
        values.push(env.owner.run_id.0.clone());
        if let Some(row)=tx.query_opt(&format!("SELECT CASE WHEN octet_length(message_json)<=4186112 THEN message_json END FROM turn_outputs WHERE {SCOPE} AND run_id=$5"),&parameters(&values)).await.map_err(unavailable)?{let old:Message=serde_json::from_str(&field(&row,0)?).map_err(|_|CoordinationError::Corrupt)?;if old.role!=Role::Assistant||!old.tool_calls.is_empty()||old.tool_call_id.is_some(){return Err(CoordinationError::Corrupt.into());}}
        values.push(raw);
        tx.execute("INSERT INTO turn_outputs VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(organization_id,tenant_id,project_id,agent_uid,run_id) DO UPDATE SET message_json=excluded.message_json",&parameters(&values)).await.map_err(unavailable)?;
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }
    async fn append_run_transcript(
        &self,
        env: &EnterpriseOwnerEnvelope,
        operation: &str,
        messages: &[Message],
    ) -> Result<TranscriptSeq, RepositoryError> {
        if operation.is_empty()
            || operation.len() > 1024
            || messages.is_empty()
            || messages.len() > 257
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        encode(messages, 4 * 1024 * 1024 - 8192)?;
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        execution_in(&tx, env, &proof).await?;
        let meta = metadata_in(&tx, &env.scope, &env.owner.session_key, true)
            .await?
            .ok_or(CoordinationError::Forbidden)?;
        let seq = insert_batch(
            &tx,
            &env.scope,
            &meta,
            operation,
            Some(&env.owner.run_id),
            messages,
        )
        .await?;
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(seq)
    }
}
