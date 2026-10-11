//! 工具执行的原 prepared/running/terminal 与批次转录，不自动重放 running 工具。
use crate::canonical::{PgCanonicalStore, field, set_scope};
use crate::events::{append_in, seal_in};
use crate::facts::{frozen_in, receipts_in};
use crate::sessions::{encode, insert_batch, metadata_in};
use crate::turns::execution_in;
use crate::{SCOPE, now, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::{RunToolRepository, verify_artifact_receipt};
use sha2::{Digest, Sha256};
use tokio_postgres::Transaction;
fn valid_round(round: usize) -> Result<(), CoordinationError> {
    if round == 0 || round > i64::MAX as usize {
        Err(CoordinationError::InvalidRequest)
    } else {
        Ok(())
    }
}
fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 1024
}
fn values(env: &EnterpriseOwnerEnvelope, round: usize) -> Vec<String> {
    let mut values = scope_values(&env.scope);
    values.extend([env.owner.run_id.0.clone(), round.to_string()]);
    values
}
pub(crate) async fn finish_in(
    db: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    completion: &ToolArtifactCompletion,
    reference: &str,
) -> Result<(), RepositoryError> {
    crate::maintenance::artifact_write_gate(db).await?;
    let mut values = values(env, completion.round);
    values.extend([
        completion.call_id.clone(),
        completion.outcome.clone(),
        reference.into(),
        encode(&completion.receipt, 1048576)?,
        now(db).await?.to_string(),
    ]);
    let changed=db.execute(&format!("UPDATE tool_executions SET status='terminal',outcome=$8,artifact_ref=$9,receipt_json=$10,finished_at_ms=$11::text::bigint WHERE {SCOPE} AND run_id=$5 AND round=$6::text::bigint AND call_id=$7 AND status='running'"),&parameters(&values)).await.map_err(unavailable)?;
    if changed != 1 {
        return Err(CoordinationError::Conflict.into());
    }
    crate::billing::finish_sandbox_in(db,env,&format!("tool:{}:{}",completion.round,completion.call_id),completion.outcome=="outcome_unknown").await?;
    Ok(())
}
#[async_trait::async_trait]
impl RunToolRepository for PgCanonicalStore {
    async fn prepare_run_tool_batch(
        &self,
        env: &EnterpriseOwnerEnvelope,
        round: usize,
        calls: &[(ToolCall, String, String, bool)],
        descriptors: &[ToolDescriptor],
    ) -> Result<(), RepositoryError> {
        valid_round(round)?;
        if calls.is_empty() || calls.len() > 256 || calls.len() != descriptors.len() {
            return Err(CoordinationError::InvalidRequest.into());
        }
        encode(calls, 1048576)?;
        let descriptors_raw = encode(descriptors, 1048576)?;
        let mut ids = std::collections::BTreeSet::new();
        for ((call, effect, digest, safe), descriptor) in calls.iter().zip(descriptors) {
            let actual = match descriptor.effect {
                ToolEffect::Read => "read",
                ToolEffect::Write => "write",
                ToolEffect::External => "external_side_effect",
            };
            if !valid_id(&call.id)
                || !valid_id(&call.name)
                || !ids.insert(&call.id)
                || effect != actual
                || *safe != matches!(descriptor.replay, ReplayPolicy::Safe)
                || *digest
                    != format!(
                        "{:x}",
                        Sha256::digest(call.arguments.to_string().as_bytes())
                    )
            {
                return Err(CoordinationError::InvalidRequest.into());
            }
        }
        let expected = calls
            .iter()
            .map(|(call, _, _, _)| call.clone())
            .collect::<Vec<_>>();
        let raw = encode(&expected, 1048576)?;
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        execution_in(&tx, env, &proof).await?;
        let values = values(env, round);
        let state: String = tx
            .query_one(
                &format!("SELECT status FROM runs WHERE {SCOPE} AND id=$5"),
                &parameters(&values[..5]),
            )
            .await
            .map_err(unavailable)?
            .try_get(0)
            .map_err(unavailable)?;
        if state != "running" {
            return Err(CoordinationError::Conflict.into());
        }
        if let Some(row)=tx.query_opt(&format!("SELECT CASE WHEN octet_length(calls_json)<=1048576 THEN calls_json END,CASE WHEN octet_length(descriptor_json)<=1048576 THEN descriptor_json END FROM tool_batches WHERE {SCOPE} AND run_id=$5 AND round=$6::text::bigint"),&parameters(&values)).await.map_err(unavailable)?{
            let old:Vec<ToolCall>=serde_json::from_str(&field(&row,0)?).map_err(|_|CoordinationError::Corrupt)?;let old_descriptors:Vec<ToolDescriptor>=serde_json::from_str(&field(&row,1)?).map_err(|_|CoordinationError::Corrupt)?;
            if old!=expected||serde_json::to_value(old_descriptors).map_err(|_|CoordinationError::Corrupt)?!=serde_json::to_value(descriptors).map_err(|_|CoordinationError::InvalidRequest)?{return Err(CoordinationError::Conflict.into());}
            let receipts=receipts_in(&tx,&env.scope,&env.owner).await?;let receipts=receipts.iter().filter(|item|item.round==round as i64).collect::<Vec<_>>();if receipts.len()!=calls.len(){return Err(CoordinationError::Corrupt.into());}for (call,effect,digest,safe) in calls{let saved=receipts.iter().find(|item|item.call_id==call.id).ok_or(CoordinationError::Corrupt)?;if saved.name!=call.name||saved.effect!=*effect||saved.argument_digest!=*digest||saved.safe_to_replay!=*safe{return Err(CoordinationError::Corrupt.into());}}
            Self::before_commit(&tx,&proof).await?;tx.commit().await.map_err(unavailable)?;return Ok(());
        }
        let saved = receipts_in(&tx, &env.scope, &env.owner).await?;
        let budget=tx.query_one(&format!("SELECT (SELECT max_tool_calls FROM delegations WHERE {SCOPE} AND child_run_id=$5),EXISTS(SELECT 1 FROM hook_continuations WHERE {SCOPE} AND child_run_id=$5)"),&parameters(&values[..5])).await.map_err(unavailable)?;
        let mut max = budget.try_get::<_, Option<i64>>(0).map_err(unavailable)?;
        if budget.try_get::<_, bool>(1).map_err(unavailable)? {
            max = Some(max.map_or(8, |limit| limit.min(8)));
        }
        if let Some(limit)=frozen_in(&tx,&env.scope,&env.owner.run_id).await?.and_then(|s|s.max_tool_calls) {max=Some(max.map_or(limit as i64,|m|m.min(limit as i64)));}
        if max.is_some_and(|limit| {
            limit < 0 || saved.len().saturating_add(calls.len()) > limit as usize
        }) {
            return Err(CoordinationError::Conflict.into());
        }
        if saved.len().saturating_add(calls.len()) > 1000 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let mut batch = values.clone();
        batch.extend([raw, descriptors_raw]);
        tx.execute("INSERT INTO tool_batches(organization_id,tenant_id,project_id,agent_uid,run_id,round,calls_json,descriptor_json) VALUES($1,$2,$3,$4,$5,$6::text::bigint,$7,$8)",&parameters(&batch)).await.map_err(unavailable)?;
        let time = now(&tx).await?;
        for (call, effect, digest, safe) in calls {
            let mut row = values.clone();
            row.extend([
                env.owner.session_lifetime_id.0.clone(),
                env.owner.run_generation.0.to_string(),
                call.id.clone(),
                call.name.clone(),
                effect.clone(),
                digest.clone(),
                time.to_string(),
                safe.to_string(),
            ]);
            tx.execute("INSERT INTO tool_executions(organization_id,tenant_id,project_id,agent_uid,run_id,round,lifetime,generation,call_id,name,status,effect,argument_digest,prepared_at_ms,safe_to_replay) VALUES($1,$2,$3,$4,$5,$6::text::bigint,$7,$8::text::bigint,$9,$10,'prepared',$11,$12,$13::text::bigint,$14::text::boolean)",&parameters(&row)).await.map_err(unavailable)?;
        }
        append_in(&tx,env,"tool_batch_prepared",&serde_json::json!({"round":round,"calls":calls.iter().map(|(call,_,_,_)|&call.id).collect::<Vec<_>>()})).await?;
        seal_in(&tx, env).await?;
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }
    async fn start_run_tool(
        &self,
        env: &EnterpriseOwnerEnvelope,
        round: usize,
        id: &str,
    ) -> Result<(), RepositoryError> {
        valid_round(round)?;
        if !valid_id(id) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        execution_in(&tx, env, &proof).await?;
        let mut values = values(env, round);
        values.extend([id.into(), now(&tx).await?.to_string()]);
        let changed=tx.execute(&format!("UPDATE tool_executions SET status='running',started_at_ms=$8::text::bigint WHERE {SCOPE} AND run_id=$5 AND round=$6::text::bigint AND call_id=$7 AND status='prepared'"),&parameters(&values)).await.map_err(unavailable)?;
        if changed != 1 {
            return Err(CoordinationError::Conflict.into());
        }
        crate::billing::start_tool_in(&tx,env,round,id).await?;
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }
    async fn close_run_tool_exchange(
        &self,
        env: &EnterpriseOwnerEnvelope,
        round: usize,
        messages: &[Message],
    ) -> Result<(), RepositoryError> {
        valid_round(round)?;
        encode(messages, 4 * 1024 * 1024 - 8192)?;
        let first = messages.first().ok_or(CoordinationError::InvalidRequest)?;
        if first.role != Role::Assistant
            || first.tool_calls.is_empty()
            || messages.len() != first.tool_calls.len() + 1
            || messages.len() > 257
            || first
                .tool_calls
                .iter()
                .zip(&messages[1..])
                .any(|(call, result)| {
                    result.role != Role::Tool
                        || result.tool_call_id.as_deref() != Some(call.id.as_str())
                })
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        execution_in(&tx, env, &proof).await?;
        let values = values(env, round);
        let row=tx.query_opt(&format!("SELECT CASE WHEN octet_length(calls_json)<=1048576 THEN calls_json END,state FROM tool_batches WHERE {SCOPE} AND run_id=$5 AND round=$6::text::bigint"),&parameters(&values)).await.map_err(unavailable)?.ok_or(CoordinationError::Conflict)?;
        let calls: Vec<ToolCall> =
            serde_json::from_str(&field(&row, 0)?).map_err(|_| CoordinationError::Corrupt)?;
        if first.tool_calls != calls {
            return Err(CoordinationError::Conflict.into());
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
            messages,
        )
        .await?;
        if row.try_get::<_, String>(1).map_err(unavailable)? != "closed" {
            tx.execute(&format!("UPDATE tool_batches SET state='closed' WHERE {SCOPE} AND run_id=$5 AND round=$6::text::bigint"),&parameters(&values)).await.map_err(unavailable)?;
            append_in(
                &tx,
                env,
                "tool_exchange_closed",
                &serde_json::json!({"round":round}),
            )
            .await?;
            seal_in(&tx, env).await?;
        }
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }
    async fn finish_run_tool_batch(
        &self,
        env: &EnterpriseOwnerEnvelope,
        round: usize,
    ) -> Result<(), RepositoryError> {
        valid_round(round)?;
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        execution_in(&tx, env, &proof).await?;
        let values = values(env, round);
        let row=tx.query_opt(&format!("SELECT state FROM tool_batches WHERE {SCOPE} AND run_id=$5 AND round=$6::text::bigint"),&parameters(&values)).await.map_err(unavailable)?.ok_or(CoordinationError::Conflict)?;
        if row.try_get::<_, String>(0).map_err(unavailable)? != "closed" {
            return Err(CoordinationError::Conflict.into());
        }
        let incomplete:bool=tx.query_one(&format!("SELECT EXISTS(SELECT 1 FROM tool_executions WHERE {SCOPE} AND run_id=$5 AND round=$6::text::bigint AND (status<>'terminal' OR outcome='outcome_unknown'))"),&parameters(&values)).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
        if incomplete {
            return Err(CoordinationError::Conflict.into());
        }
        let published:bool=tx.query_one(&format!("SELECT EXISTS(SELECT 1 FROM events WHERE {SCOPE} AND run_id=$5 AND event='tool_batch_completed' AND data_json::jsonb->>'round'=$6)"),&parameters(&values)).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
        if !published {
            append_in(
                &tx,
                env,
                "tool_batch_completed",
                &serde_json::json!({"round":round}),
            )
            .await?;
            seal_in(&tx, env).await?;
        }
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }
    async fn finish_file_tool_output(
        &self,
        env: &EnterpriseOwnerEnvelope,
        completion: &ToolArtifactCompletion,
        content: String,
    ) -> Result<String, RepositoryError> {
        valid_round(completion.round)?;
        if !valid_id(&completion.call_id)
            || content.len() as u64 > MAX_ARTIFACT_BYTES
            || !matches!(
                completion.outcome.as_str(),
                "success" | "tool_error" | "outcome_unknown"
            )
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        encode(completion, 1048576)?;
        let proof = self.execution_proof(env).await?;
        {
            let mut client = self.inner.client.lock().await;
            let tx = client.transaction().await.map_err(unavailable)?;
            set_scope(&tx, &env.scope).await?;
            execution_in(&tx, env, &proof).await?;
            if env.scope != TenantContext::local() {
                return Err(ArtifactError::Forbidden.into());
            }
            if frozen_in(&tx, &env.scope, &env.owner.run_id)
                .await?
                .ok_or(CoordinationError::Corrupt)?
                .artifact_store_id
                .is_some()
            {
                return Err(ArtifactError::Forbidden.into());
            }
            Self::before_commit(&tx, &proof).await?;
            tx.commit().await.map_err(unavailable)?;
        }
        let files = self
            .local_files
            .clone()
            .ok_or(ArtifactError::StorageUnavailable)?;
        let permit = self
            .file_budget
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ArtifactError::StorageUnavailable)?;
        let directory = files.clone();
        let staged = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            directory.stage(&content)
        })
        .await
        .map_err(|_| ArtifactError::StorageUnavailable)??;
        verify_artifact_receipt(&staged.identity, &completion.receipt, true)?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        crate::maintenance::artifact_write_gate(&tx).await?;
        execution_in(&tx, env, &proof).await?;
        let receipts = receipts_in(&tx, &env.scope, &env.owner).await?;
        let previous = receipts
            .iter()
            .find(|receipt| {
                receipt.round == completion.round as i64 && receipt.call_id == completion.call_id
            })
            .ok_or(ArtifactError::Conflict)?;
        let reference = if previous.status == "terminal"
            && previous.outcome.as_deref() == Some(completion.outcome.as_str())
            && previous.receipt.as_ref() == Some(&completion.receipt)
        {
            let reference = previous
                .artifact_ref
                .clone()
                .ok_or(ArtifactError::Corrupt)?;
            let directory = files.clone();
            let requested = reference.clone();
            let identity = staged.identity.clone();
            let permit = self
                .file_budget
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| ArtifactError::StorageUnavailable)?;
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let (saved, _) = directory.read(&requested, 0, 1)?;
                if saved != identity {
                    return Err(ArtifactError::Corrupt);
                }
                Ok::<_, ArtifactError>(())
            })
            .await
            .map_err(|_| ArtifactError::StorageUnavailable)??;
            reference
        } else if previous.status == "running" {
            let permit = self
                .file_budget
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| ArtifactError::StorageUnavailable)?;
            let reference = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                files.finalize(staged)
            })
            .await
            .map_err(|_| ArtifactError::StorageUnavailable)??;
            finish_in(&tx, env, completion, &reference).await?;
            reference
        } else {
            return Err(ArtifactError::Conflict.into());
        };
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(reference)
    }
    async fn read_tool_artifact_binding(
        &self,
        env: &EnterpriseOwnerEnvelope,
    ) -> Result<Option<String>, RepositoryError> {
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        Self::fence_envelope_in(&tx, &proof, env).await?;
        let binding = frozen_in(&tx, &env.scope, &env.owner.run_id)
            .await?
            .ok_or(CoordinationError::Corrupt)?
            .artifact_store_id;
        if binding.as_ref().is_some_and(|id| {
            !ArtifactIdentity {
                sha256: id.clone(),
                bytes: 0,
            }
            .valid()
        }) {
            return Err(CoordinationError::Corrupt.into());
        }
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(binding)
    }
}
