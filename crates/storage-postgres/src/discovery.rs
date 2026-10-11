//! 固定目录 generation 的持久发现许可及原取消事件。
use crate::canonical::{PgCanonicalStore, field, set_scope};
use crate::events::{append_in, seal_in};
use crate::executions::record_in;
use crate::facts::frozen_in;
use crate::identities::exact_owner_in;
use crate::sessions::encode;
use crate::{SCOPE, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::{
    RunDiscoveryRepository, validate_discovery_receipt, validate_frozen_tool_catalog,
};
use sha2::{Digest, Sha256};
use tokio_postgres::Transaction;
fn values(scope: &TenantContext, run: &RunId) -> Vec<String> {
    let mut values = scope_values(scope);
    values.push(run.0.clone());
    values
}
async fn catalog_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    owner: &ExactOwner,
    generation: &str,
) -> Result<RunSnapshot, CoordinationError> {
    let snapshot = frozen_in(db, scope, &owner.run_id)
        .await?
        .ok_or(CoordinationError::Corrupt)?;
    validate_frozen_tool_catalog(&snapshot, generation)?;
    Ok(snapshot)
}
async fn receipts_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    owner: &ExactOwner,
) -> Result<Vec<ToolDiscoveryReceipt>, CoordinationError> {
    let values = values(scope, &owner.run_id);
    let size=db.query_one(&format!("SELECT count(*)::bigint,COALESCE(sum(octet_length(receipt_json)+octet_length(operation_id)+octet_length(receipt_digest)),0)::bigint FROM tool_discoveries WHERE {SCOPE} AND run_id=$5"),&parameters(&values)).await.map_err(unavailable)?;
    if size.try_get::<_, i64>(0).map_err(unavailable)? > 128
        || size.try_get::<_, i64>(1).map_err(unavailable)? > 1024 * 1024
    {
        return Err(CoordinationError::Corrupt);
    }
    let rows=db.query(&format!("SELECT CASE WHEN octet_length(operation_id) BETWEEN 1 AND 512 THEN operation_id END,CASE WHEN octet_length(receipt_json)<=32768 THEN receipt_json END,CASE WHEN octet_length(receipt_digest)=64 THEN receipt_digest END FROM tool_discoveries WHERE {SCOPE} AND run_id=$5 ORDER BY storage_row_id LIMIT 128"),&parameters(&values)).await.map_err(unavailable)?;
    let mut output = Vec::new();
    for row in rows {
        let raw = field(&row, 1)?;
        if format!("{:x}", Sha256::digest(raw.as_bytes())) != field(&row, 2)? {
            return Err(CoordinationError::Corrupt);
        }
        let receipt: ToolDiscoveryReceipt =
            serde_json::from_str(&raw).map_err(|_| CoordinationError::Corrupt)?;
        let snapshot = catalog_in(db, scope, owner, &receipt.result.catalog_generation)
            .await
            .map_err(|error| {
                if error == CoordinationError::Conflict {
                    CoordinationError::Corrupt
                } else {
                    error
                }
            })?;
        validate_discovery_receipt(&receipt, &snapshot, owner)?;
        if field(&row, 0)? != receipt.operation_id {
            return Err(CoordinationError::Corrupt);
        }
        output.push(receipt);
    }
    encode(&output, 1024 * 1024)?;
    Ok(output)
}
async fn cancelled_in(
    db: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
) -> Result<bool, CoordinationError> {
    db.query_one(&format!("SELECT EXISTS(SELECT 1 FROM events WHERE {SCOPE} AND run_id=$5 AND event='cancellation_requested')"),&parameters(&values(&env.scope,&env.owner.run_id))).await.map_err(unavailable)?.try_get(0).map_err(unavailable)
}
#[async_trait::async_trait]
impl RunDiscoveryRepository for PgCanonicalStore {
    async fn publish_run_discovery(
        &self,
        env: &EnterpriseOwnerEnvelope,
        receipt: &ToolDiscoveryReceipt,
    ) -> Result<ToolDiscoveryReceipt, RepositoryError> {
        if receipt.owner != env.owner {
            return Err(AuthorizationError::StaleOwner.into());
        }
        let raw = encode(receipt, 32768)?;
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        Self::fence_envelope_in(&tx, &proof, env).await?;
        let snapshot = catalog_in(
            &tx,
            &env.scope,
            &env.owner,
            &receipt.result.catalog_generation,
        )
        .await?;
        validate_discovery_receipt(receipt, &snapshot, &env.owner)
            .map_err(|_| CoordinationError::InvalidRequest)?;
        let previous = receipts_in(&tx, &env.scope, &env.owner).await?;
        if let Some(old) = previous
            .iter()
            .find(|old| old.operation_id == receipt.operation_id)
        {
            if old != receipt {
                return Err(CoordinationError::Conflict.into());
            }
            Self::before_commit(&tx, &proof).await?;
            tx.commit().await.map_err(unavailable)?;
            return Ok(old.clone());
        }
        let record = record_in(&tx, &env.scope, &env.owner.run_id).await?;
        if record.kind != RunKind::Chat
            || record.status != RunStatus::Running
            || cancelled_in(&tx, env).await?
            || previous.len() >= 128
        {
            return Err(CoordinationError::Conflict.into());
        }
        if encode(&previous, 1024 * 1024)?
            .len()
            .saturating_add(raw.len())
            .saturating_add(receipt.operation_id.len() + 64)
            > 1024 * 1024
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let digest = format!("{:x}", Sha256::digest(raw.as_bytes()));
        let mut values = scope_values(&env.scope);
        values.extend([
            receipt.operation_id.clone(),
            env.owner.run_id.0.clone(),
            raw,
            digest,
        ]);
        tx.execute("INSERT INTO tool_discoveries(organization_id,tenant_id,project_id,agent_uid,operation_id,run_id,receipt_json,receipt_digest) VALUES($1,$2,$3,$4,$5,$6,$7,$8)",&parameters(&values)).await.map_err(unavailable)?;
        append_in(&tx,env,"tool_discovered",&serde_json::json!({"operation_id":receipt.operation_id,"catalog_generation":receipt.result.catalog_generation,"names":receipt.result.tools.iter().map(|tool|&tool.name).collect::<Vec<_>>()})).await?;
        seal_in(&tx, env).await?;
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(receipt.clone())
    }
    async fn read_discovered_tool(
        &self,
        env: &EnterpriseOwnerEnvelope,
        generation: &str,
        name: &str,
    ) -> Result<bool, RepositoryError> {
        if generation.len() != 64
            || !generation.bytes().all(|byte| byte.is_ascii_hexdigit())
            || name.is_empty()
            || name.len() > 256
            || name.chars().any(char::is_whitespace)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        Self::fence_envelope_in(&tx, &proof, env).await?;
        catalog_in(&tx, &env.scope, &env.owner, generation).await?;
        if record_in(&tx, &env.scope, &env.owner.run_id).await?.status != RunStatus::Running
            || cancelled_in(&tx, env).await?
        {
            return Err(CoordinationError::Conflict.into());
        }
        let permitted = receipts_in(&tx, &env.scope, &env.owner)
            .await?
            .iter()
            .any(|receipt| {
                receipt.result.catalog_generation == generation
                    && receipt.result.tools.iter().any(|tool| tool.name == name)
            });
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(permitted)
    }
    async fn read_run_discoveries(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Vec<ToolDiscoveryReceipt>, RepositoryError> {
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
        let owner = exact_owner_in(&tx, scope, run).await?;
        let receipts = receipts_in(&tx, scope, &owner).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(receipts)
    }
    async fn request_run_cancellation(
        &self,
        env: &EnterpriseOwnerEnvelope,
    ) -> Result<(), RepositoryError> {
        let proof = self.execution_proof(env).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &env.scope).await?;
        Self::fence_envelope_in(&tx, &proof, env).await?;
        if !matches!(
            record_in(&tx, &env.scope, &env.owner.run_id).await?.status,
            RunStatus::Running | RunStatus::WaitingInteraction
        ) {
            return Err(CoordinationError::Conflict.into());
        }
        if !cancelled_in(&tx, env).await? {
            append_in(
                &tx,
                env,
                "cancellation_requested",
                &serde_json::json!({"owner":env.owner}),
            )
            .await?;
            seal_in(&tx, env).await?;
        }
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }
}
