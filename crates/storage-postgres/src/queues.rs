//! PostgreSQL原持久队列与稳定恢复游标；scope过滤先于LIMIT和私有正文。
use crate::canonical::{PgCanonicalStore, field, set_scope};
use crate::executions::record_in;
use crate::identities::exact_owner_in;
use crate::sessions::{encode, metadata_in};
use crate::{SCOPE, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::RunQueueRepository;
use tokio_postgres::Transaction;
pub(crate) async fn queues_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    key: &SessionKey,
    run: Option<&RunId>,
) -> Result<Vec<QueuedMessage>, CoordinationError> {
    let meta = metadata_in(db, scope, key, false)
        .await?
        .filter(|meta| !meta.deleted)
        .ok_or(CoordinationError::Forbidden)?;
    if let Some(run) = run {
        if exact_owner_in(db, scope, run).await?.session_key != *key {
            return Err(CoordinationError::Forbidden);
        }
    }
    let mut values = scope_values(scope);
    values.extend([key.0.clone(), meta.lifetime.0.clone()]);
    // 独立run读取也计完整队列位置，不在过滤后重新编号。
    let row=db.query_one("SELECT count(*)::bigint,COALESCE(sum(octet_length(message)),0)::bigint FROM (SELECT q.message FROM queued_messages q JOIN runs r ON (r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id)=(q.organization_id,q.tenant_id,q.project_id,q.agent_uid,q.run_id) WHERE q.organization_id=$1 AND q.tenant_id=$2 AND q.project_id=$3 AND q.agent_uid=$4 AND q.session_id=$5 AND r.lifetime=$6 AND q.status='queued' LIMIT 65) bounded",&parameters(&values)).await.map_err(unavailable)?;
    if row.try_get::<_, i64>(0).map_err(unavailable)? > 64
        || row.try_get::<_, i64>(1).map_err(unavailable)? > 1024 * 1024 - 16384
    {
        return Err(CoordinationError::InvalidRequest);
    }
    let filter = run.map(|run| run.0.as_str());
    let mut params = parameters(&values);
    params.push(&filter);
    let rows=db.query("SELECT q.id,CASE WHEN octet_length(q.run_id) BETWEEN 1 AND 1024 THEN q.run_id END,CASE WHEN octet_length(q.message)<=65536 THEN q.message END,q.status,q.status=r.status OR q.status='running' AND r.status='waiting_interaction',CASE WHEN q.status='queued' THEN (SELECT count(*) FROM queued_messages prior WHERE (prior.organization_id,prior.tenant_id,prior.project_id,prior.agent_uid,prior.session_id)=(q.organization_id,q.tenant_id,q.project_id,q.agent_uid,q.session_id) AND prior.status='queued' AND prior.id<=q.id) ELSE 0 END FROM queued_messages q JOIN runs r ON (r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id,r.session_id)=(q.organization_id,q.tenant_id,q.project_id,q.agent_uid,q.run_id,q.session_id) WHERE q.organization_id=$1 AND q.tenant_id=$2 AND q.project_id=$3 AND q.agent_uid=$4 AND q.session_id=$5 AND r.lifetime=$6 AND (($7::text IS NULL AND q.status='queued') OR q.run_id=$7::text) ORDER BY q.id LIMIT 65",&params).await.map_err(unavailable)?;
    if rows.len() > 64 {
        return Err(CoordinationError::InvalidRequest);
    }
    let mut result = Vec::new();
    for row in rows {
        let position: i64 = row.try_get(5).map_err(unavailable)?;
        let status: String = row.try_get(3).map_err(unavailable)?;
        RunStatus::parse(&status).map_err(|_| CoordinationError::Corrupt)?;
        if !row.try_get::<_, bool>(4).map_err(unavailable)? || !(0..=64).contains(&position) {
            return Err(CoordinationError::Corrupt);
        }
        result.push(QueuedMessage {
            id: row.try_get(0).map_err(unavailable)?,
            position,
            session_id: key.clone(),
            run_id: RunId(field(&row, 1)?),
            message: field(&row, 2)?,
            status,
        });
    }
    encode(&result, 1024 * 1024 - 16384)?;
    Ok(result)
}
#[async_trait::async_trait]
impl RunQueueRepository for PgCanonicalStore {
    async fn read_queued_messages(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
    ) -> Result<Vec<QueuedMessage>, RepositoryError> {
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let result = queues_in(&tx, scope, key, None).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
    async fn read_queued_message(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        run: &RunId,
    ) -> Result<Option<QueuedMessage>, RepositoryError> {
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let mut result = queues_in(&tx, scope, key, Some(run)).await?;
        if result.len() > 1 {
            return Err(CoordinationError::Corrupt.into());
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(result.pop())
    }
    async fn read_recoverable_queue(
        &self,
        scope: &TenantContext,
        after: Option<QueueRecoveryCursor>,
        limit: usize,
    ) -> Result<QueueRecoveryPage, RepositoryError> {
        if limit == 0 || limit > 32 || after.is_some_and(|cursor| cursor.row_id <= 0) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let mut values = scope_values(scope);
        let cursor = after.unwrap_or(QueueRecoveryCursor {
            created_at_ms: i64::MIN,
            row_id: 0,
        });
        values.extend([
            cursor.created_at_ms.to_string(),
            cursor.row_id.to_string(),
            (limit + 1).to_string(),
        ]);
        let rows=tx.query("SELECT CASE WHEN octet_length(r.id) BETWEEN 1 AND 1024 THEN r.id END,r.created_at_ms,r.storage_row_id FROM runs r JOIN session_heads h ON (h.organization_id,h.tenant_id,h.project_id,h.agent_uid,h.session_id,h.lifetime)=(r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.session_id,r.lifetime) WHERE r.organization_id=$1 AND r.tenant_id=$2 AND r.project_id=$3 AND r.agent_uid=$4 AND r.status='queued' AND NOT h.deleted AND (r.created_at_ms,r.storage_row_id)>($5::text::bigint,$6::text::bigint) AND NOT EXISTS(SELECT 1 FROM plan_decisions d WHERE (d.organization_id,d.tenant_id,d.project_id,d.agent_uid,d.run_id)=(r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id) AND d.decision='execute') ORDER BY r.created_at_ms,r.storage_row_id LIMIT $7::text::bigint",&parameters(&values)).await.map_err(unavailable)?;
        let has_more = rows.len() > limit as usize;
        let mut runs = Vec::new();
        let mut cursor = after;
        for row in rows.into_iter().take(limit as usize) {
            let run = RunId(field(&row, 0)?);
            exact_owner_in(&tx, scope, &run).await?;
            runs.push(run);
            cursor = Some(QueueRecoveryCursor {
                created_at_ms: row.try_get(1).map_err(unavailable)?,
                row_id: row.try_get(2).map_err(unavailable)?,
            });
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(QueueRecoveryPage {
            runs,
            cursor,
            has_more,
        })
    }
    async fn lookup_request_run(
        &self,
        scope: &TenantContext,
        key: Option<&SessionKey>,
        request: &RequestId,
    ) -> Result<Option<RunRecord>, RepositoryError> {
        let wire = encode(request, 8192)?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        if let Some(key) = key {
            metadata_in(&tx, scope, key, false)
                .await?
                .filter(|meta| !meta.deleted)
                .ok_or(CoordinationError::Forbidden)?;
        }
        let mut values = scope_values(scope);
        values.push(wire);
        let key_value = key.map(|key| key.0.as_str());
        let mut params = parameters(&values);
        params.push(&key_value);
        let rows=tx.query("SELECT CASE WHEN octet_length(r.id) BETWEEN 1 AND 1024 THEN r.id END FROM runs r JOIN session_heads h ON (h.organization_id,h.tenant_id,h.project_id,h.agent_uid,h.session_id,h.lifetime)=(r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.session_id,r.lifetime) WHERE r.organization_id=$1 AND r.tenant_id=$2 AND r.project_id=$3 AND r.agent_uid=$4 AND r.request_id_wire_json=$5 AND NOT h.deleted AND ($6::text IS NULL OR r.session_id=$6::text) ORDER BY r.id LIMIT 2",&params).await.map_err(unavailable)?;
        if rows.len() > 1 {
            return Err(CoordinationError::Conflict.into());
        }
        let run = rows
            .into_iter()
            .next()
            .map(|row| field(&row, 0).map(RunId))
            .transpose()?;
        if let Some(run) = &run {
            exact_owner_in(&tx, scope, run).await?;
        }
        let record = if let Some(run) = &run {
            Some(record_in(&tx, scope, run).await?)
        } else {
            None
        };
        tx.commit().await.map_err(unavailable)?;
        Ok(record)
    }
    async fn read_run_record(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Option<RunRecord>, RepositoryError> {
        if run.0.is_empty() || run.0.len() > 1024 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let mut values = scope_values(scope);
        values.push(run.0.clone());
        let exists: bool = tx
            .query_one(
                &format!("SELECT EXISTS(SELECT 1 FROM runs WHERE {SCOPE} AND id=$5)"),
                &parameters(&values),
            )
            .await
            .map_err(unavailable)?
            .try_get(0)
            .map_err(unavailable)?;
        let result = if exists {
            exact_owner_in(&tx, scope, run).await?;
            Some(record_in(&tx, scope, run).await?)
        } else {
            None
        };
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
}
