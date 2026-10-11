//! 持久子运行复用原准入事务、冻结能力和结果 reservation 状态。
use crate::canonical::{PgCanonicalStore, field, set_scope, unsigned};
use crate::events::append_in;
use crate::executions::{AdmissionPurpose, record_in};
use crate::facts::frozen_in;
use crate::identities::{InheritedAuthorization, exact_owner_in};
use crate::sessions::{encode, metadata_in};
use crate::{SCOPE, now, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::{IdentityReadRepository, RunDelegationRepository};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio_postgres::Transaction;
fn run_input(run: &RunId) -> Result<(), CoordinationError> {
    if run.0.is_empty() || run.0.len() > 1024 {
        Err(CoordinationError::InvalidRequest)
    } else {
        Ok(())
    }
}
fn request_input(
    env: &EnterpriseOwnerEnvelope,
    request: &DelegationRequest,
) -> Result<(), CoordinationError> {
    if request.parent_session_id != env.owner.session_key
        || request.parent_run_id != env.owner.run_id
        || request.child_session_id.0.is_empty()
        || request.child_session_id.0.len() > 1024
        || request.child_session_id == request.parent_session_id
        || request.spawn_key.trim().is_empty()
        || request.spawn_key.len() > 128
        || request.task.trim().is_empty()
        || request.task.len() > 65536
        || request.context_source_ids.len() > 4
        || request.context_source_ids.iter().any(|id| id.len() > 512)
        || request.tools.is_empty()
        || request.tools.len() > 128
        || request.tools.iter().any(|name| name != "read_file")
        || request.permission_mode != "request_approval"
        || request.cwd.len() > 16384
        || !(1..=15).contains(&request.max_rounds)
        || !(1..=32000).contains(&request.max_tokens)
        || !(1..=32).contains(&request.max_tool_calls)
    {
        return Err(CoordinationError::InvalidRequest);
    }
    encode(request, 131072)?;
    Ok(())
}
pub(crate) async fn read_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    child: &RunId,
    allowed: Option<&RunId>,
    lock: bool,
) -> Result<Option<DelegationRecord>, RepositoryError> {
    run_input(child)?;
    let mut values = scope_values(scope);
    values.push(child.0.clone());
    let suffix = if lock { " FOR UPDATE OF d" } else { "" };
    // 大小/JSON检查在正文转换前完成；四域过滤在传输 payload 前完成。
    let row=db.query_opt(&format!("SELECT CASE WHEN octet_length(d.parent_lifetime)<=128 THEN d.parent_lifetime END,CASE WHEN octet_length(d.child_lifetime)<=128 THEN d.child_lifetime END,CASE WHEN octet_length(d.root_session_id) BETWEEN 1 AND 1024 AND octet_length(d.root_run_id) BETWEEN 1 AND 1024 AND octet_length(d.parent_session_id) BETWEEN 1 AND 1024 AND octet_length(d.parent_run_id) BETWEEN 1 AND 1024 AND octet_length(d.child_session_id) BETWEEN 1 AND 1024 AND octet_length(d.spawn_key) BETWEEN 1 AND 128 AND octet_length(d.tools_json)<=65536 AND octet_length(d.permission_mode)<=64 AND octet_length(d.cwd)<=16384 AND COALESCE(octet_length(d.route_json),0)<=1048576 AND COALESCE(octet_length(d.terminal_json),0)<=131072 AND COALESCE(octet_length(d.reservation_owner),0)<=256 AND pg_input_is_valid(d.tools_json,'jsonb') AND (d.route_json IS NULL OR pg_input_is_valid(d.route_json,'jsonb')) AND (d.terminal_json IS NULL OR pg_input_is_valid(d.terminal_json,'jsonb')) THEN ((to_jsonb(d)-ARRAY['organization_id','tenant_id','project_id','agent_uid','id','parent_lifetime','child_lifetime','tools_json','route_json','terminal_json'])||jsonb_build_object('tools',d.tools_json::jsonb,'route',d.route_json::jsonb,'terminal',d.terminal_json::jsonb,'content',r.content,'error_code',r.error_code,'error_message',r.error_message))::text END FROM delegations d JOIN runs r ON (r.organization_id,r.tenant_id,r.project_id,r.agent_uid,r.id)=(d.organization_id,d.tenant_id,d.project_id,d.agent_uid,d.child_run_id) WHERE d.organization_id=$1 AND d.tenant_id=$2 AND d.project_id=$3 AND d.agent_uid=$4 AND d.child_run_id=$5{suffix}"),&parameters(&values)).await.map_err(unavailable)?;
    let Some(row) = row else { return Ok(None) };
    let raw = field(&row, 2)?;
    if raw.len() > 4 * 1024 * 1024 {
        return Err(CoordinationError::Corrupt.into());
    }
    let record: DelegationRecord =
        serde_json::from_str(&raw).map_err(|_| CoordinationError::Corrupt)?;
    if record.child_run_id != *child
        || record.child_run_id == record.parent_run_id
        || record.child_run_id == record.root_run_id
        || !(1..=2).contains(&record.depth)
        || record.revision < 0
        || record.depth == 1 && record.root_run_id != record.parent_run_id
        || record.depth == 2 && record.root_run_id == record.parent_run_id
        || match record.result_state.as_str() {
            "unconsumed" => {
                record.reservation_owner.is_some() || record.reservation_expires_at_ms.is_some()
            }
            "reserved" => {
                record.reservation_owner.is_none() || record.reservation_expires_at_ms.is_none()
            }
            "delivered" => {
                record.reservation_owner.is_none() || record.reservation_expires_at_ms.is_some()
            }
            _ => true,
        }
        || record.max_rounds <= 0
        || record.max_tokens <= 0
        || record.max_tool_calls <= 0
        || !matches!(
            record.result_state.as_str(),
            "unconsumed" | "reserved" | "delivered"
        )
        || record.tools.len() > 128
        || record.tools.iter().any(|name| name != "read_file")
    {
        return Err(CoordinationError::Corrupt.into());
    }
    if allowed.is_some_and(|run| run != &record.parent_run_id && run != &record.root_run_id) {
        return Err(AuthorizationError::Forbidden.into());
    }
    let actual = exact_owner_in(db, scope, child).await?;
    let parent = exact_owner_in(db, scope, &record.parent_run_id)
        .await
        .map_err(|error| {
            if error == CoordinationError::Forbidden {
                CoordinationError::Corrupt
            } else {
                error
            }
        })?;
    let root = exact_owner_in(db, scope, &record.root_run_id)
        .await
        .map_err(|error| {
            if error == CoordinationError::Forbidden {
                CoordinationError::Corrupt
            } else {
                error
            }
        })?;
    if actual.session_key != record.child_session_id
        || actual.session_lifetime_id.0 != field(&row, 1)?
        || parent.session_key != record.parent_session_id
        || parent.session_lifetime_id.0 != field(&row, 0)?
        || root.session_key != record.root_session_id
    {
        return Err(CoordinationError::Corrupt.into());
    }
    if record.depth == 2 {
        let mut parent_values = scope_values(scope);
        parent_values.push(record.parent_run_id.0.clone());
        let ancestor = db.query_opt(&format!("SELECT depth,CASE WHEN octet_length(root_run_id) BETWEEN 1 AND 1024 THEN root_run_id END FROM delegations WHERE {SCOPE} AND child_run_id=$5"), &parameters(&parent_values)).await.map_err(unavailable)?.ok_or(CoordinationError::Corrupt)?;
        if ancestor.try_get::<_, i64>(0).map_err(unavailable)? != 1
            || field(&ancestor, 1)? != record.root_run_id.0
        {
            return Err(CoordinationError::Corrupt.into());
        }
    }
    let run = record_in(db, scope, child).await?;
    if run.status != record.status {
        return Err(CoordinationError::Corrupt.into());
    }
    if let Some(terminal) = &record.terminal {
        let saved: RunRecord =
            serde_json::from_value(terminal.clone()).map_err(|_| CoordinationError::Corrupt)?;
        if saved.run_id != run.run_id
            || saved.session_id != run.session_id
            || saved.turn_id != run.turn_id
            || saved.status != run.status
            || saved.content != run.content
            || saved.error_code != run.error_code
            || saved.error_message != run.error_message
        {
            return Err(CoordinationError::Corrupt.into());
        }
    }
    Ok(Some(record))
}
async fn spawned_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    parent: &RunId,
    spawn: &str,
) -> Result<Option<DelegationRecord>, RepositoryError> {
    let mut values = scope_values(scope);
    values.extend([parent.0.clone(), spawn.into()]);
    let row=db.query_opt(&format!("SELECT CASE WHEN octet_length(child_run_id) BETWEEN 1 AND 1024 THEN child_run_id END FROM delegations WHERE {SCOPE} AND parent_run_id=$5 AND spawn_key=$6"),&parameters(&values)).await.map_err(unavailable)?;
    match row {
        Some(row) => read_in(db, scope, &RunId(field(&row, 0)?), Some(parent), false).await,
        None => Ok(None),
    }
}
pub(crate) struct Prepared {
    pub snapshot: RunSnapshot,
    root_key: SessionKey,
    root_run: RunId,
    depth: i64,
}
pub(crate) async fn prepare_in(
    db: &Transaction<'_>,
    parent: &EnterpriseOwnerEnvelope,
    request: &DelegationRequest,
) -> Result<Prepared, RepositoryError> {
    request_input(parent, request)?;
    let mut snapshot = frozen_in(db, &parent.scope, &parent.owner.run_id)
        .await?
        .ok_or(CoordinationError::Corrupt)?;
    if snapshot.cwd != request.cwd
        || request.max_tokens as usize > snapshot.context_token_budget
        || snapshot
            .max_tool_calls
            .is_some_and(|budget| request.max_tool_calls as u64 > budget)
        || request.tools.iter().any(|name| {
            !snapshot
                .tools
                .iter()
                .any(|descriptor| &descriptor.name == name)
        })
    {
        return Err(AuthorizationError::Forbidden.into());
    }
    snapshot.entry_channel = HookChannel::Subagent;
    // 执行指令仅属于原工作流节点，派生 run 保留冻结授权但不重复执行父节点。
    snapshot.workflow = None;
    let mut ids = request.context_source_ids.clone();
    for id in &mut ids {
        if id == "parent_input" {
            let mut values = scope_values(&parent.scope);
            values.push(parent.owner.run_id.0.clone());
            let row=db.query_opt(&format!("SELECT start_seq FROM transcript_batches WHERE {SCOPE} AND run_id=$5 ORDER BY start_seq,storage_row_id LIMIT 1"),&parameters(&values)).await.map_err(unavailable)?.ok_or(CoordinationError::Conflict)?;
            *id = format!(
                "{}:{}",
                parent.owner.session_lifetime_id.0,
                unsigned(row.try_get(0).map_err(unavailable)?)?
            );
        }
        let (life, seq) = id
            .rsplit_once(':')
            .ok_or(CoordinationError::InvalidRequest)?;
        let seq = seq
            .parse::<u64>()
            .map_err(|_| CoordinationError::InvalidRequest)?;
        *id = format!("{life}:{seq}");
    }
    if ids.iter().collect::<std::collections::BTreeSet<_>>().len() != ids.len() {
        return Err(CoordinationError::InvalidRequest.into());
    }
    snapshot.delegation_context = if ids.is_empty() {
        None
    } else {
        let mut sources = Vec::new();
        for id in &ids {
            sources.push(
                crate::memories::source_in(db, &parent.scope, &parent.owner, id, 1024).await?,
            );
        }
        let raw = encode(&(&parent.owner, &sources), 8192)?;
        let context = DelegationContext {
            version: 1,
            parent: parent.owner.clone(),
            sources,
            digest: format!("{:x}", Sha256::digest(raw.as_bytes())),
        };
        let size = encode(&context, 8192)?.len();
        if size > (request.max_tokens as usize).min(snapshot.context_token_budget) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        Some(context)
    };
    snapshot
        .tools
        .retain(|descriptor| request.tools.contains(&descriptor.name));
    snapshot.tool_catalog_digest = format!(
        "{:x}",
        Sha256::digest(encode(&snapshot.tools, 65536)?.as_bytes())
    );
    snapshot.context_token_budget = snapshot
        .context_token_budget
        .min(request.max_tokens as usize);
    if let Some(policy) = &snapshot.context_policy_fingerprint {
        let (budget, suffix) = policy.split_once(':').ok_or(CoordinationError::Corrupt)?;
        let budget = budget
            .parse::<usize>()
            .map_err(|_| CoordinationError::Corrupt)?;
        snapshot.context_policy_fingerprint = Some(format!(
            "{}:{suffix}",
            budget.min(request.max_tokens as usize)
        ));
    }
    snapshot.permission_mode = request.permission_mode.clone();
    snapshot.max_tool_calls = Some(
        snapshot
            .max_tool_calls
            .unwrap_or(u64::MAX)
            .min(request.max_tool_calls as u64),
    );
    let ancestor = read_in(db, &parent.scope, &parent.owner.run_id, None, false).await?;
    let (root_key, root_run, depth) = ancestor.map_or(
        (
            parent.owner.session_key.clone(),
            parent.owner.run_id.clone(),
            1,
        ),
        |record| (record.root_session_id, record.root_run_id, record.depth + 1),
    );
    if depth > 2 {
        return Err(CoordinationError::Conflict.into());
    }
    if let Some(existing) =
        spawned_in(db, &parent.scope, &parent.owner.run_id, &request.spawn_key).await?
    {
        let stored = frozen_in(db, &parent.scope, &existing.child_run_id)
            .await?
            .ok_or(CoordinationError::Corrupt)?;
        let mut values = scope_values(&parent.scope);
        values.push(existing.child_run_id.0.clone());
        let input=db.query_one(&format!("SELECT CASE WHEN octet_length(input)<=65536 THEN input END FROM runs WHERE {SCOPE} AND id=$5"),&parameters(&values)).await.map_err(unavailable)?;
        if existing.child_session_id != request.child_session_id
            || existing.max_rounds != request.max_rounds
            || existing.max_tokens != request.max_tokens
            || existing.max_tool_calls != request.max_tool_calls
            || existing.deadline_ms != request.deadline_ms
            || field(&input, 0)? != request.task
            || encode(&snapshot, 1048576)? != encode(&stored, 1048576)?
        {
            return Err(CoordinationError::Conflict.into());
        }
    } else {
        if metadata_in(db, &parent.scope, &request.child_session_id, false)
            .await?
            .is_some()
        {
            return Err(CoordinationError::Conflict.into());
        }
        if !matches!(
            record_in(db, &parent.scope, &parent.owner.run_id)
                .await?
                .status,
            RunStatus::Running | RunStatus::WaitingInteraction
        ) || request.deadline_ms <= now(db).await?
        {
            return Err(CoordinationError::Conflict.into());
        }
        let mut values = scope_values(&parent.scope);
        values.push(root_run.0.clone());
        let row=db.query_one(&format!("SELECT count(*)::bigint,count(*) FILTER(WHERE status IN ('queued','running','waiting_interaction'))::bigint FROM delegations WHERE {SCOPE} AND root_run_id=$5"),&parameters(&values)).await.map_err(unavailable)?;
        if row.try_get::<_, i64>(0).map_err(unavailable)? >= 8
            || row.try_get::<_, i64>(1).map_err(unavailable)? >= 4
        {
            return Err(CoordinationError::Conflict.into());
        }
    }
    Ok(Prepared {
        snapshot,
        root_key,
        root_run,
        depth,
    })
}
pub(crate) async fn insert_in(
    db: &Transaction<'_>,
    parent: &EnterpriseOwnerEnvelope,
    child: &EnterpriseOwnerEnvelope,
    request: &DelegationRequest,
    prepared: &Prepared,
    at: i64,
) -> Result<(), RepositoryError> {
    let mut values = scope_values(&parent.scope);
    values.extend([
        prepared.root_key.0.clone(),
        prepared.root_run.0.clone(),
        parent.owner.session_key.0.clone(),
        parent.owner.run_id.0.clone(),
        request.spawn_key.clone(),
        child.owner.session_key.0.clone(),
        child.owner.run_id.0.clone(),
        parent.owner.session_lifetime_id.0.clone(),
        child.owner.session_lifetime_id.0.clone(),
        prepared.depth.to_string(),
        "queued".into(),
        at.to_string(),
        encode(&request.tools, 65536)?,
        request.permission_mode.clone(),
        prepared
            .snapshot
            .route
            .as_ref()
            .map(|route| encode(route, 1048576))
            .transpose()?
            .unwrap_or_default(),
        request.cwd.clone(),
        request.max_rounds.to_string(),
        request.max_tokens.to_string(),
        request.max_tool_calls.to_string(),
        request.deadline_ms.to_string(),
    ]);
    db.execute("INSERT INTO delegations(organization_id,tenant_id,project_id,agent_uid,root_session_id,root_run_id,parent_session_id,parent_run_id,spawn_key,child_session_id,child_run_id,parent_lifetime,child_lifetime,depth,status,created_at_ms,tools_json,permission_mode,route_json,cwd,max_rounds,max_tokens,max_tool_calls,deadline_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::text::bigint,$15,$16::text::bigint,$17,$18,NULLIF($19,''),$20,$21::text::bigint,$22::text::bigint,$23::text::bigint,$24::text::bigint)",&parameters(&values)).await.map_err(unavailable)?;
    append_in(db,parent,"delegation_spawned",&serde_json::json!({"child_session_id":child.owner.session_key,"child_run_id":child.owner.run_id,"depth":prepared.depth,"status":"queued"})).await?;
    Ok(())
}
#[async_trait::async_trait]
impl RunDelegationRepository for PgCanonicalStore {
    async fn read_delegation_record(
        &self,
        scope: &TenantContext,
        child: &RunId,
    ) -> Result<Option<DelegationRecord>, RepositoryError> {
        self.delegation_read(scope, child, None).await
    }
    async fn read_scoped_delegation(
        &self,
        scope: &TenantContext,
        parent: &RunId,
        child: &RunId,
    ) -> Result<Option<DelegationRecord>, RepositoryError> {
        run_input(parent)?;
        self.delegation_read(scope, child, Some(parent)).await
    }
    async fn read_delegation_list(
        &self,
        scope: &TenantContext,
        root: &RunId,
    ) -> Result<Vec<DelegationRecord>, RepositoryError> {
        run_input(root)?;
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        exact_owner_in(&tx, scope, root).await?;
        let mut values = scope_values(scope);
        values.push(root.0.clone());
        let rows=tx.query(&format!("SELECT CASE WHEN octet_length(child_run_id) BETWEEN 1 AND 1024 THEN child_run_id END FROM delegations WHERE {SCOPE} AND root_run_id=$5 ORDER BY id LIMIT 9"),&parameters(&values)).await.map_err(unavailable)?;
        if rows.len() > 8 {
            return Err(CoordinationError::Corrupt.into());
        }
        let mut records = Vec::new();
        for row in rows {
            records.push(
                read_in(&tx, scope, &RunId(field(&row, 0)?), Some(root), false)
                    .await?
                    .ok_or(CoordinationError::Corrupt)?,
            );
        }
        encode(&records, 4 * 1024 * 1024)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(records)
    }
    async fn read_spawned_delegation(
        &self,
        scope: &TenantContext,
        parent: &RunId,
        spawn: &str,
    ) -> Result<Option<DelegationRecord>, RepositoryError> {
        run_input(parent)?;
        if spawn.is_empty() || spawn.len() > 128 {
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
        exact_owner_in(&tx, scope, parent).await?;
        let result = spawned_in(&tx, scope, parent, spawn).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
    async fn read_session_delegation(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
    ) -> Result<Option<DelegationRecord>, RepositoryError> {
        if key.0.is_empty() || key.0.len() > 1024 {
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
        let mut values = scope_values(scope);
        values.push(key.0.clone());
        let row=tx.query_opt(&format!("SELECT CASE WHEN octet_length(child_run_id) BETWEEN 1 AND 1024 THEN child_run_id END FROM delegations WHERE {SCOPE} AND child_session_id=$5"),&parameters(&values)).await.map_err(unavailable)?;
        let result = match row {
            Some(row) => read_in(&tx, scope, &RunId(field(&row, 0)?), None, false).await?,
            None => None,
        };
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
    async fn admit_child_delegation(
        &self,
        parent: &EnterpriseOwnerEnvelope,
        request: &DelegationRequest,
    ) -> Result<DelegationRecord, RepositoryError> {
        request_input(parent, request)?;
        let authorization = self
            .read_authorization(parent)
            .await?
            .ok_or(AuthorizationError::Forbidden)?;
        let snapshot = {
            let proof = self.execution_proof(parent).await?;
            let mut client = self.inner.client.lock().await;
            let tx = client.transaction().await.map_err(unavailable)?;
            set_scope(&tx, &parent.scope).await?;
            Self::fence_envelope_in(&tx, &proof, parent).await?;
            let prepared = prepare_in(&tx, parent, request).await?;
            tx.commit().await.map_err(unavailable)?;
            prepared.snapshot
        };
        let mut auth = AuthorizationRequest {
            context: authorization.context.clone(),
            expected_policy: authorization.policy.version.clone(),
            action: Action::SessionPrompt,
            attributes: authorization.attributes.clone(),
        };
        auth.context.request_id =
            RequestId::String(format!("delegation:{}", request.child_session_id.0));
        auth.attributes.entry_channel = HookChannel::Subagent;
        let admission = RunAdmission {
            workflow_node: None,
            session_key: request.child_session_id.clone(),
            expected_lifetime: None,
            request_id: auth.context.request_id.clone(),
            input: request.task.clone(),
            mode: AdmissionMode::Queue,
            plan_execution: None,
        };
        let (_, owner) = self
            .admit_in(
                &admission,
                &snapshot,
                &auth,
                Arc::new(InheritedAuthorization {
                    snapshot: authorization,
                    channel: HookChannel::Subagent,
                    create_session: true,
                }),
                AdmissionPurpose::Delegation { parent, request },
            )
            .await?;
        self.read_delegation_record(&parent.scope, &owner.run_id)
            .await?
            .ok_or_else(|| CoordinationError::Corrupt.into())
    }
    async fn reserve_child_result(
        &self,
        parent: &EnterpriseOwnerEnvelope,
        child: &RunId,
        owner: &str,
        revision: i64,
    ) -> Result<DelegationRecord, RepositoryError> {
        self.delegation_result(parent, child, owner, revision, None)
            .await
    }
    async fn transition_child_result(
        &self,
        parent: &EnterpriseOwnerEnvelope,
        child: &RunId,
        owner: &str,
        revision: i64,
        target: &str,
    ) -> Result<DelegationRecord, RepositoryError> {
        if !matches!(target, "unconsumed" | "delivered") {
            return Err(CoordinationError::InvalidRequest.into());
        }
        self.delegation_result(parent, child, owner, revision, Some(target))
            .await
    }
}
impl PgCanonicalStore {
    async fn delegation_read(
        &self,
        scope: &TenantContext,
        child: &RunId,
        parent: Option<&RunId>,
    ) -> Result<Option<DelegationRecord>, RepositoryError> {
        run_input(child)?;
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let result = read_in(&tx, scope, child, parent, false).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
    async fn delegation_result(
        &self,
        parent: &EnterpriseOwnerEnvelope,
        child: &RunId,
        owner: &str,
        revision: i64,
        target: Option<&str>,
    ) -> Result<DelegationRecord, RepositoryError> {
        run_input(child)?;
        if owner.is_empty() || owner.len() > 256 || revision < 0 || revision > i64::MAX - 2 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let proof = self.execution_proof(parent).await?;
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &parent.scope).await?;
        Self::fence_envelope_in(&tx, &proof, parent).await?;
        let record = read_in(&tx, &parent.scope, child, Some(&parent.owner.run_id), true)
            .await?
            .ok_or(CoordinationError::Conflict)?;
        let at = now(&tx).await?;
        let same_owner = record.reservation_owner.as_deref() == Some(owner);
        let mut values = scope_values(&parent.scope);
        values.extend([child.0.clone(), owner.into(), at.to_string()]);
        match target {
            None => {
                if !(same_owner
                    && (record.result_state == "delivered"
                        || record.result_state == "reserved"
                            && record
                                .reservation_expires_at_ms
                                .is_some_and(|expires| expires > at)))
                {
                    if !record.status.terminal() || record.revision != revision {
                        return Err(CoordinationError::Conflict.into());
                    }
                    if record.result_state == "reserved"
                        && record
                            .reservation_expires_at_ms
                            .is_some_and(|expires| expires <= at)
                    {
                        let mut released = values[..5].to_vec();
                        released.push(at.to_string());
                        tx.execute(&format!("UPDATE delegations SET result_state='unconsumed',reservation_owner=NULL,reservation_expires_at_ms=NULL,reservation_released_at_ms=$6::text::bigint,revision=revision+1 WHERE {SCOPE} AND child_run_id=$5"),&parameters(&released)).await.map_err(unavailable)?;
                    } else if record.result_state != "unconsumed" {
                        return Err(CoordinationError::Conflict.into());
                    }
                    tx.execute(&format!("UPDATE delegations SET result_state='reserved',reservation_owner=$6,reservation_expires_at_ms=$7::text::bigint+30000,revision=revision+1 WHERE {SCOPE} AND child_run_id=$5"),&parameters(&values)).await.map_err(unavailable)?;
                }
            }
            Some(target) => {
                if !(target == "delivered" && record.result_state == "delivered" && same_owner) {
                    if record.result_state != "reserved"
                        || !same_owner
                        || record.revision != revision
                        || target == "delivered"
                            && record
                                .reservation_expires_at_ms
                                .is_none_or(|expires| expires <= at)
                    {
                        return Err(CoordinationError::Conflict.into());
                    }
                    values.push(target.into());
                    tx.execute(&format!("UPDATE delegations SET result_state=$8,reservation_owner=CASE WHEN $8='delivered' THEN reservation_owner ELSE NULL END,reservation_expires_at_ms=NULL,reservation_released_at_ms=CASE WHEN $8='unconsumed' THEN $7::text::bigint ELSE reservation_released_at_ms END,revision=revision+1 WHERE {SCOPE} AND child_run_id=$5 AND reservation_owner=$6"),&parameters(&values)).await.map_err(unavailable)?;
                }
            }
        }
        let result = read_in(&tx, &parent.scope, child, Some(&parent.owner.run_id), false)
            .await?
            .ok_or(CoordinationError::Corrupt)?;
        Self::before_commit(&tx, &proof).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
}
