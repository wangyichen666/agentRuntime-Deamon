//! Durable child-run ownership and result delivery on the existing RunStore connection.
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::json;

use super::{
    RequestId, RunId, RunRecord, RunStatus, RunStore, RuntimeError, SessionId, TurnId,
    insert_event, now_ms, read_run_in,
};

const MAX_DEPTH: i64 = 2;
const MAX_ROOT_SPAWNS: i64 = 8;
const MAX_ROOT_ACTIVE: i64 = 4;

pub use agent_core::{DelegationRecord, DelegationRequest};

impl RunStore {
    pub fn admit_delegation(
        &self,
        request: DelegationRequest,
    ) -> Result<DelegationRecord, RuntimeError> {
        self.admit_delegation_scoped(request, None)
    }

    pub(crate) fn admit_delegation_scoped(
        &self,
        request: DelegationRequest,
        envelope: Option<&agent_core::EnterpriseOwnerEnvelope>,
    ) -> Result<DelegationRecord, RuntimeError> {
        if request.context_source_ids.len() > 4
            || request.context_source_ids.iter().any(|id| id.len() > 512)
            || request
                .context_source_ids
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != request.context_source_ids.len()
            || request.task.len() > 65536
            || request.spawn_key.len() > 128
            || request.task.trim().is_empty()
            || request.spawn_key.trim().is_empty()
            || request.tools.is_empty()
            || request.max_rounds <= 0
            || request.max_tokens <= 0
            || request.max_tool_calls <= 0
        {
            return Err(RuntimeError::Protocol("子 Agent 参数或预算无效".into()));
        }
        let mut connection = self.lock_connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        crate::run_delegations::check_parent_execution_in(
            &transaction,
            &request.parent_session_id,
            &request.parent_run_id,
            envelope,
        )?;
        if envelope.is_some() {
            let frozen = super::requests::frozen_in(&transaction, &request.parent_run_id)?;
            if request.cwd != frozen.cwd
                || request.permission_mode != "request_approval"
                || request.tools.iter().any(|name| name != "read_file")
                || request.max_tokens as u64 > frozen.context_token_budget as u64
                || frozen
                    .max_tool_calls
                    .is_some_and(|limit| request.max_tool_calls as u64 > limit)
                || request.max_rounds > 15
            {
                return Err(agent_core::AuthorizationError::Forbidden.into());
            }
        }
        let parent = read_run_in(&transaction, &request.parent_run_id.0)?
            .ok_or_else(|| RuntimeError::Protocol("父 run 不存在".into()))?;
        if parent.session_id != request.parent_session_id {
            return Err(RuntimeError::Protocol("父 run 不属于指定 session".into()));
        }
        let mut request = request;
        // 显式别名让调用方无需猜测lifetime；映射到父run首条准入输入，重试不变。
        if request
            .context_source_ids
            .iter()
            .any(|id| id == "parent_input")
        {
            let (lifetime,seq):(String,u64)=transaction.query_row("SELECT lifetime,start_seq FROM transcript_batches WHERE run_id=?1 ORDER BY start_seq LIMIT 1",params![request.parent_run_id.0],|r|Ok((r.get(0)?,r.get(1)?)))?;
            for id in &mut request.context_source_ids {
                if id == "parent_input" {
                    *id = format!("{lifetime}:{seq}");
                }
            }
        }
        for id in &mut request.context_source_ids {
            let (lifetime, seq) = id
                .rsplit_once(':')
                .ok_or_else(|| RuntimeError::Protocol("交接来源ID无效".into()))?;
            let seq = seq
                .parse::<u64>()
                .map_err(|_| RuntimeError::Protocol("交接来源序号无效".into()))?;
            *id = format!("{lifetime}:{seq}");
        }
        if request
            .context_source_ids
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len()
            != request.context_source_ids.len()
        {
            return Err(RuntimeError::Protocol("交接来源存在重复ID".into()));
        }
        let duplicate: Option<String> = transaction
            .query_row(
                "SELECT child_run_id FROM delegations WHERE parent_run_id=?1 AND spawn_key=?2",
                params![request.parent_run_id.0, request.spawn_key],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(child) = duplicate {
            let record = read_delegation_in(&transaction, &child)?.ok_or_else(|| {
                RuntimeError::Internal("缺少持久记录：existing delegation".into())
            })?;
            let existing_task: Option<String> = transaction.query_row(
                "SELECT CASE WHEN length(CAST(input AS BLOB))<=65536 THEN input END FROM runs WHERE id=?1",
                params![child],
                |row| row.get(0),
            )?;
            let exists: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM run_snapshots WHERE run_id=?1)",
                params![child],
                |r| r.get(0),
            )?;
            let captured = if exists {
                Some(super::requests::frozen_in(
                    &transaction,
                    &RunId(child.clone()),
                )?)
            } else {
                None
            };
            let saved_ids =
                captured
                    .and_then(|s| s.delegation_context)
                    .map_or_else(Vec::new, |c| {
                        c.sources
                            .into_iter()
                            .map(|s| s.source_id)
                            .collect::<Vec<_>>()
                    });
            if saved_ids != request.context_source_ids
                || existing_task.as_deref() != Some(request.task.as_str())
                || record.tools != request.tools
                || record.max_rounds != request.max_rounds
                || record.max_tokens != request.max_tokens
                || record.max_tool_calls != request.max_tool_calls
            {
                return Err(RuntimeError::Protocol(
                    "相同 spawn_key 的委派参数冲突".into(),
                ));
            }
            super::views::commit(transaction)?;
            return Ok(record);
        }
        if !matches!(
            parent.status,
            RunStatus::Running | RunStatus::WaitingInteraction
        ) {
            return Err(RuntimeError::Protocol("父 run 状态不允许委派".into()));
        }
        if request.deadline_ms <= now_ms() {
            return Err(RuntimeError::Protocol("子 Agent deadline 已过".into()));
        }
        let ancestor: Option<(String, String, i64)> = transaction
            .query_row(
                "SELECT root_session_id, root_run_id, depth FROM delegations WHERE child_run_id=?1",
                params![request.parent_run_id.0],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let (root_session_id, root_run_id, depth) = match ancestor {
            Some((session, run, parent_depth)) => (session, run, parent_depth + 1),
            None => (parent.session_id.0.clone(), parent.run_id.0.clone(), 1),
        };
        if depth > MAX_DEPTH {
            return Err(RuntimeError::Protocol("子 Agent 深度上限为 2".into()));
        }
        let (total, active): (i64, i64) = transaction.query_row(
            "SELECT count(*), COALESCE(sum(CASE WHEN status IN ('queued','running','waiting_interaction') THEN 1 ELSE 0 END),0)
             FROM delegations WHERE root_run_id=?1",
            params![root_run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if total >= MAX_ROOT_SPAWNS || active >= MAX_ROOT_ACTIVE {
            return Err(RuntimeError::Protocol("子 Agent 总量或并发上限已满".into()));
        }
        let route_json: Option<Option<String>> = transaction
            .query_row(
                "SELECT CASE WHEN length(CAST(snapshot_json AS BLOB))<=1048576 THEN snapshot_json END FROM run_routes WHERE run_id=?1",
                params![request.parent_run_id.0],
                |row| row.get(0),
            )
            .optional()?;
        let route_json = route_json
            .map(|raw| raw.ok_or(agent_core::CoordinationError::Corrupt))
            .transpose()?;
        // 委派也消费原持久队列总预算，不能借由另一准入入口无限扩张。
        let queued: i64 =
            transaction.query_row("SELECT count(*) FROM runs WHERE status='queued'", [], |r| {
                r.get(0)
            })?;
        if queued >= 1024 {
            return Err(agent_core::CoordinationError::LeaseBusy.into());
        }
        let now = now_ms();
        let _ = super::sessions::create_in(&transaction, &request.child_session_id)?;
        let child_request_id =
            RequestId::String(format!("delegation:{}", request.child_session_id.0));
        let request_json = serde_json::to_string(&child_request_id)
            .map_err(|error| RuntimeError::Protocol(error.to_string()))?;
        transaction.execute(
            "INSERT INTO runs(id, session_id, request_id_json, status, input, created_at_ms, updated_at_ms)
             VALUES ('pending', ?1, ?2, 'queued', ?3, ?4, ?4)",
            params![request.child_session_id.0, request_json, request.task, now],
        )?;
        let run_rowid = transaction.last_insert_rowid();
        let child_run_id = RunId(format!("run-{run_rowid}"));
        let child_turn_id = TurnId(format!("turn-{run_rowid}"));
        transaction.execute(
            "UPDATE runs SET id=?1 WHERE rowid=?2",
            params![child_run_id.0, run_rowid],
        )?;
        transaction.execute(
            "INSERT INTO turns(id, run_id, status) VALUES (?1, ?2, 'queued')",
            params![child_turn_id.0, child_run_id.0],
        )?;
        if let Some(route) = &route_json {
            transaction.execute(
                "INSERT INTO run_routes(run_id, snapshot_json) VALUES (?1, ?2)",
                params![child_run_id.0, route],
            )?;
        }
        let exists: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM run_snapshots WHERE run_id=?1)",
            params![request.parent_run_id.0],
            |r| r.get(0),
        )?;
        let parent_snapshot = if exists {
            Some(super::requests::frozen_in(
                &transaction,
                &request.parent_run_id,
            )?)
        } else {
            None
        };
        if let Some(mut snapshot) = parent_snapshot {
            snapshot.entry_channel = agent_core::HookChannel::Subagent;
            // 执行指令仅属于原工作流节点，派生 run 保留冻结授权但不重复执行父节点。
            snapshot.workflow = None;
            // 仅显式选择本父 run 的来源；祖先捕获包不自动传给孙任务。
            snapshot.delegation_context = if request.context_source_ids.is_empty() {
                None
            } else {
                let owner = super::sessions::owner_in(&transaction, &request.parent_run_id)?;
                super::sessions::fence_in(&transaction, &owner)?;
                let sources = request
                    .context_source_ids
                    .iter()
                    .map(|id| super::memory::read_source_evidence(&transaction, &owner, id, 1024))
                    .collect::<Result<Vec<_>, _>>()?;
                use sha2::{Digest, Sha256};
                let digest = format!(
                    "{:x}",
                    Sha256::digest(
                        serde_json::to_vec(&(&owner, &sources))
                            .map_err(|e| RuntimeError::Protocol(e.to_string()))?
                    )
                );
                let context = agent_core::DelegationContext {
                    version: 1,
                    parent: owner,
                    sources,
                    digest,
                };
                let bytes = serde_json::to_vec(&context)
                    .map_err(|e| RuntimeError::Protocol(e.to_string()))?
                    .len();
                if bytes > 8192
                    || bytes > (request.max_tokens as usize).min(snapshot.context_token_budget)
                {
                    return Err(RuntimeError::Protocol(
                        "交接包超过8KiB或子任务材料预算".into(),
                    ));
                }
                Some(context)
            };
            if request
                .tools
                .iter()
                .any(|name| !snapshot.tools.iter().any(|t| &t.name == name))
            {
                return Err(RuntimeError::Protocol(
                    "child tool catalog 扩大父权限".into(),
                ));
            }
            snapshot.tools.retain(|t| request.tools.contains(&t.name));
            use sha2::{Digest, Sha256};
            snapshot.tool_catalog_digest = format!(
                "{:x}",
                Sha256::digest(
                    serde_json::to_vec(&snapshot.tools)
                        .map_err(|e| RuntimeError::Protocol(e.to_string()))?
                )
            );
            snapshot.context_token_budget = snapshot
                .context_token_budget
                .min(request.max_tokens as usize);
            if let Some(policy) = &snapshot.context_policy_fingerprint {
                let (budget, suffix) = policy
                    .split_once(':')
                    .ok_or_else(|| RuntimeError::Protocol("父 context policy 无效".into()))?;
                let budget = budget
                    .parse::<usize>()
                    .map_err(|_| RuntimeError::Protocol("父 context budget 无效".into()))?;
                snapshot.context_policy_fingerprint = Some(format!(
                    "{}:{suffix}",
                    budget.min(request.max_tokens as usize)
                ));
            }
            snapshot.permission_mode = request.permission_mode.clone();
            snapshot.cwd = request.cwd.clone();
            snapshot.max_tool_calls = Some(
                snapshot
                    .max_tool_calls
                    .unwrap_or(u64::MAX)
                    .min(request.max_tool_calls as u64),
            );
            transaction.execute(
                "INSERT INTO run_snapshots VALUES(?1,?2)",
                params![
                    child_run_id.0,
                    serde_json::to_string(&snapshot)
                        .map_err(|e| RuntimeError::Protocol(e.to_string()))?
                ],
            )?;
            super::enterprise::inherit_authorization_in(
                &transaction,
                &request.parent_run_id,
                &child_run_id,
                &snapshot,
            )?;
        } else if !request.context_source_ids.is_empty() {
            return Err(RuntimeError::Protocol(
                "父run缺少冻结快照，不能交接上下文".into(),
            ));
        }
        insert_event(
            &transaction,
            &child_run_id,
            "user_message",
            &json!({"content": request.task}),
        )?;
        transaction.execute(
            "INSERT INTO queued_messages(session_id, run_id, message, status) VALUES (?1, ?2, ?3, 'queued')",
            params![request.child_session_id.0, child_run_id.0, request.task],
        )?;
        let tools_json = serde_json::to_string(&request.tools)
            .map_err(|error| RuntimeError::Protocol(error.to_string()))?;
        transaction.execute(
            "INSERT INTO delegations(root_session_id, root_run_id, parent_session_id, parent_run_id,
                spawn_key, child_session_id, child_run_id, depth, status, created_at_ms, tools_json,
                permission_mode, route_json, cwd, max_rounds, max_tokens, max_tool_calls, deadline_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'queued', ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            params![root_session_id, root_run_id, request.parent_session_id.0,
                request.parent_run_id.0, request.spawn_key, request.child_session_id.0, child_run_id.0, depth,
                now, tools_json, request.permission_mode, route_json, request.cwd,
                request.max_rounds, request.max_tokens, request.max_tool_calls, request.deadline_ms],
        )?;
        insert_event(
            &transaction,
            &request.parent_run_id,
            "delegation_spawned",
            &json!({"child_session_id": request.child_session_id, "child_run_id": child_run_id,
                "depth": depth, "status": "queued"}),
        )?;
        let record = read_delegation_in(&transaction, &child_run_id.0)?
            .ok_or_else(|| RuntimeError::Internal("缺少持久记录：new delegation".into()))?;
        let child_env=super::billing::envelope_in(&transaction,&child_run_id)?;
        let child_snapshot=super::turns::run_snapshot_in(&transaction,&child_run_id)?;
        super::billing::reserve_in(&transaction,&child_env,child_snapshot.as_ref(),false)?;
        super::ledgers::admit_in(&transaction,&child_run_id)?;
        super::views::commit(transaction)?;
        Ok(record)
    }

    pub fn delegation(
        &self,
        child_run_id: &RunId,
    ) -> Result<Option<DelegationRecord>, RuntimeError> {
        self.delegation_scoped(&agent_core::TenantContext::local(), child_run_id, None)
    }
    pub fn delegation_by_spawn_key(
        &self,
        parent_run_id: &RunId,
        spawn_key: &str,
    ) -> Result<Option<DelegationRecord>, RuntimeError> {
        self.delegation_lookup_scoped(
            &agent_core::TenantContext::local(),
            None,
            Some((parent_run_id, spawn_key)),
        )
    }
    pub fn delegation_for_session(
        &self,
        session_id: &str,
    ) -> Result<Option<DelegationRecord>, RuntimeError> {
        self.delegation_lookup_scoped(
            &agent_core::TenantContext::local(),
            Some(&SessionId(session_id.to_owned())),
            None,
        )
    }
    pub fn list_delegations(
        &self,
        root_run_id: &RunId,
    ) -> Result<Vec<DelegationRecord>, RuntimeError> {
        self.delegation_list_scoped(&agent_core::TenantContext::local(), root_run_id)
    }

    pub fn reserve_delegation_result(
        &self,
        child_run_id: &RunId,
        owner: &str,
        revision: i64,
    ) -> Result<DelegationRecord, RuntimeError> {
        self.reserve_delegation_result_scoped(child_run_id, owner, revision, None)
    }

    pub(crate) fn reserve_delegation_result_scoped(
        &self,
        child_run_id: &RunId,
        owner: &str,
        revision: i64,
        envelope: Option<&agent_core::EnterpriseOwnerEnvelope>,
    ) -> Result<DelegationRecord, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        crate::run_delegations::check_result_execution_in(&transaction, child_run_id, envelope)?;
        let record = read_delegation_in(&transaction, &child_run_id.0)?
            .ok_or_else(|| RuntimeError::Protocol("子 Agent 不存在".into()))?;
        if owner.is_empty() {
            return Err(RuntimeError::Protocol("reservation owner 不能为空".into()));
        }
        if record.reservation_owner.as_deref() == Some(owner)
            && (record.result_state == "delivered"
                || record.result_state == "reserved"
                    && record
                        .reservation_expires_at_ms
                        .is_some_and(|expires| expires > now_ms()))
        {
            super::views::commit(transaction)?;
            return Ok(record);
        }
        if !record.status.terminal() || record.revision != revision {
            return Err(RuntimeError::Protocol(
                "结果尚未终态或 revision 冲突".into(),
            ));
        }
        if record.result_state == "reserved"
            && record
                .reservation_expires_at_ms
                .is_some_and(|expires| expires <= now_ms())
        {
            transaction.execute(
                "UPDATE delegations SET result_state='unconsumed',
                reservation_owner=NULL, reservation_expires_at_ms=NULL,
                reservation_released_at_ms=?2, revision=revision+1 WHERE child_run_id=?1",
                params![child_run_id.0, now_ms()],
            )?;
        } else if record.result_state != "unconsumed" {
            return Err(RuntimeError::Protocol("结果已由其他 owner 领取".into()));
        }
        transaction.execute(
            "UPDATE delegations SET result_state='reserved', reservation_owner=?2,
            reservation_expires_at_ms=?3, revision=revision+1 WHERE child_run_id=?1",
            params![child_run_id.0, owner, now_ms() + 30_000],
        )?;
        let result = read_delegation_in(&transaction, &child_run_id.0)?
            .ok_or_else(|| RuntimeError::Internal("缺少持久记录：delegation".into()))?;
        super::views::commit(transaction)?;
        Ok(result)
    }

    pub fn release_delegation_result(
        &self,
        child_run_id: &RunId,
        owner: &str,
        revision: i64,
    ) -> Result<DelegationRecord, RuntimeError> {
        self.transition_result(child_run_id, owner, revision, "unconsumed")
    }

    pub fn deliver_delegation_result(
        &self,
        child_run_id: &RunId,
        owner: &str,
        revision: i64,
    ) -> Result<DelegationRecord, RuntimeError> {
        self.transition_result(child_run_id, owner, revision, "delivered")
    }

    fn transition_result(
        &self,
        child_run_id: &RunId,
        owner: &str,
        revision: i64,
        target: &str,
    ) -> Result<DelegationRecord, RuntimeError> {
        self.transition_result_scoped(child_run_id, owner, revision, target, None)
    }

    pub(crate) fn transition_result_scoped(
        &self,
        child_run_id: &RunId,
        owner: &str,
        revision: i64,
        target: &str,
        envelope: Option<&agent_core::EnterpriseOwnerEnvelope>,
    ) -> Result<DelegationRecord, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        crate::run_delegations::check_result_execution_in(&transaction, child_run_id, envelope)?;
        let record = read_delegation_in(&transaction, &child_run_id.0)?
            .ok_or_else(|| RuntimeError::Protocol("子 Agent 不存在".into()))?;
        if target == "delivered"
            && record.result_state == "delivered"
            && record.reservation_owner.as_deref() == Some(owner)
        {
            super::views::commit(transaction)?;
            return Ok(record);
        }
        if record.result_state != "reserved"
            || record.reservation_owner.as_deref() != Some(owner)
            || record.revision != revision
            || target == "delivered"
                && record
                    .reservation_expires_at_ms
                    .is_some_and(|expires| expires <= now_ms())
        {
            return Err(RuntimeError::Protocol(
                "结果 reservation owner/revision 冲突".into(),
            ));
        }
        transaction.execute("UPDATE delegations SET result_state=?2,
            reservation_owner=CASE WHEN ?2='delivered' THEN reservation_owner ELSE NULL END,
            reservation_expires_at_ms=NULL,
            reservation_released_at_ms=CASE WHEN ?2='unconsumed' THEN ?3 ELSE reservation_released_at_ms END,
            revision=revision+1 WHERE child_run_id=?1", params![child_run_id.0, target, now_ms()])?;
        let result = read_delegation_in(&transaction, &child_run_id.0)?
            .ok_or_else(|| RuntimeError::Internal("缺少持久记录：delegation".into()))?;
        super::views::commit(transaction)?;
        Ok(result)
    }
}

pub(super) fn mark_terminal_in(
    connection: &Connection,
    child_run_id: &RunId,
    run: &RunRecord,
) -> Result<(), RuntimeError> {
    let parent: Option<String> = connection
        .query_row(
            "SELECT parent_run_id FROM delegations WHERE child_run_id=?1",
            params![child_run_id.0],
            |row| row.get(0),
        )
        .optional()?;
    let Some(parent) = parent else {
        return Ok(());
    };
    connection.execute(
        "UPDATE delegations SET status=?2, finished_at_ms=?3, terminal_json=?4
        WHERE child_run_id=?1",
        params![
            child_run_id.0,
            run.status.as_str(),
            now_ms(),
            json!({"status": run.status, "content": run.content, "error_code": run.error_code,
                "error_message": run.error_message})
            .to_string()
        ],
    )?;
    if let Some(parent_run) = read_run_in(connection, &parent)?
        && !parent_run.status.terminal()
    {
        insert_event(
            connection,
            &parent_run.run_id,
            "delegation_terminal",
            &json!({"child_run_id": child_run_id, "status": run.status,
                    "summary": run.content.as_deref().unwrap_or("").chars().take(512).collect::<String>()}),
        )?;
    }
    Ok(())
}

pub(crate) fn read_delegation_in(
    connection: &Connection,
    child_run_id: &str,
) -> Result<Option<DelegationRecord>, RuntimeError> {
    crate::run_delegations::delegation_budget_in(connection, child_run_id)?;
    let raw = connection
        .query_row(
            "SELECT d.root_session_id, d.root_run_id, d.parent_session_id, d.parent_run_id,
            d.child_session_id, d.spawn_key, d.depth, d.status, d.created_at_ms, d.started_at_ms,
            d.finished_at_ms, d.tools_json, d.permission_mode, d.route_json, d.cwd,
            d.max_rounds, d.max_tokens, d.max_tool_calls, d.deadline_ms, d.terminal_json,
            d.result_state, d.reservation_owner, d.revision, d.reservation_released_at_ms,
            d.reservation_expires_at_ms,
            r.content, r.error_code, r.error_message
         FROM delegations d JOIN runs r ON r.id=d.child_run_id WHERE d.child_run_id=?1",
            params![child_run_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, Option<i64>>(9)?,
                    row.get::<_, Option<i64>>(10)?,
                    row.get::<_, String>(11)?,
                    row.get::<_, String>(12)?,
                    row.get::<_, Option<String>>(13)?,
                    row.get::<_, String>(14)?,
                    row.get::<_, i64>(15)?,
                    row.get::<_, i64>(16)?,
                    row.get::<_, i64>(17)?,
                    row.get::<_, i64>(18)?,
                    row.get::<_, Option<String>>(19)?,
                    row.get::<_, String>(20)?,
                    row.get::<_, Option<String>>(21)?,
                    row.get::<_, i64>(22)?,
                    row.get::<_, Option<i64>>(23)?,
                    row.get::<_, Option<i64>>(24)?,
                    row.get::<_, Option<String>>(25)?,
                    row.get::<_, Option<i64>>(26)?,
                    row.get::<_, Option<String>>(27)?,
                ))
            },
        )
        .optional()?;
    let Some((
        root_session,
        root_run,
        parent_session,
        parent_run,
        child_session,
        spawn_key,
        depth,
        status,
        created,
        started,
        finished,
        tools,
        permission,
        route,
        cwd,
        rounds,
        tokens,
        tool_calls,
        deadline,
        terminal,
        result_state,
        owner,
        revision,
        released,
        expires,
        content,
        error_code,
        error_message,
    )) = raw
    else {
        return Ok(None);
    };
    Ok(Some(DelegationRecord {
        root_session_id: SessionId(root_session),
        root_run_id: RunId(root_run),
        parent_session_id: SessionId(parent_session),
        parent_run_id: RunId(parent_run),
        child_session_id: SessionId(child_session),
        child_run_id: RunId(child_run_id.to_owned()),
        spawn_key,
        depth,
        status: RunStatus::parse(&status)
            .map_err(|_| RuntimeError::Coordination(agent_core::CoordinationError::Corrupt))?,
        created_at_ms: created,
        started_at_ms: started,
        finished_at_ms: finished,
        tools: serde_json::from_str(&tools)
            .map_err(|_| RuntimeError::Coordination(agent_core::CoordinationError::Corrupt))?,
        permission_mode: permission,
        route: route
            .map(|value| {
                serde_json::from_str(&value)
                    .map_err(|_| RuntimeError::Coordination(agent_core::CoordinationError::Corrupt))
            })
            .transpose()?,
        cwd,
        max_rounds: rounds,
        max_tokens: tokens,
        max_tool_calls: tool_calls,
        deadline_ms: deadline,
        terminal: terminal
            .map(|value| {
                serde_json::from_str(&value)
                    .map_err(|_| RuntimeError::Coordination(agent_core::CoordinationError::Corrupt))
            })
            .transpose()?,
        result_state,
        reservation_owner: owner,
        revision,
        reservation_released_at_ms: released,
        reservation_expires_at_ms: expires,
        content,
        error_code,
        error_message,
    }))
}

#[cfg(test)]
mod context_tests {
    use super::*;
    use crate::{SessionLifecycle, SessionQuery, TranscriptStore, TurnRepository};
    use agent_core::*;
    fn parent(store: &RunStore, key: &str) -> ExactOwner {
        let key = SessionKey(key.into());
        store.create_session(&key).unwrap();
        let snapshot = RunSnapshot {
            workflow: None,
            installed_generation:None,
            billing_policy: None,
            artifact_store_id: None,
            entry_channel: agent_core::HookChannel::DaemonRpc,
            route: None,
            tools: vec![ToolSpec {
                name: "read_file".into(),
                description: "读取".into(),
                parameters: json!({"type":"object"}),
            }],
            cwd: "/workspace".into(),
            permission_mode: "request_approval".into(),
            sandbox_requested: "native".into(),
            sandbox_effective: "native".into(),
            sandbox_notice: None,
            docker_image: None,
            delegation_context: None,
            context_read_only: false,
            context_token_budget: 8192,
            context_policy_fingerprint: None,
            tool_catalog_digest: "test".into(),
            memory_entry_budget: 8,
            memory_token_budget: 1024,
            max_tool_calls: Some(16),
            config_generation: 0,
        };
        let Admission::New(run) = store
            .admit_run(
                &RunAdmission {
                    workflow_node: None,
                    plan_execution: None,
                    session_key: key,
                    expected_lifetime: None,
                    request_id: RequestId::Number(1),
                    input: "禁止上传".into(),
                    mode: AdmissionMode::Queue,
                },
                &snapshot,
            )
            .unwrap()
        else {
            panic!("new")
        };
        store.try_start_queued(&run.run_id).unwrap();
        store.run_owner(&run.run_id).unwrap()
    }
    fn request(owner: &ExactOwner, child: &str, ids: Vec<String>) -> DelegationRequest {
        DelegationRequest {
            parent_session_id: owner.session_key.clone(),
            parent_run_id: owner.run_id.clone(),
            child_session_id: SessionId(child.into()),
            spawn_key: child.into(),
            task: "核实构建约束".into(),
            context_source_ids: ids,
            tools: vec!["read_file".into()],
            permission_mode: "request_approval".into(),
            cwd: "/workspace".into(),
            max_rounds: 8,
            max_tokens: 8192,
            max_tool_calls: 16,
            deadline_ms: now_ms() + 60_000,
        }
    }
    #[test]
    fn delegation_freezes_explicit_evidence_and_does_not_implicitly_pass_it_on() {
        let store = RunStore::open(std::path::Path::new(":memory:")).unwrap();
        let owner = parent(&store, "parent-context");
        let mut message = Message::assistant_with_thinking(
            "构建前先核实\nAPI_KEY=sk-secret\n".to_owned() + &"😀".repeat(2000),
            Some("私有思考".into()),
        );
        message.image_urls = vec!["私有图片".into()];
        store
            .append_transcript(&owner, "material", &[message])
            .unwrap();
        let ids = vec![
            format!("{}:0", owner.session_lifetime_id.0),
            format!("{}:1", owner.session_lifetime_id.0),
        ];
        let command = request(&owner, "captured", ids);
        let child = store.admit_delegation(command.clone()).unwrap();
        let captured = store
            .run_snapshot(&child.child_run_id)
            .unwrap()
            .unwrap()
            .delegation_context
            .unwrap();
        assert_eq!(captured.parent, owner);
        assert_eq!(captured.sources[0].text, "禁止上传");
        assert!(captured.sources[1].truncated);
        assert_eq!(captured.sources[1].text.chars().count(), 1024);
        let raw = serde_json::to_string(&captured).unwrap();
        for secret in ["sk-secret", "私有思考", "私有图片"] {
            assert!(!raw.contains(secret));
        }
        store
            .append_transcript(&owner, "later", &[Message::text(Role::User, "后续新材料")])
            .unwrap();
        assert_eq!(
            store
                .admit_delegation(command.clone())
                .unwrap()
                .child_run_id,
            child.child_run_id
        );
        let mut changed = command;
        changed.context_source_ids.pop();
        assert!(store.admit_delegation(changed).is_err());
        assert!(store.try_start_queued(&child.child_run_id).unwrap());
        let child_owner = store.run_owner(&child.child_run_id).unwrap();
        let grandchild = store
            .admit_delegation(request(&child_owner, "grandchild", vec![]))
            .unwrap();
        assert!(
            store
                .run_snapshot(&grandchild.child_run_id)
                .unwrap()
                .unwrap()
                .delegation_context
                .is_none()
        );
    }
    #[test]
    fn delegation_rejects_foreign_owner_bad_digest_duplicates_and_budget_overflow_atomically() {
        let store = RunStore::open(std::path::Path::new(":memory:")).unwrap();
        let owner = parent(&store, "own");
        let foreign = parent(&store, "foreign");
        let id = format!("{}:0", owner.session_lifetime_id.0);
        for ids in [
            vec![format!("{}:0", foreign.session_lifetime_id.0)],
            vec![id.clone(), id.clone()],
            vec![id.clone(), format!("{}:0000", owner.session_lifetime_id.0)],
            vec![format!("{}:999", owner.session_lifetime_id.0)],
            vec!["bad".into()],
            vec![id.clone(); 5],
        ] {
            assert!(
                store
                    .admit_delegation(request(&owner, "rejected", ids))
                    .is_err()
            );
        }
        store
            .append_transcript(
                &owner,
                "long",
                &[Message::text(Role::Assistant, "😀".repeat(2000))],
            )
            .unwrap();
        let mut small = request(
            &owner,
            "budget",
            vec![format!("{}:1", owner.session_lifetime_id.0)],
        );
        small.max_tokens = 512;
        assert!(store.admit_delegation(small).is_err());
        store
            .lock_connection()
            .unwrap()
            .execute(
                "UPDATE transcript_batches SET digest='bad' WHERE lifetime=?1",
                params![owner.session_lifetime_id.0],
            )
            .unwrap();
        assert!(
            store
                .admit_delegation(request(&owner, "digest", vec![id]))
                .is_err()
        );
        assert!(store.list_delegations(&owner.run_id).unwrap().is_empty());
        let leaked: i64 = store
            .lock_connection()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE id IN ('rejected','budget','digest')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(leaked, 0);
    }
}
