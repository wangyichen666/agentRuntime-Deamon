use super::{
    RunStore, RuntimeError, insert_event,
    sessions::{fence_in, insert_batch, owner_in},
};
use agent_core::*;
use rusqlite::{Connection, OptionalExtension, params};

pub trait TurnRepository: Send + Sync {
    fn plan_readback(&self, key: &SessionKey) -> Result<PlanReadback, RuntimeError>;
    fn discard_plan(
        &self,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        identity: &PlanExecution,
    ) -> Result<PlanDecisionReceipt, RuntimeError>;
    fn start_plan_execution(&self, owner: &ExactOwner) -> Result<(), RuntimeError>;
    fn read_plan(&self, owner: &ExactOwner) -> Result<PlanSnapshot, RuntimeError>;
    fn stage_plan(
        &self,
        owner: &ExactOwner,
        expected: u64,
        value: &serde_json::Value,
    ) -> Result<PlanSnapshot, RuntimeError>;
    fn reject_tool_batch(
        &self,
        owner: &ExactOwner,
        round: usize,
        calls: &[ToolCall],
        reason: &str,
    ) -> Result<Vec<Message>, RuntimeError>;
    fn stage_assistant(&self, owner: &ExactOwner, message: &Message) -> Result<(), RuntimeError>;
    fn commit_turn(&self, command: &TurnCommit) -> Result<RunRecord, RuntimeError>;
    fn close_tool_exchange(
        &self,
        owner: &ExactOwner,
        round: usize,
        messages: &[Message],
    ) -> Result<(), RuntimeError>;
    fn run_snapshot(&self, run: &RunId) -> Result<Option<RunSnapshot>, RuntimeError>;
}
impl TurnRepository for RunStore {
    fn plan_readback(&self, key: &SessionKey) -> Result<PlanReadback, RuntimeError> {
        self.read_plan_document(key)
    }
    fn discard_plan(
        &self,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        identity: &PlanExecution,
    ) -> Result<PlanDecisionReceipt, RuntimeError> {
        self.discard_plan_document(key, lifetime, identity)
    }
    fn start_plan_execution(&self, owner: &ExactOwner) -> Result<(), RuntimeError> {
        self.begin_plan_execution(owner)
    }
    fn read_plan(&self, owner: &ExactOwner) -> Result<PlanSnapshot, RuntimeError> {
        let db = self.lock_connection()?;
        fence_in(&db, owner)?;
        read_plan_in(&db, owner)
    }
    fn stage_plan(
        &self,
        owner: &ExactOwner,
        expected: u64,
        value: &serde_json::Value,
    ) -> Result<PlanSnapshot, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        execution_fence_in(&tx, owner)?;
        let current = read_plan_in(&tx, owner)?;
        if current.revision != expected {
            return Err(RuntimeError::Protocol("plan revision CAS 冲突".into()));
        }
        let document = super::plans::stage_document(owner, &current, value)?;
        let next = document.revision;
        let value =
            serde_json::to_value(&document).map_err(|e| RuntimeError::Protocol(e.to_string()))?;
        let base: u64 = tx
            .query_row(
                "SELECT revision FROM session_plans WHERE lifetime=?1",
                params![owner.session_lifetime_id.0],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0);
        tx.execute("INSERT INTO turn_plan_stages VALUES (?1,?2,?3,?4) ON CONFLICT(run_id) DO UPDATE SET revision=excluded.revision,data_json=excluded.data_json",params![owner.run_id.0,base,next,value.to_string()])?;
        super::views::commit(tx)?;
        Ok(PlanSnapshot {
            revision: next,
            value,
        })
    }
    fn reject_tool_batch(
        &self,
        owner: &ExactOwner,
        round: usize,
        calls: &[ToolCall],
        reason: &str,
    ) -> Result<Vec<Message>, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        execution_fence_in(&tx, owner)?;
        let calls_json =
            serde_json::to_string(calls).map_err(|e| RuntimeError::Protocol(e.to_string()))?;
        tx.execute(
            "INSERT INTO tool_batches(run_id,round,calls_json,state) VALUES(?1,?2,?3,'closed')",
            params![owner.run_id.0, round as i64, calls_json],
        )?;
        let mut messages = vec![Message::assistant_tool_calls(calls.to_vec())];
        for call in calls {
            let output =
                serde_json::json!({"outcome":"not_executed","reason":reason,"batch_rejected":true})
                    .to_string();
            tx.execute("INSERT INTO tool_executions(run_id,round,call_id,name,status,effect,argument_digest,prepared_at_ms,finished_at_ms,outcome,receipt_json) VALUES(?1,?2,?3,?4,'terminal','not_executed','',?5,?5,'not_executed',?6)",params![owner.run_id.0,round as i64,call.id,call.name,super::now_ms(),serde_json::json!({"output":output,"success":false,"batch_rejected":true}).to_string()])?;
            messages.push(Message::tool_result(call, output));
        }
        validate_exchange(&messages)?;
        insert_batch(
            &tx,
            &owner.session_key,
            &owner.session_lifetime_id,
            &format!("tool-batch:{round}"),
            Some(&owner.run_id),
            &messages,
        )?;
        insert_event(
            &tx,
            &owner.run_id,
            "tool_batch_rejected",
            &serde_json::json!({"round":round,"reason":reason}),
        )?;
        super::views::commit(tx)?;
        Ok(messages)
    }
    fn stage_assistant(&self, owner: &ExactOwner, message: &Message) -> Result<(), RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        execution_fence_in(&tx, owner)?;
        if message.role != Role::Assistant || !message.tool_calls.is_empty() {
            return Err(RuntimeError::Protocol("终态输出必须为助手文本".into()));
        }
        let payload =
            serde_json::to_string(message).map_err(|e| RuntimeError::Protocol(e.to_string()))?;
        tx.execute("INSERT INTO turn_outputs VALUES(?1,?2) ON CONFLICT(run_id) DO UPDATE SET message_json=excluded.message_json",params![owner.run_id.0,payload])?;
        super::views::commit(tx)?;
        Ok(())
    }
    fn commit_turn(&self, command: &TurnCommit) -> Result<RunRecord, RuntimeError> {
        self.finish_owned(
            &command.owner,
            command.status,
            command.content.as_deref(),
            command
                .error
                .as_ref()
                .map(|(code, msg)| (*code, msg.as_str())),
        )
    }
    fn close_tool_exchange(
        &self,
        owner: &ExactOwner,
        round: usize,
        messages: &[Message],
    ) -> Result<(), RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        execution_fence_in(&tx, owner)?;
        validate_exchange(messages)?;
        let calls_json: String = tx.query_row(
            "SELECT calls_json FROM tool_batches WHERE run_id=?1 AND round=?2",
            params![owner.run_id.0, round as i64],
            |r| r.get(0),
        )?;
        let calls: Vec<ToolCall> =
            serde_json::from_str(&calls_json).map_err(|e| RuntimeError::Protocol(e.to_string()))?;
        if messages[0].tool_calls != calls {
            return Err(RuntimeError::Protocol(
                "closed exchange 与已准入 batch 不一致".into(),
            ));
        }
        insert_batch(
            &tx,
            &owner.session_key,
            &owner.session_lifetime_id,
            &format!("tool-batch:{round}"),
            Some(&owner.run_id),
            messages,
        )?;
        let changed = tx.execute(
            "UPDATE tool_batches SET state='closed' WHERE run_id=?1 AND round=?2 AND state!='closed'",
            params![owner.run_id.0, round as i64],
        )?;
        if changed == 0 {
            super::views::commit(tx)?;
            return Ok(());
        }
        insert_event(
            &tx,
            &owner.run_id,
            "tool_exchange_closed",
            &serde_json::json!({"round":round}),
        )?;
        super::views::commit(tx)?;
        Ok(())
    }
    fn run_snapshot(&self, run: &RunId) -> Result<Option<RunSnapshot>, RuntimeError> {
        self.lock_connection()?
            .query_row(
                "SELECT snapshot_json FROM run_snapshots WHERE run_id=?1",
                params![run.0],
                |r| r.get::<_, String>(0),
            )
            .optional()?
            .map(|raw| {
                serde_json::from_str(&raw).map_err(|e| RuntimeError::Protocol(e.to_string()))
            })
            .transpose()
    }
}
fn validate_exchange(messages: &[Message]) -> Result<(), RuntimeError> {
    let first = messages
        .first()
        .ok_or_else(|| RuntimeError::Protocol("空 tool exchange".into()))?;
    if first.role != Role::Assistant
        || first.tool_calls.is_empty()
        || messages.len() != first.tool_calls.len() + 1
    {
        return Err(RuntimeError::Protocol("tool pair 不完整".into()));
    }
    for (call, result) in first.tool_calls.iter().zip(&messages[1..]) {
        if result.role != Role::Tool || result.tool_call_id.as_deref() != Some(&call.id) {
            return Err(RuntimeError::Protocol("tool pair id 不匹配".into()));
        }
    }
    Ok(())
}
pub(crate) fn close_open_batches(db: &Connection, run: &RunId) -> Result<(), RuntimeError> {
    let owner = owner_in(db, run)?;
    fence_in(db, &owner)?;
    let mut q=db.prepare("SELECT round,calls_json FROM tool_batches WHERE run_id=?1 AND state!='closed' ORDER BY round")?;
    let batches = q
        .query_map(params![run.0], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(q);
    for (round, raw) in batches {
        let calls: Vec<ToolCall> =
            serde_json::from_str(&raw).map_err(|e| RuntimeError::Protocol(e.to_string()))?;
        let mut messages = vec![Message::assistant_tool_calls(calls.clone())];
        for call in calls {
            let (status,receipt):(String,Option<String>)=db.query_row("SELECT status,receipt_json FROM tool_executions WHERE run_id=?1 AND round=?2 AND call_id=?3",params![run.0,round,call.id],|r|Ok((r.get(0)?,r.get(1)?)))?;
            let output = if status == "terminal" {
                let receipt: serde_json::Value = receipt
                    .map(|r| serde_json::from_str(&r))
                    .transpose()
                    .map_err(|e| RuntimeError::Protocol(e.to_string()))?
                    .unwrap_or_default();
                receipt
                    .get("output")
                    .and_then(|v| v.as_str())
                    .unwrap_or("[outcome_unknown] 结果回执不含可恢复输出")
                    .to_owned()
            } else if status == "prepared" {
                "[not_executed] 工具未启动".into()
            } else {
                "[outcome_unknown] 副作用结果未知，禁止自动重放".into()
            };
            messages.push(Message::tool_result(&call, output));
            db.execute("UPDATE tool_executions SET status='terminal',outcome=CASE WHEN status='prepared' THEN 'not_executed' ELSE 'outcome_unknown' END WHERE run_id=?1 AND round=?2 AND call_id=?3 AND status!='terminal'",params![run.0,round,call.id])?;
        }
        insert_batch(
            db,
            &owner.session_key,
            &owner.session_lifetime_id,
            &format!("tool-batch:{round}"),
            Some(run),
            &messages,
        )?;
        db.execute(
            "UPDATE tool_batches SET state='closed' WHERE run_id=?1 AND round=?2",
            params![run.0, round],
        )?;
    }
    Ok(())
}
pub(crate) fn commit_output_in(
    db: &Connection,
    run: &RunId,
    status: RunStatus,
    content: Option<&str>,
) -> Result<(), RuntimeError> {
    let owner = owner_in(db, run)?;
    fence_in(db, &owner)?;
    if status == RunStatus::Completed
        && let Some(content) = content
    {
        let staged = db
            .query_row(
                "SELECT message_json FROM turn_outputs WHERE run_id=?1",
                params![run.0],
                |r| r.get::<_, String>(0),
            )
            .optional()?;
        let message = if let Some(raw) = staged {
            serde_json::from_str::<Message>(&raw)
                .map_err(|e| RuntimeError::Protocol(e.to_string()))?
        } else {
            Message::text(Role::Assistant, content)
        };
        if message.content.as_deref() != Some(content) {
            return Err(RuntimeError::Protocol(
                "assistant stage 与 terminal 内容冲突".into(),
            ));
        }
        let last:Option<String>=db.query_row("SELECT messages_json FROM transcript_batches WHERE run_id=?1 ORDER BY rowid DESC LIMIT 1",params![run.0],|r|r.get(0)).optional()?;
        let already_present = last
            .map(|raw| serde_json::from_str::<Vec<Message>>(&raw))
            .transpose()
            .map_err(|e| RuntimeError::Protocol(e.to_string()))?
            .is_some_and(|batch| batch.last() == Some(&message));
        if !already_present {
            insert_batch(
                db,
                &owner.session_key,
                &owner.session_lifetime_id,
                "assistant-terminal",
                Some(run),
                &[message],
            )?;
        }
    }
    let mut q = db.prepare("SELECT data_json FROM provider_attempts WHERE run_id=?1")?;
    let attempts = q
        .query_map(params![run.0], |r| r.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    drop(q);
    let mut usage = ProviderUsage::default();
    for raw in attempts {
        let attempt: ProviderAttempt =
            serde_json::from_str(&raw).map_err(|e| RuntimeError::Protocol(e.to_string()))?;
        if let Some(value) = attempt.usage {
            usage.add_attempt(&value);
        }
    }
    let stage: Option<(u64, u64, String)> = db
        .query_row(
            "SELECT base_revision,revision,data_json FROM turn_plan_stages WHERE run_id=?1",
            params![run.0],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let plan_revision = if status == RunStatus::Completed
        && let Some((base, revision, payload)) = stage
    {
        let current: u64 = db
            .query_row(
                "SELECT revision FROM session_plans WHERE lifetime=?1",
                params![owner.session_lifetime_id.0],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0);
        if current != base {
            return Err(RuntimeError::Protocol(
                "TurnCommit plan source 已变化".into(),
            ));
        }
        super::plans::publish_document(db, &owner.session_lifetime_id, revision, &payload)?;
        db.execute("INSERT INTO session_plans VALUES(?1,?2,?3) ON CONFLICT(lifetime) DO UPDATE SET revision=excluded.revision,data_json=excluded.data_json",params![owner.session_lifetime_id.0,revision,payload])?;
        Some(revision)
    } else {
        None
    };
    super::plans::settle_execution_in(db, &owner, status)?;
    db.execute(
        "INSERT INTO turn_commits(run_id,owner_json,usage_json,status,plan_revision) VALUES(?1,?2,?3,?4,?5)",
        params![
            run.0,
            serde_json::to_string(&owner).map_err(|e| RuntimeError::Protocol(e.to_string()))?,
            serde_json::to_string(&usage).map_err(|e| RuntimeError::Protocol(e.to_string()))?,
            status.as_str(),plan_revision
        ],
    )?;
    Ok(())
}

fn read_plan_in(db: &Connection, owner: &ExactOwner) -> Result<PlanSnapshot, RuntimeError> {
    let raw: Option<(u64, String)> = db
        .query_row(
            "SELECT revision,data_json FROM turn_plan_stages WHERE run_id=?1",
            params![owner.run_id.0],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?
        .or(db
            .query_row(
                "SELECT revision,data_json FROM session_plans WHERE lifetime=?1",
                params![owner.session_lifetime_id.0],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?);
    match raw {
        Some((revision, payload)) => {
            let value: serde_json::Value = serde_json::from_str(&payload)
                .map_err(|e| RuntimeError::Protocol(e.to_string()))?;
            if value.get("plan_id").is_some() {
                let doc: PlanDocument = serde_json::from_value(value.clone())
                    .map_err(|e| RuntimeError::Protocol(e.to_string()))?;
                super::plans::validate_document(&doc)?;
                if doc.revision != revision {
                    return Err(RuntimeError::Protocol("plan revision 损坏".into()));
                }
            } else {
                super::plans::validate_legacy_plan(&value)?;
            }
            Ok(PlanSnapshot { revision, value })
        }
        None => Ok(PlanSnapshot {
            revision: 0,
            value: serde_json::json!({"steps":[]}),
        }),
    }
}

fn execution_fence_in(db: &Connection, owner: &ExactOwner) -> Result<(), RuntimeError> {
    fence_in(db, owner)?;
    let status: String = db.query_row(
        "SELECT status FROM runs WHERE id=?1",
        params![owner.run_id.0],
        |r| r.get(0),
    )?;
    if !matches!(status.as_str(), "running" | "waiting_interaction") {
        return Err(RuntimeError::Protocol("execution permit 已结算".into()));
    }
    Ok(())
}

impl RunStore {
    pub fn legacy_plan_imported(&self, source: &str) -> Result<bool, RuntimeError> {
        Ok(self.lock_connection()?.query_row(
            "SELECT count(*) FROM legacy_plan_imports WHERE source=?1",
            params![source],
            |r| r.get::<_, i64>(0),
        )? != 0)
    }
    pub fn import_legacy_plan(
        &self,
        source: &str,
        digest: &str,
        key: &SessionKey,
        value: &serde_json::Value,
    ) -> Result<(), RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        let exists: i64 = tx.query_row(
            "SELECT count(*) FROM legacy_plan_imports WHERE source=?1",
            params![source],
            |r| r.get(0),
        )?;
        if exists == 0 {
            let meta = super::sessions::metadata_in(&tx, key)?
                .ok_or_else(|| RuntimeError::Protocol("旧 plan 的来源 session 无效".into()))?;
            if !meta.deleted {
                tx.execute(
                    "INSERT OR IGNORE INTO session_plans VALUES(?1,0,?2)",
                    params![meta.lifetime.0, value.to_string()],
                )?;
                tx.execute("INSERT OR IGNORE INTO plan_legacy_evidence SELECT lifetime,revision,data_json FROM session_plans WHERE lifetime=?1",params![meta.lifetime.0])?;
            }
            tx.execute(
                "INSERT INTO legacy_plan_imports VALUES(?1,?2,?3)",
                params![source, digest, meta.lifetime.0],
            )?;
        }
        super::views::commit(tx)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::{SessionLifecycle, SessionQuery};
    use super::*;
    fn admitted() -> (RunStore, ExactOwner) {
        let store = RunStore::open(std::path::Path::new(":memory:")).unwrap();
        let key = SessionKey("turn-atomic".into());
        store.create_session(&key).unwrap();
        let Admission::New(run) = store
            .admit_with_route(
                key,
                RequestId::Number(1),
                "input",
                AdmissionMode::Queue,
                None,
            )
            .unwrap()
        else {
            panic!("new run")
        };
        store.try_start_queued(&run.run_id).unwrap();
        let owner = store.run_owner(&run.run_id).unwrap();
        (store, owner)
    }
    #[test]
    fn plan_definition_revision_ignores_progress_and_publishes_stable_digest() {
        let (store, owner) = admitted();
        let first = store
            .stage_plan(
                &owner,
                0,
                &serde_json::json!({"steps":[{"id":"a","description":"验证","status":"pending"}]}),
            )
            .unwrap();
        assert!(
            first
                .value
                .get("content_digest")
                .and_then(serde_json::Value::as_str)
                .is_some()
        );
        let progress = store
            .stage_plan(
                &owner,
                first.revision,
                &serde_json::json!({"steps":[{"id":"a","description":"验证","status":"done"}]}),
            )
            .unwrap();
        assert_eq!(first.revision, progress.revision);
        assert_eq!(first.value["plan_id"], progress.value["plan_id"]);
        assert_eq!(
            first.value["content_digest"],
            progress.value["content_digest"]
        );
        let changed = store
            .stage_plan(
                &owner,
                progress.revision,
                &serde_json::json!({"steps":[{"id":"a","description":"完整验证","status":"done"}]}),
            )
            .unwrap();
        assert_eq!(changed.revision, first.revision + 1);
        assert_ne!(
            changed.value["content_digest"],
            first.value["content_digest"]
        );
    }

    #[test]
    fn terminal_fault_rolls_back_assistant_plan_usage_and_transcript_revision() {
        let (store, owner) = admitted();
        store
            .stage_assistant(&owner, &Message::text(Role::Assistant, "answer"))
            .unwrap();
        store
            .stage_plan(
                &owner,
                0,
                &serde_json::json!({"steps":[{"id":"1","description":"完成","status":"done"}]}),
            )
            .unwrap();
        assert!(
            store
                .stage_plan(&owner, 0, &serde_json::json!({"steps":[]}))
                .is_err()
        );
        let before = store.session_snapshot(&owner.session_key).unwrap();
        store.lock_connection().unwrap().execute_batch("CREATE TRIGGER fail_terminal BEFORE INSERT ON events WHEN NEW.event='terminal' BEGIN SELECT RAISE(ABORT,'fault'); END;").unwrap();
        let command = TurnCommit {
            owner: owner.clone(),
            status: RunStatus::Completed,
            content: Some("answer".into()),
            error: None,
        };
        assert!(store.commit_turn(&command).is_err());
        let after = store.session_snapshot(&owner.session_key).unwrap();
        assert_eq!(before.messages, after.messages);
        assert_eq!(before.revision, after.revision);
        let db = store.lock_connection().unwrap();
        let count: i64 = db
            .query_row("SELECT count(*) FROM turn_commits", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
        let count: i64 = db
            .query_row("SELECT count(*) FROM session_plans", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
        db.execute_batch("DROP TRIGGER fail_terminal").unwrap();
        drop(db);
        let committed = store.commit_turn(&command).unwrap();
        assert_eq!(committed.status, RunStatus::Completed);
        assert_eq!(
            store
                .session_snapshot(&owner.session_key)
                .unwrap()
                .messages
                .len(),
            2
        );
        assert_eq!(
            store.commit_turn(&command).unwrap().last_seq,
            committed.last_seq
        );
        assert!(
            store
                .stage_assistant(&owner, &Message::text(Role::Assistant, "late"))
                .is_err()
        );
        assert!(store.stage_plan(&owner, 1, &serde_json::json!({})).is_err());
    }
    #[test]
    fn interrupted_tool_batch_is_closed_atomically_with_typed_unknown_and_not_executed() {
        let (store, owner) = admitted();
        let calls = (0..2)
            .map(|n| {
                (
                    ToolCall {
                        id: format!("call-{n}"),
                        name: "write_file".into(),
                        arguments: serde_json::json!({"path":"file"}),
                    },
                    "external_side_effect".into(),
                    "digest".into(),
                    false,
                )
            })
            .collect::<Vec<_>>();
        store.prepare_tool_batch(&owner.run_id, 1, &calls).unwrap();
        store.start_tool(&owner.run_id, 1, "call-0").unwrap();
        assert_eq!(
            store
                .session_snapshot(&owner.session_key)
                .unwrap()
                .messages
                .len(),
            1
        );
        store.recover().unwrap();
        let snapshot = store.session_snapshot(&owner.session_key).unwrap();
        assert_eq!(snapshot.messages.len(), 4);
        assert_eq!(snapshot.messages[1].tool_calls.len(), 2);
        assert_eq!(snapshot.messages[2].tool_call_id.as_deref(), Some("call-0"));
        assert!(
            snapshot.messages[2]
                .content
                .as_ref()
                .unwrap()
                .contains("outcome_unknown")
        );
        assert!(
            snapshot.messages[3]
                .content
                .as_ref()
                .unwrap()
                .contains("not_executed")
        );
        let db = store.lock_connection().unwrap();
        let count: i64 = db
            .query_row(
                "SELECT count(*) FROM transcript_batches WHERE operation_id LIKE '%tool-batch%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }
}
