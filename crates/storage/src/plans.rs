//! 计划 canonical 文档/历史/决策与 run 准入共用 SQLite lane。
use crate::{RunStore, RuntimeError, sessions};
use agent_core::*;
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};

fn error(e: impl std::fmt::Display) -> RuntimeError {
    RuntimeError::Protocol(e.to_string())
}

fn definition_digest(definition: &PlanDefinition) -> Result<String, RuntimeError> {
    Ok(format!(
        "{:x}",
        Sha256::digest(definition.canonical_bytes().map_err(error)?)
    ))
}

pub(crate) fn validate_document(document: &PlanDocument) -> Result<(), RuntimeError> {
    document.validate().map_err(error)?;
    if definition_digest(&document.definition)? != document.content_digest {
        return Err(error("计划定义摘要损坏"));
    }
    let markdown = document.markdown();
    let artifact = document
        .artifact
        .as_ref()
        .ok_or_else(|| error("计划 artifact 缺失"))?;
    if artifact.byte_length != markdown.len()
        || artifact.content_digest != format!("{:x}", Sha256::digest(markdown.as_bytes()))
        || artifact.media_type != "text/markdown; charset=utf-8"
    {
        return Err(error("计划 artifact 元数据损坏"));
    }
    Ok(())
}

pub(crate) fn validate_legacy_plan(value: &serde_json::Value) -> Result<(), RuntimeError> {
    let definition: PlanDefinition = serde_json::from_value(value.clone()).map_err(error)?;
    definition.canonical_bytes().map_err(error)?;
    Ok(())
}

pub(crate) fn stage_document(
    owner: &ExactOwner,
    current: &PlanSnapshot,
    value: &serde_json::Value,
) -> Result<PlanDocument, RuntimeError> {
    let definition: PlanDefinition = serde_json::from_value(value.clone()).map_err(error)?;
    let digest = definition_digest(&definition)?;
    let previous = if current.value.get("plan_id").is_some() {
        let doc: PlanDocument = serde_json::from_value(current.value.clone()).map_err(error)?;
        validate_document(&doc)?;
        Some(doc)
    } else {
        validate_legacy_plan(&current.value)?;
        None
    };
    let changed = previous.as_ref().is_none_or(|p| p.content_digest != digest);
    let revision = if changed {
        current
            .revision
            .checked_add(1)
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or_else(|| error("plan revision 耗尽"))?
    } else {
        current.revision
    };
    let plan_id = previous.as_ref().map_or_else(
        || format!("plan:{}:{}", owner.session_lifetime_id.0, owner.run_id.0),
        |p| p.plan_id.clone(),
    );
    let review = if changed {
        PlanReviewStatus::PendingReview
    } else {
        previous
            .as_ref()
            .map_or(PlanReviewStatus::PendingReview, |p| p.review)
    };
    let mut document = PlanDocument {
        plan_id,
        revision,
        content_digest: digest,
        definition,
        review,
        artifact: None,
    };
    let markdown = document.markdown();
    document.artifact = Some(PlanArtifact {
        content_digest: format!("{:x}", Sha256::digest(markdown.as_bytes())),
        byte_length: markdown.len(),
        media_type: "text/markdown; charset=utf-8".into(),
    });
    Ok(document)
}

pub(crate) fn publish_document(
    db: &Connection,
    lifetime: &SessionLifetimeId,
    revision: u64,
    raw: &str,
) -> Result<(), RuntimeError> {
    db.execute("INSERT OR IGNORE INTO plan_legacy_evidence SELECT lifetime,revision,data_json FROM session_plans WHERE lifetime=?1 AND CASE WHEN json_valid(data_json) THEN json_extract(data_json,'$.plan_id') IS NULL ELSE 1 END",params![lifetime.0])?;
    let document: PlanDocument = serde_json::from_str(raw).map_err(error)?;
    validate_document(&document)?;
    if revision != document.revision {
        return Err(error("plan row/document revision 不匹配"));
    }
    let markdown = document.markdown();
    let artifact = document
        .artifact
        .as_ref()
        .ok_or_else(|| error("计划 Markdown artifact 缺失"))?;
    if artifact.byte_length != markdown.len()
        || artifact.content_digest != format!("{:x}", Sha256::digest(markdown.as_bytes()))
    {
        return Err(error("计划 artifact 摘要不匹配"));
    }
    let old: Option<(String, String)> = db
        .query_row(
            "SELECT plan_id,content_digest FROM plan_versions WHERE lifetime=?1 AND revision=?2",
            params![lifetime.0, revision],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if old.is_some_and(|old| old != (document.plan_id.clone(), document.content_digest.clone())) {
        return Err(error("计划历史版本冲突"));
    }
    db.execute("INSERT INTO plan_versions VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(lifetime,revision) DO UPDATE SET document_json=excluded.document_json,markdown=excluded.markdown",params![lifetime.0,revision,document.plan_id,document.content_digest,raw,markdown])?;
    Ok(())
}

fn document_in(
    db: &Connection,
    lifetime: &SessionLifetimeId,
) -> Result<PlanDocument, RuntimeError> {
    let (revision, raw): (u64, String) = db
        .query_row(
            "SELECT revision,data_json FROM session_plans WHERE lifetime=?1",
            params![lifetime.0],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?
        .ok_or_else(|| error("计划不存在"))?;
    let doc: PlanDocument =
        serde_json::from_str(&raw).map_err(|_| error("旧计划仅可读取，没有执行身份"))?;
    validate_document(&doc)?;
    if doc.revision != revision {
        return Err(error("计划 revision 损坏"));
    }
    Ok(doc)
}

fn identity_matches(doc: &PlanDocument, identity: &PlanExecution) -> Result<(), RuntimeError> {
    if identity.operation_id.is_empty()
        || identity.operation_id.len() > 128
        || identity.operation_id.chars().any(char::is_control)
    {
        return Err(error("计划 operation_id 无效"));
    }
    if doc.plan_id != identity.plan_id
        || doc.revision != identity.revision
        || doc.content_digest != identity.content_digest
    {
        return Err(error("stale plan identity/digest"));
    }
    Ok(())
}

pub(crate) fn receipt_in(
    db: &Connection,
    key: &SessionKey,
    lifetime: &SessionLifetimeId,
    identity: &PlanExecution,
    decision: PlanDecision,
    input: &str,
) -> Result<Option<PlanDecisionReceipt>, RuntimeError> {
    let row: Option<(String, String)> = db
        .query_row(
            "SELECT receipt_json,input FROM plan_decisions WHERE operation_id=?1",
            params![identity.operation_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if let Some((raw, stored_input)) = row {
        let receipt: PlanDecisionReceipt = serde_json::from_str(&raw).map_err(error)?;
        if receipt.session_key != *key
            || receipt.lifetime != *lifetime
            || receipt.identity != *identity
            || receipt.decision != decision
            || stored_input != input
        {
            return Err(error("plan operation_id 身份或输入冲突"));
        }
        return Ok(Some(receipt));
    }
    Ok(None)
}

pub(crate) fn admit_execution(
    db: &Connection,
    key: &SessionKey,
    lifetime: &SessionLifetimeId,
    identity: &PlanExecution,
    input: &str,
    run: &RunId,
) -> Result<(), RuntimeError> {
    let mut document = document_in(db, lifetime)?;
    identity_matches(&document, identity)?;
    if !matches!(
        document.review,
        PlanReviewStatus::PendingReview | PlanReviewStatus::PendingExecution
    ) {
        return Err(error("计划当前不可执行"));
    }
    document.review = PlanReviewStatus::PendingExecution;
    write_document(db, lifetime, &document)?;
    let receipt = PlanDecisionReceipt {
        session_key: key.clone(),
        lifetime: lifetime.clone(),
        identity: identity.clone(),
        decision: PlanDecision::Execute,
        run_id: Some(run.clone()),
    };
    db.execute(
        "INSERT INTO plan_decisions VALUES(?1,?2,?3,?4,'execute',?5,?6,?7)",
        params![
            identity.operation_id,
            key.0,
            lifetime.0,
            serde_json::to_string(identity).map_err(error)?,
            input,
            run.0,
            serde_json::to_string(&receipt).map_err(error)?
        ],
    )?;
    Ok(())
}

fn write_document(
    db: &Connection,
    lifetime: &SessionLifetimeId,
    document: &PlanDocument,
) -> Result<(), RuntimeError> {
    let raw = serde_json::to_string(document).map_err(error)?;
    publish_document(db, lifetime, document.revision, &raw)?;
    db.execute(
        "UPDATE session_plans SET data_json=?2 WHERE lifetime=?1",
        params![lifetime.0, raw],
    )?;
    Ok(())
}

fn cancel_pending_execution_in(
    db: &Connection,
    key: &SessionKey,
    lifetime: &SessionLifetimeId,
    document: &PlanDocument,
) -> Result<Option<RunId>, RuntimeError> {
    if document.review != PlanReviewStatus::PendingExecution {
        return Ok(None);
    }
    let row:Option<(String,String)>=db.query_row("SELECT r.id,d.receipt_json FROM runs r JOIN plan_decisions d ON d.run_id=r.id WHERE r.session_id=?1 AND r.lifetime=?2 AND r.status='queued' AND d.decision='execute'",params![key.0,lifetime.0],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    let Some((id, raw)) = row else {
        return Ok(None);
    };
    let receipt: PlanDecisionReceipt = serde_json::from_str(&raw).map_err(error)?;
    identity_matches(document, &receipt.identity)?;
    if receipt.session_key != *key || receipt.lifetime != *lifetime {
        return Err(error("pending plan run 身份不匹配"));
    }
    let run = RunId(id);
    let owner = sessions::owner_in(db, &run)?;
    sessions::fence_in(db, &owner)?;
    crate::turns::commit_output_in(db, &run, RunStatus::Cancelled, None)?;
    crate::insert_event(
        db,
        &run,
        "terminal",
        &serde_json::json!({"status":RunStatus::Cancelled,"reason":"plan_discarded"}),
    )?;
    db.execute(
        "UPDATE runs SET status='cancelled',updated_at_ms=?2 WHERE id=?1 AND status='queued'",
        params![run.0, crate::now_ms()],
    )?;
    db.execute(
        "UPDATE turns SET status='cancelled' WHERE run_id=?1",
        params![run.0],
    )?;
    db.execute(
        "UPDATE queued_messages SET status='cancelled' WHERE run_id=?1",
        params![run.0],
    )?;
    Ok(Some(run))
}

impl RunStore {
    pub(crate) fn read_plan_document(
        &self,
        key: &SessionKey,
    ) -> Result<PlanReadback, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        let meta = sessions::metadata_in(&tx, key)?
            .filter(|m| !m.deleted)
            .ok_or_else(|| error("session 不存在或已删除"))?;
        let raw: Option<String> = tx
            .query_row(
                "SELECT data_json FROM session_plans WHERE lifetime=?1",
                params![meta.lifetime.0],
                |r| r.get(0),
            )
            .optional()?;
        let (plan, legacy_plan, markdown) = match raw {
            Some(raw) => {
                let value: serde_json::Value = serde_json::from_str(&raw).map_err(error)?;
                if value.get("plan_id").is_some() {
                    let document = document_in(&tx, &meta.lifetime)?;
                    let persisted: String = tx.query_row(
                        "SELECT markdown FROM plan_versions WHERE lifetime=?1 AND revision=?2",
                        params![meta.lifetime.0, document.revision],
                        |r| r.get(0),
                    )?;
                    if persisted != document.markdown() {
                        return Err(error("计划 Markdown artifact 损坏"));
                    }
                    (Some(document), None, Some(persisted))
                } else {
                    validate_legacy_plan(&value)?;
                    (None, Some(value), None)
                }
            }
            None => (None, None, None),
        };
        let snapshot_revision = SnapshotRevision(tx.query_row(
            "SELECT revision FROM snapshot_clock WHERE id=1",
            [],
            |r| r.get(0),
        )?);
        super::views::commit(tx)?;
        Ok(PlanReadback {
            session_key: key.clone(),
            lifetime: meta.lifetime,
            snapshot_revision,
            plan,
            legacy_plan,
            markdown,
        })
    }

    pub(crate) fn discard_plan_document(
        &self,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        identity: &PlanExecution,
    ) -> Result<PlanDecisionReceipt, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        let meta = sessions::metadata_in(&tx, key)?
            .filter(|m| !m.deleted && m.lifetime == *lifetime)
            .ok_or_else(|| error("stale plan lifetime"))?;
        if let Some(receipt) = receipt_in(
            &tx,
            key,
            &meta.lifetime,
            identity,
            PlanDecision::Discard,
            "",
        )? {
            return Ok(receipt);
        }
        let mut document = document_in(&tx, lifetime)?;
        identity_matches(&document, identity)?;
        let cancelled_run = cancel_pending_execution_in(&tx, key, lifetime, &document)?;
        sessions::ensure_idle_in(&tx, key, lifetime)?;
        document.review = PlanReviewStatus::Rejected;
        write_document(&tx, lifetime, &document)?;
        let receipt = PlanDecisionReceipt {
            session_key: key.clone(),
            lifetime: lifetime.clone(),
            identity: identity.clone(),
            decision: PlanDecision::Discard,
            run_id: cancelled_run,
        };
        tx.execute(
            "INSERT INTO plan_decisions VALUES(?1,?2,?3,?4,'discard','',NULL,?5)",
            params![
                identity.operation_id,
                key.0,
                lifetime.0,
                serde_json::to_string(identity).map_err(error)?,
                serde_json::to_string(&receipt).map_err(error)?
            ],
        )?;
        super::views::commit(tx)?;
        Ok(receipt)
    }

    pub(crate) fn begin_plan_execution(&self, owner: &ExactOwner) -> Result<(), RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        sessions::fence_in(&tx, owner)?;
        let row: Option<String> = tx
            .query_row(
                "SELECT receipt_json FROM plan_decisions WHERE run_id=?1 AND decision='execute'",
                params![owner.run_id.0],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(raw) = row {
            let receipt: PlanDecisionReceipt = serde_json::from_str(&raw).map_err(error)?;
            let status: String = tx.query_row(
                "SELECT status FROM runs WHERE id=?1",
                params![owner.run_id.0],
                |r| r.get(0),
            )?;
            if status != "running"
                || receipt.lifetime != owner.session_lifetime_id
                || receipt.session_key != owner.session_key
            {
                return Err(error("plan execution writer 不匹配"));
            }
            let mut document = document_in(&tx, &owner.session_lifetime_id)?;
            identity_matches(&document, &receipt.identity)?;
            if document.review != PlanReviewStatus::PendingExecution {
                return Err(error("计划不是 pending_execution"));
            }
            document.review = PlanReviewStatus::Executing;
            write_document(&tx, &owner.session_lifetime_id, &document)?;
        }
        super::views::commit(tx)?;
        Ok(())
    }
}

pub(crate) fn settle_execution_in(
    db: &Connection,
    owner: &ExactOwner,
    status: RunStatus,
) -> Result<(), RuntimeError> {
    let exists: i64 = db.query_row(
        "SELECT count(*) FROM plan_decisions WHERE run_id=?1 AND decision='execute'",
        params![owner.run_id.0],
        |r| r.get(0),
    )?;
    if exists != 0 && status != RunStatus::Completed {
        let mut doc = document_in(db, &owner.session_lifetime_id)?;
        if status == RunStatus::Cancelled && doc.review == PlanReviewStatus::PendingExecution {
            return Ok(());
        }
        doc.review = PlanReviewStatus::Blocked;
        write_document(db, &owner.session_lifetime_id, &doc)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SessionLifecycle, SessionQuery, TurnRepository};

    fn snapshot() -> RunSnapshot {
        RunSnapshot {
            entry_channel: agent_core::HookChannel::DaemonRpc,
            route: None,
            tools: vec![],
            cwd: ".".into(),
            permission_mode: "request".into(),
            sandbox_requested: "native".into(),
            sandbox_effective: "native".into(),
            sandbox_notice: None,
            docker_image: None,
            delegation_context: None,
            context_read_only: false,
            context_token_budget: 4096,
            context_policy_fingerprint: None,
            tool_catalog_digest: "empty".into(),
            memory_entry_budget: 8,
            memory_token_budget: 1024,
            max_tool_calls: None,
            config_generation: 0,
        }
    }

    fn author(store: &RunStore, key: &SessionKey) -> PlanReadback {
        store.create_session(key).unwrap();
        let Admission::New(run) = store
            .admit_with_route(
                key.clone(),
                RequestId::Number(1),
                "制定计划",
                AdmissionMode::Queue,
                None,
            )
            .unwrap()
        else {
            panic!("new")
        };
        store.try_start_queued(&run.run_id).unwrap();
        let owner = store.run_owner(&run.run_id).unwrap();
        store.stage_plan(&owner,0,&serde_json::json!({"title":"审计","goal":"恢复安全","verification":["重启验证"],"steps":[{"id":"a","description":"验证 canonical state"}]})).unwrap();
        store
            .finish(&run.run_id, RunStatus::Completed, Some("待审"), None)
            .unwrap();
        store.plan_readback(key).unwrap()
    }

    fn identity(read: &PlanReadback, operation: &str) -> PlanExecution {
        let plan = read.plan.as_ref().unwrap();
        PlanExecution {
            plan_id: plan.plan_id.clone(),
            revision: plan.revision,
            content_digest: plan.content_digest.clone(),
            operation_id: operation.into(),
        }
    }

    fn admission(read: &PlanReadback, id: PlanExecution) -> RunAdmission {
        RunAdmission {
            session_key: read.session_key.clone(),
            expected_lifetime: Some(read.lifetime.clone()),
            request_id: RequestId::Number(2),
            input: "精确执行".into(),
            mode: AdmissionMode::RejectIfBusy,
            plan_execution: Some(id),
        }
    }

    #[test]
    fn plan_decisions_are_exact_idempotent_and_share_file_and_memory_contract() {
        let path = std::env::temp_dir().join(format!("plan-contract-{}", std::process::id()));
        std::fs::create_dir_all(&path).unwrap();
        for backend in [
            std::path::PathBuf::from(":memory:"),
            path.join("runtime.sqlite3"),
        ] {
            let store = RunStore::open(&backend).unwrap();
            let key = SessionKey("plan".into());
            let read = author(&store, &key);
            assert_eq!(
                store.plan_readback(&key).unwrap().snapshot_revision,
                read.snapshot_revision
            );
            assert!(
                !serde_json::to_string(&read)
                    .unwrap()
                    .contains(path.to_str().unwrap())
            );
            let id = identity(&read, "execute-1");
            let mut command = admission(&read, id.clone());
            command.plan_execution.as_mut().unwrap().content_digest = "stale".into();
            assert!(store.admit_run(&command, &snapshot()).is_err());
            assert_eq!(
                store.plan_readback(&key).unwrap().snapshot_revision,
                read.snapshot_revision
            );
            command = admission(&read, id.clone());
            command.expected_lifetime = Some(SessionLifetimeId("old".into()));
            assert!(store.admit_run(&command, &snapshot()).is_err());
            command = admission(&read, id.clone());
            let Admission::New(run) = store.admit_run(&command, &snapshot()).unwrap() else {
                panic!("new")
            };
            assert_eq!(
                store.plan_readback(&key).unwrap().plan.unwrap().review,
                PlanReviewStatus::PendingExecution
            );
            assert!(
                matches!(store.admit_run(&command,&snapshot()).unwrap(),Admission::Existing(existing) if existing.run_id==run.run_id)
            );
            let mut different_snapshot = snapshot();
            different_snapshot.context_read_only = true;
            assert!(store.admit_run(&command, &different_snapshot).is_err());
            different_snapshot = snapshot();
            different_snapshot.sandbox_requested = "docker".into();
            assert!(store.admit_run(&command, &different_snapshot).is_err());
            command.input = "替换输入".into();
            assert!(store.admit_run(&command, &snapshot()).is_err());
            assert!(
                store
                    .admit_run(
                        &admission(&read, identity(&read, "execute-busy")),
                        &snapshot()
                    )
                    .is_err()
            );
            assert!(store.recoverable_queued().unwrap().is_empty());
            let owner = store.run_owner(&run.run_id).unwrap();
            assert!(store.start_plan_execution(&owner).is_err());
            store.try_start_queued(&run.run_id).unwrap();
            assert_eq!(
                store.plan_readback(&key).unwrap().plan.unwrap().review,
                PlanReviewStatus::PendingExecution
            );
            assert!(
                store
                    .discard_plan(&key, &read.lifetime, &identity(&read, "discard-busy"))
                    .is_err()
            );
            store.start_plan_execution(&owner).unwrap();
            assert_eq!(
                store.plan_readback(&key).unwrap().plan.unwrap().review,
                PlanReviewStatus::Executing
            );
            store.recover().unwrap();
            assert_eq!(
                store.read_run(&run.run_id).unwrap().unwrap().status,
                RunStatus::UnknownAfterRestart
            );
            assert_eq!(
                store.plan_readback(&key).unwrap().plan.unwrap().review,
                PlanReviewStatus::Blocked
            );
            let discard = identity(&read, "discard-1");
            let receipt = store.discard_plan(&key, &read.lifetime, &discard).unwrap();
            assert_eq!(
                receipt,
                store.discard_plan(&key, &read.lifetime, &discard).unwrap()
            );
            assert_eq!(
                store.plan_readback(&key).unwrap().plan.unwrap().review,
                PlanReviewStatus::Rejected
            );
            assert!(
                store
                    .admit_run(&admission(&read, discard), &snapshot())
                    .is_err()
            );
            assert!(
                store
                    .admit_run(
                        &admission(&read, identity(&read, "rejected-execute")),
                        &snapshot()
                    )
                    .is_err()
            );
            let count: i64 = store
                .lock_connection()
                .unwrap()
                .query_row("SELECT count(*) FROM plan_versions", [], |r| r.get(0))
                .unwrap();
            assert_eq!(count, 1);
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn pending_plan_survives_restart_without_automatic_execution_and_old_plan_stays_readonly() {
        let dir = std::env::temp_dir().join(format!("plan-restart-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("runtime.sqlite3");
        let store = RunStore::open(&path).unwrap();
        let read = author(&store, &SessionKey("plan".into()));
        let command = admission(&read, identity(&read, "continue"));
        let Admission::New(run) = store.admit_run(&command, &snapshot()).unwrap() else {
            panic!("new")
        };
        drop(store);
        let store = RunStore::open(&path).unwrap();
        store.recover().unwrap();
        assert!(store.recoverable_queued().unwrap().is_empty());
        assert_eq!(
            store.read_run(&run.run_id).unwrap().unwrap().status,
            RunStatus::Queued
        );
        assert_eq!(
            store
                .plan_readback(&read.session_key)
                .unwrap()
                .plan
                .unwrap()
                .review,
            PlanReviewStatus::PendingExecution
        );
        assert!(matches!(
            store.admit_run(&command, &snapshot()).unwrap(),
            Admission::Existing(_)
        ));
        let old = SessionKey("legacy".into());
        store.create_session(&old).unwrap();
        store
            .import_legacy_plan("legacy", "digest", &old, &serde_json::json!({"steps":[]}))
            .unwrap();
        let legacy = store.plan_readback(&old).unwrap();
        assert!(legacy.plan.is_none());
        assert!(legacy.legacy_plan.is_some());
        let forged = PlanExecution {
            plan_id: "forged".into(),
            revision: 1,
            content_digest: "fake".into(),
            operation_id: "forged".into(),
        };
        assert!(
            store
                .admit_run(&admission(&legacy, forged), &snapshot())
                .is_err()
        );
        let preserved: String = store
            .lock_connection()
            .unwrap()
            .query_row(
                "SELECT data_json FROM plan_legacy_evidence WHERE lifetime=?1",
                params![legacy.lifetime.0],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&preserved).unwrap(),
            serde_json::json!({"steps":[]})
        );
        store.lock_connection().unwrap().execute("UPDATE session_plans SET data_json=?2 WHERE lifetime=?1",params![legacy.lifetime.0,serde_json::json!({"definition":{"steps":[]},"revision":1,"content_digest":"broken"}).to_string()]).unwrap();
        assert!(store.plan_readback(&old).is_err());
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn committed_progress_keeps_definition_version_and_changed_definition_preserves_history() {
        let store = RunStore::open(std::path::Path::new(":memory:")).unwrap();
        let key = SessionKey("progress".into());
        let first = author(&store, &key).plan.unwrap();
        let mut definition = first.definition.clone();
        definition.steps[0].status = PlanStatus::Done;
        for (request, change) in [(2, false), (3, true)] {
            if change {
                definition.goal = "新的成功目标".into();
            }
            let Admission::New(run) = store
                .admit_with_route(
                    key.clone(),
                    RequestId::Number(request),
                    "更新计划",
                    AdmissionMode::Queue,
                    None,
                )
                .unwrap()
            else {
                panic!("new")
            };
            store.try_start_queued(&run.run_id).unwrap();
            let owner = store.run_owner(&run.run_id).unwrap();
            let current = store.read_plan(&owner).unwrap();
            store
                .stage_plan(
                    &owner,
                    current.revision,
                    &serde_json::to_value(&definition).unwrap(),
                )
                .unwrap();
            store
                .finish(&run.run_id, RunStatus::Completed, Some("已更新"), None)
                .unwrap();
            let published = store.plan_readback(&key).unwrap().plan.unwrap();
            assert_eq!(published.plan_id, first.plan_id);
            assert_eq!(published.revision, first.revision + u64::from(change));
            assert_eq!(published.definition.steps[0].status, PlanStatus::Done);
            if change {
                assert_ne!(published.content_digest, first.content_digest);
            } else {
                assert_eq!(published.content_digest, first.content_digest);
                assert_ne!(published.artifact, first.artifact);
            }
        }
        let count: i64 = store
            .lock_connection()
            .unwrap()
            .query_row("SELECT count(*) FROM plan_versions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn discard_pending_plan_cancels_only_the_exact_unstarted_run() {
        let store = RunStore::open(std::path::Path::new(":memory:")).unwrap();
        let read = author(&store, &SessionKey("discard-pending".into()));
        let command = admission(&read, identity(&read, "execute-pending"));
        let Admission::New(run) = store.admit_run(&command, &snapshot()).unwrap() else {
            panic!("new")
        };
        let discard = identity(&read, "discard-pending");
        let receipt = store
            .discard_plan(&read.session_key, &read.lifetime, &discard)
            .unwrap();
        assert_eq!(receipt.run_id, Some(run.run_id.clone()));
        assert_eq!(
            store.read_run(&run.run_id).unwrap().unwrap().status,
            RunStatus::Cancelled
        );
        assert_eq!(
            store
                .plan_readback(&read.session_key)
                .unwrap()
                .plan
                .unwrap()
                .review,
            PlanReviewStatus::Rejected
        );
        assert_eq!(
            receipt,
            store
                .discard_plan(&read.session_key, &read.lifetime, &discard)
                .unwrap()
        );
        assert!(!store.try_start_queued(&run.run_id).unwrap());
        assert!(
            store
                .queued_messages(&read.session_key.0)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn markdown_publication_fault_rolls_back_plan_terminal_and_revision() {
        let store = RunStore::open(std::path::Path::new(":memory:")).unwrap();
        let key = SessionKey("fault".into());
        store.create_session(&key).unwrap();
        let Admission::New(run) = store
            .admit_with_route(
                key.clone(),
                RequestId::Number(1),
                "计划",
                AdmissionMode::Queue,
                None,
            )
            .unwrap()
        else {
            panic!("new")
        };
        store.try_start_queued(&run.run_id).unwrap();
        let owner = store.run_owner(&run.run_id).unwrap();
        store
            .stage_plan(
                &owner,
                0,
                &serde_json::json!({"steps":[{"id":"a","description":"检查"}]}),
            )
            .unwrap();
        let before = store
            .session_readback(&key, HistoryReadMode::Canonical)
            .unwrap();
        store.lock_connection().unwrap().execute_batch("CREATE TRIGGER artifact_fault BEFORE INSERT ON plan_versions BEGIN SELECT RAISE(ABORT,'artifact fault'); END;").unwrap();
        assert!(
            store
                .finish(&run.run_id, RunStatus::Completed, Some("完成"), None)
                .is_err()
        );
        let after = store
            .session_readback(&key, HistoryReadMode::Canonical)
            .unwrap();
        assert_eq!(before.snapshot_revision, after.snapshot_revision);
        assert_eq!(before.transcript_revision, after.transcript_revision);
        assert!(store.plan_readback(&key).unwrap().plan.is_none());
    }

    #[test]
    fn schema14_upgrade_is_backed_up_atomic_and_rejects_future_versions() {
        let dir = std::env::temp_dir().join(format!("plan-migration-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("runtime.sqlite3");
        let key = SessionKey("old".into());
        let store = RunStore::open(&path).unwrap();
        let old = store.create_session(&key).unwrap();
        store
            .import_legacy_plan("old-file", "digest", &key, &serde_json::json!({"steps":[]}))
            .unwrap();
        store.lock_connection().unwrap().execute_batch("DROP TABLE provider_requests; DROP TABLE event_view_stamps; DROP TABLE tool_discoveries; DROP TABLE compact_run_links; DROP TABLE plan_versions; DROP TABLE plan_decisions; DROP TABLE plan_legacy_evidence; DROP TABLE hook_outcomes; DROP TABLE hook_publications; DROP TABLE hook_continuations; DELETE FROM schema_migrations WHERE version>=15; CREATE TRIGGER migration_fault BEFORE INSERT ON schema_migrations WHEN NEW.version=15 BEGIN SELECT RAISE(ABORT,'migration fault'); END;").unwrap();
        drop(store);
        assert!(RunStore::open(&path).is_err());
        let db = Connection::open(&path).unwrap();
        let version: i64 = db
            .query_row("SELECT max(version) FROM schema_migrations", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(version, 14);
        let count:i64=db.query_row("SELECT count(*) FROM sqlite_master WHERE name IN ('plan_versions','plan_decisions','plan_legacy_evidence')",[],|r|r.get(0)).unwrap();
        assert_eq!(count, 0);
        assert!(path.with_extension("v14.backup.sqlite3").exists());
        assert!(path.with_extension("v14.backup.verified.json").exists());
        db.execute_batch("DROP TRIGGER migration_fault").unwrap();
        drop(db);
        let store = RunStore::open(&path).unwrap();
        let read = store.plan_readback(&key).unwrap();
        assert_eq!(read.lifetime, old.lifetime);
        assert!(read.plan.is_none() && read.legacy_plan.is_some());
        drop(store);
        let store = RunStore::open(&path).unwrap();
        assert_eq!(store.plan_readback(&key).unwrap().lifetime, old.lifetime);
        store
            .lock_connection()
            .unwrap()
            .execute("INSERT INTO schema_migrations VALUES(21,0)", [])
            .unwrap();
        drop(store);
        assert!(RunStore::open(&path).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
