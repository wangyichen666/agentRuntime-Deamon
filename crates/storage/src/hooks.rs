//! Hook 的幂等 claim、typed outcome 与 lifecycle publication outbox。
use super::{RunStore, RuntimeError};
use agent_core::*;
use rusqlite::{Connection, OptionalExtension, params};

pub trait HookRepository: Send + Sync {
    fn claim_hook(&self, payload: &HookPayload) -> Result<HookClaim, RuntimeError>;
    fn settle_hook(&self, outcome: &HookOutcome) -> Result<HookOutcome, RuntimeError>;
    #[cfg(any(test, feature = "test-support"))]
    fn hook_outcomes(
        &self,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
    ) -> Result<Vec<HookOutcome>, RuntimeError>;
    fn hook_publications(&self) -> Result<Vec<HookPublication>, RuntimeError>;
    fn hook_readback(
        &self,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        after: u64,
        limit: usize,
    ) -> Result<HookReadback, RuntimeError>;
    fn acknowledge_hook_publication(&self, operation: &str) -> Result<(), RuntimeError>;
    fn admit_hook_continuation(
        &self,
        parent: &ExactOwner,
        operation: &str,
        prompt: &str,
        snapshot: &RunSnapshot,
    ) -> Result<Admission, RuntimeError>;
    fn hook_continuation_parent(&self, run: &RunId) -> Result<Option<RunId>, RuntimeError>;
    fn hook_continuation_child(&self, run: &RunId) -> Result<Option<RunId>, RuntimeError>;
}

pub(crate) fn publish_in(
    db: &Connection,
    meta: &SessionMetadata,
    event: HookEvent,
) -> Result<(), RuntimeError> {
    let event_label = serde_json::to_string(&event).map_err(protocol)?;
    let publication = HookPublication {
        session_key: meta.key.clone(),
        session_lifetime_id: meta.lifetime.clone(),
        event,
        operation_id: format!("lifecycle:{event_label}:{}", meta.lifetime.0),
    };
    db.execute("INSERT OR IGNORE INTO hook_publications(operation_id,payload_json,dispatched) VALUES(?1,?2,0)",
        params![publication.operation_id,serde_json::to_string(&publication).map_err(protocol)?])?;
    Ok(())
}

fn protocol(error: serde_json::Error) -> RuntimeError {
    RuntimeError::Protocol(error.to_string())
}

fn validate_outcome(outcome: &HookOutcome) -> Result<(), RuntimeError> {
    let payload = &outcome.payload;
    let owner_valid = payload.owner.as_ref().map_or_else(
        || {
            matches!(
                payload.event,
                HookEvent::SessionStart | HookEvent::SessionEnd | HookEvent::Notification
            )
        },
        |owner| {
            owner.session_key == payload.session_key
                && owner.session_lifetime_id == payload.session_lifetime_id
        },
    );
    let state_valid = match outcome.status {
        HookStatus::Running => outcome.effect.is_none() && outcome.failure.is_none(),
        HookStatus::Succeeded => {
            outcome.failure.is_none()
                && outcome
                    .effect
                    .as_ref()
                    .is_some_and(|effect| payload.event.permits(effect))
        }
        HookStatus::Failed => {
            outcome.effect.is_none()
                && outcome
                    .failure
                    .is_some_and(|failure| failure != HookFailure::UnknownAfterRestart)
        }
        HookStatus::UnknownAfterRestart => {
            outcome.effect.is_none() && outcome.failure == Some(HookFailure::UnknownAfterRestart)
        }
    };
    if payload.schema_version != 1
        || payload.operation_id.is_empty()
        || payload.operation_id.len() > 256
        || serde_json::to_vec(payload).map_err(protocol)?.len() > 16384
        || !owner_valid
        || !state_valid
    {
        return Err(RuntimeError::Protocol("hook 持久记录身份或状态损坏".into()));
    }
    Ok(())
}

impl HookRepository for RunStore {
    fn admit_hook_continuation(
        &self,
        parent: &ExactOwner,
        operation: &str,
        prompt: &str,
        snapshot: &RunSnapshot,
    ) -> Result<Admission, RuntimeError> {
        let mut snapshot = snapshot.clone();
        snapshot.entry_channel = HookChannel::Continuation;
        let admission = RunAdmission {
            plan_execution: None,
            session_key: parent.session_key.clone(),
            expected_lifetime: Some(parent.session_lifetime_id.clone()),
            request_id: crate::RequestId::String(format!("continuation:{}", parent.run_id.0)),
            input: prompt.into(),
            mode: AdmissionMode::Queue,
        };
        self.admit_run_inner(
            &admission,
            snapshot.route.as_ref(),
            Some(&snapshot),
            Some((parent, operation)),
            None,
        )
    }
    fn hook_continuation_parent(&self, run: &RunId) -> Result<Option<RunId>, RuntimeError> {
        Ok(self
            .lock_connection()?
            .query_row(
                "SELECT parent_run_id FROM hook_continuations WHERE child_run_id=?1",
                params![run.0],
                |r| r.get::<_, String>(0),
            )
            .optional()?
            .map(RunId))
    }
    fn hook_continuation_child(&self, run: &RunId) -> Result<Option<RunId>, RuntimeError> {
        Ok(self
            .lock_connection()?
            .query_row(
                "SELECT child_run_id FROM hook_continuations WHERE parent_run_id=?1",
                params![run.0],
                |r| r.get::<_, String>(0),
            )
            .optional()?
            .map(RunId))
    }
    fn claim_hook(&self, payload: &HookPayload) -> Result<HookClaim, RuntimeError> {
        if payload.schema_version != 1
            || payload.operation_id.is_empty()
            || payload.operation_id.len() > 256
            || serde_json::to_vec(payload).map_err(protocol)?.len() > 16384
        {
            return Err(RuntimeError::Protocol(
                "hook payload/schema/operation 超过预算".into(),
            ));
        }
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        if payload.owner.is_none()
            && !matches!(
                payload.event,
                HookEvent::SessionStart | HookEvent::SessionEnd | HookEvent::Notification
            )
        {
            return Err(RuntimeError::Protocol("run hook 缺 exact owner".into()));
        }
        let previous: Option<String> = tx
            .query_row(
                "SELECT outcome_json FROM hook_outcomes WHERE operation_id=?1",
                params![payload.operation_id],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(previous) = previous {
            let outcome: HookOutcome = serde_json::from_str(&previous).map_err(protocol)?;
            validate_outcome(&outcome)?;
            if outcome.payload != *payload {
                return Err(RuntimeError::Protocol(
                    "hook operation 身份或输入冲突".into(),
                ));
            }
            return Ok(HookClaim::Existing(Box::new(outcome)));
        }
        if let Some(owner) = &payload.owner {
            if owner.session_key != payload.session_key
                || owner.session_lifetime_id != payload.session_lifetime_id
            {
                return Err(RuntimeError::Protocol("hook owner 不匹配".into()));
            }
            super::sessions::fence_in(&tx, owner)?;
        } else {
            let current = super::sessions::metadata_in(&tx, &payload.session_key)?;
            let ended: i64 = tx.query_row(
                "SELECT count(*) FROM session_tombstones WHERE lifetime=?1 AND session_id=?2",
                params![payload.session_lifetime_id.0, payload.session_key.0],
                |r| r.get(0),
            )?;
            let historical_publication: Option<String> = tx
                .query_row(
                    "SELECT payload_json FROM hook_publications WHERE operation_id=?1",
                    params![payload.operation_id],
                    |r| r.get(0),
                )
                .optional()?;
            let historical_start = if let Some(raw) = historical_publication {
                let publication: HookPublication = serde_json::from_str(&raw).map_err(protocol)?;
                publication.event == HookEvent::SessionStart
                    && payload.event == HookEvent::SessionStart
                    && publication.session_key == payload.session_key
                    && publication.session_lifetime_id == payload.session_lifetime_id
            } else {
                false
            };
            if !(current.is_some_and(|m| m.lifetime == payload.session_lifetime_id)
                || ended == 1 && (payload.event == HookEvent::SessionEnd || historical_start))
            {
                return Err(RuntimeError::Protocol("hook stale lifetime".into()));
            }
        }
        let outcome = HookOutcome {
            payload: payload.clone(),
            status: HookStatus::Running,
            effect: None,
            failure: None,
        };
        tx.execute("INSERT INTO hook_outcomes(operation_id,session_id,lifetime,outcome_json) VALUES(?1,?2,?3,?4)",params![payload.operation_id,payload.session_key.0,payload.session_lifetime_id.0,serde_json::to_string(&outcome).map_err(protocol)?])?;
        tx.execute(
            "UPDATE hook_publications SET dispatched=1 WHERE operation_id=?1",
            params![payload.operation_id],
        )?;
        super::views::commit(tx)?;
        Ok(HookClaim::New)
    }
    fn settle_hook(&self, outcome: &HookOutcome) -> Result<HookOutcome, RuntimeError> {
        validate_outcome(outcome)?;
        if !matches!(outcome.status, HookStatus::Succeeded | HookStatus::Failed)
            || (outcome.status == HookStatus::Succeeded
                && (outcome.failure.is_some()
                    || outcome
                        .effect
                        .as_ref()
                        .is_none_or(|e| !outcome.payload.event.permits(e))))
            || (outcome.status == HookStatus::Failed
                && (outcome.failure.is_none() || outcome.effect.is_some()))
        {
            return Err(RuntimeError::Protocol("hook outcome/effect 非法".into()));
        }
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        let raw: String = tx.query_row(
            "SELECT outcome_json FROM hook_outcomes WHERE operation_id=?1",
            params![outcome.payload.operation_id],
            |r| r.get(0),
        )?;
        let old: HookOutcome = serde_json::from_str(&raw).map_err(protocol)?;
        validate_outcome(&old)?;
        if old.payload != outcome.payload {
            return Err(RuntimeError::Protocol("hook settle 身份冲突".into()));
        }
        if old.status != HookStatus::Running {
            if old == *outcome || old.status == HookStatus::UnknownAfterRestart {
                return Ok(old);
            }
            return Err(RuntimeError::Protocol("hook 已结算".into()));
        }
        tx.execute(
            "UPDATE hook_outcomes SET outcome_json=?2 WHERE operation_id=?1",
            params![
                outcome.payload.operation_id,
                serde_json::to_string(outcome).map_err(protocol)?
            ],
        )?;
        super::views::commit(tx)?;
        Ok(outcome.clone())
    }
    #[cfg(any(test, feature = "test-support"))]
    fn hook_outcomes(
        &self,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
    ) -> Result<Vec<HookOutcome>, RuntimeError> {
        Ok(self.hook_readback(key, lifetime, 0, 32)?.outcomes)
    }
    fn hook_readback(
        &self,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        after: u64,
        limit: usize,
    ) -> Result<HookReadback, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        let db = &tx;
        let valid: i64 = db.query_row(
            "SELECT count(*) FROM session_heads WHERE session_id=?1 AND lifetime=?2",
            params![key.0, lifetime.0],
            |r| r.get(0),
        )?;
        let ended: i64 = db.query_row(
            "SELECT count(*) FROM session_tombstones WHERE session_id=?1 AND lifetime=?2",
            params![key.0, lifetime.0],
            |r| r.get(0),
        )?;
        if valid == 0 && ended == 0 {
            return Err(RuntimeError::Protocol(
                "hook readback lifetime 不存在".into(),
            ));
        }
        let snapshot_revision = SnapshotRevision(db.query_row(
            "SELECT revision FROM snapshot_clock WHERE id=1",
            [],
            |r| r.get(0),
        )?);
        let mut q=db.prepare("SELECT rowid,operation_id,outcome_json FROM hook_outcomes WHERE session_id=?1 AND lifetime=?2 AND rowid>?3 ORDER BY rowid LIMIT ?4")?;
        let raws = q
            .query_map(
                params![
                    key.0,
                    lifetime.0,
                    i64::try_from(after)
                        .map_err(|_| RuntimeError::Protocol("hook cursor 超限".into()))?,
                    limit.clamp(1, 32) + 1
                ],
                |r| {
                    Ok((
                        r.get::<_, u64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = raws.len() > limit.clamp(1, 32);
        let page = raws
            .into_iter()
            .take(limit.clamp(1, 32))
            .collect::<Vec<_>>();
        let cursor = page.last().map_or(after, |(cursor, _, _)| *cursor);
        let outcomes = page
            .into_iter()
            .map(|(_, operation, raw)| {
                let outcome: HookOutcome = serde_json::from_str(&raw).map_err(protocol)?;
                validate_outcome(&outcome)?;
                if outcome.payload.session_key != *key
                    || outcome.payload.session_lifetime_id != *lifetime
                    || outcome.payload.operation_id != operation
                {
                    return Err(RuntimeError::Protocol("hook readback 行身份损坏".into()));
                }
                Ok(outcome)
            })
            .collect::<Result<_, _>>()?;
        drop(q);
        super::views::commit(tx)?;
        Ok(HookReadback {
            schema_version: 1,
            session_key: key.clone(),
            session_lifetime_id: lifetime.clone(),
            snapshot_revision,
            outcomes,
            cursor,
            has_more,
        })
    }
    fn hook_publications(&self) -> Result<Vec<HookPublication>, RuntimeError> {
        let db = self.lock_connection()?;
        let mut q = db.prepare(
            "SELECT payload_json FROM hook_publications WHERE dispatched=0 ORDER BY rowid LIMIT 64",
        )?;
        let raws = q
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        raws.into_iter()
            .map(|raw| serde_json::from_str(&raw).map_err(protocol))
            .collect()
    }
    fn acknowledge_hook_publication(&self, operation: &str) -> Result<(), RuntimeError> {
        self.lock_connection()?.execute(
            "UPDATE hook_publications SET dispatched=1 WHERE operation_id=?1",
            params![operation],
        )?;
        Ok(())
    }
}

pub(crate) fn recover_in(db: &Connection) -> Result<(), RuntimeError> {
    db.execute("UPDATE hook_outcomes SET outcome_json=json_set(outcome_json,'$.status','unknown_after_restart','$.failure','unknown_after_restart') WHERE json_extract(outcome_json,'$.status')='running'",[])?;
    Ok(())
}

pub(crate) fn validate_continuation_in(
    db: &Connection,
    parent: &ExactOwner,
    operation: &str,
    prompt: &str,
    snapshot: Option<&RunSnapshot>,
) -> Result<(), RuntimeError> {
    super::sessions::fence_in(db, parent)?;
    let raw: String = db.query_row(
        "SELECT snapshot_json FROM run_snapshots WHERE run_id=?1",
        params![parent.run_id.0],
        |r| r.get(0),
    )?;
    let mut frozen: RunSnapshot = serde_json::from_str(&raw).map_err(protocol)?;
    frozen.entry_channel = HookChannel::Continuation;
    if serde_json::to_value(&frozen).map_err(protocol)?
        != serde_json::to_value(
            snapshot
                .ok_or_else(|| RuntimeError::Protocol("continuation 缺 frozen snapshot".into()))?,
        )
        .map_err(protocol)?
    {
        return Err(RuntimeError::Protocol(
            "continuation 不得扩大父 run 冻结授权".into(),
        ));
    }
    let status: String = db.query_row(
        "SELECT status FROM runs WHERE id=?1",
        params![parent.run_id.0],
        |r| r.get(0),
    )?;
    let descendant: i64 = db.query_row(
        "SELECT count(*) FROM hook_continuations WHERE child_run_id=?1",
        params![parent.run_id.0],
        |r| r.get(0),
    )?;
    let existing: Option<String> = db
        .query_row(
            "SELECT operation_id FROM hook_continuations WHERE parent_run_id=?1",
            params![parent.run_id.0],
            |r| r.get(0),
        )
        .optional()?;
    if existing.as_ref().is_some_and(|old| old != operation) {
        return Err(RuntimeError::Protocol("continuation operation 冲突".into()));
    }
    if status != "completed" || descendant != 0 {
        return Err(RuntimeError::Protocol(
            "Stop continuation budget/parent status 不允许".into(),
        ));
    }
    let raw: String = db.query_row(
        "SELECT outcome_json FROM hook_outcomes WHERE operation_id=?1",
        params![operation],
        |r| r.get(0),
    )?;
    let outcome: HookOutcome = serde_json::from_str(&raw).map_err(protocol)?;
    if outcome.payload.owner.as_ref() != Some(parent)
        || outcome.payload.event != HookEvent::Stop
        || outcome.status != HookStatus::Succeeded
        || outcome.effect
            != Some(HookEffect::Continue {
                prompt: prompt.into(),
            })
    {
        return Err(RuntimeError::Protocol(
            "Stop continuation 没有精确成功指令".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SessionLifecycle, SessionQuery};
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    fn path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "hook-repo-{}-{}.sqlite3",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ))
    }
    fn payload(meta: &SessionMetadata, event: HookEvent, operation: &str) -> HookPayload {
        HookPayload {
            schema_version: 1,
            event,
            session_key: meta.key.clone(),
            session_lifetime_id: meta.lifetime.clone(),
            owner: None,
            cwd: ".".into(),
            channel: "daemon_rpc".into(),
            permission_mode: "observation".into(),
            route_digest: None,
            data: serde_json::json!({}),
            operation_id: operation.into(),
        }
    }
    #[test]
    fn hook_claim_effect_fence_recovery_and_lifecycle_outbox_share_both_backends() {
        for path in [std::path::PathBuf::from(":memory:"), path()] {
            let store = RunStore::open(&path).unwrap();
            let key = SessionKey("hooks".into());
            let meta = store.create_session(&key).unwrap();
            assert_eq!(store.hook_publications().unwrap().len(), 1);
            let request = payload(&meta, HookEvent::SessionStart, "start");
            assert_eq!(store.claim_hook(&request).unwrap(), HookClaim::New);
            assert!(
                matches!(store.claim_hook(&request).unwrap(),HookClaim::Existing(existing) if existing.status==HookStatus::Running)
            );
            let mut changed = request.clone();
            changed.data = serde_json::json!({"changed":true});
            assert!(store.claim_hook(&changed).is_err());
            let invalid = HookOutcome {
                payload: request.clone(),
                status: HookStatus::Succeeded,
                effect: Some(HookEffect::Deny {
                    reason: "越权".into(),
                }),
                failure: None,
            };
            assert!(store.settle_hook(&invalid).is_err());
            let final_result = HookOutcome {
                effect: Some(HookEffect::Observe {}),
                ..invalid
            };
            assert_eq!(store.settle_hook(&final_result).unwrap(), final_result);
            assert_eq!(store.settle_hook(&final_result).unwrap(), final_result);
            let unfinished = payload(&meta, HookEvent::Notification, "uncertain");
            store.claim_hook(&unfinished).unwrap();
            store.recover().unwrap();
            let old = store.hook_outcomes(&key, &meta.lifetime).unwrap();
            assert_eq!(old[1].status, HookStatus::UnknownAfterRestart);
            assert_eq!(old[1].failure, Some(HookFailure::UnknownAfterRestart));
            assert!(
                matches!(store.claim_hook(&unfinished).unwrap(),HookClaim::Existing(record) if record.status==HookStatus::UnknownAfterRestart)
            );
            let end = SessionCommand::End {
                key: key.clone(),
                delete: true,
            };
            store
                .execute_lifecycle(&end, Some(&meta.lifetime), "close")
                .unwrap();
            store
                .execute_lifecycle(&end, Some(&meta.lifetime), "close")
                .unwrap();
            let publications = store.hook_publications().unwrap();
            assert_eq!(publications.len(), 2);
            assert_eq!(publications[1].event, HookEvent::SessionEnd);
            let new = store
                .execute_lifecycle(
                    &SessionCommand::Create { key: key.clone() },
                    None,
                    "recreate",
                )
                .unwrap();
            assert_ne!(new.lifetime, meta.lifetime);
            assert_eq!(store.hook_publications().unwrap().len(), 3);
            let published_start = publications
                .iter()
                .find(|publication| publication.event == HookEvent::SessionStart)
                .unwrap();
            let historical_start = payload(
                &meta,
                HookEvent::SessionStart,
                &published_start.operation_id,
            );
            assert_eq!(store.claim_hook(&historical_start).unwrap(), HookClaim::New);
            assert!(
                store
                    .claim_hook(&payload(
                        &meta,
                        HookEvent::SessionStart,
                        "unpublished-start"
                    ))
                    .is_err()
            );
            let mut stale = payload(&meta, HookEvent::PreTool, "stale");
            assert!(store.claim_hook(&stale).is_err());
            stale.session_lifetime_id = SessionLifetimeId("not-found".into());
            assert!(
                store
                    .hook_outcomes(&key, &stale.session_lifetime_id)
                    .is_err()
            );
            let recovered_history = store.hook_outcomes(&key, &meta.lifetime).unwrap();
            assert_eq!(recovered_history[..2], old);
            assert_eq!(recovered_history.len(), 3);
            drop(store);
            if path.to_str() != Some(":memory:") {
                assert_eq!(
                    RunStore::open(&path)
                        .unwrap()
                        .hook_outcomes(&key, &meta.lifetime)
                        .unwrap(),
                    recovered_history
                );
                std::fs::remove_file(path).unwrap();
            }
        }
    }
    #[test]
    fn hook_publication_failure_rolls_back_the_lifecycle_receipt_and_head() {
        let store = RunStore::open(std::path::Path::new(":memory:")).unwrap();
        store.lock_connection().unwrap().execute_batch("CREATE TRIGGER fault BEFORE INSERT ON hook_publications BEGIN SELECT RAISE(ABORT,'publication fault'); END").unwrap();
        let key = SessionKey("fault".into());
        assert!(
            store
                .execute_lifecycle(&SessionCommand::Create { key: key.clone() }, None, "create")
                .is_err()
        );
        assert!(store.session_metadata(&key).unwrap().is_none());
        let receipts: i64 = store
            .lock_connection()
            .unwrap()
            .query_row(
                "SELECT count(*) FROM lifecycle_receipts WHERE operation_id='create'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(receipts, 0);
    }
    #[test]
    fn paged_hook_readback_is_readonly_and_rejects_corrupt_identity_or_effect() {
        for path in [std::path::PathBuf::from(":memory:"), path()] {
            let store = RunStore::open(&path).unwrap();
            let meta = store.create_session(&SessionKey("pages".into())).unwrap();
            for index in 0..35 {
                store
                    .claim_hook(&payload(
                        &meta,
                        HookEvent::Notification,
                        &format!("page-{index}"),
                    ))
                    .unwrap();
            }
            let first = store
                .hook_readback(&meta.key, &meta.lifetime, 0, 1000)
                .unwrap();
            assert_eq!(first.outcomes.len(), 32);
            assert!(first.has_more);
            let second = store
                .hook_readback(&meta.key, &meta.lifetime, first.cursor, 32)
                .unwrap();
            assert_eq!(second.outcomes.len(), 3);
            assert!(!second.has_more);
            assert_eq!(first.snapshot_revision, second.snapshot_revision);
            assert_eq!(first.outcomes[31].payload.operation_id, "page-31");
            assert_eq!(second.outcomes[0].payload.operation_id, "page-32");
            let reread = store
                .hook_readback(&meta.key, &meta.lifetime, 0, 32)
                .unwrap();
            assert_eq!(reread.snapshot_revision, first.snapshot_revision);
            assert_eq!(reread.outcomes, first.outcomes);
            store.lock_connection().unwrap().execute("UPDATE hook_outcomes SET outcome_json=json_set(outcome_json,'$.payload.session_key','wrong') WHERE operation_id='page-0'", []).unwrap();
            assert!(
                store
                    .hook_readback(&meta.key, &meta.lifetime, 0, 32)
                    .is_err()
            );
            store.lock_connection().unwrap().execute("UPDATE hook_outcomes SET outcome_json=json_set(outcome_json,'$.payload.session_key','pages','$.status','succeeded','$.effect',json('{\"effect\":\"continue\",\"prompt\":\"越权\"}')) WHERE operation_id='page-0'", []).unwrap();
            assert!(
                store
                    .hook_readback(&meta.key, &meta.lifetime, 0, 32)
                    .is_err()
            );
            drop(store);
            if path.to_str() != Some(":memory:") {
                std::fs::remove_file(path).unwrap();
            }
        }
    }
    #[test]
    fn continuation_admission_is_atomic_frozen_bounded_and_restart_safe() {
        use crate::TurnRepository;
        for path in [std::path::PathBuf::from(":memory:"), path()] {
            let store = RunStore::open(&path).unwrap();
            let meta = store
                .create_session(&SessionKey("continuation".into()))
                .unwrap();
            let snapshot = RunSnapshot {
                entry_channel: HookChannel::Cli,
                route: None,
                tools: vec![],
                cwd: ".".into(),
                permission_mode: "request_approval".into(),
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
            };
            let admission = RunAdmission {
                plan_execution: None,
                session_key: meta.key.clone(),
                expected_lifetime: Some(meta.lifetime.clone()),
                request_id: RequestId::String("parent".into()),
                input: "完成".into(),
                mode: AdmissionMode::Queue,
            };
            let Admission::New(parent) = store.admit_run(&admission, &snapshot).unwrap() else {
                panic!("new parent")
            };
            store.try_start_queued(&parent.run_id).unwrap();
            let owner = store.run_owner(&parent.run_id).unwrap();
            store
                .commit_turn(&TurnCommit {
                    owner: owner.clone(),
                    status: RunStatus::Completed,
                    content: Some("完成".into()),
                    error: None,
                })
                .unwrap();
            let mut request = payload(&meta, HookEvent::Stop, "stop");
            request.owner = Some(owner.clone());
            store.claim_hook(&request).unwrap();
            store
                .settle_hook(&HookOutcome {
                    payload: request,
                    status: HookStatus::Succeeded,
                    effect: Some(HookEffect::Continue {
                        prompt: "续跑".into(),
                    }),
                    failure: None,
                })
                .unwrap();
            let mut widened = snapshot.clone();
            widened.permission_mode = "auto_approve".into();
            assert!(
                store
                    .admit_hook_continuation(&owner, "stop", "续跑", &widened)
                    .is_err()
            );
            assert!(
                store
                    .admit_hook_continuation(&owner, "stop", "不同输入", &snapshot)
                    .is_err()
            );
            store.lock_connection().unwrap().execute_batch("CREATE TRIGGER continuation_fault BEFORE INSERT ON hook_continuations BEGIN SELECT RAISE(ABORT,'fault'); END").unwrap();
            assert!(
                store
                    .admit_hook_continuation(&owner, "stop", "续跑", &snapshot)
                    .is_err()
            );
            assert!(
                store
                    .hook_continuation_child(&parent.run_id)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                store
                    .lock_connection()
                    .unwrap()
                    .query_row("SELECT count(*) FROM runs", [], |r| r.get::<_, i64>(0))
                    .unwrap(),
                1
            );
            store
                .lock_connection()
                .unwrap()
                .execute_batch("DROP TRIGGER continuation_fault")
                .unwrap();
            let Admission::New(child) = store
                .admit_hook_continuation(&owner, "stop", "续跑", &snapshot)
                .unwrap()
            else {
                panic!("new child")
            };
            assert_ne!(child.run_id, parent.run_id);
            let Admission::Existing(repeated) = store
                .admit_hook_continuation(&owner, "stop", "续跑", &snapshot)
                .unwrap()
            else {
                panic!("same child")
            };
            assert_eq!(child.run_id, repeated.run_id);
            assert!(
                store
                    .admit_hook_continuation(&owner, "other-operation", "续跑", &snapshot)
                    .is_err()
            );
            store.recover().unwrap();
            assert_eq!(
                store.read_run(&child.run_id).unwrap().unwrap().status,
                RunStatus::Queued
            );
            store.try_start_queued(&child.run_id).unwrap();
            let calls = (0..8)
                .map(|index| {
                    (
                        ToolCall {
                            id: format!("call-{index}"),
                            name: "read_file".into(),
                            arguments: serde_json::json!({"path":"a"}),
                        },
                        "read".into(),
                        "digest".into(),
                        true,
                    )
                })
                .collect::<Vec<_>>();
            store.prepare_tool_batch(&child.run_id, 1, &calls).unwrap();
            let mut extra = calls[0].clone();
            extra.0.id = "extra".into();
            assert!(
                store
                    .prepare_tool_batch(&child.run_id, 2, &[extra])
                    .is_err()
            );
            let child_owner = store.run_owner(&child.run_id).unwrap();
            assert!(
                store
                    .admit_hook_continuation(&child_owner, "descendant", "续跑", &snapshot)
                    .is_err()
            );
            store.recover().unwrap();
            assert_eq!(
                store.read_run(&child.run_id).unwrap().unwrap().status,
                RunStatus::UnknownAfterRestart
            );
            assert_eq!(
                store.read_run(&parent.run_id).unwrap().unwrap().status,
                RunStatus::Completed
            );
            assert_eq!(
                store.hook_continuation_child(&parent.run_id).unwrap(),
                Some(child.run_id)
            );
            drop(store);
            if path.to_str() != Some(":memory:") {
                std::fs::remove_file(path).unwrap();
            }
        }
    }
    #[test]
    fn schema15_hook_upgrade_rolls_back_preserves_backup_and_rejects_future() {
        let path = path();
        let store = RunStore::open(&path).unwrap();
        let meta = store.create_session(&SessionKey("legacy".into())).unwrap();
        store.lock_connection().unwrap().execute_batch("DROP TABLE provider_requests; DROP TABLE event_view_stamps; DROP TABLE tool_discoveries; DROP TABLE compact_run_links; DROP TABLE hook_outcomes; DROP TABLE hook_publications; DROP TABLE hook_continuations; DELETE FROM schema_migrations WHERE version>=16; CREATE TRIGGER fault BEFORE INSERT ON schema_migrations WHEN NEW.version=16 BEGIN SELECT RAISE(ABORT,'hook migration fault'); END").unwrap();
        drop(store);
        assert!(RunStore::open(&path).is_err());
        let db = Connection::open(&path).unwrap();
        assert_eq!(
            db.query_row("SELECT MAX(version) FROM schema_migrations", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            15
        );
        assert_eq!(
            db.query_row(
                "SELECT count(*) FROM sqlite_master WHERE name='hook_outcomes'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
        assert!(path.with_extension("v15.backup.sqlite3").exists());
        assert!(path.with_extension("v15.backup.verified.json").exists());
        db.execute_batch("DROP TRIGGER fault").unwrap();
        drop(db);
        let store = RunStore::open(&path).unwrap();
        assert_eq!(
            store.session_metadata(&meta.key).unwrap().unwrap().lifetime,
            meta.lifetime
        );
        store
            .lock_connection()
            .unwrap()
            .execute("INSERT INTO schema_migrations VALUES(21,0)", [])
            .unwrap();
        drop(store);
        assert!(RunStore::open(&path).is_err());
        std::fs::remove_file(path).unwrap();
    }
}
