use super::{
    RunStore, RuntimeError,
    sessions::{fence_in, metadata_in},
};
use agent_core::*;
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};

pub trait ContextRepository: Send + Sync {
    fn compact_result(
        &self,
        owner: &ExactOwner,
        operation: &str,
        expected_revision: Option<TranscriptSeq>,
    ) -> Result<Option<ProjectionGeneration>, RuntimeError>;
    fn context_anchor(
        &self,
        owner: &ExactOwner,
        generation: ProjectionGeneration,
        route: &str,
        provider_identity: &str,
    ) -> Result<Option<(u64, u64)>, RuntimeError>;
    fn context_projection(
        &self,
        snapshot: &SessionSnapshot,
    ) -> Result<ContextProjection, RuntimeError>;
    fn begin_compact(&self, intent: &CompactIntent) -> Result<(), RuntimeError>;
    fn settle_compact(
        &self,
        intent: &CompactIntent,
        replacement: Option<&[Message]>,
        reason: &str,
    ) -> Result<ProjectionGeneration, RuntimeError>;
    fn record_context(
        &self,
        owner: &ExactOwner,
        round: usize,
        envelope: &ContextEnvelope,
    ) -> Result<(), RuntimeError>;
}
fn source_check(
    db: &Connection,
    key: &SessionKey,
    source: &ContextSource,
) -> Result<(), RuntimeError> {
    let meta = metadata_in(db, key)?
        .ok_or_else(|| RuntimeError::Protocol("compact session 不存在".into()))?;
    let generation: u64 = db.query_row(
        "SELECT projection_generation FROM session_heads WHERE session_id=?1",
        params![key.0],
        |r| r.get(0),
    )?;
    if source.source_start.0 != 0 {
        return Err(RuntimeError::Protocol(
            "compact 源必须从 canonical 起点开始".into(),
        ));
    }
    if meta.deleted
        || meta.lifetime != source.lifetime
        || meta.revision.0 < source.source_end.0
        || generation != source.generation.0
    {
        return Err(RuntimeError::Protocol("compact source stale".into()));
    }
    let mut q=db.prepare("SELECT messages_json,end_seq FROM transcript_batches WHERE lifetime=?1 AND start_seq<?2 ORDER BY start_seq,rowid")?;
    let rows = q
        .query_map(params![source.lifetime.0, source.source_end.0], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, u64>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut messages = Vec::new();
    for (raw, end) in rows {
        if end > source.source_end.0 {
            return Err(RuntimeError::Protocol("compact 拆分 batch".into()));
        }
        messages.extend(
            serde_json::from_str::<Vec<Message>>(&raw)
                .map_err(|e| RuntimeError::Protocol(e.to_string()))?,
        );
    }
    let digest = format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&messages).map_err(|e| RuntimeError::Protocol(e.to_string()))?
        )
    );
    if messages.len() as u64 != source.source_end.0 || digest != source.prefix_digest {
        return Err(RuntimeError::Protocol("compact prefix 已变化".into()));
    }
    Ok(())
}
impl ContextRepository for RunStore {
    fn compact_result(
        &self,
        owner: &ExactOwner,
        operation: &str,
        expected_revision: Option<TranscriptSeq>,
    ) -> Result<Option<ProjectionGeneration>, RuntimeError> {
        let db = self.lock_connection()?;
        fence_in(&db, owner)?;
        let record:Option<(String,Option<u64>,String)>=db.query_row("SELECT run_id,result_generation,source_json FROM compact_operations WHERE operation=?1 AND state IN ('committed','rejected')",params![operation],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        record
            .map(|(run, generation, source_json)| {
                let source: ContextSource = serde_json::from_str(&source_json)
                    .map_err(|e| RuntimeError::Protocol(e.to_string()))?;
                if expected_revision.is_some_and(|revision| revision != source.source_end) {
                    return Err(RuntimeError::Protocol(
                        "compact operation source 冲突".into(),
                    ));
                }
                if run != owner.run_id.0 {
                    return Err(RuntimeError::Protocol(
                        "compact operation owner 冲突".into(),
                    ));
                }
                generation
                    .map(ProjectionGeneration)
                    .ok_or_else(|| RuntimeError::Protocol("compact 未取得成功回执".into()))
            })
            .transpose()
    }
    fn context_anchor(
        &self,
        owner: &ExactOwner,
        generation: ProjectionGeneration,
        route: &str,
        provider_identity: &str,
    ) -> Result<Option<(u64, u64)>, RuntimeError> {
        let db = self.lock_connection()?;
        fence_in(&db, owner)?;
        Ok(db.query_row("SELECT json_extract(a.data_json,'$.usage.input_tokens'),json_extract(l.envelope_json,'$.stable_tokens')+json_extract(l.envelope_json,'$.history_tokens')+json_extract(l.envelope_json,'$.retrieved_tokens')+json_extract(l.envelope_json,'$.overlay_tokens') FROM provider_attempts a JOIN runs r ON r.id=a.run_id JOIN context_ledgers l ON l.run_id=a.run_id AND l.round=a.round WHERE r.lifetime=?1 AND r.status='completed' AND a.status='succeeded' AND json_extract(l.envelope_json,'$.provider_identity')=?4 AND json_extract(l.envelope_json,'$.route')=?2 AND json_extract(a.data_json,'$.model')=?2 AND json_extract(l.envelope_json,'$.source.generation')=?3 AND json_extract(a.data_json,'$.usage.input_tokens')>0 ORDER BY r.rowid DESC,a.round DESC LIMIT 1",params![owner.session_lifetime_id.0,route,generation.0,provider_identity],|r|Ok((r.get(0)?,r.get(1)?))).optional()?)
    }
    fn context_projection(
        &self,
        snapshot: &SessionSnapshot,
    ) -> Result<ContextProjection, RuntimeError> {
        let db = self.lock_connection()?;
        let meta = metadata_in(&db, &snapshot.session_key)?
            .ok_or_else(|| RuntimeError::Protocol("session 不存在".into()))?;
        if meta.deleted || meta.lifetime != snapshot.lifetime {
            return Err(RuntimeError::Protocol("projection stale lifetime".into()));
        }
        let head: Option<(u64, u64, String)> = db
            .query_row(
                "SELECT source_end,generation,messages_json FROM context_heads WHERE lifetime=?1",
                params![snapshot.lifetime.0],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        if let Some((end, generation, raw)) = head {
            if end > snapshot.revision.0 || generation != snapshot.projection_generation.0 {
                return Err(RuntimeError::Protocol("projection source stale".into()));
            }
            let mut messages: Vec<Message> =
                serde_json::from_str(&raw).map_err(|e| RuntimeError::Protocol(e.to_string()))?;
            messages.extend_from_slice(&snapshot.messages[end as usize..]);
            Ok(ContextProjection {
                source_end: snapshot.revision,
                generation: ProjectionGeneration(generation),
                messages,
            })
        } else {
            Ok(ContextProjection {
                source_end: snapshot.revision,
                generation: snapshot.projection_generation,
                messages: snapshot.messages.clone(),
            })
        }
    }
    fn begin_compact(&self, intent: &CompactIntent) -> Result<(), RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        fence_in(&tx, &intent.owner)?;
        source_check(&tx, &intent.owner.session_key, &intent.source)?;
        let read_only: Option<String> = tx
            .query_row(
                "SELECT snapshot_json FROM run_snapshots WHERE run_id=?1",
                params![intent.owner.run_id.0],
                |r| r.get(0),
            )
            .optional()?;
        let frozen = read_only
            .map(|raw| serde_json::from_str::<RunSnapshot>(&raw))
            .transpose()
            .map_err(|e| RuntimeError::Protocol(e.to_string()))?;
        if frozen
            .as_ref()
            .and_then(|s| s.context_policy_fingerprint.as_ref())
            .is_some_and(|f| f != &intent.source.policy_fingerprint)
        {
            return Err(RuntimeError::Protocol(
                "compact policy 与冻结快照不匹配".into(),
            ));
        }
        if frozen.is_some_and(|s| s.context_read_only) {
            return Err(RuntimeError::Protocol(
                "context_read_only 禁止 compact".into(),
            ));
        }
        tx.execute(
            "INSERT INTO compact_operations(operation,run_id,lifetime,source_json,state,reason) VALUES(?1,?2,?3,?4,'pending',NULL)",
            params![
                intent.operation,
                intent.owner.run_id.0,
                intent.source.lifetime.0,
                serde_json::to_string(&intent.source)
                    .map_err(|e| RuntimeError::Protocol(e.to_string()))?
            ],
        )?;
        tx.commit()?;
        Ok(())
    }
    fn settle_compact(
        &self,
        intent: &CompactIntent,
        replacement: Option<&[Message]>,
        reason: &str,
    ) -> Result<ProjectionGeneration, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        let (run, raw, state): (String, String, String) = tx.query_row(
            "SELECT run_id,source_json,state FROM compact_operations WHERE operation=?1",
            params![intent.operation],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        if run != intent.owner.run_id.0
            || raw
                != serde_json::to_string(&intent.source)
                    .map_err(|e| RuntimeError::Protocol(e.to_string()))?
            || state != "pending"
        {
            return Err(RuntimeError::Protocol(
                "compact intent 不匹配或已结算".into(),
            ));
        }
        let check = fence_in(&tx, &intent.owner)
            .and_then(|_| source_check(&tx, &intent.owner.session_key, &intent.source));
        if check.is_err() || replacement.is_none() {
            tx.execute(
                "UPDATE compact_operations SET state='rejected',reason=?2,result_generation=?3 WHERE operation=?1",
                params![intent.operation, reason,if check.is_ok(){Some(intent.source.generation.0)}else{None}],
            )?;
            tx.commit()?;
            check?;
            return Ok(intent.source.generation);
        }
        let next = intent
            .source
            .generation
            .0
            .checked_add(1)
            .filter(|v| *v <= i64::MAX as u64)
            .ok_or_else(|| RuntimeError::Protocol("projection generation 耗尽".into()))?;
        let candidate =
            replacement.ok_or_else(|| RuntimeError::Protocol("缺少 compact 候选".into()))?;
        if candidate.is_empty()
            || candidate
                .iter()
                .all(|m| m.content.as_deref().is_none_or(|s| s.trim().is_empty()))
        {
            return Err(RuntimeError::Protocol("空 compact 候选".into()));
        }
        let mut pending = Vec::new();
        for message in candidate {
            if message.role == Role::Tool {
                if pending.first().copied() != message.tool_call_id.as_deref() {
                    return Err(RuntimeError::Protocol("compact tool pair 不完整".into()));
                }
                pending.remove(0);
            } else {
                if !pending.is_empty() {
                    return Err(RuntimeError::Protocol("compact tool pair 不完整".into()));
                }
                pending = message.tool_calls.iter().map(|c| c.id.as_str()).collect();
            }
        }
        if !pending.is_empty() {
            return Err(RuntimeError::Protocol("compact tool pair 未闭合".into()));
        }
        let payload = serde_json::to_string(&replacement)
            .map_err(|e| RuntimeError::Protocol(e.to_string()))?;
        tx.execute("INSERT INTO context_heads VALUES(?1,?2,?3,?4) ON CONFLICT(lifetime) DO UPDATE SET source_end=excluded.source_end,generation=excluded.generation,messages_json=excluded.messages_json",params![intent.source.lifetime.0,intent.source.source_end.0,next,payload])?;
        tx.execute(
            "UPDATE session_heads SET projection_generation=?2 WHERE session_id=?1",
            params![intent.owner.session_key.0, next],
        )?;
        tx.execute(
            "UPDATE compact_operations SET state='committed',reason=?2,result_generation=?3,candidate_digest=?4 WHERE operation=?1",
            params![intent.operation, reason,next,format!("{:x}",Sha256::digest(payload.as_bytes()))],
        )?;
        super::insert_event(
            &tx,
            &intent.owner.run_id,
            "context_compacted",
            &serde_json::json!({"generation":next,"operation":intent.operation,"mode":reason}),
        )?;
        tx.commit()?;
        Ok(ProjectionGeneration(next))
    }
    fn record_context(
        &self,
        owner: &ExactOwner,
        round: usize,
        envelope: &ContextEnvelope,
    ) -> Result<(), RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        fence_in(&tx, owner)?;
        tx.execute("INSERT INTO context_ledgers VALUES(?1,?2,?3) ON CONFLICT(run_id,round) DO UPDATE SET envelope_json=excluded.envelope_json",params![owner.run_id.0,round as i64,serde_json::to_string(envelope).map_err(|e|RuntimeError::Protocol(e.to_string()))?])?;
        tx.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SessionLifecycle, SessionQuery, TranscriptStore};
    fn fixture(path: &std::path::Path) -> (RunStore, ExactOwner) {
        let store = RunStore::open(path).unwrap();
        let key = SessionKey("compact-cas".into());
        store.create_session(&key).unwrap();
        let Admission::New(run) = store
            .admit_with_route(
                key,
                RequestId::Number(1),
                &"source".repeat(300),
                AdmissionMode::Queue,
                None,
            )
            .unwrap()
        else {
            panic!("new")
        };
        store.try_start_queued(&run.run_id).unwrap();
        let owner = store.run_owner(&run.run_id).unwrap();
        (store, owner)
    }
    fn intent(store: &RunStore, owner: &ExactOwner, operation: &str) -> CompactIntent {
        let snapshot = store.session_snapshot(&owner.session_key).unwrap();
        CompactIntent {
            operation: operation.into(),
            owner: owner.clone(),
            source: ContextSource {
                lifetime: snapshot.lifetime,
                source_start: TranscriptSeq(0),
                pressure_route: "test".into(),
                summary_route: "test".into(),
                source_end: snapshot.revision,
                generation: snapshot.projection_generation,
                prefix_digest: format!(
                    "{:x}",
                    Sha256::digest(serde_json::to_vec(&snapshot.messages).unwrap())
                ),
                policy_fingerprint: "test".into(),
            },
        }
    }
    #[test]
    fn compact_cas_preserves_concurrent_suffix_and_rejects_competing_head() {
        let (store, owner) = fixture(std::path::Path::new(":memory:"));
        let first = intent(&store, &owner, "first");
        let second = intent(&store, &owner, "second");
        store.begin_compact(&first).unwrap();
        store.begin_compact(&second).unwrap();
        store
            .append_transcript(
                &owner,
                "suffix",
                &[Message::text(Role::User, "fresh suffix")],
            )
            .unwrap();
        store
            .settle_compact(
                &first,
                Some(&[Message::text(Role::System, "checkpoint")]),
                "summary",
            )
            .unwrap();
        assert!(
            store
                .settle_compact(
                    &second,
                    Some(&[Message::text(Role::System, "stale")]),
                    "summary"
                )
                .is_err()
        );
        let snapshot = store.session_snapshot(&owner.session_key).unwrap();
        assert_eq!(snapshot.messages.len(), 2);
        let projection = store.context_projection(&snapshot).unwrap();
        assert_eq!(projection.generation.0, 1);
        assert_eq!(
            projection.messages[1].content.as_deref(),
            Some("fresh suffix")
        );
        let third = intent(&store, &owner, "third");
        store.begin_compact(&third).unwrap();
        store
            .finish(&owner.run_id, RunStatus::Completed, Some("answer"), None)
            .unwrap();
        store
            .end_session(&owner.session_key, &owner.session_lifetime_id, true)
            .unwrap();
        store.create_session(&owner.session_key).unwrap();
        assert!(
            store
                .settle_compact(
                    &third,
                    Some(&[Message::text(Role::System, "late")]),
                    "summary"
                )
                .is_err()
        );
        assert!(
            store
                .session_snapshot(&owner.session_key)
                .unwrap()
                .messages
                .is_empty()
        );
    }
    #[test]
    fn restart_keeps_projection_and_settles_uncommitted_intent() {
        let dir = std::env::temp_dir().join(format!(
            "context-restart-{}-{}",
            std::process::id(),
            super::super::now_ms()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("runtime.sqlite3");
        let (store, owner) = fixture(&path);
        let first = intent(&store, &owner, "committed");
        store.begin_compact(&first).unwrap();
        store
            .settle_compact(
                &first,
                Some(&[Message::text(Role::System, "checkpoint")]),
                "summary",
            )
            .unwrap();
        let pending = intent(&store, &owner, "pending");
        store.begin_compact(&pending).unwrap();
        drop(store);
        let store = RunStore::open(&path).unwrap();
        store.recover().unwrap();
        let snapshot = store.session_snapshot(&owner.session_key).unwrap();
        assert_eq!(store.context_projection(&snapshot).unwrap().generation.0, 1);
        assert!(
            store
                .settle_compact(
                    &pending,
                    Some(&[Message::text(Role::System, "late")]),
                    "summary"
                )
                .is_err()
        );
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn usage_anchor_requires_committed_turn_exact_provider_and_projection_generation() {
        let dir = std::env::temp_dir().join(format!(
            "context-ledger-{}-{}",
            std::process::id(),
            super::super::now_ms()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("runtime.sqlite3");
        let (store, owner) = fixture(&path);
        let source = intent(&store, &owner, "ledger").source;
        let envelope = ContextEnvelope {
            source,
            route: "m".into(),
            provider_identity: "provider-identity".into(),
            tool_catalog_digest: "catalog".into(),
            stable_tokens: 10,
            history_tokens: 20,
            retrieved_tokens: 5,
            overlay_tokens: 5,
            calibrated_input_tokens: 40,
            output_reserve: 100,
            budget: 1000,
        };
        store.record_context(&owner, 1, &envelope).unwrap();
        let mut attempt = ProviderAttempt {
            attempt_id: "anchor-attempt".into(),
            run_id: owner.run_id.clone(),
            round: 1,
            candidate_index: 0,
            provider_profile_id: "p".into(),
            api_type: ApiType::OpenaiChat,
            model: "m".into(),
            status: AttemptStatus::Started,
            error_kind: None,
            diagnostic: None,
            retry_after_ms: None,
            stream_committed: false,
            started_at_ms: 0,
            first_event_at_ms: None,
            finished_at_ms: None,
            usage: None,
        };
        store.start_provider_attempt(&attempt).unwrap();
        attempt.status = AttemptStatus::Succeeded;
        attempt.usage = Some(ProviderUsage {
            input_tokens: Some(70),
            ..Default::default()
        });
        store.finish_provider_attempt(&attempt).unwrap();
        assert!(
            store
                .context_anchor(&owner, ProjectionGeneration(0), "m", "provider-identity")
                .unwrap()
                .is_none()
        );
        store
            .finish(&owner.run_id, RunStatus::Completed, Some("done"), None)
            .unwrap();
        drop(store);
        let store = RunStore::open(&path).unwrap();
        assert_eq!(
            store
                .context_anchor(&owner, ProjectionGeneration(0), "m", "provider-identity")
                .unwrap(),
            Some((70, 40))
        );
        assert!(
            store
                .context_anchor(&owner, ProjectionGeneration(1), "m", "provider-identity")
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .context_anchor(&owner, ProjectionGeneration(0), "m", "other-provider")
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .context_anchor(
                    &owner,
                    ProjectionGeneration(0),
                    "other-model",
                    "provider-identity"
                )
                .unwrap()
                .is_none()
        );
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
