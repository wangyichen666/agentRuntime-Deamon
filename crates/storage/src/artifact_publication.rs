//! File 两阶段发布：临时对象完成后，由原工具收据事务发布引用。
use crate::{RunStore, RuntimeError, now_ms, sessions};
use agent_core::{ArtifactError, ArtifactIdentity, ExactOwner, RunId, ToolArtifactCompletion};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::Value;

pub(crate) fn verify_receipt(
    identity: &ArtifactIdentity,
    receipt: &Value,
    required: bool,
) -> Result<(), RuntimeError> {
    agent_storage_ports::verify_artifact_receipt(identity, receipt, required).map_err(Into::into)
}

pub(crate) fn finish_in(
    db: &Connection,
    run_id: &RunId,
    round: usize,
    call_id: &str,
    outcome: &str,
    artifact_ref: Option<&str>,
    receipt: &Value,
) -> Result<(), RuntimeError> {
    let changed = db.execute("UPDATE tool_executions SET status='terminal', finished_at_ms=?4, outcome=?5, artifact_ref=?6, receipt_json=?7 WHERE run_id=?1 AND round=?2 AND call_id=?3 AND status='running'",params![run_id.0,round as i64,call_id,now_ms(),outcome,artifact_ref,receipt.to_string()])?;
    if changed != 1 {
        return Err(RuntimeError::Protocol("工具不是 running 状态".into()));
    }
    let env=crate::billing::envelope_in(db,run_id)?;
    crate::billing::finish_sandbox_in(db,&env,&format!("tool:{round}:{call_id}"),outcome=="outcome_unknown")?;
    Ok(())
}
impl RunStore {
    pub fn finish_owned_tool_output(
        &self,
        owner: &ExactOwner,
        completion: &ToolArtifactCompletion,
        content: &str,
    ) -> Result<String, RuntimeError> {
        {
            let db = self.lock_connection()?;
            sessions::fence_in(&db, owner)?;
        }
        let staged = self.file_artifacts.stage(content)?;
        verify_receipt(&staged.identity, &completion.receipt, true)?;
        self.publish_owned_tool_output(owner, completion, || self.file_artifacts.finalize(staged))
    }
    pub(crate) fn finish_file_tool_output_scoped(
        &self,
        envelope: &agent_core::EnterpriseOwnerEnvelope,
        completion: &ToolArtifactCompletion,
        content: &str,
    ) -> Result<String, RuntimeError> {
        {
            let mut db = self.lock_connection()?;
            let tx = db.transaction()?;
            crate::execution::check_execution_in(&tx, envelope)?;
            tx.commit()?;
        }
        let staged = self.file_artifacts.stage(content)?;
        verify_receipt(&staged.identity, &completion.receipt, true)?;
        self.publish_owned_tool_output_scoped(&envelope.owner, completion, Some(envelope), || {
            self.file_artifacts.finalize(staged)
        })
    }
    fn publish_owned_tool_output(
        &self,
        owner: &ExactOwner,
        completion: &ToolArtifactCompletion,
        finalize: impl FnOnce() -> Result<String, ArtifactError>,
    ) -> Result<String, RuntimeError> {
        self.publish_owned_tool_output_scoped(owner, completion, None, finalize)
    }
    fn publish_owned_tool_output_scoped(
        &self,
        owner: &ExactOwner,
        completion: &ToolArtifactCompletion,
        envelope: Option<&agent_core::EnterpriseOwnerEnvelope>,
        finalize: impl FnOnce() -> Result<String, ArtifactError>,
    ) -> Result<String, RuntimeError> {
        if completion.round > i64::MAX as usize
            || completion.call_id.is_empty()
            || completion.call_id.len() > 1024
            || !matches!(
                completion.outcome.as_str(),
                "success" | "tool_error" | "outcome_unknown"
            )
        {
            return Err(ArtifactError::InvalidReference.into());
        }
        let mut connection = self.lock_connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(envelope) = envelope {
            crate::execution::check_execution_in(&tx, envelope)?;
        }
        sessions::fence_in(&tx, owner)?;
        let prior = tx.query_row("SELECT status,outcome,artifact_ref,receipt_json FROM tool_executions WHERE run_id=?1 AND round=?2 AND call_id=?3",params![owner.run_id.0,completion.round as i64,completion.call_id],|r|Ok((r.get::<_,String>(0)?,r.get::<_,Option<String>>(1)?,r.get::<_,Option<String>>(2)?,r.get::<_,Option<String>>(3)?))).optional()?;
        match prior {
            Some((status, _, _, _)) if status == "running" => {}
            Some((status, Some(outcome), Some(reference), Some(raw)))
                if status == "terminal" && outcome == completion.outcome =>
            {
                let old: Value = serde_json::from_str(&raw).map_err(|_| ArtifactError::Corrupt)?;
                if old != completion.receipt {
                    return Err(ArtifactError::Conflict.into());
                }
                let (identity, _) = self.file_artifacts.read(&reference, 0, 1)?;
                verify_receipt(&identity, &completion.receipt, true)?;
                tx.commit()?;
                return Ok(reference);
            }
            _ => return Err(ArtifactError::Conflict.into()),
        }
        let reference = finalize()?;
        let (identity, _) = self.file_artifacts.read(&reference, 0, 1)?;
        verify_receipt(&identity, &completion.receipt, true)?;
        // finalize 之后、commit 之前任何故障都回滚同一原 tool receipt。
        finish_in(
            &tx,
            &owner.run_id,
            completion.round,
            &completion.call_id,
            &completion.outcome,
            Some(&reference),
            &completion.receipt,
        )?;
        sessions::fence_in(&tx, owner)?;
        tx.commit()?;
        Ok(reference)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArtifactRepository, MaintenanceRepository, SessionQuery};
    use agent_core::*;
    use sha2::{Digest, Sha256};
    use std::sync::{Arc, Barrier, mpsc};

    #[test]
    fn process_kill_after_finalize_rolls_back_reference_and_preserves_original_running_receipt() {
        const CHILD_PATH: &str = "AGENT_TEST_ARTIFACT_CRASH_DATABASE";
        if let Some(path) = std::env::var_os(CHILD_PATH) {
            use std::io::Write as _;
            let store = RunStore::open(std::path::Path::new(&path)).unwrap();
            let owner = store
                .run_owner(&RunId(
                    std::env::var("AGENT_TEST_ARTIFACT_CRASH_RUN").unwrap(),
                ))
                .unwrap();
            let body = "真实子进程崩溃输出";
            let completion = ToolArtifactCompletion {
                round: 1,
                call_id: "crash-call".into(),
                outcome: "success".into(),
                receipt: serde_json::json!({"output_sha256":format!("{:x}",Sha256::digest(body.as_bytes())),"output_bytes":body.len()}),
            };
            let staged = store.file_artifacts.stage(body).unwrap();
            store
                .publish_owned_tool_output(&owner, &completion, || {
                    let _reference = store.file_artifacts.finalize(staged)?;
                    std::io::stdout()
                        .write_all(b"ARTIFACT_FINALIZED\n")
                        .unwrap();
                    std::io::stdout().flush().unwrap();
                    loop {
                        std::thread::park();
                    }
                })
                .unwrap();
            unreachable!();
        }
        let root = std::env::temp_dir().join(format!("ma-artifact-kill-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("runtime.sqlite3");
        let store = RunStore::open(&path).unwrap();
        let Admission::New(run) = store
            .admit(
                SessionKey("crash-artifact".into()),
                RequestId::Number(1),
                "工具输出",
            )
            .unwrap()
        else {
            panic!("新 run")
        };
        store.try_start_queued(&run.run_id).unwrap();
        store
            .prepare_tool_batch(
                &run.run_id,
                1,
                &[(
                    ToolCall {
                        id: "crash-call".into(),
                        name: "read".into(),
                        arguments: serde_json::json!({}),
                    },
                    "read_only".into(),
                    "digest".into(),
                    false,
                )],
            )
            .unwrap();
        store.start_tool(&run.run_id, 1, "crash-call").unwrap();
        let original = serde_json::to_value(store.tool_receipts(&run.run_id).unwrap()).unwrap();
        drop(store);
        let mut child = std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact","artifact_publication::tests::process_kill_after_finalize_rolls_back_reference_and_preserves_original_running_receipt","--nocapture","--test-threads=1"]).env(CHILD_PATH,&path).env("AGENT_TEST_ARTIFACT_CRASH_RUN",&run.run_id.0).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sent, received) = mpsc::sync_channel(1);
        let reader = std::thread::spawn(move || {
            use std::io::BufRead as _;
            for line in std::io::BufReader::new(stdout).lines() {
                if line.unwrap().contains("ARTIFACT_FINALIZED") {
                    sent.send(()).unwrap();
                    return;
                }
            }
        });
        let observed = received.recv_timeout(std::time::Duration::from_secs(10));
        child.kill().unwrap();
        let killed = child.wait().unwrap();
        reader.join().unwrap();
        assert!(
            observed.is_ok(),
            "必须在真实 finalize 后、原 receipt commit 前杀子进程"
        );
        assert!(!killed.success());
        let store = RunStore::open(&path).unwrap();
        assert_eq!(
            serde_json::to_value(store.tool_receipts(&run.run_id).unwrap()).unwrap(),
            original
        );
        assert_eq!(
            std::fs::read_dir(path.with_extension("artifacts"))
                .unwrap()
                .count(),
            1
        );
        assert_eq!(store.gc_artifacts(std::time::Duration::ZERO).unwrap(), 1);
        assert_eq!(
            store.tool_receipts(&run.run_id).unwrap()[0].status,
            "running"
        );
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn second_database_connection_gc_cannot_cross_reference_publication_transaction() {
        let root = std::env::temp_dir().join(format!("ma-artifact-tx-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("runtime.sqlite3");
        let store = Arc::new(RunStore::open(&path).unwrap());
        let Admission::New(run) = store
            .admit(SessionKey("gc-race".into()), RequestId::Number(1), "输出")
            .unwrap()
        else {
            panic!("新 run")
        };
        store.try_start_queued(&run.run_id).unwrap();
        store
            .prepare_tool_batch(
                &run.run_id,
                1,
                &[(
                    ToolCall {
                        id: "call".into(),
                        name: "read".into(),
                        arguments: serde_json::json!({}),
                    },
                    "read_only".into(),
                    "digest".into(),
                    false,
                )],
            )
            .unwrap();
        store.start_tool(&run.run_id, 1, "call").unwrap();
        let owner = store.run_owner(&run.run_id).unwrap();
        let body = "GC 不能越过引用发布";
        let completion = ToolArtifactCompletion {
            round: 1,
            call_id: "call".into(),
            outcome: "success".into(),
            receipt: serde_json::json!({"output_sha256":format!("{:x}",Sha256::digest(body.as_bytes())),"output_bytes":body.len()}),
        };
        let collector = RunStore::open(&path).unwrap();
        let gate = Arc::new(Barrier::new(2));
        let (sent, received) = mpsc::sync_channel(1);
        let writer = store.clone();
        let writer_owner = owner.clone();
        let writer_gate = gate.clone();
        let publication = std::thread::spawn(move || {
            let staged = writer.file_artifacts.stage(body).unwrap();
            writer.publish_owned_tool_output(&writer_owner, &completion, || {
                let reference = writer.file_artifacts.finalize(staged)?;
                sent.send(reference.clone()).unwrap();
                writer_gate.wait();
                Ok(reference)
            })
        });
        let reference = received.recv().unwrap();
        let probe = Connection::open(&path).unwrap();
        probe.busy_timeout(std::time::Duration::ZERO).unwrap();
        let excluded = matches!(probe.execute_batch("BEGIN IMMEDIATE"),Err(rusqlite::Error::SqliteFailure(error,_)) if error.code==rusqlite::ErrorCode::DatabaseBusy);
        let (started, ready) = mpsc::sync_channel(1);
        let gc = std::thread::spawn(move || {
            started.send(()).unwrap();
            collector.gc_artifacts(std::time::Duration::ZERO)
        });
        ready.recv().unwrap();
        gate.wait();
        assert!(
            excluded,
            "跨连接真实数据库写锁必须覆盖 finalize 到引用 commit"
        );
        assert_eq!(publication.join().unwrap().unwrap(), reference);
        assert_eq!(gc.join().unwrap().unwrap(), 0);
        assert_eq!(
            store
                .read_run_artifact(&owner, &reference, 0, 16384)
                .unwrap(),
            body.as_bytes()
        );
        drop(probe);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
}
