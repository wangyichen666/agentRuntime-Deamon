//! 原工具事务的异步适配，沿用唯一四 permit 和 canonical Connection。
use crate::{RunStore, RuntimeError, execution};
use agent_core::*;
use sha2::{Digest, Sha256};
fn valid(envelope: &EnterpriseOwnerEnvelope, round: usize) -> bool {
    execution::valid_owner(&envelope.owner) && round > 0 && round <= i64::MAX as usize
}
fn call_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 1024
}
impl RunStore {
    fn tool_artifact_binding_scoped(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<Option<String>, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        execution::check_execution_in(&tx, envelope)?;
        use rusqlite::OptionalExtension;
        let bytes: Option<u64> = tx
            .query_row(
                "SELECT length(CAST(snapshot_json AS BLOB)) FROM run_snapshots WHERE run_id=?1",
                [&envelope.owner.run_id.0],
                |r| r.get(0),
            )
            .optional()?;
        if bytes.is_some_and(|n| n > 1024 * 1024) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let binding = crate::turns::run_snapshot_in(&tx, &envelope.owner.run_id)?
            .and_then(|s| s.artifact_store_id);
        if binding.as_ref().is_some_and(|id| {
            !ArtifactIdentity {
                sha256: id.clone(),
                bytes: 0,
            }
            .valid()
        }) {
            return Err(CoordinationError::Corrupt.into());
        }
        tx.commit()?;
        Ok(binding)
    }
}
#[async_trait::async_trait]
impl agent_storage_ports::RunToolRepository for RunStore {
    async fn prepare_run_tool_batch(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        round: usize,
        calls: &[(ToolCall, String, String, bool)],
        descriptors: &[ToolDescriptor],
    ) -> Result<(), RepositoryError> {
        if !valid(envelope, round)
            || calls.is_empty()
            || calls.len() > 256
            || calls.len() != descriptors.len()
            || !execution::within_json_budget(&calls, 1024 * 1024)
            || !execution::within_json_budget(&descriptors, 1024 * 1024)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let mut seen = std::collections::BTreeSet::new();
        for ((call, effect, digest, safe), descriptor) in calls.iter().zip(descriptors) {
            let actual_effect = match descriptor.effect {
                ToolEffect::Read => "read",
                ToolEffect::Write => "write",
                ToolEffect::External => "external_side_effect",
            };
            if !call_id(&call.id)
                || !call_id(&call.name)
                || !seen.insert(&call.id)
                || effect != actual_effect
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
        let (envelope, calls, descriptors) =
            (envelope.clone(), calls.to_vec(), descriptors.to_vec());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .prepare_batch_inner(
                    &envelope.owner.run_id,
                    round,
                    &calls,
                    Some((&envelope.owner, &descriptors)),
                    Some(&envelope),
                )
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn start_run_tool(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        round: usize,
        id: &str,
    ) -> Result<(), RepositoryError> {
        if !valid(envelope, round) || !call_id(id) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, id) = (envelope.clone(), id.to_owned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .start_tool_scoped(&envelope.owner.run_id, round, &id, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn close_run_tool_exchange(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        round: usize,
        messages: &[Message],
    ) -> Result<(), RepositoryError> {
        if !valid(envelope, round)
            || messages.is_empty()
            || messages.len() > 257
            || !execution::within_json_budget(&messages, 4 * 1024 * 1024)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, messages) = (envelope.clone(), messages.to_vec());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .close_tool_exchange_scoped(&envelope.owner, round, &messages, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn finish_run_tool_batch(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        round: usize,
    ) -> Result<(), RepositoryError> {
        if !valid(envelope, round) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let envelope = envelope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .finish_tool_batch_scoped(&envelope.owner.run_id, round, Some(&envelope))
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn finish_file_tool_output(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        completion: &ToolArtifactCompletion,
        content: String,
    ) -> Result<String, RepositoryError> {
        if !valid(envelope, completion.round)
            || !call_id(&completion.call_id)
            || content.len() as u64 > MAX_ARTIFACT_BYTES
            || !matches!(
                completion.outcome.as_str(),
                "success" | "tool_error" | "outcome_unknown"
            )
            || !completion.receipt.is_object()
            || !execution::within_json_budget(completion, 1024 * 1024)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, completion) = (envelope.clone(), completion.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .finish_file_tool_output_scoped(&envelope, &completion, &content)
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn read_tool_artifact_binding(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<Option<String>, RepositoryError> {
        if !execution::valid_owner(&envelope.owner) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let envelope = envelope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .tool_artifact_binding_scoped(&envelope)
                .map_err(RuntimeError::repository_error)
        })
        .await
    }
}

#[cfg(test)]
mod commit_cancellation_contract {
    use super::*;
    use agent_storage_ports::RunToolRepository;
    use std::sync::{Arc, Condvar, Mutex};

    struct Release(Arc<(Mutex<bool>, Condvar)>);
    impl Drop for Release {
        fn drop(&mut self) {
            let (lock, wake) = &*self.0;
            *lock.lock().unwrap_or_else(|error| error.into_inner()) = true;
            wake.notify_all();
        }
    }

    #[tokio::test]
    async fn committed_file_receipt_survives_cancelled_awaiter_without_releasing_worker_budget() {
        let root = std::env::temp_dir().join(format!("ma-tool-cancel-{}", std::process::id()));
        let store = RunStore::open(&root.join("runtime.sqlite3")).unwrap();
        let Admission::New(run) = store
            .admit(
                SessionKey("tool-cancel".into()),
                RequestId::Number(1),
                "input",
            )
            .unwrap()
        else {
            panic!("new")
        };
        store.try_start_queued(&run.run_id).unwrap();
        let envelope = EnterpriseOwnerEnvelope {
            scope: TenantContext::local(),
            owner: crate::sessions::owner_in(&store.lock_connection().unwrap(), &run.run_id)
                .unwrap(),
        };
        let call = ToolCall {
            id: "cancel-call".into(),
            name: "effect".into(),
            arguments: serde_json::json!({}),
        };
        store
            .prepare_run_tool_batch(
                &envelope,
                1,
                &[(
                    call,
                    "external_side_effect".into(),
                    format!("{:x}", Sha256::digest(b"{}")),
                    false,
                )],
                &[ToolDescriptor::external()],
            )
            .await
            .unwrap();
        store
            .start_run_tool(&envelope, 1, "cancel-call")
            .await
            .unwrap();
        let content = "已经落盘的工具结果";
        let completion = ToolArtifactCompletion {
            round: 1,
            call_id: "cancel-call".into(),
            outcome: "success".into(),
            receipt: serde_json::json!({
                "output_sha256": format!("{:x}", Sha256::digest(content.as_bytes())),
                "output_bytes": content.len()
            }),
        };
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _release = Release(gate.clone());
        let (committed_tx, committed_rx) = tokio::sync::oneshot::channel();
        let task = {
            let (store, envelope, completion, gate) = (
                store.clone(),
                envelope.clone(),
                completion.clone(),
                gate.clone(),
            );
            tokio::spawn(async move {
                store
                    .sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
                        let reference = store
                            .finish_file_tool_output_scoped(&envelope, &completion, content)
                            .map_err(execution::execution_error)?;
                        committed_tx.send(reference.clone()).unwrap();
                        let (lock, wake) = &*gate;
                        let mut ready = lock.lock().unwrap();
                        while !*ready {
                            ready = wake.wait(ready).unwrap();
                        }
                        Ok::<_, RepositoryError>(reference)
                    })
                    .await
            })
        };
        let committed_reference = committed_rx.await.unwrap();
        assert_eq!(store.blocking_budget.available_permits(), 3);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(store.blocking_budget.available_permits(), 3);
        let before = {
            let db = store.lock_connection().unwrap();
            let receipt: (String, String, String) = db.query_row(
                "SELECT status,artifact_ref,receipt_json FROM tool_executions WHERE call_id='cancel-call'",
                [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            ).unwrap();
            assert_eq!(receipt.0, "terminal");
            assert_eq!(receipt.1, committed_reference);
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&receipt.2).unwrap(),
                completion.receipt
            );
            db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |row| row.get(0))
                .unwrap()
        };
        assert_eq!(
            store
                .finish_file_tool_output(&envelope, &completion, content.into())
                .await
                .unwrap(),
            committed_reference
        );
        assert_eq!(
            store
                .file_artifacts
                .read(&committed_reference, 0, 1024)
                .unwrap()
                .1,
            content.as_bytes()
        );
        assert_eq!(
            store
                .lock_connection()
                .unwrap()
                .query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |row| row.get(0))
                .unwrap(),
            before
        );
        {
            let (lock, wake) = &*gate;
            *lock.lock().unwrap() = true;
            wake.notify_all();
        }
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while store.blocking_budget.available_permits() != 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
}
