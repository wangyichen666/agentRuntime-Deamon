//! 原执行事务的异步适配；不持有独立队列或终态。
use crate::{RunStore, RuntimeError, sessions};
use agent_core::*;
use rusqlite::Connection;
use std::sync::Arc;

pub(crate) fn check_execution_in(
    db: &Connection,
    envelope: &EnterpriseOwnerEnvelope,
) -> Result<(), RuntimeError> {
    // 先按实际 run 完整隔离范围，不从 caller 声明的 key 推断归属。
    crate::enterprise::check_run_scope_in(db, &envelope.scope, &envelope.owner.run_id)?;
    let actual = sessions::owner_in(db, &envelope.owner.run_id)?;
    actual
        .fence(&envelope.owner)
        .map_err(|_| AuthorizationError::StaleOwner)?;
    let metadata =
        sessions::metadata_in(db, &actual.session_key)?.ok_or(AuthorizationError::StaleOwner)?;
    if metadata.deleted || metadata.lifetime != actual.session_lifetime_id {
        return Err(AuthorizationError::StaleOwner.into());
    }
    sessions::lifetime_owner_in(db, &actual.session_key, &actual.session_lifetime_id)?;
    // 原 writer proof 与冻结授权复核继续在当前 mutation 事务内执行。
    sessions::fence_in(db, &actual)
}
pub(crate) fn execution_error(error: RuntimeError) -> RepositoryError {
    match error {
        RuntimeError::Protocol(_) => CoordinationError::Conflict.into(),
        error => error.repository_error(),
    }
}
// 只计编码字节，不先为超大请求分配完整 JSON；serializer 达到预算立即停止。
struct JsonBudget(usize);
impl std::io::Write for JsonBudget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.0 {
            return Err(std::io::Error::other("执行请求超出编码预算"));
        }
        self.0 -= bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
pub(crate) fn within_json_budget(value: &impl serde::Serialize, limit: usize) -> bool {
    serde_json::to_writer(JsonBudget(limit), value).is_ok()
}
pub(crate) fn valid_owner(owner: &ExactOwner) -> bool {
    [
        &owner.session_key.0,
        &owner.session_lifetime_id.0,
        &owner.run_id.0,
        &owner.turn_id.0,
    ]
    .iter()
    .all(|value| !value.is_empty() && value.len() <= 1024)
        && owner.run_generation.0 <= i64::MAX as u64
}
#[async_trait::async_trait]
impl agent_storage_ports::RunExecutionRepository for RunStore {
    async fn admit_workload_run(
        &self,
        admission: &RunAdmission,
        snapshot: &RunSnapshot,
        authorization: &AuthorizationRequest,
        verifier: Arc<dyn AuthorizationVerifier>,
    ) -> Result<(Admission, ExactOwner), RepositoryError> {
        if admission.session_key.0.is_empty()
            || admission.session_key.0.len() > 1024
            || admission.input.len() > 65536
            || !within_json_budget(snapshot, 1024 * 1024)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (admission, snapshot, authorization) =
            (admission.clone(), snapshot.clone(), authorization.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .admit_workload_owned(&admission, &snapshot, &authorization, verifier.as_ref())
                .map_err(execution_error)
        })
        .await
    }

    async fn admit_run_authorized(
        &self,
        admission: &RunAdmission,
        snapshot: &RunSnapshot,
        authorization: &AuthorizationRequest,
        verifier: Arc<dyn AuthorizationVerifier>,
    ) -> Result<(Admission, ExactOwner), RepositoryError> {
        if admission.session_key.0.is_empty()
            || admission.session_key.0.len() > 1024
            || admission.input.len() > 65536
            || !within_json_budget(snapshot, 1024 * 1024)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (admission, snapshot, authorization) =
            (admission.clone(), snapshot.clone(), authorization.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .admit_authorized_owned(&admission, &snapshot, &authorization, verifier.as_ref())
                .map_err(execution_error)
        })
        .await
    }
    async fn start_queued_run(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<bool, RepositoryError> {
        if !valid_owner(&envelope.owner) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let envelope = envelope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .start_queued_scoped(&envelope.owner.run_id, Some(&envelope))
                .map_err(execution_error)
        })
        .await
    }
    async fn cancel_queued_run(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<bool, RepositoryError> {
        if !valid_owner(&envelope.owner) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let envelope = envelope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .remove_queued_scoped(&envelope.owner.run_id, Some(&envelope))
                .map_err(execution_error)
        })
        .await
    }
    async fn commit_run_turn(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        commit: &TurnCommit,
    ) -> Result<RunRecord, RepositoryError> {
        if !valid_owner(&envelope.owner)
            || commit.owner != envelope.owner
            || !matches!(
                commit.status,
                RunStatus::Completed
                    | RunStatus::Failed
                    | RunStatus::Cancelled
                    | RunStatus::UnknownAfterRestart
            )
            || !within_json_budget(commit, 4 * 1024 * 1024 - 8192)
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, commit) = (envelope.clone(), commit.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .finish_owned_scoped(
                    &commit.owner,
                    commit.status,
                    commit.content.as_deref(),
                    commit
                        .error
                        .as_ref()
                        .map(|(code, message)| (*code, message.as_str())),
                    Some(&envelope),
                )
                .map_err(execution_error)
        })
        .await
    }
}

#[cfg(test)]
mod commit_cancellation_contract {
    use super::*;
    use agent_storage_ports::RunExecutionRepository;
    use std::sync::{Arc, Condvar, Mutex};
    struct Release(Arc<(Mutex<bool>, Condvar)>);
    impl Drop for Release {
        fn drop(&mut self) {
            let (lock, wake) = &*self.0;
            *lock.lock().unwrap_or_else(|e| e.into_inner()) = true;
            wake.notify_all();
        }
    }
    #[tokio::test]
    async fn execution_committed_turn_survives_cancelled_awaiter_and_keeps_budget_until_worker_returns()
     {
        let root = std::env::temp_dir().join(format!("ma-execution-cancel-{}", std::process::id()));
        let store = RunStore::open(&root.join("runtime.sqlite3")).unwrap();
        let Admission::New(run) = store
            .admit(
                SessionKey("cancel-commit".into()),
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
            owner: sessions::owner_in(&store.lock_connection().unwrap(), &run.run_id).unwrap(),
        };
        let commit = TurnCommit {
            owner: envelope.owner.clone(),
            status: RunStatus::Completed,
            content: Some("committed".into()),
            error: None,
        };
        let before: u64 = store
            .lock_connection()
            .unwrap()
            .query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _release = Release(gate.clone());
        let (committed_tx, committed_rx) = tokio::sync::oneshot::channel();
        let task = {
            let store = store.clone();
            let envelope = envelope.clone();
            let commit = commit.clone();
            let gate = gate.clone();
            tokio::spawn(async move {
                store
                    .sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
                        let result = store
                            .finish_owned_scoped(
                                &commit.owner,
                                commit.status,
                                commit.content.as_deref(),
                                None,
                                Some(&envelope),
                            )
                            .map_err(execution_error)?;
                        committed_tx.send(()).unwrap();
                        let (lock, wake) = &*gate;
                        let mut ready = lock.lock().unwrap();
                        while !*ready {
                            ready = wake.wait(ready).unwrap();
                        }
                        Ok::<_, RepositoryError>(result)
                    })
                    .await
            })
        };
        committed_rx.await.unwrap();
        assert_eq!(store.blocking_budget.available_permits(), 3);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(store.blocking_budget.available_permits(), 3);
        let committed_clock: u64 = store
            .lock_connection()
            .unwrap()
            .query_row("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
            .unwrap();
        assert!(committed_clock > before);
        let repeated = store.commit_run_turn(&envelope, &commit).await.unwrap();
        assert_eq!(repeated.content.as_deref(), Some("committed"));
        {
            let db = store.lock_connection().unwrap();
            assert_eq!(
                db.query_row::<u64, _, _>("SELECT revision FROM snapshot_clock", [], |r| r.get(0))
                    .unwrap(),
                committed_clock
            );
            assert_eq!(
                db.query_row::<u64, _, _>(
                    "SELECT count(*) FROM events WHERE run_id=?1 AND event='terminal'",
                    [&run.run_id.0],
                    |r| r.get(0)
                )
                .unwrap(),
                1
            );
        }
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
