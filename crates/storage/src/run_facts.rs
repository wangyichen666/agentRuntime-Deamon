//! 原 run 的同事务只读投影，不构造运行对象或第二事实。
use crate::{RunStore, RuntimeError, sessions};
use agent_core::*;
use rusqlite::Connection;
const RESPONSE_BYTES: usize = 1024 * 1024 - 1024;

fn check_bytes(db: &Connection, run: &RunId, sql: &str) -> Result<(), RuntimeError> {
    let (rows, bytes): (u64, u64) = db.query_row(sql, [&run.0], |r| Ok((r.get(0)?, r.get(1)?)))?;
    if rows > 1000 || bytes > RESPONSE_BYTES as u64 {
        return Err(CoordinationError::InvalidRequest.into());
    }
    Ok(())
}
fn bounded<T: serde::Serialize>(value: T) -> Result<T, RuntimeError> {
    if serde_json::to_vec(&value)
        .map_err(|_| CoordinationError::Corrupt)?
        .len()
        > RESPONSE_BYTES
    {
        return Err(CoordinationError::InvalidRequest.into());
    }
    Ok(value)
}
fn validate_continuation_in(
    db: &Connection,
    scope: &TenantContext,
    current: &RunId,
    related: Option<RunId>,
) -> Result<Option<RunId>, RuntimeError> {
    if let Some(run) = &related {
        if run == current || run.0.is_empty() || run.0.len() > 1024 {
            return Err(CoordinationError::Corrupt.into());
        }
        if let Err(error) = crate::enterprise::check_run_scope_in(db, scope, run) {
            return Err(match error {
                RuntimeError::Authorization(_) => CoordinationError::Corrupt.into(),
                error => error,
            });
        }
        let owner = sessions::owner_in(db, run)?;
        sessions::lifetime_owner_in(db, &owner.session_key, &owner.session_lifetime_id)?;
    }
    Ok(related)
}
impl RunStore {
    fn scoped_run_read<T>(
        &self,
        scope: &TenantContext,
        run: &RunId,
        read: impl FnOnce(&Connection, &RunId) -> Result<T, RuntimeError>,
    ) -> Result<T, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        crate::enterprise::check_run_scope_in(&tx, scope, run)?;
        let owner = sessions::owner_in(&tx, run)?;
        sessions::lifetime_owner_in(&tx, &owner.session_key, &owner.session_lifetime_id)?;
        let value = read(&tx, run)?;
        tx.commit()?;
        Ok(value)
    }
    fn run_fact_scoped(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<RunFactReadback, RuntimeError> {
        self.scoped_run_read(scope,run,|db,run|{
            check_bytes(db,run,"SELECT count(*),COALESCE(sum(n),0) FROM (SELECT length(CAST(COALESCE(content,'') AS BLOB))+length(CAST(COALESCE(error_message,'') AS BLOB))+length(CAST(request_id_wire_json AS BLOB))+length(CAST(id AS BLOB))+length(CAST(session_id AS BLOB)) AS n FROM runs WHERE id=?1 UNION ALL SELECT length(CAST(snapshot_json AS BLOB)) FROM run_snapshots WHERE run_id=?1 UNION ALL SELECT length(CAST(stamp_json AS BLOB)) FROM event_view_stamps WHERE run_id=?1 AND seq=(SELECT last_seq FROM runs WHERE id=?1) UNION ALL SELECT length(CAST(source_json AS BLOB))+length(CAST(request_json AS BLOB)) FROM compact_run_links WHERE run_id=?1 UNION ALL SELECT length(CAST(child_run_id AS BLOB)) FROM hook_continuations WHERE parent_run_id=?1 UNION ALL SELECT length(CAST(parent_run_id AS BLOB)) FROM hook_continuations WHERE child_run_id=?1)")?;
            let record=crate::read_run_in(db,&run.0)?.ok_or(CoordinationError::Corrupt)?;
            let compact=if record.kind==RunKind::Compact {Some(crate::compact_runs::readback_in(db,run)?.ok_or(CoordinationError::Corrupt)?)}else{None};
            bounded(RunFactReadback{
                view_stamp:crate::views::stamp_in(db,run,record.last_seq)?,
                snapshot:crate::turns::run_snapshot_in(db,run)?,
                continuation_parent_run_id:validate_continuation_in(db,scope,run,crate::hooks::continuation_parent_in(db,run)?)?,
                continuation_run_id:validate_continuation_in(db,scope,run,crate::hooks::continuation_child_in(db,run)?)?,
                run:record,compact,
            })
        })
    }
    fn tools_scoped(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Vec<ToolReceipt>, RuntimeError> {
        self.scoped_run_read(scope,run,|db,run|{
            check_bytes(db,run,"SELECT count(*),COALESCE(sum(length(CAST(COALESCE(receipt_json,'') AS BLOB))+length(CAST(call_id AS BLOB))+length(CAST(name AS BLOB))+length(CAST(status AS BLOB))+length(CAST(effect AS BLOB))+length(CAST(argument_digest AS BLOB))+length(CAST(COALESCE(outcome,'') AS BLOB))+length(CAST(COALESCE(artifact_ref,'') AS BLOB))),0) FROM tool_executions WHERE run_id=?1")?;
            bounded(crate::tool_receipts_in(db,run)?)
        })
    }
    fn provider_scoped(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<RunProviderReadback, RuntimeError> {
        self.scoped_run_read(scope,run,|db,run|{
            check_bytes(db,run,"SELECT count(*),COALESCE(sum(n),0) FROM (SELECT length(CAST(data_json AS BLOB))+length(CAST(attempt_id AS BLOB))+length(CAST(status AS BLOB)) AS n FROM provider_attempts WHERE run_id=?1 UNION ALL SELECT length(CAST(snapshot_json AS BLOB)) FROM run_routes WHERE run_id=?1)")?;
            let attempts=crate::provider_attempts_in(db,run)?;
            let mut usage=ProviderUsage::default();
            for attempt in &attempts { if &attempt.run_id!=run{return Err(CoordinationError::Corrupt.into());}
                if let Some(value)=&attempt.usage{usage.add_attempt(value);} }
            let final_candidate_index=attempts.last().map(|attempt|attempt.candidate_index);
            bounded(RunProviderReadback{route:crate::route_snapshot_in(db,run)?,attempts,usage,final_candidate_index})
        })
    }
}
#[async_trait::async_trait]
impl agent_storage_ports::RunFactRepository for RunStore {
    async fn read_exact_run_owner(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<ExactOwner, CoordinationError> {
        if run.0.is_empty() || run.0.len() > 1024 {
            return Err(CoordinationError::InvalidRequest);
        }
        let (scope, run) = (scope.clone(), run.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable,move|store|{
            store.scoped_run_read(&scope,&run,|db,run|{
                let bytes:u64=db.query_row("SELECT length(CAST(session_id AS BLOB))+length(CAST(lifetime AS BLOB)) FROM runs WHERE id=?1",[&run.0],|r|r.get(0))?;
                if bytes>2048 {return Err(CoordinationError::Corrupt.into());}
                let owner=sessions::owner_in(db,run)?;
                if !crate::execution::valid_owner(&owner) {return Err(CoordinationError::Corrupt.into());}
                Ok(owner)
            }).map_err(crate::readback::read_error)
        }).await
    }

    async fn read_run_fact(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<RunFactReadback, CoordinationError> {
        if run.0.is_empty() || run.0.len() > 1024 {
            return Err(CoordinationError::InvalidRequest);
        }
        let (scope, run) = (scope.clone(), run.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable, move |store| {
            store
                .run_fact_scoped(&scope, &run)
                .map_err(crate::readback::read_error)
        })
        .await
    }
    async fn read_tool_receipts(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Vec<ToolReceipt>, CoordinationError> {
        if run.0.is_empty() || run.0.len() > 1024 {
            return Err(CoordinationError::InvalidRequest);
        }
        let (scope, run) = (scope.clone(), run.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable, move |store| {
            store
                .tools_scoped(&scope, &run)
                .map_err(crate::readback::read_error)
        })
        .await
    }
    async fn read_provider_history(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<RunProviderReadback, CoordinationError> {
        if run.0.is_empty() || run.0.len() > 1024 {
            return Err(CoordinationError::InvalidRequest);
        }
        let (scope, run) = (scope.clone(), run.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable, move |store| {
            store
                .provider_scoped(&scope, &run)
                .map_err(crate::readback::read_error)
        })
        .await
    }
}

#[cfg(test)]
mod async_contract {
    use super::*;
    use agent_storage_ports::RunFactRepository;
    #[tokio::test(flavor = "current_thread")]
    async fn run_fact_readback_yields_while_original_connection_is_occupied() {
        let root = std::env::temp_dir().join(format!("ma-run-fact-adapter-{}", std::process::id()));
        let store = RunStore::open(&root.join("runtime.sqlite3")).unwrap();
        let Admission::New(run) = store
            .admit(SessionKey("facts".into()), RequestId::Number(1), "事实")
            .unwrap()
        else {
            panic!("新准入")
        };
        let occupied = store.clone();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let holder = std::thread::spawn(move || {
            let db = occupied.lock_connection().unwrap();
            db.query_row("SELECT count(*) FROM runs", [], |r| r.get::<_, u64>(0))
                .unwrap();
            ready_tx.send(()).unwrap();
            let _ = release_rx.recv_timeout(std::time::Duration::from_secs(5));
            drop(db);
        });
        ready_rx.await.unwrap();
        let response = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            store.read_run_fact(&TenantContext::local(), &run.run_id),
        )
        .await;
        assert_eq!(
            store.blocking_budget.available_permits(),
            3,
            "被取消的真实查询仍占permit"
        );
        let _ = release_tx.send(());
        holder.join().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while store.blocking_budget.available_permits() != 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            store
                .read_run_fact(&TenantContext::local(), &run.run_id)
                .await
                .is_ok()
        );
        assert!(
            response.is_err(),
            "100ms timer应先结束，不能被同步查询阻塞：{response:?}"
        );
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
}
