//! 原Hook续跑准入与关系的同连接异步适配。
use crate::{RunStore, RuntimeError, enterprise, execution};
use agent_core::*;
use agent_storage_ports::HookContinuationRepository;
use rusqlite::OptionalExtension;
fn valid_input(parent: &ExactOwner, operation: &str, prompt: &str, snapshot: &RunSnapshot) -> bool {
    execution::valid_owner(parent)
        && !operation.is_empty()
        && operation.len() <= 256
        && !prompt.trim().is_empty()
        && prompt.len() <= 4096
        && execution::within_json_budget(snapshot, 1024 * 1024)
}
impl RunStore {
    pub(crate) fn admit_hook_continuation_scoped(
        &self,
        parent: &ExactOwner,
        operation: &str,
        prompt: &str,
        snapshot: &RunSnapshot,
        envelope: Option<&EnterpriseOwnerEnvelope>,
    ) -> Result<Admission, RuntimeError> {
        if !valid_input(parent, operation, prompt, snapshot) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let mut snapshot = snapshot.clone();
        snapshot.entry_channel = HookChannel::Continuation;
        // 执行指令仅属于原工作流节点，派生 run 保留冻结授权但不重复执行父节点。
        snapshot.workflow = None;
        let admission = RunAdmission {
            workflow_node: None,
            plan_execution: None,
            session_key: parent.session_key.clone(),
            expected_lifetime: Some(parent.session_lifetime_id.clone()),
            request_id: RequestId::String(format!("continuation:{}", parent.run_id.0)),
            input: prompt.into(),
            mode: AdmissionMode::Queue,
        };
        self.admit_run_inner_owned(
            &admission,
            snapshot.route.as_ref(),
            Some(&snapshot),
            Some((parent, operation, envelope)),
            None,
            None,
            true,
            false,
        )
        .map(|(admission, _)| admission)
    }
    pub(crate) fn hook_relation_scoped(
        &self,
        scope: &TenantContext,
        run: &RunId,
        read_parent: bool,
    ) -> Result<Option<RunId>, RuntimeError> {
        if run.0.is_empty() || run.0.len() > 1024 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        enterprise::check_run_scope_in(&tx, scope, run)?;
        let bound:u64=tx.query_row("SELECT EXISTS(SELECT 1 FROM runs r JOIN session_lifetime_owners o ON o.session_id=r.session_id AND o.lifetime=r.lifetime AND o.provenance='canonical' WHERE r.id=?1)",[&run.0],|r|r.get(0))?;
        if bound != 1 {
            return Err(CoordinationError::Corrupt.into());
        }
        let (column, selector) = if read_parent {
            ("parent_run_id", "child_run_id")
        } else {
            ("child_run_id", "parent_run_id")
        };
        // 只从固定方向常量形成SQL，不把输入run/operation拼入SQL。
        let sql = format!(
            "SELECT CASE WHEN typeof(h.{column})='text' AND length(CAST(h.{column} AS BLOB)) BETWEEN 1 AND 1024 THEN h.{column} END,CASE WHEN typeof(h.operation_id)='text' AND length(CAST(h.operation_id AS BLOB)) BETWEEN 1 AND 256 THEN h.operation_id END,EXISTS(SELECT 1 FROM runs p JOIN runs c ON c.id=h.child_run_id AND c.session_id=p.session_id AND c.lifetime=p.lifetime JOIN session_lifetime_owners o ON o.session_id=p.session_id AND o.lifetime=p.lifetime AND o.provenance='canonical' WHERE p.id=h.parent_run_id AND p.id<>c.id) FROM hook_continuations h WHERE h.{selector}=?1"
        );
        let row: Option<(Option<String>, Option<String>, bool)> = tx
            .query_row(&sql, [&run.0], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .optional()?;
        let related = match row {
            Some((Some(related), Some(_), true)) => Some(RunId(related)),
            None => None,
            _ => return Err(CoordinationError::Corrupt.into()),
        };
        tx.commit()?;
        Ok(related)
    }
}
#[async_trait::async_trait]
impl HookContinuationRepository for RunStore {
    async fn admit_stop_continuation(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        operation: &str,
        prompt: &str,
        snapshot: &RunSnapshot,
    ) -> Result<Admission, RepositoryError> {
        if !valid_input(&envelope.owner, operation, prompt, snapshot) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, operation, prompt, snapshot) = (
            envelope.clone(),
            operation.to_owned(),
            prompt.to_owned(),
            snapshot.clone(),
        );
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .admit_hook_continuation_scoped(
                    &envelope.owner,
                    &operation,
                    &prompt,
                    &snapshot,
                    Some(&envelope),
                )
                .map_err(execution::execution_error)
        })
        .await
    }
    async fn read_hook_parent(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Option<RunId>, RepositoryError> {
        if run.0.is_empty() || run.0.len() > 1024 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, run) = (scope.clone(), run.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .hook_relation_scoped(&scope, &run, true)
                .map_err(RuntimeError::repository_error)
        })
        .await
    }
    async fn read_hook_child(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Option<RunId>, RepositoryError> {
        if run.0.is_empty() || run.0.len() > 1024 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (scope, run) = (scope.clone(), run.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .hook_relation_scoped(&scope, &run, false)
                .map_err(RuntimeError::repository_error)
        })
        .await
    }
}
