//! 原daemon维护和人工核验的异步适配，恢复保持唯一原事务。
use crate::{RunStore, RuntimeError, execution};
use agent_core::*;
use rusqlite::params;
impl RunStore {
    pub(crate) fn storage_health_scoped(
        &self,
        scope: &TenantContext,
    ) -> Result<serde_json::Value, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let db = connection.transaction()?;
        let integrity: String = db.query_row("PRAGMA quick_check", [], |r| r.get(0))?;
        let version: i64 = db.query_row("SELECT max(version) FROM schema_migrations", [], |r| {
            r.get(0)
        })?;
        let visible_sessions = "SELECT s.session_id FROM session_scopes s JOIN tenants t ON t.id=s.tenant_id WHERE t.organization_id=?1 AND s.tenant_id=?2 AND s.project_id=?3 AND s.agent_uid=?4";
        let visible_runs = format!("SELECT id FROM runs WHERE session_id IN ({visible_sessions})");
        let visible_lives = format!(
            "SELECT lifetime FROM session_lifetime_owners WHERE provenance='canonical' AND session_id IN ({visible_sessions})"
        );
        let mut counts = serde_json::Map::new();
        for (name, sql) in [
            (
                "sessions",
                format!(
                    "SELECT count(*) FROM session_heads WHERE deleted=0 AND session_id IN ({visible_sessions})"
                ),
            ),
            (
                "queued",
                format!(
                    "SELECT count(*) FROM runs WHERE status='queued' AND id IN ({visible_runs})"
                ),
            ),
            (
                "running",
                format!(
                    "SELECT count(*) FROM runs WHERE status IN ('running','waiting_interaction') AND id IN ({visible_runs})"
                ),
            ),
            (
                "unknown_runs",
                format!(
                    "SELECT count(*) FROM runs WHERE status='unknown_after_restart' AND id IN ({visible_runs})"
                ),
            ),
            (
                "maintenance_failed",
                format!(
                    "SELECT count(*) FROM maintenance_diagnostics WHERE diagnostic='failed' AND run_id IN ({visible_runs})"
                ),
            ),
            (
                "maintenance_skipped",
                format!(
                    "SELECT count(*) FROM maintenance_diagnostics WHERE diagnostic='skipped_read_only' AND run_id IN ({visible_runs})"
                ),
            ),
            (
                "orphaned_resources",
                format!(
                    "SELECT count(*) FROM resources WHERE state='orphaned' AND run_id IN ({visible_runs})"
                ),
            ),
            (
                "pending_compacts",
                format!(
                    "SELECT count(*) FROM compact_operations WHERE state='pending' AND lifetime IN ({visible_lives})"
                ),
            ),
            (
                "legacy_quarantined_memories",
                format!(
                    "SELECT count(*) FROM memories WHERE scope='legacy' AND source_run IN ({visible_runs})"
                ),
            ),
        ] {
            let count: i64 = db.query_row(
                &sql,
                params![
                    scope.organization_id.as_str(),
                    scope.tenant_id.as_str(),
                    scope.project_id.as_str(),
                    scope.agent_uid.as_str()
                ],
                |r| r.get(0),
            )?;
            counts.insert(name.into(), count.into());
        }
        let writer = crate::writer::health_scoped_in(&db, scope)?;
        let result = serde_json::json!({"writer":writer,"schema_version":version,"integrity":integrity,"counts":counts,"budgets":{"active_resources":16,"queued_per_session":64,"queued_total":1024,"tool_calls_per_batch":64,"resource_log_bytes":65536,"artifact_page_bytes":16384},"transaction_wait_ms":self.wait_ms.load(std::sync::atomic::Ordering::Relaxed),"transaction_lock_count":self.lock_count.load(std::sync::atomic::Ordering::Relaxed)});
        db.commit()?;
        Ok(result)
    }
}
#[async_trait::async_trait]
impl agent_storage_ports::DaemonMaintenanceRepository for RunStore {
    async fn read_storage_health(
        &self,
        scope: &TenantContext,
    ) -> Result<serde_json::Value, RepositoryError> {
        let scope = scope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .storage_health_scoped(&scope)
                .map_err(RuntimeError::repository_error)
        })
        .await
    }
    async fn recover_interrupted_work(
        &self,
        scope: &TenantContext,
    ) -> Result<usize, RepositoryError> {
        let scope = scope.clone();
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .recover_scoped(&scope)
                .map_err(RuntimeError::repository_error)
        })
        .await
    }
    async fn collect_local_artifacts(
        &self,
        scope: &TenantContext,
        retention_ms: u64,
    ) -> Result<usize, RepositoryError> {
        // 原filesystem GC属于Local物理存储维护；其引用检查仍覆盖全部事实，不能删除他域引用。
        if scope != &TenantContext::local() {
            return Err(AuthorizationError::Forbidden.into());
        }
        if retention_ms < 60000 || retention_ms > 365 * 86400000 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            crate::MaintenanceRepository::gc_artifacts(
                &store,
                std::time::Duration::from_millis(retention_ms),
            )
            .map_err(RuntimeError::repository_error)
        })
        .await
    }
    async fn reconcile_run_outcome(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        expected_seq: EventSeq,
        status: RunStatus,
        content: Option<&str>,
        evidence: &str,
    ) -> Result<RunRecord, RepositoryError> {
        if !execution::valid_owner(&envelope.owner)
            || expected_seq.0 > i64::MAX as u64
            || content.is_some_and(|s| s.len() > 512 * 1024)
            || evidence.len() > 4096
            || evidence.chars().count() < 16
            || !matches!(
                status,
                RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled
            )
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, content, evidence) = (
            envelope.clone(),
            content.map(str::to_owned),
            evidence.to_owned(),
        );
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .reconcile_unknown_scoped(
                    &envelope.scope,
                    &envelope.owner.run_id,
                    &envelope.owner.session_key,
                    expected_seq,
                    status,
                    content.as_deref(),
                    &evidence,
                    Some(&envelope),
                )
                .map_err(execution::execution_error)
        })
        .await
    }
}
