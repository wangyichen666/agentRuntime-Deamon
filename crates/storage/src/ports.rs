//! 已接线的能力端口；维护权限独立于在线控制面。
use super::TurnRepository;
use super::{
    DelegationRecord, DelegationRequest, RunStore, RuntimeError, SessionLifecycle, SessionQuery,
    TranscriptStore,
};
use agent_core::SessionKey as SessionId;
use agent_core::*;
use serde_json::Value;

pub trait RunRepository: Send + Sync {
    fn admit_authorized_run(
        &self,
        admission: &RunAdmission,
        snapshot: &RunSnapshot,
        authorization: &AuthorizationRequest,
        verifier: &dyn AuthorizationVerifier,
    ) -> Result<Admission, RuntimeError>;
    fn admit_run(
        &self,
        admission: &RunAdmission,
        snapshot: &RunSnapshot,
    ) -> Result<Admission, RuntimeError>;
    #[cfg(feature = "test-support")]
    fn admit(
        &self,
        session_id: SessionId,
        request_id: RequestId,
        input: &str,
    ) -> Result<Admission, RuntimeError> {
        self.admit_with_route(session_id, request_id, input, AdmissionMode::Queue, None)
    }

    fn admit_with_route(
        &self,
        session_id: SessionId,
        request_id: RequestId,
        input: &str,
        mode: AdmissionMode,
        route: Option<&RouteSnapshot>,
    ) -> Result<Admission, RuntimeError>;
    fn admit_with_lifetime(
        &self,
        session_id: SessionId,
        expected: Option<&SessionLifetimeId>,
        request_id: RequestId,
        input: &str,
        mode: AdmissionMode,
        route: Option<&RouteSnapshot>,
    ) -> Result<Admission, RuntimeError>;
    fn try_start_queued(&self, run_id: &RunId) -> Result<bool, RuntimeError>;
    fn queued_messages(&self, session_id: &str) -> Result<Vec<QueuedMessage>, RuntimeError>;
    fn queued_message(
        &self,
        session_id: &str,
        run_id: &RunId,
    ) -> Result<Option<QueuedMessage>, RuntimeError>;
    fn recoverable_queued(&self) -> Result<Vec<RunRecord>, RuntimeError>;
    fn session_exists(&self, session_id: &str) -> Result<bool, RuntimeError>;
    fn remove_queued(&self, run_id: &RunId) -> Result<bool, RuntimeError>;
    fn prepare_owned_tool_batch(
        &self,
        owner: &ExactOwner,
        round: usize,
        calls: &[(ToolCall, String, String, bool)],
        descriptors: &[ToolDescriptor],
    ) -> Result<(), RuntimeError>;
    fn prepare_tool_batch(
        &self,
        run_id: &RunId,
        round: usize,
        calls: &[(ToolCall, String, String, bool)],
    ) -> Result<(), RuntimeError>;
    fn start_tool(&self, run_id: &RunId, round: usize, call_id: &str) -> Result<(), RuntimeError>;
    fn finish_tool(
        &self,
        run_id: &RunId,
        round: usize,
        call_id: &str,
        outcome: &str,
        artifact_ref: Option<&str>,
        receipt: &Value,
    ) -> Result<(), RuntimeError>;
    fn finish_tool_batch(&self, run_id: &RunId, round: usize) -> Result<(), RuntimeError>;
    fn tool_receipts(&self, run_id: &RunId) -> Result<Vec<ToolReceipt>, RuntimeError>;
    fn append_event(
        &self,
        run_id: &RunId,
        event: &str,
        data: &Value,
    ) -> Result<EventSeq, RuntimeError>;
    fn finish(
        &self,
        run_id: &RunId,
        status: RunStatus,
        content: Option<&str>,
        error: Option<(i64, &str)>,
    ) -> Result<RunRecord, RuntimeError>;
    fn read_run(&self, run_id: &RunId) -> Result<Option<RunRecord>, RuntimeError>;
    fn route_snapshot(&self, run_id: &RunId) -> Result<Option<RouteSnapshot>, RuntimeError>;
    fn start_provider_attempt(&self, attempt: &ProviderAttempt) -> Result<(), RuntimeError>;
    fn finish_provider_attempt(&self, attempt: &ProviderAttempt) -> Result<(), RuntimeError>;
    fn provider_attempts(&self, run_id: &RunId) -> Result<Vec<ProviderAttempt>, RuntimeError>;
    fn provider_usage(&self, run_id: &RunId) -> Result<ProviderUsage, RuntimeError>;
    fn find_request(
        &self,
        session_id: &str,
        request_id: &RequestId,
    ) -> Result<Option<RunRecord>, RuntimeError>;
    fn find_request_unique(
        &self,
        request_id: &RequestId,
    ) -> Result<Option<RunRecord>, RuntimeError>;
    fn events_after(
        &self,
        run_id: &RunId,
        after: EventSeq,
        limit: usize,
    ) -> Result<Vec<StoredEvent>, RuntimeError>;
}

impl RunRepository for RunStore {
    fn admit_authorized_run(
        &self,
        admission: &RunAdmission,
        snapshot: &RunSnapshot,
        authorization: &AuthorizationRequest,
        verifier: &dyn AuthorizationVerifier,
    ) -> Result<Admission, RuntimeError> {
        RunStore::admit_authorized_run(self, admission, snapshot, authorization, verifier)
    }
    fn admit_run(
        &self,
        admission: &RunAdmission,
        snapshot: &RunSnapshot,
    ) -> Result<Admission, RuntimeError> {
        RunStore::admit_run(self, admission, snapshot)
    }
    fn admit_with_route(
        &self,
        session_id: SessionId,
        request_id: RequestId,
        input: &str,
        mode: AdmissionMode,
        route: Option<&RouteSnapshot>,
    ) -> Result<Admission, RuntimeError> {
        RunStore::admit_with_route(self, session_id, request_id, input, mode, route)
    }
    fn admit_with_lifetime(
        &self,
        session_id: SessionId,
        expected: Option<&SessionLifetimeId>,
        request_id: RequestId,
        input: &str,
        mode: AdmissionMode,
        route: Option<&RouteSnapshot>,
    ) -> Result<Admission, RuntimeError> {
        RunStore::admit_with_lifetime(self, session_id, expected, request_id, input, mode, route)
    }
    fn try_start_queued(&self, run_id: &RunId) -> Result<bool, RuntimeError> {
        RunStore::try_start_queued(self, run_id)
    }
    fn queued_messages(&self, session_id: &str) -> Result<Vec<QueuedMessage>, RuntimeError> {
        RunStore::queued_messages(self, session_id)
    }
    fn queued_message(
        &self,
        session_id: &str,
        run_id: &RunId,
    ) -> Result<Option<QueuedMessage>, RuntimeError> {
        RunStore::queued_message(self, session_id, run_id)
    }
    fn recoverable_queued(&self) -> Result<Vec<RunRecord>, RuntimeError> {
        RunStore::recoverable_queued(self)
    }
    fn session_exists(&self, session_id: &str) -> Result<bool, RuntimeError> {
        RunStore::session_exists(self, session_id)
    }
    fn remove_queued(&self, run_id: &RunId) -> Result<bool, RuntimeError> {
        RunStore::remove_queued(self, run_id)
    }
    fn prepare_owned_tool_batch(
        &self,
        owner: &ExactOwner,
        round: usize,
        calls: &[(ToolCall, String, String, bool)],
        descriptors: &[ToolDescriptor],
    ) -> Result<(), RuntimeError> {
        RunStore::prepare_owned_tool_batch(self, owner, round, calls, descriptors)
    }
    fn prepare_tool_batch(
        &self,
        run_id: &RunId,
        round: usize,
        calls: &[(ToolCall, String, String, bool)],
    ) -> Result<(), RuntimeError> {
        RunStore::prepare_tool_batch(self, run_id, round, calls)
    }
    fn start_tool(&self, run_id: &RunId, round: usize, call_id: &str) -> Result<(), RuntimeError> {
        RunStore::start_tool(self, run_id, round, call_id)
    }
    fn finish_tool(
        &self,
        run_id: &RunId,
        round: usize,
        call_id: &str,
        outcome: &str,
        artifact_ref: Option<&str>,
        receipt: &Value,
    ) -> Result<(), RuntimeError> {
        RunStore::finish_tool(self, run_id, round, call_id, outcome, artifact_ref, receipt)
    }
    fn finish_tool_batch(&self, run_id: &RunId, round: usize) -> Result<(), RuntimeError> {
        RunStore::finish_tool_batch(self, run_id, round)
    }
    fn tool_receipts(&self, run_id: &RunId) -> Result<Vec<ToolReceipt>, RuntimeError> {
        RunStore::tool_receipts(self, run_id)
    }
    fn append_event(
        &self,
        run_id: &RunId,
        event: &str,
        data: &Value,
    ) -> Result<EventSeq, RuntimeError> {
        RunStore::append_event(self, run_id, event, data)
    }
    fn finish(
        &self,
        run_id: &RunId,
        status: RunStatus,
        content: Option<&str>,
        error: Option<(i64, &str)>,
    ) -> Result<RunRecord, RuntimeError> {
        RunStore::finish(self, run_id, status, content, error)
    }
    fn read_run(&self, run_id: &RunId) -> Result<Option<RunRecord>, RuntimeError> {
        RunStore::read_run(self, run_id)
    }
    fn route_snapshot(&self, run_id: &RunId) -> Result<Option<RouteSnapshot>, RuntimeError> {
        RunStore::route_snapshot(self, run_id)
    }
    fn start_provider_attempt(&self, attempt: &ProviderAttempt) -> Result<(), RuntimeError> {
        RunStore::start_provider_attempt(self, attempt)
    }
    fn finish_provider_attempt(&self, attempt: &ProviderAttempt) -> Result<(), RuntimeError> {
        RunStore::finish_provider_attempt(self, attempt)
    }
    fn provider_attempts(&self, run_id: &RunId) -> Result<Vec<ProviderAttempt>, RuntimeError> {
        RunStore::provider_attempts(self, run_id)
    }
    fn provider_usage(&self, run_id: &RunId) -> Result<ProviderUsage, RuntimeError> {
        RunStore::provider_usage(self, run_id)
    }
    fn find_request(
        &self,
        session_id: &str,
        request_id: &RequestId,
    ) -> Result<Option<RunRecord>, RuntimeError> {
        RunStore::find_request(self, session_id, request_id)
    }
    fn find_request_unique(
        &self,
        request_id: &RequestId,
    ) -> Result<Option<RunRecord>, RuntimeError> {
        RunStore::find_request_unique(self, request_id)
    }
    fn events_after(
        &self,
        run_id: &RunId,
        after: EventSeq,
        limit: usize,
    ) -> Result<Vec<StoredEvent>, RuntimeError> {
        RunStore::events_after(self, run_id, after, limit)
    }
}

pub trait InteractionRepository: Send + Sync {
    fn read_interaction(
        &self,
        id: &InteractionId,
    ) -> Result<Option<InteractionRecord>, RuntimeError>;
    fn list_interactions(&self, session_id: &str) -> Result<Vec<InteractionRecord>, RuntimeError>;
    fn claim_interaction(
        &self,
        id: &InteractionId,
        session_id: &SessionId,
        run_id: &RunId,
        revision: i64,
        approved: bool,
    ) -> Result<InteractionRecord, RuntimeError>;
}

impl InteractionRepository for RunStore {
    fn read_interaction(
        &self,
        id: &InteractionId,
    ) -> Result<Option<InteractionRecord>, RuntimeError> {
        RunStore::read_interaction(self, id)
    }
    fn list_interactions(&self, session_id: &str) -> Result<Vec<InteractionRecord>, RuntimeError> {
        RunStore::list_interactions(self, session_id)
    }
    fn claim_interaction(
        &self,
        id: &InteractionId,
        session_id: &SessionId,
        run_id: &RunId,
        revision: i64,
        approved: bool,
    ) -> Result<InteractionRecord, RuntimeError> {
        RunStore::claim_interaction(self, id, session_id, run_id, revision, approved)
    }
}

pub trait ArtifactRepository: Send + Sync {
    fn finish_owned_tool_output(
        &self,
        owner: &ExactOwner,
        completion: &ToolArtifactCompletion,
        content: &str,
    ) -> Result<String, RuntimeError>;
    fn store_tool_output(&self, content: &str) -> Result<String, RuntimeError>;
    fn store_owned_output(&self, owner: &ExactOwner, content: &str)
    -> Result<String, RuntimeError>;
    fn read_run_artifact(
        &self,
        owner: &ExactOwner,
        reference: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<u8>, RuntimeError>;
}

impl ArtifactRepository for RunStore {
    fn finish_owned_tool_output(
        &self,
        owner: &ExactOwner,
        completion: &ToolArtifactCompletion,
        content: &str,
    ) -> Result<String, RuntimeError> {
        RunStore::finish_owned_tool_output(self, owner, completion, content)
    }
    fn store_owned_output(
        &self,
        owner: &ExactOwner,
        content: &str,
    ) -> Result<String, RuntimeError> {
        let db = self.lock_connection()?;
        crate::sessions::fence_in(&db, owner)?;
        let reference = self.store_tool_output(content)?;
        crate::sessions::fence_in(&db, owner)?;
        Ok(reference)
    }
    fn read_run_artifact(
        &self,
        owner: &ExactOwner,
        reference: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<u8>, RuntimeError> {
        self.read_legacy_artifact_scoped(&TenantContext::local(), owner, reference, offset, limit)
    }
    fn store_tool_output(&self, content: &str) -> Result<String, RuntimeError> {
        RunStore::store_tool_output(self, content)
    }
}

pub trait DelegationRepository: Send + Sync {
    fn admit_delegation(
        &self,
        request: DelegationRequest,
    ) -> Result<DelegationRecord, RuntimeError>;
    fn delegation(&self, child_run_id: &RunId) -> Result<Option<DelegationRecord>, RuntimeError>;
    fn delegation_by_spawn_key(
        &self,
        parent_run_id: &RunId,
        spawn_key: &str,
    ) -> Result<Option<DelegationRecord>, RuntimeError>;
    fn delegation_for_session(
        &self,
        session_id: &str,
    ) -> Result<Option<DelegationRecord>, RuntimeError>;
    fn list_delegations(&self, root_run_id: &RunId) -> Result<Vec<DelegationRecord>, RuntimeError>;
    fn reserve_delegation_result(
        &self,
        child_run_id: &RunId,
        owner: &str,
        revision: i64,
    ) -> Result<DelegationRecord, RuntimeError>;
    fn release_delegation_result(
        &self,
        child_run_id: &RunId,
        owner: &str,
        revision: i64,
    ) -> Result<DelegationRecord, RuntimeError>;
    fn deliver_delegation_result(
        &self,
        child_run_id: &RunId,
        owner: &str,
        revision: i64,
    ) -> Result<DelegationRecord, RuntimeError>;
}

impl DelegationRepository for RunStore {
    fn admit_delegation(
        &self,
        request: DelegationRequest,
    ) -> Result<DelegationRecord, RuntimeError> {
        RunStore::admit_delegation(self, request)
    }
    fn delegation(&self, child_run_id: &RunId) -> Result<Option<DelegationRecord>, RuntimeError> {
        RunStore::delegation(self, child_run_id)
    }
    fn delegation_by_spawn_key(
        &self,
        parent_run_id: &RunId,
        spawn_key: &str,
    ) -> Result<Option<DelegationRecord>, RuntimeError> {
        RunStore::delegation_by_spawn_key(self, parent_run_id, spawn_key)
    }
    fn delegation_for_session(
        &self,
        session_id: &str,
    ) -> Result<Option<DelegationRecord>, RuntimeError> {
        RunStore::delegation_for_session(self, session_id)
    }
    fn list_delegations(&self, root_run_id: &RunId) -> Result<Vec<DelegationRecord>, RuntimeError> {
        RunStore::list_delegations(self, root_run_id)
    }
    fn reserve_delegation_result(
        &self,
        child_run_id: &RunId,
        owner: &str,
        revision: i64,
    ) -> Result<DelegationRecord, RuntimeError> {
        RunStore::reserve_delegation_result(self, child_run_id, owner, revision)
    }
    fn release_delegation_result(
        &self,
        child_run_id: &RunId,
        owner: &str,
        revision: i64,
    ) -> Result<DelegationRecord, RuntimeError> {
        RunStore::release_delegation_result(self, child_run_id, owner, revision)
    }
    fn deliver_delegation_result(
        &self,
        child_run_id: &RunId,
        owner: &str,
        revision: i64,
    ) -> Result<DelegationRecord, RuntimeError> {
        RunStore::deliver_delegation_result(self, child_run_id, owner, revision)
    }
}

pub trait MaintenanceRepository: Send + Sync {
    fn record_maintenance(
        &self,
        owner: &ExactOwner,
        kind: &str,
        outcome: &str,
    ) -> Result<(), RuntimeError>;
    fn health_report(&self) -> Result<Value, RuntimeError>;
    fn gc_artifacts(&self, retention: std::time::Duration) -> Result<usize, RuntimeError>;
    fn legacy_plan_imported(&self, source: &str) -> Result<bool, RuntimeError>;
    fn import_legacy_plan(
        &self,
        source: &str,
        digest: &str,
        key: &SessionKey,
        value: &Value,
    ) -> Result<(), RuntimeError>;
    fn recover(&self) -> Result<usize, RuntimeError>;
    fn reconcile_unknown(
        &self,
        run_id: &RunId,
        session_id: &SessionId,
        expected_last_seq: EventSeq,
        status: RunStatus,
        content: Option<&str>,
        evidence: &str,
    ) -> Result<RunRecord, RuntimeError>;
}

impl MaintenanceRepository for RunStore {
    fn record_maintenance(
        &self,
        owner: &ExactOwner,
        kind: &str,
        outcome: &str,
    ) -> Result<(), RuntimeError> {
        let mut connection = self.lock_connection()?;
        let db = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        super::sessions::fence_in(&db, owner)?;
        maintenance_diagnostic_in(&db, owner, kind, outcome)?;
        db.commit()?;
        Ok(())
    }
    fn health_report(&self) -> Result<Value, RuntimeError> {
        self.storage_health_scoped(&TenantContext::local())
    }
    fn gc_artifacts(&self, retention: std::time::Duration) -> Result<usize, RuntimeError> {
        let mut connection = self.lock_connection()?;
        // 跨 RunStore/进程共享同一写锁：不能与引用发布事务交错删除对象。
        let db = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let mut deleted = 0;
        for path in self.file_artifacts.candidates()? {
            let references: i64 = db.query_row(
                "SELECT count(*) FROM tool_executions WHERE artifact_ref=?1",
                rusqlite::params![path.to_string_lossy()],
                |r| r.get(0),
            )?;
            let hash = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_prefix("sha256-"))
                .and_then(|n| n.strip_suffix(".txt"))
                .ok_or(ArtifactError::InvalidReference)?;
            let prepared: i64 = db.query_row(
                "SELECT count(*) FROM artifact_publications WHERE store_id=?1 AND sha256=?2",
                rusqlite::params![self.file_artifacts.store_id(), hash],
                |r| r.get(0),
            )?;
            if references == 0
                && prepared == 0
                && self.file_artifacts.remove_if_old(&path, retention)?
            {
                deleted += 1;
            }
        }
        deleted += self.file_artifacts.clean_abandoned_temporary(retention)?;
        db.commit()?;
        Ok(deleted)
    }
    fn legacy_plan_imported(&self, source: &str) -> Result<bool, RuntimeError> {
        RunStore::legacy_plan_imported(self, source)
    }
    fn import_legacy_plan(
        &self,
        source: &str,
        digest: &str,
        key: &SessionKey,
        value: &Value,
    ) -> Result<(), RuntimeError> {
        RunStore::import_legacy_plan(self, source, digest, key, value)
    }
    fn recover(&self) -> Result<usize, RuntimeError> {
        RunStore::recover(self)
    }
    fn reconcile_unknown(
        &self,
        run_id: &RunId,
        session_id: &SessionId,
        expected_last_seq: EventSeq,
        status: RunStatus,
        content: Option<&str>,
        evidence: &str,
    ) -> Result<RunRecord, RuntimeError> {
        RunStore::reconcile_unknown(
            self,
            run_id,
            session_id,
            expected_last_seq,
            status,
            content,
            evidence,
        )
    }
}

pub trait SessionRepository:
    RunRepository
    + InteractionRepository
    + ArtifactRepository
    + DelegationRepository
    + SessionQuery
    + SessionLifecycle
    + TranscriptStore
    + TurnRepository
    + crate::ContextRepository
    + crate::MemoryRepository
    + crate::ResourceRepository
    + crate::HookRepository
    + crate::CompactRunRepository
    + crate::RequestRepository
    + crate::ViewRepository
    + crate::DiscoveryRepository
    + crate::IdentityRepository
{
}
impl<
    T: RunRepository
        + InteractionRepository
        + ArtifactRepository
        + DelegationRepository
        + SessionQuery
        + SessionLifecycle
        + TranscriptStore
        + TurnRepository
        + crate::ContextRepository
        + crate::MemoryRepository
        + crate::ResourceRepository
        + crate::HookRepository
        + crate::CompactRunRepository
        + crate::RequestRepository
        + crate::ViewRepository
        + crate::DiscoveryRepository
        + crate::IdentityRepository,
> ControlRepository for T
{
}

/// Wave 1 兼容名称，指向同一个组合能力而非另一套 owner。
pub use SessionRepository as ControlRepository;

pub(crate) fn maintenance_diagnostic_in(
    db: &rusqlite::Connection,
    owner: &ExactOwner,
    kind: &str,
    outcome: &str,
) -> Result<(), RuntimeError> {
    db.execute("INSERT INTO maintenance_diagnostics VALUES(?1,?2,?3) ON CONFLICT(run_id,kind) DO UPDATE SET diagnostic=excluded.diagnostic",rusqlite::params![owner.run_id.0,kind,outcome])?;
    Ok(())
}
