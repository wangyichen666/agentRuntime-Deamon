//! daemon 的完整领域端口装配；同一 adapter 提供同一数据库事实源。
use agent_storage_ports::*;
use std::sync::Arc;

/// 只保存端口句柄，不保存会话、运行或审批的业务状态。
/// 必须提供全部领域能力，缺少端口的后端不能用空实现进入生产装配。
pub struct DaemonRepositories {
    pub(crate) workflow_store: Arc<dyn agent_storage_ports::WorkflowRepository>,
    pub(crate) registry_store: Arc<dyn agent_storage_ports::RegistryRepository>,
    pub(crate) billing_store: Arc<dyn BillingRepository>,
    pub(crate) credential_store: Arc<dyn CredentialRepository>,
    pub(crate) access_store: Arc<dyn AccessRepository>,
    pub(crate) daemon_maintenance_store: Arc<dyn DaemonMaintenanceRepository>,
    pub(crate) observation_store: Arc<dyn RuntimeObservationRepository>,
    pub(crate) plan_control_store: Arc<dyn PlanControlRepository>,
    pub(crate) publication_store: Arc<dyn PublicationRepository>,
    pub(crate) readback_store: Arc<dyn SessionReadRepository>,
    pub(crate) audit_store: Arc<dyn AuditReadRepository>,
    pub(crate) quota_store: Arc<dyn QuotaRepository>,
    pub(crate) quota_management: Arc<dyn QuotaManagementRepository>,
    pub(crate) identity_management: Arc<dyn agent_storage_ports::IdentityManagementRepository>,
    pub(crate) identity_store: Arc<dyn IdentityReadRepository>,
    pub(crate) event_store: Arc<dyn RunEventRepository>,
    pub(crate) queue_store: Arc<dyn RunQueueRepository>,
    pub(crate) run_fact_store: Arc<dyn RunFactRepository>,
    pub(crate) run_execution_store: Arc<dyn RunExecutionRepository>,
    pub(crate) run_request_store: Arc<dyn RunContextRepository>,
    pub(crate) delegation_store: Arc<dyn RunDelegationRepository>,
    pub(crate) interaction_store: Arc<dyn RunInteractionRepository>,
    pub(crate) resource_store: Arc<dyn RunResourceRepository>,
    pub(crate) memory_maintenance: Arc<dyn MemoryMaintenanceRepository>,
    pub(crate) memory_store: Arc<dyn ScopedMemoryRepository>,
    pub(crate) hook_continuation_store: Arc<dyn HookContinuationRepository>,
    pub(crate) hook_execution_store: Arc<dyn HookExecutionRepository>,
    pub(crate) storage_work_drain: Arc<dyn StorageWorkDrainRepository>,
    pub(crate) hook_observation_store: Arc<dyn HookObservationRepository>,
    pub(crate) run_discovery_store: Arc<dyn RunDiscoveryRepository>,
    pub(crate) run_turn_store: Arc<dyn RunTurnRepository>,
    pub(crate) run_tool_store: Arc<dyn RunToolRepository>,
    pub(crate) run_attempt_store: Arc<dyn RunAttemptRepository>,
    pub(crate) session_control_store: Arc<dyn SessionControlRepository>,
    pub(crate) lifecycle_store: Arc<dyn SessionLifecycleRepository>,
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) legacy_control: Option<Arc<dyn agent_storage::ControlRepository>>,
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) legacy_maintenance: Option<Arc<dyn agent_storage::MaintenanceRepository>>,
}

impl DaemonRepositories {
    /// 从一个完整后端构造全部端口，避免运行装配混用不同事实源。
    pub fn from_backend<T>(
        store: Arc<T>,
        #[cfg(any(test, feature = "test-support"))] legacy_control: Option<
            Arc<dyn agent_storage::ControlRepository>,
        >,
        #[cfg(any(test, feature = "test-support"))] legacy_maintenance: Option<
            Arc<dyn agent_storage::MaintenanceRepository>,
        >,
    ) -> Self
    where
        T: DaemonMaintenanceRepository
            + RuntimeObservationRepository
            + PlanControlRepository
            + PublicationRepository
            + SessionReadRepository
            + AuditReadRepository
            + QuotaRepository
            + QuotaManagementRepository
            + WorkflowRepository
            + RegistryRepository
            + BillingRepository
            + CredentialRepository
            + AccessRepository
            + IdentityManagementRepository
            + IdentityReadRepository
            + RunEventRepository
            + RunQueueRepository
            + RunFactRepository
            + RunExecutionRepository
            + RunContextRepository
            + RunDelegationRepository
            + RunInteractionRepository
            + RunResourceRepository
            + MemoryMaintenanceRepository
            + ScopedMemoryRepository
            + HookContinuationRepository
            + HookExecutionRepository
            + StorageWorkDrainRepository
            + HookObservationRepository
            + RunDiscoveryRepository
            + RunTurnRepository
            + RunToolRepository
            + RunAttemptRepository
            + SessionControlRepository
            + SessionLifecycleRepository
            + 'static,
    {
        Self {
            workflow_store:store.clone(),
            registry_store:store.clone(),
            billing_store:store.clone(),
            credential_store: store.clone(),
            access_store: store.clone(),
            daemon_maintenance_store: store.clone(),
            observation_store: store.clone(),
            plan_control_store: store.clone(),
            publication_store: store.clone(),
            readback_store: store.clone(),
            audit_store: store.clone(),
            quota_store: store.clone(),
            quota_management: store.clone(),
            identity_store: store.clone(),
            identity_management: store.clone(),
            event_store: store.clone(),
            queue_store: store.clone(),
            run_fact_store: store.clone(),
            run_execution_store: store.clone(),
            run_request_store: store.clone(),
            delegation_store: store.clone(),
            interaction_store: store.clone(),
            resource_store: store.clone(),
            memory_maintenance: store.clone(),
            memory_store: store.clone(),
            hook_continuation_store: store.clone(),
            hook_execution_store: store.clone(),
            storage_work_drain: store.clone(),
            hook_observation_store: store.clone(),
            run_discovery_store: store.clone(),
            run_turn_store: store.clone(),
            run_tool_store: store.clone(),
            run_attempt_store: store.clone(),
            session_control_store: store.clone(),
            lifecycle_store: store.clone(),
            #[cfg(any(test, feature = "test-support"))]
            legacy_control,
            #[cfg(any(test, feature = "test-support"))]
            legacy_maintenance,
        }
    }

    /// Local 启动与兼容 SDK 复用同一套端口装配。
    pub fn from_sqlite(store: Arc<agent_storage::RunStore>) -> Self {
        Self::from_backend(
            store.clone(),
            #[cfg(any(test, feature = "test-support"))]
            Some(store.clone()),
            #[cfg(any(test, feature = "test-support"))]
            Some(store),
        )
    }
}
