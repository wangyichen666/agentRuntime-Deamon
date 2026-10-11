//! 有实际 daemon publisher 调用者的异步协调端口；各后端独立实现。
#![forbid(unsafe_code)]
mod memory;
use agent_core::*;
pub use memory::{MemoryEpisodeExcerpt, redact_memory_evidence};
use sha2::{Digest, Sha256};

/// 两个后端共用 canonical 批次完整性解码；单批次和消息数都有预算。
pub fn decode_transcript_batch(
    payload: &str,
    digest: &str,
    start: u64,
    end: u64,
) -> Result<Vec<Message>, CoordinationError> {
    if payload.len() > 16 * 1024 * 1024
        || digest.len() != 64
        || end < start
        || end - start > 32768
        || format!("{:x}", Sha256::digest(payload.as_bytes())) != digest
    {
        return Err(CoordinationError::Corrupt);
    }
    let batch: Vec<Message> =
        serde_json::from_str(payload).map_err(|_| CoordinationError::Corrupt)?;
    if batch.len() as u64 != end - start {
        return Err(CoordinationError::Corrupt);
    }
    Ok(batch)
}

/// canonical 会话投影端口；scope 来自服务端可信上下文，查询在数据库内隔离。
#[async_trait::async_trait]
pub trait SessionReadRepository: Send + Sync {
    async fn read_session(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        mode: HistoryReadMode,
    ) -> Result<SessionReadback, CoordinationError>;
    async fn list_sessions(
        &self,
        scope: &TenantContext,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<SessionListEntry>, CoordinationError>;
}

/// 身份与原冻结授权只读端口；准入最终 verifier 仍由原业务事务调用。
#[async_trait::async_trait]
pub trait IdentityReadRepository: Send + Sync {
    async fn read_identity(
        &self,
        scope: &TenantContext,
        subject: &SubjectId,
    ) -> Result<IdentityReadback, RepositoryError>;
    async fn read_authorization(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<Option<AuthorizationSnapshot>, RepositoryError>;
}

/// 原session创建与directory发布同一事务；共享限额与租约只拥有Access Plane预算。
#[async_trait::async_trait]
pub trait AccessRepository: Send + Sync {
    async fn find_directory_session(&self,request:&AuthorizationRequest,operation:&str,verifier:std::sync::Arc<dyn AuthorizationVerifier>) -> Result<Option<DirectorySession>,RepositoryError>;
    async fn oauth_login(
        &self, request: &AuthorizationRequest, proof: &OAuthLoginProof,
        consume: bool, verifier: std::sync::Arc<dyn AuthorizationVerifier>,
    ) -> Result<(), RepositoryError>;
    async fn create_directory_session(
        &self,
        request: &AuthorizationRequest,
        key: &SessionKey,
        endpoint: &str,
        operation: &str,
        verifier: std::sync::Arc<dyn AuthorizationVerifier>,
    ) -> Result<DirectorySession, RepositoryError>;
    async fn resolve_directory_session(
        &self,
        request: &AuthorizationRequest,
        external_id: &str,
        verifier: std::sync::Arc<dyn AuthorizationVerifier>,
    ) -> Result<DirectorySession, RepositoryError>;
    async fn acquire_access(
        &self,
        request: &AuthorizationRequest,
        class: AccessClass,
        operation: &str,
        verifier: std::sync::Arc<dyn AuthorizationVerifier>,
    ) -> Result<AccessLease, RepositoryError> {self.acquire_access_at(request,class,"legacy",operation,verifier).await}
    async fn acquire_access_at(&self,request:&AuthorizationRequest,class:AccessClass,endpoint:&str,operation:&str,verifier:std::sync::Arc<dyn AuthorizationVerifier>)->Result<AccessLease,RepositoryError>;
    async fn release_access(
        &self,
        request: &AuthorizationRequest,
        operation: &str,
        verifier: std::sync::Arc<dyn AuthorizationVerifier>,
    ) -> Result<(), RepositoryError>;
}

#[async_trait::async_trait]
pub trait BillingRepository: Send + Sync {
    async fn configure_billing(&self, request:&AuthorizationRequest, policy:&BillingPolicy, operation:&str, verifier:std::sync::Arc<dyn AuthorizationVerifier>) -> Result<FrozenBillingPolicy,RepositoryError>;
    async fn read_billing_policy(&self,scope:&TenantContext) -> Result<Option<FrozenBillingPolicy>,RepositoryError>;
    async fn read_billing_accounts(&self,scope:&TenantContext)->Result<Vec<BillingAccount>,RepositoryError>;
    async fn read_billing_usage(&self,scope:&TenantContext,after:u64,limit:u32) -> Result<UsagePage,RepositoryError>;
}
/// 版本化、稳定的领域封存；同一纯规则用于两个 adapter。
pub fn freeze_billing(policy:&BillingPolicy)->Result<FrozenBillingPolicy,RepositoryError> {
    policy.validate()?;
    let mut policy=policy.clone();policy.budgets.sort_by(|a,b|a.dimension.cmp(&b.dimension));
    let bytes=serde_json::to_vec(&policy).map_err(|_|CoordinationError::InvalidRequest)?;
    if bytes.len()>65536 {return Err(CoordinationError::InvalidRequest.into());}
    let digest=format!("{:x}",Sha256::digest(&bytes));
    Ok(FrozenBillingPolicy{digest,policy})
}
pub fn billing_input_upper_bound(capture:&ProviderRequestCapture)->Result<u64,RepositoryError> {
    if capture.input.messages.iter().any(|m|!m.image_urls.is_empty()) {
        return Err(CoordinationError::Forbidden.into());
    }
    let raw=serde_json::to_vec(&(&capture.input.messages,&capture.input.tools)).map_err(|_|CoordinationError::Corrupt)?;
    // 支持的文本 provider 用线长的四倍并加每个消息/tool framing，拒绝无法界定的多模态输入。
    (raw.len() as u64).checked_mul(4).and_then(|n|n.checked_add(4096)).ok_or(CoordinationError::InvalidRequest.into())
}
pub fn billing_charge_operation(run:&RunId,operation:&str)->String {
    format!("charge:{:x}",Sha256::digest(serde_json::json!([run.0,operation]).to_string().as_bytes()))
}
pub fn billing_budget_key(scope:&TenantContext,dimension:&BillingDimension)->Result<String,RepositoryError> {
    let value=match dimension {
        BillingDimension::Tenant=>serde_json::json!(["tenant"]),
        BillingDimension::Project=>serde_json::json!(["project",scope.project_id]),
        BillingDimension::Agent=>serde_json::json!(["agent",scope.project_id,scope.agent_uid]),
        BillingDimension::Model(model)=>serde_json::json!(["model",scope.project_id,scope.agent_uid,model]),
    };
    serde_json::to_string(&value).map_err(|_|CoordinationError::InvalidRequest.into())
}

#[async_trait::async_trait]
pub trait CredentialRepository: Send + Sync {
    async fn issue_api_credential(
        &self,
        request: &AuthorizationRequest,
        subject: &SubjectId,
        ttl_secs: u64,
        operation: &str,
        verifier: std::sync::Arc<dyn AuthorizationVerifier>,
    ) -> Result<IssuedApiCredential, RepositoryError>;
    async fn revoke_api_credential(
        &self,
        request: &AuthorizationRequest,
        key_id: &str,
        operation: &str,
        verifier: std::sync::Arc<dyn AuthorizationVerifier>,
    ) -> Result<ApiCredentialMetadata, RepositoryError>;
    async fn authenticate_api_credential(
        &self,
        scope: &TenantContext,
        token: &SecretToken,
    ) -> Result<ApiCredentialMetadata, RepositoryError>;
}

/// 原 canonical 事件：固定准入身份写入、完整 scope 下有界读回。
#[async_trait::async_trait]
pub trait RunEventRepository: Send + Sync {
    async fn append_run_live_event(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        event: &str,
        data: &serde_json::Value,
    ) -> Result<EventSeq, RepositoryError>;

    async fn read_run_view_event(
        &self,
        scope: &TenantContext,
        run: &RunId,
        event: &str,
        child: Option<&RunId>,
    ) -> Result<Option<StoredEvent>, CoordinationError>;

    async fn read_run_events(
        &self,
        scope: &TenantContext,
        run: &RunId,
        after: EventSeq,
        limit: u32,
    ) -> Result<RunEventPage, CoordinationError>;
}

/// 原会话 lifecycle command/receipt 的异步端口，不增加第二份状态。
#[async_trait::async_trait]
pub trait SessionLifecycleRepository: Send + Sync {
    /// 使用面授权与原lifecycle receipt在同一事务核验；管理入口保留显式受信任方法。
    async fn commit_session_authorized(
        &self,
        request: &AuthorizationRequest,
        command: &SessionCommand,
        expected: Option<&SessionLifetimeId>,
        operation: &str,
        verifier: std::sync::Arc<dyn AuthorizationVerifier>,
    ) -> Result<SessionMetadata, RepositoryError>;
    async fn commit_session_command(
        &self,
        scope: &TenantContext,
        command: &SessionCommand,
        expected: Option<&SessionLifetimeId>,
        operation: &str,
    ) -> Result<SessionMetadata, CoordinationError>;
    async fn read_close_receipt(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        operation: &str,
    ) -> Result<Option<SessionMetadata>, CoordinationError>;
}

/// 会话运行装配所需的元数据、Local偏好和一次性旧文件导入。
#[async_trait::async_trait]
pub trait SessionControlRepository: Send + Sync {
    async fn read_selected_session(
        &self,
        scope: &TenantContext,
    ) -> Result<Option<SessionKey>, RepositoryError>;
    async fn read_session_metadata(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
    ) -> Result<Option<SessionMetadata>, RepositoryError>;
    async fn select_session(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
    ) -> Result<(), RepositoryError>;
    async fn import_legacy_session(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        expected: Option<&SessionLifetimeId>,
        messages: &[Message],
    ) -> Result<(), RepositoryError>;
}

/// daemon关闭与启动维护；调用者先停止准入并join生产者，恢复只结算未知事实。
#[async_trait::async_trait]
pub trait DaemonMaintenanceRepository: Send + Sync {
    async fn read_storage_health(
        &self,
        scope: &TenantContext,
    ) -> Result<serde_json::Value, RepositoryError>;
    async fn recover_interrupted_work(
        &self,
        scope: &TenantContext,
    ) -> Result<usize, RepositoryError>;
    async fn collect_local_artifacts(
        &self,
        scope: &TenantContext,
        retention_ms: u64,
    ) -> Result<usize, RepositoryError>;
    async fn reconcile_run_outcome(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        expected_seq: EventSeq,
        status: RunStatus,
        content: Option<&str>,
        evidence: &str,
    ) -> Result<RunRecord, RepositoryError>;
}

/// 原计划文档、决策receipt与旧文件导入；不新增审批或计划状态机。
#[async_trait::async_trait]
pub trait PlanControlRepository: Send + Sync {
    async fn read_session_plan(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
    ) -> Result<PlanReadback, RepositoryError>;
    async fn discard_session_plan(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        identity: &PlanExecution,
    ) -> Result<PlanDecisionReceipt, RepositoryError>;
    async fn read_legacy_plan_import(
        &self,
        scope: &TenantContext,
        source: &str,
    ) -> Result<bool, RepositoryError>;
    async fn import_legacy_plan_file(
        &self,
        scope: &TenantContext,
        source: &str,
        digest: &str,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        value: &serde_json::Value,
    ) -> Result<(), RepositoryError>;
}

pub fn seal_event(event: &OutboxEvent) -> Result<(String, String, String), CoordinationError> {
    if !event.valid() {
        return Err(CoordinationError::InvalidRequest);
    }
    let mut value = serde_json::to_value(event).map_err(|_| CoordinationError::InvalidRequest)?;
    value.sort_all_objects();
    let payload = serde_json::to_string(&value).map_err(|_| CoordinationError::InvalidRequest)?;
    if payload.len() > 4 * 1024 * 1024 {
        return Err(CoordinationError::InvalidRequest);
    }
    let key = serde_json::to_vec(&(&event.envelope, event.event_seq))
        .map_err(|_| CoordinationError::InvalidRequest)?;
    Ok((
        format!("{:x}", Sha256::digest(key)),
        format!("{:x}", Sha256::digest(payload.as_bytes())),
        payload,
    ))
}
pub fn decode_record(
    cursor: u64,
    event_id: String,
    payload: String,
    digest: String,
    delivered: bool,
    binding: OutboxRowBinding,
) -> Result<OutboxRecord, CoordinationError> {
    if payload.len() > 4 * 1024 * 1024
        || format!("{:x}", Sha256::digest(payload.as_bytes())) != digest
    {
        return Err(CoordinationError::Corrupt);
    }
    let event: OutboxEvent =
        serde_json::from_str(&payload).map_err(|_| CoordinationError::Corrupt)?;
    if event.lease_key() != binding.lease_key
        || event.envelope.owner.run_id != binding.run_id
        || event.event_seq != binding.event_seq
    {
        return Err(CoordinationError::Corrupt);
    }
    let (expected_id, expected_digest, _) =
        seal_event(&event).map_err(|_| CoordinationError::Corrupt)?;
    if expected_id != event_id || expected_digest != digest {
        return Err(CoordinationError::Corrupt);
    }
    Ok(OutboxRecord {
        cursor,
        event_id,
        event,
        payload_digest: digest,
        delivered,
    })
}
#[async_trait::async_trait]
pub trait PublicationRepository: Send + Sync {
    async fn append_publication(
        &self,
        event: &OutboxEvent,
    ) -> Result<OutboxRecord, CoordinationError>;
    async fn acquire_delivery_lease(
        &self,
        key: &DeliveryLeaseKey,
        owner: &str,
        ttl_ms: i64,
    ) -> Result<DeliveryLease, CoordinationError>;
    async fn release_delivery_lease(&self, lease: &DeliveryLease) -> Result<(), CoordinationError>;
    async fn pending_delivery_keys(
        &self,
        scope: &TenantContext,
        limit: u32,
    ) -> Result<Vec<DeliveryLeaseKey>, CoordinationError>;
    async fn read_publications(
        &self,
        scope: &TenantContext,
        after: u64,
        limit: u32,
    ) -> Result<Vec<OutboxRecord>, CoordinationError>;
    async fn claim_publications(
        &self,
        lease: &DeliveryLease,
        limit: u32,
    ) -> Result<Vec<OutboxRecord>, CoordinationError>;
    /// 本地幂等 inbox 只保存 event_id/digest 引用，不复制业务事实；提交后 ack 前可故障。
    async fn deliver_to_inbox(
        &self,
        lease: &DeliveryLease,
        consumer: &str,
        record: &OutboxRecord,
    ) -> Result<bool, CoordinationError>;
    async fn acknowledge_publication(
        &self,
        lease: &DeliveryLease,
        event_id: &str,
        digest: &str,
    ) -> Result<(), CoordinationError>;
    async fn inbox_count(
        &self,
        scope: &TenantContext,
        consumer: &str,
    ) -> Result<u64, CoordinationError>;
}
pub async fn publish_to_local_inbox(
    repository: &dyn PublicationRepository,
    scope: &TenantContext,
    owner: &str,
) -> Result<usize, CoordinationError> {
    let keys = repository.pending_delivery_keys(scope, 16).await?;
    let mut count = 0;
    for key in keys {
        let lease = match repository.acquire_delivery_lease(&key, owner, 5000).await {
            Ok(lease) => lease,
            Err(CoordinationError::LeaseBusy) => continue,
            Err(error) => return Err(error),
        };
        let result = async {
            for record in repository.claim_publications(&lease, 32).await? {
                repository
                    .deliver_to_inbox(&lease, "local-journal", &record)
                    .await?;
                repository
                    .acknowledge_publication(&lease, &record.event_id, &record.payload_digest)
                    .await?;
                count += 1;
            }
            Ok::<(), CoordinationError>(())
        }
        .await;
        let released = repository.release_delivery_lease(&lease).await;
        result?;
        released?;
        // 单批最多一个有效租约；忙租约可跳过，其余 key 留待下一 tick。
        break;
    }
    Ok(count)
}

pub fn reserve_object_artifact(
    envelope: EnterpriseOwnerEnvelope,
    completion: &ToolArtifactCompletion,
    identity: ArtifactIdentity,
    store_id: String,
) -> Result<ObjectArtifactReservation, ArtifactError> {
    let id = serde_json::to_vec(&(&envelope, completion.round, &completion.call_id))
        .map_err(|_| ArtifactError::InvalidReference)?;
    let mut payload =
        serde_json::to_value(completion).map_err(|_| ArtifactError::InvalidReference)?;
    payload.sort_all_objects();
    let digest = serde_json::to_vec(&payload).map_err(|_| ArtifactError::InvalidReference)?;
    let value = ObjectArtifactReservation {
        publication_id: format!("{:x}", Sha256::digest(id)),
        envelope,
        round: completion.round,
        call_id: completion.call_id.clone(),
        identity,
        store_id,
        completion_digest: format!("{:x}", Sha256::digest(digest)),
        published: false,
    };
    if !value.valid() {
        return Err(ArtifactError::InvalidReference);
    }
    Ok(value)
}
#[async_trait::async_trait]
pub trait ArtifactStore: Send + Sync {
    fn store_id(&self) -> String;
    async fn publish_object(
        &self,
        reservation: &ObjectArtifactReservation,
        body: &str,
    ) -> Result<ArtifactIdentity, ArtifactError>;
    async fn read_object(
        &self,
        reservation: &ObjectArtifactReservation,
        offset: u64,
        limit: usize,
    ) -> Result<ArtifactPage, ArtifactError>;
}
#[async_trait::async_trait]
pub trait ArtifactPublicationRepository: Send + Sync {
    async fn reserve_object_artifact(
        &self,
        reservation: &ObjectArtifactReservation,
        completion: &ToolArtifactCompletion,
    ) -> Result<ObjectArtifactReservation, ArtifactError>;
    async fn commit_object_artifact(
        &self,
        reservation: &ObjectArtifactReservation,
        completion: &ToolArtifactCompletion,
        verified: &ArtifactIdentity,
    ) -> Result<(), ArtifactError>;
    async fn read_object_artifact(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        reference: &str,
    ) -> Result<ObjectArtifactReservation, ArtifactError>;
}

/// 原 session lifetime 的 writer 授权；epoch 使用数据库时钟和事务 CAS。
#[async_trait::async_trait]
pub trait SessionWriterRepository: Send + Sync {
    async fn acquire_writer_lease(
        &self,
        key: &SessionWriterLeaseKey,
        owner: &str,
        ttl_ms: i64,
    ) -> Result<SessionWriterLease, CoordinationError>;
    async fn renew_writer_lease(
        &self,
        lease: &SessionWriterLease,
        ttl_ms: i64,
    ) -> Result<SessionWriterLease, CoordinationError>;
    async fn release_writer_lease(
        &self,
        lease: &SessionWriterLease,
    ) -> Result<(), CoordinationError>;
    async fn read_writer_lease(
        &self,
        key: &SessionWriterLeaseKey,
    ) -> Result<Option<SessionWriterLease>, CoordinationError>;
    /// daemon 内部续租已获得的 proof，不会重新 acquire 过期 epoch。
    async fn renew_bound_writers(&self) -> Result<usize, CoordinationError>;
    /// 只在 daemon 停准入、join 所有受管执行体并持久化恢复事实后调用。
    async fn release_bound_writers(&self) -> Result<usize, CoordinationError>;
}

/// 只读原 run facts；所有资源查询都先在同事务 SQL 判定完整 scope。
#[async_trait::async_trait]
pub trait RunFactRepository: Send + Sync {
    async fn read_exact_run_owner(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<ExactOwner, CoordinationError>;

    async fn read_run_fact(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<RunFactReadback, CoordinationError>;
    async fn read_tool_receipts(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Vec<ToolReceipt>, CoordinationError>;
    async fn read_provider_history(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<RunProviderReadback, CoordinationError>;
}

/// 原准入、队列和 TurnCommit 的唯一异步执行端口；owner 在准入事务内冻结。
#[async_trait::async_trait]
pub trait RunExecutionRepository: Send + Sync {
    /// 原Local后台创建session与run的同事务准入；冻结独立workload授权并返回固定owner。
    async fn admit_workload_run(
        &self,
        admission: &RunAdmission,
        snapshot: &RunSnapshot,
        authorization: &AuthorizationRequest,
        verifier: std::sync::Arc<dyn AuthorizationVerifier>,
    ) -> Result<(Admission, ExactOwner), RepositoryError>;

    async fn admit_run_authorized(
        &self,
        admission: &RunAdmission,
        snapshot: &RunSnapshot,
        authorization: &AuthorizationRequest,
        verifier: std::sync::Arc<dyn AuthorizationVerifier>,
    ) -> Result<(Admission, ExactOwner), RepositoryError>;
    async fn start_queued_run(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<bool, RepositoryError>;
    async fn cancel_queued_run(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<bool, RepositoryError>;
    async fn commit_run_turn(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        commit: &TurnCommit,
    ) -> Result<RunRecord, RepositoryError>;
}

/// 原 Provider attempt 的准入 marker/终态及路由事实；网络调用在事务之外。
#[async_trait::async_trait]
pub trait RunAttemptRepository: Send + Sync {
    async fn start_run_attempt(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        attempt: &ProviderAttempt,
    ) -> Result<(), RepositoryError>;
    async fn finish_run_attempt(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        attempt: &ProviderAttempt,
    ) -> Result<(), RepositoryError>;
    async fn read_attempt_usage(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<ProviderUsage, RepositoryError>;
    async fn append_provider_event(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        event: &str,
        data: &serde_json::Value,
    ) -> Result<EventSeq, RepositoryError>;
}

// 原工具事务的异步接口；不持有另一份工具状态。
#[async_trait::async_trait]
pub trait RunToolRepository: Send + Sync {
    async fn prepare_run_tool_batch(
        &self,
        envelope: &agent_core::EnterpriseOwnerEnvelope,
        round: usize,
        calls: &[(agent_core::ToolCall, String, String, bool)],
        descriptors: &[agent_core::ToolDescriptor],
    ) -> Result<(), agent_core::RepositoryError>;
    async fn start_run_tool(
        &self,
        envelope: &agent_core::EnterpriseOwnerEnvelope,
        round: usize,
        call_id: &str,
    ) -> Result<(), agent_core::RepositoryError>;
    async fn close_run_tool_exchange(
        &self,
        envelope: &agent_core::EnterpriseOwnerEnvelope,
        round: usize,
        messages: &[agent_core::Message],
    ) -> Result<(), agent_core::RepositoryError>;
    async fn finish_run_tool_batch(
        &self,
        envelope: &agent_core::EnterpriseOwnerEnvelope,
        round: usize,
    ) -> Result<(), agent_core::RepositoryError>;
    async fn finish_file_tool_output(
        &self,
        envelope: &agent_core::EnterpriseOwnerEnvelope,
        completion: &agent_core::ToolArtifactCompletion,
        content: String,
    ) -> Result<String, agent_core::RepositoryError>;
    async fn read_tool_artifact_binding(
        &self,
        envelope: &agent_core::EnterpriseOwnerEnvelope,
    ) -> Result<Option<String>, agent_core::RepositoryError>;
}

/// 原 turn/transcript/计划事务的异步端口；scope 来自原准入上下文。
#[async_trait::async_trait]
pub trait RunTurnRepository: Send + Sync {
    async fn read_run_plan(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<RunPlanReadback, RepositoryError>;
    async fn stage_run_plan(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        expected_revision: u64,
        value: &serde_json::Value,
    ) -> Result<PlanSnapshot, RepositoryError>;
    async fn start_run_plan(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<(), RepositoryError>;
    async fn reject_run_tool_batch(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        round: usize,
        calls: &[ToolCall],
        reason: &str,
    ) -> Result<Vec<Message>, RepositoryError>;
    async fn stage_run_assistant(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        message: &Message,
    ) -> Result<(), RepositoryError>;
    async fn append_run_transcript(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        operation: &str,
        messages: &[Message],
    ) -> Result<TranscriptSeq, RepositoryError>;
}

/// 原发送前捕获及只读重建，native 写入使用固定准入 owner。
#[async_trait::async_trait]
pub trait RunRequestRepository: Send + Sync {
    async fn read_run_request_source(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        route: &str,
        identity: &str,
    ) -> Result<RunRequestSource, RepositoryError>;
    async fn capture_run_request(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        capture: &ProviderRequestCapture,
        context: &ContextEnvelope,
    ) -> Result<(), RepositoryError>;
    async fn read_provider_request(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        run: Option<&RunId>,
        capture: Option<&str>,
    ) -> Result<(ProviderRequestCapture, RunSnapshot), RepositoryError>;
}

/// 原请求端口的上下文领域扩展：实际执行 source 与原 compact 事务。
#[async_trait::async_trait]
pub trait RunContextRepository: RunRequestRepository {
    async fn read_execution_settings(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<RunSnapshot, RepositoryError>;
    async fn read_execution_context(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<RunContextReadback, RepositoryError>;
    async fn read_compaction_result(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        operation: &str,
        expected_revision: Option<TranscriptSeq>,
    ) -> Result<Option<ProjectionGeneration>, RepositoryError>;
    async fn begin_compaction(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        intent: &CompactIntent,
    ) -> Result<(), RepositoryError>;
    async fn settle_compaction(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        intent: &CompactIntent,
        replacement: Option<&[Message]>,
        reason: &str,
    ) -> Result<ProjectionGeneration, RepositoryError>;
    async fn admit_compaction(
        &self,
        request: &CompactRunRequest,
        snapshot: &RunSnapshot,
        authorization: &AuthorizationRequest,
        verifier: std::sync::Arc<dyn AuthorizationVerifier>,
    ) -> Result<(Admission, ExactOwner), RepositoryError>;
    async fn read_compact_fact(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Option<RunCompactReadback>, RepositoryError>;
    async fn read_compact_request(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        operation: &str,
    ) -> Result<Option<CompactRunRequest>, RepositoryError>;
    async fn read_compact_operation(
        &self,
        scope: &TenantContext,
        request: &CompactRunRequest,
    ) -> Result<Option<RunRecord>, RepositoryError>;
}

/// 原发现许可与取消请求；固定准入proof，历史只核永久归属。
#[async_trait::async_trait]
pub trait RunDiscoveryRepository: Send + Sync {
    async fn publish_run_discovery(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        receipt: &ToolDiscoveryReceipt,
    ) -> Result<ToolDiscoveryReceipt, RepositoryError>;
    async fn read_discovered_tool(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        generation: &str,
        name: &str,
    ) -> Result<bool, RepositoryError>;
    async fn read_run_discoveries(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Vec<ToolDiscoveryReceipt>, RepositoryError>;
    async fn request_run_cancellation(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<(), RepositoryError>;
}

/// 原 Hook 观察事实；读回核永久归属，ACK 携带实际交付的原 publication 身份。
#[async_trait::async_trait]
pub trait HookObservationRepository: Send + Sync {
    async fn read_hook_history(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
        after: u64,
        limit: usize,
    ) -> Result<HookReadback, RepositoryError>;
    async fn read_pending_hooks(
        &self,
        scope: &TenantContext,
    ) -> Result<Vec<HookPublication>, RepositoryError>;
    async fn acknowledge_pending_hook(
        &self,
        scope: &TenantContext,
        publication: &HookPublication,
    ) -> Result<(), RepositoryError>;
}

/// 原Stop续跑准入与永久关系读回，不创建第二业务状态。
#[async_trait::async_trait]
pub trait HookContinuationRepository: Send + Sync {
    async fn admit_stop_continuation(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        operation: &str,
        prompt: &str,
        snapshot: &RunSnapshot,
    ) -> Result<Admission, RepositoryError>;
    async fn read_hook_parent(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Option<RunId>, RepositoryError>;
    async fn read_hook_child(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Option<RunId>, RepositoryError>;
}

/// 实际Hook actor使用的冻结读取和原幂等claim/settle，不保存进程状态副本。
#[async_trait::async_trait]
pub trait HookExecutionRepository: Send + Sync {
    async fn read_hook_snapshot(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<RunSnapshot, RepositoryError>;
    async fn read_hook_interaction(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        id: &InteractionId,
    ) -> Result<Option<InteractionRecord>, RepositoryError>;
    async fn claim_hook_effect(
        &self,
        scope: &TenantContext,
        envelope: Option<&EnterpriseOwnerEnvelope>,
        payload: &HookPayload,
    ) -> Result<HookClaim, RepositoryError>;
    async fn settle_hook_effect(
        &self,
        scope: &TenantContext,
        envelope: Option<&EnterpriseOwnerEnvelope>,
        outcome: &HookOutcome,
    ) -> Result<HookOutcome, RepositoryError>;
}
/// 停止入口并join执行体后，实际shutdown等待已接受的数据库worker收敛。
#[async_trait::async_trait]
pub trait StorageWorkDrainRepository: Send + Sync {
    async fn drain_admitted_work(&self) -> Result<(), CoordinationError>;
}

/// 原记忆/学习领域；原source_run隔离，曝光仅为材料准备证据。
#[async_trait::async_trait]
pub trait ScopedMemoryRepository: Send + Sync {
    async fn read_memory_settings(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<RunSnapshot, RepositoryError>;
    async fn read_memory_candidates(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        visibility: &MemoryVisibility,
    ) -> Result<Vec<MemoryRecord>, RepositoryError>;
    async fn read_memory_page(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        visibility: &MemoryVisibility,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MemoryRecord>, RepositoryError>;
    async fn read_memory_evidence(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        visibility: &MemoryVisibility,
        id: &str,
        after: usize,
        limit: usize,
    ) -> Result<MemoryEvidencePage, RepositoryError>;
    async fn read_memory_assessments(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        visibility: &MemoryVisibility,
    ) -> Result<Vec<MemoryAssessment>, RepositoryError>;
    async fn read_memory_flywheel(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        lifetime: &SessionLifetimeId,
    ) -> Result<serde_json::Value, RepositoryError>;
    async fn store_run_memory(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        entry: &MemoryRecord,
    ) -> Result<MemoryRecord, RepositoryError>;
    async fn forget_run_memory(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        visibility: &MemoryVisibility,
        id: &str,
        revision: u64,
    ) -> Result<bool, RepositoryError>;
    async fn record_run_memory_exposures(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        entries: &[MemoryExposure],
        channel: &str,
        policy: &str,
    ) -> Result<(), RepositoryError>;
    async fn record_run_memory_feedback(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        operation: &str,
        id: &str,
        feedback: MemoryFeedback,
    ) -> Result<(), RepositoryError>;
    async fn ingest_run_memory(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<(), RepositoryError>;
    async fn record_memory_maintenance(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        outcome: &str,
    ) -> Result<(), RepositoryError>;
}

/// 原摄入恢复与退避记录，有限扫描，不形成新的维护队列。
#[async_trait::async_trait]
pub trait MemoryMaintenanceRepository: Send + Sync {
    async fn maintain_memory_turns(
        &self,
        scope: &TenantContext,
        limit: usize,
    ) -> Result<(usize, Option<i64>), RepositoryError>;
}

/// 原后台资源状态机、启动身份和持久日志，不保存进程PID影子终态。
#[async_trait::async_trait]
pub trait RunResourceRepository: Send + Sync {
    async fn read_resource_settings(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
    ) -> Result<RunSnapshot, RepositoryError>;
    async fn create_run_resource(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        cwd: &str,
        requested: &str,
        effective: &str,
    ) -> Result<ResourceRecord, RepositoryError>;
    async fn update_run_resource(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        id: ResourceId,
        state: &str,
        identity: Option<&str>,
        reason: Option<&str>,
    ) -> Result<ResourceRecord, RepositoryError>;
    async fn append_run_resource_log(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        id: ResourceId,
        content: &str,
    ) -> Result<u64, RepositoryError>;
    async fn reconcile_run_resource(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        id: ResourceId,
        state: &str,
        evidence: &str,
    ) -> Result<ResourceRecord, RepositoryError>;
    async fn read_resource_record(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        life: &SessionLifetimeId,
        id: ResourceId,
    ) -> Result<ResourceRecord, RepositoryError>;
    async fn read_resource_page(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        life: &SessionLifetimeId,
        after: ResourceId,
        limit: usize,
    ) -> Result<Vec<ResourceRecord>, RepositoryError>;
    async fn read_resource_log_page(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        life: &SessionLifetimeId,
        id: ResourceId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<(u64, String)>, RepositoryError>;
}

/// 原interaction事实，回答持固定owner与revision CAS，EOF不是回答。
#[async_trait::async_trait]
pub trait RunInteractionRepository: Send + Sync {
    async fn respond_interaction_authorized(
        &self,
        request: &AuthorizationRequest,
        envelope: &EnterpriseOwnerEnvelope,
        id: &InteractionId,
        revision: i64,
        approved: bool,
        verifier: std::sync::Arc<dyn AuthorizationVerifier>,
    ) -> Result<InteractionRecord, RepositoryError>;
    async fn read_interaction_record(
        &self,
        scope: &TenantContext,
        id: &InteractionId,
    ) -> Result<Option<InteractionRecord>, RepositoryError>;
    async fn read_interaction_list(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
    ) -> Result<Vec<InteractionRecord>, RepositoryError>;
    async fn respond_run_interaction(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        id: &InteractionId,
        revision: i64,
        approved: bool,
    ) -> Result<InteractionRecord, RepositoryError>;
    async fn read_interaction_stamp(
        &self,
        scope: &TenantContext,
        id: &InteractionId,
    ) -> Result<Option<ViewStamp>, RepositoryError>;
}

/// 原持久委派与结果reservation，范围验证在加载child正文前完成。
#[async_trait::async_trait]
pub trait RunDelegationRepository: Send + Sync {
    async fn read_delegation_record(
        &self,
        scope: &TenantContext,
        child: &RunId,
    ) -> Result<Option<DelegationRecord>, RepositoryError>;
    async fn read_scoped_delegation(
        &self,
        scope: &TenantContext,
        parent: &RunId,
        child: &RunId,
    ) -> Result<Option<DelegationRecord>, RepositoryError>;
    async fn read_delegation_list(
        &self,
        scope: &TenantContext,
        root: &RunId,
    ) -> Result<Vec<DelegationRecord>, RepositoryError>;
    async fn read_spawned_delegation(
        &self,
        scope: &TenantContext,
        parent: &RunId,
        spawn: &str,
    ) -> Result<Option<DelegationRecord>, RepositoryError>;
    async fn read_session_delegation(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
    ) -> Result<Option<DelegationRecord>, RepositoryError>;
    async fn admit_child_delegation(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        request: &DelegationRequest,
    ) -> Result<DelegationRecord, RepositoryError>;
    async fn reserve_child_result(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        child: &RunId,
        owner: &str,
        revision: i64,
    ) -> Result<DelegationRecord, RepositoryError>;
    async fn transition_child_result(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        child: &RunId,
        owner: &str,
        revision: i64,
        target: &str,
    ) -> Result<DelegationRecord, RepositoryError>;
}

/// 原队列、请求去重与运行定位；所有查找都显式携带数据库租户范围。
#[async_trait::async_trait]
pub trait RunQueueRepository: Send + Sync {
    async fn read_queued_messages(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
    ) -> Result<Vec<QueuedMessage>, RepositoryError>;
    async fn read_queued_message(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        run: &RunId,
    ) -> Result<Option<QueuedMessage>, RepositoryError>;
    async fn read_recoverable_queue(
        &self,
        scope: &TenantContext,
        after: Option<QueueRecoveryCursor>,
        limit: usize,
    ) -> Result<QueueRecoveryPage, RepositoryError>;
    async fn lookup_request_run(
        &self,
        scope: &TenantContext,
        key: Option<&SessionKey>,
        request: &RequestId,
    ) -> Result<Option<RunRecord>, RepositoryError>;
    async fn read_run_record(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Option<RunRecord>, RepositoryError>;
}

/// 实际诊断和旧filesystem artifact读回，所有读取核永久归属，无需获取writer。
#[async_trait::async_trait]
pub trait RuntimeObservationRepository: Send + Sync {
    async fn read_run_transcript(
        &self,
        scope: &TenantContext,
        run: &RunId,
    ) -> Result<Vec<Message>, RepositoryError>;
    async fn read_context_summary(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
    ) -> Result<SessionContextSummary, RepositoryError>;
    async fn read_legacy_artifact(
        &self,
        scope: &TenantContext,
        owner: &ExactOwner,
        reference: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<u8>, RepositoryError>;
}

/// 原旧记忆隔离导入，材料不会自动升级为可信记忆。
#[async_trait::async_trait]
pub trait LegacyMemoryImportRepository: Send + Sync {
    async fn import_legacy_memory_file(
        &self,
        scope: &TenantContext,
        source: &str,
        bytes: &[u8],
    ) -> Result<(), RepositoryError>;
}

/// 同一事实源中的审计链读取，完整scope过滤后校验封存JSON及哈希。
#[async_trait::async_trait]
pub trait AuditReadRepository: Send + Sync {
    async fn read_audit_entries(
        &self,
        scope: &TenantContext,
        after: u64,
        limit: u32,
    ) -> Result<Vec<AuditEntry>, CoordinationError>;
}
/// 配额预占与结算由原业务事务调用；结算不能从released恢复，也不能重复计费。
#[async_trait::async_trait]
pub trait QuotaRepository: Send + Sync {
    async fn read_quota_account(
        &self,
        scope: &TenantContext,
        metric: &str,
        window_start_ms: i64,
    ) -> Result<QuotaAccount, LedgerError>;
    async fn reserve_run_quota(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        operation: &str,
        metric: &str,
        amount: u64,
    ) -> Result<QuotaReservation, LedgerError>;
    async fn settle_run_quota(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        reservation: &QuotaReservation,
        state: QuotaState,
    ) -> Result<QuotaReservation, LedgerError>;
}
/// 两后端审计使用同一规范封存格式，摘要不包含自身。
pub fn seal_audit_entry(entry: &AuditEntry) -> Result<(String, String), CoordinationError> {
    if entry.seq == 0
        || entry.command.operation_id.is_empty()
        || entry.command.operation_id.len() > 1024
        || entry.command.action.is_empty()
        || entry.command.action.len() > 128
        || entry.command.outcome.is_empty()
        || entry.command.outcome.len() > 128
        || !entry.command.resource.is_object()
        || entry.previous_digest.len() != 64
        || !entry.previous_digest.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(CoordinationError::InvalidRequest);
    }
    let mut value = serde_json::to_value(entry).map_err(|_| CoordinationError::InvalidRequest)?;
    value
        .as_object_mut()
        .ok_or(CoordinationError::InvalidRequest)?
        .remove("digest");
    value.sort_all_objects();
    let raw = serde_json::to_string(&value).map_err(|_| CoordinationError::InvalidRequest)?;
    if raw.len() > 65536 {
        return Err(CoordinationError::InvalidRequest);
    }
    Ok((format!("{:x}", Sha256::digest(raw.as_bytes())), raw))
}
pub fn decode_audit_entry(raw: &str, digest: &str) -> Result<AuditEntry, CoordinationError> {
    if raw.len() > 65536
        || digest.len() != 64
        || format!("{:x}", Sha256::digest(raw.as_bytes())) != digest
    {
        return Err(CoordinationError::Corrupt);
    }
    let mut value: serde_json::Value =
        serde_json::from_str(raw).map_err(|_| CoordinationError::Corrupt)?;
    let fields = value.as_object_mut().ok_or(CoordinationError::Corrupt)?;
    if fields
        .insert("digest".into(), serde_json::Value::String(digest.into()))
        .is_some()
    {
        return Err(CoordinationError::Corrupt);
    }
    let entry: AuditEntry =
        serde_json::from_value(value).map_err(|_| CoordinationError::Corrupt)?;
    let (expected, _) = seal_audit_entry(&entry).map_err(|_| CoordinationError::Corrupt)?;
    if expected != digest {
        return Err(CoordinationError::Corrupt);
    }
    Ok(entry)
}

/// 两种后端共用计划定义及 Markdown 身份校验。
pub fn validate_plan_document(document: &PlanDocument) -> Result<(), CoordinationError> {
    use sha2::{Digest, Sha256};
    document
        .validate()
        .map_err(|_| CoordinationError::Corrupt)?;
    let bytes = document
        .definition
        .canonical_bytes()
        .map_err(|_| CoordinationError::Corrupt)?;
    let markdown = document.markdown();
    let artifact = document
        .artifact
        .as_ref()
        .ok_or(CoordinationError::Corrupt)?;
    if document.content_digest != format!("{:x}", Sha256::digest(bytes))
        || artifact.byte_length != markdown.len()
        || artifact.content_digest != format!("{:x}", Sha256::digest(markdown.as_bytes()))
        || artifact.media_type != "text/markdown; charset=utf-8"
    {
        return Err(CoordinationError::Corrupt);
    }
    Ok(())
}

pub fn stage_plan_document(
    owner: &ExactOwner,
    current: &PlanSnapshot,
    value: &serde_json::Value,
) -> Result<PlanDocument, CoordinationError> {
    use sha2::{Digest, Sha256};
    let definition: PlanDefinition =
        serde_json::from_value(value.clone()).map_err(|_| CoordinationError::InvalidRequest)?;
    let digest = format!(
        "{:x}",
        Sha256::digest(
            definition
                .canonical_bytes()
                .map_err(|_| CoordinationError::InvalidRequest)?
        )
    );
    let previous = if current.value.get("plan_id").is_some() {
        let doc: PlanDocument = serde_json::from_value(current.value.clone())
            .map_err(|_| CoordinationError::Corrupt)?;
        validate_plan_document(&doc)?;
        if doc.revision != current.revision {
            return Err(CoordinationError::Corrupt);
        }
        Some(doc)
    } else {
        let old: PlanDefinition = serde_json::from_value(current.value.clone())
            .map_err(|_| CoordinationError::Corrupt)?;
        old.canonical_bytes()
            .map_err(|_| CoordinationError::Corrupt)?;
        None
    };
    let changed = previous
        .as_ref()
        .is_none_or(|doc| doc.content_digest != digest);
    let revision = if changed {
        current
            .revision
            .checked_add(1)
            .filter(|value| *value <= i64::MAX as u64)
            .ok_or(CoordinationError::Conflict)?
    } else {
        current.revision
    };
    let plan_id = previous.as_ref().map_or_else(
        || format!("plan:{}:{}", owner.session_lifetime_id.0, owner.run_id.0),
        |doc| doc.plan_id.clone(),
    );
    let review = if changed {
        PlanReviewStatus::PendingReview
    } else {
        previous
            .as_ref()
            .map_or(PlanReviewStatus::PendingReview, |doc| doc.review)
    };
    let mut document = PlanDocument {
        plan_id,
        revision,
        content_digest: digest,
        definition,
        review,
        artifact: None,
    };
    let markdown = document.markdown();
    document.artifact = Some(PlanArtifact {
        content_digest: format!("{:x}", Sha256::digest(markdown.as_bytes())),
        byte_length: markdown.len(),
        media_type: "text/markdown; charset=utf-8".into(),
    });
    Ok(document)
}

/// 发现许可只包含固定目录中的精确工具，不由索引缓存授予执行权。
pub fn validate_discovery_receipt(
    receipt: &ToolDiscoveryReceipt,
    snapshot: &RunSnapshot,
    owner: &ExactOwner,
) -> Result<(), CoordinationError> {
    if receipt.schema_version != 1
        || receipt.owner != *owner
        || receipt.operation_id.is_empty()
        || receipt.operation_id.len() > 512
        || receipt.result.schema_version != 1
        || receipt.result.catalog_generation != snapshot.tool_catalog_digest
        || receipt.query.query.trim().is_empty()
        || receipt.query.query.len() > 2048
        || !(1..=TOOL_SEARCH_LIMIT).contains(&receipt.query.limit.unwrap_or(5))
        || receipt.result.tools.len() > receipt.query.limit.unwrap_or(5)
        || serde_json::to_vec(&receipt.result)
            .map_err(|_| CoordinationError::Corrupt)?
            .len()
            > TOOL_SEARCH_BYTES
        || receipt
            .result
            .tools
            .iter()
            .any(|tool| !snapshot.tools.contains(tool))
        || receipt
            .result
            .tools
            .iter()
            .map(|tool| &tool.name)
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != receipt.result.tools.len()
    {
        return Err(CoordinationError::Corrupt);
    }
    Ok(())
}

pub fn validate_frozen_tool_catalog(
    snapshot: &RunSnapshot,
    generation: &str,
) -> Result<(), CoordinationError> {
    let digest = format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&snapshot.tools).map_err(|_| CoordinationError::Corrupt)?
        )
    );
    if digest != snapshot.tool_catalog_digest
        || snapshot
            .tools
            .windows(2)
            .any(|pair| pair[0].name >= pair[1].name)
    {
        return Err(CoordinationError::Corrupt);
    }
    if digest != generation {
        return Err(CoordinationError::Conflict);
    }
    Ok(())
}

pub fn verify_artifact_receipt(
    identity: &ArtifactIdentity,
    receipt: &serde_json::Value,
    required: bool,
) -> Result<(), ArtifactError> {
    if !receipt.is_object()
        || (required
            && (receipt.get("output_sha256").is_none() || receipt.get("output_bytes").is_none()))
        || receipt
            .get("output_sha256")
            .is_some_and(|value| value.as_str() != Some(identity.sha256.as_str()))
        || receipt
            .get("output_bytes")
            .is_some_and(|value| value.as_u64() != Some(identity.bytes))
    {
        return Err(ArtifactError::Corrupt);
    }
    Ok(())
}

pub fn verify_artifact_completion(
    reservation: &ObjectArtifactReservation,
    completion: &ToolArtifactCompletion,
) -> Result<(), ArtifactError> {
    if !reservation.valid()
        || completion.call_id != reservation.call_id
        || completion.round != reservation.round
        || !matches!(
            completion.outcome.as_str(),
            "success" | "tool_error" | "outcome_unknown"
        )
    {
        return Err(ArtifactError::InvalidReference);
    }
    verify_artifact_receipt(&reservation.identity, &completion.receipt, true)?;
    let mut value =
        serde_json::to_value(completion).map_err(|_| ArtifactError::InvalidReference)?;
    value.sort_all_objects();
    if format!("{:x}", Sha256::digest(value.to_string().as_bytes()))
        != reservation.completion_digest
    {
        return Err(ArtifactError::Conflict);
    }
    Ok(())
}

pub fn request_route_matches(
    snapshot: &RunSnapshot,
    capture: &ProviderRequestCapture,
) -> Result<bool, CoordinationError> {
    let Some(route) = &snapshot.route else {
        return Ok(true);
    };
    let Some(candidate) = route.candidates.get(capture.candidate_index as usize) else {
        return Ok(false);
    };
    let identity = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(candidate).map_err(|_| CoordinationError::Corrupt)?)
    );
    Ok(capture.input.route == candidate.model && capture.input.provider_identity == identity)
}

pub fn validate_frozen_request(
    snapshot: &RunSnapshot,
    capture: &ProviderRequestCapture,
    context: Option<&ContextEnvelope>,
) -> Result<(), CoordinationError> {
    let catalog_digest = format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&capture.input.catalog).map_err(|_| CoordinationError::Corrupt)?
        )
    );
    let policy_digest = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(snapshot).map_err(|_| CoordinationError::Corrupt)?)
    );
    if capture.schema_version != 1
        || capture.round == 0
        || capture.capture_id.is_empty()
        || capture.capture_id.len() > 256
        || capture.input.source.source_start.0 != 0
        || capture.policy_fingerprint.len() != 64
        || capture.request_digest.len() != 64
        || !request_route_matches(snapshot, capture)?
        || snapshot.tools != capture.input.catalog
        || snapshot.tool_catalog_digest != catalog_digest
        || snapshot.context_token_budget as u64 != capture.input.budget
        || capture.policy_fingerprint != policy_digest
        || snapshot.context_policy_fingerprint.as_deref()
            != Some(capture.input.source.policy_fingerprint.as_str())
    {
        return Err(CoordinationError::Corrupt);
    }
    if let Some(context) = context {
        let catalog_value =
            serde_json::to_value(&capture.input.catalog).map_err(|_| CoordinationError::Corrupt)?;
        let context_digest = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&catalog_value).map_err(|_| CoordinationError::Corrupt)?
            )
        );
        if context.source != capture.input.source
            || context.route != capture.input.route
            || context.provider_identity != capture.input.provider_identity
            || context.budget != capture.input.budget
            || context.tool_catalog_digest != context_digest
        {
            return Err(CoordinationError::Corrupt);
        }
    }
    Ok(())
}

/// 管理操作仍在canonical事务内核验策略，不能由Gateway写派生预算。
#[async_trait::async_trait]
pub trait QuotaManagementRepository: Send + Sync {
    async fn configure_quota_window(
        &self,
        request: &AuthorizationRequest,
        account: &QuotaAccount,
        verifier: std::sync::Arc<dyn AuthorizationVerifier>,
    ) -> Result<(), RepositoryError>;
}
/// 管理面身份写入；与原principals/role bindings/policy heads共用事实源。
#[async_trait::async_trait]
pub trait IdentityManagementRepository: Send + Sync {
    async fn manage_identity(
        &self,
        request: &AuthorizationRequest,
        command: &IdentityCommand,
        operation: &str,
        verifier: std::sync::Arc<dyn AuthorizationVerifier>,
    ) -> Result<IdentityManagementReceipt, RepositoryError>;
}

/// 包与配置治理仍使用canonical数据库；使用面只能读取原session固定代。
#[async_trait::async_trait]
pub trait RegistryRepository:Send+Sync {
    async fn mutate_registry(&self,request:&AuthorizationRequest,mutation:&RegistryMutation,verifier:std::sync::Arc<dyn AuthorizationVerifier>)->Result<RegistryReceipt,RepositoryError>;
    async fn read_registry(&self,scope:&TenantContext)->Result<RegistryReadback,RepositoryError>;
    async fn read_session_generation(&self,scope:&TenantContext,key:&SessionKey,life:&SessionLifetimeId)->Result<Option<InstalledGeneration>,RepositoryError>;
}

/// DAG编排与原native事实的同库端口；lease不是另一份run/approval状态。
#[async_trait::async_trait]
pub trait WorkflowRepository:Send+Sync {
    async fn bind_workflow_session(&self,request:&AuthorizationRequest,lease:&WorkflowNodeLease,key:&SessionKey,verifier:std::sync::Arc<dyn AuthorizationVerifier>)->Result<WorkflowNodeRecord,RepositoryError>;
    async fn submit_workflow_message(&self,request:&AuthorizationRequest,message:&WorkflowMessage,verifier:std::sync::Arc<dyn AuthorizationVerifier>)->Result<WorkflowInstance,RepositoryError>;
    async fn start_workflow_child(&self,request:&AuthorizationRequest,lease:&WorkflowNodeLease,verifier:std::sync::Arc<dyn AuthorizationVerifier>)->Result<WorkflowInstance,RepositoryError>;
    async fn publish_workflow(&self,request:&AuthorizationRequest,command:&WorkflowPublish,verifier:std::sync::Arc<dyn AuthorizationVerifier>)->Result<FrozenWorkflowDefinition,RepositoryError>;
    async fn start_workflow(&self,request:&AuthorizationRequest,command:&WorkflowStart,verifier:std::sync::Arc<dyn AuthorizationVerifier>)->Result<WorkflowInstance,RepositoryError>;
    async fn control_workflow(&self,request:&AuthorizationRequest,command:&WorkflowControl,verifier:std::sync::Arc<dyn AuthorizationVerifier>)->Result<WorkflowInstance,RepositoryError>;
    async fn read_workflow(&self,scope:&TenantContext,id:&str,after:u64,limit:u32)->Result<WorkflowReadback,RepositoryError>;
    async fn claim_workflow_nodes(&self,request:&AuthorizationRequest,executor:&str,limit:u32,verifier:std::sync::Arc<dyn AuthorizationVerifier>)->Result<Vec<WorkflowClaim>,RepositoryError>;
    async fn renew_workflow_node(&self,request:&AuthorizationRequest,lease:&WorkflowNodeLease,verifier:std::sync::Arc<dyn AuthorizationVerifier>)->Result<(),RepositoryError>;
    async fn finish_workflow_node(&self,request:&AuthorizationRequest,lease:&WorkflowNodeLease,result:&WorkflowNodeResult,verifier:std::sync::Arc<dyn AuthorizationVerifier>)->Result<WorkflowNodeRecord,RepositoryError>;
}
