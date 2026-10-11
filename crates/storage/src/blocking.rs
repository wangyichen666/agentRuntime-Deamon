//! 所有已接线 async SQLite 端口共享有界执行预算；SQL owner 仍属于原领域事务。
use crate::RunStore;

impl RunStore {
    pub(crate) async fn sqlite_blocking<T, E>(
        &self,
        unavailable: E,
        operation: impl FnOnce(Self) -> Result<T, E> + Send + 'static,
    ) -> Result<T, E>
    where
        T: Send + 'static,
        E: Clone + Send + 'static,
    {
        let permit = self
            .blocking_budget
            .clone()
            .try_acquire_owned()
            .map_err(|_| unavailable.clone())?;
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            // caller 取消仅 detach 等待；已经开始的 SQL 至提交/回滚仍占有预算。
            let _permit = permit;
            operation(store)
        })
        .await
        .map_err(|_| unavailable)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_core::*;
    use agent_storage_ports::{
        ArtifactPublicationRepository, HookObservationRepository, IdentityReadRepository,
        PublicationRepository, RunAttemptRepository, RunContextRepository, RunDiscoveryRepository,
        RunEventRepository, RunExecutionRepository, RunFactRepository, RunRequestRepository,
        RunToolRepository, RunTurnRepository, SessionLifecycleRepository, SessionReadRepository,
        SessionWriterRepository,
    };
    use std::sync::{Arc, Condvar, Mutex};

    struct BudgetVerifier;
    impl AuthorizationVerifier for BudgetVerifier {
        fn verify(
            &self,
            _: &AuthorizationRequest,
            _: &IdentityReadback,
        ) -> Result<(), AuthorizationError> {
            Err(AuthorizationError::Forbidden)
        }
    }
    struct ReleaseOnDrop(Arc<(Mutex<bool>, Condvar)>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            let (lock, signal) = &*self.0;
            *lock.lock().unwrap_or_else(|error| error.into_inner()) = true;
            signal.notify_all();
        }
    }

    #[tokio::test]
    async fn cancelled_callers_keep_the_shared_sqlite_budget_until_real_work_finishes() {
        let root = std::env::temp_dir().join(format!("ma-adapter-budget-{}", std::process::id()));
        let store = RunStore::open(&root.join("runtime.sqlite3")).unwrap();
        let Admission::New(run) = store
            .admit_with_route(
                SessionKey("budget".into()),
                RequestId::Number(1),
                "budget",
                AdmissionMode::Queue,
                None,
            )
            .unwrap()
        else {
            panic!("new");
        };
        let envelope = EnterpriseOwnerEnvelope {
            scope: TenantContext::local(),
            owner: crate::SessionQuery::run_owner(&store, &run.run_id).unwrap(),
        };
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let _release = ReleaseOnDrop(gate.clone());
        let (started_tx, mut started_rx) = tokio::sync::mpsc::channel(4);
        let mut callers = Vec::new();
        for _ in 0..4 {
            let store = store.clone();
            let gate = gate.clone();
            let started = started_tx.clone();
            callers.push(tokio::spawn(async move {
                store
                    .sqlite_blocking(CoordinationError::StorageUnavailable, move |_| {
                        started.blocking_send(()).unwrap();
                        let (lock, signal) = &*gate;
                        let mut ready = lock.lock().unwrap();
                        while !*ready {
                            ready = signal.wait(ready).unwrap();
                        }
                        Ok(())
                    })
                    .await
            }));
        }
        for _ in 0..4 {
            started_rx.recv().await.unwrap();
        }
        assert_eq!(store.blocking_budget.available_permits(), 0);
        for caller in callers {
            caller.abort();
            assert!(caller.await.unwrap_err().is_cancelled());
        }
        assert_eq!(
            store.read_publications(&TenantContext::local(), 0, 1).await,
            Err(CoordinationError::StorageUnavailable)
        );
        assert_eq!(
            store.renew_bound_writers().await,
            Err(CoordinationError::StorageUnavailable)
        );
        assert!(matches!(
            store
                .read_session(
                    &TenantContext::local(),
                    &envelope.owner.session_key,
                    HistoryReadMode::Canonical
                )
                .await,
            Err(CoordinationError::StorageUnavailable)
        ));
        assert!(matches!(
            store.list_sessions(&TenantContext::local(), None, 1).await,
            Err(CoordinationError::StorageUnavailable)
        ));
        assert!(matches!(
            store
                .read_identity(
                    &TenantContext::local(),
                    &"local-subject".to_owned().try_into().unwrap()
                )
                .await,
            Err(RepositoryError::Coordination(
                CoordinationError::StorageUnavailable
            ))
        ));
        assert!(matches!(
            store.read_authorization(&envelope).await,
            Err(RepositoryError::Coordination(
                CoordinationError::StorageUnavailable
            ))
        ));
        assert!(matches!(
            store
                .commit_session_command(
                    &TenantContext::local(),
                    &SessionCommand::Close {
                        key: envelope.owner.session_key.clone()
                    },
                    Some(&envelope.owner.session_lifetime_id),
                    "overloaded"
                )
                .await,
            Err(CoordinationError::StorageUnavailable)
        ));
        assert!(matches!(
            store
                .read_close_receipt(
                    &TenantContext::local(),
                    &envelope.owner.session_key,
                    &envelope.owner.session_lifetime_id,
                    "overloaded"
                )
                .await,
            Err(CoordinationError::StorageUnavailable)
        ));
        assert!(matches!(
            store
                .read_run_events(
                    &TenantContext::local(),
                    &envelope.owner.run_id,
                    EventSeq(0),
                    1
                )
                .await,
            Err(CoordinationError::StorageUnavailable)
        ));
        assert_eq!(
            store
                .read_run_fact(&TenantContext::local(), &envelope.owner.run_id)
                .await
                .unwrap_err(),
            CoordinationError::StorageUnavailable
        );
        assert_eq!(
            store
                .read_tool_receipts(&TenantContext::local(), &envelope.owner.run_id)
                .await
                .unwrap_err(),
            CoordinationError::StorageUnavailable
        );
        assert_eq!(
            store
                .read_provider_history(&TenantContext::local(), &envelope.owner.run_id)
                .await
                .unwrap_err(),
            CoordinationError::StorageUnavailable
        );
        let snapshot: RunSnapshot=serde_json::from_value(serde_json::json!({"cwd":".","tools":[],"permission_mode":"risk","sandbox_requested":"native","sandbox_effective":"native","context_read_only":false,"config_generation":0})).unwrap();
        let admission = RunAdmission {
            workflow_node: None,
            plan_execution: None,
            session_key: envelope.owner.session_key.clone(),
            expected_lifetime: Some(envelope.owner.session_lifetime_id.clone()),
            request_id: RequestId::Number(2),
            input: "预算".into(),
            mode: AdmissionMode::Queue,
        };
        let authorization = AuthorizationRequest {
            context: RequestContext {
                scope: TenantContext::local(),
                principal: AuthenticatedPrincipal {
                    subject_id: "local-subject".to_owned().try_into().unwrap(),
                    kind: SubjectKind::Human,
                    revocation_epoch: 0,
                },
                request_id: admission.request_id.clone(),
                trace: TraceContext {
                    trace_id: "budget".into(),
                },
            },
            expected_policy: PolicyDocument::local().version,
            action: Action::SessionPrompt,
            attributes: AuthorizationAttributes {
                workspace: ".".into(),
                entry_channel: HookChannel::DaemonRpc,
                tool_effect: None,
                timestamp_ms: 0,
                network: NetworkBoundary::None,
            },
        };
        assert_eq!(
            store
                .admit_run_authorized(
                    &admission,
                    &snapshot,
                    &authorization,
                    Arc::new(BudgetVerifier)
                )
                .await
                .unwrap_err(),
            RepositoryError::Coordination(CoordinationError::StorageUnavailable)
        );
        assert_eq!(
            store
                .admit_workload_run(
                    &admission,
                    &snapshot,
                    &authorization,
                    Arc::new(BudgetVerifier)
                )
                .await
                .unwrap_err(),
            CoordinationError::StorageUnavailable.into()
        );
        assert_eq!(
            store.start_queued_run(&envelope).await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        assert_eq!(
            store.cancel_queued_run(&envelope).await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        let commit = TurnCommit {
            owner: envelope.owner.clone(),
            status: RunStatus::Completed,
            content: Some("output".into()),
            error: None,
        };
        assert_eq!(
            store.commit_run_turn(&envelope, &commit).await.unwrap_err(),
            RepositoryError::Coordination(CoordinationError::StorageUnavailable)
        );
        let mut attempt = ProviderAttempt {
            attempt_id: "budget-attempt".into(),
            run_id: envelope.owner.run_id.clone(),
            round: 1,
            candidate_index: 0,
            provider_profile_id: "budget".into(),
            api_type: ApiType::Ollama,
            model: "budget".into(),
            status: AttemptStatus::Started,
            error_kind: None,
            diagnostic: None,
            retry_after_ms: None,
            stream_committed: false,
            started_at_ms: 1,
            first_event_at_ms: None,
            finished_at_ms: None,
            usage: None,
        };
        assert_eq!(
            store.start_run_attempt(&envelope, &attempt).await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        attempt.status = AttemptStatus::Succeeded;
        attempt.finished_at_ms = Some(2);
        assert_eq!(
            store.finish_run_attempt(&envelope, &attempt).await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        assert_eq!(
            store.read_attempt_usage(&envelope).await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        assert_eq!(
            store
                .append_provider_event(&envelope, "provider_retry", &serde_json::json!({}))
                .await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        use sha2::{Digest, Sha256};
        let tool = ToolCall {
            id: "budget-tool".into(),
            name: "budget-tool".into(),
            arguments: serde_json::json!({}),
        };
        let prepared = [(
            tool.clone(),
            "external_side_effect".into(),
            format!("{:x}", Sha256::digest(b"{}")),
            false,
        )];
        assert_eq!(
            store
                .prepare_run_tool_batch(&envelope, 1, &prepared, &[ToolDescriptor::external()])
                .await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        assert_eq!(
            store.start_run_tool(&envelope, 1, &tool.id).await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        let messages = [
            Message::assistant_tool_calls(vec![tool.clone()]),
            Message::tool_result(&tool, "result"),
        ];
        assert_eq!(
            store
                .read_run_request_source(&envelope, "budget", "identity")
                .await
                .unwrap_err(),
            CoordinationError::StorageUnavailable.into()
        );
        let source = ContextSource {
            lifetime: envelope.owner.session_lifetime_id.clone(),
            source_start: TranscriptSeq(0),
            source_end: TranscriptSeq(0),
            prefix_digest: "a".repeat(64),
            generation: ProjectionGeneration(0),
            policy_fingerprint: "budget".into(),
            pressure_route: "budget".into(),
            summary_route: "budget".into(),
        };
        let capture = ProviderRequestCapture {
            schema_version: 1,
            capture_id: "budget".into(),
            owner: envelope.owner.clone(),
            round: 1,
            candidate_index: 0,
            snapshot_revision: SnapshotRevision(0),
            input: ProviderRequestInput {
                purpose: ProviderRequestPurpose::Model,
                source: source.clone(),
                messages: vec![],
                tools: vec![],
                catalog: vec![],
                route: "budget".into(),
                provider_identity: "identity".into(),
                images: false,
                tool_calls: false,
                budget: 4096,
                calibration: None,
            },
            policy_fingerprint: "a".repeat(64),
            request_digest: "b".repeat(64),
        };
        let context = ContextEnvelope {
            source,
            route: "budget".into(),
            provider_identity: "identity".into(),
            tool_catalog_digest: "a".repeat(64),
            stable_tokens: 0,
            history_tokens: 0,
            retrieved_tokens: 0,
            overlay_tokens: 0,
            calibrated_input_tokens: 0,
            output_reserve: 0,
            budget: 4096,
        };
        assert_eq!(
            store
                .capture_run_request(&envelope, &capture, &context)
                .await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        assert_eq!(
            store
                .read_provider_request(
                    &envelope.scope,
                    &envelope.owner.session_key,
                    &envelope.owner.session_lifetime_id,
                    None,
                    None
                )
                .await
                .unwrap_err(),
            CoordinationError::StorageUnavailable.into()
        );
        assert_eq!(
            store.read_run_plan(&envelope).await.unwrap_err(),
            CoordinationError::StorageUnavailable.into()
        );
        assert_eq!(
            store
                .stage_run_plan(&envelope, 0, &serde_json::json!({"steps":[]}))
                .await
                .unwrap_err(),
            CoordinationError::StorageUnavailable.into()
        );
        assert_eq!(
            store.start_run_plan(&envelope).await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        assert_eq!(
            store
                .reject_run_tool_batch(&envelope, 1, std::slice::from_ref(&tool), "reject")
                .await
                .unwrap_err(),
            CoordinationError::StorageUnavailable.into()
        );
        assert_eq!(
            store
                .stage_run_assistant(&envelope, &Message::text(Role::Assistant, "output"))
                .await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        assert_eq!(
            store
                .append_run_transcript(&envelope, "overload", &messages)
                .await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        assert_eq!(
            store.close_run_tool_exchange(&envelope, 1, &messages).await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        assert_eq!(
            store.finish_run_tool_batch(&envelope, 1).await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        assert_eq!(
            store.read_tool_artifact_binding(&envelope).await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        assert_eq!(
            store
                .read_run_view_event(&envelope.scope, &envelope.owner.run_id, "terminal", None)
                .await
                .unwrap_err(),
            CoordinationError::StorageUnavailable
        );
        assert_eq!(
            store
                .append_run_live_event(
                    &envelope,
                    "text_delta",
                    &serde_json::json!({"delta":"共享预算"})
                )
                .await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        let discovery = ToolDiscoveryReceipt {
            schema_version: 1,
            owner: envelope.owner.clone(),
            operation_id: "shared-discovery".into(),
            query: ToolSearchQuery {
                query: "list".into(),
                limit: Some(1),
            },
            result: ToolSearchIndex::new(Vec::new(), "0".repeat(64))
                .unwrap()
                .search(&ToolSearchQuery {
                    query: "list".into(),
                    limit: Some(1),
                })
                .unwrap(),
        };
        assert_eq!(
            store.publish_run_discovery(&envelope, &discovery).await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        assert_eq!(
            store
                .read_discovered_tool(&envelope, &"0".repeat(64), "lookup")
                .await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        assert_eq!(
            store
                .read_run_discoveries(&envelope.scope, &envelope.owner.run_id)
                .await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        assert_eq!(
            store.request_run_cancellation(&envelope).await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        assert_eq!(
            store
                .read_hook_history(
                    &envelope.scope,
                    &envelope.owner.session_key,
                    &envelope.owner.session_lifetime_id,
                    0,
                    32
                )
                .await
                .unwrap_err(),
            CoordinationError::StorageUnavailable.into()
        );
        assert_eq!(
            store.read_pending_hooks(&envelope.scope).await.unwrap_err(),
            CoordinationError::StorageUnavailable.into()
        );
        let publication = HookPublication {
            session_key: envelope.owner.session_key.clone(),
            session_lifetime_id: envelope.owner.session_lifetime_id.clone(),
            event: HookEvent::SessionStart,
            operation_id: "shared-hook".into(),
        };
        assert_eq!(
            store
                .acknowledge_pending_hook(&envelope.scope, &publication)
                .await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        let intent = CompactIntent {
            operation: "budget-compact".into(),
            owner: envelope.owner.clone(),
            source: context.source.clone(),
        };
        let compact = CompactRunRequest {
            session_key: envelope.owner.session_key.clone(),
            session_lifetime_id: envelope.owner.session_lifetime_id.clone(),
            operation_id: intent.operation.clone(),
            expected_revision: TranscriptSeq(0),
            expected_projection_generation: ProjectionGeneration(0),
            compatibility_owner_run_id: None,
        };
        assert_eq!(
            store
                .read_exact_run_owner(&envelope.scope, &envelope.owner.run_id)
                .await
                .unwrap_err(),
            CoordinationError::StorageUnavailable
        );
        assert_eq!(
            store.read_execution_context(&envelope).await.unwrap_err(),
            CoordinationError::StorageUnavailable.into()
        );
        assert_eq!(
            store
                .read_compaction_result(&envelope, &intent.operation, None)
                .await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        assert_eq!(
            store.begin_compaction(&envelope, &intent).await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        assert_eq!(
            store
                .settle_compaction(&envelope, &intent, None, "no_gain")
                .await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        assert_eq!(
            store
                .admit_compaction(
                    &compact,
                    &snapshot,
                    &authorization,
                    Arc::new(BudgetVerifier)
                )
                .await
                .unwrap_err(),
            CoordinationError::StorageUnavailable.into()
        );
        assert_eq!(
            store
                .read_compact_fact(&envelope.scope, &envelope.owner.run_id)
                .await
                .unwrap_err(),
            CoordinationError::StorageUnavailable.into()
        );
        assert_eq!(
            store
                .read_compact_request(
                    &envelope.scope,
                    &compact.session_key,
                    &compact.session_lifetime_id,
                    &compact.operation_id
                )
                .await
                .unwrap_err(),
            CoordinationError::StorageUnavailable.into()
        );
        assert_eq!(
            store
                .read_compact_operation(&envelope.scope, &compact)
                .await
                .unwrap_err(),
            CoordinationError::StorageUnavailable.into()
        );
        let completion = ToolArtifactCompletion {
            round: 1,
            call_id: tool.id,
            outcome: "success".into(),
            receipt: serde_json::json!({"output":"result","output_sha256":format!("{:x}",Sha256::digest(b"result")),"output_bytes":6}),
        };
        assert_eq!(
            store
                .finish_file_tool_output(&envelope, &completion, "result".into())
                .await,
            Err(CoordinationError::StorageUnavailable.into())
        );
        assert_eq!(
            store
                .read_object_artifact(&envelope, "artifact:v1:invalid")
                .await,
            Err(ArtifactError::StorageUnavailable)
        );
        {
            let (lock, signal) = &*gate;
            *lock.lock().unwrap() = true;
            signal.notify_all();
        }
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while store.blocking_budget.available_permits() != 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            store
                .read_publications(&TenantContext::local(), 0, 1)
                .await
                .unwrap()
                .len(),
            1
        );
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
}
