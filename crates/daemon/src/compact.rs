//! compact 的唯一后台 command owner；入口只持 native run 身份。
use super::{ActiveKey, ActiveRequest, DaemonState};
use agent_core::*;
use agent_runtime::loop_engine::CancellationToken;
use agent_storage::RuntimeError;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::Arc;

impl DaemonState {
    pub(super) async fn start_compact_command(
        self: &Arc<Self>,
        request: CompactRunRequest,
        channel: HookChannel,
        wait: bool,
    ) -> Result<Value, (i64, String)> {
        let fail = |error: RepositoryError| (error.rpc_code(), error.to_string());
        if let Some(run) = self
            .run_request_store
            .read_compact_operation(&self.scope(), &request)
            .await
            .map_err(fail)?
        {
            return self.compact_command_response(&run.run_id, wait).await;
        }
        let session = self.session_runtime(Some(&request.session_key.0)).await?;
        let writer = session
            .writer
            .clone()
            .try_lock_owned()
            .map_err(|_| (-32001, "session 正在执行，compact busy".into()))?;
        let mut engine = session
            .engine
            .freeze_tools()
            .map_err(|error| (-32603, error.to_string()))?;
        let installed_generation=self.registry_store.read_session_generation(&self.scope(),&request.session_key,&session.lifetime).await.map_err(fail)?;
        if let Some(g)=&installed_generation {engine=engine.with_installed_generation(g).map_err(|e|(-32603,e.to_string()))?;}
        let route = self
            .provider_manager
            .as_ref()
            .map(|manager| manager.freeze(engine.token_budget())).map(|r|installed_generation.as_ref().map_or(Ok(r.clone()),|g|r.restrict_models(&g.effective.config.models))).transpose().map_err(|e|(-32603,e.to_string()))?;
        if let Some(route) = route.as_ref() {
            engine = engine.with_route(route.clone());
        }
        let engine = engine
            .for_compact()
            .map_err(|error| (-32603, error.to_string()))?;
        let snapshot = RunSnapshot {
            workflow: None,
            installed_generation,
            billing_policy:self.billing_store.read_billing_policy(&self.scope()).await.map_err(fail)?,
            artifact_store_id: None,
            entry_channel: channel,
            route: route.as_ref().map(|route| route.snapshot.clone()),
            tools: vec![],
            cwd: self.safety.as_ref().map_or_else(
                || ".".into(),
                |safety| safety.workspace().to_string_lossy().into_owned(),
            ),
            permission_mode: self
                .safety
                .as_ref()
                .map_or("risk_approval", |safety| safety.mode().key())
                .into(),
            sandbox_requested: "native".into(),
            sandbox_effective: "native".into(),
            sandbox_notice: None,
            docker_image: None,
            delegation_context: None,
            context_read_only: false,
            context_token_budget: engine.token_budget(),
            context_policy_fingerprint: Some(engine.context_policy_fingerprint()),
            tool_catalog_digest: format!("{:x}", Sha256::digest(b"[]")),
            memory_entry_budget: 0,
            memory_token_budget: 0,
            max_tool_calls: Some(0),
            config_generation: route
                .as_ref()
                .map_or(0, |route| route.snapshot.config_generation),
        };
        let authorization = self
            .authorize_local_request(
                RequestId::String(format!("compact:{}", request.operation_id)),
                Action::SessionPrompt,
            )
            .await
            .map_err(|error| match error {
                RuntimeError::Authorization(e) => (e.rpc_code(), e.to_string()),
                RuntimeError::Coordination(e) => (e.rpc_code(), e.to_string()),
                error => (-32603, error.to_string()),
            })?;
        let (admission, owner) = self
            .run_request_store
            .admit_compaction(
                &request,
                &snapshot,
                &authorization,
                Arc::new(agent_policy::PolicyEngine),
            )
            .await
            .map_err(fail)?;
        let run = match admission {
            Admission::Existing(run) => {
                return self.compact_command_response(&run.run_id, wait).await;
            }
            Admission::New(run) => run,
        };
        let envelope = EnterpriseOwnerEnvelope {
            scope: self.scope(),
            owner: owner.clone(),
        };
        let cancellation = CancellationToken::new();
        let active_key = ActiveKey {
            session_id: run.session_id.0.clone(),
            request_id: run.request_id.clone(),
        };
        self.run_coordinator.active.lock().await.insert(
            active_key.clone(),
            ActiveRequest::new(cancellation.clone(), run.run_id.clone()),
        );
        let (completed, receiver) = tokio::sync::oneshot::channel();
        let state = self.clone();
        let native = run.clone();
        let operation = request.operation_id.clone();
        let launch_key = active_key.clone();
        let task_envelope = envelope.clone();
        let launched = self
            .spawn_owned(async move {
                let _writer = writer;
                let result = agent_runtime::loop_engine::with_native_execution(
                    task_envelope.clone(),
                    agent_runtime::loop_engine::NativeExecutionPorts {
                        attempts: state.run_attempt_store.clone(),
                        tools: state.run_tool_store.clone(),
                        turns: state.run_turn_store.clone(),
                        requests: state.run_request_store.clone(),
                        discoveries: state.run_discovery_store.clone(),
                        memories: state.memory_store.clone(),
                    },
                    agent_runtime::hooks::with_scope(
                        state.hook_scope(&task_envelope),
                        engine.compact_session(&cancellation, &operation),
                    ),
                )
                .await;
                if let Ok(Some(read)) = state
                    .run_request_store
                    .read_compact_fact(&state.scope(), &native.run_id)
                    .await
                    && read.receipt.outcome == CompactRunOutcome::Started
                {
                    let reason = if cancellation.is_cancelled() {
                        "cancelled"
                    } else {
                        "summary_failed"
                    };
                    if let Err(error) = state
                        .run_request_store
                        .settle_compaction(
                            &task_envelope,
                            &CompactIntent {
                                operation: operation.clone(),
                                owner: task_envelope.owner.clone(),
                                source: read.receipt.source,
                            },
                            None,
                            reason,
                        )
                        .await
                    {
                        tracing::error!(%error,"compact fallback 结算失败");
                    }
                }
                if let Err(error) = result {
                    tracing::warn!(%error,run_id=%native.run_id.0,"compact 后台任务结束");
                }
                let response = state.compact_response(&native).await.map_err(fail);
                let events = state
                    .event_store
                    .read_run_events(&state.scope(), &native.run_id, EventSeq(0), 1000)
                    .await;
                let terminal = state
                    .terminal_update(&native.run_id, response.clone())
                    .await;
                let mut active = state.run_coordinator.active.lock().await;
                if let Some(active) = active.get_mut(&active_key) {
                    if let Ok(events) = events {
                        for event in events.events {
                            if let Some(update) = super::handlers::stored_event_update(event) {
                                active.publish_external(native.request_id.clone(), update);
                            }
                        }
                    }
                    active.publish_external(native.request_id.clone(), terminal);
                }
                drop(active);
                state
                    .run_coordinator
                    .active
                    .lock()
                    .await
                    .remove(&active_key);
                state.run_coordinator.queue_notify.notify_waiters();
                let _ = completed.send(response);
            })
            .await;
        if !launched {
            // 执行体未启动，因此没有未知 Provider 副作用；同 intent 结算失败并释放缓存。
            if let Some(read) = self
                .run_request_store
                .read_compact_fact(&self.scope(), &run.run_id)
                .await
                .map_err(fail)?
            {
                let fact = read.receipt;
                self.run_request_store
                    .settle_compaction(
                        &EnterpriseOwnerEnvelope {
                            scope: self.scope(),
                            owner: owner.clone(),
                        },
                        &CompactIntent {
                            operation: fact.operation_id,
                            owner: fact.owner,
                            source: fact.source,
                        },
                        None,
                        "summary_failed",
                    )
                    .await
                    .map_err(fail)?;
            }
            self.run_coordinator.active.lock().await.remove(&launch_key);
            self.run_coordinator.queue_notify.notify_waiters();
            return Err((-32603, "daemon shutdown，compact owner 未启动".into()));
        }
        if wait {
            let response = receiver
                .await
                .map_err(|_| (-32603, "compact 执行体已消失，需 native readback".into()))??;
            Self::unary_compact_response(response)
        } else {
            self.compact_response(&run).await.map_err(fail)
        }
    }

    pub(super) async fn compact_response(&self, run: &RunRecord) -> Result<Value, RepositoryError> {
        let fact = self
            .run_request_store
            .read_compact_fact(&self.scope(), &run.run_id)
            .await?
            .ok_or(CoordinationError::Corrupt)?;
        Ok(Self::compact_projection(
            &fact.run,
            &fact.receipt,
            fact.view_stamp,
        ))
    }
    pub(super) fn compact_projection(
        run: &RunRecord,
        receipt: &CompactRunReadback,
        stamp: Option<ViewStamp>,
    ) -> Value {
        let mut value = json!(run);
        value["compact"] = json!(receipt);
        value["projection_generation"] = json!(
            receipt
                .result_generation
                .unwrap_or(receipt.source.generation)
        );
        value["transcript_revision"] = json!(receipt.source.source_end);
        if let Some(stamp) = stamp {
            value["_my_agent_view"] = json!(stamp);
        }
        value
    }

    async fn compact_command_response(
        &self,
        run: &RunId,
        wait: bool,
    ) -> Result<Value, (i64, String)> {
        loop {
            let changed = self.run_coordinator.queue_notify.notified();
            let (record, _) = self
                .run_request_store
                .read_compact_fact(&self.scope(), run)
                .await
                .and_then(|fact| fact.ok_or(CoordinationError::Corrupt.into()))
                .map(|fact| (fact.run, fact.receipt))
                .map_err(|e| (-32603, e.to_string()))?;
            if !wait || record.status.terminal() {
                let response = self
                    .compact_response(&record)
                    .await
                    .map_err(|e| (-32603, e.to_string()))?;
                return if wait {
                    Self::unary_compact_response(response)
                } else {
                    Ok(response)
                };
            }
            tokio::select! { _ = changed => {}, _ = self.shutdown.cancelled() => return Err((-32603,"daemon shutdown；compact 状态须读回".into())) }
        }
    }

    fn unary_compact_response(value: Value) -> Result<Value, (i64, String)> {
        if matches!(
            value["compact"]["outcome"].as_str(),
            Some("committed" | "no_gain")
        ) {
            Ok(value)
        } else {
            Err((
                value["error_code"].as_i64().unwrap_or(-32603),
                value["error_message"]
                    .as_str()
                    .unwrap_or("compact 未完成")
                    .into(),
            ))
        }
    }
}
