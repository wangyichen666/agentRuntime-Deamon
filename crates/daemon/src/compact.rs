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
        let fail = |error: RuntimeError| (-32001, error.to_string());
        if let Some(run) = self.run_store.compact_operation(&request).map_err(fail)? {
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
        let route = self
            .provider_manager
            .as_ref()
            .map(|manager| manager.freeze(engine.token_budget()));
        if let Some(route) = route.as_ref() {
            engine = engine.with_route(route.clone());
        }
        let engine = engine
            .for_compact()
            .map_err(|error| (-32603, error.to_string()))?;
        let snapshot = RunSnapshot {
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
        let admission = self
            .run_store
            .admit_compact_run(&request, &snapshot)
            .map_err(fail)?;
        let run = match admission {
            Admission::Existing(run) => {
                return self.compact_command_response(&run.run_id, wait).await;
            }
            Admission::New(run) => run,
        };
        let owner = self.run_store.run_owner(&run.run_id).map_err(fail)?;
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
        let launched = self
            .spawn_owned(async move {
                let _writer = writer;
                let result = agent_runtime::loop_engine::with_tool_audit(
                    state.run_store.clone(),
                    native.run_id.clone(),
                    agent_runtime::hooks::with_scope(
                        agent_runtime::hooks::HookScope {
                            dispatcher: state.hooks.clone(),
                            store: state.run_store.clone(),
                            owner,
                        },
                        engine.compact_session(&cancellation, &operation),
                    ),
                )
                .await;
                if let Ok(Some(fact)) = state.run_store.compact_run(&native.run_id)
                    && fact.outcome == CompactRunOutcome::Started
                {
                    let reason = if cancellation.is_cancelled() {
                        "cancelled"
                    } else {
                        "summary_failed"
                    };
                    if let Err(error) = state.run_store.settle_compact(
                        &CompactIntent {
                            operation: operation.clone(),
                            owner: fact.owner,
                            source: fact.source,
                        },
                        None,
                        reason,
                    ) {
                        tracing::error!(%error,"compact fallback 结算失败");
                    }
                }
                if let Err(error) = result {
                    tracing::warn!(%error,run_id=%native.run_id.0,"compact 后台任务结束");
                }
                let response = state
                    .run_store
                    .read_run(&native.run_id)
                    .map_err(fail)
                    .and_then(|run| run.ok_or((-32603, "compact run 缺失".into())))
                    .and_then(|run| state.compact_response(&run).map_err(fail));
                let events = state
                    .run_store
                    .events_after(&native.run_id, EventSeq(0), 1000);
                let mut active = state.run_coordinator.active.lock().await;
                if let Some(active) = active.get_mut(&active_key) {
                    if let Ok(events) = events {
                        for event in events {
                            if let Some(update) = super::handlers::stored_event_update(event) {
                                active.publish_external(native.request_id.clone(), update);
                            }
                        }
                    }
                    active.publish_external(
                        native.request_id.clone(),
                        state.terminal_update(&native.run_id, response.clone()),
                    );
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
            if let Some(fact) = self.run_store.compact_run(&run.run_id).map_err(fail)? {
                self.run_store
                    .settle_compact(
                        &CompactIntent {
                            operation: fact.operation_id,
                            owner: fact.owner,
                            source: fact.source,
                        },
                        None,
                        "summary_failed",
                    )
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
            self.compact_response(&run).map_err(fail)
        }
    }

    pub(super) fn compact_response(&self, run: &RunRecord) -> Result<Value, RuntimeError> {
        let (run, receipt) = self.run_store.compact_snapshot(&run.run_id)?;
        let mut value = json!(run);
        value["compact"] = json!(receipt);
        value["projection_generation"] = json!(
            receipt
                .result_generation
                .unwrap_or(receipt.source.generation)
        );
        value["transcript_revision"] = json!(receipt.source.source_end);
        if let Some(stamp) = self.run_store.event_view_stamp(&run.run_id, run.last_seq)? {
            value["_my_agent_view"] = json!(stamp);
        }
        Ok(value)
    }

    async fn compact_command_response(
        &self,
        run: &RunId,
        wait: bool,
    ) -> Result<Value, (i64, String)> {
        loop {
            let changed = self.run_coordinator.queue_notify.notified();
            let (record, _) = self
                .run_store
                .compact_snapshot(run)
                .map_err(|e| (-32603, e.to_string()))?;
            if !wait || record.status.terminal() {
                let response = self
                    .compact_response(&record)
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
