//! ACP v2 协议投影；业务与 terminal 始终委托 daemon。
use agent_client_protocol::schema::{ProtocolVersion, v2};
use agent_client_protocol::{
    Agent, Client, ConnectTo, Error, Responder, UntypedMessage, V2ConnectionTo,
};
use agent_core::{ExactOwner, HistoryReadMode, Role, RunStatus, SessionKey, SessionReadback};
use agent_daemon_client::{DaemonClient, RpcStream};
use agent_daemon_protocol::acp::{
    ACP_NAMESPACE, ACP_V2_FINGERPRINT, AcpControl, AcpNegotiation, validate_acp_fields,
};
use agent_daemon_protocol::{EventFrame, EventKind, ServerFrame};
use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

#[derive(Clone)]
struct Connection {
    sdk: V2ConnectionTo<Client>,
    budget: Arc<super::acp_transport::WireBudget>,
}
impl Connection {
    fn spawn(
        &self,
        task: impl std::future::Future<Output = Result<(), Error>> + Send + 'static,
    ) -> Result<(), Error> {
        let permit = self.budget.task()?;
        self.sdk.spawn(async move {
            let _permit = permit;
            task.await
        })
    }
    fn reply(&self, responder: Responder<Value>, value: Value) -> Result<(), Error> {
        self.budget.output(&json!({"result":value}))?;
        responder.respond(value)
    }
    fn failure(&self, responder: Responder<Value>, error: Error) -> Result<(), Error> {
        self.budget.output(&json!({"error":error}))?;
        responder.respond_with_error(error)
    }
    fn permission(
        &self,
        request: UntypedMessage,
    ) -> Result<agent_client_protocol::SentRequest<Value>, Error> {
        self.budget
            .output(&json!({"method":request.method,"params":request.params}))?;
        Ok(self.sdk.send_request(request))
    }
}

pub async fn run_acp_v2_server(client: DaemonClient, workspace: PathBuf) -> Result<()> {
    let budget = Arc::new(super::acp_transport::WireBudget::default());
    let server = build_acp_v2_agent(client, workspace, budget.clone())
        .connect_to(super::acp_transport::stdio(budget.clone()));
    tokio::select! {result=server=>result.context("ACP v2 stdio 连接异常结束"),_=budget.detached()=>anyhow::bail!("ACP v2 慢消费者/帧预算溢出，连接 detach；从 daemon durable cursor 恢复") }
}
fn build_acp_v2_agent(
    client: DaemonClient,
    workspace: PathBuf,
    budget: Arc<super::acp_transport::WireBudget>,
) -> impl ConnectTo<Client> {
    let negotiation = Arc::new(Mutex::new(AcpNegotiation::default()));
    let gate = negotiation.clone();
    let requests = Arc::new(Semaphore::new(32));
    let notification_client = client.clone();
    let notification_budget = budget.clone();
    Agent
        .v2()
        .name("my-agent-acp-v2")
        .on_receive_request(
            async move |request: UntypedMessage, responder, connection| {
                let connection = Connection {
                    sdk: connection,
                    budget: budget.clone(),
                };
                if request.method == "initialize" {
                    let _: v2::InitializeRequest = parse(request.params.clone())?;
                    let advertisement = match negotiation.lock().await.initialize(&request.params) {
                        Ok(value) => value,
                        Err(error) => return connection.failure(responder, invalid(error)),
                    };
                    let response = v2::InitializeResponse::new(
                        ProtocolVersion::V2,
                        v2::Implementation::new("my-agent", env!("CARGO_PKG_VERSION")),
                    )
                    .capabilities(
                        v2::AgentCapabilities::new().session(
                            if advertisement
                                .capabilities
                                .iter()
                                .any(|capability| capability == "management")
                            {
                                v2::SessionCapabilities::new()
                                    .delete(v2::SessionDeleteCapabilities::new())
                            } else {
                                v2::SessionCapabilities::new()
                            },
                        ),
                    )
                    .meta(meta(json!(advertisement)));
                    return connection.reply(responder, json!(response));
                }
                if let Err(error) = validate_acp_fields(&request.method, &request.params, 2) {
                    return connection.failure(responder, invalid(error));
                }
                if let Err(error) = negotiation
                    .lock()
                    .await
                    .require(capability(&request.method))
                {
                    return connection.failure(responder, invalid(error));
                }
                let control = if request.method == "session/list" {
                    None
                } else {
                    match AcpControl::parse(&request.method, &request.params) {
                        Ok(control) => Some(control),
                        Err(error) => return connection.failure(responder, invalid(error)),
                    }
                };
                if control
                    .as_ref()
                    .is_some_and(|control| control.plan_execution.is_some())
                {
                    negotiation
                        .lock()
                        .await
                        .require(Some("plan"))
                        .map_err(invalid)?;
                }
                let permit = match requests.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => return connection.failure(responder, invalid("ACP 并发请求超过32")),
                };
                let client = client.clone();
                let workspace = workspace.clone();
                let task_connection = connection.clone();
                connection.spawn(async move {
                    let _permit = permit;
                    handle_request(
                        client,
                        workspace,
                        task_connection,
                        request,
                        control,
                        responder,
                    )
                    .await
                })?;
                Ok(())
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_notification(
            async move |request: UntypedMessage, connection| {
                let connection = Connection {
                    sdk: connection,
                    budget: notification_budget.clone(),
                };
                validate_acp_fields(&request.method, &request.params, 2).map_err(invalid)?;
                gate.lock()
                    .await
                    .require(capability(&request.method))
                    .map_err(invalid)?;
                if request.method != "session/cancel" {
                    return Err(Error::method_not_found());
                }
                let _: v2::CancelSessionNotification = parse(request.params.clone())?;
                let control =
                    AcpControl::parse(&request.method, &request.params).map_err(invalid)?;
                let client = notification_client.clone();
                connection.spawn(async move { cancel(&client, control).await.map(|_| ()) })
            },
            agent_client_protocol::on_receive_notification!(),
        )
}
fn capability(method: &str) -> Option<&'static str> {
    if method.starts_with("_my_agent/plan/") {
        Some("plan")
    } else if method.starts_with("_my_agent/compact/") {
        Some("compact")
    } else if method.starts_with("_my_agent/interaction/") {
        Some("interaction")
    } else if method == "_my_agent/session/read" {
        Some("readback")
    } else if method == "session/delete" {
        Some("management")
    } else {
        None
    }
}
async fn handle_request(
    client: DaemonClient,
    workspace: PathBuf,
    connection: Connection,
    request: UntypedMessage,
    control: Option<AcpControl>,
    responder: Responder<Value>,
) -> Result<(), Error> {
    let mut responder = Some(responder);
    let result = handle_inner(
        &client,
        &workspace,
        &connection,
        &request,
        control,
        &mut responder,
    )
    .await;
    match (result, responder) {
        (Ok(Some(value)), Some(responder)) => connection.reply(responder, value),
        (Err(error), Some(responder)) => connection.failure(responder, error),
        (Err(error), None) => Err(error),
        _ => Ok(()),
    }
}
async fn handle_inner(
    client: &DaemonClient,
    workspace: &std::path::Path,
    connection: &Connection,
    request: &UntypedMessage,
    control: Option<AcpControl>,
    responder: &mut Option<Responder<Value>>,
) -> Result<Option<Value>, Error> {
    if request.method == "session/list" {
        let request: v2::ListSessionsRequest = parse(request.params.clone())?;
        if request
            .cwd
            .as_ref()
            .is_some_and(|cwd| AsRef::<std::path::Path>::as_ref(cwd) != workspace)
        {
            return Err(invalid("ACP cwd 必须与 daemon 工作区一致"));
        }
        let result = rpc(
            client,
            "sessions.list",
            json!({"after_id":request.cursor.map(|cursor|cursor.to_string()),"limit":100}),
        )
        .await?;
        let sessions = result["sessions"]
            .as_array()
            .ok_or_else(|| internal("缺少 session list"))?
            .iter()
            .map(|session| {
                let id = session["id"]
                    .as_str()
                    .ok_or_else(|| internal("缺少 session id"))?;
                Ok(v2::SessionInfo::new(id.to_owned(), workspace.to_path_buf())
                    .meta(meta(session.clone())))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let mut response = v2::ListSessionsResponse::new(sessions);
        if result["has_more"] == true {
            response = response.next_cursor(
                result["cursor"]
                    .as_str()
                    .map(|cursor| v2::SessionListCursor::new(cursor.to_owned())),
            );
        }
        return Ok(Some(json!(response)));
    }
    let control = control.ok_or_else(|| invalid("缺少 ACP control"))?;
    if request.method == "session/new" {
        let request: v2::NewSessionRequest = parse(request.params.clone())?;
        require_workspace(request.cwd.as_ref(), workspace)?;
        if !request.mcp_servers.is_empty() || !request.additional_directories.is_empty() {
            return Err(invalid(
                "ACP 连接不接受临时 MCP/额外目录，使用 daemon 冻结配置",
            ));
        }
        let created = rpc(
            client,
            "sessions.create",
            json!({"operation_id":control.operation_id}),
        )
        .await?;
        let id = created["session_id"]
            .as_str()
            .ok_or_else(|| internal("缺少 canonical session id"))?;
        return Ok(Some(json!(
            v2::NewSessionResponse::new(id.to_owned()).meta(meta(created))
        )));
    }
    let id = request.params["sessionId"]
        .as_str()
        .filter(|id| !id.is_empty())
        .ok_or_else(|| invalid("缺少 sessionId"))?;
    match request.method.as_str() {
        "session/resume" | "_my_agent/session/read" => {
            if request.method == "session/resume" {
                let request: v2::ResumeSessionRequest = parse(request.params.clone())?;
                require_workspace(request.cwd.as_ref(), workspace)?;
                if !request.mcp_servers.is_empty() || !request.additional_directories.is_empty() {
                    return Err(invalid("恢复不能扩展授权目录或 MCP"));
                }
                if request.replay_from.is_some() {
                    return Err(invalid("当前只支持从 durable 基线完整恢复"));
                }
            }
            let snapshot = read_snapshot(client, id, &control).await?;
            if request.method == "_my_agent/session/read" {
                return Ok(Some(json!(snapshot)));
            }
            replay_snapshot(client, connection, &snapshot).await?;
            Ok(Some(json!(
                v2::ResumeSessionResponse::new().meta(meta(json!(snapshot)))
            )))
        }
        "session/close" | "session/delete" => {
            if request.method == "session/close" {
                let _: v2::CloseSessionRequest = parse(request.params.clone())?;
            } else {
                let _: v2::DeleteSessionRequest = parse(request.params.clone())?;
            }
            let value=rpc(client,if request.method=="session/close"{"session.close"}else{"sessions.delete"},json!({"session_id":id,"expected_lifetime":control.expected_lifetime,"operation_id":control.operation_id})).await?;
            if request.method == "session/close" {
                Ok(Some(json!(
                    v2::CloseSessionResponse::new().meta(meta(value))
                )))
            } else {
                Ok(Some(json!(
                    v2::DeleteSessionResponse::new().meta(meta(value))
                )))
            }
        }
        "session/prompt" | "_my_agent/plan/execute" => {
            let text = if request.method == "session/prompt" {
                let request: v2::PromptRequest = parse(request.params.clone())?;
                prompt_text(&request.prompt)?
            } else {
                let plan = control
                    .plan_execution
                    .as_ref()
                    .ok_or_else(|| invalid("缺少 exact plan identity"))?;
                format!(
                    "执行已确认计划 {} 版本 {} 摘要 {}。",
                    plan.plan_id, plan.revision, plan.content_digest
                )
            };
            let mut stream=client.request("chat.send",json!({"session_id":id,"expected_lifetime":control.expected_lifetime,"message":text,"entry_channel":"acp","admission_mode":if control.plan_execution.is_some(){"reject_if_busy"}else{"queue"},"plan_execution":control.plan_execution})).await.map_err(internal)?;
            let first = stream
                .next()
                .await
                .ok_or_else(|| internal("daemon 在准入前断开"))?;
            match first {
                ServerFrame::Response(response) => {
                    if let Some(error) = response.error {
                        return Err(Error::new(
                            i32::try_from(error.code).unwrap_or(-32603),
                            error.message,
                        ));
                    }
                    Ok(Some(json!(
                        v2::PromptResponse::new()
                            .meta(meta(response.result.unwrap_or(Value::Null)))
                    )))
                }
                ServerFrame::Event(event) => {
                    if !matches!(event.event, EventKind::TurnStarted | EventKind::RunStarted) {
                        return Err(internal("daemon 未返回 durable Started"));
                    }
                    let owner = event_owner(&event)?;
                    let response=json!(v2::PromptResponse::new().meta(meta(json!({"schema_version":1,"fingerprint":ACP_V2_FINGERPRINT,"owner":owner,"accepted":true}))));
                    // response 只表示 durable 准入；后续 terminal 由事件/读回投影。
                    connection.reply(
                        responder.take().ok_or_else(|| internal("ACP 请求已回应"))?,
                        response,
                    )?;
                    forward_event(client, connection, id, event).await?;
                    let client = client.clone();
                    let target = connection.clone();
                    let id = id.to_owned();
                    connection.spawn(async move {
                        forward_stream(&client, &target, &id, stream, owner).await
                    })?;
                    Ok(None)
                }
            }
        }
        "_my_agent/plan/read" => {
            let snapshot = read_snapshot(client, id, &control).await?;
            let value = rpc(client, "sessions.plan.readback", json!({"session_id":id})).await?;
            if value["lifetime"] != json!(snapshot.session_lifetime_id) {
                return Err(invalid("plan readback lifetime 已改变"));
            }
            let plan: agent_core::PlanReadback = parse(value.clone())?;
            if let (Some(document), Some(markdown)) = (plan.plan, plan.markdown) {
                update(
                    connection,
                    id,
                    v2::SessionUpdate::PlanUpdate(
                        v2::PlanUpdate::new(v2::PlanUpdateContent::markdown(
                            document.plan_id,
                            markdown,
                        ))
                        .meta(meta(value.clone())),
                    ),
                )?;
            }
            Ok(Some(value))
        }
        "_my_agent/plan/discard" => {
            let value=rpc(client,"sessions.plan.discard",json!({"session_id":id,"expected_lifetime":control.expected_lifetime,"identity":control.plan_execution})).await?;
            // discard 是历史 review 状态，不能伪装为删除计划。
            Ok(Some(value))
        }
        "_my_agent/compact/start" => {
            let value=rpc(client,"compact.start",json!({"session_id":id,"expected_lifetime":control.expected_lifetime,"operation_id":control.operation_id,"expected_revision":control.expected_revision,"expected_projection_generation":control.expected_projection_generation,"entry_channel":"acp"})).await?;
            compact_update(connection, id, &value)?;
            if value["compact"]["outcome"] == "started" {
                let run: agent_core::RunRecord =
                    agent_daemon_protocol::decode_run_readback(value.clone()).map_err(internal)?;
                let stream = client
                    .request(
                        "agent.subscribe",
                        json!({"request_id":run.request_id,"session_id":id}),
                    )
                    .await
                    .map_err(internal)?;
                let owner: ExactOwner = parse(value["compact"]["owner"].clone())?;
                connection.reply(
                    responder.take().ok_or_else(|| internal("ACP 请求已回应"))?,
                    value,
                )?;
                let client = client.clone();
                let target = connection.clone();
                let id = id.to_owned();
                connection.spawn(async move {
                    forward_stream(&client, &target, &id, stream, owner).await
                })?;
                Ok(None)
            } else {
                Ok(Some(value))
            }
        }
        "_my_agent/run/cancel" => cancel(client, control).await.map(Some),
        "_my_agent/interaction/respond" => {
            let owner = control.owner.ok_or_else(|| invalid("缺少 exact owner"))?;
            rpc(client,"interaction.respond",json!({"approval_id":control.interaction_id,"approved":control.approved,"session_id":id,"owner_run_id":owner.run_id,"exact_owner":owner,"revision":control.interaction_revision})).await.map(Some)
        }
        _ => Err(Error::method_not_found()),
    }
}
async fn cancel(client: &DaemonClient, control: AcpControl) -> Result<Value, Error> {
    let owner = control
        .owner
        .ok_or_else(|| invalid("缺少 cancel exact owner"))?;
    rpc(
        client,
        "agent.cancel",
        json!({"session_id":owner.session_key,"run_id":owner.run_id,"exact_owner":owner}),
    )
    .await
}
async fn read_snapshot(
    client: &DaemonClient,
    id: &str,
    control: &AcpControl,
) -> Result<SessionReadback, Error> {
    let snapshot = client
        .read_session(&SessionKey(id.into()), HistoryReadMode::Canonical)
        .await
        .map_err(internal)?;
    if control.expected_lifetime.as_ref() != Some(&snapshot.session_lifetime_id) {
        return Err(invalid("ACP session lifetime 已过期"));
    }
    Ok(snapshot)
}
async fn forward_stream(
    client: &DaemonClient,
    connection: &Connection,
    id: &str,
    mut stream: RpcStream,
    owner: ExactOwner,
) -> Result<(), Error> {
    while let Some(frame) = stream.next().await {
        match frame {
            ServerFrame::Event(event) => forward_event(client, connection, id, event).await?,
            ServerFrame::Response(response) => {
                if agent_daemon_client::response_view_ignored(&response) {
                    return Ok(());
                }
                // 仅带 owner 的 durable terminal 是业务结算证据；transport error 不造 idle。
                if let Some(result) = response.result {
                    if result.get("compact").is_some() {
                        compact_update(connection, id, &result)?;
                    } else if let Some(stamp) = result.get("_my_agent_view") {
                        let stamp: agent_core::ViewStamp = parse(stamp.clone())?;
                        if stamp.terminal {
                            project_terminal(
                                client,
                                connection,
                                id,
                                stamp.owner.ok_or_else(|| internal("terminal 缺少 owner"))?,
                            )
                            .await?;
                        }
                    }
                } else if let Some(error) = response.error {
                    let run = client.read_run(&owner.run_id).await.map_err(internal)?;
                    if run.status.terminal() {
                        project_terminal(client, connection, id, owner.clone()).await?;
                    } else {
                        return Err(Error::new(
                            i32::try_from(error.code).unwrap_or(-32603),
                            error.message,
                        ));
                    }
                }
                return Ok(());
            }
        }
    }
    Err(internal("daemon 断开，终态需 durable readback"))
}
async fn forward_event(
    client: &DaemonClient,
    connection: &Connection,
    id: &str,
    event: EventFrame,
) -> Result<(), Error> {
    if event.event == EventKind::ViewResynced {
        let (snapshot, terminal) =
            agent_daemon_protocol::decode_view_sync(event.data).map_err(internal)?;
        update(
            connection,
            id,
            v2::SessionUpdate::SessionInfoUpdate(parse(
                json!({"_meta":meta(json!({"readback":snapshot}))}),
            )?),
        )?;
        if let Some(terminal) = terminal {
            let value = rpc(client, "run.read", json!({"run_id":terminal.run_id})).await?;
            let owner: ExactOwner = parse(value["_my_agent_view"]["owner"].clone())?;
            project_terminal(client, connection, id, owner).await?;
        }
        return Ok(());
    }
    let owner = event_owner(&event)?;
    let identity = json!({"schema_version":1,"fingerprint":ACP_V2_FINGERPRINT,"owner":owner,"seq":event.seq,"view":event.data.get("_my_agent_view")});
    match event.event {
        EventKind::RunStarted | EventKind::TurnStarted => {
            if event.data["kind"] == "compact" || event.event == EventKind::RunStarted {
                let value = rpc(client, "run.read", json!({"run_id":owner.run_id})).await?;
                compact_update(connection, id, &value)
            } else {
                update(
                    connection,
                    id,
                    v2::SessionUpdate::StateUpdate(v2::StateUpdate::Running(
                        v2::RunningStateUpdate::new().meta(meta(identity)),
                    )),
                )
            }
        }
        EventKind::TextDelta => update(
            connection,
            id,
            v2::SessionUpdate::AgentMessageChunk(
                v2::ContentChunk::new(
                    event.data["delta"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned()
                        .into(),
                    format!("{}:assistant", owner.turn_id.0),
                )
                .meta(meta(identity)),
            ),
        ),
        EventKind::ThinkingDelta | EventKind::ThinkingFinished => Ok(()),
        EventKind::ToolStarted | EventKind::ToolFinished => {
            let call = event.data["tool_call_id"]
                .as_str()
                .ok_or_else(|| internal("tool event 缺少 call id"))?;
            let status = if event.event == EventKind::ToolStarted {
                v2::ToolCallStatus::InProgress
            } else if event.data["error"].is_null() {
                v2::ToolCallStatus::Completed
            } else {
                v2::ToolCallStatus::Failed
            };
            update(
                connection,
                id,
                v2::SessionUpdate::ToolCallUpdate(
                    v2::ToolCallUpdate::new(call.to_owned())
                        .title(event.data["name"].as_str().unwrap_or("工具").to_owned())
                        .status(status)
                        .meta(meta(identity)),
                ),
            )
        }
        EventKind::ApprovalRequired => permission(client, connection, id, event.data).await,
        EventKind::CompactTerminal => {
            let value = rpc(client, "run.read", json!({"run_id":owner.run_id})).await?;
            compact_update(connection, id, &value)
        }
        EventKind::TurnCompleted => project_terminal(client, connection, id, owner).await,
        EventKind::DelegationSpawned | EventKind::DelegationTerminal => Ok(()),
        EventKind::ViewResynced => Ok(()),
    }
}
fn event_owner(event: &EventFrame) -> Result<ExactOwner, Error> {
    let stamp: agent_core::ViewStamp = parse(event.data["_my_agent_view"].clone())?;
    stamp
        .owner
        .ok_or_else(|| internal("durable 事件缺少 exact owner"))
}
async fn project_terminal(
    client: &DaemonClient,
    connection: &Connection,
    id: &str,
    owner: ExactOwner,
) -> Result<(), Error> {
    let value = rpc(client, "run.read", json!({"run_id":owner.run_id})).await?;
    let run = agent_daemon_protocol::decode_run_readback(value.clone()).map_err(internal)?;
    if !run.status.terminal() {
        return Ok(());
    }
    if run.kind == agent_core::RunKind::Compact {
        return compact_update(connection, id, &value);
    }
    if let Some(content) = run.content {
        update(
            connection,
            id,
            v2::SessionUpdate::AgentMessage(
                v2::AgentMessage::new(format!("{}:assistant", owner.turn_id.0))
                    .content(vec![content.into()])
                    .meta(meta(json!({"owner":owner}))),
            ),
        )?;
    }
    let reason = if run.status == RunStatus::Cancelled {
        Some(v2::StopReason::Cancelled)
    } else if run.status == RunStatus::Completed {
        Some(v2::StopReason::EndTurn)
    } else {
        None
    };
    update(
        connection,
        id,
        v2::SessionUpdate::StateUpdate(v2::StateUpdate::Idle(
            v2::IdleStateUpdate::new()
                .stop_reason(reason)
                .meta(meta(value)),
        )),
    )
}
fn compact_update(connection: &Connection, id: &str, value: &Value) -> Result<(), Error> {
    let run = agent_daemon_protocol::decode_run_readback(value.clone()).map_err(internal)?;
    if run.kind != agent_core::RunKind::Compact {
        return Err(internal("缺少 native compact identity"));
    }
    let outcome = value["compact"]["outcome"]
        .as_str()
        .ok_or_else(|| internal("compact 缺少 outcome"))?;
    let status = match outcome {
        "started" => v2::CompactionStatus::InProgress,
        "committed" => v2::CompactionStatus::Completed,
        "cancelled" => v2::CompactionStatus::Cancelled,
        other => v2::CompactionStatus::Other(format!("_my_agent/{other}")),
    };
    update(
        connection,
        id,
        v2::SessionUpdate::CompactionUpdate(
            v2::CompactionUpdate::new(run.run_id.0, status).meta(meta(value.clone())),
        ),
    )
}
async fn permission(
    client: &DaemonClient,
    connection: &Connection,
    id: &str,
    data: Value,
) -> Result<(), Error> {
    let approval = data["approval"].clone();
    let interaction_id = approval["id"]
        .as_str()
        .ok_or_else(|| internal("approval 缺少 id"))?
        .to_owned();
    let owner: ExactOwner = parse(data["_my_agent_view"]["owner"].clone())?;
    let record = rpc(
        client,
        "interaction.read",
        json!({"interaction_id":interaction_id}),
    )
    .await?;
    let record: agent_core::InteractionRecord = parse(record["interaction"].clone())?;
    request_permission(client, connection, id, owner, record).await
}
async fn request_permission(
    client: &DaemonClient,
    connection: &Connection,
    id: &str,
    owner: ExactOwner,
    record: agent_core::InteractionRecord,
) -> Result<(), Error> {
    update(
        connection,
        id,
        v2::SessionUpdate::StateUpdate(v2::StateUpdate::RequiresAction(
            v2::RequiresActionStateUpdate::new()
                .meta(meta(json!({"owner":owner,"interaction":record}))),
        )),
    )?;
    let permission_request=v2::RequestPermissionRequest::new(id.to_owned(),record.prompt.clone(),vec![v2::PermissionOption::new("allow_once","允许一次",v2::PermissionOptionKind::AllowOnce),v2::PermissionOption::new("reject_once","拒绝一次",v2::PermissionOptionKind::RejectOnce)]).meta(meta(json!({"schema_version":1,"fingerprint":ACP_V2_FINGERPRINT,"owner":owner,"interaction":record})));
    let pending = connection.permission(UntypedMessage::new(
        "session/request_permission",
        permission_request,
    )?)?;
    let client = client.clone();
    // 不阻塞 stream 或连接：其他 exact cancel/interaction 请求仍可处理。
    connection.spawn(async move {
        let response=pending.block_task().await?;
        let Some(approved)=agent_daemon_protocol::acp::decode_acp_permission(response,&owner).map_err(invalid)? else{return Ok(());};
        rpc(&client,"interaction.respond",json!({"approval_id":record.interaction_id,"session_id":owner.session_key,"owner_run_id":owner.run_id,"exact_owner":owner,"revision":record.revision,"approved":approved})).await.map(|_|())
    })
}
async fn replay_snapshot(
    client: &DaemonClient,
    connection: &Connection,
    snapshot: &SessionReadback,
) -> Result<(), Error> {
    let id = &snapshot.session_id.0;
    for (index, message) in snapshot.messages.iter().enumerate() {
        if let Some(content) = message.content.as_ref() {
            let message_id = snapshot
                .message_ids
                .get(index)
                .cloned()
                .ok_or_else(|| internal("ACP canonical history 缺少 message identity"))?;
            let update = match message.role {
                Role::User => Some(v2::SessionUpdate::UserMessage(
                    v2::UserMessage::new(message_id).content(vec![content.clone().into()]),
                )),
                Role::Assistant => Some(v2::SessionUpdate::AgentMessage(
                    v2::AgentMessage::new(message_id).content(vec![content.clone().into()]),
                )),
                _ => None,
            };
            if let Some(event) = update {
                self::update(connection, id, event)?;
            }
        }
    }
    for interaction in &snapshot.pending_interactions {
        let owner = snapshot
            .run_owners
            .iter()
            .find(|owner| owner.run_id == interaction.owner_run_id)
            .ok_or_else(|| internal("interaction 缺少 owner"))?;
        request_permission(client, connection, id, owner.clone(), interaction.clone()).await?;
    }
    for run in &snapshot.active_runs {
        let stream = client
            .request(
                "agent.subscribe",
                json!({"request_id":run.request_id,"session_id":id,"after_seq":run.last_seq}),
            )
            .await
            .map_err(internal)?;
        let client = client.clone();
        let target = connection.clone();
        let id = id.clone();
        let owner = snapshot
            .run_owners
            .iter()
            .find(|owner| owner.run_id == run.run_id)
            .cloned()
            .ok_or_else(|| internal("active run 缺少 owner"))?;
        connection
            .spawn(async move { forward_stream(&client, &target, &id, stream, owner).await })?;
    }
    update(
        connection,
        id,
        v2::SessionUpdate::SessionInfoUpdate(parse(
            json!({"_meta":meta(json!({"readback":snapshot}))}),
        )?),
    )
}
fn update(connection: &Connection, id: &str, update: v2::SessionUpdate) -> Result<(), Error> {
    let notification = v2::UpdateSessionNotification::new(id.to_owned(), update);
    connection
        .budget
        .output(&json!({"method":"session/update","params":notification}))?;
    connection.sdk.send_notification(notification)
}
fn meta(value: Value) -> v2::Meta {
    let mut meta = v2::Meta::new();
    meta.insert(ACP_NAMESPACE.into(), value);
    meta
}
fn parse<T: DeserializeOwned>(value: Value) -> Result<T, Error> {
    serde_json::from_value(value).map_err(invalid)
}
fn require_workspace(cwd: &std::path::Path, workspace: &std::path::Path) -> Result<(), Error> {
    if cwd == workspace {
        Ok(())
    } else {
        Err(invalid("ACP cwd 必须与 daemon 工作区一致"))
    }
}
fn prompt_text(blocks: &[v2::ContentBlock]) -> Result<String, Error> {
    let mut parts = Vec::new();
    for block in blocks {
        match block {
            v2::ContentBlock::Text(text) => parts.push(text.text.clone()),
            v2::ContentBlock::ResourceLink(link) => {
                parts.push(format!("[资源：{}]({})", link.name, link.uri))
            }
            _ => return Err(invalid("ACP prompt 只支持 text/resource_link")),
        }
    }
    let text = parts.join("\n");
    if text.trim().is_empty() {
        Err(invalid("prompt 不能为空"))
    } else {
        Ok(text)
    }
}
async fn rpc(client: &DaemonClient, method: &str, params: Value) -> Result<Value, Error> {
    agent_entry_support::rpc::request_result(client, method, params)
        .await
        .map_err(internal)
}
fn invalid(error: impl std::fmt::Display) -> Error {
    Error::invalid_params().data(error.to_string())
}
fn internal(error: impl std::fmt::Display) -> Error {
    Error::internal_error().data(error.to_string())
}
