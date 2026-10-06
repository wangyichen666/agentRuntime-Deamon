# ACP v2 连接与控制面

`my-agent editor` 默认稳定 v1；`my-agent editor --acp-v2` 使用锁定 SDK 2.1.0 的真实 `Agent.v2()` 与 v2 类型。两者通过同一个 daemon-client/reducer 读取 SQLite 事实。v2 尚属 SDK 的 unstable protocol；真实 IDE 互操作未验证。

初始化请求使用标准 `protocolVersion: 2`、`info`，并在 `_meta["my-agent"]` 提供：

```json
{"schema_version":1,"fingerprint":"my-agent/acp-v2/control-schema-1","capabilities":["plan","compact","interaction","readback","management"]}
```

返回协商交集；未知或重复 capability、版本/指纹不符、重复初始化、连接中热变更均拒绝。重连重新初始化。没有协商的控制方法或字段在 mutation 前拒绝；v1 不广告且拒绝此命名空间。标准请求顶层和本命名空间采用严格允许字段，其他 ACP metadata 命名空间不作为授权依据。

所有会话控制请求均有标准 `sessionId` 及命名空间 `schema_version/fingerprint/expected_lifetime`。new 必须提供 operation_id；close/delete/compact 也必须提供 operation_id。cancel 与 interaction 必须提供完整 ExactOwner，不能用 transport 请求 ID 代替 native run。计划执行/废弃采用 plan_execution 的 plan_id/revision/content_digest/operation_id。compact 还要求 expected_revision 和 expected_projection_generation。无关控制字段即使为 null 也拒绝。

| 方法 | 协商能力 | 唯一业务命令与展示 |
| --- | --- | --- |
| session/new、list、resume、prompt、cancel、close | v2 基线 | canonical session、读取恢复、chat.send、exact cancel、持久 close receipt |
| session/delete | management | sessions.delete，严格 lifetime/operation |
| _my_agent/session/read | readback | 同一类型化 SessionReadback |
| _my_agent/plan/read | plan | 同源 Markdown，标准 PlanUpdate/PlanMarkdown |
| _my_agent/plan/execute、discard | plan | 同一 chat.send 精确确认、sessions.plan.discard |
| _my_agent/compact/start | compact | 同一 compact.start，标准 CompactionUpdate |
| _my_agent/run/cancel | compact | 同一 agent.cancel 的 exact owner |
| _my_agent/interaction/respond | interaction | 同一 interaction.respond，exact owner/revision |

SDK 没有标准 plan execute/discard 和 compact start 请求类型，因此使用上述命名空间扩展；响应/通知使用 SDK 标准 v2 类型。resume 只读取 canonical/history/active run/pending interaction，不重放 Provider 或修改首选会话；临时 MCP、额外目录、非空 replayFrom 当前明确拒绝。

prompt 接收 durable Started 后立即返回标准 PromptResponse，metadata 含 accepted 与 native owner，后台消费原请求的 durable stream。Started、StateUpdate、ToolCallUpdate、CompactionUpdate 和 terminal 只来自持久事实；传输 response、ACK、EOF、popup 消失不会制造完成。canonical message_ids 从 transcript batch/native turn 派生，stream 与 resume 使用相同 assistant ID。共享 reducer 拒绝旧 lifetime/revision 和 cursor 缺口。

pending interaction 通过标准 RequestPermissionRequest 恢复，response 的 selected option 仅接受 allow_once/reject_once；未知字段/选项拒绝，Cancelled 只结束弹窗，不作批准/拒绝。选择必须提交捕获的 exact owner 和 interaction revision。断线只 detach，后续连接仍可恢复。

标准 close 先取消捕获的 exact 活跃 owner，等待真实 terminal，再在生命周期屏障内记录幂等 receipt；保留 lifetime/transcript。重复 receipt 不会取消后来新启动的工作。旧 unary compact、CLI、HTTP/WS 的命令和 SQLite schema v20 保持兼容，无新增表或入口状态 owner。

两个 ACP 版本共享物理连接预算：最多 256 个登记帧、8 MiB 排队字节、每帧 4 MiB、128 个后台任务；v2 控制请求并发最多 32。标准 I/O 使用有界桥接和逐帧写入回执，阻塞 pipe 不拖住 Tokio 关闭。SDK 队列超限只断开连接，从 daemon durable cursor 重建，不结算业务。

验证合同位于 tests/runtime_contract.rs：严格协商、plan/v1 拒绝、提前 compact Started/精确取消、长 prompt 同连接控制、pending interaction 重连与 popup Cancelled、真实不消费 stdout 的预算 detach。均对比真实 daemon 事实，使用 Provider barrier/持久 cursor，不以固定 sleep 证明并发。完整门禁见本次 change document。

最终审计补充：v1/v2 permission response 共享严格原始 Value decoder，保留标准 selected outcome 的其他命名空间 metadata；v1 的根/selected 中 my-agent 控制字段均在审批前拒绝。真实 stdio RED 曾返回 EndTurn 并执行文件，修复后返回协议错误、canonical pending 保持，重连 v2 可明确批准，Provider 仅按实际后续轮次调用。合同 acp_v1_permission_rejects_v2_identity_before_approval_and_v2_can_recover。
