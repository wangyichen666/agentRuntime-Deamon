# 版本化计划控制面（SQLite v15）

所有入口通过 daemon-client 调用同一 daemon handler。canonical 文档、Markdown、版本历史与决策回执由 SQLite repository 持有；入口不读取 `plan.json`、不计算 digest、不推断 run 终态。

## 定义与进度

`plan` 工具 `set` 支持 `title`、`goal`、`success_criteria`、`constraints`、`verification` 和有序 `steps`；旧的仅 steps 参数仍可用。步骤包含 `id`、`description`、`status`。定义采用 core 的固定字段 UTF-8 编码，storage 用已有 SHA-256 实现计算 `content_digest`；步骤状态、review 和 artifact 不参与定义摘要。仅进度变化不会增加 revision。

首次实际 authoring 分配 stable `plan_id`，随后同一逻辑计划的定义变化增加 revision。stage 沿用原有 CAS，成功 TurnCommit 同事务发布 session plan、版本文档、同源 Markdown 和终态。失败不发布 staged 新版本。历史导入计划在 `plan_legacy_evidence` 保留原始数据，只有只读展示权；update/add 必须先通过显式 set 进行新定义 authoring，不会在查询或执行时自动补身份。

## RPC

`sessions.plan.readback`：参数为 `{ "session_id": "session-….jsonl" }`。返回严格 `PlanReadback`：session_key、lifetime、snapshot_revision、plan、legacy_plan、markdown。versioned plan 包含 plan_id、revision、content_digest、definition、review 和 artifact 的媒体类型/字节长度/摘要。默认与当前实现均不返回 daemon 本机 artifact 路径。读取不构造 runtime，不调用 Provider/MCP，也不导入旧文件或写数据库。

执行只使用 `chat.send` 的类型化 metadata；没有第二个执行 RPC owner：

```json
{
  "session_id": "session-….jsonl",
  "expected_lifetime": "从 readback 获取的 lifetime",
  "admission_mode": "reject_if_busy",
  "message": "本次执行指令，重试保持相同",
  "plan_execution": {
    "plan_id": "从 readback 获取",
    "revision": 1,
    "content_digest": "从 readback 获取的定义 SHA-256",
    "operation_id": "稳定且全局唯一的操作 ID"
  }
}
```

`sessions.plan.discard`：参数包含 session_id、expected_lifetime 和上述 `plan_execution` 同型的 `identity`。返回 `PlanDecisionReceipt`；run_id 通常为空，若废弃了自己的尚未启动 pending run，则返回精确取消的 native run_id。回执不删除或覆盖执行历史。

operation_id 同时绑定 session、lifetime、plan_id、revision、digest、决策、输入和请求的 sandbox/context_read_only/准入模式。相同输入返回原 run/receipt；换身份或输入冲突。配置在重试时的自动变化不会改写原冻结 run snapshot。

## 共享文本适配

CLI、TUI、ACP 的普通 prompt 与 Web 工作台支持：

```text
/plan read [session_id]
/plan execute <lifetime> <plan_id> <revision> <digest> <operation_id>
/plan discard <session_id> <lifetime> <plan_id> <revision> <digest> <operation_id>
```

`read`/`discard` 由 daemon slash adapter 投影同一 repository command；execute 的解析集中在 daemon-protocol，转换为同一个 `chat.send`。execute 使用当前 session，必须先显式恢复目标 session，不能用另一会话的 lifetime 授权。转换后的规范 message 为 `执行已确认计划 {plan_id} 版本 {revision} 摘要 {digest}。`。混用文本与原生 metadata 重试时，message 和 sandbox/context_read_only 也必须一致。

ACP v1 继续使用标准 prompt 与消息通知；没有新增 ACP v2 方法或能力广告。幂等完成回执没有新的 live delta 时，ACP 从 daemon response 展示已提交正文，不另造终态。Web 的 read/discard 结果作为活动信息显示，不写入聊天 transcript，不显示虚构的聊天完成。

## 生命周期与失败

首次准入同事务发布 pending_execution、queued run、冻结快照和决策回执。只有 daemon 已获得 session writer 锁、复核 ExactOwner 且即将进入 engine 时，才推进 executing。review 的 executing 表示该版本已获执行准入；具体 running/completed/cancelled/unknown 由关联 run 的 durable 状态读取。失败或不确定执行将该版本标为 blocked，不自动重放。

重启不自动调度计划的 queued run；明确继续同一 operation/digest 才会恢复它。也可精确废弃尚未启动的 pending run：取消、terminal、review 与回执同事务提交，随后 daemon 唤醒该 native run 的等待任务。其他活动 run、已开始执行、受管资源或错误身份仍 fail closed；拒绝会回滚取消与 review。

错误 lifetime/digest/revision、busy、非法 operation、旧计划执行和损坏数据均返回冲突，不能变为空计划或猜测当前身份。Markdown/历史写入失败与 terminal 故障全部回滚；未知未来 schema 拒绝，升级前保存校验备份及 marker。不恢复 JSONL 双写。

## HTTP 适配

同一 Gateway 提供 `GET /api/plans/readback?session_id=...&workspace=...`、`POST /api/plans/execute` 和 `POST /api/plans/discard`。两个 POST 的严格 body 为 `{ "workspace": "可选工作区", "params": "上述对应 RPC 参数对象" }`；execute 必须携带 plan_execution。外层及身份对象未知字段均拒绝。沿用 Gateway bearer 鉴权和 WorkspaceRouter，转发同一 daemon readback/chat.send/discard owner。HTTP 执行消费 durable 事件并返回 native run 结果；无人值守 HTTP 审批沿用安全拒绝策略。

旧 queued run 的工具 schema 与当前实现不一致时，继续沿用冻结 catalog 恢复失败策略，不扩大既有授权；需要新请求重新 authoring/执行。
