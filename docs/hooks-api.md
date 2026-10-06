# 受治理 hook 协议与配置

实现 owner：`core::HookEvent/HookEffect/HookOutcome` 定义严格领域 schema；`storage::HookRepository` 保存 claim、结算、publication 和续跑关联；daemon 的单一 `HookDispatcher` 执行配置，入口只投影同一回执。

## 显式启用与信任

默认禁用。启动 daemon 时显式设置 `MY_AGENT_HOOKS_CONFIG` 为配置文件的绝对路径，工作区中的文件不会自行启用。配置只读取这一来源，不搜索或自动合并工作区、用户和项目脚本。修改配置需重新启动 daemon。

```json
{
  "schema_version": 1,
  "hooks": [
    {"event": "post_tool", "executable": "/受信私有目录/observe", "timeout_ms": 1000}
  ]
}
```

配置目录、配置文件和 executable 必须由 daemon 用户拥有，禁止 group/other 权限、symlink 和 executable hardlink；executable 位于该私有目录并有 owner execute 权限。工作区必须同用户拥有且不可被 group/other 写入。配置以 no-follow 打开并复核 inode/device，最大 16 KiB、最多 16 个事件、不允许重复事件和未知字段。无效配置保存 typed failure，控制事件拒绝、观察事件记录后继续。

每次执行前重新检查 executable 和目录权限。直接启动 executable，不解析 shell command 字符串；stdin 是有界 JSON，stdout 是严格 `HookEffect` JSON，stderr 不向用户公开。清空环境，仅传固定 PATH 和 LANG；cwd 为受信工作区。stdout/stderr 各限 8 KiB，超限结束整个进程组。timeout 范围 10–5000 ms；取消或 future 被丢弃同样终止子进程并结算 `cancelled`。崩溃后 running claim 变为 `unknown_after_restart`，绝不重新执行。

## 事件与效果

完整 inventory：user_prompt_submit、pre_tool、post_tool、tool_error、approval_requested、approval_resolved、subagent_start、subagent_end、assistant_reply、after_turn、stop、session_start、session_end、notification、pre_compact、post_compact。

所有事件允许 `{"effect":"observe"}`。user_prompt_submit/pre_tool/pre_compact 额外允许 `allow` 与 `deny`（reason 字符串）；stop 额外允许 `continue`（非空 prompt，最大 4096 bytes）。其他事件不能控制运行。未知字段/效果、超时、非零退出、无效输出、权限和预算失败均为 `HookFailure` 枚举；不使用错误字符串推断授权。观察失败默认 fail-open，控制失败 fail-closed。已提交 terminal 不因 Stop 失败被改写。

payload schema_version=1，含 session_key、session_lifetime_id、适用时完整 ExactOwner、cwd、channel、permission_mode、route_digest、事件数据与 operation_id。用户输入/正文/工具参数与结果只传长度和摘要，route 只传 digest，不传密钥或完整 daemon 环境。审批事件读取持久 interaction 的 revision/status/实际批准结果，不从客户端弹窗或临时 broker 状态猜测。channel 是审计标签，不授予权限；run channel 随 snapshot 冻结。

SessionStart/End 来自与生命周期发布同事务的 outbox；create/fork 才发布 Start，同一 lifetime 的 Active→Closed/Archived 只发布一个 End。重试不重新运行。旧 incarnation 尚未 claim 的 Start 只能凭真实 publication 恢复，不能临时伪造。启动时分批排空恢复 outbox，已执行或不确定的 claim 不重放。

PreCompact 在 intent 已持久化后、摘要开始前触发；PostCompact 在结算后带 succeeded/no_gain/rejected/failed 真实结果。恢复 receipt 不再触发 Pre；已拒绝/失败的 unary compact 重试保持拒绝，不能返回成功。手动 compact 的独立 run 属于后续 Wave 3。

## Stop 续跑

成功的父 run 最多接受一次 continuation。repository 同事务复核精确 Stop outcome、父 run terminal、冻结 snapshot 与 session lifetime，建立新 run/turn/队列及父子关联。子 run 复用父授权工具、route、sandbox、context policy，不能扩大权限。最多 8 轮、8192 累计输入/输出 tokens（含 reasoning，usage 与确定性估算取较大值）、8 次工具调用、30 秒；子 run 和原委派不能再次 continuation。请求前拒绝超预算输入，流式输出在转发前计量并中断超额 delta，最终文本也受限。每轮一次 Provider attempt，不执行额外 retry、overflow 摘要或自动 compact，避免预算外的请求；只读复用既有 projection。超预算通过 canonical cancel/失败结算，不以客户端计时推断终态。

相同父身份、operation、prompt 和 frozen snapshot 返回原 child；修改任一项冲突。Queued child 可按原队列恢复，已 running child 在崩溃后 unknown，不重放。Stop claim 已执行但尚未发布 child 时发生崩溃，不会自动再执行该脚本。`run.read` 的 continuation_parent_run_id/continuation_run_id 只投影持久关联；精确取消 child 不影响父 completed terminal。

## 只读回执

RPC `hooks.readback`（session capability）：

```json
{"session_id":"session-example.jsonl","expected_lifetime":"原始 lifetime","after_cursor":0,"limit":32}
```

返回严格 `HookReadback`：schema_version、session_key、session_lifetime_id、snapshot_revision、outcomes、cursor、has_more。默认/上限 32 条，单只读事务，允许按 tombstone 读历史 lifetime，不创建 runtime 或脚本进程。损坏身份、未知 schema、非法 effect/status 均拒绝。cursor 是稳定插入行游标；已有 running outcome 的结算会推进 snapshot_revision，刷新该页应从其起始 cursor 重读，不能把插入 cursor 当成更新订阅。

CLI/TUI/ACP v1/WebSocket：`/hooks <session> <lifetime> [cursor] [limit]`，走同一 daemon repository handler。Web 显示审计活动，不建立聊天 run 或猜测任务完成。HTTP：`GET /api/hooks/readback?session_id=…&expected_lifetime=…&after_cursor=0&limit=32&workspace=…`，沿用 bearer/gateway 安全策略。

SQLite v16 新增 hook_outcomes、hook_publications、hook_continuations，沿用升级前校验备份与 marker。schema 未知未来版本拒绝；没有 JSONL 双写或文件事实源。
