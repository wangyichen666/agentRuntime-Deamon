# 独立 compact 控制面

SQLite `compact_run_links`、既有 `compact_operations`、runs/turns/events 是唯一持久事实源；daemon `start_compact_command` 持有执行 writer。原始 transcript 不写入 compact 请求、摘要或 terminal 文本，只有模型输入 projection 可替换。

## 准入与恢复

`compact.start`（context capability）接受严格参数：

```json
{"session_id":"session-example.jsonl","expected_lifetime":"lifetime","operation_id":"compact-1","expected_revision":8,"expected_projection_generation":0,"entry_channel":"cli"}
```

无须借用聊天 owner。相同 operation 和完整 source/身份返回原 native run；输入冲突、过期 lifetime/source、busy session、只读上下文均拒绝。首次准入在同一事务写 run/turn、frozen snapshot、intent/source 和 `run_started`，返回 native `run_id`、`kind: compact`、request_id 以及 `compact` 类型化 receipt，摘要由 daemon owned task 后台执行。很快结束的操作可以直接读到最终 receipt；Started 总在 terminal 前持久化。

`agent.subscribe` 使用返回的 canonical request_id、session_id、after_seq；它重放 durable Started/`compact_terminal`，随后通知与最终 response 只是加速读取。`run.events` 提供持久 cursor，慢消费者断开后重新读回。`run.read` 返回同事务 native 状态与 source/receipt，严格 decoder 校验其 kind/owner/状态一致性。断线不取消任务。

取消只能提交 `agent.cancel {session_id, run_id}` 的 native compact run id。旧聊天 owner 或客户端 reservation 不替代该身份。取消后 receipt 和 Cancelled terminal 原子发布，释放原 writer；同会话可继续聊天。

`compact.outcome` 为 `started/committed/no_gain/rejected/failed/cancelled/unknown`。`no_gain` 表示保持原 generation，没有成功摘要；`committed` 的 projection 和 terminal 同事务发布。Provider/校验失败不能发布摘要。重启将未结算 run 收敛为 unknown，持久化 `compact_terminal`；不按时间猜结果或重放 Provider。通用聊天 finish/reconcile 不能改写 compact 或伪造 projection。

## 入口与旧接口

CLI/TUI/Web 的 `/compact <session> <lifetime> <revision> <generation> <operation_id>` 经 daemon 同一 command 返回 Started 并订阅持久事件。`/run <native_run_id>` 在 CLI/ACP v1/Web 返回同一 source/receipt；ACP v1 不接受 compact mutation slash，ACP v2 投影属于后续 Wave 7。

HTTP `POST /api/compact/start` 接收 `{workspace?, params: <compact.start 参数>}`，沿用 Gateway bearer 校验与 workspace 绑定，审计 channel 固定为 http。最终恢复和 exact cancel 使用同一 WebSocket RPC。

旧 `session.compact`/`sessions.compact` 参数保持 `session_id/owner_run_id/operation_id/expected_revision`，由旧 owner 验证来源，再分配独立 native run，等待原 receipt；completed/no_gain 返回，拒绝/失败/取消/unknown 返回错误。重试返回原 run 与 `replayed: true`。兼容来源身份属于 operation 绑定，不能由缺少来源的请求冒领。已有旧 receipt 只读返回或报原失败，不补 native 身份或重放摘要。

## 迁移

v17 仅新增 compact linkage，复用既有 intent/projection/turn 表；升级前保留 verified backup 和 marker，故障回滚本次迁移，未来 schema 拒绝。旧二进制不能写 v17，回滚需使用已校验旧库备份。无 JSONL 双写。
