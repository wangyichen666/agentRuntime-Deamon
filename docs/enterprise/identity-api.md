# Wave1-S1 身份与 run 授权读回

所有入口使用 daemon 持久事实。Local scope 固定为 local-organization / local-tenant / local-project / local-agent；可信主体为 local-subject/human。它代表原工作区权限，并不创建远程 Enterprise 认证。响应 enterprise_ready=false。

## 身份

JSON-RPC：identity.readback，params 必须是 `{}`。协商能力 configuration v1。返回 scope、principal、roles、policy、policy_digest、enterprise_ready；无 secret 或 prompt。

- CLI：`my-agent --workspace /path identity`。
- CLI/TUI/ACP v1：`/identity`，共享 slash registry；v1 输出同一个 JSON 事实。
- ACP v2：初始化协商 readback 后 `_my_agent/identity/read`，params `{}`。不可携带 sessionId、tenant 或主体 metadata。
- HTTP：GET /api/identity?workspace=/path，沿用本地 Gateway Bearer 校验和 workspace 选择。
- WebSocket：连接 workspace 后透传 identity.readback。

## 已准入 run 的授权

JSON-RPC：run.authorization，协商 runs v1，params 为 `{ "owner": { "session_key": "session.jsonl", "session_lifetime_id": "…", "run_id": "run-1", "run_generation": 1, "turn_id": "turn-1" } }`。

返回 `{ "snapshot": … }`。没有 S1 授权历史的旧 run 返回 snapshot=null。快照包含 owner/scope、principal、角色、policy version/document/digest、RequestId/TraceContext、属性与原 RunSnapshot SHA。读取不重新调用 Provider，不重建授权历史，不推进事件游标。

## 稳定授权错误

| kind | JSON-RPC/ACP code | HTTP | recovery_action |
|---|---:|---:|---|
| unauthenticated | -32040 | 401 | authenticate |
| forbidden/revoked | -32041 | 403 | contact_administrator |
| stale_policy | -32042 | 409 | refresh_policy |
| stale_owner | -32043 | 409 | refresh_owner |
| protocol_violation | -32602 | 400 | correct_request |

授权失败 data 同时给出 kind、retryable=false、safe_auto_retry=false、recovery_action。ACP v2/WS/HTTP 保留原 code/message/data；CLI 保留 ClientError::Rpc 信息并退出失败。普通 legacy 错误映射保持原行为；本表并非已完成全平台统一 RuntimeError。未知身份字段属于 strict protocol 错误，不能选择请求主体。API 不提供 policy publish/revoke、独立 Admin audience、OIDC、双人审批或全资源跨租户查询。

## 操作边界

策略的 workspace 条件只使用服务器 SafetyPolicy 工作区；入口 channel 属性取可信 daemon_rpc。客户端 entry_channel 继续用作原 hook 展示标签，不能提升权限。工具、protected path、secret 和 sandbox 的原 hard constraints 继续由原执行链检查，RBAC 没有跳过它们的分支。

IdentityRepository scoped SQL 与授权准入事务已接线；其他 legacy Local ports、cron/compact 等后台的完整授权与新子任务的独立策略准入和持久审批人规则仍是后续切片。不得把 Enterprise listener 指向当前 Local daemon 后声称获得租户隔离。
