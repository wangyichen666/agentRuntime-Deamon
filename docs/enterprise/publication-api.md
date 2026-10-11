# Wave2-S1 持久发布读回与本地 worker

所有使用面入口读 daemon 同一份 outbox，scope 由 Local 可信入口确定。没有 tenant/principal 参数。返回摘要不包含 prompt、工具参数或 payload 正文。

JSON-RPC `outbox.readback`，协商 maintenance v1。params 严格只允许 `after`（默认 0）和 `limit`（默认 32，范围 1–64）；cursor 为数据库稳定分页游标。返回 records/cursor/has_more；空页仍返回传入 cursor。每条摘要包含 event_id、完整 envelope、event_seq、event 名称、payload_digest 和 delivered。

- CLI：`my-agent --workspace /path outbox --after 0 --limit 32`。
- CLI/TUI/ACP v1：`/outbox`，读取第一页，输出相同 DTO。
- ACP v2：协商 readback 后 `_my_agent/outbox/read`，params 同 RPC；不要求 session control，不接受伪造域。
- HTTP：`GET /api/outbox?workspace=/path&after=0&limit=32`，沿用 Local Gateway Bearer/workspace 校验。
- WebSocket：连接 workspace 后透传 outbox.readback。

| kind | RPC/ACP | HTTP | recovery_action |
|---|---:|---:|---|
| invalid_request | -32602 | 400 | correct_request |
| forbidden | -32041 | 403 | contact_administrator |
| conflict | -32050 | 409 | read_existing_operation |
| lease_busy | -32051 | 429 | wait_for_owner |
| stale_lease | -32052 | 409 | acquire_new_epoch |
| storage_unavailable | -32053 | 503 | restore_storage_then_readback |
| corrupt | -32054 | 500 | verify_and_restore_backup |
| schema_too_new | -32055 | 500 | use_compatible_binary |

RPC error data 带 kind/retryable/safe_auto_retry/recovery_action；ACP v2/HTTP/WS 保留同一 code/message/data。对只读请求，只有 busy/storage_unavailable 可建议重试。授权 deny 继续经 Wave1 中央 gate；解析未知身份字段为 strict protocol error，不提升权限。表中的全部错误类型供存储协调使用，使用面只开放读回，不开放租约和 ack mutation。

## 生产调用与预算

Unix daemon 每 250ms 调度一次，最多一个受管 blocking 批次，扫描最多 16 个 key，跳过忙 key，处理最多一个有效租约/32 条事件。SQLite busy 上限 5 秒；所有失败保留 outbox。不因 listener 的 select 分支同步跑整批 SQL而阻塞接收。空闲退出前检查持久 backlog；关闭等待已开始批次结束，关闭期间的新 terminal 留待重启续发。

PG 独立本地 worker 与 daemon 使用同一 `publish_to_local_inbox`。显式本机 DB 配置示例：

```bash
cargo build --locked -p agent-storage-postgres
AGENT_PUBLICATION_DB=agent_publication AGENT_PUBLICATION_USER=agent_user \
AGENT_PUBLICATION_SCHEMA=local_publication target/debug/agent-storage-postgres --once
```

目标数据库由操作者准备。worker 不接受密码 DSN，不负责复制原 session/run 事实，不替代 daemon 的准入与执行。输出只有 consumer 与 delivered 数；SQL 日志只保留错误类别/代码和源码行，API 不返回 SQL、凭据或内部路径。PG statement_timeout=5s、lock_timeout=1s；正常退出等待 driver 关闭。

测试用隔离数据库的真实合同：

```bash
python3 scripts/enterprise-postgres-lane.py --user pilot
```

脚本新建 `codex_agent_wave2_<随机标识>`，构建真实 worker，运行显式 PG lane，finally 只删除该数据库。普通 cargo tests 中 PG 合同为 ignored；只有此独立 lane 的日志才是 PG 验证证据。拒绝把发布协调测试写成完整 PG run repository、远程 TLS、外部事件总线或 HA writer lease 已通过。
