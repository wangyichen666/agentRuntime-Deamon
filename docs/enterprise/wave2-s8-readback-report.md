# Wave2-S8：scoped 异步 canonical 会话读回

状态：implemented / locally verified（2026-10-08）。完整 Wave2–10 未完成，enterprise_ready=false。

## 范围与所有权

修改 core/storage-ports/storage/daemon/daemon-client/cli/acp/gateway 与原合同。schema 从 v24 前向到 v25，新增永久 lifetime 归属；PG 发布协调仍 v1。原 canonical facts 和单事务读回继续为唯一 owner。SessionReadRepository 只依赖 core，两个方法均有 SQLite 实现和 daemon 生产 caller，复用统一四 permit 的有界阻塞适配器。列表摘要 SessionListEntry 是纯投影 DTO。

原 session load/page/resume/create/close 的 canonical readback 和 slash snapshot 改 await 新端口。列表的 metadata/preview/status/count 在同一个数据库快照中产生，替换旧同步 metadata→逐项 preview→逐项 readback 拼接。daemon 只附加展示路径和本地 active 标签。组合时所有端口仍指向原 RunStore 的同一物理连接与 writer proof。

CLI 新增 `my-agent --workspace <path> session-read <key> --history-mode canonical|model|omitted`；HTTP `POST /api/sessions/read` 使用严格 `{workspace,params:{session_id,history_mode}}`，透传同一 daemon command。ACP v1 /snapshot、v2 _my_agent/session/read、WS sessions.read 使用原路径。scope 是服务端 Local 可信域，DTO 不接受 tenant/project/agent 覆盖。

## 真实 RED 与修复

`wave2-s8-readback-valid-red.log`：旧原读回可读取 foreign session；旧分页把 foreign 放到 limit=1 的第一页；退休 ownerless lifetime 可重新绑到另一会话；原 run lifetime 损坏把 B 的 terminal 投影到 A。初始 fixture 的 Message/finish API 错名和 duplicate 当前 head 的 UNIQUE 拒绝不是真实 RED，分别保留 fixture/初始日志；有效 RED 保留原 UNIQUE，改用退休 lifetime 重绑定。

v25 归属/迁移/回滚语义见 [schema 文档](schema-v25.md)。scoped metadata 与页查询在 SQL 层完整约束 org/tenant/project/agent/key，越域和不存在返回同一 Forbidden。原 transcript/plan 通过永久 owner JOIN，native 子查询同时带 session/lifetime，interaction/queue 复核对应 owner 字段。

真实六入口测试发现 DaemonClient 把 error.data 当成请求方法的成功 SessionReadback 解码，ACP v2 read_snapshot 又把 RpcError 改 Internal。修复后普通错误不参与 reducer，显式 session_changed/compact_started/stamp 的 canonical 错误证据仍保留；ACP v2 保留原 typed code/data。错误按稳定 code 投影，HTTP 403 对应同一 Forbidden，SQL/内部路径不进入消息。错误不是新的终态。

第二次进程测试的 clock 变化来自测试启动两次 `chat` 自动创建 Local 会话，不能把它算成 repository 写入。改用真实只读 session-read CLI，保持全局 snapshot_clock 零写入断言，不降低断言。

## 合同与验证

七项 readback 合同覆盖四类 RED、完整 scope 四维、分页 limit/cursor、无全局 ID 后过滤、三个 history mode、重启、只读 clock、历史保留、迁移冲突/故障整事务回滚、备份实际 SHA-256 和不可改删归属。共享 adapter 取消/过载测试补新 read/list 端口，保持实际工作持有 permit。原读写并发、transcript/plan/fork/Cron/native writer/对象/六入口合同全部纳入九门禁。

新增真实进程 scoped_async_session_readback_is_consistent_across_six_entries_and_restart：RPC/CLI/ACP v1/v2/HTTP/WS 对 foreign/absent 同一拒绝，HTTP 严格 DTO、列表不可见、全局 clock 零写、Provider 调用零次、有序退出后重启仍同一拒绝。成功路径核原 RPC/ACP v2/HTTP DTO，原全入口恢复测试继续覆盖 canonical 成功读回。

最终 [九门禁](evidence/wave2-s8-readback-initial-gates.json)全部退出0：467 Rust /36 Web /10外部ignored；[真实TLS MinIO](evidence/wave2-s8-readback-regression-s3.json)四项和[真实PG发布](evidence/wave2-s8-readback-regression-postgres.json)六项通过。mock Provider 不代表商业 Provider 验收，PG 发布合同不代表全业务 PG。

## 运维与后续

升级、legacy_orphan 隔离证据与停机回滚见 schema-v25。没有提交、推送或部署。新增库依赖/第三方版本为零。

剩余是全部 native 业务 async ports、完整 PG schema/复合租户键/原事务/PG writer、审计配额账本与离线迁移。旧同步 Local 端口在尚未迁移的 runtime/启动/维护调用者处保留；不能把本次会话读回迁移称为全业务异步化。canonical 完整历史仍保持原全量语义，后续全业务读预算/分页迁移不得伪造当前完整完成。Wave3–10 依用户波次门禁顺序等待完整 Wave2，不能先标完成。
