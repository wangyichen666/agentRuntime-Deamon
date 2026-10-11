# Wave2-S5：原生 session writer 租约

状态：implemented / locally verified；最终九门禁与真实 S3、PG 发布协调回归已通过。完整 Wave2、Wave3–10 未完成。本切片新增 SQLite v24，保持旧 Local 在没有 native lease 行时的行为；显式 `AGENT_SESSION_WRITER=1` 启用本地单工作区保护。全业务 PostgreSQL 与离线迁移继续待实现，Enterprise readiness 仍为 false。

## 问题与真实 RED

两个独立运行目录的活 daemon 连接同一 SQLite 工作区时，原 startup recover 会把首个活 run 改为 unknown。`second_live_daemon_cannot_recover_the_first_daemons_native_running_session` 在原实现断言 Running、实际 UnknownAfterRestart：[RED](evidence/wave2-s5-writer-red.log)。最初 SQL 列名错误、测试 fixture 或观测时序错误分别保留，不能充当需求 RED。

## 实际实现

- core SessionWriterLease/Key 使用独立类型，复用既有纯租约 CAS 算法；完整 scope/session/lifetime/owner nonce/epoch/DB expiry。
- async SessionWriterRepository 的 SQLite 实现有生产调用者；阻塞事务统一留在 storage adapter，最多四个受管工作，共享预算满时立即 typed StorageUnavailable，取消不能丢弃已开始工作持有的 permit。
- 原 admission、tool、provider、terminal、transcript、resource、Hook、生命周期的实际写入事务复核 writer。原 view commit 在开始与实际 commit 前检查；原 tool receipt 的无 event 提交也直接检查。失效进程不能自动取得新 epoch。对象发布失效返回 StaleWriter / -32063 / HTTP 409 / 非自动重试，不能伪装为可重试 StorageUnavailable。
- daemon 启动先取得所有 head 的恢复保护，再执行原 recover。第二活实例租约 busy 时整笔恢复回滚，不改变原 run。每三秒受管续租，TTL 十五秒；启动探针期间也续租，开放 listener 前核验，listener 的 ready/fingerprint 发布阶段由原受管 task 覆盖。失败只报告 suspect，实际数据库 fence 决定写入；不凭 heartbeat 取消或伪造终态。
- 有序关闭停准入、join 原执行体、持久化未知结果、join 后台与 outbox 后才释放缓存租约。SIGKILL 保留 DB 到期与 unknown/no-replay 恢复语义。
- `runtime.doctor`、CLI doctor、ACP v1 `/doctor`、ACP v2 `_my_agent/runtime/doctor`、HTTP `GET /api/runtime/doctor`、WebSocket 同原 RPC 读取 writer 摘要，不包含租约写入 API或凭据。数据含 DB 时间、held/stale/expired/max epoch 与预算，读取不推进业务 clock/event。
- ready marker 改为完整 JSON 原子发布。已绑定 socket、尚未发布 marker 时客户端有限等待，不能误判旧 binary 后替换活 owner。进程 fixture 等待真实 ready 证据。

## 验证与故障

`tests/writer_contract.rs` 验证真实 DB 时钟/完整域/续租/release/epoch CAS，两物理 SQLite 连接 Barrier 只有一胜者，活 owner recovery 冲突零写，到期 takeover 后原 terminal/transcript/tool/provider/resource/ownerless Hook 迟到结果拒绝，未配置连接跨新 session 绕过拒绝，fork/clear/tombstone/recreate 的 writer fence，v23 备份 SHA/迁移故障回滚/未来版本拒绝。真实 daemon 合同验证活实例竞争、强杀后注入 DB 到期再接管、unknown Provider 不重放，以及诊断六入口与有序退出后立即重启。

复核新增 [对象错误分类 RED](evidence/wave2-s5-writer-artifact-red.log)与[活旧 run 启用冲突 RED](evidence/wave2-s5-writer-activation-red.log)，修复后的完整 native 仓库合同通过。另用原 SQL trigger 在 tool terminal 与 view commit 内注入到期，验证工具收据、turn、transcript、event、snapshot clock 与 lease 故障同事务回滚；ready socket 双观测 barrier 证明缺 marker 时不发送升级 stop。第一轮完整九门禁 447 Rust/36 Web 全通过；这不是最终新增错误/激活/提交故障代码的验收。最终 [九门禁](evidence/wave2-s5-writer-final-gates.json) 全部退出0：451 Rust、36 Web，九项外部测试显式 ignored。独立 [真实 TLS MinIO 回归](evidence/wave2-s5-writer-regression-s3.json) 三项通过，包含实际对象、原工具收据与真实 daemon 六入口/重启。独立 [真实 PG 发布协调回归](evidence/wave2-s5-writer-regression-postgres.json) 六项通过。PG lane 只验证既有发布协调六项，绝不作为 PG native writer 或全业务事实源验收。

## 配置、限制与回滚

```sh
AGENT_SESSION_WRITER=1 my-agent --workspace /path/to/workspace daemon
my-agent --workspace /path/to/workspace doctor
```

首次启用前须停准入、join 执行体并按旧 Local 原恢复/人工核对收敛；未持 native lease 的活 run/resource/Hook 存在时配置 Conflict 拒绝，不能打开开关就接管它们。后续 daemon 必须通过相同配置启动。已经建立 native lease 后，unset flag 会拒绝 canonical recovery/writes；这不是一个可绕过的开关。API 没有暴露 owner nonce、任意 acquire、release 或 epoch 注入。测试的 SQL expiry 修改只用于受控故障 oracle，不能作为运维接管命令。

当前恢复取得本工作区全部 head，超过 1024 个 fail closed；不能声称多 worker 的按 session 分配、PG HA、remote worker fencing 或强隔离已经完成。SQLite 旧状态与授权 owner 保留，新的 scope 查询不是完整 Wave1 全域隔离的替代。后台配置、MCP、Cron 尚未整体企业化。原 Cron runner 仍自行创建另一 RunStore，native 模式下不能取得 daemon proof，执行会 fail closed；该当时限制已由 [S6](wave2-s6-cron-report.md) 修正：复用 canonical repository/proof/ArtifactService，补真实后台 caller、Provider冻结/unknown与定时触发恢复合同；后台整体企业化仍未完成。

迁移/锁序与恢复说明见 [schema v24](schema-v24.md)。没有提交、推送、发布或部署；schema 只前进。旧 binary 回滚必须使用停机且一致的 v23 备份与对象，不得直接打开 v24；已写入的 v24 数据须离线一致性导出后再决定回退。
