# Wave2-S24 原发布协调完整key与保存行校验

状态：已实现并完成本地验收。完整九门禁全部exit0：640 Rust /36 Web，44执行体、14外部环境ignored；48实测daemon合同通过。真实TLS S3四组业务与PostgreSQL发布协调10项合同回归全部通过，自己的容器与独立DB已清理。SQLite12项/真实PostgreSQL10项局部合同亦通过。SQLite schema27、PG发布协调schema1均未增加新表或迁移，没有新第三方包。完整Wave2–10未完成，enterprise_ready=false。

原两个adapter的record、claim、claim_owner读取、ACK更新及幂等lookup已使用完整DeliveryLeaseKey与event_id SQL谓词。外域记录在装入payload前返回Forbidden。原唯一 `storage-ports::decode_record` 同时核SQL scope/session/lifetime/run_id/event_seq与封存JSON；`OutboxRowBinding`只是这些原列的临时只读投影，不保存第二份发布事实。JSON/原SHA/原event_id算法、原inbox引用、lease epoch与同事务提交保留。

保存payload最多4MiB，id/digest必须64字节；scope≤128字节、session/run≤1024字节、lifetime≤128字节。两个adapter用SQL CASE抑制超预算字段，PG不会先把大payload传给Rust；SQLite同时核TEXT类型，逐行经过原decoder，不先collect所有raw。pending_delivery_keys的session/lifetime也在SQL预检，坏候选明确Corrupt，不静默跳过。此处是单字段与原行数预算，没有声明新的累计page字节协议或全业务PG后端。

三类有效功能RED分别在两后端实际复现，共六个独立合同：六维外域租约ACK/inbox共12次均先Corrupt；保存SQL域列不同、原JSON/SHA合法时六维读回均Ok(1)返回原envelope；保存>1MiB session_key的pending读取仍Ok(1)。日志 `wave2-s24-publication-sqlite-functional-red.log`、`wave2-s24-publication-functional-red-postgres.log`、`wave2-s24-pending-budget-sqlite-functional-red.log`、`wave2-s24-pending-budget-functional-red-postgres.log`。pending RED在第一个session_key断言即停止，不声称lifetime也已独立RED；GREEN同时验证两字段。全部仅在隔离故障夹具显式移除/关闭原immutable触发器，正常业务写不能据此绕过该触发器。

局部GREEN最终为 `wave2-s24-publication-sqlite-final-corrected-green.log`（12项）与 `wave2-s24-publication-final-green-postgres.log`（10项）。补充SQL run/seq列时，首SQLite夹具被真实FK正确拒绝、未到decoder，失败留在sqlite-final-green.log，不计业务RED；改用真实events引用目标并移除其协调row避免唯一键碰撞，保留FK约束。PG大JSON在自己的隔离DB生成，避免把大payload放进进程命令行。合法大领域JSON的拒绝以及零claim/inbox/ACK/clock修复为故障回归，旧decoder也有后置4MiB拒绝，不伪称它是新增功能RED或独立内存测量。

覆盖组织/租户/项目/agent/session/lifetime、额外SQL run/seq身份损坏、超大保存字段、原不可变触发器、scope外先拒绝、两真实Connection lease竞选、收据后ACK前重启幂等、过期epoch和断连/未来schema、原事件/outbox整体回滚。所有真实PG lane只操作随机隔离DB并清理自己的DB，不把环境创建步骤计作业务合同。

Hook/MCP/memory/resource/delegation/interaction/管理等剩余async链路、全业务native PostgreSQL facts及正确租户键、PGwriter、完整audit/quota与离线迁移仍欠；未提交、推送或部署。

最终证据：`wave2-s24-reviewed-final-gates.json`、`wave2-s24-reviewed-regression-s3.json`、`wave2-s24-reviewed-regression-postgres.json`。14外部ignored包含本批新增PG合同，未计作workspace已执行项；实际PG10项由独立真实lane执行。完整Wave2仍进行中。
