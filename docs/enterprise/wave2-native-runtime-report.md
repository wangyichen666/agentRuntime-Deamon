# 2026-10-09 PG 记忆、委派、维护与本机启动

本批按用户要求敏捷开发，只做必要编译和局部冒烟，不执行完整测试或九项门禁。完整 Wave2–10 仍未完成，enterprise_ready=false。

## 实施

- `ScopedMemoryRepository` 十二方法、`MemoryMaintenanceRepository` 及 `LegacyMemoryImportRepository` 已在 PG 实现。读写核对四维 scope、永久来源、冻结授权及 writer；记忆曝光、人工反馈、遗忘回执和有限重试沿用原表。遗忘删除正文后保留原反馈幂等证据，拒绝重新写回已遗忘 ID。SQLite/PG 共用有界摘录和凭据过滤。
- `RunDelegationRepository` 八方法已在 PG 实现。子运行复用原 `admit_in` 事务，冻结受限工具、预算、来源和继承授权，父子事件及 outbox 一并提交。限制两层委派、总量八个、同时活跃四个；原结果预留/交付使用 revision 和数据库时钟。过期预留在同一 owner 重领时也先释放，再取得新预留。
- `DaemonMaintenanceRepository` 四方法已在 PG 实现。恢复扫描有待恢复事实的会话，跳过其他活跃 writer；只收敛为未知，不重放 Provider/工具。人工核验使用原 owner/sequence CAS 和审计。文件回收与发布共用数据库锁；已接受的回收任务由存储持有并排空，调用者取消不提前释放锁。
- PG canonical 迁移版本升为 2，coordination 仍为 1。保留 v1 校验和，v2 删除可遗忘正文的反向外键、增加稳定记忆顺序/索引及相关原事实的 snapshot clock trigger，不另建业务事实表。
- daemon 启动可以从同一 adapter 构造全部 26 个领域端口。SQLite 默认保留；`postgres-local` 显式连接本机 Unix socket，强制 writer。会话/计划/记忆导入、恢复、Cron、artifact 和正常 chat 共用选中的后端；没有错误配置回退。仅空 Local 域初始化 Local human 及独立 Cron workload，不覆盖已管理域的主体或权限。
- 执行和事件消费通过 `tokio::join!` 同时驱动：事件写入等待 PG 连接时仍持续轮询执行中的事务；完成信号停止继续接收，并等待已接收事件写入完成、排空剩余事件后才提交终态。修复真实 PG chat 的连接等待闭环，不新增执行 actor 或副本状态。
- 本机 PG 文件目录按 database/schema 摘要隔离，复用原 no-follow 文件能力。`file-v1` 和 S3 的对象发布均使用同一 PG 事实源。测试兼容 SDK 句柄成为可选，PG 装配无需伪造同步 SQLite repository。
- 实际启动检查发现并修正异步请求端口遗漏冻结 token 预算的问题：发送前校验同时识别 native 与兼容端口。trace 文件校验同时接受原 SQLite 的 32 位十六进制 lifetime 及 PG UUID，仍拒绝路径字符。

## 本机开发入口

已有正常模型配置后，显式设置：

```sh
export AGENT_STORAGE_BACKEND=postgres-local
export AGENT_PUBLICATION_DB=你的开发数据库
export AGENT_PUBLICATION_USER=你的数据库用户
export AGENT_PUBLICATION_SCHEMA=agent_development
export AGENT_ARTIFACT_BACKEND=file-v1
cargo run --locked --bin my-agent -- daemon --workspace /你的工作区
```

支持 PostgreSQL 16+，本机 `/tmp` Unix socket；当前入口需要可信本机数据库操作身份及迁移权限。跨域物理文件回收需要数据库 superuser/BYPASSRLS，普通 RLS 角色不能执行该回收。`AGENT_SESSION_WRITER` 未设置时 PG 自动启用，显式 `0` 拒绝启动。SQLite 可继续不设置以上变量。

这个入口保留 Local 身份和传输，不能视为企业远程租户入口。切换后端不是现有 SQLite run/approval/事件的全量迁移；原 JSONL/计划/记忆兼容导入也不能替代离线迁移。

## 局部验证

- `cargo check --locked --offline -p agent-daemon -p agent-storage-postgres` 通过；daemon test-support 及内部测试源码编译通过，未运行其全量测试。
- 真实 PostgreSQL 18.6 局部合同 `postgres_native_memory_delegation_recovery_smoke` 通过：schema v2、Local Cron 独立主体、记忆/过滤/曝光/人工评价/遗忘后反馈幂等、子运行准入幂等、结果过期重领、摄入与维护、活跃其他 writer 不被恢复、重连后未知收口及人工核验。
- 既有 `postgres_resource_lifecycle_scope_logs_and_transaction_rollback` 在 schema v2 下通过：资源生命周期、日志、作用域及事件写入故障时的事务回滚与重连读回。故障注入产生的 SQLSTATE P0001 是预期结果。
- `cargo build --locked --offline --bin my-agent` 通过；真实 binary 的 SQLite / postgres-local 启动、会话创建、正常 chat、canonical run 读回、优雅停止、重启后请求幂等均通过，每个后端假模型只调用一次。PG 工作区没有生成 SQLite 事实库。脚本 `scripts/smoke-local-backends.py` 使用独立临时工作区、本机假模型，不访问真实模型服务。Postgres 参数必须指向自行创建的临时测试数据库。
- 未运行 Clippy、release、MSRV、完整合同套件或门禁；局部结果不证明企业 HA、多租户安全或远程 worker 全流程。
- 本批自行创建的临时 PostgreSQL 数据库已删除；双后端冒烟的 daemon 和临时工作区已清理。

## 剩余

远程 PostgreSQL TLS、企业租户启动/路由、SQLite audit/quota 对等和真实配额消费、可校验/回滚的 SQLite→PG 离线迁移及完整共享合同仍待实现；Wave3–10 尚未完整开发。未提交、推送或部署。
