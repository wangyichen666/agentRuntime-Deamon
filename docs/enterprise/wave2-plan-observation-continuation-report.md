# 2026-10-09 计划、诊断、Stop 续跑与 daemon 装配

按用户要求继续敏捷开发，沿用已有业务表和状态机。SQLite schema27、PG canonical1/coordination1 不变，没有增加第三方依赖，没有提交、推送或部署。

## 已实施

- PG PlanControlRepository 四方法：计划读回、丢弃计划、旧文件导入查询及写入。计划正文、Markdown、版本、identity、保存回执和永久运行归属相互校验。丢弃待执行计划调用原终态事务 helper，取消 queued run、计划拒绝和决策回执全部写入后才封存事件/stamp/outbox，并在提交前检查 writer。已运行的执行仍由原空闲检查拒绝。旧文件导入仅允许 Local；同 source 的不同 digest/lifetime 返回冲突。
- PG RuntimeObservationRepository 三方法：运行 transcript、上下文摘要和旧文件 artifact 读回。数据库读回使用同一 REPEATABLE READ 快照，校验永久 owner，正文与数量有界。文件读取复用 no-follow 目录实现和四 permit，结束数据库事务后执行阻塞文件 I/O；awaiter 取消不释放仍在执行 worker 的 permit。宿主文件仅允许显式配置的 Local 模式。
- PG HookContinuationRepository 三方法：成功 Stop Hook 准入一次续跑，以及父/子关系读回。调用原 admit_in，同事务校验父 Completed、成功的精确 Continue 指令、未发生过递归续跑、当前 writer/授权与原冻结能力。永久关系、child run、队列、冻结授权、事件和 outbox 一次提交。RunFact 与 Hook 读取共用关系 decoder。请求 ID 命中旧 run 时还需匹配永久父子关系。
- daemon 新增 DaemonRepositories，由同一完整 adapter 构造 26 个领域端口。实际 Local runtime 调用 bootstrap_with_repositories；旧 async SDK 签名保留并委托新方法。会话选择、历史导入、创建和读回由领域端口完成，assemble_prepared 接收端口集合。当前启动流程的租户仍是 Local，不能据此声称已实现企业租户路由。
- SQLite/PG 共用 transcript 批次 decoder，校验正文 SHA、序列区间、16MiB 正文和 32768 条消息预算。终态事务 helper 由最外层 command 统一封存，避免丢弃计划时 stamp 版本早于最终事务事实。准入 proof 容量满额时允许复用已经绑定的会话；计划决策输入按实际 JSON 编码后的字节数检查预算。

## 验证范围

最终生产路径 `cargo check -p agent-daemon -p agent-storage-postgres --locked --offline` exit0，无告警。开发格式化和 `git diff --check` 通过。最终 `cargo check -p agent-daemon --features test-support --locked --offline` exit0；仅兼容测试字段和资源测试 helper 的两组 dead_code 告警，没有通过 lint 例外隐藏。

本轮没有执行测试、真实 PG 数据库合同、Clippy、release、MSRV 或完整门禁；编译不能证明 SQL 行为、事务故障回滚和并发合同。上一轮资源测试只证明资源切片，S25 旧的全量结果不能证明本轮源码。

## 尚欠

PG 记忆、委派和 daemon 维护端口仍缺，不能构造完整 DaemonRepositories，PG runtime 仍不能完整启动。远程 TLS、租户启动/路由、离线迁移、SQLite 审计/配额对等和实际配额消费仍待实现。完整 Wave2–10 尚未完成，enterprise_ready=false。
