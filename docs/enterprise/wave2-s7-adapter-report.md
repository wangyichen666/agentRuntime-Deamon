# Wave2-S7：统一 SQLite 异步端口的有界执行适配

状态：implemented / locally verified；完整九门禁及真实 S3/PG 回归均已通过。保持 SQLite v24 / PG 发布协调 v1；没有新增业务表或 SQL 状态机。完整 Wave2–10 未完成。

## 问题与真实 RED

原 PublicationRepository 只有 async 签名，SQLite 实现直接调用同步 Connection 锁/SQL。daemon 用 spawn_blocking + runtime.block_on 包住 publisher，其他读回、生命周期调用者仍可能阻塞执行器。

[current_thread RED](evidence/wave2-s7-adapter-red.log) 使用另一真实 SQLite 连接持有 BEGIN IMMEDIATE 写锁：100ms timeout 被阻塞至五秒 busy_timeout 才得到 Ok(Err(StorageUnavailable))，全部耗时5.21s。不能把签名当作数据库异步隔离。修复后的同一合同及时 timeout，取消不提供回滚证明；解除锁后原事实可读回，[SQLite 发布合同8项](evidence/wave2-s7-adapter-green.log) 全通过。

## 实际实现

原 RunStore 共享连接与四个 permit 改为通用 blocking_budget。所有已接线的 PublicationRepository（九个方法）、SessionWriterRepository、ArtifactPublicationRepository 统一调用 storage::sqlite_blocking；仅该适配器安排 spawn_blocking，预算满立即 typed unavailable，不排无界任务。数据库 SQL、epoch、授权、原 event/view/outbox 和 commit fence 留在原事务方法，没有移动或复制业务语义。

已经开始的工作持有 permit 至真实提交/回滚，即使等待的 caller 被取消也不能提前释放预算。调用者仍需读回原事实，不能把取消、timeout 或 join error 当作 rollback。对象 adapter 原五秒 permit 等待改为立即拒绝；其已有实际 S3 并发预算和网络 deadline 保留。

daemon 使用原 JoinSet 单个 async 发布批次，移除 caller 的 spawn_blocking/runtime.block_on 包装。关闭仍先原执行体和后台收敛、原恢复，再 join 已开始 publisher，并释放 writer。原 readback/六入口无需各自增加线程或存储 owner。

## 验证

[取消与预算合同](evidence/wave2-s7-adapter-cancel-budget-fixed.log) 以四个真实 blocking 工作的开始通知及 Condvar 控制 barrier：取消四个 async caller 后，三个实际端口均返回 typed unavailable，预算仍为0；放行真实工作后恢复全部四个 permit，原 outbox 可读。测试 barrier 有 RAII 释放，断言失败也不能让阻塞线程永久悬挂。首次单包测试缺 Tokio test macro feature 的编译错误单独保存，不当作 RED；补仅开发依赖的 macros feature，不改变外部包版本或生产 feature。

[最终九门禁](evidence/wave2-s7-adapter-final-gates.json)全部退出0：459 Rust、36 Web、10外部测试显式 ignored。[真实 TLS S3](evidence/wave2-s7-adapter-final-s3.json)四项全部退出0，覆盖 SDK/TLS/对象完整性、原 native receipt/真实 daemon 六入口及后台 Cron/重启。[真实 PostgreSQL 发布协调](evidence/wave2-s7-adapter-final-postgres.json)六项均退出0；不代表 native PostgreSQL facts/writer通过。普通测试中外部 lane 仍显式 ignored，不能拿这些 ignored 当作真实环境通过。

## 配置、兼容与回滚

没有新配置，Local 无外部服务仍运行；一条 RunStore 物理连接的 clones 共享同一预算，独立进程各有自己的本地执行预算，数据库锁/epoch 才决定持久所有权。过载不伪造 run 成功或失败，恢复后按原事实读回。

schema 不变，可在支持 S6 OutcomeUnknown 和 v24 的兼容 binary 之间停机切换；不能退回不能解析新枚举的旧 v24 binary，不能 unset writer flag 绕过持久租约。既有 v24 备份、对象一致性和 expand/migrate/contract 约束仍适用。

仅这三组已存在 async ports 完成隔离；所有 native 业务的 async repository 迁移、全业务 PostgreSQL/native writer、audit/quota ledger、离线全量迁移仍待后续。不能用 PG 发布协调当作 Enterprise canonical facts。Enterprise readiness 保持 false；没有提交、推送或部署。
