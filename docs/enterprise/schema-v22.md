# SQLite v22 与 PostgreSQL 发布协调 schema v1

状态：已实现；v22 在原 `views::commit` 事务中封存新事件，旧业务表仍为唯一事实源。PostgreSQL v1 是独立发布协调 schema，不能作为完整 Enterprise 业务数据库启动。

| 表 | 键、事实和约束 | 写入边界 |
|---|---|---|
| event_outbox | 稳定 cursor；完整组织/租户/项目/Agent/ExactOwner、event seq、payload 和 SHA；域/run/seq 唯一；不可改 payload | SQLite 原 event/view 封存事务；SQLite FK 指向实际 events；PG 适配器接收已封存 envelope |
| delivery_leases | 完整域/session/lifetime 主键；owner、单调 epoch、DB 时间 expiry | SQLite BEGIN IMMEDIATE；PG 同键行锁；共享 core 状态转移 |
| publication_inbox | 域/consumer/event ID 主键；只保存 ID/digest 收据 | 独立提交；完整域+event ID+digest FK，不能引用外域或不同摘要 |

`delivered` 和 claim owner/epoch 是发布进度，不能更改 session/run 的业务状态、授权或终态。发布租约与运行 writer lease 是不同能力；本切片没有把 publication epoch 当成旧 worker terminal 的 fence。

Native 事务次序：原 session/run/授权 fence → business event → 最终 ViewStamp → SHA/outbox → 同一 commit。outbox 插入错误整体回滚原业务事件和 terminal。既有 published=1 历史不补写 outbox。

Publisher 次序：lease → outbox claim → inbox receipt commit → ack commit → release。PG 行锁在事务内验证 DB 时间、owner/epoch；SQLite 用同一写事务验证。receipt 必须已经提交才能 ack。失败保留待发记录；receipt 后 ack 前故障会再次投递同一 ID，inbox 以唯一键去重。这个保证只覆盖本地 durable journal，不代表外部 Kafka、Webhook 或客户端恰好一次交付。

锁顺序是 lease → outbox → inbox；事务不跨网络投递。SQLite 有单写事务，PG 不同域可并发，但同 key 用 FOR UPDATE 串行。源事件和 payload SHA 校验失败时拒绝读回，不返回损坏正文。

## 升级、恢复与回滚

SQLite 开库先拒绝 version>22，v1–21 开库升级前沿用 VACUUM INTO/integrity_check/版本/SHA 备份。v22 DDL 独立 BEGIN IMMEDIATE/COMMIT。回滚旧 binary 必须停止新进程、保留待发证据、恢复经过 marker 校验的备份，再按旧版本启动；禁止删迁移版本伪降级。备份本身不作为新程序的工作库，恢复到另一路径后再开库。

PG adapter 只接受本机 `/tmp` Unix socket，以及显式 database/user/schema 标识；拒绝 public/pg_* schema。schema migration 用事务 advisory lock，未来 version>1 拒绝并回滚。没有提供远程 plaintext DSN、完整业务 SQLite→PG 切换 marker 或跨数据库双写模式。

PG driver 为适配器所有；异常 Drop 取消，正常交接 `close().await` 等待退出。失败事务的 rollback 需要 driver 运行，禁止在 async runtime 中同步等待外部进程并阻塞清理。测试曾用真实 worker 捕获 55P03 锁超时，修复和原失败证据均保留。
