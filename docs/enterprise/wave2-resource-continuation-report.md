# PostgreSQL 后台资源端口敏捷续开发（2026-10-09）

本轮继续 Wave2，不代表完整 Enterprise runtime 已完成。现有树的请求捕获及 compact 实现比进度记录更新，本轮按源码复用，未重建请求或压缩状态机。

## 源码证据与范围

| 能力 | 唯一 owner / 持久事实 | 真实端口与调用者 | 本轮结论 |
|---|---|---|---|
| 后台资源启动与状态 | storage resources，daemon 只监督 Child/取消句柄 | RunResourceRepository；daemon/resources.rs、后台 exec 与 resource 工具 | 已补 PG 八个方法，Local 继续原 SQLite 路径 |
| 日志与稳定游标 | resource_logs + resources.log_cursor | append_run_resource_log/read_resource_log_page；daemon 日志 drain | 共用 64KiB/UTF-8 预算，空输出不分配新游标 |
| 人工核验 | resources orphaned -> terminal，原 exact owner | reconcile_run_resource；resources.reconcile RPC | 共用有界证据规则，不发 kill、不重放副作用 |
| 实时投影 | 原 events/event_view_stamps/event_outbox | 状态更新复用 append_in/seal_in | 原事务封存，故障整体回滚 |
| PG 请求与压缩 | provider_requests/context_ledgers/compact_operations | RunRequestRepository/RunContextRepository | 已有源码，修正压缩终态归一化参数；本轮未验收压缩业务 |
| PG 完整启动 | 现有 daemon assemble_prepared 仍接 RunStore | 其余 PG memory/delegation/maintenance 等端口仍缺 | 未完成，enterprise_ready=false |

## 实现与事务边界

PG 读操作使用 REPEATABLE READ：完整 scope + 永久 lifetime 先于正文，SQL 抑制超大字段，正文 owner 再与 runs/turns 绑定。资源正文无需 writer，退休 lifetime 保留读回。

mutation 使用已绑定 writer proof，原事务锁 head/lease、核当前 run generation、冻结授权和资源 owner，提交前再核数据库时钟。跨 session 创建资源先获取域 advisory lock（seed44），再锁 head/lease/resource；最多16个 active 资源。新域锁仅序列化资源创建，不保存事实。Native 背景启动只允许 Local，强隔离要求不退回 Native。

状态更新、人工核验产生原事件、stamp和outbox；日志追加只更新原日志与游标，不新建事件副本。日志空文本、满额或不足一个完整字符时不新增行，避免仅字节限制留下无限空行。核验证据加入保存前缀后总长最多4096字节，修复原输入单独限制导致保存后读回超预算的问题。

不新增 schema、事实表或第三方包。SQLite schema27，PG canonical schema1保持；仅新增已有 agent-policy 的测试依赖。canonical schema1之前仍处于开发状态，本轮不会重建或改写用户数据库。

## 遗留编译问题修复

- core 委派模型沿用 SessionKey 别名，修正不存在的 SessionId 导入。
- SQLite 记忆读取闭包正确借用 RunStore；资源身份比较使用真实 bool 返回值。
- runtime 兼容记忆读取显式要求旧 repository，缺失时 fail closed。
- daemon 记忆 scope 不遮蔽 TenantContext；Hook 事件发布方法保留 Arc 所有权；同步 repository 测试字段初始化跟随 cfg。
- PG compact 终态沿用四参数共享归一化规则；去掉失效导入及无调用错误包装器。
- flywheel 测试补回 MaintenanceRepository trait 导入。

## 局部验证

最终源码的局部验证全部通过：

| 命令/范围 | 结果 |
|---|---|
| cargo check -p agent-daemon -p agent-storage-postgres --locked --offline | exit0，无告警 |
| cargo test -p agent-core --lib resource::tests --locked --offline | 3 passed |
| cargo test -p agent-storage --lib resources::tests --offline | 2 passed |
| PostgreSQL resource lifecycle/scope/log/rollback 合同，--lib --ignored，显式临时DB | 1 passed，真实 PostgreSQL 18.6 |
| 修改 crate 的开发格式化、git diff --check | 成功 |

PG 合同覆盖当前真实准入/冻结授权、运行启动、资源状态、租户与旧 owner 拒绝、UTF-8边界、满额日志、事件插入故障下资源状态/快照时钟/events/stamp/outbox回滚、人工核验幂等、重新建立数据库连接后的永久事实读回。P0001告警为合同主动注入的事件故障。没有启动实际 shell 进程，不能把它称为远程 worker 或进程重启验收。测试只在本轮新建临时数据库中运行，运行后已删除该数据库。

未执行全量 workspace tests、Clippy、MSRV、release、Web、cargo-deny 或完整九项门禁。此前656项验收不能证明当前树；本轮6个局部测试也不能替代其他领域合同。仅 locally verified，没有 CI/production 验收。

## 剩余限制

PG 完整 runtime 装配、其余领域端口、远程 TLS、离线迁移及完整企业功能仍待后续开发。已有资源端口沿用原接口，没有本轮扩展 operation_id；不得称整个 mutation 幂等性要求已经闭合。当前真实 PG 合同不证明真实远程 worker、container/Kubernetes 隔离或生产发布。
