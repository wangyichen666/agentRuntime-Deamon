状态：implemented / locally verified。完整企业 Wave2–10 未完成，enterprise_ready=false。

# Wave2-S13：原准入、队列和终态事务

继续原 RunStore/Connection/单 writer proof 和状态机，不创建第二份运行对象。新异步端口会在准入事务内返回 ExactOwner，前台、工具审计、Hook 和终态使用同一份 owner。原有授权 run 的冻结 snapshot fence 仍有效；无授权 legacy run 的 scope 与 owner 采用风险须通过原 SQL 真 RED 证明。原同步兼容路径保留，与新端口共用同一事务体；不能把未迁移的同步 caller 算作已完成。

本切片准备迁移原前台授权准入、排队启动/取消与 TurnCommit；新端口 scope 和 owner 检查在原 mutation 事务内，policy/epoch/服务器时间和 writer proof 同事务复核。新异步准入先获取数据库写锁，旧同步路径保留 deferred 行为及原并发策略发布回归。schema25不变，原 unknown 收口和 transcript/turn/usage/plan/delegation/outbox 原子提交不能复制或拆开。

门禁要求：真实同步 Connection/current_thread timer、无授权 canonical foreign terminal、迟到 caller owner 重读 RED；scope/owner 各维、取消/过载、撤销、过期 proof 不重新获取、真实事务故障全回滚、双连接 request 幂等、重启/foreground caller 合同，再完整九门禁与真实 TLS S3/PG 发布回归。完整 PG schema/租户键/审计配额/离线迁移与其它后台同步端口仍待实现。

已完成三个有效 RED：原 canonical 无授权外域 terminal 返回 Completed、原 finish 重新采用后来 generation 返回 Completed、真实 Connection 占有使 current_thread timer 延迟约700ms。有效证据日志 wave2-s13-execution-red.log / wave2-s13-execution-timer-red.log；新 async 端口与原状态机提取已应用，真实 daemon 主准入/queue/terminal 和六处早期失败终态 caller 已接线，工具审计/Hook使用准入同TX返回owner。

局部 GREEN：八项 execution 合同通过；随后增加编码超限合同，预算检查使用 stream-count writer，不先分配完整 JSON。storage82 项通过，原 Connection 锁 RED 715.9ms 已改为异步 yielding；实际 commit 后 caller 取消，blocking worker 返回前仍占 permit，重复 TurnCommit 不产生第二 terminal/outbox。初始夹具把原 snapshot_clock 误当成每事务递增一次、误在活跃 legacy 运行时开启 writer，修正为原 SQL 事实计数/重复请求零写，保留原 activation Conflict；修正期间变量未声明编译失败也保留，均不计新增功能 RED。

真实前台合同 foreground_terminal_keeps_admitted_generation_and_restart_never_replays_unknown_provider 通过：Provider 接收请求后注入 generation 损坏，foreground 返回 typed StaleOwner，run 仍 Running，未发布 assistant batch 或 turn output。仅在测试中撤销该 SQL 注入后强杀重启，原 recovery 收口 unknown_after_restart，Provider 始终一次；这不提供生产自动修复或新 reconcile 授权。未撤销损坏不得声称普通重启可以恢复。

适用入口沿用原 chat/cancel 命令和现有 DTO。原会话 create/read、route/snapshot 恢复、plan startup、事件/Provider/工具 mutation、审批消失处理与 Cron 仍有同步 caller；排队取消的 owner 解析仍同步，终态/队列最终 TX 已检查完整 scope 和固定 owner。完整管理授权 verifier/策略版本的管理 mutation 最终检查仍欠，不能称完整 RBAC 或全异步后台。

首次完整门禁暴露实施回归：ACP 原 standard_acp_streams_updates_and_round_trips_permission 在默认栈发生 overflow；原用两个 async wrapper 将大执行 Future 重复内联。改为在普通函数入口 Box::pin 原执行 Future，唯一 task-local helper 持 heap pointer，未扩大栈、未 ignore 原合同。原 ACP 测试已在默认栈通过；新增256KiB执行Future/上下文小于4KiB/原owner保留的两种caller合同通过。Clippy explicit-auto-deref 修为自然借用，无 allow。失败日志 wave2-s13-execution-final-tests.log / clippy.log 保留；接着完整九项门禁重跑。

最终本地验收：wave2-s13-execution-stack-final 九项全部exit0，511Rust/36Web/45真实daemon，外部10项明确ignored。执行合同9/storage82/runtime131均通过，原ACP标准与前台generation故障合同保持，未加栈或跳过测试。真实TLS MinIO四项与PostgreSQL发布协调六项独立回归全部exit0，见 wave2-s13-execution-regression-s3.json / postgres.json。原失败门禁和夹具失败均保留；未CI/生产验证、未提交。全企业Wave2尚未完成，Wave3–10 pending，enterprise_ready=false。继续原Provider attempt/未知结果和实际runtime caller，不能称所有ports或全部PG事实已迁移。
