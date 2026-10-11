# Wave2-S6：后台 Cron 复用原执行事实源

状态：implemented / locally verified；最终九门禁与独立真实环境 lane 均已通过。完整 Wave2–10 未完成，Enterprise readiness 保持 false。DDL 保持 SQLite v24。

## 问题与 RED

原 AgentCronRunner 每次打开另一 RunStore，不能持有启用 native writer 的 daemon proof，也未继承配置的对象后端。真实 RPC `/cron run-now` 在 [writer RED](evidence/wave2-s6-cron-writer-red.log) 返回租约失效。测试初期用错 slash 参数名、工具 deferred catalog 与缺少 export 的编译错误单独记录，不充当功能 RED。

真实定时触发还发现停用未来调度后正在执行的 Cron 被 daemon 当作空闲而退出：[初诊](evidence/wave2-s6-cron-process.log)、[EOF 诊断](evidence/wave2-s6-cron-process-diagnostic.log)。原 running 集合现参与后台活跃判断；停用配置不伪造当前运行结果。

[Provider route RED](evidence/wave2-s6-cron-route-red.log) 为期望原 attempt 1、实际 0。后台现冻结原 ProviderManager 路由和配置代，Context 与 LoopEngine 使用同一 snapshot。补齐后的 [Provider unknown RED](evidence/wave2-s6-cron-route-green.log) 表明中断收据仍为 Started；原 finish/recover 事务现转换为 OutcomeUnknown，而非编造 Failed 或成功。

## 实际调用链

原 daemon → AgentCronRunner.with_canonical_execution → 同一 ControlRepository/SQLite 物理连接与 proof → 原 create/admit/queue/turn/tool/provider/finish。独立历史使用新的 session key（job ID、时间纳秒和本进程序号），同 job 的立即重复不合并 run。原 ArtifactService 与 store_id 同时注入，后台 snapshot 冻结对象后端，原工具收据仍是唯一引用 owner。

Cron 成功需原 native finish 实际返回 Completed；原 run 被收口为 unknown 时返回 core RunOutcomeUnknown，调度历史记录失败且停止自动重试。Authorization/Coordination/Persistence/Artifact 也不被当作可自动重试的普通任务失败。已有无这些错误的确定性失败保留配置的有限重试。

新 AttemptStatus::OutcomeUnknown 在原 finish/recover 同事务结算未确认 Started，保留原 attempt 身份、usage 与 stream 证据；未完成 Provider 阻止伪造 Completed/Failed/Cancelled。原 event/outbox/turn/transcript/clock 同事务提交。晚成功不能覆盖未知 receipt。关闭先 join 后台执行体，再原 recover，最后释放 writer；强杀通过 DB 到期、接管与原 recovery 收口。

Standalone Local runner 仍可使用旧默认 RunStore，native 保护数据库的未持 proof 实例继续拒绝写入。这不提供 Enterprise 身份、后台独立授权或全业务 PostgreSQL。

## 测试与证据

- runtime Cron 8项：独立持久历史、连续同 job 不碰撞、有限确定性重试、无人值守审批、真实文件副作用一次且 unknown/attempt1/no retry。工具 fixture 必须命中冻结 core 名称，不能把未执行工具的错误当作副作用证据。
- 真实 daemon canonical Cron：[六入口](evidence/wave2-s6-cron-six-entries.log)通过；运行成功、统一 owner、冻结 route、原 Provider receipt、同源 sessions/run 读回与重启不重放。
- 真实定时触发：同时覆盖正常 stop 和 SIGKILL；以原 expiry 增加观测三秒续租，owner/epoch 不变；禁用未来 schedule 后当前 run 保持 Running；重启原 run/Provider receipt unknown，Provider 请求仍为两次。
- writer 合同14项：新增 SQL trigger 使 Provider unknown 更新失败，原 run/attempt/event/clock 全回滚；正常恢复后晚 success 零覆盖；未确认 Provider 不能借三种外部终态伪装确认。
- 独立 TLS MinIO lane 增加真实 native Cron 对象收据：opaque artifact:v1、冻结 backend、actual GET、正常退出与重启，同一原 receipt且无 File fallback。[真实 TLS lane](evidence/wave2-s6-cron-audited-s3.json) 四项全部退出0。

审阅新增 [损坏收据 RED](evidence/wave2-s6-cron-provider-corrupt-red.log)：JSON attempt_id 可指向另一条数据库收据。原恢复现在核对数据库主键、round、candidate、run 与状态，损坏输入在任何更新前拒绝，两个 run/全部 receipt/event 零写入。

首次九门禁仅 tests 因 Web fixture 端口预选释放竞态失败，其他八项退出0，失败证据保留。服务现显示真实 local_addr；八个 Web 子进程绑定端口0并读取自己 stdout 的实际地址，真实 daemon/入口合同38项全通过。首轮 reviewed 九门禁456 Rust/36 Web均通过；它不覆盖后来新增的损坏收据防护。最终 [audited 九门禁](evidence/wave2-s6-cron-audited-gates.json) 全部退出0：457 Rust、36 Web、10 外部测试显式 ignored。独立 [真实 PG 发布协调](evidence/wave2-s6-cron-audited-postgres.json) 六项全通过；不代表 native PostgreSQL facts/writer完成。

## 兼容、回滚和未完成范围

DDL 未新增表；OutcomeUnknown 是新增持久枚举，旧 binary 不能解析它，不能凭同为 v24 就原地回退。使用支持该枚举的兼容 binary，或停机恢复一致旧版本备份及匹配对象。升级/回退均先停准入并收敛执行，不在活运行中切换 writer 或对象 backend。

JSON Cron 配置仍为 Local 配置 owner，尚未整体迁到企业 PostgreSQL；Cron 的独立服务身份、scope/policy/revocation、审计/配额账本仍待后续。全业务异步 repository、native PostgreSQL facts/writer、离线全量迁移未实现，因此完整 Wave2 不通过，Wave3–10 尚不能标完成。没有提交、推送或部署。
