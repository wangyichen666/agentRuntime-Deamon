# Wave2-S17 请求捕获与上下文来源异步迁移

状态：implemented / locally verified。完整 Wave2–10 尚未完成，enterprise_ready=false，本切片初始 SQLite schema25；后续S18/S19使当前程序schema27。

原 `request_context_source`、`record_provider_request`、`provider_request` 只保留同步兼容 wrapper，唯一事务 body 接受 scope 或固定准入 envelope。`RunRequestRepository` 三个端口复用原 Connection 和共享四 permit；前台普通模型、压缩摘要、daemon `context.readback` 和 `/context` 真实 await。来源与原冻结快照在同一读事务返回，摘要回调改为 owned 消息和 boxed async Future，保持原 task-local owner、默认执行栈。

native 捕获在原 BEGIN IMMEDIATE 事务检查完整范围、固定 owner、永久 lifetime、writer proof、冻结授权和 running 状态；原 provider_requests/context_ledgers 同事务提交，数量1024、capture id/digest幂等保持。重复捕获额外核原 SQL round/candidate、saved JSON 和预算，损坏事实不能冒充幂等。冻结 route 的候选、model 和 provider identity 必须与输入一致，读取复用相同校验。只读重建核所选 SQL run/capture/round/candidate 与完整 JSON owner，允许同范围读者读取终态历史，不取得或续租 writer。

捕获使用流式计数先拒16MiB编码预算；保存 capture/冻结 snapshot 在读取前检查 SQL BLOB 字节长度。canonical transcript 总量16MiB、当前 projection4MiB、冻结 snapshot1MiB，来源最多32768个batch与32768条消息；超出预算明确拒绝，不截断历史、不构造空来源。目录、网络和新的业务账本均不由这些端口创建。

六项有效功能 RED 已保留：scope来源读、capture写、重建读，以及完整合法 JSON/digest 对应错误 SQL capture_id（四项同一测试运行独立失败）；合法冻结 policy/catalog 摘要但实际 route/provider identity 不匹配仍成功写入（另一个独立 RED）。另有32769个合法空batch使来源成功分配过多行的有效RED（wave2-s17-request-row-budget-functional-red.log）；修后15项合同全部通过：完整scope/owner、SQL与JSON身份、同 session 的两 run 合法捕获错行、终态前拒发送而历史读回零写、回滚、幂等、两连接竞争、真实 current_thread SQLite 写锁下 timer 与取消后 worker 完成、输入/保存 JSON预算、撤销/过期且零新epoch、冻结路由。证据 `wave2-s17-request-functional-red.log` / `wave2-s17-request-route-red.log` / `wave2-s17-request-audited-green.log`。

实施错误已保留：生命周期验证提前返回Conflict而非StaleOwner、SQLidentity元组替换未命中导致类型编译错误、对未结算旧运行开启writer被安全activation拒绝的夹具错误；另有首次库回归混用typed ToolSpec与排序Value历史合法摘要表示导致两个未知副作用合同在发送前失败，已按各自原编码核同一catalog，两个合同保持；均修正且不计功能RED，不放宽原保护或测试断言。原库回归经修正兼容摘要表示后通过：133 runtime / 83 storage，全新三端口满额合同与摘要await-before-wire合同包含在内；真实普通请求及compact-summary两步局部回归通过，完整九门禁/S3/PG已通过，见最终记录。

仍欠原 compact 准入、读取、结算与 memory exposures 等同步领域；Cron由S19接固定owner及同canonicalports，但该后续批次尚未完整验收，全业务 native PostgreSQL、正确租户键、native PG writer、完整 outbox/audit/quota/offline 迁移均未闭合；此切片不能证明全企业后端或完整 Wave2 已完成。


S17–S19 implemented / locally verified：wave2-s17-s19-reviewed-final 九项全部exit0，588 Rust / 36 Web / 48真实daemon，42个test executables、10外部环境项明确ignored；runtime136/storage83、request15/event12/workload7合同全部通过。独立真实TLS MinIO四组业务与PostgreSQL发布协调六项全部exit0（wave2-s17-s19-reviewed-regression-s3.json / postgres.json，PG runner五步骤不等于五合同）。S17三请求端口实际模型/摘要/读回await；S18原events不可变来源及native缺stampCorrupt；S19原同TX后台准入、固定owner及Local独立workload。schema27，不新增第三方包。首轮Clippy常量放testmodule后失败、foreign夹具writerFK失败及0tests过滤错误均保留，不计功能RED；移动常量后完整九项重跑，未放宽lint/超时/栈/架构。全企业Wave2、Wave3–10仍未完成，enterprise_ready=false，未提交推送部署。继续S20 compact实际RED与原唯一helper迁移。
