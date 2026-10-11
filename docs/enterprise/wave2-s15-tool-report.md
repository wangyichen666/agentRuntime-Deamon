状态：implemented / locally verified。完整 Wave2–10 未完成，enterprise_ready=false。

# Wave2-S15：原工具事务异步端口、固定 owner 与未知结果

RunToolRepository 六项方法由原 foreground runtime 真实 await：准备批次、Started、关闭 exchange、完成批次、File 输出两阶段提交、冻结对象后端读回。沿用原 tool_batches/tool_executions/transcript/events 和原 helper SQL，统一四个 blocking permit、canonical Connection；scope、固定 ExactOwner、当前 head/永久 lifetime、writer proof 和冻结授权在原事务内复核。未另建工具状态机、事件账本或存储事实源，schema 保持25。Cron 兼容调用仍用原 helper，未称全业务异步化。

四项各自有效 RED 保存在 wave2-s15-tool-functional-red.log：旧 generation 通过重读后来 owner 启动工具、越域 legacy run 启动工具、不存在的 batch 生成完成事件、越域生成完成事件。修后固定准入 owner 贯穿实际 caller；Started 仅一次 prepared→running。原 native 批次重复准备核完整 calls/descriptors/effect/digest/replay，payload 一致零写；两 Connection 可读回同一批，但只有一个能进入副作用。批次完成先核原 closed 状态并复用原事件幂等，不为缺失批次制造事实。保存批次先在 SQL 核编码字节预算，避免先分配无限文本。

原 execute_in_waves 在工具已接受并提交 outcome_unknown 后仍开始后续外部 wave。实际原 runtime + native ports + 本地副作用探针 RED 保存在 wave2-s15-tool-unknown-functional-red.log：后续副作用1、Provider请求1，最终是通用 Coordination Conflict；该 RED 没有观察到第二模型轮，不夸大证据。修后全部已启动 future join，未知收据先持久化，随后返回 typed ToolOutcomeUnknown，不开始后续 wave/模型轮。GREEN 为未知副作用1、后续副作用0、Provider请求1、原 run UnknownAfterRestart。该合同不是商业外部服务验收。

File 大结果以 owned String 进入 worker，避免 permit 前再复制64MiB输出；hash/stage/finalize 在共享 worker 内，原发布事务唯一提交工具终态。新 core RepositoryError 保留纯 ArtifactError 分类，不含 SQL/路径。收据摘要错误、原 SQL terminal 故障回滚、相同收据重放、冲突拒绝与两个 Connection 的终态竞争均有实际合同。取消等待者发生在原 File commit 后时，原收据和对象仍可读回；实际 worker 返回前 permit 不释放，重复端口调用不产生新 clock。

局部证据：wave2-s15-tool-scoped-green.log（四项）；wave2-s15-tool-race-green.log（十项）；wave2-s15-tool-all-contracts.log（十一项，六方法撤销/过期零新 epoch）；wave2-s15-storage-local.log（83）；wave2-s15-tool-unknown-green.log（原 runtime 未知结果）；wave2-s15-tool-caller-green.log（真实 daemon 工具发现→执行→结果回填）。最终扩展十二项合同全部通过（wave2-s15-tool-reviewed-contracts.log），132 runtime 单测在全量门禁中通过；全部九门禁和实际 TLS S3 四业务/PG 发布协调六业务回归已通过，最终结果见末尾记录。

初扩展 Message 类型推断、测试 Tool trait 方法签名等编译错误和误认为拒绝后 artifact 目录应已创建的夹具失败全部保留；不计功能 RED。Scoped File 拒绝先于 lazy artifact 目录创建，合法拒绝时目录不存在。未增加栈或超时、未跳过原合同。

限制：原工具 terminal receipt 与后续 ToolFinished 投影仍有分离事务，未声称全部业务事实/outbox 原子性闭合。其余 reject batch/plan/context/assistant staging、Hook/MCP/resource/delegation/管理/recovery 端口，全部 native PostgreSQL facts/租户唯一键/native PG writer/audit/quota/离线迁移仍欠。旧二进制无本切片固定 owner 和 unknown 停止语义，不能把 schema 相同当生产回滚安全。未提交、推送或部署。

S15 implemented / locally verified：wave2-s15-tool-audited-final 九项全部exit0，540 Rust / 36 Web / 48真实daemon，39个test executables、10外部环境项明确ignored；12项工具存储合同、83 storage单测、132 runtime单测通过。真实TLS MinIO四业务与PostgreSQL发布协调六业务回归全部exit0。原工具六async端口/固定owner/原File两阶段receipt事务及未知工具停止后续wave已接实际前台caller，无新表/状态机/第三方依赖，schema25。原功能RED、编译/夹具失败证据保留，不计虚假RED；未放宽超时/栈/架构。Cron兼容和其余业务ports/fullPG/租户键/nativePGwriter/完整outbox/auditquota/offline仍欠；完整Wave2–10未完成，enterprise_ready=false。详见 docs/enterprise/wave2-s15-tool-report.md。
