# ADR 0005：Local SQLite 与 Enterprise PostgreSQL 的同合同事实源

日期 2026-10-07。状态 **partial implemented**；Wave2-S1 已实现 SQLite 事务 outbox 与 PG 发布协调适配器，S4 已接线 File/S3 ArtifactStore、原收据两阶段发布；S5 SQLite native writer lease 已接线，门禁执行中；全部业务 PG/PG writer lease/离线迁移尚未完成。基线 schema v20，48 表；[逐表证据](../enterprise/schema-v20.md)。

## 决策

Local 继续 SQLite/UDS，Enterprise 使用 PG16+ 和 ArtifactStore。一个部署模式只有一个 canonical DB，禁止长期双写；cache/broadcast/LISTEN/NOTIFY/Redis/CRD 不是业务事实源。先梳理已接线 SessionRepository 和细分 ports，异步 PG 需要新 async transaction ports 时，先以实际调用者迁移相同领域 command，再实现两个 backend，不能让 runtime 随处 spawn_blocking 或复制第二套 admission/settlement。

选型限开源 sqlx 或 tokio-postgres（实施时核对版本/MSRV/license 并固定 lock）。业务 state 与 error 在 core/ports，SQL backend 返回相同 typed result。不在 sqlite implementation 导入 PG implementation，反向也禁止；ports 依赖 core，Gateway 依赖 client，不依赖 backend。

PG schema 覆盖全部 v20 canonical facts，管理数据/tenant/project/agent 域、writer lease、outbox、audit/quota 再扩展。全域唯一键/FK/索引显式 scope，不通过全局 ID 然后在内存授权。运行中的 schema/capability negotiation 要声明版本窗口，新旧 binary 不使用不兼容 schema。

## Writer lease 与 transaction outbox

单 session lifetime 一个 owner_id/lease_epoch/expires_at；lease 使用 DB 时间与 epoch CAS。Acquire/renew/takeover 在事务锁定 session lease，mutation 同事务核对 scope/owner/epoch/expiry/revision；执行体不能持跨网络 DB transaction。旧 worker 回包即使网络连接有效也不得写；heartbeat 只标 suspect。

锁顺序为 scope/session row→lease→run/interaction/receipt→outbox/audit/ledger；跨 session 操作用稳定 ID 排序，不依赖偶然 caller 次序。使用两真实 PG 连接 barrier 模拟竞争和旧 epoch 回包，不能 sleep 猜时序。

业务事实和 event/outbox 同事务；publisher claim 分页发布，数据库 cursor 可恢复。发布到外部后、标记已发前 crash 会造成重复，因此传输保证是 **至少一次**，event_id 和消费者幂等达到一次业务效果，不虚称端到端 exactly-once。NOTIFY 是有界唤醒优化，漏信号后扫描补齐。

## Artifact 两阶段发布

ArtifactStore 真正调用者负责 stage 临时对象、计算 SHA-256/size→数据库验证 owner/capability/预算→引用发布事务→finalize/清理可恢复 receipt。文件/S3-compatible 双实现复用同合同，不绑定私有云 SDK。发布引用必须绑定 immutable object identity/digest；只按路径或裸 bucket URL 读取不合法。

DB/object 不一致时 typed storage/unknown/repair evidence，不能伪装为空文件。临时清理和垃圾收集受 retention/legal hold 约束；Gateway 验证 tenant capability 后短期 URL 或鉴权流式下载。不持 DB transaction 等 S3 I/O。

## 离线迁移与回滚

预检（schema/version/tenant map/unknown/open tools/resources）→停准入和受管任务收敛→SQLite 只读备份/校验→dry-run 逐表清单与摘要→复制到隔离目标 schema→count/digest/owner/event/artifact reconciliation→发布切换 marker→新模式启动。cron/config 管理事实也必须明确迁移，不能遗漏现有 JSON owner或两个 scheduler 并存。

失败时不写 marker、不启用 Enterprise；目标 staging 可废弃，Local 仍读原备份/原库。切换后回滚需离线一致性导出/备份恢复，不能启动旧 binary 继续写原 SQLite 造成分叉。schema 只前进，expand→migrate→contract 每个窗口有部署记录。

## 证据要求

同一 repository contract suite 对 file/memory SQLite 和真实 PostgreSQL 跑；两 daemon lease 竞争、旧 epoch terminal 零写、commit 前后 crash、outbox duplicate、DB断连、artifact digest/reference 故障。已有“both_backends”仅两种 SQLite；发布协调 PG lane 已在真实 PostgreSQL 18.6 验证，完整业务 PG/HA/ArtifactStore lane 仍 **未验证**，不得以 mock 代替。首次端口提取也必须真实调用和保持 Local 门禁。

## Wave2-S1 已接线部分

[实施报告](../enterprise/wave2-s1-report.md)与[版本/锁序说明](../enterprise/schema-v22.md)。采用 tokio-postgres =0.7.18；SQLite 原 view 事务追加 event_outbox；纯 core publisher lease 状态转移由两个后端共用。发布 epoch 只 fence claim/delivery/ack，不能 fence runtime terminal。PG adapter 固定 Unix socket、隔离 schema 和有界 query；受管连接 driver 正常交接必须 close().await。durable inbox 是本地收据，不是外部 bus 的验收。


## Wave2-S2/S3 File 已接线部分

[路径报告](../enterprise/wave2-s2-file-report.md)、[发布事务报告](../enterprise/wave2-s3-file-report.md)、[API](../enterprise/artifact-api.md)。schema 保持 v22；原 tool_executions 是引用与工具终态唯一 owner。File stage 不持 DB 写事务；finalize/校验/原 receipt 提交在同一 BEGIN IMMEDIATE 事务。GC 同样取得 DB 写事务，然后获取目录能力，不反向持目录锁等待 DB。文件 root 绑定 mutex 只用于身份检查，不跨 DB lock 等待。

相同 run/round/call 与 receipt payload 幂等，其他 payload Conflict。失败后原工具仍 running/unknown，禁止仅因对象存在伪造成功或自动重放工具。进程 kill 之后 WAL 回滚，未引用对象可清理；private temp 崩溃残留至少一天后有界清理。没有增加另一个 artifact/run terminal 数据库。File 事务含受限本地 fsync，不能将 S3 网络调用搬进这个事务；S3 adapter 和跨网络 durable publication protocol 尚未实现。

旧 Local 大输出 receipt.output 保存纯预览，完整引用保留 artifact_ref；对模型返回仍含引用提示。所有外部 artifact 投影返回类型化、脱敏错误。正式对象保持原 SHA 命名；坏目录/链接/摘要现在 fail closed。新旧代码共用 v22，但旧代码仍有路径风险，不建议以恢复不安全行为作为修复。

## Wave2-S5 native writer

[原生 writer 报告](../enterprise/wave2-s5-writer-report.md)与[schema v24](../enterprise/schema-v24.md)。这是原 session 的 native writer，与 publisher delivery 类型和表分开，二者复用纯 epoch/CAS 算法。Local 默认无租约时兼容；显式开启后原 mutation 和 recovery 都核验 DB 时钟与本连接 proof。旧进程过期不自动 acquire。第二活 daemon 无法取得全部恢复保护时整笔回滚。SQLite 恢复目前按工作区、1024 head 上限，不能称 PG 分布式 HA；PG native port 尚未实现。

启动与 listener 各有至多一个受管续租批次，storage 阻塞工作最多四个、过载立即拒绝。停准入/join/原 unknown 结算/后台 outbox 收敛之后释放，强杀等待 DB 到期。ready JSON 原子发布，缺 marker 时有限等待，不凭 socket 已绑定就判旧 binary。原 run/tool 等 terminal owner 没有迁移到 lease 表。

## Wave2-S6 后台 canonical caller

Cron runner 在 daemon 组合中注入原 ControlRepository 与同一个 ArtifactService，复用物理连接/租约 proof/受管续租，不能另开未配置 RunStore。独立历史使用新 native session，纳秒 occurrence + 进程内序号避免秒级碰撞；创建仍由原 session transaction 裁决。对象后端在原 run snapshot 冻结。standalone Local 的无注入构造保持兼容，已有 native lease 时仍 fail closed。

Cron 文本返回不证明 native Completed；原 terminal owner 结算 unknown 时返回 core RunOutcomeUnknown。CronManager 按类型停止 unknown/授权/租约/存储/artifact 故障的自动重试，不解析错误字符串。shutdown 必须先 join 后台 Cron 执行体，再做原全局 recovery，最后释放 writer。JSON 作业配置的完整企业迁移/管理身份/后台独立授权仍待后续，不能把本 caller 接线当成企业工作流。


S6 审阅补充：后台每次准入前从原 ProviderManager 冻结 route/配置代，Context、LoopEngine 和原 run snapshot 使用相同候选；Provider attempt 由原 repository 记录，不新增 Cron attempt 表。新 AttemptStatus::OutcomeUnknown 在原 finish/recover 事务封闭未确认 Started；不能凭成功文本结算未终止 Provider。失败注入必须回滚 attempt/run/turn/event/clock；晚成功不能覆盖已未知收据。禁用 Cron future schedule 不终止正在运行的原执行体，daemon 的活跃判断必须包括原 running 集合。schema DDL 保持 v24，但旧 binary 不能解析新增 provider enum，因此回退只允许停机一致备份或支持该枚举的 binary；不能原地退回旧 v24 binary。


S7 预审决策：已存在的三组异步端口必须在SQLite adapter统一执行有界阻塞工作，不能要求每个daemon caller自行包spawn_blocking。共享四个permit，满额即 typed unavailable，不排无界等待；已经开始的工作在异步caller取消后仍持permit至SQL提交/回滚。PublicationRepository保留原SQL/fence/DB时钟和原事务事件，不改PG的native async实现；daemon单批publisher使用原JoinSet async任务，不再runtime.block_on。先以current_thread+真实SQLite写锁证明旧异步端口阻塞定时器的RED；统一adapter需取消/过载、SQLite/PG同合同和真实daemon、对象回归证据。此决策不是全业务async ports/nativePG完成声明。

## Wave2-S8：canonical scoped readback 与永久 lifetime owner

新增 core-only SessionReadRepository，daemon 原 readback/list caller await，SQLite 共用有界 adapter。scope 先在同一读事务 SQL 内约束，不从请求 DTO 接受域覆盖；page scope 在 limit/cursor 前过滤。列表的 preview/active/status 单事务形成纯 DTO，daemon 不再按多次 SQL 拼投影。原同步接口保留给未迁移 Local caller，完整 async/PG 合同仍待后续。

原 schema 已有当前 lifetime UNIQUE；v25 进一步永久绑定退休 lifetime。迁移只从 heads/runs/原 lifecycle receipt 取可证明归属，无 owner 的旧 transcript/plan 只隔离不猜测。原 run incarnation 不能事后改 key/lifetime。迁移冲突和故障全回滚，旧 binary 不能写升级库，回滚只能停机恢复已校验备份。详见 schema-v25 和 S8 报告。

## S9 预审：中心授权读回异步化

中心 request guard 仍同步调用 IdentityRepository，每次请求都可能在 Tokio 线程阻塞等原 Connection。下一个切片将身份/冻结 run 授权读回提到 core-only async port，SQLite仍调用原单事务 SQL，经同一个四 permit adapter执行；所有中心授权和 identity/run.authorization/slash caller真实 await。准入事务内的 verifier、授权冻结和 late mutation fence继续保留，不能把异步前置检查作为原事务最终授权。schema保持v25。

端口错误必须在core表达授权错误与协调/存储错误，不能带rusqlite/SQL/路径；daemon旧RuntimeError过渡转换要保留错误类别，不能把预算/连接失败转成Forbidden或Internal。取消、满额、重启与撤销/策略并发需要合同；current_thread下原Connection受控占有的真实RED证明同步中心查询不能调度定时器。该决定仍不是全部native async ports或全业务PG完成。

S9实施锁序：涉及daemon控制屏障的原caller可持control_lock，再取得SQLite四permit、Connection mutex和事务；adapter闭包只持原RunStore，不获取daemon/control_lock/session writer异步锁，所以没有DB→daemon锁的反向等待。取消后旧身份查询仍持permit至实际结束；已经开始的只读工作不会因等待者消失产生新业务事实。原授权读回现在用完整scope SQL JOIN复核原exact owner，StaleOwner保留typed分类，native proof和原immutable授权snapshot继续是来源。

## Wave2-S10：canonical 事件隔离与有界恢复

事件事实仍由原 events/insert_event/views.commit/outbox transaction 拥有；纯 RunEventPage 与 RunEventRepository 仅投影。同一只读 TX 先在 SQL 内约束 organization/tenant/project/agent + run/session，再核永久 lifetime owner，读取原 payload 与已封存 stamp。原同步事务/维护调用者与 async adapter 共用唯一 events_in/decode_event_in，保留原 EventSeq、payload 与 transport metadata 校验。

在线页为 1–1000 行，encoded events 预算 1MiB 减 1KiB envelope 空间。SQLite CASE 长度预检拒绝超大 raw payload，逐条序列化计费，首条无法传输则 typed InvalidRequest；页尾未返回事件不能推进 cursor。预算耗尽或实际 DB 故障为 StorageUnavailable，不显示 SQL。退休历史按永久归属读取，不取得 writer proof，也不要求当前 session head 仍指向旧 lifetime；所有 mutation 的原 native fence 不变。

锁序为四 permit → 原 Connection → 只读事务；闭包不获取 daemon 锁，不写 clock/outbox/lease/授权。daemon run.events 和 agent.subscribe 的历史部分 await 同一 port，六入口都调用这些 canonical handler。未完整交付历史时返回原 resync_required，cursor 指向已经交付的位置，而非尚未发送的页末；不能在缺历史时宣告恢复。其余 run inspection/mutation 等旧同步端口仍待后续迁移，不能标全部 async/PG 完成。

## Wave2-S11：原生命周期事务的 async port

SessionLifecycleRepository 的 commit_session_command/read_close_receipt 已接 daemon create/close/end/fork 原 caller。SQLite 原 execute_lifecycle SQL 提取至唯一 lifecycle_scoped，BEGIN IMMEDIATE 先串行取得写事务，完整 scope 判定先于 operation receipt。原 command/expected payload、v6–v13兼容 receipt、原 Hook/lifecycle state和 writer fence继续复用，不新增第二账本。当前新资源仅支持 Local，不能用端口接线冒充全租户键或管理授权闭合。

receipt replay 与 close reader 共用 decoder 校验 command 输出 key及永久 lifetime owner；历史结果不要求与当前 head相等。writer expired后的不可变receipt读取不重取epoch；新mutation仍由原事务拒绝旧proof。driver故障/预算满返回typedUnavailable，业务Conflict保持原RPC -32001，其它typed core错误不降成Conflict，错误不携SQL/路径。

锁序为原 control_lock（适用caller）→四 permit→原 Connection→原事务；blocking闭包不回调daemon锁。取消等待者不撤销已经开始的DB工作，permit至实际提交/回滚才释放；若已commit而response丢失，按原operation读回，不能自动重放未知外部副作用。受控barrier实际commit后取消、双独立Connection并发同operation、receipt故障全rollback、scope不可见与真实daemon重启合同提供证据。

原 session_end 事务前同步metadata查询已删除，由原事务统一scope校验，避免“资源不存在”和“无权访问”不同错误。仍未迁移 set_preferred_session及其它native mutation端口；最终policy/epoch verifier、Admin audience和后台独立主体仍待完整企业工作。schema保持v25，回滚按v25/OutcomeUnknown兼容窗口。

## Wave2-S12：原 run 事实的一笔异步读回

RunFactRepository 的三项纯core DTO读回已经接 run.read/run.tools/run.provider_attempts 和原 /run caller。SQLite 共用 check_run_scope_in，organization/tenant/project/agent在读取任何 payload 前于SQL判定，核验永久lifetime owner；每次只读事务内读取原 record、冻结snapshot、stamp、compact linkage及Hook continuation。旧维护同步方法与新端口共用原Connection helpers，避免多次查询拼出不同事务的投影，不写clock/outbox/lease、不重取writer或执行Provider。

关联的continuation不能跨scope、缺失或自环，发现损坏返回Corrupt；退休run的原永久life仍可读。SQL先检查BLOB长度/最多1000行，再解码和校验序列化总预算1MiB减1KiB，超限InvalidRequest，不静默丢弃记录。Provider decoder验证SQL/JSON attempt_id/run_id/round/candidate/status一致；新unknown写入serde拼写，明确旧Debug拼写只兼容读取、不重写历史。

原 /run 默认人类摘要不变，compact默认共用原纯compact_projection；显式fact/tools/provider投影和CLI run-read、HTTP三个严格readback endpoint、ACP2 readback协商、ACP1及WS/RPC均调用同daemon handler。未知tenant等字段拒绝，不赋权。首次接线重复原/run及遗漏ACP2字段白名单已修，失败证据保留；六入口真实业务/故障/范围/重启合同通过。schema25不变；全部mutation/nativePG与audit/quota/offline仍需完整实施。

## Wave2-S13：原运行执行事务

RunExecutionRepository 复用原准入、队列启动/取消和 TurnCommit SQL/状态机；原同步接口薄委托同事务体，新异步事务先取数据库写锁，准入 verifier/服务器时间在锁内复核，提交前返回 ExactOwner，foreground、工具审计、Hook、终态贯穿同一份 owner。旧同步准入保留 deferred 行为及原并发策略发布测试。scope 查实际 run 完整域，owner/head/永久 lifetime、原 writer proof/冻结授权均在 mutation TX；queue 的 false 返回也不能先于 scope 检查。

三项有效 RED 为 legacy 无授权 run 越域完成、重读 later generation 后完成、真实 Connection 锁阻塞 current_thread 715.9ms；已有授权 run 的原 snapshot fence 保持。八项 execution/storage82/真实 foreground 合同通过，最终九门禁及 TLS S3/PG 回归待收口。schema25；完整 PG、后台主体和其余同步 ports 未闭合。

最终本地验收：wave2-s13-execution-stack-final 九项全部exit0，511Rust/36Web/45真实daemon，外部10项明确ignored。执行合同9/storage82/runtime131均通过，原ACP标准与前台generation故障合同保持，未加栈或跳过测试。真实TLS MinIO四项与PostgreSQL发布协调六项独立回归全部exit0，见 wave2-s13-execution-regression-s3.json / postgres.json。原失败门禁和夹具失败均保留；未CI/生产验证、未提交。全企业Wave2尚未完成，Wave3–10 pending，enterprise_ready=false。继续原Provider attempt/未知结果和实际runtime caller，不能称所有ports或全部PG事实已迁移。

S14 implemented / locally verified：wave2-s14-attempt-audited-final 九项全部exit0，526Rust/36Web/48真实daemon、10外部环境项明确ignored。12项attempt存储合同/131 runtime单测/3真实HTTP接受后断连取消超时全部GREEN。独立TLS MinIO四项业务与PostgreSQL发布协调六项全部exit0；PG runner五步骤不等于五业务合同。首次依赖清单/Clippy遗漏及两项旧取消断言失败保留，未放宽架构护栏、未加栈/跳过测试。schema25，不新增第三方包，未提交推送部署；完整Wave2–10未完成，enterprise_ready=false。继续原工具事务async/固定owner，后续完整PG/租户键/nativewriter/auditquota/offline仍必须闭合。


S15 implemented / locally verified：wave2-s15-tool-audited-final 九项全部exit0，540 Rust / 36 Web / 48真实daemon，39个test executables、10外部环境项明确ignored；12项工具存储合同、83 storage单测、132 runtime单测通过。真实TLS MinIO四业务与PostgreSQL发布协调六业务回归全部exit0。原工具六async端口/固定owner/原File两阶段receipt事务及未知工具停止后续wave已接实际前台caller，无新表/状态机/第三方依赖，schema25。原功能RED、编译/夹具失败证据保留，不计虚假RED；未放宽超时/栈/架构。Cron兼容和其余业务ports/fullPG/租户键/nativePGwriter/完整outbox/auditquota/offline仍欠；完整Wave2–10未完成，enterprise_ready=false。详见 docs/enterprise/wave2-s15-tool-report.md。


S16 implemented / locally verified：wave2-s16-turn-audited-final 九项全部exit0，556 Rust / 36 Web / 48真实daemon，40个test executables、10外部环境项明确ignored；16项turn/transcript/计划integration合同、83 storage/132 runtime单测通过。独立真实TLS MinIO四业务和PostgreSQL发布协调六业务全部exit0。原六async端口接实际前台/PlanStore/daemon start-plan，同一Connection/四permit/固定准入envelope；六越域RED、已结算无决策仍给执行许可RED、receipt错误run_id仍执行RED修复。JSON四维和SQL四维身份GREEN，不把第一个截停RED夸成多项RED；故障回滚/CAS/幂等/撤销/过期和真实daemon原计划六入口/恢复合同保持，无新表/第三方包、schema25。编译/夹具失败及测试无效import警告保留，不计功能RED；未放宽超时/栈/护栏。全业务ports/nativePG/租户键/nativePGwriter/outbox/auditquota/offline仍欠，完整Wave2–10未完成，enterprise_ready=false。详见 docs/enterprise/wave2-s16-turn-report.md。


S17–S19 implemented / locally verified：wave2-s17-s19-reviewed-final 九项全部exit0，588 Rust / 36 Web / 48真实daemon，42个test executables、10外部环境项明确ignored；runtime136/storage83、request15/event12/workload7合同全部通过。独立真实TLS MinIO四组业务与PostgreSQL发布协调六项全部exit0（wave2-s17-s19-reviewed-regression-s3.json / postgres.json，PG runner五步骤不等于五合同）。S17三请求端口实际模型/摘要/读回await；S18原events不可变来源及native缺stampCorrupt；S19原同TX后台准入、固定owner及Local独立workload。schema27，不新增第三方包。首轮Clippy常量放testmodule后失败、foreign夹具writerFK失败及0tests过滤错误均保留，不计功能RED；移动常量后完整九项重跑，未放宽lint/超时/栈/架构。全企业Wave2、Wave3–10仍未完成，enterprise_ready=false，未提交推送部署。继续S20 compact实际RED与原唯一helper迁移。

S20/S21 implemented，完整本地验证进行中：原上下文/compact八async端口与exactOwner端口、即时事件投影async端口已接真实caller，scope/fixedOwner/CAS/原stamp/预算在唯一原事务复核。context16/event17局部通过，runtime136/storage83及五项真实daemon compact通过；新增child SQL预算和锁合同在统一门禁中验证。schema27，无新第三方依赖，未提交推送部署；全业务async/nativePG/租户键/PGwriter/auditquota/offline与完整Wave2–10未完成。详见wave2-s20-context-report.md及wave2-s21-event-projection-report.md。

2026-10-09 S20/S21 implemented / locally verified：wave2-s20-s21-reviewed-final 九项全部exit0，614 Rust /36 Web /48实测daemon，43测试执行体、10外部环境项明确ignored；runtime136/storage84/context19/event18全部通过。独立真实TLS S3四组业务及PostgreSQL发布协调六项全部exit0（wave2-s20-s21-reviewed-regression-s3.json / postgres.json），自己的MinIO容器与PG隔离DB已清理。第10项有效compact来源身份RED已修，同原事务核JSON lifetime/起点/结果generation，正常revision和摘要失败返回Conflict，损坏Corrupt。首次九门禁tests101/ACP初始化EOF保留；独立与原并发daemon复核、最终完整门禁均未复现，根因未定位，64KiB有界诊断并持续drain保留，未修改超时/栈/并行度。schema27，无新第三方包。全企业Wave2、Wave3–10仍未完成，enterprise_ready=false；未提交推送部署。继续S22实际Live事件mutation，再S23发现端口，统一本批验收。

2026-10-09 S22/S23 已实现并完成本地验收：wave2-s22-s23-reviewed-corrected-final九项全部exit0，636 Rust/36 Web/48实测daemon，44执行体、10外部ignored；runtime137/storage86/context19/discovery14/event23全部通过。真实TLS S3四组业务及PostgreSQL发布六项全部exit0（wave2-s22-s23-reviewed-regression-s3.json / postgres.json）；自己的MinIO容器与PG独立DB已清理。原实时事件caller固定准入env、发现四端口及取消一次捕获owner已接actualawait；原事实/SQL/同TX不复制。首次九门禁Clippy5处Copy.clone失败及发现夹具错误完整留，不计业务RED，不放宽lint/断言/超时/栈/并行度。schema27，无新第三方包，全Wave2/Wave3–10仍未完成，enterprise_ready=false；未提交推送部署。后续S24发布完整key先SQL/列绑定与预算实际合同，预审与草稿在/tmp，仅未执行时不计RED。

S24 原发布两后端的完整DeliveryLeaseKey SQL谓词、SQL列与封存JSON绑定及保存字段预算已实现并本地验收。最终九门禁640 Rust/36 Web/48实测daemon、真实TLS S3四组业务与PG发布10合同全通过；SQLite发布12项。schema27/PG发布schema1，无新依赖。此结果只证明发布协调，不能代替全业务native PG/fence/租户唯一键/离线迁移；完整Wave2未闭合。详见wave2-s24-publication-report.md。
