# ADR 0003：版本化计划与受治理运行控制

日期：2026-10-05。状态：接受设计；实际完成范围见 [实施记录](../changes/governed-runtime-controls.md)。

## 所有权和依赖

继续沿用 ADR 0001/0002。core 定义计划、hook、compact、discovery、上下文和 reducer 的领域身份；protocol 提供严格 DTO；storage 的单个 SQLite repository 持有全部 canonical facts；daemon 持有唯一 command owner 和实际 writer permit。入口仅通过 client 读取/决策/显示，不读取 plan.json，不计算执行摘要。

## Wave 1 决策

计划定义（标题、目标、成功标准、约束、验证方式、有序步骤 ID/描述）以固定字段顺序的 UTF-8 JSON 计算 SHA-256。进度和 review 不在定义摘要内。plan_id 在第一次实际 authoring 时由持久 lifetime 和 run 身份分配；旧导入数据保持只读，无执行身份；update/add 不能把历史进度变成执行权，显式 set 新定义时在 plan_legacy_evidence 保留原数据。定义变化增加 revision，进度变化保留 revision。

plan stage 继续随成功 TurnCommit 原子发布。版本历史和同源 Markdown 在同一 SQLite 事务中保存，artifact 不使用入口或本机文件作为事实源；默认 readback 无绝对路径。写入 artifact/history 或 terminal 任一失败全部回滚，不发布新计划。

execute 使用 chat.send 的类型化 plan_execution，绑定 session、expected_lifetime、plan_id、revision、digest 和 operation_id；discard 使用 sessions.plan.discard。两者共用 repository 决策校验和 receipt。相同 operation_id 不同身份/输入或请求 sandbox/context_read_only/准入模式冲突。首次 execute 准入必须 reject_if_busy，事务登记 pending_execution 和 run linkage；进入 runtime 的 writer 临界区后以 ExactOwner 推进 executing。重启不自动调度 plan execution queued run；继续必须再次传同一身份和操作。已经不确定的运行保持 unknown，计划 blocked，不能隐式重放。

discard 保留历史和回执，标记 rejected。自己的 pending_execution 若仍为精确 queued run，可在同事务写 cancelled terminal 并废弃；其他 busy 状态仍拒绝，回滚全部变更。被废弃的同一定义不能靠 set 重置 review；实际定义修改才产生待审新版本。错误 digest/lifetime、损坏文档、忙会话均 fail closed。读回使用单个只读 repository transaction，绝不启动 Provider/MCP/runtime。

## Wave 2 决策

core 定义严格的 16 类事件、有限效果和 typed failure；单一 daemon dispatcher 先做廉价配置检查，未配置不构建 payload/runtime。只从显式 MY_AGENT_HOOKS_CONFIG 读取受信私有配置，集中权限、no-follow、大小和执行预算治理，清空子进程环境。SQLite v16 保存 outcome claim/结算、lifecycle publication outbox 与 continuation linkage；claim 即代表脚本可能已启动，崩溃后 unknown 不重执行。观察失败 fail-open，控制失败 fail-closed。SessionStart/End 在生命周期发布同事务生成，关闭重试不重复；启动时分批排空尚未 claim 的真实 publication。

Stop 成功指令只在父 completed 后以 exact owner 和同一 frozen snapshot 发布一个新 run/turn；run gate、工具、sandbox 和审批复用原链路。独立预算限制为一次后续执行、8 轮、8192 累计输入/输出 tokens、8 次工具调用和 30 秒，冻结 context policy 不为预算而改写。请求前和流式转发前计量，最终文本也受限；每轮一次 attempt，不作额外 retry/auto compact。取消只作用于 exact child，原 terminal 不改变；queued 可按原队列恢复，running 崩溃后 unknown 不重放。审批 hooks 查询已发布 interaction，Pre/PostCompact 围绕真实 intent/结算，不以 broker/elapsed 推断结果。详见 [Hook API](../hooks-api.md)。

## Wave 3 决策与已验证实现

以同一 daemon compact command owner 提供 additive `compact.start`，强制 session/lifetime/source revision/projection generation/operation 身份；旧 sessions.compact unary 仅作兼容等待适配，旧 owner_run_id 用于确认来源，实际新执行必须分配独立 native run/turn。既有 legacy receipt 保持只读，不补 native 身份或重执行。

SQLite v17 的 compact run linkage 绑定 request/source 和独立 run kind，复用原 admission/writer/busy/backpressure、ExactOwner、compact intent/receipt 和 TurnCommit。准入同事务分配 run/turn、持久 intent/source、Started，禁止向 transcript 注入伪造 compact 用户消息。摘要在 daemon owned 后台任务运行；入口断开只 detach。安装 projection 或 no_gain/rejected/failed/cancelled 的 receipt 与 native terminal 同事务发布，避免已安装摘要却丢失业务终态；run.events/readback 是恢复入口。崩溃中尚未结算的 Started run 按 unknown 收敛，不重放摘要。

精确 cancel 使用 native compact run 身份，既有取消路由复用；旧聊天 owner 不可替代该身份。source CAS、并发 busy、同 operation 幂等冲突都在 repository 校验，writer 只决定执行顺序。CLI/TUI/WS/HTTP 共用同一 command 与 durable events，ACP v1 可只读 native /run，ACP v2 mutation 投影在 Wave 7 接入。旧 unary 来源只读标记仍拒绝；通用聊天 finish/reconcile 禁止改写 compact，compact turn 排除聊天记忆摄入。正常 channel 关闭继续排空 response，只有真实 overflow 才断开。2026-10-06 本波门禁通过，见实施记录。

## 后续波的约束（Wave 5–8 尚未完成）

hooks 的 inventory、配置治理和 effects 必须集中；观察失败为 typed outcome，控制失败 fail closed；Stop 只能通过新的 durable run 身份续跑。独立 compact run 与 chat 共享 writer 准入，先 durable Started 再后台摘要，exact cancel 和 receipt 不得借旧聊天身份。tool_search 使用冻结授权 catalog 和 generation，nested dispatcher 仍走原 schema/safety/approval。reducer 统一比较 lifetime/revision/seq，不由 broadcast 决定终态。只读请求重建与实际请求共享 assembler，动态来源明确 non-replayable。ACP v2 能力绑定连接，后台 prompt 不阻塞控制输入。行为闭合后才拆 crate，依赖不反转。

## 迁移、兼容和回滚

SQLite v14→v15 前沿用可校验备份与 marker；增加计划版本历史和决策回执表及 snapshot triggers，不改写旧计划来赋予执行权。严格 additive DTO，旧 chat 不带 plan_execution 时保留行为，RPC aliases 只指向同一 handler。未来 schema 拒绝。迁移失败保留旧事务事实和升级前备份；不自动降级 schema，不双写 JSONL。回滚应用版本时必须使用已校验旧库备份，不能让旧二进制写新 schema。

## 时序

定义 authoring → stage CAS → TurnCommit（canonical 文档、Markdown/history、terminal 同事务）→ readback → 明确 execute（receipt + pending_execution + queued run 同事务）→ daemon writer permit → executing → 原有运行/终态提交。断线仅 detach；重启跳过待明确继续的执行，不以时间/ACK 推断批准或完成。


## Wave 4 决策与已验证实现（2026-10-06）

继续冻结授权后的完整有序 ToolSpec catalog，catalog_generation 取完整快照摘要；缓存只保存不可变索引，不保存权限。Provider 常驻 read_file/write_file/edit_file/exec/plan/sub_agent 和原 spawn_subagent 兼容核心工具；有延迟工具时提供 tool_search，其他内置、MCP 和子 Agent 管理工具延迟发现。冻结 registry 保存原 descriptor schema；重新加载不扩大本 run 集合，重启 queued 恢复需再次冻结并与原 snapshot 精确一致。

tool_search 搜索参数为严格 {query,limit?}，nested invoke 为严格 {name,arguments}；两者仍以顶层 tool_search 回填，保持 wire call身份。名称必须 exact，隐藏工具直接调用拒绝。nested descriptor/schema/preflight/effect/resource conflict、安全与审批均委托原工具，不通过模糊搜索触发副作用。搜索字段加权覆盖名称/命名空间/关键词/description/schema，支持多种英文分词与中文单/双字；exact/select/list 有界，平分按 canonical name。

SQLite v18 discovery receipt 绑定 ExactOwner、原冻结 generation、查询 operation 和完整结果；只已持久发现的同 run 工具允许 nested invoke。CPU 搜索在 blocking pool，取消后不能发表结果；daemon 取消请求先持久 cancellation_requested 观察事实再发 token，repository 同事务 publication 检查它，既不伪造终态也不释放 gate。索引缓存有界且由完整快照键控，schema/描述/授权集合变化必失效。结果有数量/字节预算，损坏或身份缺失拒绝；只读 run.discovery 不启动工具/Provider。

2026-10-06 本波实现和全部本地门禁已通过（Rust350、Web31）；操作绑定run/round/wire id，回执摘要与事件原子发布，核心只读子目录保持原工具面。API与失败边界见 [工具发现 API](../tool-discovery-api.md) 和实施记录。Wave5–8尚未完成。


## Wave 5 准备实施决策（2026-10-06）

core共享ViewReducer只持展示游标，不成为canonical事实源。输入为repository发布的readback/durable/live版本标记，比较session/lifetime、全局snapshot、metadata/transcript/projection、exact run generation/event seq和interaction revision；更换lifetime只能由更晚的单事务readback建立基线，旧incarnation不得重新进入。无可证明版本或序列gap要求resync，不能据此终结业务。

SQLite v19保存事件发布时的版本标记，不在重放时把历史事件贴成当前snapshot/lifetime；兼容历史无标记记录要求durable基线。daemon-client复用core reducer过滤入口live/readback，Web通过纯读取views.reduce RPC调用同一个core函数，并串行应用展示决策；浏览器不维护第二套时序规则。RPC输入/输出只影响展示，不能授权或改写业务。入口丢失、重复、旧响应不改terminal/gate，重连先单事务readback再durable cursor。

本节仅记录设计；Wave5尚未实现/验收，Wave6–8仍未实现。


## Wave 5 完成：共享 revision reducer 与持久版本封存（2026-10-06）

agent_core 是唯一展示时序规则；SQLite v19 event_view_stamps 是版本事实源。所有事件发布在原 mutation 事务封存最终全版本向量；读取校验 SHA/full ExactOwner，缺戳历史不追补。SessionReadback::supersedes 委托同一 reducer。run_owners additive 读取严格校验；嵌套 slash、native compact、终态和交互回执也接线。去掉 private seq 推断和入口各自比较路径。

Rust DaemonClient 各消费者独立游标，projector actor 保持 next 可取消安全；有界队列溢出 detach。Web 通过纯 views.reduce RPC 调用完全相同函数，256 帧/8MiB 有界串行队列；Gateway 只转协议。resync 先单事务 page 基线，再 32 条 durable 页补充仍活动完整 owner，ViewResynced 不是业务终态。原始审计数据保持，返回帧保留元数据剥离后按 repository 来源添加。

RED /tmp/control-waves-wave5-red.log 缺 RPC 真失败。核心 4 个领域测试覆盖乱序、同版本补充、IR 冲突、多 run、退休 life、durable replay。storage 新 3 项双后端/封存/迁移测试覆盖零写读取、损坏和 fault 回滚。真实 daemon 新 2 项 revision_reducer_rejects_delayed_readback_and_retired_lifetime_over_socket、shared_client_resyncs_real_dropped_frame_and_duplicate_without_provider_replay，后者 proxy 真丢 TextDelta/重复 Started，Provider 次数保持 1。重启 direct run.read 需要完整 source 初始化展示已修复。新增 stamp 使旧测试 helper 误判 compact 为 session，现按完整 compact DTO 逐字段比对，未放宽旧断言。Web 新 4 项连同原 31 项通过。

门禁均退出0：cargo test --locked --workspace --all-targets --all-features 359 项含24真实daemon（/tmp/control-waves-wave5-tests-final-corrected.log）；严格 Clippy（clippy-final-corrected）、MSRV1.88（msrv-final）、release（release-final）、fmt（fmt-final）、diff（diff-final）；Web35（web-final）、JS语法。cargo deny --offline check --hide-inclusion-graph 四项通过（deny-final），仅已有缓存，未在线刷新。API docs/view-reducer-api.md。

Wave6–8尚未完成；真实商业 Provider/MCP、Linux、生产慢消费者和 CI 未验证；未提交/推送/部署。


## Wave 6 设计补充（实施前）

Provider 发送前只使用 agent-context 的纯 assembler；同一输入同时产生规范化消息/工具、ContextEnvelope、分区计量、能力降级与请求摘要。SQLite v20 前进迁移保存完整 exact owner 的不可变 request capture 与 SHA；包括该次调用的 source revision/generation、冻结完整 catalog、实际小工具面、route/provider capability、校准与 policy。captured retrieved/overlay 保存为当时材料，不在只读路径重新启动 Git/MCP/Provider/skill 搜索/memory ingest。

context.readback 在同一只读事务核对 current lifetime、owner、capture SHA、source 和冻结 snapshot，再以同一个 assembler 重建。输出明确区分当时可重建的 capture 和当前不可重放的动态环境；不声称其等于后来状态。无 capture 的旧 run 报明确 unavailable，而非空上下文。默认仅元数据/摘要，显式本地诊断才返回经脱敏的完整材料；memory 分区不返回原文，当前权限变化禁止完整诊断。损坏 fail closed。


## Wave 6 完成：实际请求 capture 与零写共享重建（2026-10-06）

状态 owner 为 SQLite v20 provider_requests；保存模型/fallback/自动与手动摘要的发送前不可变材料，SHA与ContextEnvelope同事务。实际 Provider 发送与只读 context.readback 都使用 agent-context::assemble_request；删掉 loop_engine 原独立 envelope 计量与媒体降级路径。ContextManager 摘要也接入相同 assembler，避免旁路。cron 新准入保存真正冻结 RunSnapshot，旧 usage fixture 改 canonical 准入，原安全/重启/预算断言保留。完整瞬时 DTO 在 core，不建立 context 私有历史 owner；架构测试未放宽。

capture 明确是当时可重建材料，当前动态环境/skill/plan/memory不重放。默认只返回类型化 envelope/digest，显式 local 诊断保守隐藏凭据/memory/媒体/工具参数/摘要原文；权限变化或凭据不可核对拒绝。读取同一 readonly 事务核对 full owner/life、冻结政策、catalog、source prefix、projection与 SHA，无新 Provider/MCP/runtime/compact/ingest。旧历史无 capture 明确 unavailable，不补身份/空请求。CLI/TUI/ACP v1/WebSocket/HTTP共用 handler；Web 只读不造 chat/run 终态。API docs/provider-context-api.md。

真实 RED /tmp/control-waves-wave6-red.log 缺 context.readback 方法。新纯 assembler 2 项、storage 2 项双后端/迁移/fault/损坏、协议 strict DTO 1项、真实daemon 1项和Web1项；既有compact合同补实际摘要capture与下一chat generation。真实wire消息/小工具SHA与返回一致，三入口及HTTP逐字段相同，读回前后clock/event不变，kill/restart调用次数仍1，权限变化/损坏拒绝。测试 helper 识别新增 DTO，保持完整 JSON 精确比对；child16K预算与route32K取冻结有效最小值，原断言保留。中间失败日志保留，未通过降低测试换绿。

最终门禁全部退出0：Rust365（含25真实daemon）/tmp/control-waves-wave6-tests-final-complete.log；严格Clippy、MSRV1.88、release分别 clippy/msrv/release-final-complete.log；fmt/diff；Web36 web-final-complete.log、JS语法。cargo deny --offline check 四项通过 deny-final.log，仅已有缓存，未刷新在线公告。无新外部依赖，Cargo.lock未修改。Wave7–8未完成；商业Provider、真实MCP/用户hooks、Docker/Keychain、Linux与CI未验证；未提交/推送/部署。


## Wave 7 设计补充（实施前）

ACP 默认仍运行 SDK 稳定 v1；editor --acp-v2 显式选择 SDK Agent.v2()/V2ConnectionTo，并仅接受物理连接初始化协商出的 v2。连接保存的只有不可变能力/版本指纹，不保存 plan/session/compact 第二事实。SDK v2 当前没有标准 execute/discard/compact-start 请求，使用命名空间扩展方法，通知优先标准 PlanMarkdown、CompactionUpdate、StateUpdate、RequestPermission。扩展 metadata 严格 schema/fingerprint/完整生命周期及 exact owner；未协商、重复初始化、未知身份字段在 mutation 前拒绝。

prompt 准入后及时响应 accepted，后台消费共享 DaemonClient reducer 的 canonical 事件，终态只来自 durable readback/run；长 prompt 不阻塞控制请求。resume 先单事务 sessions.read，再按 durable active owner/interaction 恢复，不切换业务 session、不重放 Provider；断线 detach。plan/compact/管理命令委托既有 daemon owner，exact cancel 不使用客户端 reservation ID。优先保持原始数据库 schema v20，无新增业务事实或持久迁移。


## Wave 7 完成：真实 opt-in ACP v2 与有界传输（2026-10-07）

稳定 v1 默认路径保留，--acp-v2 使用 SDK Agent.v2/V2ConnectionTo/标准 v2 类型；物理连接一次严格协商 schema/fingerprint/capability。未协商、未知身份、版本/重复/热变更在 mutation 前拒绝。计划 Markdown/execute/discard、compact 持久 Started/真实结算/exact cancel、pending interaction 恢复、session readback/new/list/resume/close/delete 共用 daemon 命令；SDK 缺标准请求的操作采用命名空间扩展，未伪造 v1 metadata 为 v2。

prompt 的 durable Started -> accepted response -> 后台 stream 不阻塞 inbound；标准 permission popup Cancelled 不提交决策。canonical message_ids 从真实 batch/native turn 派生，readback/page/replay 一致；foreign batch fail closed。close 使用现有 lifecycle_receipts，取消 captured exact owners 并等待真实结算后发布一次 SessionEnd，保持 lifetime/transcript；重复 receipt 不取消新工作。schema 保持 v20。

v1/v2 物理预算 256 帧/8MiB、4MiB 单帧、128 tasks；v2 请求32。真实慢 stdout 合同首次 RED 发现 Tokio 阻塞 stdin 关闭问题，改为有界独立 I/O 桥接，stderr 测试夹具单独排空；budget detach 不改 canonical，不取消 run，不重放 Provider。严格默认 UI Cancelled 保留 pending。新合同包含严格协商、三入口计划、提前 compact/长 prompt/close、interaction 断线恢复和真实慢消费者。未删除/放宽既有测试。

最终门禁全部退出0：Rust376（30真实daemon）/tmp/control-waves-wave7-tests-accepted.log；严格Clippy/MSRV1.88/release 分别 clippy/msrv/release-accepted.log；fmt/JS/diff static-accepted.log；Web36 web-accepted.log；离线 cargo deny 四项通过 deny-accepted.log，仅已有公告缓存，未在线刷新。Cargo.lock 未变，无新增外部包。API docs/acp-v2-api.md。真实 IDE/商业Provider/MCP/Docker/Keychain/Linux/CI 未验证。Wave8 尚未完成；未提交/推送/部署。


## Wave 8 设计补充（实施前，2026-10-07）

在 Wave1–7 门禁已闭合后依次迁移 sandbox -> runtime -> daemon -> entry-support -> CLI/TUI -> ACP -> Gateway/Web。每个 crate 迁移后先 workspace 编译与目标测试，再进行下一步。根 binary 只组合 host 服务、参数和启动；共享 DTO/SQLite owner 不复制。runtime 不反向依赖 daemon/入口，sandbox 接收已冻结 image/owner 和取消端口，不取 repository。生命周期启动/配置组合通过真实 host 端口注入 Gateway，入口生产依赖仅协议/client/共享展示支持，测试可使用 daemon fixture。已有 facade 在迁移期间短暂使用，完成全部删除。

保持 SQLite v20、配置/socket/binary/HTTP/WS/CLI。安全系统调用改用已锁定 rustix 的 safe fd API，保持 O_NOFOLLOW/openat/目录句柄/CAS/原子 rename 与进程组 kill；不退化成路径 reopen。根进程加载环境仅在多线程 runtime 建立前完成，库内移除 unsafe 环境写入。增加真实物理路径/manifest/AST 依赖与 unsafe 护栏，原断言保留迁移后的对应代码，禁止用 src facade 掩盖 owner。Cargo.lock 只允许新增内部 package 及已锁依赖的使用边，不刷新第三方版本。回滚为工作区代码恢复，持久 schema 不做倒退。


## Wave 8 完成与最终验收（2026-10-07）

七个新库按 sandbox→runtime→daemon→entry-support→CLI/TUI→ACP→Gateway/Web 顺序迁移，每个均先 workspace check 与目标测试。根仅 main.rs/bootstrap.rs；旧根业务模块及 client/storage/protocol facade 全部删除。唯一 owner、实际物理边与非默认 fixture features 由 12 项架构检查保护。库 forbid unsafe，文件能力使用已锁 rustix safe fd API 保留 no-follow/目录句柄/CAS/原子 rename，环境加载在根多线程 runtime 前执行。配置/binary/socket/HTTP/WS/CLI/schema v20 保持，Cargo.lock 第三方身份/checksum 与基线完全一致。

最终审计补真实 v1 permission RED：SDK 容错解码吞入 v2 owner metadata 并错误执行文件。共享 raw Value strict decoder 在审批前拒绝根/selected 的 my-agent v2 控制字段，保留标准其他 namespace；GREEN 验证 canonical pending 不变、无写文件/Provider 重放，v2 重连可精确恢复批准。日志 /tmp/control-waves-wave8-v1-permission-red.log 与 green-fixed.log。独立 crate 的 policy SHA 序列化、safe fd、锁损坏和测试时间构造问题均修复，原行为断言保持。

修复后所有门禁退出0：Rust379（31真实daemon/入口、12架构）/tmp/control-waves-wave8-tests-audited-final.log；strict Clippy/MSRV1.88/release 对应 clippy/msrv/release-audited-final.log；fmt/JS/diff static-audited-final.log，文档收尾再检查 delivery-static.log；Web36 web-accepted.log。cargo deny --offline check --hide-inclusion-graph 四项通过 deny-accepted.log，仅已有公告缓存，未在线刷新。完整逐项 owner、迁移、删旧路径、兼容、失败、测试和实际环境边界见 docs/changes/governed-runtime-final-report.md；物理 API docs/runtime-crates.md。

八波实现与本地门禁全部完成。商业 Provider/真实 IDE v2/MCP/用户 hook/Docker成功隔离/真实Keychain/Linux/CI仍未验收；不声称这些生产环境已通过。没有提交、推送或部署。
