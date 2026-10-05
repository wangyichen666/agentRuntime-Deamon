# Agent Runtime 重构实施记录

日期：2026-10-01。需求：仓库内完成、按 Wave 0–7 顺序、每一 wave 编译/迁移/测试独立闭合。

## 当前验收结果（2026-10-02）

本章节保留 2026-10-02 的历史实施证据。2026-10-05 源码复核已更正原“Wave 0–7 主链全部完成”的结论，当前 schema v14；版本化计划执行/hooks、手动 compact 独立 owner、tool_search 等仍未实现。以 [最新实施记录](./runtime-readback-fences.md) 与 known-issues 的逐项状态为准；本文件旧章节中的完成结论不代表全部用户目标已验收。

## 基线

- `git status --short --branch`：`main...origin/main`，干净；`git log -1 --oneline`：`2f204df feat: 补齐运行时门禁与持久子 Agent`。
- `cargo test --all-targets`：退出 0，197 个单元测试、7 个真实 daemon 进程契约测试，无已有失败。
- 已核对 README、roadmap、known-issues、daemon/entry/storage/provider/tools/safety、session/context/memory/plan/mcp 与 runtime_contract。当前目录不存在额外 AGENTS.md；遵守用户给出的中文规则。
- [ADR 0001](../adr/0001-runtime-state-ownership.md) 记录现状、目标依赖、状态副本、迁移与时序；先于实现建立。

## Wave 0：已完成

### 状态 owner 与依赖边

根项目成为 Cargo workspace；新增 `agent-core` 和 `agent-daemon-protocol` 两个真实接线 crate。core 拥有唯一 ID、RunStatus、消息/工具 DTO、SessionInfo/Status、RequestId、PendingApprovalInfo 与 DomainError 定义；协议 crate 拥有唯一 wire 帧与编码实现，只依赖 core 和序列化/错误库。原模块只 re-export，未保留复制实现。

原 `protocol → storage → daemon::protocol` 类型依赖环解除：storage 的 RequestId 直接来自 core，协议的 RunId/EventSeq 也直接来自 core。Provider attempt 的 RunId 不再来自 storage。CLI/TUI/ACP/HTTP/WS 展示和恢复改用同一 core DTO / workspace wire 类型；入口不直接导入 storage/runtime/session concrete 类型。CI 的测试、Clippy、MSRV 和 macOS 检查已扩大到 workspace。

**业务状态 owner 本轮未改变**：没有删除 SQLite/JSONL/SessionRuntime.history 中任何一份状态，也没有新增存储或双写。删除的是旧 ID/消息/协议的本地定义，统一为同一 Rust 类型。SessionRuntime.history 的唯一现有 owner 被护栏冻结，必须在 Wave 2 迁移删除；不是把其当前权威地位描述成已修复。

### 迁移与兼容

schema 仍为 **v5**，本 wave 无数据 migration、无 dual-write，保留既有 v1→v5 前进升级和未来版本拒绝。旧 JSONL、session list、pending approval、数字/字符串 request ID、NDJSON 帧和 resync cursor 兼容测试通过。旧 wire 的未知字段宽容策略仍保持；严格 versioned/deny_unknown_fields 请求 DTO 属于 Wave 1，未实现。内部 lifetime 不发布到兼容公开 DTO。

### 新不变量与测试

| 不变量 | 测试 |
|---|---|
| exact owner 逐项匹配 key/lifetime/run/generation/turn | `exact_owner_accepts_only_all_matching_dimensions` |
| 同公开 key 的旧 lifetime 不能通过纯领域 fence | `reused_public_key_does_not_authorize_old_lifetime`（非持久集成保证） |
| generation 溢出不得回绕 | `generations_never_wrap_and_reauthorize_stale_work` |
| ID 与 run 状态保留旧 wire 表示 | `legacy_ids_keep_transparent_wire_representation`、`persisted_run_statuses_keep_their_wire_names_and_terminal_meaning` |
| core/协议兼容旧消息、session、审批和 readback | `legacy_dto.rs` 3 项、`compatibility.rs` 5 项；原协议 3 项完整移动保留 |
| 入口、storage、Provider 禁止边；context 无私有历史；历史 owner 不增加；共享类型不复制 | `architecture_contract.rs` 8 项；AST 检查识别分组、别名和完整路径，忽略显式测试 fixture |
| 文件与内存 SQLite 后端共用幂等、单 writer、队列、终态、会话隔离合同 | `control_fact_contract_is_shared_by_memory_and_file_sqlite_backends` |
| terminal 插入失败会回滚回答、状态和 event cursor | `terminal_write_failure_rolls_back_content_state_and_event_cursor`（SQLite trigger 故障注入，两后端） |
| mutation→kill/restart→durable readback→CLI/ACP/WS 验证，未知副作用不重放 | 保留 `committed_terminal_survives_real_daemon_restart_and_uncertain_run_is_not_replayed` 与另 6 个真实进程契约测试 |

新增库代码在 Clippy 中拒绝 unwrap、expect 和 stdout print（测试除外），没有新增 allow 或 TODO 业务骨架。架构检查冻结的是可解析的依赖/字段形态，不能证明所有业务行为安全，必须与后续 repository CAS/竞态测试共同使用。

### 实际命令与结果

| 命令 | 结果 |
|---|---|
| `cargo check --workspace --all-targets` | 退出 0 |
| `cargo fmt --check`、`cargo fmt --all -- --check` | 退出 0 |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | 退出 0 |
| `cargo test --workspace --all-features` | 退出 0，228 项：core 6 + legacy DTO 3 + protocol 3 + wire compatibility 5 + 根单元 196 + 架构 8 + 真实 daemon 契约 7；无跳过 |
| `cargo +1.88.0 check --locked --workspace --all-targets --all-features` | 退出 0 |
| `cargo build --locked --release` | 退出 0 |
| `node --test web/app.test.cjs`、`node --check web/app.js` | 退出 0，27 项测试 |
| `cargo deny --offline check --hide-inclusion-graph` | 退出 0，advisories/bans/licenses/sources 均通过；使用缓存公告库，未验证在线公告更新 |
| `git diff --check` | 退出 0 |

中间发现并修复：架构测试的 slice 类型推导、测试函数类型的 Clippy complexity、新 path dependency 的 wildcard 约束。未降低断言或放宽门禁。全部旧测试继续通过，基线 204 项增加到 228 项。

### 收口与风险

本轮仅完成 Wave 0，按需求优先交付完整早期 wave；没有进入后续业务迁移。core/协议已提取并不表示 Wave 1 已完成。当前 SQLite/JSONL/内存历史仍割裂；pure fence 未在持久事务中使用；ResourceId 仅统一类型，现有进程资源仍不持久；跨会话 memory scope 未实现。新 CI 未在 GitHub Actions 运行。

## Wave 1：已完成

### 状态 owner 与依赖边

新增实际接线的 `agent-daemon-client` 与 `agent-storage` crate。Unix/测试传输、请求分发、typed RPC 错误、显式重连与 cursor readback 只在 client 实现；根 `src/client.rs` 仅 re-export。CLI/TUI、ACP、HTTP/WS 使用这一 client，入口不再导入 daemon concrete、根 client facade、configuration/provider concrete。启动与配置组装集中到 `bootstrap`；`sessions` 默认走 daemon RPC，`sessions --offline` 是明确的 maintenance 兼容读。

SQLite 控制事实与委派实现从根 storage 物理迁入 storage crate；真实接线的 Run/Interaction/Artifact/Delegation ports 共用唯一 RunStore。在线 ControlRepository 不包含 recover/reconcile；维护走独立 MaintenanceRepository，但两者共享同一个存储实例。storage 只依赖 core 和持久化基础库，不依赖 daemon、runtime、protocol 或 Provider concrete。Provider route/attempt/usage/error kind/retry facts 归 core，网络适配保留原 Provider 实现。

所有 handler 参数 DTO 移入 protocol；新 client 发 `protocol_version=1`。v1 envelope、请求 DTO、模型 profile 和 response/event envelope 拒绝未知字段；旧请求保留已知字段过滤和兼容方法，规范别名只映射唯一既有 handler。未实现的方法继续拒绝，不返回假成功。

删除旧 client/storage 的复制定义、入口的独立响应解包与直接业务配置导入；未新增第二套业务 owner 或 dual-write。**会话消息 owner 本 wave 未改变**：SessionRuntime.history、JSONL、SQLite 的统一身份属于 Wave 2，未实现。

### 迁移与兼容

schema 保持 **v5**，本 wave 无数据迁移；现有数据库、JSONL 和旧 wire golden 测试保留。新协议严格策略仅在 v1 生效；未来未知协议版本 fail closed。client 重连不重发 mutation，复用单调 request counter，传输失败与业务 terminal 区分。业务结果未知时须 readback/reconcile。

### 核心不变量与测试

| 不变量 | 测试 |
|---|---|
| 连接丢失后重连只读回 cursor，不重复 mutation，不复用 request ID | `reconnect_uses_cursor_readback_without_replaying_mutation` |
| readback 必须匹配请求 run，事件连续且 page 有界 | `run_readback_rejects_a_different_run_owner`、`readback_rejects_wrong_owner_and_missing_event_sequence` |
| resync 错误保留 typed snapshot/cursor | `resync_required_keeps_typed_snapshot_and_cursor` |
| v1 未知 envelope/参数/模型嵌套字段在 handler 前拒绝 | `v1_rejects_unknown_envelope_and_params_before_dispatch` |
| legacy 扩展字段兼容且只执行同一命令 | `legacy_extensions_are_discarded_and_share_the_same_command` |
| wire 与内部调用都拒绝未知版本 | `unknown_protocol_version_fails_closed_for_wire_and_internal_callers` |
| response/event 的版本策略明确 | `versioned_response_and_event_envelopes_reject_unknown_fields` |
| alias 保留 request ID 与 interaction owner 字段 | `versioned_aliases_preserve_ids_and_interaction_owner_fields` |
| storage concrete 禁止边、入口禁止边、兼容 facade 无复制定义 | `architecture_contract.rs` 8 项 |
| 两持久后端共用既有控制合同与 terminal 写故障回滚 | storage 原 15 项测试完整迁移，包含 `control_fact_contract_is_shared_by_memory_and_file_sqlite_backends` |
| 真实 daemon 重启后 typed client 重连/page readback，CLI/ACP/WS 仍同事实 | 扩展 `committed_terminal_survives_real_daemon_restart_and_uncertain_run_is_not_replayed`，同时验证未知 v1 字段拒绝且 Provider 请求数不增加 |

### 实际命令与结果

- `cargo test --workspace --all-features`：退出 0，237 项（core 8 + legacy DTO 3 + client 4 + protocol 3 + compatibility 5 + versioned 5 + storage 15 + 根单元 179 + 架构 8 + 真实 daemon 7）；没有忽略或删除旧断言。
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`：通过。
- `cargo +1.88.0 check --locked --workspace --all-targets --all-features`：通过。
- `cargo build --locked --release`：通过。
- `cargo fmt --all -- --check`、`git diff --check`：通过。
- `node --test web/app.test.cjs`、`node --check web/app.js`：通过，27 项。
- `cargo deny --offline check --hide-inclusion-graph`：advisories/bans/licenses/sources 通过；缓存公告库，未验证在线公告更新。

### 剩余风险与未实现阶段

Wave 1 只完成模块边界与协议统一。重连 API 是显式传输恢复，不能把传输中断当作业务取消，也不保证所有 UI 自动恢复。离线 sessions 读取兼容 JSONL，属于维护路径；在线 sessions 启动 daemon，当前仍需可用模型配置。GitHub CI 未运行。

- Wave 2：SessionRepository、迁移备份、持久 lifetime、tombstone、routing lane、删除重建事务 fence，**未实现**。
- Wave 3：统一 TurnCommit、跨介质 intent/marker 与工具闭合，**未实现**。
- Wave 4：无状态 ContextEngine、durable compact CAS、四层 envelope 和唯一完整请求计量，**未实现**。
- Wave 5：MemoryEngine scope/source、legacy 隔离、召回与维护 fence，**未实现**。
- Wave 6：ToolDescriptor、冲突 waves、daemon 后台资源、Docker 强隔离，**未实现**。
- Wave 7：远程 MCP、secret 迁移、doctor/reconcile、发布故障注入，**未实现**。

Wave 2 已核对现有在线写入点，但未写入不完整 migration 或引入第二 transcript owner。现有 Native sandbox、持久 run/delegation、JSONL、remember 工具不等于未来 wave 已完成。

## Wave 2：已完成（2026-10-02）

状态 owner：session_heads 与 transcript_batches 成为 lifetime/revision/消息的 SQLite canonical owner；SessionRuntime.history 删除，每 turn 从持久 snapshot 构造局部执行输入。Run/Interaction/Tool/Delegation/Provider/Event 共享持久 lifetime/run generation，并由事务 fence/SQLite triggers 防止旧事实写入新 incarnation。在线与 cron 删除 JSONL append 路径，legacy append 只留测试 fixture。trace 按 lifetime 物理隔离。

迁移：v5→v6（同时支持更旧版本前进升级）；升级前 VACUUM 备份并校验 integrity/schema/SHA256 marker；旧 JSONL 首次导入前保留字节备份与校验，坏尾保留在备份中，完整坏行拒绝。schema/trigger 在同一 migration 事务发布；未来版本拒绝。旧 wire request id 不变，内部幂等键按 lifetime namespace，delete/recreate 可重用 request id。

新增合同：`lifetime_transcript_contract_is_shared_by_file_and_memory_sqlite` 覆盖两后端、append 幂等/冲突、busy barrier、fork revision CAS、delete/recreate 旧 append/event/attempt/准入零写入、clear 与 run generation；`snapshot_and_lifetime_survive_restart_and_corruption_fails_closed` 验证重启及摘要损坏；`session_delete_recreate_and_fork_survive_restart_without_old_history_or_dual_write` 验证真实 daemon mutation→kill/restart→readback→CLI/ACP/WS；架构 guard 现在要求长期 mutable transcript owner 集合为空。

命令：`cargo test --workspace --all-features` 退出 0，240 项；`cargo clippy --workspace --all-targets --all-features -- -D warnings` 退出 0；`cargo fmt --all` 已应用。旧门禁断言保留，cron 持久性断言改为验证 canonical SQLite snapshot 和不存在旧 JSONL 双写。

剩余风险：tool exchange 及 assistant/terminal 仍需 Wave 3 原子闭合；当前 compact 仅作用本 turn 局部输入，Wave 4 durable projection 未实现；Wave 5–7 未实现。destructive lifecycle 在 active/queued run 未结算时 fail closed，必须先精确取消/结算；不按运行时长强拆 gate。没有提交或推送。

### Wave 3 实施验证（2026-10-02）
RunSnapshot、TurnState/RoundState 与原子 TurnCommit 已接入生产链路。工具整批预检、取消 join、崩溃闭合、计划 CAS 与权限冻结通过 workspace 回归（243 项 Rust 测试）；Clippy 无警告。后续 Wave 4–7 继续执行，尚未宣称完成。修复工具委派时过大的异步 future：在执行/监督边界 Box::pin；未增加线程栈或放宽门禁。

### Wave 4 持久投影阶段验证（2026-10-02）
提取 agent-context 无状态计量/验证；schema v8 支持 compact intent、source prefix CAS、projection head 与 context ledger。在线 ContextManager 从 canonical snapshot 读取投影，原始 transcript 不改写；L1 媒体投影降级、L2 工具结果投影裁剪、L3 摘要，取消/无收益拒绝，摘要失败可单独提交 prune_only/partial。workspace 245 项 Rust 测试与 Clippy 通过，包含并发后缀、竞争 head、delete/recreate 和重启遗留 intent。Wave 5 开始；手动 compact RPC、多入口 generation 合同及 ledger 校准还需在后续集成收尾，不宣称整套验收完成。

### Wave 5 实施阶段验证（2026-10-02）
agent-memory 提供 typed scope/layer/kind、可见性复核与确定性 ranking；SQLite v9 保存记忆、legacy quarantine 与 committed-turn ingest receipts。在线 remember/recall 不再写 JSONL；runtime bounded recall 独立检索分区，terminal 发布后单次有界摄入，维护失败不改 terminal。memory store/recall/list/forget/scope RPC 接通，context_read_only 同时阻止 memory 写入/forget/compact。248 项 Rust 测试及 Clippy 通过，覆盖过滤后 limit、TTL、全局确认、legacy 隔离、重启与迟到 lifetime fence。接下来推进 Wave 6，三入口集成合同及完成后维护诊断仍在最终集成清单。

## Wave 3–5 最终收口

**状态 owner 与删除的副本**：SessionSupervisor 拥有实例、writer 与生命周期屏障；RunCoordinator 拥有活跃任务、冻结能力和排队唤醒。SQLite 的 canonical transcript、TurnCommit、计划、compact head、memory 与 receipts 是唯一 durable truth。ContextManager 只接收不可变历史并返回候选，由 runtime 调 repository 安装；没有私有 session map 或在线 JSONL 双写。MemoryEngine 先审查作用域/TTL/确认，再排序、去重和截取预算。计划和技能进入 retrieved_context，memory 独立插入该分区，当前输入/观察保留在 overlay。

**迁移与不变量**：v7 原子 turn/tool/plan；v8 compact/head/ledger；v9 scope/source/legacy memory；v11 compact 结果 receipt 与维护诊断；v12 forget receipt。compact prefix digest/generation/lifetime CAS 允许 fresh suffix；重复 operation 在 append/restart 后仍读回同 receipt，不同 owner/source 拒绝。Provider 完整请求包含 system/tools/media/四分区和 output reserve；fallback 重计量。成功 committed turn 的 usage 锚点同时绑定 lifetime、完整 provider identity、模型和 projection generation，失败 turn/不同 provider/generation 不复用。取消、空摘要、坏工具交换、无收益、截断/未完整结束均不安装；overflow 每轮至多一次 durable compact，read-only 禁止安装。

| 不变量 | 实际回归 |
|---|---|
| 原子 terminal 故障不半提交；整批预检零启动 | `crates/storage/src/turns.rs` 的提交回滚合同；原 daemon 整批预检/权限合同 |
| compact source CAS、不覆盖并发后缀、竞争 head 拒绝 | `compact_cas_preserves_concurrent_suffix_and_rejects_competing_head` |
| 重启投影读回、遗留 intent 结算 | `restart_keeps_projection_and_settles_uncommitted_intent` |
| 完整摘要与预先取消 | `truncated_summary_and_cancelled_candidate_are_never_installed` |
| usage 校准不跨 provider/模型/generation，重启可读 | `usage_anchor_requires_committed_turn_exact_provider_and_projection_generation` |
| 记忆过滤后 limit、TTL/确认/legacy 隔离 | `ranking_filters_scope_trust_and_expiry_before_limit`；storage memory scope 合同 |
| 遗忘 receipt 复用，不复活，旧 lifetime 拒绝 | `forget_receipt_replays_exact_result_and_old_lifetime_is_rejected` |
| 三入口、mutation/kill/restart/readback、readonly/clear fence | `compact_memory_receipts_survive_restart_and_all_three_entries`；扩展 `entry_views` 在 CLI/ACP/WS 对比 memory/context/resources 的完整事实 |

## Wave 6：已完成主链

工具元数据、完整 JSON Schema 校验、资源 read/write/external 冲突 waves、整批准入与所有 started future join 接入真实 ToolRegistry/LoopEngine。拒绝批次没有 running receipt；未取得可信外部回执记 outcome_unknown，不能宣称成功。artifact 先内容寻址持久化，transcript/event/receipt 仅保留有界 preview、hash 和 reference；范围读取验证 exact owner、原始文件摘要与路径边界。

schema v10 保存 descriptors、resources/logs。daemon 监督真实后台 Child、进程组、管道 drain、cancel/stop/shutdown 和 join。resource 的 wait timeout 只结束等待；重启不能验证进程身份时标 orphaned，不按旧 PID kill。完成日志仍可 readback，clear/delete/recreate 阻断迟到写入。

Native 是软边界。可选 Docker exec 使用成熟 Docker CLI、预装镜像、readonly workspace、无网络、非 root、只读 rootfs、cap drop、no-new-privileges 和内存/CPU/pid/tmpfs 上限。backend 不可用时 docker 请求 fail closed；auto 明确返回 requested/effective/notice。政策与镜像选择在准入冻结，child 保持或收窄政策。正常清理有界等待，异常 Drop 的精确容器清理不会阻塞运行时线程。

实际回归：`scheduling_parallelizes_independent_resources_and_fences_conflicting_writes`、完整 JSON Schema 关键字合同、`background_wait_timeout_is_not_terminal_and_stop_joins_exact_resource`、storage resource restart/lifetime 合同、`artifact_gc_keeps_referenced_evidence_and_cleans_only_unreferenced_files`。Docker 本机检查：daemon 存在但无 `alpine:3.21`；实容器成功路径未验证，不自动拉镜像。后台 Docker resource **未实现**；强隔离后台请求拒绝启动，不降为 Native。

## Wave 7：已完成主链与本地发布门禁

**远程能力与 secrets**：MCP 支持 stdio 与 HTTPS Streamable HTTP/JSON/SSE。同一冻结 catalog 的旧客户端不会被 reload 提前关闭。每个 HTTP 请求重查全部有界 DNS 结果并 pin 地址，禁代理/redirect，默认禁私网/保留地址，逐 server 显式私网授权；TLS 验证保留。RPC 身份、完整 SSE event、session header、pending、catalog/schema 与 body 有界。外部工具响应丢失/错误身份/中断不自动重发，记 typed outcome_unknown。stdio 不继承完整 daemon 环境，stderr 只 drain 并计字节，不输出上游正文。

Provider 和敏感 MCP env 的旧明文先写系统 secret store、逐项读回，再以临时文件/fsync/读回/原子 rename 发布引用。失败保留旧配置读取能力；新明文写入不得绕过迁移。env 与 macOS Keychain 实现接入生产组合，摘要/Debug/诊断不返回密钥正文。其他平台系统 secret store **未实现**，可用 `env:NAME`。

**治理与背压**：doctor 报 integrity/schema、排队/未知 run/orphans/compact/legacy/维护诊断、锁等待及预算；bounded artifact GC 只删过保留期且没有 receipt 引用的文件。持久队列总 1024、每 session 64；输入 64 KiB、工具 batch 64、参数 64 KiB、MCP pending 64/body 256 KiB、资源 active 16/log 64 KiB、artifact range 16 KiB。列表采用稳定 cursor 和响应字节预算。连接帧队列 256，慢消费者断开后读取 durable cursor，不能把断线当 terminal；超长请求在整帧分配前拒绝。人工 resources.reconcile 仅核对 orphaned exact owner，要求终态和证据；同命令幂等，不向旧 PID kill。memory 维护完成/失败/read-only skip 独立持久诊断，不修改已发布 terminal。

实际回归：`tls_json_sse_receipt_and_budget_contract` 使用本地自签证书信任进行真实 TLS 握手，覆盖 JSON/SSE、超预算、redirect 与错误身份；`migration_failure_preserves_exact_old_bytes_and_redacts_debug`；`queue_budget_rejects_new_work_but_allows_exact_readback`；`slow_consumer_disconnects_instead_of_buffering_without_limit`；resource orphan 手工核对/重试/旧 lifetime 合同。没有调用真实 Keychain 写入；远端 OAuth、真实服务互操作、在线公告刷新和 GitHub Actions 尚未验收。

## 最终实际门禁（2026-10-02）

| 命令 | 结果 |
|---|---|
| `cargo test --workspace --all-features` | 退出 0；261 项 Rust 测试（包括 9 项真实 daemon 黑盒合同），0 failed/ignored |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | 退出 0，无警告 |
| `cargo +1.88.0 check --locked --workspace --all-targets --all-features` | 退出 0 |
| `cargo build --locked --release` | 退出 0 |
| `cargo fmt --all -- --check`、`git diff --check` | 退出 0 |
| `node --test web/app.test.cjs`、`node --check web/app.js` | 退出 0，27 项测试 |
| `cargo deny --offline check --hide-inclusion-graph` | advisories/bans/licenses/sources 均通过；重复版本仅按现有配置 warn，未新增公告忽略 |

原始日志：`/tmp/wave7-final-tests.log`、`/tmp/wave7-final-clippy.log`、`/tmp/wave7-final-msrv.log`、`/tmp/wave7-final-release.log`、`/tmp/wave7-final-web.log`、`/tmp/wave7-final-deny.log`。MIT-0 许可证已核对本地 LICENSE，仅对 `borrow-or-share@0.2.4` 允许；没有放宽全局许可或关闭测试。测试后没有源码变更，只更新验收文档。

本轮 Wave 2–7 主链已完成。部署限制：后台 Docker resources、非 macOS 系统凭据适配器、远程 OAuth **未实现**；真实 Docker 容器成功路径、真实 Keychain 写入、真实远程服务互操作、在线公告更新和 GitHub CI 未验证。当前环境缺预装 Docker 镜像，强隔离准入拒绝符合合同。人工 reconcile 只记录带证据的核对，不验证证据真实性、不按旧 PID kill。后续可扩大这些后端能力；不影响已验收的 canonical truth/lifetime/terminal/compact/memory owner 主链。

## Codex 对比后的上下文与记忆改进

2026-10-02：保护最新用户轮次，压缩附带有界旧用户原文并支持再次压缩延续；memory crate 负责完整 JSON 证据渲染，预算包含提示/元数据/消息开销；Episode 摄入16KiB总上限、逐批扫描、明确截断与空结果幂等回执。保留SQLite v12、CAS、作用域和确认晋升规则。研究依据、取舍、后续计划与266 Rust/27 Web验证详见 [对比报告](../research/codex-context-memory.md)。
