# Runtime 控制面八波实施记录

日期：2026-10-05。基线 HEAD：`a8ac3dcbbdd9f77f4988a5359d03c201e4df9f2e`，初始工作区干净。

基线沙箱测试退出 101（Unix socket EPERM）；同命令在允许 socket 的环境重跑退出 0，302 项 Rust 测试通过。日志 `/tmp/control-waves-baseline.log`、`/tmp/control-waves-baseline-authorized.log`。

## 实际进度

截至 2026-10-07，审计、ADR 0003、Wave 1–8 的实现与本地门禁已完成。最终 379 项 Rust、36 项 Web 全部通过；新增七个真实库 crate，根源码仅组合启动，schema v20。最终逐项交付见 [最终报告](governed-runtime-final-report.md)。以下每波记录中的剩余范围、旧源码路径和测试数量仅描述该波结束时的历史；最后完成记录与当前报告为准。

## 实施前源码审计证据（历史路径）

- 既有 `TurnRepository::stage_plan`/TurnCommit 提供 revision CAS 和原子发布，但进度也增加版本；`src/plan.rs` 缺定义摘要与身份。
- `src/daemon/handlers.rs::session_compact` 仍借历史 owner，unary 等待；独立生命周期未实现。
- 既有 `SessionReadback::supersedes` 和连接协商继续复用，不声称其已经完成全部入口 live reducer/ACP v2。

## Wave 1 实际交付

| 领域 | 实际所有权、删除路径与行为 |
|---|---|
| 定义与摘要 | core 只负责领域值、固定字段定义编码与同源 Markdown；storage 复用已有 SHA-256 依赖。title/goal/成功标准/约束/验证/有序步骤 ID 与描述决定摘要，进度/review 不改变定义版本。没有放宽 core 的依赖白名单。 |
| canonical publication | 原 `stage_plan` CAS 和成功 TurnCommit 同事务发布 `session_plans`、`plan_versions` 文档与 Markdown。进度更新不增加 revision；历史版本不删除。artifact/history 或 terminal 故障全部回滚。 |
| 决策与身份 | SQLite `plan_decisions` 保存 exact session/lifetime/plan_id/revision/digest/operation、输入及请求策略绑定。`chat.send.plan_execution` 是唯一执行命令；discard RPC 与 slash 复用 `discard_plan_command`。原 run 准入、冻结 route/catalog、审批、安全、writer 和 terminal 链路保持。 |
| 审阅时序 | 准入同事务登记 pending_execution、queued run 和回执。writer 临界区以 ExactOwner 复核后才进入 executing。失败/unknown 后 blocked。review executing 表示该版本已经获执行身份，实际终态读关联 run；不是入口本地计时状态。 |
| 重启与废弃 | recovery 不自动调度计划 queued run；显式同操作/同摘要继续才恢复。精确废弃自己的未启动 pending run 时，cancelled terminal、review rejected 和回执同事务提交；其他 busy/run/resource 仍拒绝。daemon 只唤醒这个 native run。 |
| 只读与旧路径 | `sessions.plan.readback` 只读事务返回文档和 Markdown，无 runtime/Provider/MCP/旧文件导入；默认无 daemon artifact 绝对路径。旧计划保持只读，查询、执行、update/add 都不临时补身份；显式 set 定义后才获得新身份，原始数据保留在 plan_legacy_evidence。删除生产 `PlanStore` 的无 owner 本地文件写入 fallback；旧文件保留一次性导入与证据用途，测试 fixture 的文件往返仍独立验证。 |
| 入口 | CLI/TUI/ACP/WebSocket/HTTP 适配集中进入 daemon/protocol；三入口及 HTTP 实际执行相同 execute/discard 重试并读取同一 run/receipt/计划。Web read/discard 不伪造聊天 terminal；ACP 幂等 completed response 从已提交正文展示，无新 delta 也能读到结果。 |

具体严格 DTO、RPC 和共享命令见 [计划 API](../plan-control-api.md)。旧 chat 没有 plan_execution 时保持原行为；原 aliases 仍映射同一 handler。协商只有 runs 能力时，不能借 chat metadata 绕过 session 能力；没有广告 ACP v2 或实现其专属控制面。

## Migration 与失败语义

- v14→v15 沿用 upgrade 前 `VACUUM INTO`、integrity/schema/SHA-256 备份校验和 verified marker；新增三张领域表（旧计划原始证据、版本历史、决策回执）与 snapshot-clock triggers，旧计划数据不被改写为可执行身份。
- 原最旧受支持 fixture→latest 测试保持；v14 专项覆盖故障回滚、备份/marker、重启和未来 v16 拒绝。低版本 fixture 只撤销新增 schema 后才构造历史库，没有放宽迁移断言。
- stale digest/revision/lifetime、同 operation 换身份/输入/sandbox/context_read_only/准入模式、旧计划执行和 busy 均拒绝。取消与 review 在同事务回滚，不按 elapsed/ACK/断线猜测批准或终态。
- 没有恢复 JSONL 双写。未知副作用继续沿用 unknown_after_restart，不自动重放。

## RED/GREEN 与测试证据

`plan_definition_revision_ignores_progress_and_publishes_stable_digest` 在修改前因缺 content_digest 失败，日志 `/tmp/control-waves-wave1-red.log`；后已通过。追加的 `discard_pending_plan_cancels_only_the_exact_unstarted_run` 先因 busy 拒绝复现，日志 `/tmp/control-waves-wave1-discard-red.log`；修复后通过。

| 不变量 | 测试 |
|---|---|
| 固定定义编码，进度不改摘要，字段变更影响定义，重复 ID 拒绝 | `digest_is_definition_only_and_order_sensitive`、`plan_definition_revision_ignores_progress_and_publishes_stable_digest` |
| 文件/内存同一 CAS/receipt/review/busy 合同；策略变更不能复用操作 | `plan_decisions_are_exact_idempotent_and_share_file_and_memory_contract` |
| pending 计划重启不自启，旧计划不得执行 | `pending_plan_survives_restart_without_automatic_execution_and_old_plan_stays_readonly` |
| pending 废弃只取消 exact 未启动 run | `discard_pending_plan_cancels_only_the_exact_unstarted_run` |
| Markdown publication 失败回滚 transcript/review/terminal/cursor | `markdown_publication_fault_rolls_back_plan_terminal_and_revision`，原 `terminal_fault_rolls_back_assistant_plan_usage_and_transcript_revision` 保持 |
| v14 迁移故障/备份/marker/重启/未来拒绝 | `schema14_upgrade_is_backed_up_atomic_and_rejects_future_versions`，原最旧 fixture 迁移合同保持 |
| 严格嵌套身份，文本适配幂等，能力不绕过 | `plan_execution_is_typed_exact_and_text_adapter_preserves_identity`、`plan_metadata_cannot_bypass_session_capability_using_chat_method` |
| 真实 repository→kill/restart→daemon→CLI/ACP/Web 同一执行/废弃/读回 | `versioned_plan_readback_execute_discard_and_restart_share_daemon_facts` |
| 真实 pending 重启后精确废弃，native terminal 与 gate，后续聊天和三入口读回 | `restarted_pending_plan_can_be_discarded_by_exact_identity_and_release_its_queue` |
| 前端 read/discard 不写 transcript 或伪造聊天完成 | `计划读取和废弃通过 daemon 命令显示，不伪造聊天终态` |

完整回归一度在原队列合同的 1 秒 Web 启动轮询中失败；独立重跑通过。启动检查改为带进程退出/stderr 诊断的有界就绪等待，原业务断言未放宽，完整回归重新通过。日志 `/tmp/control-waves-wave1-tests-final-audit.log`、`/tmp/control-waves-wave1-readiness-diagnosis.log`。

新并发/重启路径用持久 queued/run 状态构造确定性交错，没有固定 sleep 证明业务正确；既有 socket 启动 readiness 轮询保留。所有原测试继续运行，架构断言未删除或放宽。

## 实际门禁

| 命令 | 结果与日志 |
|---|---|
| `cargo fmt --all -- --check` | 退出 0 |
| `cargo test --locked --workspace --all-targets --all-features` | 退出 0，315 项，全部通过，无忽略；`/tmp/control-waves-wave1-tests-verified-final.log` |
| `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` | 退出 0；`/tmp/control-waves-wave1-clippy-verified-final.log` |
| `cargo +1.88.0 check --locked --workspace --all-targets --all-features` | 退出 0；`/tmp/control-waves-wave1-msrv-final-audit.log` |
| `cargo build --locked --release` | 退出 0；`/tmp/control-waves-wave1-release-final-audit.log` |
| `node --test web/app.test.cjs` | 退出 0，28 项；`/tmp/control-waves-wave1-web-verified-final.log` |
| `node --check web/app.js`、`git diff --check` | 退出 0 |
| `cargo deny --offline check --hide-inclusion-graph` | 允许缓存锁写入环境退出 0；四项检查通过，保留原 duplicate warnings；`/tmp/control-waves-wave1-deny-authorized.log` |

供应链初次沙箱检查不能获取只读公告缓存锁，日志 `/tmp/control-waves-wave1-deny.log`；环境重跑通过。只使用既有公告缓存，没有在线公告刷新。新增真实 daemon fixture 初次因 macOS SUN_LEN 和非法 session key 失败，修复 fixture 并加强 stderr/提前退出诊断及 kill-on-drop 后通过；生产身份校验未放宽。

## Wave 1 结束时审计与剩余范围

本次没有新增第二个事实源、入口私有业务终态、按时间推断、未接线 trait 或生产库 panic/stdout；新增计划生产路径不写本地 plan 文件。版本历史、Markdown 与决策均在同一 SQLite lane；原 transcript 与工具副作用证据保持。计划 readback 不包含 daemon artifact 本地路径、Provider key 或 bearer token；冻结 route 的旧脱敏政策保持。Cargo.lock 与依赖白名单未改变，没有提交、推送或部署。

Wave 2 hooks/Stop continuation、Wave 3 独立 compact、Wave 4 tool_search、Wave 5 共享 reducer、Wave 6 完整只读请求重建、Wave 7 ACP v2、Wave 8 crate 提取均未实现，也没有声称已经通过这些波的验收。真实商业 Provider/MCP、Docker、Keychain、Linux 和 GitHub Actions 未验证；本次只使用本机 mock Provider 与真实本地 daemon/入口进程。

## Wave 2 实际交付

core 提供 16 类严格事件/效果/失败与入口 channel。SQLite v16 的 hook_outcomes 是 operation 级幂等执行审计，hook_publications 与 create/fork/end 的 publication 原子写入，hook_continuations 与新 native run/turn/queue 原子写入。scope 持有 ExactOwner；payload 只含必要身份、权限、摘要、长度和真实事件数据。入口没有脚本执行器、私有终态或 hook 事实缓存。

单一 HookDispatcher 显式 opt-in、受信私有目录、权限/软硬链接/大小检查、no-follow 读取、严格 effect、清空环境、有界 stdout/stderr、timeout/cancel/进程组清理；观察失败 typed audit 后继续，控制失败拒绝。SessionStart/End 与同一 lifetime 的真实发布绑定，重试关闭不会重复。PreCompact 在 intent 后，PostCompact 在结算后；manual/auto、无收益/拒绝/失败和恢复不重执行均已验证。修复旧 compact_result 将失败回执当作成功的问题。

Stop continuation 只有父 completed 后才能发布一个独立 child；完整父 frozen snapshot 必须相同，不能提高权限。限 8 轮、8192 累计 tokens、8 次工具调用和 30 秒；budget 不修改冻结 context policy，请求前/流式发布前计量，最终文本也受限，不作额外重试或摘要请求。子 continuation 和原委派不能递归续跑。queued 恢复沿用原准入，running 崩溃后 unknown，不重放 Provider/hook。exact cancel child 保留父 completed。存储身份查询失败拒绝，不能绕过预算。

CLI/TUI/ACP/WS 的 `/hooks`、HTTP GET `/api/hooks/readback` 调用同一只读分页 handler，含 snapshot_revision，损坏记录拒绝；Web 只显示回执活动。run.read 的 continuation 附加身份由共享严格解码器逐字段验证，其他未知字段仍拒绝。Channel 随原 run 冻结，仅作审计标签。具体配置/协议/兼容见 [Hook API](../hooks-api.md)。

### RED/GREEN 和失败修复证据

- 最初 RED 缺 hook repository：`/tmp/control-waves-wave2-red.log`。Serde 无字段 unit effect 曾忽略未知字段，改成严格空 struct effect；`/tmp/control-waves-wave2-strict-effects.log`。
- 子进程 stdin shutdown 后须 drop 才真实 EOF；原失败与 green：`/tmp/control-waves-wave2-session-green.log`、`/tmp/control-waves-wave2-process-tests.log`。
- 真实 compact 失败重启重试原先错误返回成功，已修复并保持不重执行：`/tmp/control-waves-wave2-compact-failure.log`。
- 真实 continuation 崩溃合同发现借用 for_delegation 修改 context policy 导致 frozen CAS 失败；改独立预算构造器，冻结 policy 保留。`/tmp/control-waves-wave2-stop-crash-diagnosis.log`（失败）、`/tmp/control-waves-wave2-stop-budget-green.log`（两项通过）。
- full 回归发现 shared run decoder 缺 continuation 字段，补严格 typed 校验；未放宽未知字段。新进程夹具的正常脚本采用允许的 5 秒预算，专门超时测试仍为 100 ms。原记忆排空测试曾受并行负载失败，独立诊断通过，未改业务断言；完整回归已重新全过。

### 关键合同

| 不变量 | 测试 |
|---|---|
| 16 类 effect inventory/严格 DTO | `inventory_controls_are_explicit_and_observers_cannot_continue_or_deny`、`run_readback_validates_additive_continuation_identity_and_remains_strict` |
| 双后端 claim 幂等、旧 lifetime、unknown 与真实 publication | `hook_claim_effect_fence_recovery_and_lifecycle_outbox_share_both_backends` |
| lifecycle 故障同事务回滚 | `hook_publication_failure_rolls_back_the_lifecycle_receipt_and_head` |
| bounded page/零写入/损坏拒绝 | `paged_hook_readback_is_readonly_and_rejects_corrupt_identity_or_effect` |
| continuation frozen/CAS/回滚/工具预算/重启 | `continuation_admission_is_atomic_frozen_bounded_and_restart_safe` |
| v15 迁移故障/备份 marker/未来 schema 拒绝 | `schema15_hook_upgrade_rolls_back_preserves_backup_and_rejects_future`，最旧受支持 fixture 原合同保持 |
| timeout/nonzero/oversized/非法 effect/不可信配置/取消 | `timeout_nonzero_output_limit_and_invalid_effect_have_typed_results`、`control_cancel_untrusted_config_and_descendant_stop_fail_closed`、`dropping_an_inflight_hook_settles_cancelled_without_replaying_it` |
| 最终文本和大输入预算/冻结 policy | `continuation_keeps_frozen_context_and_bounds_final_text_before_publication` |
| create/fork 和并发 close exactly once，kill/restart | `governed_session_hooks_follow_publication_once_and_survive_restart` |
| 真正工具成功/错误/持久审批 owner，脱敏和三入口 | `governed_tool_approval_error_and_terminal_hooks_use_durable_owners_and_redacted_payloads` |
| auto compact Pre/Post 顺序与真实结果 | `automatic_compact_hooks_follow_real_intents_and_settlements` |
| manual 成功/恢复不重跑，以及 no_gain/rejected/failed | `compact_memory_receipts_survive_restart_and_all_three_entries`、`manual_compact_hooks_publish_no_gain_rejection_and_summary_failure_after_settlement` |
| subagent Pre/Post exact owner 和入口 channel | 扩展 `durable_subagent_spawn_wait_restart_and_scoped_cancel` |
| Stop 新 native run/exact cancel/重启/三入口终态 | `stop_continuation_has_a_new_native_run_and_exact_cancel_preserves_parent_terminal`、`stop_continuation_crash_preserves_unknown_child_without_replaying_hook_or_provider` |
| Web 回执不创建聊天或 terminal | `Hook 回执读取只显示持久事实，不创建聊天 run 或终态` |

### 本波门禁

| 命令 | 实际结果 |
|---|---|
| `cargo fmt --all -- --check` | 退出 0；`/tmp/control-waves-wave2-fmt-final.log` |
| `cargo test --locked --workspace --all-targets --all-features` | 退出 0；332 项，包含 19 项真实 daemon 合同，无忽略；`/tmp/control-waves-wave2-tests-verified.log` |
| `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` | 退出 0；`/tmp/control-waves-wave2-clippy-final.log` |
| `cargo +1.88.0 check --locked --workspace --all-targets --all-features` | 退出 0；`/tmp/control-waves-wave2-msrv-final.log` |
| `cargo build --locked --release` | 退出 0；`/tmp/control-waves-wave2-release-final.log` |
| `node --test web/app.test.cjs` | 退出 0；29 项；`/tmp/control-waves-wave2-web-final.log` |
| `node --check web/app.js`、`git diff --check` | 退出 0；对应 jscheck/diff 日志 |
| `cargo deny --offline check --hide-inclusion-graph` | 退出 0；四项检查通过，保留原 duplicates warnings；`/tmp/control-waves-wave2-deny-authorized.log` |

离线 deny 首次因只读公告库锁失败，`/tmp/control-waves-wave2-deny-final.log`；授权相同检查后通过，仅使用已有缓存，没有在线公告刷新。没有新增依赖、提交、推送或部署。Wave 3–8 仍待完成，商业 Provider/MCP、真实用户脚本、Docker、Keychain、Linux 和 CI 尚未验证。


## Wave 3 实际交付（2026-10-06）

SQLite v17 compact_run_links 把严格 source/request 与独立 native run/turn 关联，既有 compact_operations 继续持有 intent/receipt。准入、frozen snapshot 和 Started 同事务；后台摘要持 session writer，安装 projection、receipt、native terminal 同事务。手动请求不进聊天 transcript，compact turn 不进入聊天记忆摄入。通用聊天 finish/reconcile 无权改写该 kind。run.read 返回同事务 typed source/outcome；共享 decoder 拒绝身份或终态不一致。

唯一 daemon start_compact_command 处理 compact.start 和旧 unary 等待适配。取消仅 native owner；断线 detach、durable cursor 恢复，崩溃未结算 unknown，不重摘要。相同 operation/source 重取原 run，不同 source/来源冲突、busy 拒绝。兼容 owner 只验证来源与只读权限，不再用于执行；历史 receipt 只读不补身份。旧 replayed 标签保留，失败/no_gain 语义不改变。CLI/TUI/Web 的 /compact 立即 Started 并订阅，HTTP POST /api/compact/start 与 bearer/workspace 绑定复用；ACP v1 只读 /run，v2 mutation 留 Wave 7。参见 [Compact API](../compact-control-api.md)。

### RED/GREEN 与终审修复

- 独立 native RPC 原 RED MethodNotFound：`/tmp/control-waves-wave3-red.log`；native Started/取消/kill-restart GREEN：`/tmp/control-waves-wave3-native-green.log`。
- 双后端 source CAS/幂等/busy、terminal 故障回滚、readonly/corruption、迁移 backup/marker/future schema：`/tmp/control-waves-wave3-storage-green.log`；最终增量包含在全 workspace 日志。
- 旧迁移夹具须同时去掉 v17 表与 marker，保持其原旧版本/故障断言；新 slash 不改变原菜单顺序。未删除或放宽测试。
- 新 producer detach 合同发现 watch 的正常关闭不能表示 overflow；继续排空最后 response，真实 256 帧超限仍断开。
- 原记忆40项排空间歇失败定位为 due 查询返回较晚 now，调度器与较早 now 比较产生毫秒竞态。稳定 RED：`/tmp/control-waves-wave3-maintenance-race-red.log`；改未尝试任务 due=0，不修改退避/重试预算；GREEN：`/tmp/control-waves-wave3-maintenance-green.log`。原 daemon 合同保持且最终通过。
- 终审补旧只读 owner 拒绝、compact 非聊天记忆边界，以及 TUI/Web 的 no_gain/cancelled/unknown 真实显示，不能以成功 RPC 伪造成功摘要。heartbeat 的 context/OpenAI 预算增量保留并一起回归。

### 核心测试

- `manual_compact_has_immediate_native_started_exact_cancel_and_unknown_restart`：真实 socket Started 早于摘要，订阅 detach，旧聊天取消无效，native exact cancel 后可聊天，双 compact busy/source 冲突，kill/restart unknown/HTTP重取，CLI/ACP/WS同 receipt。
- `native_admission_source_cas_idempotency_busy_and_terminal_are_atomic_on_both_backends`：无伪 transcript/记忆，类型冲突/聊天 finish 禁止，projection+terminal 故障原子回滚。
- `native_no_gain_cancel_failure_recovery_and_corruption_fail_closed`：各 typed outcome、恢复不重放、reconcile 禁止与损坏拒绝。
- `schema16_compact_upgrade_has_verified_backup_rollback_and_future_rejection`：v16→17 前备份、迁移故障与未来版本拒绝；原最旧 fixture 仍通过。
- 扩展 `manual_compact_hooks_publish_no_gain_rejection_and_summary_failure_after_settlement`、`compact_memory_receipts_survive_restart_and_all_three_entries`：旧 unary 新 owner、只读来源拒绝、真实 Pre/Post 和三入口 native receipt。
- `native_compact_readback_binds_source_kind_owner_and_terminal_strictly`、`producer_detach_drains_committed_response_but_real_overflow_still_disconnects`、`unattempted_ingest_is_immediately_due_without_a_wall_clock_race`。
- TUI `compact_receipt_displays_no_gain_cancelled_and_unknown_without_chat_success`；Web `独立 compact 立即展示 Started、精确取消且无收益不产生聊天原文`、`compact 重连从 native readback 展示 unknown，不伪造完成`。

### 本波门禁

| 命令 | 实际结果 |
|---|---|
| cargo fmt --all -- --check | 退出0；/tmp/control-waves-wave3-fmt-final-complete.log |
| cargo test --locked --workspace --all-targets --all-features | 退出0；341项，含20项真实daemon；/tmp/control-waves-wave3-tests-final-complete.log |
| cargo clippy --locked --workspace --all-targets --all-features -- -D warnings | 退出0；/tmp/control-waves-wave3-clippy-final-complete.log |
| cargo +1.88.0 check --locked --workspace --all-targets --all-features | 退出0；/tmp/control-waves-wave3-msrv-final-complete.log |
| cargo build --locked --release | 退出0；/tmp/control-waves-wave3-release-final-complete.log |
| node --test web/app.test.cjs | 退出0；31项；/tmp/control-waves-wave3-web-final-complete.log |
| node --check web/app.js、git diff --check | 退出0；对应jscheck/diff-final-complete日志 |
| cargo deny --offline check --hide-inclusion-graph | 退出0；advisories/bans/licenses/sources通过，原duplicates warnings保留；/tmp/control-waves-wave3-deny-final.log |

离线供应链只用已有公告缓存，没有在线刷新。没有新增依赖或修改 Cargo.lock；未提交、推送、部署。Wave 4–8 未完成；真实商业摘要 Provider/MCP、用户脚本、Docker/Keychain、Linux 和 CI 未验证。


## Wave 4 完成：冻结工具发现与稳定 dispatcher（2026-10-06）

状态owner仍为SQLite repository：v18 `tool_discoveries`保存exact owner、full snapshot generation、run/round/wire-call操作身份、查询、完整结果和receipt摘要。发表与tool_discovered事件同事务；恢复读取验证身份/schema/摘要并保持只读。取消先持久cancellation_requested再发送token，publication事务与取消串行化，非running/过期owner不得发表；不把取消意图当终态。128 receipts/run、8项/16KiB搜索、8192项/16MiB catalog、最多16份不可变索引。

原每次Provider注入完整目录的路径已替换为小型核心工具面与tool_search。全目录仍作为唯一冻结快照保存；nested仍委托原schema/descriptor/resource conflict/preflight/审批，wire身份不重写；隐藏direct/unfound/fuzzy拒绝。MCP、内置与子管理工具共用registry边界。仅核心read_file的child保留原单工具合同；spawn_subagent原核心alias保持。queued恢复重新冻结后必须与持久快照一致。核心领域库未新增依赖，Cargo.lock未变。

RPC `run.discovery`与 `/tools <run_id>` 接通CLI/TUI、ACP v1与WebSocket同一事实；新RPC严格DTO并受runs capability约束。连接、断线和重启不重放搜索/执行。API见 [工具发现 API](../tool-discovery-api.md)。ACP v2 mutation不提前实现。

### RED、修复和核心测试

真实mock Provider初始缺tool_search的RED：/tmp/control-waves-wave4-red.log。中间编译/回归失败日志保留；修复测试fixture缺session创建、测试trait import和test module位置。旧审批mock扩为搜索→nested，而非放宽审批/取消/副作用断言；wire receipt断言改为明确两份tool_search且实际output/artifact仍approved、副作用仍仅1次。child原1项工具断言保留。

- `ranking_fields_chinese_english_exact_select_list_and_ties_are_stable`、`strict_dispatch_and_output_budget_never_grant_an_omitted_schema`。
- `discovery_exact_owner_generation_idempotency_cancel_rollback_and_corruption`：文件/内存CAS、跨owner不共享权限、schema/generation/denied拒绝、事务注入故障、取消屏障、恢复只读与损坏拒绝。
- `schema17_discovery_upgrade_backup_transaction_rollback_and_future_rejection`：v17备份/marker、迁移事务回滚、future19拒绝；原最旧fixture仍升级至18。
- `concurrent_queries_share_only_the_complete_snapshot_index_and_invalidate_changes`：8个并发查询共享同一index，schema/description/授权范围变化失效。
- `cancelled_blocking_worker_cannot_publish_a_discovery_after_return`：锁和channel构造worker阻塞→取消→调用返回→释放worker，无sleep；无receipt/event。
- `frozen_schema_survives_reload_and_hidden_direct_or_unowned_invokes_fail_closed`：冻结schema、子目录scope、保留名碰撞和direct/unowned拒绝。
- `deferred_tool_search_roundtrip_freezes_catalog_and_refills_same_dispatcher`：真实socket两轮search重复wire id有独立receipt→nested invoke→Provider回填；完整schema与小初始工具面；kill/restart后三入口同一发现记录且Provider调用次数不增加。
- `deferred_tools_reject_direct_unfound_fuzzy_and_invalid_nested_schema_without_execution`：真实Provider四类拒绝路径，目标无执行记录。
- 原CLI恢复/ACP permission往返/WebSocket approval与重连测试现均走搜索→nested→审批/回填，并保留原安全断言。三入口 `/tools` 对同一durable fact逐一比对。

### 本波门禁

| 命令 | 实际结果与日志 |
|---|---|
| cargo fmt --all -- --check | 退出0；/tmp/control-waves-wave4-fmt-complete.log |
| cargo test --locked --workspace --all-targets --all-features | 退出0；350项，含22项真实daemon；/tmp/control-waves-wave4-tests-exact-round.log |
| cargo clippy --locked --workspace --all-targets --all-features -- -D warnings | 退出0；/tmp/control-waves-wave4-clippy-complete.log |
| cargo +1.88.0 check --locked --workspace --all-targets --all-features | 退出0；/tmp/control-waves-wave4-msrv-complete.log |
| cargo build --locked --release | 退出0；/tmp/control-waves-wave4-release-complete.log |
| node --test web/app.test.cjs | 退出0；31项；/tmp/control-waves-wave4-web-final.log |
| node --check web/app.js | 退出0；/tmp/control-waves-wave4-jscheck-final.log |
| git diff --check | 退出0；/tmp/control-waves-wave4-diff-complete.log |
| cargo deny --offline check | 退出0；四项通过，保留既有duplicate warnings；/tmp/control-waves-wave4-deny-final.log |

供应链仅使用已有缓存，未刷新在线公告。Wave5–8未完成；商业Provider、真实MCP大目录/生产reload、Linux和GitHub Actions未验证。未提交、推送、部署。


## Wave 5 完成：共享 revision reducer 与持久版本封存（2026-10-06）

agent_core 是唯一展示时序规则；SQLite v19 event_view_stamps 是版本事实源。所有事件发布在原 mutation 事务封存最终全版本向量；读取校验 SHA/full ExactOwner，缺戳历史不追补。SessionReadback::supersedes 委托同一 reducer。run_owners additive 读取严格校验；嵌套 slash、native compact、终态和交互回执也接线。去掉 private seq 推断和入口各自比较路径。

Rust DaemonClient 各消费者独立游标，projector actor 保持 next 可取消安全；有界队列溢出 detach。Web 通过纯 views.reduce RPC 调用完全相同函数，256 帧/8MiB 有界串行队列；Gateway 只转协议。resync 先单事务 page 基线，再 32 条 durable 页补充仍活动完整 owner，ViewResynced 不是业务终态。原始审计数据保持，返回帧保留元数据剥离后按 repository 来源添加。

RED /tmp/control-waves-wave5-red.log 缺 RPC 真失败。核心 4 个领域测试覆盖乱序、同版本补充、IR 冲突、多 run、退休 life、durable replay。storage 新 3 项双后端/封存/迁移测试覆盖零写读取、损坏和 fault 回滚。真实 daemon 新 2 项 revision_reducer_rejects_delayed_readback_and_retired_lifetime_over_socket、shared_client_resyncs_real_dropped_frame_and_duplicate_without_provider_replay，后者 proxy 真丢 TextDelta/重复 Started，Provider 次数保持 1。重启 direct run.read 需要完整 source 初始化展示已修复。新增 stamp 使旧测试 helper 误判 compact 为 session，现按完整 compact DTO 逐字段比对，未放宽旧断言。Web 新 4 项连同原 31 项通过。

门禁均退出0：cargo test --locked --workspace --all-targets --all-features 359 项含24真实daemon（/tmp/control-waves-wave5-tests-final-corrected.log）；严格 Clippy（clippy-final-corrected）、MSRV1.88（msrv-final）、release（release-final）、fmt（fmt-final）、diff（diff-final）；Web35（web-final）、JS语法。cargo deny --offline check --hide-inclusion-graph 四项通过（deny-final），仅已有缓存，未在线刷新。API docs/view-reducer-api.md。

Wave6–8尚未完成；真实商业 Provider/MCP、Linux、生产慢消费者和 CI 未验证；未提交/推送/部署。


## Wave 6 完成：实际请求 capture 与零写共享重建（2026-10-06）

状态 owner 为 SQLite v20 provider_requests；保存模型/fallback/自动与手动摘要的发送前不可变材料，SHA与ContextEnvelope同事务。实际 Provider 发送与只读 context.readback 都使用 agent-context::assemble_request；删掉 loop_engine 原独立 envelope 计量与媒体降级路径。ContextManager 摘要也接入相同 assembler，避免旁路。cron 新准入保存真正冻结 RunSnapshot，旧 usage fixture 改 canonical 准入，原安全/重启/预算断言保留。完整瞬时 DTO 在 core，不建立 context 私有历史 owner；架构测试未放宽。

capture 明确是当时可重建材料，当前动态环境/skill/plan/memory不重放。默认只返回类型化 envelope/digest，显式 local 诊断保守隐藏凭据/memory/媒体/工具参数/摘要原文；权限变化或凭据不可核对拒绝。读取同一 readonly 事务核对 full owner/life、冻结政策、catalog、source prefix、projection与 SHA，无新 Provider/MCP/runtime/compact/ingest。旧历史无 capture 明确 unavailable，不补身份/空请求。CLI/TUI/ACP v1/WebSocket/HTTP共用 handler；Web 只读不造 chat/run 终态。API docs/provider-context-api.md。

真实 RED /tmp/control-waves-wave6-red.log 缺 context.readback 方法。新纯 assembler 2 项、storage 2 项双后端/迁移/fault/损坏、协议 strict DTO 1项、真实daemon 1项和Web1项；既有compact合同补实际摘要capture与下一chat generation。真实wire消息/小工具SHA与返回一致，三入口及HTTP逐字段相同，读回前后clock/event不变，kill/restart调用次数仍1，权限变化/损坏拒绝。测试 helper 识别新增 DTO，保持完整 JSON 精确比对；child16K预算与route32K取冻结有效最小值，原断言保留。中间失败日志保留，未通过降低测试换绿。

最终门禁全部退出0：Rust365（含25真实daemon）/tmp/control-waves-wave6-tests-final-complete.log；严格Clippy、MSRV1.88、release分别 clippy/msrv/release-final-complete.log；fmt/diff；Web36 web-final-complete.log、JS语法。cargo deny --offline check 四项通过 deny-final.log，仅已有缓存，未刷新在线公告。无新外部依赖，Cargo.lock未修改。Wave7–8未完成；商业Provider、真实MCP/用户hooks、Docker/Keychain、Linux与CI未验证；未提交/推送/部署。


## Wave 7 完成：真实 opt-in ACP v2 与有界传输（2026-10-07）

稳定 v1 默认路径保留，--acp-v2 使用 SDK Agent.v2/V2ConnectionTo/标准 v2 类型；物理连接一次严格协商 schema/fingerprint/capability。未协商、未知身份、版本/重复/热变更在 mutation 前拒绝。计划 Markdown/execute/discard、compact 持久 Started/真实结算/exact cancel、pending interaction 恢复、session readback/new/list/resume/close/delete 共用 daemon 命令；SDK 缺标准请求的操作采用命名空间扩展，未伪造 v1 metadata 为 v2。

prompt 的 durable Started -> accepted response -> 后台 stream 不阻塞 inbound；标准 permission popup Cancelled 不提交决策。canonical message_ids 从真实 batch/native turn 派生，readback/page/replay 一致；foreign batch fail closed。close 使用现有 lifecycle_receipts，取消 captured exact owners 并等待真实结算后发布一次 SessionEnd，保持 lifetime/transcript；重复 receipt 不取消新工作。schema 保持 v20。

v1/v2 物理预算 256 帧/8MiB、4MiB 单帧、128 tasks；v2 请求32。真实慢 stdout 合同首次 RED 发现 Tokio 阻塞 stdin 关闭问题，改为有界独立 I/O 桥接，stderr 测试夹具单独排空；budget detach 不改 canonical，不取消 run，不重放 Provider。严格默认 UI Cancelled 保留 pending。新合同包含严格协商、三入口计划、提前 compact/长 prompt/close、interaction 断线恢复和真实慢消费者。未删除/放宽既有测试。

最终门禁全部退出0：Rust376（30真实daemon）/tmp/control-waves-wave7-tests-accepted.log；严格Clippy/MSRV1.88/release 分别 clippy/msrv/release-accepted.log；fmt/JS/diff static-accepted.log；Web36 web-accepted.log；离线 cargo deny 四项通过 deny-accepted.log，仅已有公告缓存，未在线刷新。Cargo.lock 未变，无新增外部包。API docs/acp-v2-api.md。真实 IDE/商业Provider/MCP/Docker/Keychain/Linux/CI 未验证。Wave8 尚未完成；未提交/推送/部署。


## Wave 8 完成与最终验收（2026-10-07）

七个新库按 sandbox→runtime→daemon→entry-support→CLI/TUI→ACP→Gateway/Web 顺序迁移，每个均先 workspace check 与目标测试。根仅 main.rs/bootstrap.rs；旧根业务模块及 client/storage/protocol facade 全部删除。唯一 owner、实际物理边与非默认 fixture features 由 12 项架构检查保护。库 forbid unsafe，文件能力使用已锁 rustix safe fd API 保留 no-follow/目录句柄/CAS/原子 rename，环境加载在根多线程 runtime 前执行。配置/binary/socket/HTTP/WS/CLI/schema v20 保持，Cargo.lock 第三方身份/checksum 与基线完全一致。

最终审计补真实 v1 permission RED：SDK 容错解码吞入 v2 owner metadata 并错误执行文件。共享 raw Value strict decoder 在审批前拒绝根/selected 的 my-agent v2 控制字段，保留标准其他 namespace；GREEN 验证 canonical pending 不变、无写文件/Provider 重放，v2 重连可精确恢复批准。日志 /tmp/control-waves-wave8-v1-permission-red.log 与 green-fixed.log。独立 crate 的 policy SHA 序列化、safe fd、锁损坏和测试时间构造问题均修复，原行为断言保持。

修复后所有门禁退出0：Rust379（31真实daemon/入口、12架构）/tmp/control-waves-wave8-tests-audited-final.log；strict Clippy/MSRV1.88/release 对应 clippy/msrv/release-audited-final.log；fmt/JS/diff static-audited-final.log，文档收尾再检查 delivery-static.log；Web36 web-accepted.log。cargo deny --offline check --hide-inclusion-graph 四项通过 deny-accepted.log，仅已有公告缓存，未在线刷新。完整逐项 owner、迁移、删旧路径、兼容、失败、测试和实际环境边界见 docs/changes/governed-runtime-final-report.md；物理 API docs/runtime-crates.md。

八波实现与本地门禁全部完成。商业 Provider/真实 IDE v2/MCP/用户 hook/Docker成功隔离/真实Keychain/Linux/CI仍未验收；不声称这些生产环境已通过。没有提交、推送或部署。
