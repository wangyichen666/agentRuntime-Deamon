# Runtime 控制面八波最终交付报告

完成日期：2026-10-07。基线 HEAD：`a8ac3dcbbdd9f77f4988a5359d03c201e4df9f2e`，开始时工作区干净，基线 302 项 Rust 通过。本次八波均已实现并通过各自本地门禁；最终为 379 项 Rust（31 项真实 daemon/入口合同、12 项架构检查）与 36 项 Web。没有提交、推送或部署。

逐波 RED/GREEN、故障诊断、门禁及当时剩余范围保留在 [实施记录](governed-runtime-controls.md)；所有权、时序、回滚决策见 [ADR 0003](../adr/0003-governed-runtime-controls.md)。历史记录里的旧路径和“后续未完成”描述保留原时间含义，以本报告为当前状态。

## 逐项实现、owner、迁移与兼容

| 波次 | 状态 owner 与真实接线 | 迁移、删除或替换的旧路径 | 兼容与失败语义 |
| --- | --- | --- | --- |
| 1 版本化计划 | SQLite `session_plans`、`plan_versions`、`plan_decisions` 与 legacy evidence；core 同源定义摘要/Markdown，daemon 为唯一 execute/discard command owner，CLI/ACP/Web/HTTP 同 RPC | v14→v15；替换进度更新制造新定义版本的路径，删除无 owner 的生产 `PlanStore` 本地文件写入 fallback | 旧无身份计划仅只读；exact digest/lifetime/revision/operation 必须匹配。确认先 pending_execution，取得 writer 后 executing；重启不自动执行，必须明确同摘要继续或废弃。artifact/terminal 发布失败全事务回滚 |
| 2 hooks/Stop | SQLite hook outcomes/publication outbox/continuation linkage；runtime 单一 HookDispatcher，daemon 准入独立 continuation run/turn | v15→v16；16 类类型化事件接入既有工具/审批/compact/生命周期路径，脚本无入口私有执行器 | 默认禁用，受信私有配置、no-follow、清空环境、有界输出/超时/进程组清理。观察失败审计后继续，控制失败拒绝；Start 仅真实 create/fork，End 按 lifetime exactly once；Stop 不在已 terminal run 上续写，受 8 轮/8192 tokens/8 工具/30 秒限制，未知执行不重放 |
| 3 独立 compact | SQLite native run/owner/compact linkage/receipt；daemon 共用 session writer，先持久 Started，再后台摘要；projection、receipt、terminal 原子结算 | v16→v17；删除手动 compact 借历史聊天 owner 的路径，替换 unary 内执行摘要为共享 native 操作 | 旧 unary 等待同一 receipt；v1 `/run` 只读，v2/CLI/Web 显示同一 native Started/terminal。source CAS、exact run cancel、operation 幂等；no_gain/rejected/failed/cancelled/unknown 不伪装成功。重启 Started 收敛 unknown，无自动 Provider 重放 |
| 4 tool_search | SQLite discovery receipts；每 run 冻结完整授权 catalog SHA，core 字段检索，runtime blocking index/稳定 dispatcher，原 schema/effect/resource/safety/审批链 | v17→v18；Provider 每次完整大目录注入替换为小核心工具面加 `tool_search`；不复制 MCP/子 Agent/内置授权规则 | exact select、bounded list/query，中文 uni/bigram、稳定同分排序；8 项/16KiB 结果、8192 项/16MiB catalog、16 索引。冻结授权、schema/generation 变化失效；取消 worker 不发布。未发现、越权、隐藏 direct、模糊副作用调用均拒绝 |
| 5 共享 reducer | SQLite event_view_stamps 封存版本事实；core 唯一 `reduce_view`，DaemonClient 独立 bounded actor；Web 纯 `views.reduce` RPC 同函数 | v18→v19；删除各入口 private seq/新旧比较路径，`supersedes` 委托共享 reducer；原事件/审计内容不改写 | lifetime/revision/projection/run/seq/interaction 全向量；旧 incarnation、重复、乱序拒绝或幂等忽略。同 revision 只接受有证据补充。gap 先单事务基线再 durable cursor；慢外部消费者 detach，不产生业务 terminal，不重放 Provider |
| 6 完整请求重建 | SQLite v20 `provider_requests` 不可变 capture；agent-context 唯一纯 assembler 同时供实际模型/摘要发送和只读重建；daemon 只读事务核对 owner/source/catalog/policy/SHA | v19→v20；删除 loop_engine 独立 envelope 计量/媒体降级拼接；自动/手动摘要与 cron 真实 frozen admission 接入同一链路 | 默认仅 envelope/摘要；显式本地诊断保守脱敏，隐藏 memory/凭据/工具参数/媒体，当前权限变化拒绝。动态环境标 non-replayable，旧无 capture 明确 unavailable；损坏 fail closed。零写、零 runtime/Provider/MCP/compact/ingest |
| 7 ACP v2 | agent-acp 真实 SDK v2 适配，protocol 严格 schema/fingerprint/不可变连接能力；业务 owner 全部复用 daemon/SQLite；canonical message ID 来自 batch/native turn | 无新 schema，复用 v20 与现有 lifecycle_receipts；替换阻塞 prompt dispatch 和无界 stdio 路径；close 复用 exact owner 结算与 outbox | `editor` 默认 v1，`editor --acp-v2` 显式 opt-in；未协商/未知身份/热变更在 mutation 前拒绝。标准 Markdown/compaction/state/permission 投影，SDK 缺标准 execute/discard/start 使用命名空间扩展。断线/popup Cancelled 仅 detach/无决策；close 等待真实 terminal，保留历史/lifetime；重复 receipt 不取消新 run |
| 8 物理拆分 | 根仅 main/bootstrap 组合；runtime、daemon、sandbox、CLI/TUI、ACP、Gateway/Web 与 entry-support 各为真实 crate；共享类型/SQLite owner 各仅一份 | 无 schema 变更；顺序迁移而非复制，每个新 crate 单独 workspace check+目标测试。删除全部旧根业务模块及 `src/client.rs`、`src/storage/mod.rs`、`src/daemon/protocol.rs` facade | binary/config/socket/HTTP/WS/CLI 保持；入口无 concrete storage/runtime 生产依赖，runtime 不反向依赖 daemon/入口，client 不编译 server。已锁 rustix safe fd/kill API 保持路径/权限/CAS；库 forbid unsafe，环境加载在根多线程启动前 |

最终 workspace 为根 binary 加 13 个库。具体依赖和组合端口调用链见 [crate 边界](../runtime-crates.md)。迁移前沿用可校验备份与 marker，最旧受支持 v1 fixture→v20、每次专项迁移的故障回滚/重启/未来 schema 拒绝均通过。持久 schema 不倒退；回滚使用已验证备份和匹配版本，而非在已升级数据库上运行旧 binary。没有恢复 JSONL 双写。

## 删除的根源码与当前实际实现

| 旧路径（均已删除） | 当前唯一实现 |
| --- | --- |
| `src/loop_engine.rs`、`context.rs`、`plan.rs`、`session.rs`、`config.rs`、`cron.rs`、`memory.rs`、`skills.rs`、`mcp.rs`、`mcp_http.rs`、`secrets.rs`、`safety*`、`provider*`、`tools*`、`tool_calls.rs` | [crates/runtime/src](../../crates/runtime/src)；其中 sandbox 与取消端口迁到 [crates/sandbox/src/lib.rs](../../crates/sandbox/src/lib.rs) |
| `src/daemon/`、`src/maintenance.rs` | [crates/daemon/src](../../crates/daemon/src)；严格协议仅在 agent-daemon-protocol |
| `src/entry/cli.rs`、`tui*` 与原 main 的 Clap/命令分派 | [crates/cli/src](../../crates/cli/src) |
| `src/entry/editor.rs` 与后续 v2/传输实现 | [crates/acp/src](../../crates/acp/src) |
| `src/entry/serve.rs` | [crates/gateway/src/serve.rs](../../crates/gateway/src/serve.rs)；静态 `web/` 路径保留 |
| `src/entry/recovery.rs`、`web.rs` 与共享 RPC helper | [crates/entry-support/src](../../crates/entry-support/src) |
| `src/slash.rs` | [crates/daemon-protocol/src/slash.rs](../../crates/daemon-protocol/src/slash.rs)，解析/定义唯一 |
| `src/client.rs`、`src/storage/mod.rs`、`src/daemon/protocol.rs` | facade 全部删除，直接使用已有 client/storage/protocol crate |

根 [main.rs](../../src/main.rs) 为 19 行，仅建立配置、tracing、Tokio 与调用命令；[bootstrap.rs](../../src/bootstrap.rs) 完整实现 CommandHost/GatewayHost 的启动组合。端口有真实调用和实现，不保存 plan/session/run 第二事实。迁移保留既有工具 metadata 预算与 OpenAI 回归改动；没有覆盖并行工作留下的评测材料。

## 测试证据

下列是实际全量运行中的代表合同；完整测试名和结果见最终测试日志，逐波 RED 与中间失败日志见实施记录。没有删除或放宽旧业务断言。

| 波次 | 代表测试名与覆盖 |
| --- | --- |
| 1 | `plan_decisions_are_exact_idempotent_and_share_file_and_memory_contract`；`pending_plan_survives_restart_without_automatic_execution_and_old_plan_stays_readonly`；`markdown_publication_fault_rolls_back_plan_terminal_and_revision`；真实三入口 `versioned_plan_readback_execute_discard_and_restart_share_daemon_facts` 与 `restarted_pending_plan_can_be_discarded_by_exact_identity_and_release_its_queue` |
| 2 | `hook_claim_effect_fence_recovery_and_lifecycle_outbox_share_both_backends`；`hook_publication_failure_rolls_back_the_lifecycle_receipt_and_head`；`governed_session_hooks_follow_publication_once_and_survive_restart`；`automatic_compact_hooks_follow_real_intents_and_settlements`；`manual_compact_hooks_publish_no_gain_rejection_and_summary_failure_after_settlement`；`stop_continuation_has_a_new_native_run_and_exact_cancel_preserves_parent_terminal`；`stop_continuation_crash_preserves_unknown_child_without_replaying_hook_or_provider` |
| 3 | `native_no_gain_cancel_failure_recovery_and_corruption_fail_closed`；`schema16_compact_upgrade_has_verified_backup_rollback_and_future_rejection`；真实 socket `manual_compact_has_immediate_native_started_exact_cancel_and_unknown_restart` |
| 4 | `ranking_fields_chinese_english_exact_select_list_and_ties_are_stable`；`discovery_exact_owner_generation_idempotency_cancel_rollback_and_corruption`；`cancelled_blocking_worker_cannot_publish_a_discovery_after_return`；`deferred_tool_search_roundtrip_freezes_catalog_and_refills_same_dispatcher`；`deferred_tools_reject_direct_unfound_fuzzy_and_invalid_nested_schema_without_execution` |
| 5 | `schema18_stamp_upgrade_keeps_unversioned_history_and_rolls_back_on_fault`；`revision_reducer_rejects_delayed_readback_and_retired_lifetime_over_socket`；实际 proxy 丢帧/重复 `shared_client_resyncs_real_dropped_frame_and_duplicate_without_provider_replay`；Web 乱序/reconnect/resync 合同 |
| 6 | `schema19_capture_migration_keeps_legacy_unavailable_and_rolls_back_on_fault`；`provider_context_readback_rebuilds_captured_request_without_replay_or_writes`：actual wire SHA、HTTP/三入口元数据相同，读取 clock/event 不变，重启 Provider 次数不变，损坏/权限变化拒绝；原 compact 合同追加实际 summary capture/新 generation 对齐 |
| 7 | `acp_v2_stdio_negotiates_strict_connection_before_shared_readback`；`acp_v2_long_prompt_and_native_compact_allow_exact_control_without_private_terminal`；`acp_v2_plan_controls_and_v1_reject_metadata_share_canonical_decisions`；`acp_v2_pending_interaction_reconnect_does_not_cancel_or_replay_provider`；`acp_slow_stdio_detaches_without_settling_native_run`；`close_receipts_preserve_lifetime_history_and_are_idempotent_on_both_backends` |
| 8/最终审计 | `physical_runtime_and_adapter_crates_have_real_implementations`；`root_is_composition_only_and_test_support_is_not_a_production_bypass`，连同原 10 项加强后的物理依赖/唯一 owner/生产 AST 检查；真实 v1 权限回应 `acp_v1_permission_rejects_v2_identity_before_approval_and_v2_can_recover` |

### 最终审计发现与修复

物理拆分先后通过 sandbox 1（加 runtime exec 5）、runtime 128、daemon 27、entry-support 2、CLI/TUI 29（原 main 日志测试迁入后 30）、ACP 5、Gateway 7 项目标测试。独立 crate 编译发现 serde feature 合并会改变 Value 的对象序列化顺序，冻结政策 SHA 改为直接序列化同一类型化 RunSnapshot，与 repository 完全一致。safe fd 重构保持既有符号链接/硬链接/目录句柄/内容 CAS 断言；CircuitBreaker 锁损坏改为保守 Open，原 half-open 测试用明确设置状态时间构造，不以固定 sleep 证明并发。

最后对 SDK 容错解码的审计发现：v1 的 permission response 会吞掉 v2 `_meta.my-agent` 身份。真实 stdio RED 确实错误执行了文件写入（`/tmp/control-waves-wave8-v1-permission-red.log`）。已通过原始 Value 的共享严格 permission decoder 在审批前拒绝；标准其他命名空间 metadata 仍兼容。GREEN 验证 canonical pending 整体不变、文件未写、Provider 次数不增，并在断线后 v2 明确恢复批准且仅写一次。此项是 v1 不接受 v2 身份的不变量修复，不新增独立业务状态；完整最终门禁在修复后重新执行。

## 最终命令结果

全部退出 0。日志为本机 `/tmp` 实际执行记录，不是 CI 或真实商业服务结果。

| 命令 | 实际结果/日志 |
| --- | --- |
| `cargo fmt --all -- --check` | 通过；`/tmp/control-waves-wave8-static-audited-final.log`；最终文档更新后再检查，`/tmp/control-waves-wave8-delivery-static.log` |
| `cargo test --locked --workspace --all-targets --all-features` | 379 通过、0 失败、0 忽略；其中 31 真实 daemon/入口、12 架构；`/tmp/control-waves-wave8-tests-audited-final.log` |
| `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` | 通过；`/tmp/control-waves-wave8-clippy-audited-final.log` |
| `cargo +1.88.0 check --locked --workspace --all-targets --all-features` | 通过；`/tmp/control-waves-wave8-msrv-audited-final.log` |
| `cargo build --locked --release` | 通过；`/tmp/control-waves-wave8-release-audited-final.log` |
| `node --test web/app.test.cjs` | 36 通过、0 失败；`/tmp/control-waves-wave8-web-accepted.log`；后续审计未改 Web 源码 |
| `node --check web/app.js`、`git diff --check` | 通过；`/tmp/control-waves-wave8-static-audited-final.log`；最终交付再检查同 delivery-static 日志 |
| `cargo deny --offline check --hide-inclusion-graph` | advisories/bans/licenses/sources 均通过，既有 duplicate warnings 保留；`/tmp/control-waves-wave8-deny-accepted.log` |

供应链仅使用已缓存公告，没有在线公告刷新。Cargo.lock 只增加七个内部 package 与依赖边；第三方 name/version/source/checksum 集合与 HEAD 完全一致，没有新增第三方包或升级版本。

## 不变量最终核对与真实环境边界

- SQLite 是业务事实源；runtime 能力/cache、client reducer、Web 展示和 adapter host 不保存第二份持久 plan/run/session。入口生产 manifest/AST 禁止 concrete storage/runtime；daemon-client 正常依赖图不编译服务端。
- 业务终态来自 native 结算；elapsed、ACK、断线、popup 消失、readback 失败与 ViewResynced 不产生批准/取消/完成或释放 gate。未知副作用/Provider/hook 不重放。
- transcript 与审计保留；compact 只替换模型投影。readback、diagnostic 与恢复不自动切换 preferred session、不造 run 或写空上下文。
- 新库 forbid unsafe；生产 AST 检查 unwrap/expect/print/unsafe，fixture feature 非默认且只有明确测试支持条件可豁免。真实实现扫描没有 TODO、todo!、unimplemented!；CommandHost/GatewayHost 有实现与调用链。
- 默认请求诊断只返摘要；本地完整诊断必须通过权限核对且保守脱敏。hook payload/route snapshot 不包含 Provider key/bearer；没有为诊断重取未授权 memory。
- 外部慢消费者预算覆盖 DaemonClient/Web/ACP 传输；已有内部 Provider 事件通道不是本报告“所有队列均有界”的保证。实际慢 ACP 输出 detach 保留 native run，durable cursor 可恢复。

仍未验证：商业 Provider/tokenizer 与生产凭据组合、真实 IDE v2 互操作、外部 MCP/OAuth/大型目录负载、真实用户 hook/续跑生产负载、Docker 容器隔离成功路径与 Keychain 真实读写、Linux/跨平台生产压力及 GitHub Actions。Native 仍是同 UID 软边界，Docker 后台强隔离尚未实现并 fail closed；Docker 成功路径需要预装镜像，不自动 pull。这些均保留在 [已知问题](../known-issues.md)，没有把本机 mock 合同记成真实环境验收。
