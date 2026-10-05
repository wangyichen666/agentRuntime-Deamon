# Runtime 主链复核与补齐记录

日期：2026-10-05。用户要求在当前仓库按 Wave 顺序实施，不搜索或依赖其他仓库。

## 基线与范围

- HEAD `35af558`；`git status --short --branch` 干净。
- 现有六个 workspace 库：core/protocol/client/storage/context/memory；runtime/daemon/sandbox/entry 仍是根 package 模块。
- `cargo test --locked --workspace --all-features` 初次因沙箱禁止本地 socket 失败（client transport 四项 EPERM）；同命令经环境审批后全绿，源码未修改。日志 `/tmp/runtime-refactor-baseline.log`。
- 先补早期 Wave 0–2 的读取、身份、协议合同，ADR 0002 在实现前完成；测试后逐项记录实际结果。

## 源码核验差距

- 读取 `session.load/load_page/resume` 会调用 session_runtime，可能兼容导入、构造 engine 并改变缓存；公开恢复快照缺 lifetime 与持久控制区。
- `session_end` 从当前 metadata 取 lifetime，迟到的删除请求可能作用于同 key 新 lifetime；lifecycle receipt 没绑定请求的 expected lifetime。
- 协议有 versioned DTO，但没有绑定物理连接的能力协商。
- Wave 5B **未实现**：plan 缺 stable identity/digest CAS、execute/discard 与 pending_execution，进度更新会推进 revision；boundary hooks 未接线。
- 手动 compact 的独立 canonical run/Started/exact cancel **未实现**，当前复用历史 owner 与 daemon shutdown token。
- runtime/daemon/sandbox 与三个入口的最终物理 crate 提取 **未实现**；现有根模块暂保留，不能声称全部目标结构已完成。
- 工具目录检索 `tool_search` **未实现**；后台 Docker、远程 OAuth 与非 macOS native secrets **未实现**。

这些差距不以 trait/TODO 掩盖。本轮交付早期恢复读取与身份屏障纵切，保留既有上下文/记忆实现；schema 升级为 v14。后续完整 Wave 验收未完成。

## 本轮 Wave 交付范围

| Wave | 本轮状态 | owner、依赖和删除的平行路径 | 迁移/兼容 | 核心证据 |
|---|---|---|---|---|
| 0 | 护栏补齐已完成 | 库生产代码 AST 禁止 unwrap/expect/stdout；提取 context 不得获得 storage/session owner。没有增加事实源。 | 无独立迁移；原架构断言保留。 | `workspace_libraries_cannot_panic_or_print_in_production`、`extracted_context_cannot_acquire_session_or_storage_owners`；10 项架构测试全绿。 |
| 1 | 连接协议与恢复 DTO 纵切已完成；全入口 live 防回退未实现 | protocol 拥有物理连接协商与严格 DTO，client 每次重连重新初始化；daemon 在 callback 调度前拒绝非法版本/schema/能力。入口 `/snapshot` 共用 daemon 投影，没有直连 store。 | 协议 v1；legacy 方法名集中映射，legacy 无版本连接保持兼容。协商后禁止降级/热变更。 | `capabilities_are_intersected_by_schema_and_bound_to_the_connection`、`malformed_initialization_does_not_publish_a_capability_snapshot`、4 项客户端 transport 测试及三入口进程合同。 |
| 2 | 只读 repository 与公共 lifetime 屏障纵切已完成 | SQLite 同事务拥有 metadata/history/control/plan/ledger。删除 load/resume/page 的 runtime 构造、缓存 active/approval 拼事实、查询时兼容导入；memory/resource 查询不再构造 runtime。destructive receipt 绑定请求 lifetime；fork 先验证源再 publication。 | v13→v14；全库 snapshot_clock、metadata_revision 和事务触发器。旧 lifetime 不变，无 transcript 双写。所有 clear/delete/fork RPC 必须传 expected_lifetime。 | 两后端合同、迁移、故障、并发读写与真实 daemon 删除重建/重启，详见下表。 |
| 3 | 既有 TurnCommit/工具闭合保持；整 wave 重新验收未完成 | 本轮读取复用原 terminal 提交，不另造 terminal owner；失败提交游标一起回滚。手动 compact 独立 owner 未实现。 | 无新增该 wave migration。 | 原工具/队列/terminal 合同继续通过；新增失败 terminal readback 回滚合同。 |
| 4 | 既有 durable compact 保持；完整四层只读恢复未实现 | Model 模式仅复用已安装 projection+suffix，不写模型历史；context storage 模块改名 `context_projection`，算法公共接口不变。 | 无新 compact 格式；历史原始证据保留。 | 原 compact CAS/lifetime/重启测试继续通过，三入口读取同一 projection generation。 |
| 5 | 既有作用域记忆及摄入保持；本轮无新模型迁移 | memory 查询按持久 metadata 授权，不靠执行缓存；不建立第二个记忆 owner。 | 保留 v13 flywheel 数据，旧版本 fixture 正确撤销 v14 后测试升级。 | 原跨 session、摄入、忘记与重启维护合同继续通过。 |
| 5B | **未实现** | 尚无 stable plan identity、digest CAS execute/discard、pending_execution/executing 或治理 hooks；只读 current_plan/digest 不授予执行身份。 | 未发布新 plan/hook schema。 | 无完成证据，不能把只读摘要称为 plan 执行合同。 |
| 6 | 既有工具/Native/Docker foreground 保持；剩余未实现 | generation-scoped tool_search、后台 Docker resources **未实现**，物理 sandbox crate 提取未实现。 | 无新增。 | 原本地工具资源测试继续通过；本轮未进行真实容器验收。 |
| 7 | 既有 MCP/secrets/doctor 保持；剩余未实现 | 远端 OAuth、非 macOS native secrets **未实现**；没有访问真实远端/Keychain。 | doctor 报 v14。 | 原本地合同保持；Linux/远端/GitHub Actions 验收未执行。 |

Wave 0–2 的这些纵切已经实际接线；不将其描述为用户全部 Wave 0–7 目标已完成。六个库之外的 runtime/daemon/sandbox/cli/acp/gateway 最终物理 crate 提取**未实现**，继续沿用根模块及禁止依赖边护栏。

## 新状态读取合同

- `SessionQuery::session_readback` 在一个 SQLite read transaction 读取 metadata/lifetime、全库 snapshot revision、metadata revision、transcript revision、projection generation、active exact owner、稳定 queue ID/position、pending interaction、最后已提交 terminal、plan/digest 和同 generation 的 context ledger。
- `snapshot_revision` 是全库单调游标；其他 session 写入也能推进，不能把它当消息数量或每 session 独立 counter。事务回滚时触发器写入也回滚，读取本身零写入。
- canonical 检查 batch digest、连续序号及消息数量；model 用 canonical snapshot 构建已安装 projection+fresh suffix，省略 stable prefix/recall/overlay；omitted 明确省略 messages/batch_ranges，不把损坏读成空历史。
- 队列最多 64、active 最多 65（一个 writer 加队列）、pending 最多 256；超预算 fail closed，不静默丢恢复控制项。最后 terminal 按持久 terminal event 提交顺序读取，不按 run 准入 generation 猜最近完成。
- `session.load_page` 只输出完整 batch，控制区来自同一次读取；分页结果显式标记 complete_messages/batch_ranges 省略，保留专用分页合同，不交给完整 snapshot 解码器。
- daemon 展示用的 active_requests/pending_approvals/status 都从该快照导出；broker 仅唤醒，broadcast/replay 仅通知，runtime 缓存不参与恢复事实判断。
- 当前 plan_digest 是兼容 plan value 的 SHA-256，只供读回比较；omitted 包含 plan_execution_identity。工具 catalog generation 与协议展示 capability_generation 不混用。

## Lifecycle 和 wire 兼容

clear/delete/fork RPC 的 expected_lifetime 必填（所有版本）；先用 sessions.read 取得身份，再带稳定 operation_id 请求。v1 非法参数在客户端发送前返回原有 `ClientError::Rpc(-32602)` 合同；服务端也在创建业务 callback 前独立校验。旧方法名仍可用，缺身份的 destructive 请求有意拒绝，不能从当前会话读一个 lifetime 来替迟到请求授权。

生命周期 CAS 与 receipt 在同一持久事务中校验；旧操作 receipt 只返回原结果，不能授权新 lifetime。旧格式 receipt 保留匹配原 lifetime 的兼容读取；相同 operation 换 lifetime 视为冲突。旧 create receipt 如已属于删除前 lifetime，会拒绝，不改 preferred session。损坏源无法 fork 出空 session。

旧 JSONL 保留并由既有 daemon 启动迁移受控导入一次，在线 transcript 不双写。只读加载不导入、不创建缺失 session、不设置 preferred session，也不启动 provider/MCP。缺失、删除、损坏都返回错误，omitted 只表示未读取消息区。

## 核心不变量与测试

| 不变量 | 实际测试 |
|---|---|
| 文件和内存 SQLite 共用 readonly/queue/terminal/lifetime/receipt 合同 | `canonical_readback_and_lifecycle_fences_share_file_and_memory_contract` |
| v13 升级保留 lifetime、历史、游标；可校验备份和 marker 存在 | `schema13_upgrade_keeps_identity_and_restarts_with_same_cursor` |
| 损坏不能变空历史、不能 fork publication；稀疏读取显式省略 | `corrupt_transcript_cannot_be_forked_or_recovered_as_empty_and_sparse_read_is_explicit` |
| terminal 失败回滚 transcript、owner 状态与 snapshot revision | `failed_terminal_rolls_back_snapshot_revision_and_pending_state` |
| 两连接并发读写不混合 turn 边界 | `simultaneous_reader_and_writer_connections_never_mix_turn_boundaries`（32 turn、128 readback） |
| readback/resume/page/memory/resource 查询零 runtime 构造、零 ownership、零写入 | `readback_does_not_construct_runtime_import_files_or_acquire_run_ownership` |
| destructive 所有版本 require lifetime，合法 alias 仍统一 | `all_destructive_mutations_require_lifetime_and_legacy_aliases_stay_centralized` |
| model-only 为纯读取；冲突/非法参数拒绝 | `model_only_read_is_an_explicit_pure_projection_and_illegal_combinations_fail` |
| response result/error 冲突、嵌套未知 DTO 字段拒绝 | `response_rejects_conflicting_success_failure_and_nested_snapshot_fields` |
| mutation→kill/restart→readback→CLI/ACP/WS 相同 durable facts | `committed_terminal_survives_real_daemon_restart_and_uncertain_run_is_not_replayed`、`compact_memory_receipts_survive_restart_and_all_three_entries` |
| 删除重建后旧 send/clear/delete/fork 零写入，重启不串旧历史 | `session_delete_recreate_and_fork_survive_restart_without_old_history_or_dual_write` |

storage 的 `context.rs` 改名 `context_projection.rs`，用于区分自身投影存储与根 context 执行算法；保留原 storage 禁止依赖根 context 的架构断言，没有放宽白名单。新增库 AST 护栏只排除显式测试 fixture，不给生产代码添加 allow。

## Migration v14

既有 migration 流程在升级前生成校验过的 v13 SQLite 备份与 `v13.backup.verified.json` marker，随后同事务新增 snapshot_clock、metadata_revision、相关表 INSERT/UPDATE/DELETE 游标触发器。未知未来 schema 继续 fail closed；旧 metadata/lifetime、transcript、plan、memory 与 audit 不改写。游标达到 SQLite 整数上限时拒绝写入，不回绕重新授权旧状态。

## 实际命令与结果

以下均在当前 macOS 工作区执行，没有网络研究、克隆或依赖其他仓库，也未改 Cargo.lock。

| 命令 | 结果 / 日志 |
|---|---|
| `cargo test --locked --workspace --all-features`（基线） | 退出 0，289 项；`/tmp/runtime-refactor-baseline.log` |
| `cargo test --locked --workspace --all-targets --all-features` | 退出 0，302 项，无失败/忽略/过滤；`/tmp/runtime-refactor-tests-final.log` |
| `cargo fmt --all -- --check` | 退出 0 |
| `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` | 退出 0；`/tmp/runtime-refactor-clippy-final.log` |
| `cargo +1.88.0 check --locked --workspace --all-targets --all-features` | 退出 0；`/tmp/runtime-refactor-msrv.log` |
| `cargo build --locked --release` | 退出 0；`/tmp/runtime-refactor-release.log` |
| `node --test web/app.test.cjs`、`node --check web/app.js` | 退出 0，27 项；`/tmp/runtime-refactor-web-final.log` |
| `cargo deny --offline check --hide-inclusion-graph` | 退出 0，advisories/bans/licenses/sources 通过；保留既有 duplicate warnings，使用缓存公告，未验证在线更新；`/tmp/runtime-refactor-deny.log` |
| `git diff --check` | 退出 0 |

测试分布：context 5、core 8+legacy DTO 3、client transport 4、protocol 3+compatibility 5+connection 5+versioned 7、memory 2、storage 48、根单元 191、架构 10、真实 daemon 11，共 302。相对基线新增 13 项，并加强原有重启/三入口断言。

验收中发现并修复：v14 残存导致降级 fixture 缺表、备份 marker 路径写错、strict RunRecord 与既有 run.read 附加 snapshot 兼容、客户端本地校验改变原 RPC 错误合同。修复实现与 fixture 后全量通过；没有删除测试、放宽业务断言、跳过测试或新增生产 allow。Socket 测试沙箱 EPERM、离线公告锁路径只读是环境限制，经环境审批重跑相同命令通过。

## 未实现项与风险

- 完整 Wave 0–7 验收**未完成**；本轮优先交付早期闭合纵切，没有新增后续 TODO/空接口或双写。
- Wave 5B 的版本化 plan 身份、digest CAS、execute/discard、pending_execution/executing 与 Session/Compact/Tool/Turn hooks **未实现**。
- 手动 compact 的独立 canonical Started/terminal/exact cancel **未实现**；仍沿用历史 run owner，不承诺该路径已经满足新完整生命周期要求。
- 所有入口 live event 与 readback 的统一 revision 防回退 **未实现**；`supersedes` 仅提供领域比较，当前恢复入口使用持久事实但未接统一 live reducer。
- `/context` 的完整请求展示仍沿用既有路径；新 Model readback 不是完整 Provider 请求确定性重建证据。完整四层只读恢复、全部 public DTO 深层扩展 schema 验收**未实现**。
- `tool_search`、后台 Docker resource、远端 OAuth、非 macOS native secrets、剩余物理 crate 提取 **未实现**。
- 本轮未执行真实 Docker 容器、真实远端 Provider/MCP、真实 Keychain、Linux 和 GitHub Actions 验收。静态架构护栏不能代替这些集成证据。

验收完成后，用户已授权提交并推送云端；使用当前 main 分支常规推送至 origin，不强制覆盖远端。没有部署。原历史实施记录保留，但其“全部 Wave 完成”的结论被本次源码核验更正。
