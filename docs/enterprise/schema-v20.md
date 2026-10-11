# SQLite v20：逐表事实与事务边界

依据：`crates/storage/src/lib.rs::open` 中 v1–v5 DDL 与同目录 v6–v20 migration SQL，当前 repository 实现。共 **48 张最终表**；v2 临时重命名的 tool_executions_v1 已删除，不计入。表名存在只证明 schema，能力接线另见 [baseline.md](baseline.md)。

| 表 | 领域职责 | mutation owner / 事务 |
|---|---|---|
| schema_migrations | 向前版本/应用时间 | open，每次 migration BEGIN IMMEDIATE/COMMIT |
| sessions | Local session 注册/创建时间 | SessionLifecycle；不保存 transcript |
| session_heads | 当前 lifetime、删除标记、transcript/projection/run generations | sessions.rs，创建/lifecycle/admission/append 同事务 CAS |
| session_tombstones | 退役 lifetime 证据 | lifecycle transaction；迟到写入不能复用 |
| lifecycle_receipts | operation/payload/result 幂等决策 | execute_lifecycle 同事务校验操作和 expected lifetime |
| runtime_settings | 当前 preferred session 等本地控制设置 | sessions repository；不是每客户端影子 session |
| transcript_batches | canonical append-only 消息、operation、seq、digest | append_transcript / turn commit；完整工具交换 batch |
| runs | native admission/status/terminal/result/request identity | admit_run / finish_owned / recover；同事务 facts+events |
| turns | 与 native run 唯一对应的 turn identity/status | admission / native settlement |
| queued_messages | FIFO 准入队列及关联 run | admit/start/remove/settle 同一 owner；断线不移除 |
| events | run-scoped durable cursor 与事件数据 | insert_event 位于业务 mutation transaction；非通用 outbox |
| interactions | owner、payload、pending/answer/orphan、revision | claim_interaction owner+revision CAS；broker 后唤醒 |
| tool_executions | prepared/start/terminal receipt/effect/unknown/artifact ref | prepare_owned_tool_batch/start_tool/finish_tool + triggers |
| tool_batches | 完整 calls 与 prepared/closed batch | TurnRepository；整批 fail closed / recovery unknown |
| run_routes | 兼容冻结 Provider route | admission 同事务；不解析长期 secret 写入 |
| provider_attempts | 每轮/候选 attempt 与 usage/retry 证据 | start/finish_provider_attempt + owner trigger |
| delegations | parent/root/child native identity、预算、reservation/CAS | delegation.rs；spawn admission/consume/cancel/settle 同事务 |
| run_snapshots | 不可变准入 tools/route/policy/backend/config snapshot | admit_run 同事务；排队恢复不取新配置 |
| turn_outputs | 已 staged assistant output | stage_assistant / commit_output_in；非第二 transcript |
| turn_commits | 已结算 owner/status/usage/plan revision | commit_turn / finish_owned；与输出、plan、terminal 一致 |
| legacy_plan_imports | 旧 plan 文件已导入来源/digest/lifetime | 一次兼容导入 transaction |
| session_plans | current Local plan 投影 | stage/publish；由已提交 plan 决定 |
| turn_plan_stages | 执行体待提交的 plan stage | stage_plan → commit turn CAS；失败不发布 |
| plan_legacy_evidence | 旧计划只读证据 | v15 前进迁移；旧数据不猜可执行身份 |
| plan_versions | immutable definition/version/progress/document | plans.rs stage/publish，definition/progress 分离 |
| plan_decisions | execute/discard exact identity 与幂等 receipt | 同一准入/取消事务；不由入口确认弹窗决定 |
| compact_operations | compact intent/source/state/result generation/digest | begin_compact / settle_compact；prefix/generation/policy CAS |
| context_heads | 当前模型投影/闭合 prefix/generation | settle_compact 单事务安装；不覆写 transcript |
| context_ledgers | run/round 实际上下文 envelope | record_context；与 request capture 同事务 |
| compact_run_links | 手动 compact native run→operation/source | CompactRunRepository admission/settle；不能 chat.finish 旁路 |
| provider_requests | 每次发送前不可变 capture 和 SHA | RequestRepository::record_provider_request；读回验 owner/source/冻结 snapshot |
| maintenance_diagnostics | run/kind 恢复/维护诊断 | maintenance transaction；诊断不能推出成功 |
| memories | scoped、source/revision、内容 | MemoryRepository；lifetime/source/trust CAS |
| memory_legacy_imports | 已导入旧 memory 来源摘要 | 一次受控导入；legacy 不自动获得企业权限 |
| memory_ingests | 每 run 摄入状态/诊断 | 已提交 turn 的维护工作；失效 owner 零写入 |
| memory_forget_receipts | forget operation/revision 结果 | memory CAS；幂等清理曝光相关证据 |
| memory_exposures | recall 暴露与 usage 引用 | FlywheelRepository/维护；不是 canonical usage 回写 |
| memory_assessments | memory 有用性评估 | flywheel 事务、source/revision 校验 |
| memory_feedback_receipts | feedback 幂等摘要/结果 | flywheel CAS；不凭入口临时状态摄入 |
| memory_ingest_retries | 有界维护 retry 的次数与到期 | maintenance；不重试未知外部副作用 |
| resources | 后台 workload owner/state/identity/backend/log cursor | ResourceRepository；update/reconcile exact owner 与 terminal evidence |
| resource_logs | resource-scoped log cursor/chunk | append_resource_log；分页查询，不猜 PID 身份 |
| hook_outcomes | hook prepared/running/terminal/unknown 与 effect | HookRepository；执行体 claim/result fence；重启 unknown 不重放 |
| hook_publications | lifecycle 已发布、未派发事件 | lifecycle 同事务；仅 Hooks 的有限 outbox，不是全局企业 outbox |
| hook_continuations | parent terminal→新 native continuation child | 原 coordinator admission；一次发布、预算与权限冻结 |
| snapshot_clock | DB-wide monotonic readback revision | triggers + views::commit；rollback 不发布游标 |
| event_view_stamps | mutation 所在版本向量/full owner/SHA | commit 时封存；重放不改历史版本，不可用于授权 |
| tool_discoveries | deferred lookup operation/receipt/digest | DiscoveryRepository exact run/round/catalog fence；结果不另建工具 owner |

## 事务与恢复

每个 `RunStore` 一个 `Mutex<Connection>`；多数写操作在该锁内开启 rusqlite transaction。会话 lane/control barrier → SQLite 事务 → owner/lifetime/revision 校验 → facts/receipts/event stamp → commit → 事件广播/唤醒；网络 I/O/Provider/工具/Hook 不在 DB transaction 内执行。`views::commit` 最终封存有版本事件；只读 readback 一个 read transaction，不能跨多个即时查询拼接快照。

`session_incarnation/run_incarnation/interaction_incarnation/tool_incarnation/delegation_incarnation` triggers 分配并绑定身份，`run_lifetime_fence` 与各 facts owner triggers 防旧 lifetime mutation。`fence_in` 还核对 key/lifetime/run/generation/turn。计划/compact/provider capture/resource 有各自 revision/source/digest CAS。**这些约束没有 tenant scope/policy snapshot/跨进程 lease**；不能拿 Local writer Mutex 作 Enterprise fencing token。

recover transaction：active run→unknown_after_restart，未闭合工具交换用 typed unknown 关闭；pending approval→orphaned；资源→orphaned；未安装 compact 拒绝；hook 不重放；queued 可恢复。人工 reconcile 必须携带证据和 exact owner。断线、ACK、时间经过不能替代该事务。

## 迁移/备份/未来版本

open 启用 WAL、foreign_keys、5 秒 busy_timeout；读取 MAX(schema_migrations)。版本 >20 立即拒绝业务打开；版本 1–19 在修改 schema 前执行 `VACUUM INTO`，只读 `PRAGMA integrity_check`、migration version、整文件 SHA-256 和 verified marker，随后逐波 BEGIN IMMEDIATE 前进升级。最初版本 0 创建新库；旧 JSONL 受控一次导入，原字节证据保留，不再在线双写。

故障测试包括 v1→当前、v13 readback、v14 plan、v15 hook、v16 native compact、v18 stamps、v19 capture migration，以及 publication fault rollback、两连接 reader/writer barrier。测试名单见 [索引](baseline-index.md)。本轮不迁移业务 DB；不能对升级后的 DB 用旧 binary 降级运行。未来 Wave 1+ 新 schema 的发布须声明 expand/migrate/contract 窗口与离线备份回滚。

## 非 SQLite 管理事实

`cron.json` 仍是 CronStore 的作业定义/运行历史事实；mcp.json、Provider 配置、Skill Markdown 和 Hooks 配置仍为本地配置。artifact 大内容由 filesystem 保存，tool_executions/artifact_ref 是引用，没有单独 artifacts 表、tenant capability 或两阶段原子发布。企业化须迁移并复用现有 manager/repository owner，不能宣称本基线已全部统一，也不能长期双写。
