# 新会话接续提示词（2026-10-09）

你正在接续 agent-rust 的企业化开发。本次任务范围是从当前工作树持续实现原规格的全部剩余功能，直到 Wave10，不是只完成下一切片或下一 Wave。请直接阅读现有改动并继续实施，不要只给方案。全程使用中文。

## 工作区和规格

- 仓库：`/Users/pilot/Documents/myproject/agent-rust`
- 原始 Wave0–10 完整规格：`/Users/pilot/.codex/attachments/a3ff7a36-f469-4862-8b0d-7d2fa1d8ae1c/已粘贴的文本.txt`
- 当前进度：`docs/enterprise/current-progress.md`
- 最新实施报告：`docs/enterprise/wave2-native-runtime-report.md`
- 最新局部验证：`docs/enterprise/wave2-native-runtime-smoke.json`
- 工作记录：`task_plan.md`、`findings.md`、`progress.md`
- 阅读适用的 AGENTS.md、ARCHITECTURE.md 和相关 ADR。

当前工作树有大量未提交改动，包含用户原有开发和前会话的实现。先看 git status/diff，再核对调用链；不要 reset、覆盖、清理已有改动或重复造轮子。未经明确要求，不提交、推送、部署。历史基线为 0af445b1453b0adfad19802cc519624f1ebb29e8，实际状态以工作树为准。

## 已完成到哪里

- Wave0 已有本地基线与架构验收记录，不能当作生产发布验收。
- Wave1 部分实现：Local 身份、冻结授权、scope/fence、独立 Cron workload 已接线；完整租户管理、双人审批等仍欠。
- Wave2 进行中：SQLite 保留默认；PostgreSQL 已实现并装配 daemon 所需全部 26 个领域端口，实际本机 Local 启动、会话、chat、读回、重启幂等已跑通。不能因此称企业多租户后端已完成。
- Wave3–10 尚未完整开展，不得宣称 Wave0–10 全部完成。S15/S25/S43 等是 Wave2 内部批次，不是 Wave15/Wave25/Wave43。

最近新增了 PG 资源、计划、诊断、Stop 续跑、记忆、委派、维护与恢复实现。子运行准入、终态、事件/stamp/outbox 复用原 canonical 事务；记忆遗忘后保留反馈幂等证据；恢复不重放未知副作用；发布和文件回收协调且受管排空。SQLite/PG 共用纯规则，未另建业务状态机。

PG canonical schema 当前为 v2，coordination 为 v1。保留 v1 校验和，通过 v2 迁移升级。daemon 的 `RuntimeBackend` 只是同一 adapter 的端口集合；默认 sqlite，显式 postgres-local 通过 /tmp Unix socket 连接，需要 AGENT_PUBLICATION_DB/USER/SCHEMA，PG 强制 session writer。仅空 Local 域初始化主体；PG artifact 目录按 database/schema 隔离。这还不是远程 TLS、OIDC、企业租户路由，也不是 SQLite 全量迁移。

关键修复：异步请求端口也核验冻结 token 预算；trace 同时接受旧 32 位 lifetime 和 UUID；chat 执行与事件消费用 tokio::join! 同时驱动，避免事件等待 PG 连接时执行事务停止轮询的闭环，排空事件后再提交终态。请保留这些语义。

## 最新验证与限制

生产局部 cargo check、my-agent binary build、daemon test-support 测试源码编译通过。真实 PostgreSQL 18.6 的 native_memory_delegation_recovery 和 resource_lifecycle_scope_logs_and_transaction_rollback 两个局部合同通过。scripts/smoke-local-backends.py 对 SQLite、postgres-local 实际 binary 的启动、chat、优雅停止、重启去重均通过；每后端本机假模型只调用一次，PG 未创建 SQLite 事实库。临时测试库和工作区已清理。

未跑当前树全量测试、Clippy、release、MSRV、完整门禁；旧 S25 全量结果不能证明现在的树。enterprise_ready=false。局部冒烟不代表真实 S3、企业 HA、多租户隔离、OIDC 或远程 worker 全流程验收。

## 接下来继续实现

先复核源码确认缺口，围绕真实入口做完整纵向切片，补齐 Wave1/Wave2 的必要基础，再依原规格推进 Wave3–10。Wave2 已确认剩余：

1. 远程 PostgreSQL TLS 和可靠的连接配置。
2. 企业租户/项目/Agent 的启动与路由，保留 Local 默认体验。
3. SQLite audit/quota 对等与真实配额消费，接入实际准入/执行/结算链路。
4. 可校验、可回滚的 SQLite→PostgreSQL 离线迁移，覆盖原 canonical 事实及 artifact 引用；不得把 JSONL 兼容导入视为全量迁移。
5. 两后端完整共享合同及后续集中验收所需证据。

以上按依赖和实际源码调整顺序，已经实现的部分复用。后续严格遵守原规格的身份、安全和事实源不变量：不新建第二套 run/session/approval/plan/MCP/cron；所有 mutation 在原事务核对 scope、ExactOwner、lifetime、generation、revision、writer 和冻结授权；未知副作用不得自动重放；断连不伪造业务终态；队列有界；入口不得直连具体存储。

## 开发方式：用户最新要求优先

我要求敏捷开发，不走完整测试和门禁。原规格“每个 Wave 全部门禁通过才进入下一 Wave”已由本要求覆盖。只做本次变更必要的编译和有针对性的局部验证；不反复跑全量套件，不因未跑完整门禁停止开发。不要降低授权、隔离、事务和迁移安全要求；区分“已实现”“局部验证通过”“完整验收未做”。

持续写实际代码、数据库迁移和入口接线，不以空 trait、TODO、独立但未接线的模块或仅文档代替完成。及时更新进度与真实验证证据，不改写历史验收为当前已通过。不要停下来反复问是否继续，也不要未经要求创建其他会话或启动子 agent。

## 持续推进到 Wave10 的执行要求

1. 原始规格全文是功能清单。先核对当前实现，补齐 Wave1/Wave2 遗漏，再按依赖推进 Wave3、4、5、6、7、8、9、10，不能把本文件列出的 Wave2 剩余项当作任务终点。
2. 一个切片或 Wave 完成后，直接进入下一项；阶段汇报使用进度消息，不以最终答复结束任务，不询问“是否继续”，不等待下一条“继续开发”。常规实现、可逆修改、数据库迁移文件、配置、必要依赖和局部验证均已授权。
3. 大任务自行拆解、排序、处理编译错误与实现冲突。依赖尚未具备时先实现其基础，再恢复被依赖功能；不要仅列出缺口便停止。每项功能必须接入真实入口与 canonical 调用链。
4. 保留 Local 产品和现有安全语义。敏捷意味着省略开发过程中完整验收，不意味着跳过身份隔离、授权、writer fence、事务、幂等、未知副作用收敛或回滚能力。
5. Wave10 的发布/供应链/质量能力和自动化配置也要实现，但本次不要求运行完整门禁，更不授权生产部署或发布；未实际执行的验收必须如实标为待验收。缺少真实集群或外部账号时，完成可实现的代码、配置与本机验证，继续其他工作，不伪造外部结果。
6. 持续维护逐 Wave 的“已实现/部分实现/缺失/待验收”清单。上下文压缩前写清已完成内容、未完成内容和下一步，压缩后接着执行，不重新开始。只有用户明确要求停止，或遇到必须由用户提供且无法绕过的信息/授权时，才中断相应工作；其他独立工作继续推进。
7. 最终答复应在 Wave0–10 的功能清单逐项核对后给出。任何仍缺失的功能都不能标为完成；如果执行环境强制中断，先保存可继续的具体进度，并如实说明未完成项。

现在先阅读上述文件、原始完整规格和当前代码改动，用简短中文说明确认的现状，然后立即开始并持续开发到 Wave10。
