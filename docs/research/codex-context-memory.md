# Codex 对比：上下文与记忆

研究日期：2026-10-02。参考官方仓库提交 `8d44977aa2fb9ae1b128660668dc5b36966613fa`；研究源码，不编译或执行外部项目，不复制其业务代码。当前项目基线为 Wave 0–7 工作区、SQLite v12。

## 比较与取舍

| 链路 | Codex 的做法与源码证据 | 本项目现状 | 决策 |
| --- | --- | --- | --- |
| 历史与窗口 | [history.rs](https://github.com/openai/codex/blob/8d44977aa2fb9ae1b128660668dc5b36966613fa/codex-rs/core/src/context_manager/history.rs)：模型窗口与 retained_context 分开，窗口带 revision | SQLite transcript 为事实，context_heads 为投影，source digest/generation CAS 防止覆盖并发追加 | 保留本项目 owner/CAS；不引入第二份内存历史 |
| 压缩后的用户意图 | [compact.rs](https://github.com/openai/codex/blob/8d44977aa2fb9ae1b128660668dc5b36966613fa/codex-rs/core/src/compact.rs) 的 build_compacted_history_with_limit 按独立预算保留用户原文，并处理被截断来源 | 固定 recent_messages，仅对齐工具结果边界；长工具轮次可能把最新用户输入交给摘要模型 | 保护当前轮次；以小额独立预算保留旧用户原文，避免仅靠模型重述 |
| 记忆注入 | [context/memory.rs](https://github.com/openai/codex/blob/8d44977aa2fb9ae1b128660668dc5b36966613fa/codex-rs/core/src/context/memory.rs)：独立类型、不同角色、字节上限 | 独立 retrieved_memory，但预算只计 content，不计 id/scope/提示和消息开销 | 保留本项目 System 角色兼容性与不可信声明；完整消息计量，结构化序列化边界 |
| 分层读取 | [read_path.md](https://github.com/openai/codex/blob/8d44977aa2fb9ae1b128660668dc5b36966613fa/codex-rs/ext/memories/templates/memories/read_path.md)：索引→相关证据→原始 rollout，有查询步数限制 | 关键词/中文双字召回，最多20条；自动召回8条/1024估计token；原始事实保存在 transcript | 不引入文件型全局记忆库；本轮修复预算，索引/证据读取列为后续独立项 |
| 自动提炼 | [phase1.rs](https://github.com/openai/codex/blob/8d44977aa2fb9ae1b128660668dc5b36966613fa/codex-rs/memories/write/src/phase1.rs)、[phase1_output.rs](https://github.com/openai/codex/blob/8d44977aa2fb9ae1b128660668dc5b36966613fa/codex-rs/memories/write/src/phase1_output.rs)：受限并发、租约、结构化输出、secret redaction、成功无输出 | 成功 TurnCommit 后确定性摄入 Session Episode，逐消息1000字符，缺少总量上限；尚非语义提炼 | 本轮增加总量上限和空结果回执；不把普通对话自动提升为用户确认的 Semantic |
| 整合与存储 | [phase2.rs](https://github.com/openai/codex/blob/8d44977aa2fb9ae1b128660668dc5b36966613fa/codex-rs/memories/write/src/phase2.rs)：全局租约、usage选择、文件diff、隔离整合agent | SQLite scope/TTL/digest/forget receipts，跨会话写入要求确认，无整合agent | 保留单一数据库真相源。文件/Git基线会增加第二套生命周期，不适合直接移植 |
| 记忆质量 | [stage_one_system.md](https://github.com/openai/codex/blob/8d44977aa2fb9ae1b128660668dc5b36966613fa/codex-rs/memories/write/templates/memories/stage_one_system.md)：低信号允许空结果，区分证据与助手自称成功 | Episode 是低置信度会话摘录，明确记忆独立；没有提炼质量评测集 | 避免伪装成智能提炼。未来先建立偏好/失败/过期/秘密/冲突评测，再启用提炼 |

Codex 当前同时存在 v1/v2 记忆路径；README 的两阶段概述不是所有版本完全相同的实现。上述结论来自各个具体文件，不把实验性实现视为稳定 API，也不推断性能优于本项目。

## 本轮实施计划

1. 压缩边界保护最近用户输入及其后完整工具轮次；图片/工具裁剪也不得触碰这段。摘要附带受限旧用户原文，重压缩延续原文，原文只作历史证据。
2. 将记忆上下文渲染放在无会话状态的 memory crate，通过调用方计量完整 Message；包括提示、JSON元数据、转义和消息开销。超大单条跳过，后续小条仍可入选；0预算不注入。
3. 自动 Episode 摄入总上限16KiB，逐条1000字符，UTF-8安全，标注省略；来源只记录实际保留消息。空内容不写无用记录但提交幂等摄入回执；不改数据库结构。
4. 验证长当前工具轮次、原文保留、超长元数据/预算、Unicode/空内容/总上限、摄入幂等，以及 workspace 测试、Clippy、MSRV与格式。

## 后续计划（本轮不宣称实现）

按依赖顺序：A. 记忆质量评测与泄密过滤契约；B. 独立 SQLite extraction_jobs（source digest、lease、backoff、read-only/旧life fence），有界结构化模型提炼；C. scope内冲突/过期/去重整合，明确用户确认后才晋升 Project/Global；D. 按实际使用记录 usage/citation、索引与按需证据读取。每阶段须验证重启恢复、忘记后不复活、跨作用域不泄漏、模型无输出，以及成本收益。

整合 agent、向量库、全局文件记忆、远端压缩均没有足够证据证明适合当前项目，暂不引入。当前计量仍是估算，provider usage 校准与冻结预算的最终 gate 继续有效。

## 实现边界

当前轮次采用最近 User 消息至历史末尾作为保护区；单个长轮次无法安全缩减时返回无候选，由冻结预算门禁拒绝发送。旧原文只保留完整文本，超预算条目跳过，不截断后冒充完整指令；预算上限为冻结窗口的1/8、2048估计token、旧前缀估计量的1/4三者最小值。无法保留的原文仍在 canonical transcript。Episode 是按时间顺序的前缀摘录，总上限可能省略后期答复，不声称完整语义总结；有截断提示和实际保留来源。存储读取逐批推进，到上限即停止。

## 本轮验证结果

最终代码：`cargo test --workspace` 266通过、0失败/忽略（含9项真实daemon合同）；Web 27通过；Clippy workspace/all-targets/all-features `-D warnings`、Rust 1.88 locked check、release locked build、cargo fmt、git diff --check 全部通过。cargo deny offline 的 advisories/bans/licenses/sources 通过（使用缓存公告库，既有重复版本警告仍保留）。

日志：`/tmp/codex-comparison-tests-final.log`、`/tmp/codex-comparison-clippy.log`、`/tmp/codex-comparison-msrv.log`、`/tmp/codex-comparison-release.log`、`/tmp/codex-comparison-web.log`、`/tmp/codex-comparison-deny.log`。未调用真实摘要服务，未证明提炼质量或成本收益；验证的是宿主上下文传递、预算、存储边界及现有合同。SQLite仍为v12，无迁移；原有Wave改动全部保留，未commit/push。
