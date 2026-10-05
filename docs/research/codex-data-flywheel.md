# Codex 对比与数据飞轮（2026-10-03）

## 研究结论

参考官方源码固定提交 `8d44977aa2fb9ae1b128660668dc5b36966613fa`，仅只读研究。远程 HEAD 被本机失效代理阻断，不能声称是当前最新版本。上一轮研究见 [上下文与记忆](codex-context-memory.md)。

| 机制 | Codex 证据 | 本项目判断与行动 |
|---|---|---|
| 历史/压缩 | core/src/context_manager/history.rs、core/src/compact.rs | 本项目已有 transcript、投影 CAS、当前轮次保护；继续保留，重复移植无收益 |
| 上下文传递 | core/src/context/memory.rs、ext/memories/templates/memories/read_path.md | 已有独立记忆预算和不可信材料声明；补充入选版本证据，不能把候选召回当成注入 |
| 使用反馈 | [state/src/runtime/memories.rs](https://github.com/openai/codex/blob/8d44977aa2fb9ae1b128660668dc5b36966613fa/codex-rs/state/src/runtime/memories.rs) 的 record_stage1_output_usage、get_phase2_input_selection | 使用次数适合调度与保留，不足以证明质量；本项目记录请求准备阶段曝光，并另收集显式 helpful/irrelevant/incorrect/outdated 评价 |
| 任务可靠性 | 同文件的 claim/lease/retry；memories/write/src/phase1.rs | 模型提炼任务租约有价值，但本轮使用已有事务级 Episode 摄入，补有界重启恢复，不新建空转 worker |
| 记忆卫生 | phase1.rs 上传前 redact_secrets；stage_one_system.md 低信号为空 | 自动摘录过滤常见凭据行，保留 source IDs；空结果 receipt 不产生无用记忆。过滤非全面 DLP，不改 canonical transcript |
| 整合 | memories/write/src/phase2.rs 全局租约与文件 diff | 本项目 SQLite 已有 owner/作用域/TTL/forget，不引入第二套文件真相源；智能整合需有质量与成本证据 |
| 运行/业务流程 | 本项目 TurnCommit、tool batch、RunSnapshot、context ledger、维护诊断 | 已有原子终态与工具闭合；应补终态后维护的崩溃窗口，维护失败仍不改业务终态 |

[OpenAI 官方压缩说明](https://developers.openai.com/api/docs/guides/compaction)要求 opaque compaction state 原样传递。当前项目支持多个 provider，宿主摘要投影已有独立验证，尚无证据表明绑定 Responses 压缩可以改善整体表现。

## 开发前确定的方案

1. SQLite v13：保存 run+memory 版本曝光（不保存查询/内容）及幂等反馈回执；反馈绑定已有曝光、exact owner 和内容 digest，限定同 lifetime。forget 级联删学习数据，session clear/delete 清除该 lifetime 的学习数据。
2. 用显式反馈调整确定性召回：incorrect/outdated 停止召回；同关键词相关度下 helpful 优先、irrelevant 降序。曝光次数及 run success 不参与质量分数；评价变化可恢复，反馈不改变事实、置信度或作用域。
3. 在线自动注入与 recall_memory 工具共用策略；只有实际进入已通过完整预算校验的模型请求材料或工具结果才写曝光。记录含 policy version；context_read_only 不写学习数据。
4. RPC + CLI 提供报告、反馈；反馈入口不注册为模型工具，模型不能自行给自己投票。报告列出曝光和各类反馈，运行结果仅作观测，不能解释为因果收益。
5. 固定独立 JSON 评测集覆盖中文、相关性、重复、作用域、过期、负反馈与预算；CLI 可离线评测，cargo test/现有 CI 自动执行。失败返回非零，今后策略改动先过此门禁。
6. 有界摄入恢复：每批最多 32 个已完成但缺 receipt 的 turn，跳过只读/旧 lifetime；空结果也闭合。daemon 启动后的首个维护 tick 与运行期间每 60 秒推进，无模型调用。失败间隔至少 60 秒、最多重试 3 次，报告暴露 pending_ingests/retry_exhausted；daemon 停止期间不执行，剩余任务在下次启动继续。

闭环：真实运行曝光 → 用户明确反馈 → 下一次召回应用评价 → 固定评测防回归 → 报告暴露样本量和反馈缺口 → 后续代码策略更新。自动化范围是记忆检索学习与维护恢复；不会把成功 run 自动转成偏好，也不宣称无人监督改写源码或已经证明生产收益。

## 后续升级条件

只有在真实反馈样本、拒绝/遗漏率和 token/延迟基线足够后，才增加模型语义提炼、向量检索或 scope 内冲突整合。开发前需扩充含人工标签的独立评测，验证删除不复活、租约失效、凭据过滤、取消、成本与真实模型质量。现阶段不自动删除未使用的事实，不扩展跨会话/全局授权。

## 实施与验证结果

六项方案已接线。SQLite v13 升级及v12数据保留通过真实文件数据库验证；独立质量样本17/17，真实daemon进程证明反馈→重启→下次召回→遗忘清理链路。全量Rust276项（包括10项daemon合同）、Web27项、严格Clippy、MSRV1.88、release、fmt/diff和离线cargo-deny均通过。离线公告来自已有缓存，非实时漏洞检查。

没有真实模型调用或生产效果实验，17项合成样本只能证明检索与预算契约。当前闭环自动应用显式评价、恢复缺失摄入并防回归；人工仍需评价样本、审查后续策略与代码，不等于自动训练模型或无人监督修改源码。参考Codex固定提交的机制并不代表全部机制适合移植。
