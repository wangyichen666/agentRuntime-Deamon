# 第二轮：压缩保护、记忆证据与维护生命周期

研究日期：2026-10-03。延续上一轮全部改动，Codex 参考仍为固定提交 `8d44977aa2fb9ae1b128660668dc5b36966613fa`，不是实时最新版本。

## 研究与取舍

Codex `core/src/context_manager/history_user_authorization.rs` 区分原始、继承、checkpoint 输入及完整性；`core/src/context/compaction_summary.rs` 把摘要标为 `compaction.summary` contextual fragment。值得借鉴的是来源与完整性契约。本项目支持多 provider，不能直接复制 Codex item metadata 或随意改摘要角色；本轮用宿主生成的结构化包装标识历史模型推断，强制当前轮次原文保留。

Codex `memories/read/src/citations.rs` 从模型引用解析来源，但引用并不自动证明内容采用或结果质量。本项目已有确定性 lifetime:seq 来源，直接提供受限证据读取，比增加文件型全局记忆库更适合当前 SQLite 架构。读取须重新校验记忆可见性、来源 lifetime 和删除状态，跨会话的记忆确认不等于授权公开整段源对话。

Codex 的 claim/lease/backoff 适合后台模型任务；当前确定性摄入已经由 SQLite 事务和幂等 receipt 保护。实际缺口是与 daemon 空闲生命周期脱节，应让健康积压按有界批次排空，退避任务不阻止空闲退出，不新建空转常驻 worker。

## 开发前计划

1. 在 core 提供当前轮次定位和 suffix 保留契约；context 候选验证与 storage 提交同时调用。持久提交用已验证 canonical prefix 核对，不依赖调用方自称保护正确。工具闭合仍保留，压缩不删除当前输入、图片、工具参数和结果。
2. 摘要增加宿主生成的版本、projection digest、模型推断标签及核实提示；保留历史原文锚点。递归达到上限报错，允许既有 prune_only 降级，不再把静默截断当完整摘要。
3. 加入独立 JSON 上下文候选评测：删除/篡改当前输入、媒体丢失、工具结果变更、断裂配对、无收益、合法压缩、Unicode。扩展 flywheel evaluate，检索与上下文门禁一起运行。
4. `memory.evidence` RPC 与只读 `memory_evidence` 工具：只读取记忆声明的 source IDs，页数/字节双上限；仅同源会话 lifetime 可读取文本，跨会话来源返回受限原因。过滤常见凭据，不返回图片/思考/工具参数；读取不构成反馈，不复制原文到学习表。
5. 维护查询暴露最早可重试时间；健康积压保持空闲生命周期，并在有进度时短间隔推进下一批。每批32、最多3次失败、未来退避不阻止退出；真实daemon测试证明大于32条能排空。
6. 验证恶意但工具闭合的持久候选拒绝、来源证据不跨会话/遗忘后不可读、凭据过滤、跨页、积压排空及完整门禁。SQLite 保持 v13，不另建记忆库。

## 未作出的结论

结构化包装和确定性评测不能证明模型没有幻觉或免疫提示注入；当前轮次与来源边界能被宿主验证，摘要语义准确性仍需真实模型和人工事实标签。子 Agent 自动继承所有父历史会扩大信息与指令面，本轮不默认开放；继续研究有界、显式任务包和来源元数据后再设计。

官方压缩文档：[Compaction](https://developers.openai.com/api/docs/guides/compaction)。本文使用宿主摘要投影，不把它等同于 Responses API opaque state，也不修改后者的返回值。

## 实施与验证结果（2026-10-05）

六项方案已接线：共同当前轮次suffix契约在候选与持久提交执行；推断摘要宿主包装和递归上限拒绝；独立JSON结构门禁；memory.evidence RPC/只读工具与来源digest校验；健康积压保持生命周期且有界排空。证据元数据也纳入32KiB上限，标识最多512字节。真实daemon合同核实40项两批恢复、来源重启读回与遗忘后拒绝；数据不跨会话公开，也不产生曝光或反馈。最终统一门禁见progress.md。
