# 第五轮：工具结果元数据的输入预算

研究日期：2026-10-06。项目已提交基线为a8ac3dc，实际工作区另有控制面Wave开发；本轮保留全部工作区变更，使用隔离快照验证后仅应用context crate增量、OpenAI出站行为测试及本轮记录。

## 固定来源与研究证据

Codex参考固定提交8d44977aa2fb9ae1b128660668dc5b36966613fa，并非实时最新；该提交workspace版本为0.0.0（codex-rs/Cargo.toml），cli继承workspace，git describe为8d44977，不能据此推断已发布版本。codex-rs/core/src/context_manager/history.rs的estimate_response_item_model_visible_bytes对FunctionCallOutput计算body、call_id、name、namespace；不计外层JSON转义或传输包装。

本项目src/provider/openai.rs::request_messages会发送Message.name与tool_call_id；tool_calls数组中的ID计量只覆盖assistant调用，不能覆盖下一条tool结果的关联字段。crates/context的TokenEstimator::messages只计content、tool_calls、图片和固定开销，因此仅增大结果name/关联ID不会增加预算。当前各provider outbound转换都不发送Message.thinking；把存储思考加入计量会形成错误压力，故不采用。

## 开发前计划

1. 独立JSON预算评测明确缺口：ASCII/中文名称、长关联ID、组合结果、工具交换闭合、存储思考不进入请求、已计工具参数。先运行RED证明旧实现漏计。
2. 共享TokenEstimator计入name与tool_call_id原文，以饱和加法避免聚合回绕；沿用现有文字/图片估算，不把JSON wire大小当token真实值。不改原始历史、作用域、provider权限或数据库schema。
3. 对比闭合压缩候选：即使text更短，若元数据让实际估算更大，应拒绝无收益候选。保留当前轮次suffix与原始工具配对合同。
4. 将新独立预算样本接入flywheel evaluate与cargo test，真实OpenAI outbound转换核对工具结果字段。验证context/storage/root相关行为、必要门禁，再以文件hash校验仅应用增量，不提交/推送/部署。

## 限制

仍是模型输入启发式估算，不是精确tokenizer。某些provider不发送name或重映射关联ID，共享估算可能偏保守；现有provider usage校准与最终预算gate继续有效。验证只针对漏计与结构门禁，不能推导真实任务成功率或成本收益。

## 聚合诊断与反馈取舍

本地`.my-agent/runtime.sqlite3`的主库只读聚合显示schema v4，不存在memory_exposures、memory_assessments、memory_feedback_receipts等飞轮表，不能获得本轮显式反馈或曝光诊断。普通mode=ro打开失败；确认当时无WAL/journal后，以immutable只读查询主库，只得到schema，不读取会话正文、不迁移数据库、不启动生产daemon。此结果不是实时daemon报告。不能把缺表当作零反馈，也不能用历史运行成功推断记忆好评。因此本轮选择源码与独立样本直接证明的计量缺口，不改变记忆排序/作用域或存储。

## 行为合同与失败复现

- 工具结果名称、关联ID纳入共享messages/request估算；既有content、tool_calls、图片和固定开销保持，聚合使用饱和加法。
- 思考只入存储，不通过当前provider请求转换发送；输入预算保持不计thinking。
- 当前轮次完整保留、工具调用/结果配对继续先验证。候选正文变短但关联字段抵消收益时，预算门禁拒绝。
- `crates/context/eval/budget-v1.json`固定8个独立样本，以预先固定增量或候选是否应接受为断言；期望不在运行中由实现推导。新增版本显式加入dataset_versions，保留原dataset_version字段兼容现有输出。

旧估算在新增评测下24/29通过，五个失败分别为ASCII名称、中文名称、长关联ID、组合元数据和闭合候选无收益。修复后29/29；工具参数、图片和不回传思考三个边界原先即通过。OpenAI行为测试核对真实request_messages转换保留名称/关联ID、排除thinking/reasoning_content，并验证request计量增加20个启发式tokens。

## 验证对象与工程边界

隔离快照包含本回合开始时的全部既有未提交控制面改动。该快照全量测试在5项storage迁移断言失败（当前新迁移v17，而旧测试预期v16），Clippy在compact_runs.rs既有复杂元组失败；不修改这些Wave 3代码或假装工作区全通过。context 5项测试及新增OpenAI行为测试在此快照通过。沙箱首轮socket合同EPERM，经相同命令环境授权验证后才记录迁移断言失败。

另从固定已提交基线a8ac3dcbbdd9f77f4988a5359d03c201e4df9f2e生成隔离源码，加上相同context/provider增量，验证本轮改动能独立通过。该基线两个修改目标与初始工作区原文件SHA256相同；应用前再次校验，防止覆盖并行开发。根研究/计划/进度只追加，不把旧快照根文档回写。

实际门禁和日志位于`/private/tmp/agent-budget-heartbeat-ifpr94gm/`，验证源码为`/private/tmp/budget-verified-base-7jsum06f/`：

| 验证 | 实际结果 | 日志 |
| --- | --- | --- |
| 旧算法RED | 退出101；24/29，5项已复现缺口 | heartbeat-budget-red.log |
| 当前开发快照全测 | 退出101；5项既有迁移断言失败 | heartbeat-budget-tests-authorized.log |
| 当前开发快照Clippy | 退出101；既有compact元组type_complexity | heartbeat-budget-clippy.log |
| 当前开发快照OpenAI新合同 | 退出0；1项通过 | heartbeat-budget-provider.log |
| 已提交基线+增量全测 | 退出0；303项，无忽略，含11项真实daemon合同 | heartbeat-base-tests.log |
| 严格Clippy | 退出0 | heartbeat-base-clippy.log |
| Rust1.88.0全目标检查 | 退出0 | heartbeat-base-msrv.log |
| Release构建 | 退出0 | heartbeat-base-release.log |
| fmt | 退出0 | heartbeat-base-fmt.log |
| Web tests/check | 退出0；27项 | heartbeat-base-web.log / heartbeat-base-web-check.log |
| 离线cargo deny | 退出0；advisories/bans/licenses/sources均通过；保留已有duplicate warnings | heartbeat-base-deny-authorized.log |
| flywheel evaluate | 退出0；记忆17/17，上下文29/29 | heartbeat-base-flywheel.json |

离线deny初次只读缓存锁失败，授权同一离线命令后通过；没有刷新公告。这里的303项不能替代尚未完成的Wave 3–8验收。启发式没有精确token上下界保证，字段覆盖提升不证明真实模型成本、任务成功率或记忆语义质量提高；尚未做真实模型、Linux/CI验证。没有提交、推送或部署。

应用复核：本轮3个代码/评测文件与通过门禁的隔离验证内容逐字节一致；应用后原仓库`git diff --check`退出0，HEAD仍为a8ac3dc，无新增提交。验证摘要保存在`/private/tmp/agent-budget-heartbeat-ifpr94gm/heartbeat-verification-summary.json`。
