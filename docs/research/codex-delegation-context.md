# 第三轮：有界、可核实的父子任务上下文交接

研究日期：2026-10-05。参考 Codex 固定提交 `8d44977aa2fb9ae1b128660668dc5b36966613fa`；不宣称实时最新。

## 对比结果

Codex `core/src/tools/handlers/multi_agents_v2/spawn.rs` 区分 full history 与其他 fork 模式，完整继承时复用模型/开发者配置；`core/src/agent/control/spawn.rs` 在 fork 前 flush rollout，按历史模式装载并可裁到最近轮次，恢复时复核父关系。可借鉴的契约是：来源快照必须完整落盘，交接应标明来源并受预算与权限约束。

本项目委派仅传 task 字符串，能力快照与预算继承已完善，但模型只能自己转述背景，原始约束可能丢失或被改写。直接复制父历史会带入思考、凭据、工具参数与无关历史，也会增加 token 成本。当前 SQLite 事务和 run_snapshot JSON 足以持久有界来源包，无需另建文件记忆库或升级数据库 schema。

## 开发前计划

1. 在 spawn_subagent RPC、模型工具及兼容 sub_agent 中增加可选 `context_source_ids`，默认空；显式选择最多4条 lifetime:seq 来源，支持 `parent_input` 别名选择父run准入输入，不隐式复制全部历史。
2. 在委派事务中对来源作 lifetime、run owner、batch digest 校验，当前切片只允许父 run 自身的消息，拒绝其他会话、旧run、重复ID与无效序号。正文过滤常见凭据，仅交接 role 和文本，不传思考、图片和工具参数。
3. 每条最多1024字符，整个包最多8KiB并受子任务预算约束；所有截断显式标明。持久在 child RunSnapshot 的可选字段内，旧snapshot默认无交接；digest说明是过滤后捕获包的校验，不能充当内容真实性证明。
4. 保持task原文独立；请求材料以独立retrieved分区加入宿主包装，明确父材料仅为证据，子能力与任务边界以冻结快照为准。正常轮次和context overflow重组都从同一持久包组装，计入完整请求预算，不产生记忆好评或自动提升scope。
5. 相同spawn_key与相同来源ID重试读回原捕获包；改选来源拒绝，不重新读变化历史。孙任务不隐式继承祖先包。测试真实provider收到的材料、重启读回、幂等、权限、凭据和预算边界。

## 范围与保留意见

这是显式委派时产生的独立快照。删除父记忆不会修改已经交接的child运行快照；清除child会话按既有持久事实保留策略处理。JSON标签和digest不能证明模型理解正确或抵抗提示注入。只验证宿主来源、预算、作用域与持久协议，语义收益待真实模型对照样本。

## 实施与验证结果（2026-10-05）

五项方案已完成。来源别名parent_input与普通序号规范化后复核去重；包复用run_snapshots JSON，数据库保持v13。真实mock provider捕获确认独立父材料进入child请求、凭据过滤且工具仍只有read_file。重启后run.read读到相同包，重复spawn_key读回原child；删除来源选择的重试拒绝。存储合同验证来源/lifetime/digest/预算错误全部事务回滚，孙任务默认无祖先包；渲染拒绝被改写的digest或未知版本。
