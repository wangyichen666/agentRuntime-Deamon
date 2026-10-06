# 冻结工具发现与 dispatcher

每个聊天 run 在准入时保存授权后的完整、有序 `ToolSpec` snapshot。`catalog_generation` 为该完整快照的 SHA-256，与 `RunSnapshot.tool_catalog_digest` 相同；目录名称、schema、描述或授权范围变化均改变 generation。已准入 run 不因 MCP reload 扩大工具集合；queued 重启需要恢复精确一致的目录，否则失败。SQLite 是发现许可事实源，索引缓存只有检索能力。

Provider 常驻核心工具为 `read_file`、`write_file`、`edit_file`、`exec`、`plan`、`sub_agent` 和兼容的 `spawn_subagent`。有延迟工具时再提供 `tool_search`；仅包含核心读取工具的子 Agent 保持原来的小工具面。其余内置、MCP 和委派管理工具均延迟发现。

## 搜索与调用

Provider 顶层工具名始终是 `tool_search`。搜索参数是严格的 `{query, limit?}`：

```json
{"query":"select:recall_memory","limit":1}
```

`select:name1,name2` 只接受精确 canonical name；`list` 返回有界名称顺序列表。普通中英文检索按名称、命名空间、关键词、描述、参数 schema 分字段评分；支持 snake/kebab/camel/alnum 分词和中文单字/双字。精确名称优先，平分按名称稳定排序。默认最多5项，limit范围1–8，总结果不超过16KiB；不能删减参数 schema 来制造可执行许可。过大的 exact select 报错；普通结果省略过大项并标记 has_more。

结果包含 schema_version、catalog_generation、完整 tools、has_more 和 dispatch_hint。搜索完成后使用同一个工具嵌套调用：

```json
{"name":"recall_memory","arguments":{"query":"既有约束","limit":1}}
```

两种参数都拒绝未知字段或混合形式。仅当前 exact run 已持久发现且仍在冻结目录中的工具可以调用。隐藏工具 direct 调用、未发现调用、拼错名称、过期 generation 和损坏 receipt 均拒绝；没有模糊名称副作用映射。执行继续走原冻结 schema、真实 effect/resource conflict、整批预检、安全审批和工具预算，保留 Provider wire name/call id。

## 持久事实与读取

SQLite v18 新增 `tool_discoveries`，同事务保存 owner、operation、query、完整结果、receipt 摘要和 `tool_discovered` 事件。操作身份绑定 run、执行轮次和 wire call id，跨轮复用 wire id不会覆盖先前发现。每个run最多128份 receipt，相同操作和输入幂等；不同输入、owner或generation冲突。回执只读复取不写入、不创建 runtime、Provider或MCP。

共享 RPC `run.discovery` 使用严格 `{run_id}`，返回 `{receipts:[...]}`；CLI/TUI、ACP v1和WebSocket可通过 `/tools <run_id>` 读取同一 daemon fact。连接v1需协商 `runs` capability，未知身份字段拒绝。读取会验证当前 lifetime、owner、frozen catalog、receipt摘要、结果完整schema与授权集合；损坏明确报错，不能退为空目录。终态和重启保留已发表记录；跨run、跨session不继承发现许可。

索引缓存按完整snapshot摘要键控，最多16份不可变索引，构建和评分在blocking pool。取消命令先持久 `cancellation_requested` 再发token；发布事务拒绝已取消/非running owner。取消后worker可以完成纯计算，但不能写 receipt/event；它也不拥有session gate或终态。已发表结果不会因为取消、断线或时间流逝被删改。

## 验证范围

本地mock Provider、SQLite文件/内存、真实daemon socket、重启和CLI/ACP/WebSocket合同见 [实施记录](./changes/governed-runtime-controls.md)。商业Provider和真实远端MCP reload负载仍未验证。ACP v2管理投影和共享revision reducer属于后续波，本文不宣称完成。
