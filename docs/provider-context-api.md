# Provider 请求上下文 API

SQLite v20 `provider_requests` 是发送前材料的唯一持久事实源，`agent-context::assemble_request` 是实际发送和重建的唯一 assembler。模型调用、fallback 每个候选、手动与自动摘要均在调用 Provider 前保存完整 ExactOwner、source revision、projection generation、冻结 policy/catalog、route/provider identity/capability、校准和预算；capture 与 ContextEnvelope ledger 同事务发布。未实际调用的候选没有 capture。

## 读取

`context.readback` 严格参数：

```json
{"session_id":"SESSION","expected_lifetime":"LIFETIME","run_id":"RUN","capture_id":"CAPTURE","local_diagnostics":false}
```

`run_id`、`capture_id` 可省略，选当前 lifetime 最新匹配 capture；lifetime 必填。响应 schema_version=1，包含 purpose（model/compact_summary）、完整 owner、round/candidate、snapshot_revision、ContextEnvelope、policy/request/message/provider_tools digest、images/tool_calls、replayability、omissions。默认没有 messages/tools。

`/context <session> [run] [--local]` 在 CLI/TUI、ACP v1、WebSocket 调同一个 handler；HTTP `POST /api/context/readback` 使用 `{workspace,params:{上述参数}}`，保持既有认证。旧无 capture 的 slash 摘要保留既有 projection 统计，并显式标明 full_provider unavailable；显式 capture/run 的缺失直接失败，不构造空请求。

## 来源与确定性

replayability=captured 仅指保存的当时材料。每次重建核对 capture SHA、冻结 RunSnapshot policy/catalog/budget、ExactOwner、当前 lifetime 与 canonical source prefix；再调用同一个 assembler 并比对请求 digest。历史 capture 的 source generation 可以小于当前已安装 projection，响应保留原 generation。动态环境、plan/skill/memory 当前版本明确 non-replayable，不运行 Git、skill 搜索、Provider、MCP、compact 或 ingest。摘要 capture 的 source 是当时 canonical 依据，摘要正文诊断只给摘要标识。

计量涵盖 system stable/history/retrieved/dynamic overlay、实际小工具 schema、媒体降级、output reserve 与成功 usage 校准。fallback 使用候选 capability 和冻结预算重新计算；不把估算器宣称为厂商 tokenizer。完整 catalog digest 与实际 Provider 工具面 digest 分开。

## 诊断与失败

显式 local_diagnostics 才返回脱敏 messages/tools。凭据来源只用于匹配脱敏，配置文件直接只读，禁止触发旧凭据迁移；不可核对凭据、配置损坏、当前权限与冻结 permission 不一致均拒绝完整诊断。memory、Tool 返回、checkpoint、媒体、thinking、工具调用参数和摘要源原文隐藏；Bearer/API key/secret 及 schema default/examples 保守脱敏。请求 digest 描述原材料，诊断材料已脱敏，不能用于逐字重放。

单 capture 最多16MiB、每 run1024；完整响应受协议帧预算约束。未知字段、缺失材料、摘要不匹配、canonical prefix/policy/catalog 损坏、life 过期均 fail closed。读取在同一只读 repository 事务验证，不申请 writer、不写 audit/global clock、不启动 runtime owner；重启只读不重放请求。

## 证据

`provider_context_readback_rebuilds_captured_request_without_replay_or_writes` 穿过真实 Provider wire、daemon socket、CLI/ACP/WebSocket/HTTP、kill/restart，验证实际消息/工具 digest 相同、默认不泄露、零写、权限变化和损坏拒绝。`compact_memory_receipts_survive_restart_and_all_three_entries` 验证 native 摘要 capture 与 compact 后下一请求 generation。文件/内存合同验证幂等冲突、原子 rollback、v19迁移/备份和 future21 拒绝；纯领域测试覆盖 fallback、校准、预算、policy/catalog/skill 变化与脱敏。
