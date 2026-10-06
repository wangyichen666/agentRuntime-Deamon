# Runtime 物理 crate 边界

2026-10-07 的 Wave8 迁移代码而非复制。binary 名称 `my-agent`、配置路径、SQLite v20、socket、HTTP/WS 和 CLI 参数保持兼容。根 `src` 仅有 main.rs/bootstrap.rs；命令解析/展示位于 agent-cli，真实服务和适配器各有独立 workspace crate。

| crate | 实际职责 | 持久/执行 owner 与依赖 |
| --- | --- | --- |
| agent-core | 类型化身份、领域状态机、SHA 定义、纯 reducer DTO | 无入口/存储依赖 |
| agent-daemon-protocol | 严格版本 DTO、ACP 协商 schema、共享 slash 解析 | 仅 core；解析不执行 mutation |
| agent-daemon-client | socket/协议、bounded view actor、durable cursor 恢复 | protocol；不编译 server |
| agent-storage | SQLite canonical transcript/run/turn/queue/interaction/plan/compact/发现/capture/receipts | 唯一持久事实 owner |
| agent-context、agent-memory | 纯 assembler/预算/检索与评测 | core；不持有 session owner |
| agent-sandbox | 执行、进程登记/清理、Docker 拒绝策略、取消端口 | core；接受冻结 owner/image，不取 runtime/repository |
| agent-runtime | LoopEngine、Provider/韧性、工具 dispatcher/安全、hooks、plan/session capability、cron/MCP/skill | 下层领域/存储 ports；不依赖 daemon/具体入口 |
| agent-daemon | SessionSupervisor、RunCoordinator、writer、控制屏障、准入/取消/业务 terminal、审批/委派/compact、socket | runtime/storage/protocol；canonical 仍在 SQLite |
| agent-entry-support | RPC helper、恢复 DTO 解码、Web 启动辅助 | client/protocol；无 runtime/storage |
| agent-cli | Clap、CLI/TUI、keymap/编辑器/展示 | client/protocol/core/support；CommandHost 组合端口 |
| agent-acp | 真实 v1/v2 SDK、strict namespace、标准通知、有界 stdio | client/protocol/core/support；无服务端 owner |
| agent-gateway | HTTP/WS、静态 Web 资源、bearer/workspace 转发 | client/protocol/core/support；GatewayHost 组合端口 |

CommandHost 和 GatewayHost 在根 BootstrapHost 有完整实现与调用链：daemon 启动、adapter 构造、首次配置、进程诊断、离线评测。业务请求只走 DaemonClient；接口不保存 plan/run/session 状态。旧 `src/client.rs`、`src/storage/mod.rs`、`src/daemon/protocol.rs` facade 以及旧业务模块全部删除。旧 source 路径迁至 `crates/runtime/src`、`crates/daemon/src`、`crates/cli/src`、`crates/acp/src`、`crates/gateway/src`；slash 共享定义只有 protocol 一份。

库全部启用 forbid(unsafe_code)。文件安全仍用原目录能力、O_NOFOLLOW/openat、身份/内容 CAS、原子 rename、原权限和 fsync；系统调用通过已锁 rustix 1.1.4 的 safe fd API，进程组清理保留。库内 panic-on-lock 改为 typed error 或保守拒绝；stdout/stderr 使用 fallible UI sink。环境加载只在根进程建立 Tokio 多线程 runtime 前执行。

跨 crate 的旧 in-memory/ephemeral/JSONL fixture 由非默认 test-support/test-transport features 提供，入口只有 dev-dependency 使用；正式默认 daemon 不编译测试 client。AST 护栏区分明确的 test/support 条件，任意 cfg(any(test, target_os=...)) 仍检查生产分支；不能用测试条件掩盖真实依赖。未删除或放宽原行为断言。

每个新 crate 迁移后都有 workspace check 与目标测试日志：sandbox 的进程登记1及工具执行5、runtime128、daemon27、support2、CLI/TUI29（随后迁入原 main 日志匹配测试，共30）、ACP5、Gateway7。最终架构12项涵盖 manifest allowlist、实际 source 边、唯一领域定义、无长期 mutable transcript owner、无生产 unwrap/expect/print/unsafe、真实物理实现及仅两个根源码文件。

Cargo.lock 只增加七个内部 package、根依赖边和已存在 rustix 的 direct 使用；第三方 name/version/source/checksum 集合与基线完全相同。没有数据库迁移或部署。完整最终门禁和失败修复记录见 [八波实施记录](changes/governed-runtime-controls.md)。真实 Linux/CI、商业 Provider、IDE v2、外部 MCP、用户 hook、Docker 成功隔离和 Keychain 的生产验收仍待真实环境验证。

最终审计补充：v1/v2 permission response 共享严格原始 Value decoder，保留标准 selected outcome 的其他命名空间 metadata；v1 的根/selected 中 my-agent 控制字段均在审批前拒绝。真实 stdio RED 曾返回 EndTurn 并执行文件，修复后返回协议错误、canonical pending 保持，重连 v2 可明确批准，Provider 仅按实际后续轮次调用。合同 acp_v1_permission_rejects_v2_identity_before_approval_and_v2_can_recover。

最终完整门禁已通过：Rust379（31真实daemon、12架构）、Web36、strict Clippy、MSRV1.88、release、fmt/JS/diff 与离线供应链。详见 [最终逐项报告](changes/governed-runtime-final-report.md)。公告只使用缓存；未提交、推送、部署。
