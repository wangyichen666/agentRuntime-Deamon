# 实施进度

## 2026-09-07

- 已读取完整需求与全局中文输出约束。
- 已启用 planning-with-files 工作方式。
- 已创建阶段计划、发现记录和进度日志。
- 正在进行：阶段 0 项目骨架与阶段 1 ReAct 循环。
- 已创建 Rust binary crate；首次 crates.io 拉取长时间无响应，已按用户建议切换项目级中国镜像。
- 阶段一完成：OpenAI 兼容流式 Provider、read_file/exec、工具注册表、50 轮上限 ReAct 循环和 REPL。
- 阶段一验证：`cargo build` 成功；`cargo test` 4/4 通过；Clippy `-D warnings` 通过。
- 正在进行：阶段二安全、审批、参数校验与并行波次。
- 阶段二验证：`cargo test` 10/10 通过；灾难命令、审批、宽进严出校验和只读并行均有测试覆盖；严格 Clippy 通过。
- 阶段三完成：稳定上下文前缀、项目规则、最小动态环境、启发式 token 估算、80% 压缩闸门、分块递归摘要、图片/PDF 降级派发。
- 阶段三验证：`cargo test` 13/13 通过；`cargo build` 与严格 Clippy 通过。
- 正在进行：阶段四 JSONL 持久化、启动恢复与 RAII 会话锁。
- 阶段四验证：`cargo test` 15/15 通过；JSONL 恢复与 RAII 锁有测试覆盖；`cargo build` 和严格 Clippy 通过。
- 正在进行：阶段五 JSONL 长期记忆与关键词/中文 bigram 召回。
- 阶段五验证：`cargo test` 17/17 通过；关键词与中文 bigram、保存与召回均有覆盖；严格 Clippy 通过。
- 正在进行：全量代码审计、恢复健壮性、README 和最终验收。
- 最终增强：流式工具 arguments 分片重组测试、崩溃残缺 JSONL 末行恢复、完整 README。
- 最终验证：`cargo build --release` 成功；`cargo test --all-targets` 20/20 通过；严格 Clippy 通过；格式检查通过。
- 配置验证：缺少 API 配置时输出清晰错误；使用占位配置时 REPL 可启动并用 `/exit` 正常退出。
- 非测试代码审计：未发现 `unwrap()`、`expect()`、`panic!`。
- 初次验收环境没有有效服务凭据，先由 mock Provider 覆盖 read_file 与 exec 的 ReAct 闭环；用户随后提供临时凭据并完成真实验证。
- 2026-09-08 用户提供临时 DeepSeek 凭据后完成真实端到端验证：`deepseek-v4-flash` 先调用 `read_file` 再总结 README；随后调用 `exec` 执行文件计数并正确回答 7。session.jsonl 核对确认两个 tool_call_id 均已严格配对。凭据未写入项目文件。
- 2026-09-08 新增 `docs/agent-system.html`：按请求进入到最终回答的顺序说明完整链路，并汇总能力状态、6 个工具、三层记忆、安全与并行、模块架构及运行方法。已在桌面和 390px 窄屏浏览器验证；页面无横向溢出、无控制台错误，且不依赖外部资源或 JavaScript。
- 2026-09-08 启动进阶增补开发；已读取 Cargo、main.rs、provider.rs，正在完成全模块基线核对。
- 已通读 loop_engine、context、safety、session、memory 和全部 tools；基线核对完成，开始第一批 plan 工具。
- plan 第一轮验证：23 个测试全部通过；clippy 发现测试辅助函数 `PlanStore::in_memory` 在非测试目标中未使用，已限定为测试代码，待重跑。
- plan 最终验证：23/23 测试、build、严格 clippy 全部通过。
- sub_agent 完成：工具注册表改为 Arc 可共享子集；ReAct 支持无 session 的临时入口；独立历史、默认工具白名单、15 轮预算与禁止递归均有测试。25/25 测试及严格 clippy 通过。
- 前缀缓存完成：稳定上下文顺序与工具排序有测试；兼容 OpenAI/DeepSeek 缓存 usage 日志。28/28 测试及严格 clippy 通过。
- 图片/PDF 完成：OpenAI 兼容图片内容块、当前 turn 临时传图、非视觉降级、16 MiB 限制；lopdf 本地抽取最多 50 页。33/33 测试及严格 clippy 通过。
- 重复检测完成：连续三次相同工具名/参数/结果后仅发建议性提醒，工具不禁用。34/34 测试及严格 clippy 通过。
- 两级压缩完成：60% 温和、85% 强力，环境变量可调；35/35 测试及严格 clippy 通过。
- skill 完成：标题/摘要索引、关键词/中文 bigram、最多三个命中正文；36/36 测试及严格 clippy 通过。
- 第三批 cron/MCP 按蓝图可停边界延期；开始文档更新与进阶全量验收。
- README 与 `docs/agent-system.html` 已同步 8 个工具、子 Agent、多模态/PDF、两级压缩、重复提醒、缓存字段与 skill；HTML 浏览器复检被本地 file URL 安全策略阻止，静态解析仅出现 xmllint 不识别 HTML5 语义标签的兼容性提示。
- 进阶最终验收：`cargo fmt --all -- --check` 通过；`cargo build --release` 通过；`cargo test --all-targets` 36/36 通过；严格 clippy 通过且零 warning。
- CLI 烟雾测试：缺少配置时以状态 1 清晰报告 `OPENAI_API_KEY` 缺失；占位配置可正常启动并由 `/exit` 退出。当前执行环境没有注入真实 LLM 三项环境变量，因此本轮未重复消耗用户 API 凭据做线上调用。
- 2026-09-08 启动 daemon + 三入口架构演进；已完整读取附件和 planning-with-files 规则，创建新的阶段 A→D 计划。
- 初步确认 main 直接组装并调用 LoopEngine，历史仍由 REPL 持有；Provider 尚未向上游流式发事件，审批仍绑定终端输入，取消与 session 列表尚不存在。继续通读剩余模块后输出正式基线清单与阶段 A 计划。
- 完成 memory/plan/skills/sub_agent/tools 与 Cargo 依赖核对；确认这些能力可复用，阶段 A 不重写其业务逻辑。
- 一次规划日志补丁因锚点文字与文件不完全一致而失败；重新读取文件尾部后使用准确锚点补写，未影响源码。
- 已向用户输出正式现状确认清单与阶段 A 七步实施计划，开始协议、事件流、取消和审批内核实现。
- Provider 已增加兼容式流事件接口，OpenAI SSE 可逐分片上送；LoopEngine 已统一输出 turn/text/tool/completed 事件，并支持显式取消。
- 已新增 JSON-RPC 协议、4 MiB 帧限制、DaemonState、审批中介、活动请求表、五个 handler、内存 server/client；原 CLI 已改走内存回环，不再直接调用 LoopEngine。
- 阶段 A 首轮测试 40/40 通过；仅剩已被 daemon 审批取代的 `TerminalApproval` dead_code warning，已移除后待严格复验。
- 一次 SessionStore import 补丁因目标 import 尚不存在而失败；随后用正确上下文加入 `serde::Serialize`，未影响源码。
- 新增取消回归测试后首次编译因测试模块漏导入 `RequestId` 失败；已补齐导入，准备重跑全套验证。
- 阶段 A 最终验收通过：41/41 测试、build、严格 clippy、fmt check 全绿且零 warning；内存回环 CLI 已实际复用 daemon handlers。
- 开始阶段 B：Unix socket、工作区生命周期、自动拉起与专业子命令。
- 阶段 B 已新增稳定工作区运行路径、0700 权限、PID/ready/log/startup lock、状态探测与自动拉起雏形，并抽出 daemon 统一运行时装配函数。
- Clap 及 Tokio net/signal 已通过项目 rsproxy 中国镜像顺利下载；44/44 测试通过，严格 Clippy 仅发现生产目标中测试专用 `JsonRpcRequest` 导入，已按 cfg(test) 收窄。
- 阶段 B 最终验证：45/45 测试、release build、严格 clippy 和 fmt 全绿；真实 UDS 烟雾测试完成自动拉起、ready 探测、sessions、stop 与清理闭环。
- `my-agent --help` 已显示 chat/status/stop/sessions/config；缺配置时 `config check` 一次报告三项必需变量并返回状态 1。
- 开始阶段 C：axum 本地 API 与 OpenAI 兼容普通/流式响应。
- Axum 已通过 rsproxy 下载；首次编译发现 SSE `unfold` 的下一状态误包了一层 `Option`，已按 Stream 状态签名修正。
- HTTP 单测增至 47/47 全过；严格 Clippy 检出鉴权函数返回大型 Axum Response，已改为轻量 bool 判定并在 handler 统一构造错误响应。
- HTTP 真实烟雾测试通过：`/health` 返回 ready；普通请求把上游连接错误映射为 JSON；流式请求输出错误事件后严格以 `[DONE]` 收尾；0.0.0.0 无 Token 启动被拒绝。
- 烟雾测试暴露 daemon 子进程继承 PTY SIGINT 后留下 stale 标记，正在补独立进程组与 daemon 侧信号清理。
- 生命周期修复完成：自动 daemon 使用独立 process group，daemon server 自身处理 Ctrl-C；复测停止 HTTP 后状态为 stopped，pid/ready/socket 全部清理，仅保留日志。
- 阶段 C 完成：默认回环 HTTP、health、OpenAI 普通与 SSE、模型名校验、非回环 Token 门禁、HTTP 审批安全拒绝均已实现。
- 开始可选阶段 D：stdio JSON-RPC 编辑器适配器骨架。
- 第二轮 HTTP 烟雾测试首次选用的 18789 端口已被占用，服务清晰报错后改用 28789，验证通过；未覆盖既有监听进程。
- 阶段 D stdio 适配器已实现并通过 NDJSON `session.list` 烟雾测试，输出保留原始 `editor-1` request_id；客户端 EOF 后 daemon 自动回到 stopped。
- 编辑器首次烟雾测试误用了尚未重新链接的旧 debug binary，出现“不识别 editor”；执行 `cargo build` 后复测通过。
- 已将烟雾测试产生的临时 runtime 和仅含两条“你好”的测试 session 移入 macOS 废纸篓，可恢复；未改动其他 `.my-agent` 状态。
- 开始最终文档同步；确认 README 与 HTML 仍残留单进程/单入口旧描述，准备按当前源码统一更新。
- README 已重写为 daemon + 三入口当前架构；HTML 已更新入口、请求链路、控制面、模块与命令说明。
- HTML 浏览器实测：1280px 桌面无溢出、无控制台错误；390px 首测发现 usage 区横向溢出，增加 grid item min-width 与长 inline code 换行后复测 scrollWidth=390、无错误。
- 并发审计发现全局审批出口可能被排队 chat 覆盖；已改用 Tokio task-local 请求上下文，并增加双请求路由测试，当前 48/48 测试通过。
- 最终质量门通过：`cargo fmt --all -- --check`、`cargo build --release`、`cargo test --all-targets`（48/48）与严格 Clippy 全绿、零 warning。
- 配置诊断复验通过：缺失三项必需变量时一次汇总并以状态 1 退出；温和阈值不小于强力阈值时给出明确关系错误。
- 最终 stdio 实进程冒烟通过：`session.list` 保留 `editor-final` request_id、返回空会话清单，客户端断开后 daemon 自动回到 stopped。首个测试帧漏写 JSON-RPC 版本并被协议层正确拒绝，修正后通过。
- `docs/agent-system.html` 已同步到 `/Users/pilot/Desktop/agent-system.html`；桌面版与仓库版逐字节一致。最终临时 runtime 已移入废纸篓，项目 `.my-agent` 保持为空。
- daemon + 三入口架构演进全部完成；README、功能总览 HTML、实现与命令帮助已一致，不再残留“单进程/单入口/启动恢复询问”等旧描述。
- 2026-09-08 启动“标准 ACP + WebSocket + 重连恢复”演进；已读取完整附件与 planning-with-files 规则，先做阶段 0 真实代码核对和 ACP crate 调研，尚未修改业务源码。
- 阶段 0 完成并已向用户输出清单：确认 7 个 daemon RPC、6 类 Event、审批真实字段、axum/CLI/editor 基线；选定官方 `agent-client-protocol =2.1.0` 稳定 v1，开始阶段 A。
- 阶段 A 依赖地基完成：`agent-client-protocol =2.1.0` 已通过 rsproxy 下载并锁定，项目 MSRV 调整为 1.88；未接入代码时 `cargo check` 通过。
- 阶段 A 完成：编辑器命令已变为标准 ACP v1 server；initialize/new/load/prompt/cancel、文本/工具 update 与 request_permission 均映射到 daemon。官方 ACP Client 集成测试覆盖 allow_once 后继续执行；51/51 测试、build、fmt、严格 Clippy 全绿。
- 开始阶段 B：在保留 `/health` 与 `/v1/chat/completions` 的前提下新增 `/ws` 全双工私有 RPC 通道。
- 阶段 B 完成：axum 新增 `/ws`，首帧 connect 复用 Token 规则，请求 ID 在连接边界映射；真实 TCP 测试覆盖错误 Token、chat Event、审批回填与继续执行。52/52 测试、build、fmt、严格 Clippy 全绿。
- 开始阶段 C：先把 active 事件与审批等待从原连接解耦，新增 daemon 订阅语义，再接入 CLI/ACP/WS 自动恢复。
- daemon active truth 已升级为有界回放+broadcast，并新增 `agent.subscribe`；`chat.send` 的审批事件先进入 daemon 内部通道，再投影到连接，原连接发送失败不再终止审批等待。
- 阶段 C 完成：`session.load` 在审批等待时改走已 flush 的 JSONL 快照，避免 history 锁阻塞恢复；CLI、标准 ACP、WebSocket 均完成断线后恢复活动请求、审批与最终文本的集成测试。
- ACP 订阅会跳过已由 `session/load` 处理的旧审批，防止重复权限请求；WebSocket 重连以 daemon active request id 作为恢复事件标识。
- 阶段 C/D 最终验收：`cargo fmt --all -- --check`、`cargo test --all-targets`（56/56）、严格 Clippy（`-D warnings`）与 `cargo build --release` 全部通过。
- TUI 完成：新增 `src/entry/tui.rs`，默认无子命令进入 ratatui 全屏界面；支持输入框、消息滚动、流式文本、工具状态、Y/N 审批、Ctrl-C 取消、退出时恢复终端，以及 session.load/agent.subscribe 重连恢复。
- TUI 依赖通过项目 rsproxy 中国镜像下载：`ratatui =0.30.2`、`crossterm =0.29.0`；TUI 单元测试新增 2 项。
- TUI 最终验收：`cargo test --all-targets`（58/58）、严格 Clippy、`cargo build --release` 和格式检查全部通过；README 与功能总览 HTML 已同步默认 TUI 用法。
- TUI 视觉优化：新增 `entry/tui/view.rs`，显式深色主题、108 列居中阅读区、Markdown 基础样式、工具详情折叠、独立可翻页审批区。输入支持粘贴/Alt+Enter 换行/Ctrl+U，q 恢复为普通输入，Esc 随时退出；修正中文滚动并避免空闲重绘。
- 优化验收：59 项全量测试通过，TUI 测试覆盖 110/44/24/16 列、长回答末尾和翻页；生成 TestBackend 预览并检查。真实 PTY 以隔离工作区和占位模型配置验证启动/输入/Esc/终端恢复，未调用真实 API。
- TUI 主题兼容修复：确认当前运行环境可出现 `TERM=dumb`、`NO_COLOR=1`，原实现仍强制 RGB 前景与整屏背景，终端降级后会产生异常亮色。默认主题现全部使用终端 Reset 色和粗体/Dim 层级；`MY_AGENT_TUI_THEME=dark` 仅作为显式真彩选项。
- 主题修复验收：新增“默认主题不绘制任何固定背景”测试，60/60 全量测试、严格 Clippy、release 与格式检查通过；隔离 PTY 中启动并用 Esc 退出，终端状态正常恢复。README、仓库 HTML 和桌面 HTML 已同步。
- session 阶段 S2 完成：SessionStore 改为稳定独立 JSONL + current 指针，兼容旧 `session.jsonl` 和 `.bak-*`；列表新增首条用户问题摘要，新增 `session.resume` RPC，并在 active map/history/turn lock 的统一顺序下安全切换。
- S2 验证通过：SessionStore 新建、指针重启恢复、按 ID 恢复、路径穿越拒绝，以及 daemon 的 new/list/resume/history 集成链路均通过。
- S3 完成：TUI/REPL 启动显式 `session.new`；TUI `/resume` 显示编号、消息数、当前标记和首条问题摘要，支持直接输入编号或 `/resume <编号|ID>`；`/new`、`/status`、`/help` 同步接入。ACP `session/load` 也改为按请求 ID 切换，并保留活动当前 session 的重连订阅。
- S4 验收：64/64 全量测试、严格 Clippy、release、fmt 和 diff check 全绿；隔离 PTY 连续两次启动生成不同 session ID。桌面/仓库 HTML 逐字节一致，旧 daemon 经确认 active=0、pending=0 后已优雅停止，下次 `myagent` 会自动启动新版本。
- 最终并发审计增加 `session_switch` 锁，使 load/new/resume 快照互斥；跨 session 切换仍同时持有 active/history/turn 锁，活动当前 session 的只读恢复不受影响。补丁后再次完成 64/64、Clippy、release 与格式全套验证。

## 六项通用能力补齐（2026-09-09）

- 已读取完整需求与 planning-with-files 技能说明。
- 技能引用的模板目录缺失；已保留仓库既有规划历史并追加本轮计划。
- 当前阶段：0，源码基线与现状确认清单；尚未修改业务代码。
- 已审阅 provider/config/loop_engine/tool registry/safety：确认 provider 目前拥有 arguments 拼接与 JSON 解析，agent 侧无 canonical assembler；schema 校验在 ToolRegistry，安全审批在具体工具链路。
- 已审阅 CLI/TUI slash、SkillLibrary、daemon handler、sub-agent 与 PlanStore：确认 slash 平行实现、skill 元数据/排序现状、daemon 生命周期挂载点及原子写法。
- 已审阅 ContextManager、SessionStore、daemon server/lifecycle/runtime：确认 provider capability、后台任务退出与 cron 独立会话需要新增明确所有权。
- 阶段 0 完成：基线 `cargo test --all-targets` 64/64 通过；`cargo clippy --all-targets --all-features -- -D warnings` 与 `cargo fmt --all -- --check` 通过。
- 当前进入阶段 1：多 Provider 协议。
- 已加入统一 ApiType/capability/execution identity/ProviderEvent 契约、OpenAI/Anthropic/Ollama wire 适配及 agent 侧 canonical assembler；首次 check 的一个类型推断错误和两个 unused import 已聚焦修正。
- 多 provider/装配重构后 73/73 测试通过；严格 Clippy 仅剩一项风格告警并已修正。
- 项目一与二完成：三协议本地 HTTP mock 闭环、identity 隔离、纯增量/快照规则、整轮 fail-closed 与边界护栏已覆盖；79/79 测试、严格 Clippy、fmt 全绿。
- 当前进入项目三：共享 Slash 命令注册表。
- 项目三完成：共享注册表、daemon `slash.execute`、CLI/TUI/ACP 投影及自动 help 已落地；80/80 测试、严格 Clippy、fmt 全绿。
- 当前进入项目四：版本化 Skill 与本地安装器。
- 项目四完成：结构化 frontmatter、semver、本地安装/更新/确认删除、共享索引与稳定相关性排序已落地；84/84 测试、严格 Clippy、fmt 全绿。
- 当前进入项目五：Cron + Heartbeat。
- 项目五完成：cron.json 原子持久化、interval/五段 cron、独立 session、有限退避重试、无人值守拒绝审批、可选无模型 heartbeat、daemon 生命周期和 `/cron` 主路径均已落地；89/89、严格 Clippy、fmt 全绿。
- 当前进入项目六：自研 MCP stdio 客户端。
- 项目六完成：`.my-agent/mcp.json` 双层错误隔离、占位符边界、stdio 双 framing、initialize/tools/list/call、动态 schema 工具桥接、默认审批、安全命令/路径检查、reload 与进程清理均已落地；97/97 全量测试、release、严格 Clippy、fmt 全绿。
- 阶段 7 完成：README/HTML/规划记录已同步，最终质量门禁全部通过，当前六项能力补齐任务完成。
- 2026-09-09 启动 TUI 交互与渲染能力补齐。已按新附件完成现状审阅和规划：确认退出控制耦合中文文案、工具输出拼接再拆分、输入/滚动能力有限、全量重排无缓存、快照只取 `.first()`、主题仅 terminal/dark。当前进入阶段 1：类型化退出。
- TUI 阶段 1 完成：`should_quit` 取代“退出”魔法文案，Esc 与 `/exit` 共用类型化入口；6 项 TUI 定向测试及严格 Clippy 通过。当前进入阶段 2：结构化 UiMessage。
- TUI 阶段 2 完成：结构化 Text/Tool UI 节点已替换旧字符串拼接，工具卡片呈现状态和耗时；修正 `tool_call_id` 事件字段，定向测试与严格 Clippy 通过。当前进入阶段 3：InputEditor。
- TUI 阶段 3–8 完成：InputEditor、滚动/follow-bottom/可选鼠标、版本化换行缓存、FIFO 队列、活动 turn/审批集合和 terminal/dark/light 语义主题均已落地；恢复快照不再用 `.first()` 丢弃并发项。
- TUI 阶段 9 完成：README 与 HTML 已同步；全量 103/103 测试、格式、严格 Clippy 与 release 构建通过。
- 最终补充验收：新增主题 render 覆盖后全量 104/104 测试、格式、严格 Clippy 与 release 构建仍全绿；在隔离临时工作区以 Ollama 占位配置真实启动 release TUI，确认初始界面绘制、Esc 退出及 alternate-screen/raw-mode 恢复均正常。未发起模型请求；临时工作区已移入废纸篓，可恢复。
- 2026-09-09 开始修复“每个窗口一个独立 session”：新增 SessionRuntime、按 `(session_id, request_id)` 隔离活动请求，TUI/REPL/ACP 显式传递 session_id，session.new 不再因其它窗口活动 turn 返回 -32001。
- 首次 ACP 恢复测试因新 session 尚无 JSONL 文件被错误拒绝；open_session 已允许当前新建的空 session，恢复链路重新通过。
- 首次并发测试发现 snapshot 读取可能早于 user append flush；测试改为短轮询，生产路径继续保持 append-only flush 后可恢复。
- 多窗口实现阶段验收：`cargo test --all-targets` 106/106 通过，严格 Clippy 通过；待完成 fmt、release、安装并验证 `myagent`。
- 2026-09-09 OpenClaude 对比完成：确认其前后台 session 分层、QueryEngine/消息队列拆分、goal 状态机与稳定 ID 恢复模式；结合当前 daemon/TUI 架构选择三项低侵入高收益改进。
- 已实现：turn lock 获取支持 cancellation select；PlanStore 的 set/update/add 使用串行 mutation lock；session.list/CLI/TUI 增加 idle/running/waiting 与 active_requests 实时状态，审批等待单独显示 waiting。
- 新增回归：排队请求取消、计划并发更新、独立 session 活动状态；定向测试通过，全量 `cargo test` 109/109 通过，严格 Clippy 已通过。
- 最终验收完成：`cargo fmt --all -- --check`、`git diff --check`、`cargo test --all-targets`（109/109）、严格 Clippy、`cargo build --release` 全部通过；`cargo install --path . --force` 已完成，`myagent --version`/`myagent status` 验证为 0.1.0/stopped，命令链接到当前 release 二进制。
- 2026-09-09 开始 OpenClaude TUI 对比：确认其分层 transcript、sticky-bottom/新消息 pill、动态 prompt 高度、footer/status line、快捷键帮助和紧凑工具反馈模式。
- 已完成 TUI 改造：消息区改为紧凑层级化 transcript；底部拆分状态线/快捷键线；滚离底部累计 unread 并显示回底部提示；F1/Ctrl+/ 帮助浮层；prompt 高度随终端高度动态上限；粘贴和鼠标滚动在帮助浮层打开时不会穿透。
- TUI 渲染回归增至 14 项，完整 `cargo test --all-targets` 增至 111/111；严格 Clippy、fmt、release 构建、`cargo install --path . --force` 和 `myagent --version` 验证通过；README、仓库 HTML 与桌面 HTML 已同步快捷键说明。
- 2026-09-09 Agent 不可用修复完成：`write_file` 自动建目录；LoopEngine 对准入/装配/执行连续 3 次失败熔断；日志记录 session/request、ReAct round、Provider 首增量/总耗时、工具耗时和成功状态；TUI/CLI/ACP 展示失败 telemetry；新增 `myagent logs`，`status/sessions` 可在无模型环境下排障；系统提示补充模糊前端请求默认行为。113/113 测试、Clippy、fmt、release 通过，已安装并重启空闲测试工作区 daemon。
- 2026-09-09 TUI 优化完成：连续工具调用默认合并为可展开摘要，Ctrl+T 展示工具调用和输出细节；每个请求完成或失败都会在 transcript 和状态栏给出明确终态。新增回归后 115/115 测试、Clippy、fmt、release 构建和安装全绿；`myagent`/`my-agent` 已更新，桌面 HTML 已同步。
- 2026-09-09 Mac UX 微调：将 TUI 所有用户可见的 F1 帮助提示改为 `Ctrl+/`，保留 F1 兼容输入；115/115 测试、Clippy、fmt、release 构建和安装再次通过。
- 2026-09-09 Agent 不可用诊断完成：真实 session JSONL 证明“给我写一个前端”最终已完成，截图截取的是 `write_file` 目录不存在后的失败/恢复阶段；确认当前 `write_file` 不自动创建父目录、LoopEngine 无连续工具失败熔断、默认日志缺少 request/round/provider telemetry，且 `myagent sessions` 被环境变量校验阻断排障。未修改生产代码。
# 2026-09-12 TUI 三项体验修复

- 已收到用户“现在开始修复”的明确授权。
- 已读取 planning-with-files 技能并复用仓库现有规划文件，新增阶段 0～4 的修复计划。
- 当前处于阶段 0：读取相关 TUI 状态、布局、事件处理和测试，定位三项问题根因。
- 已确认 Ctrl+T 跳屏来自 `show_tools` 展开后仍 follow-bottom；持续运行反馈缺失来自无事件时不重绘。继续核对空白问题和当前源码基线。
- `cargo test entry::tui --all-targets` 基线通过 16/16；现有测试覆盖缺口已确认。
- 已定位依赖源码实际为 `ratatui-core 0.1.2`；首次版本目录猜测错误已记入计划。
- 已排除 Inline viewport 导致布局下沉；继续检查首帧清屏与 transcript 对齐逻辑。
- 阶段 0 完成；三项修复第一版已落地且 `cargo check --all-targets` 通过，当前补回归测试与边界修正。
- TUI 定向回归 19/19 通过；进入实现审查与全量验证阶段。
- diff check 与严格 Clippy 通过；准备全量测试和 release 构建。
- `cargo test --all-targets` 122/122 通过。
- 格式检查与 release 构建通过；下一步做隔离 PTY 烟雾测试和本机安装。
- 隔离 PTY 烟雾测试通过：清屏、顶部绘制、Esc 退出和终端恢复均符合预期。
- 已清理隔离 PTY 临时目录（移入废纸篓，可恢复）。
- 已更新 README/已知问题记录并完成 `cargo install`；正在核对 `myagent` 命令解析和最终工作树状态。
- 已同时更新 `myagent` 指向的 release 产物和 PATH 优先命中的 `/Users/pilot/.local/bin/my-agent`。
- 最终核对中发现 locale 导致 `shasum` 不可用，以及 rustfmt 带来一处无关文件格式变化；已记录，正在用 `cmp` 替代并精确还原无关 diff。
- 已澄清 lifecycle diff 是独立的语义改动并保持不动；`cmp` 证明三份安装/构建二进制逐字节一致。本轮阶段 0～4 全部完成。
- Ctrl+T 已增强为消息级 Query 锚点，定向测试保持全绿；需重跑最终全量门禁并重新安装这一版。
- 多轮非零行锚点回归通过；开始最后一次全量门禁。
- 全量测试 122/122 通过，但严格 Clippy 检出测试 helper 的生产 dead code；已用 `#[cfg(test)]` 收窄，待重新验证。
- 修正后最终全量门禁全部通过；正在把最终 Query 锚点版本重新安装到两个实际命令位置。
- 最终安装与逐字节一致性核对完成；三项问题修复已全部交付。
# 2026-09-12 项目展示 HTML 与 README 更新

- 已读取 planning-with-files 技能并新增阶段 0～4 计划。
- 已定位桌面与仓库两份 HTML，确认 Git 分支/远端同步且工作树干净。
- 当前阶段 0：审计 HTML、README 与当前源码功能。
- 已完成桌面/仓库 HTML 差异和 README 结构初审；确认桌面版过期、README 信息完整但展示层需要整体改版。
- 已完成 TUI 预览图视觉审查，决定将其作为 README 首屏主视觉，并在正文准确注明最新交互能力。
- 已核对 CLI/Slash 命令与 HTML 全部版块，形成最新能力缺口清单；阶段 0 接近完成。
- 已核对 Cargo 元数据和仓库配套文件，README 视觉方案确定为 SVG 品牌首图 + TUI 截图 + GitHub 原生 Markdown 信息架构。
- HTML/README 初稿与 SVG 封面已落盘；SVG 直接预览工具不支持，已记录并切换到本地渲染方案。
- SVG XML 校验通过，Quick Look 预览确认配色与排版方向正确；下一步进行浏览器原尺寸验收。
- 本地浏览器因安全策略不能访问 `file://`；已改用 1600px Quick Look 渲染，HTML 首屏布局验收通过。
- 阶段 0～2 已完成；HTML 标签闭合、README 本地引用和 JSON 示例检查通过，当前进入最终质量门禁与桌面同步。
- README SVG 封面已按原始 1200×420 比例完成视觉验收，四项指标完整显示。
- 桌面 `/Users/pilot/Desktop/agent-system.html` 已与仓库文档逐字节同步。
- 最终本地门禁通过：`cargo test --all-targets` 122/122、`cargo fmt --all -- --check`、`git diff --check`、敏感信息扫描均正常；进入提交推送阶段。
- 文档提交 `b455ed6` 已推送到 `origin/main`；GitHub 仓库页已读取到新版 README。内置浏览器视觉加载连续超时，按既定错误策略停止重试。
- 阶段 0～4 全部完成；补记最终状态后将再提交并推送规划记录，确保工作树干净。
- 临时预览产物已移入 `/Users/pilot/.Trash/my-agent-doc-previews-20260912`，可恢复；最终记录待提交。

# 2026-09-13 本地 Agent Web 控制台

- 已读取 planning-with-files 技能并复用仓库现有规划文件，新增阶段 0～4 计划。
- 已确认工作树存在 8 个文件的未提交改动；这些内容属于既有工作，本轮将保留并避开破坏性覆盖。
- 当前进入阶段 0：审计 HTTP/WS、daemon 生命周期、Session 持久化、slash/TUI 与现有依赖。
- 已确认 Web API 现为独立 `serve` 前台进程，默认地址 127.0.0.1:8787；下一步核对消息时间字段与 daemon handler 数据契约，再决定最小持久化扩展。
- 已确认 Session 消息缺少逐条时间/请求关联。计划以向后兼容的消息审计字段补齐新记录，同时让 Web API 对旧 Session 明确返回 `null` 时间，不伪造历史。
- 阶段 0 完成：现有 dirty 基线 `cargo check --all-targets` 通过。已确定采用每 Session 的结构化 trace JSONL、daemon `session.trace` RPC、同源静态 Web UI 和入口侧幂等 Web launcher。
- 当前进入阶段 1：先实现 trace 存储与 LoopEngine/daemon 关联，再接入 Web 查询接口。
- 已新增向后兼容的 `SessionTraceRecord` 与每 Session `.trace` 追加/读取能力，并把 LoopEngine 的 request/model/tool/turn 生命周期接到 trace；正在补辅助序列化函数和 daemon request_id 透传后编译校验。
- 阶段 1 完成：新增 `session.trace` RPC；定向测试验证完整模型输入、响应和时间字段可从 WebSocket 查询，旧 Session 无 trace 时返回空列表。
- Web UI 与 TUI `/web` 第一版完成并通过编译；trace/session/slash/TUI 共 26 项定向测试全绿。当前进入浏览器视觉与真实进程生命周期验收。
- 浏览器真实验收完成：新建 Session、Web 调用、失败态、trace 详情和控制台日志均符合预期；TUI `/web` 复用现有服务成功，未重复启动进程。
- 发布门禁阶段：`cargo test --all-targets` 131/131、严格 Clippy、fmt、git diff check、Node JS 语法检查和 `cargo build --release` 全部通过；待执行 `cargo install` 与最终工作树审计。
- 发布门禁最终复核完成：`cargo install --path . --force` 已执行，`my-agent --version` 为 0.1.0，release 与 PATH 二进制逐字节一致；隔离 Web/TUI 生命周期验收通过，临时工作区已移入可恢复废纸篓且无残留进程。
- Web 控制台阶段 0～4 全部完成；本轮修改保留在当前工作树，未执行提交或推送。

# 2026-09-13 Web Agent 工作台与工作目录

- 已收到需求：前端拆为 Session 查看与 Agent 对话，Agent 对话可选择工作目录，并将 Agent 实际开发工作提升为页面重点。
- 已读取 planning-with-files 技能并建立阶段 0～4 计划。
- 当前进入阶段 0：审计单工作区 daemon、安全策略、Session 隔离和 Web RPC 路由，确定跨工作区实现边界。
- 已确定后端方案：一个浏览器 WebSocket 只绑定一个所选工作区；Web 服务缓存每个工作区的 daemon client，目录切换通过安全重连完成，不向现有 `chat.send` 注入可变 cwd。
- 阶段 0 完成，进入阶段 1：实现 Web 工作区路由器、鉴权目录浏览 API、WebSocket workspace 握手和 `/web` 跨工作区复用 URL。
- 已核对 serve 测试和鉴权辅助函数；将保留默认 client 的内存注入能力，并为目录 API、workspace 握手与 launcher URL 增加独立回归。
- 阶段 1 完成：WebSocket workspace 握手、按目录 daemon 路由、鉴权目录浏览、同源 Origin 校验与 `/web?workspace=...` 复用均已实现。
- Agent 工作台与 Session 查看已拆成两个一级页面；Agent 页面包含目录选择、新任务、流式对话、审批/取消、实时工具动态和跳转链路入口。JS 语法检查、serve 7 项和 launcher 2 项测试通过。
- 浏览器首屏烟雾通过：默认进入 Agent 工作台、工作目录显示为 canonical project-a、对话输入和 Agent 动态均可用；继续验证目录切换与 Session 独立页。
- 目录浏览器验收通过：打开选择器、读取当前目录、返回父目录并列出两个隔离测试项目；下一步切换到 project-b 并验证独立 daemon/Session。
- project-b 切换验收通过：单一 Web 服务按需启动第二个工作区 daemon，两个 daemon 的 PID、runtime ready 和工作区日志相互独立。
- Agent 失败路径与 Session 独立页验收通过：请求绑定 project-b，Agent 动态显示失败，Session 页面完整呈现对话与 4 条结构化链路；正在修正失败消息被刷新覆盖的细节。
- Session → Agent 继续工作入口与输入聚焦验收通过；浏览器控制台无警告或错误。失败消息保留与失败状态配色已修正。
- README/HTML 文档已同步；`git diff --check`、rustfmt check 与 `node --check web/app.js` 通过，进入严格 Clippy 与全量测试。
- 严格 Clippy 通过；全量 `cargo test --all-targets` 133/133 通过（新增目录列表与 WebSocket Origin 回归）。
- 最新嵌入资源浏览器回归确认失败提示修复生效且控制台干净；正在清除首增量前失败留下的空 assistant 占位。
- 空 assistant 占位已修正；隔离 Web 服务和两个 daemon 已停止，临时工作区移入 `/Users/pilot/.Trash/my-agent-workspace-ui-lTVt11`，可恢复且无残留进程。
- 最终门禁完成：`git diff --check`、rustfmt、JS 语法、严格 Clippy、release 构建全部通过；全量测试 133/133 通过。
- 已执行 `cargo install --path . --force` 并同步 PATH 优先位置；安装版 `my-agent --version` 为 0.1.0，阶段 0～4 全部完成。
- 日期分组浏览器回归创建了多个空 Session，确认空 Session 需要按“今天”显示；已加入当天回退与日期降序排序，继续验证折叠交互。
- 日期分组首轮交互已验证：创建 3 个空 Session 后页面显示“今天 3 个 Session”，折叠后只保留日期行，展开按钮可继续恢复内容。

# 2026-09-13 Session 日志帧超限修复

- [x] 已复现/确认：`session.load` 或 `session.trace` 的整包 JSON-RPC 响应可达 7.6 MB，超过 4 MiB 协议帧上限，前端 `Promise.all` 因此无法显示任何详情。
- [x] 新增有字节预算的消息/trace 分页 RPC，并保持旧 RPC 兼容。
- [x] Web Session 页改为首屏分页、增量加载和超大字段可见截断。
- [x] 增加大 Session 回归测试，完成全量测试、格式、Clippy、JS 检查与浏览器验收。

- [x] daemon 新增 `session.load_page` / `session.trace_page`，响应限制在协议帧上限以下，并对超大字符串做递归截断。
- [x] Web Session 查看页改为分页首屏、分别加载更多消息/链路；链路读取失败时消息仍可显示。
- [x] 增加 300 KiB 多消息页预算测试与 RPC 分页回归；全量测试 134/134、Clippy、rustfmt、JS 语法和 diff 检查通过。
- [x] 通过本地 Web 服务验证 Agent 工作台、Session 查看页、空 Session 日期分组和浏览器控制台无错误；release 构建并重新安装到 PATH。

# 2026-09-13 三档 Agent 权限模式

- [x] 完成 SafetyPolicy、工具注册和 daemon 生命周期审计，确认生产工具均共享同一安全策略。
- [x] 新增 `request_approval`、`risk_approval`、`full_access` 三档模式与 `permissions.get/set` RPC；完全访问保留灾难性命令硬拦截，Cron 继续独立拒绝无人值守危险操作。
- [x] 新增 TUI `/permissions [request|risk|full]`（别名 `/permission`、`/mode`）及 Web 顶部三档权限菜单。
- [x] 新增 SafetyPolicy、slash、daemon RPC、嵌入 Web 资源回归；当前 136/136 全量测试、Clippy、rustfmt、JS 检查均通过。
- [x] 重新构建/安装并完成 Web/TUI 真实界面验收；全量测试 136/136、Clippy、rustfmt、Node 语法与 diff 检查通过，发布版已同步到 PATH。
- [x] 权限模式改动已准备提交并推送到 `origin/main`。

# 2026-09-13 Web 流式与 Markdown 优化

- [x] Agent 对话中的工具调用、工具输出和右侧工具动态默认收起，详情仍可主动展开。
- [x] WebSocket `text_delta` 改为帧级批量重绘，流式生成期间显示光标和“生成响应”状态。
- [x] 内置安全 Markdown 渲染，支持标题、段落、列表、引用、分隔线、行内代码、代码块、强调和安全链接。
- [x] 浏览器隔离验收通过；全量测试 136/136、Clippy、rustfmt、Node 语法、嵌入资源回归和 diff 检查通过；release 已重新安装。

# 2026-09-13 Web Markdown 表格与思考流

- [x] 新增 Provider 思考增量归一化：OpenAI `reasoning_content`、Anthropic thinking block/`thinking_delta`、Ollama `thinking`/`reasoning`。
- [x] daemon/LoopEngine 新增 `thinking_delta`、`thinking_finished` 事件；思考和正文在同一真实异步流中投影到 Web，Session 消息与 ModelResponse trace 保留独立 thinking 字段。
- [x] Web Markdown renderer 增加表格、对齐和安全单元格渲染；正文继续支持标题、列表、代码、引用和安全链接。
- [x] 思考区域在生成期间自动展开并显示光标，收到 `thinking_finished` 后自动折叠，正文继续显示增量光标。
- [x] 新增 Provider 与 LoopEngine 回归，验证思考片段顺序、正文切换、历史持久化和 trace；全量 `cargo test --all-targets` 140/140、Clippy、rustfmt、Node 语法和 diff 检查通过。
- [x] 隔离 WebSocket 与浏览器验收确认真实时间间隔的思考→正文流，以及最终语义 Markdown `<table>`；验收用的服务、脚本和临时工作区已停止并移入可恢复废纸篓。

# 2026-09-13 Web 工作台布局优化

- [x] 移除 Agent 工作台无必要的英文眉题，保留 Session 查看页的记录上下文。
- [x] 扩大左侧对话栅格比例并压缩右侧实时辅助栏，模型 assistant/tool 卡片改为接近内容区满宽。
- [x] 收紧标题区和 transcript 留白，保持 composer、工作目录、Session 和活动状态操作可用。
- [x] 完成 1440px 桌面与窄屏浏览器验收；格式、Node 语法、diff 检查和发布版构建安装通过，已提交并推送。

# 2026-09-13 Web 工作台截图标注优化

- [x] 将“＋新建任务”移动到顶部操作区，移除顶部工作目录/权限选择器的重复占位。
- [x] 将工作目录与权限模式控件移入 composer 底部，并保留既有 ID、禁用逻辑、目录选择和权限模式弹窗。
- [x] 为中间 transcript 增加“模型响应区域”语义和空状态说明，真实验证流式思考及正式 Markdown 响应。
- [x] 完成桌面/390px 窄屏浏览器验收；140/140 测试、Clippy、格式/语法/diff、Release 安装通过，已提交并推送。

# 2026-09-13 全局模型配置与 `/models`

- [x] 完成配置与 Provider 审计，确定 JSON 配置文件、环境变量优先级和动态 Provider 方案。
- [x] 配置持久化与环境变量加载：默认 `~/.config/my-agent/config.json`，支持 `MY_AGENT_CONFIG`/`XDG_CONFIG_HOME`。
- [x] daemon 动态 Provider 与模型 RPC：`models.list`、`models.save`、`models.use`，活动 turn 期间保护切换。
- [x] Web 模型配置/切换 UI：设置面板可添加多厂商配置、编辑、激活；无配置时 `serve` 仍可启动设置页。
- [x] TUI `/models` 列表与切换：支持编号和配置 ID。
- [x] 全量回归：143/143 测试、严格 Clippy、格式/JS 语法检查、release 构建和 `cargo install --path . --force` 通过。
- [x] Web 模型保存/激活优先走 daemon 原子 RPC；活动 turn 期间返回冲突，不会出现全局配置已变而当前 daemon 未切换的不一致状态。

# 2026-09-13 参考 pi 的 TUI 与 Agent 架构优化

- [x] 已读取 `planning-with-files` 技能并复用仓库既有规划体系。
- [x] 已读取 pi 根级 `AGENTS.md`，确认本轮只读 pi 且宽泛改动前完整阅读目标文件。
- [x] 已恢复误覆盖的长期规划记录；恢复后 `git status --short` 为空。
- [ ] 当前阶段 0：建立两项目 TUI、Agent 运行流程、测试与模块边界基线。
- [x] 已盘点 pi 的 `tui`、`agent`、`coding-agent` 关键文件与规模，并确认与当前 Rust 技术栈的映射边界。
- [x] 已完整覆盖当前 `src/entry/tui.rs`（对截断区间另行读取），记录事件泵、状态耦合、快捷键和已有能力基线。
- [x] 已完整阅读当前 `src/entry/tui/view.rs`，记录布局、渲染缓存、长会话分配热点和帮助/快捷键双真相问题。
- [x] 已完整阅读 `input_editor.rs` 与 `loop_engine.rs`，建立输入能力、Agent 生命周期、工具调度、取消与持久化基线。
- [x] 已完整阅读 pi 的 keybinding、ScrollView、EditorComponent、Input、UndoStack、KillRing 与 word-navigation 相关实现。
- [x] 已完整阅读 pi `packages/tui/src/tui.ts`，确认失效合并、输入即时渲染、焦点/overlay 和底层终端职责边界。
- [x] 已阅读 pi main-screen 全文、alt-screen 前半及 coding-agent 的 viewport/renderer 组合根，区分可迁移交互与不应重复实现的底层 ANSI renderer。
- [x] 已完整阅读 pi alt-screen 后半，并开始完整阅读 `packages/agent/src/agent-loop.ts`；已确认事件生命周期、steering/follow-up 和截断工具调用策略差异。
- [x] 已完整阅读 pi `agent-loop.ts` 与 `types.ts`，完成工具生命周期、事件契约、partial state 与恢复元数据对照。
- [x] 已确认当前 Provider 停止原因完全丢失，锁定“截断工具调用整批拒绝”为 Agent 流程候选优化。
- [x] 已完整阅读 `tool_calls.rs` 与 OpenAI/Anthropic/Ollama provider 流解析，形成统一截断事件的最小兼容方案。
- [x] pi Agent wrapper/session runtime 与 stop-reason 映射对照完成；基线全量测试 143/143 通过。
- [x] 阶段 0 完成，已选定 TUI keymap、grapheme、安全聚合运行态、非阻塞 stream poll 和截断工具 fail-closed 五项改进。
- [x] 已确认 grapheme 实现可复用锁文件中的 `unicode-segmentation 1.13.3`。
- [x] `InputEditor` 已改为 UTF-8 字符串 + grapheme 边界光标，组合字符与 ZWJ emoji 删除回归通过（3/3）。
- [x] 集中 `TuiAction` keymap 已接通，处理逻辑与帮助文案共享定义；增加 Alt+方向、Ctrl+J、Ctrl+D 等 pi 风格兼容键，TUI 定向测试 24/24 通过。
- [x] per-turn phase 与非阻塞/公平轮询 RPC poll 已实现；审批、工具、流式、等待按事实集合聚合，TUI 定向测试 25/25 通过。
- [x] 三 Provider 已统一输出截断事件，assembler 对含工具的截断响应 fail-closed、纯文本保持兼容；tool_calls 5/5、provider 21/21 定向测试通过。
- [x] 新增 LoopEngine 端到端回归：截断但 JSON 合法的副作用工具调用执行次数为 0，系统回填 `output_truncated` 后模型可安全重试并完成。
- [x] 已完成 `cargo fmt --all` 与 `git diff --check`；一次合并差异输出因体量过大被截断，后续改为按文件聚焦审阅。
- [x] 聚焦复核修正恢复订阅失败时残留 `Recovering` phase 的问题，并保留可见失败计数；keymap 兼容带 Shift 的字符型控制键，帮助补回斜杠命令入口。
- [ ] 定向回归首次误加 `--lib`，而项目是纯 binary crate；格式化已完成，测试参数待按实际 target 重跑。
- [x] 去掉错误 target 参数后 TUI 定向测试 25/25 通过；Provider/assembler/LoopEngine 截断差异复核未发现重复组装或绕过统一重试链路。
- [x] 输入编辑器二次边界审阅发现插入 ZWJ 可能合并光标两侧字素，已增加向前吸附不变量与相邻 emoji 回归。
- [x] 字素不变量修复后 TUI 定向测试 26/26 通过；依赖锁定、ProviderEvent 全部使用点与工作树差异检查完成，`git diff --check` 通过。
- [x] 复核 Provider 事件消费确认 `OutputTruncated` 统一经过 `ToolCallAssembler`，不会绕过现有 ToolAssemblyFailed 重试；README/HTML 现有 TUI 文档已定位，待同步字素与新增快捷键。
- [x] README 与系统总览已同步 action keymap、pi 风格编辑别名、字素安全和并发 phase 聚合；模块说明标出独立 keymap 边界。
- [x] 多 Provider 文档已同步 token 上限截断工具批次 fail-closed 语义。
- [x] 全量质量门通过：fmt check、all-targets check、严格 Clippy、153/153 测试、release 构建与 diff check 全绿。
- [x] TestBackend 已覆盖 110×42、44×30、24×12、16×8 及审批态；仓库没有 JSON→PNG 脚本，本轮不引入一次性渲染工具。
- [x] 字素吸附在输入末尾走 O(1) 快路径，仅中部插入时扫描新边界，避免长提示逐字输入退化。
- [x] O(1) 末尾输入快路径加入后重新跑完整质量门：153/153、严格 Clippy、all-targets check、fmt、release 与 diff check 全绿；pi 参考仓库保持干净未修改。
- [x] findings 已补齐实施结果、结构取舍与四项后续演进建议；本轮阶段全部完成。

# 2026-09-13 Web 前端体验优化

- [x] 已读取 planning-with-files 技能，并确认保留前一轮未提交源码改动。
- [x] 已完成 Web HTML/CSS/JS 基线扫描：现有工作台已具备 Agent、Session、模型、权限和目录能力，本轮聚焦层次、反馈、可访问性与响应式细节。
- [x] 进一步确认三处体验缺口：流式重绘会强制滚到底部、审批卡只存在于当前 DOM、输入框没有自适应高度/明确的键盘提示；将优先修正并补充轻量视觉层次。
- [x] 本地静态浏览器复核基线完成：默认窄视口下页面层次清楚，但顶部操作区偏轻、连接失败缺少可操作恢复入口，composer 需要更强的聚焦/状态反馈；准备按状态持久化、滚动保持和输入体验三条线实施。
- [x] 第一轮 HTML/CSS/JS 改动已完成：新增连接重试、stateful 审批卡、滚动跟随/回到底部、textarea 自适应、字数提示、焦点轮廓、状态脉冲和 reduced-motion 支持。
- [x] IAB 窄屏回归已确认：重试连接与设置入口可见、composer 字数提示可读、页面无明显横向溢出。
- [x] JS 静态语法检查通过；复核并修正审批内容拼接的运算优先级，确保已有消息时审批卡仍会显示。
- [x] 全量门禁通过：`node --check web/app.js`、fmt、all-targets check、严格 Clippy、153/153 测试、release 构建与 diff check 全绿。
- [x] 浏览器回归通过：控制台无 error/warning；窄屏 684px 下 `scrollWidth=684` 无横向溢出，重试入口可见，输入框初始高度自适应。
- [x] 已将 Web 前端改动与取舍写入 `findings.md`；后续可继续做真实 daemon 流式/审批浏览器回归，但当前静态交互和编译链已验收。

# 2026-09-26：20 项体验优化

- [x] 读取 `planning-with-files` 技能；确认工作树干净，沿用仓库已有三份规划文件并只追加本轮章节。
- [x] 完成 20 项可复现问题清单与优先级。
- [x] 逐项修复并验证。
- [x] 全量门禁、提交和推送。

### 本轮完成情况

- [x] 已固定 20 项源码可复现问题，详见 `findings.md` 表格。
- [x] 20 项已分别修复：Web 连接与恢复 1–5、提交与停止 6–9、模型配置 10–11、工作区与 Session 竞态 12–14、阅读与输入 15–18、CLI/TUI 错误处理 19–20。
- [x] 新增 16 项 Web 交互回归；前端语法检查、Rust 200 项测试、fmt、严格 Clippy 和 diff check 均通过。
- [x] 使用进程级 Ollama 占位环境完成本机浏览器连接及历史 Session 冒烟验证，未改全局模型配置，已关闭临时服务。
- [x] Release 构建与最终差异复核通过；本轮改动已提交并推送。

# 2026-09-26：再修复 50 项

- [x] 读取本轮规划技能和 Git 基线；`main` 与 `origin/main` 一致，工作树干净。
- [x] 形成 50 项有触发路径的独立问题清单，见 `findings.md` 本轮表格。
- [x] 分批修复 50 项，按编号记录修复与验证映射；Web 回归从 16 项扩至 26 项，Rust 新增订阅失效回归。
- [x] 浏览器冒烟：独立端口成功打开项目工作区、Session 历史和模型设置；编辑后“新建配置”清空旧 ID。临时服务与页面已关闭。
- [x] 全量门禁：`node --check web/app.js`、Web 26/26、`cargo fmt --all -- --check`、严格 Clippy、Rust 201/201、`cargo build --release`、`git diff --check` 均通过。
- [x] 本轮全部变更提交并推送到 `origin/main`。


---

# 实施日志

## 2026-10-01：继续任务
- 已读取工作区状态、上轮 change doc、client/handler/storage 依赖；启动 Wave 0 后基线测试。
- 应用 planning-with-files 技能，计划文件位于项目根目录。
- 当前实施 Wave 1；后续 Wave 2–7 待实施。

- 228 项基线测试在 client/storage/core 提取后通过；新增 protocol v1/client 重连测试正在运行。
- Web 配置和 workspace 启动已收口到 composition bootstrap，生产入口不再导入 daemon/provider/config concrete。
- CLI 默认 sessions 只走 RPC；旧离线清单改为显式 sessions --offline maintenance。

### Wave 1 完成交付（最终验证）

- client/storage 为真实 workspace 实现，根 facade 仅 re-export；所有入口使用统一 client，启动与配置组装移到 bootstrap，离线 sessions 显式 maintenance 只读。
- versioned v1/legacy 策略、完整请求 DTO、未知版本 fail closed；client 重连不重发 mutation，run owner/event cursor readback 有合同校验。
- 最终 `cargo test --workspace --all-features`：237 项全部通过，无忽略；Clippy `--workspace --all-targets --all-features -- -D warnings` 退出 0。
- MSRV 1.88 locked all-targets/all-features check、locked release、fmt、diff、Node 27 项与 JS syntax、cargo deny offline 四项全部退出 0。
- schema 仍为 v5；无数据迁移、无新增 dual-write。Wave 2–7 **未实现**，只完成 Wave 2 写入路径盘点，不引入半接线 migration。
- 已恢复此前误覆盖的三份长期日志；最终相对 Git 只有本轮追加，历史未删除。未提交或推送。

### 2026-10-02：Wave 2 验证进度
SQLite v6 已接线 lifetime/run generation、canonical transcript batch、tombstone、生命周期 receipt、fork、preferred session。在线与 cron 停止 JSONL append，SessionRuntime.history 移除。240 项 Rust 测试首次通过（含两后端 lifetime 合同、坏摘要 digest、真实删除重建/fork→重启→CLI/ACP/WS）；Clippy 首次发现 nonminimal_bool 与 helper 在测试模块后，正修复。

### Wave 3 实施验证（2026-10-02）
RunSnapshot、TurnState/RoundState 与原子 TurnCommit 已接入生产链路。工具整批预检、取消 join、崩溃闭合、计划 CAS 与权限冻结通过 workspace 回归（243 项 Rust 测试）；Clippy 无警告。后续 Wave 4–7 继续执行，尚未宣称完成。修复工具委派时过大的异步 future：在执行/监督边界 Box::pin；未增加线程栈或放宽门禁。

### Wave 4 持久投影阶段验证（2026-10-02）
提取 agent-context 无状态计量/验证；schema v8 支持 compact intent、source prefix CAS、projection head 与 context ledger。在线 ContextManager 从 canonical snapshot 读取投影，原始 transcript 不改写；L1 媒体投影降级、L2 工具结果投影裁剪、L3 摘要，取消/无收益拒绝，摘要失败可单独提交 prune_only/partial。workspace 245 项 Rust 测试与 Clippy 通过，包含并发后缀、竞争 head、delete/recreate 和重启遗留 intent。Wave 5 开始；手动 compact RPC、多入口 generation 合同及 ledger 校准还需在后续集成收尾，不宣称整套验收完成。

### Wave 5 实施阶段验证（2026-10-02）
agent-memory 提供 typed scope/layer/kind、可见性复核与确定性 ranking；SQLite v9 保存记忆、legacy quarantine 与 committed-turn ingest receipts。在线 remember/recall 不再写 JSONL；runtime bounded recall 独立检索分区，terminal 发布后单次有界摄入，维护失败不改 terminal。memory store/recall/list/forget/scope RPC 接通，context_read_only 同时阻止 memory 写入/forget/compact。248 项 Rust 测试及 Clippy 通过，覆盖过滤后 limit、TTL、全局确认、legacy 隔离、重启与迟到 lifetime fence。接下来推进 Wave 6，三入口集成合同及完成后维护诊断仍在最终集成清单。

## 2026-10-02：Wave 2–7 最终验收完成

- SQLite schema v12：canonical transcript/lifetime、atomic TurnCommit、tool pairs、plan、compact projection/source CAS、memory scope/ingest/forget receipts、background resources 和维护诊断全部接线。
- SessionSupervisor/RunCoordinator 持实际锁、实例、冻结能力和任务；memory/context/resources 在 CLI、ACP、WS 三入口 kill/restart 后交叉读回一致。
- 完整 JSON Schema、冲突 waves、有界 artifact 范围与流式完整性校验、后台 Native stop/join；Docker exec 的强隔离准入及 auto 显式降级。
- MCP HTTPS/TLS/DNS pinning/完整 SSE/receipt/budgets；env/Keychain 生产适配与明文迁移读回/原子发布；doctor、queue/frame/list 背压、artifact GC、人工 orphan reconcile。
- 最终命令全部退出 0：workspace all-features tests 261，Clippy -D warnings，Rust 1.88 all targets，release，fmt/diff，Web 27/语法，离线 cargo-deny。
- 修正最后两项竞态/兼容：后台测试等待 durable ready 日志后停止，不依赖 10ms 调度；legacy chat DTO 保留 sandbox 字段，禁止静默丢弃政策；自动记忆摄入记录 lifetime+transcript seq 来源 ID。
- 部署验证限制明确保留：本机 Docker 没有预装 alpine:3.21，未拉镜像；后台 Docker、其他平台 native secret adapter、OAuth 未实现；真实 Keychain/远端/GitHub CI/在线公告未验证。所有限制已写最终文档，未把这些路径描述为已验收。
- 没有 commit/push，没有覆盖用户已有修改。

## 2026-10-02 Codex 源码对比
只读clone /tmp/codex-architecture-reference，官方HEAD固定。研究与实施计划已记录，开发前保留全部Wave改动；开始上下文与记忆三项改进。

研究实施中发现的门禁问题：memory crate 新增 JSON 渲染依赖后，架构依赖白名单拒绝 serde_json；明确仅允许序列化依赖，继续禁止 storage/context/daemon 边。新增摄入测试首次误用了 TranscriptRepository trait 名，改为实际 TranscriptStore 后编译通过。没有放宽运行时 owner、CAS 或作用域门禁。

## 本轮验证结果

最终代码：`cargo test --workspace` 266通过、0失败/忽略（含9项真实daemon合同）；Web 27通过；Clippy workspace/all-targets/all-features `-D warnings`、Rust 1.88 locked check、release locked build、cargo fmt、git diff --check 全部通过。cargo deny offline 的 advisories/bans/licenses/sources 通过（使用缓存公告库，既有重复版本警告仍保留）。

日志：`/tmp/codex-comparison-tests-final.log`、`/tmp/codex-comparison-clippy.log`、`/tmp/codex-comparison-msrv.log`、`/tmp/codex-comparison-release.log`、`/tmp/codex-comparison-web.log`、`/tmp/codex-comparison-deny.log`。未调用真实摘要服务，未证明提炼质量或成本收益；验证的是宿主上下文传递、预算、存储边界及现有合同。SQLite仍为v12，无迁移；原有Wave改动全部保留，未commit/push。
# 2026-10-03 数据飞轮

- 工作树基线干净，复用已有规划和源码对比文档；不覆盖历史记录。
- 阅读 OpenAI Docs 与 planning-with-files 技能；打开官方 compaction 文档与用户指定 GitHub 页面。
- 开始核对 Codex 使用统计/保留策略与本项目召回、摄入、维护路径。

- 开发前完成 docs/research/codex-data-flywheel.md：明确取舍与六步方案。
- SQLite v13 曝光/评价/回执/恢复退避；自动注入、工具召回、RPC 召回共用反馈策略；新增 flywheel report/feedback/evaluate；17 项独立 JSON 质量样本。
- 新增真实存储的幂等、重启、作用域、只读、删除、schema12 迁移测试及真实 daemon 重启反馈合同；自动摘录常见凭据行过滤。

## 最终验证与交付

2026-10-03：SQLite v13、lexical-feedback-v1 与 flywheel CLI/RPC 完成；代码共用既有 transcript/TurnCommit/context ledger，没有另建会话状态或轨迹真相源。维护日志与反馈保持独立，常见凭据过滤不改原文。

- `cargo test --locked --workspace --all-targets --all-features`：276通过，0失败/忽略，包括10项真实daemon合同。
- `flywheel evaluate`：17/17独立JSON质量样本；debug与release入口均验证。
- 严格Clippy、Rust1.88全目标全特性locked检查、release locked构建、fmt、diff通过；Web27/27与JS语法通过。
- 离线cargo-deny：advisories/bans/licenses/sources通过，保留既有重复依赖警告；初次缓存锁因沙箱只读失败，自动审批后成功，未联网更新公告。
- 日志：/tmp/flywheel-tests-final.log、/tmp/flywheel-clippy.log、/tmp/flywheel-msrv.log、/tmp/flywheel-release.log、/tmp/flywheel-web.log、/tmp/flywheel-deny.log、/tmp/flywheel-evaluation-final.json。
- 未调用真实模型服务，不宣称生产质量、成本或成功率收益；未commit/push。Codex参考仍为已固定源码提交，并非远程最新HEAD。

## 2026-10-03：继续研究

用户要求继续研究、不要停。保留数据飞轮全部改动，进入压缩质量、来源证据与上下文传递的第二轮研究；不重复上一轮工作。

第二轮：当前轮次持久保护、推断摘要包装与递归上限拒绝、15项独立结构门禁、受限memory.evidence读取及健康积压批次排空已接线。完整测试发现积压恢复正确，但memory.list默认只返回20项；夹具显式limit50核对40项，不改变生产分页。继续完整回归与上下文交接研究。

第三轮实施：增加可选context_source_ids、委派事务捕获并核实来源、child快照持久包，来源点查与凭据过滤复用于记忆证据。旧snapshot默认None，孙任务不隐式继承祖先包。正常与overflow重组同样加入retrieved分区并走完整预算。首次check发现一个嵌套测试literal漏加默认空来源，已补齐。

第四轮：v2锚点兼容旧数组，并增加文字覆盖状态与省略下限；checkpoint不可恢复原始完整标识。包装整体预算含元数据并最终复核；零预算的覆盖诊断仍保存在宿主摘要JSON。独立评测扩展6项，合计21项上下文保护/保留门禁。

## 2026-10-05：第二至第四轮统一最终验证

- Rust workspace/all-targets/all-features：289项通过，含真实daemon11合同。
- Web27项与语法检查通过；debug及release的flywheel evaluate均为检索17/17、上下文21/21。
- Clippy -D warnings、MSRV1.88 locked全目标、release locked、fmt/diff通过。
- cargo-deny offline：advisories/bans/licenses/sources均通过，保持既有duplicate warnings；使用已有公告缓存，不宣称实时漏洞库。
- 验证日志位于 /tmp/retention-tests-final.log、retention-clippy-final.log、retention-msrv-final.log、retention-release-final.log、retention-web-final.log、retention-deny-final.log；release评测JSON /tmp/retention-evaluate-release.json。
- Codex应用已创建并查看ACTIVE heartbeat：Agent Rust 持续研究与优化（agent-rust），每12小时在本对话延续研究与开发，无实质新进展保持安静。
- 保留所有工作区改动，未提交/推送/部署。

## 2026-10-05：Runtime 架构要求复核

HEAD35af558，工作树干净。应用 planning-with-files，保留既有日志；基线命令 cargo test --locked --workspace --all-features 经本地 socket 环境审批后全绿。新建 ADR0002 和实施 change doc，业务实现之前明确只读恢复与生命周期请求屏障，记录后续未实现项。

### 本轮最终验收（2026-10-05）

Rust 基线289→302项，全目标/全特性通过（真实daemon11、架构10），Web27；fmt、严格Clippy、MSRV1.88、release、offline cargo-deny、diff通过。日志 /tmp/runtime-refactor-{baseline,tests-final,clippy-final,msrv,release,web-final,deny}.log。Socket/公告库锁需要环境审批后重跑，均通过。修复v14降级fixture、marker路径、strict run snapshot兼容和本地参数校验RPC错误回归；原业务断言未放宽。文档纠正旧Wave0–7全完成表述，明确未实现项；代码与文档保留为工作树改动，未提交/推送/部署。

### 云端提交授权

用户明确要求“提交到云端”。已复核当前 main/origin 与本轮变更范围，执行常规提交和推送，不强制覆盖远端。


# 实施日志

- 2026-10-05：读取需求、根 manifest、README、ADR 0001/0002、既有变更记录和核心计划路径。记录初始 HEAD/干净状态。
- 基线 cargo test --locked --workspace --all-targets --all-features：沙箱内退出 101，daemon-client 四项 socket EPERM；日志 /tmp/control-waves-baseline.log。申请同命令环境重跑。


### 控制面 Wave 1 验收（2026-10-05）

版本化计划纵切完成：SQLite v15、稳定定义身份/摘要、精确执行/废弃、pending 重启不自启、旧数据证据保留、CLI/ACP/WS/HTTP 合同。Rust 315 项全过，Clippy/MSRV/release/Web28/fmt/check/offline deny 通过；日志与失败修复证据见 docs/changes/governed-runtime-controls.md。下一步 Wave 2 hooks；Wave 2–8 尚未完成。

### 控制面 Wave 2 验收（2026-10-05）

332 项 Rust（真实 daemon19）、Web29 与所有本波门禁通过；离线 deny 使用既有缓存，未在线刷新。新 hooks/default-disabled 配置、v16 审计/outbox/continuation、审批真实 revision、manual/auto compact 顺序、失败重启不重执行和三入口 readback 完成。

失败修复：Serde unit effect 忽略未知字段→严格空 struct；stdin shutdown 后 drop→真实 EOF；shared run decoder 校验新增关联身份；compact 失败重试保留失败；continuation 预算不再借 for_delegation 改写 frozen context policy；最终文本/大输入同样限 token，流式发布前中断。正常进程 fixture 超时调至允许上限5秒，专门100ms超时测试保留。原 memory backlog 合同在并行负载失败后独立诊断与全回归均通过，未更改业务断言。

日志 `/tmp/control-waves-wave2-{tests-verified,clippy-final,msrv-final,release-final,web-final,deny-authorized}.log`。README/roadmap/known issues/ADR/change/Hook API 已同步事实；未提交/推送/部署。继续 Wave 3。


## 2026-10-06：heartbeat第五轮实施与验证

已按研究→计划→RED→修复→GREEN完成工具结果元数据预算增量：共享计量补name/tool_call_id，采用饱和累加；固定8项独立预算样本和真实OpenAI出站合同验证，保持thinking排除、参数单次计量、图片与当前轮次保护。flywheel evaluate记忆17/17、上下文29/29，旧计量24/29。Codex固定8d44977/workspace 0.0.0，不标最新。

验证隔离：原工作区含另一轮Wave 3开发，全部保留。捕获快照全测因5项storage迁移断言（v17与旧预期v16）退出101，Clippy因compact_runs既有复杂元组退出101；context与OpenAI本轮合同通过。固定已提交a8ac3dc基线加相同增量303项Rust（11项真实daemon）、严格Clippy、MSRV1.88、Release、Web27、fmt、离线deny通过。本轮不修他轮代码或冒充整体完成。

原目标文件与基线SHA256相同，应用前再校验，代码只写context增量、OpenAI测试和budget JSON；根记录只追加。本地v4主库无飞轮表，反馈证据不可得，未动生产数据或作用域。日志/private/tmp/agent-budget-heartbeat-ifpr94gm/heartbeat-{budget,base}-*，完整合同和失败记录见docs/research/codex-tool-metadata-budget.md。未提交、推送或部署。

应用复核：本轮3个代码/评测文件与通过门禁的隔离验证内容逐字节一致；应用后原仓库`git diff --check`退出0，HEAD仍为a8ac3dc，无新增提交。验证摘要保存在`/private/tmp/agent-budget-heartbeat-ifpr94gm/heartbeat-verification-summary.json`。


### Wave 3 验证过程（2026-10-06）

真实native合同已GREEN，Started先于Provider返回、订阅断线只detach、旧聊天owner取消不影响compact、取消后可聊天、kill/restart unknown且不重摘要；三个入口逐字段对比native receipt，并验证HTTP原operation重取/来源身份冒领拒绝。双后端CAS/幂等/busy/terminal故障回滚/损坏与v16迁移备份/未来拒绝通过。多次完整回归曾全绿；最终增量再次门禁进行中。保留早期迁移夹具缺drop新表、菜单顺序改变、frame测试pattern guard move编译错误等日志；已修源码而未删除或放宽断言。原记忆排空间歇失败保留并以稳定RED修复due时序，不增加测试等待。


### 控制面 Wave 3 验收完成（2026-10-06）

独立 compact、v17迁移、协议/daemon/CLI/TUI/Web/HTTP纵切与旧unary同owner适配已闭合。全部门禁退出0：Rust341（daemon20）、Web31、严格Clippy、MSRV1.88、Release、fmt/JS/diff和缓存离线deny；日志与修复证据见docs/changes/governed-runtime-controls.md。下一步Wave4冻结generation工具搜索/dispatcher；Wave4–8仍未完成，未提交/推送/部署。


### 控制面 Wave 4 接线与终审（2026-10-06）

core字段检索/严格dispatcher DTO、SQLite v18发现receipt和取消发布屏障、冻结schema/小Provider工具面、同dispatcher preflight/resource/safety链、run.discovery和/tools三入口已接线。真实RED保存在/tmp/control-waves-wave4-red.log；搜索/调用/回填、否定准入、跨轮wire id、索引并发复用、取消worker和迁移/回滚/腐败测试已加入。旧审批fixture扩为先搜索再nested invoke，原安全/副作用断言保留；仅core子工具面维持原1项断言。正在运行最后完整门禁，尚不标记本波验收完成；后续Wave5–8未实现。


### 控制面 Wave 4 验收完成（2026-10-06）

v18迁移、core检索/strict DTO、冻结小Provider面、统一nested schema/资源/安全审批链、取消publication屏障、readonly RPC与三入口已闭合。最终Rust350（真实daemon22）/Web31及fmt、严格Clippy、MSRV1.88、release、JS/diff、缓存离线deny全部退出0；日志与修复记录见docs/changes/governed-runtime-controls.md，API docs/tool-discovery-api.md。下一步Wave5共享revision-aware reducer；Wave5–8未实现，未提交/推送/部署。


## Wave 5 完成：共享 revision reducer 与持久版本封存（2026-10-06）

agent_core 是唯一展示时序规则；SQLite v19 event_view_stamps 是版本事实源。所有事件发布在原 mutation 事务封存最终全版本向量；读取校验 SHA/full ExactOwner，缺戳历史不追补。SessionReadback::supersedes 委托同一 reducer。run_owners additive 读取严格校验；嵌套 slash、native compact、终态和交互回执也接线。去掉 private seq 推断和入口各自比较路径。

Rust DaemonClient 各消费者独立游标，projector actor 保持 next 可取消安全；有界队列溢出 detach。Web 通过纯 views.reduce RPC 调用完全相同函数，256 帧/8MiB 有界串行队列；Gateway 只转协议。resync 先单事务 page 基线，再 32 条 durable 页补充仍活动完整 owner，ViewResynced 不是业务终态。原始审计数据保持，返回帧保留元数据剥离后按 repository 来源添加。

RED /tmp/control-waves-wave5-red.log 缺 RPC 真失败。核心 4 个领域测试覆盖乱序、同版本补充、IR 冲突、多 run、退休 life、durable replay。storage 新 3 项双后端/封存/迁移测试覆盖零写读取、损坏和 fault 回滚。真实 daemon 新 2 项 revision_reducer_rejects_delayed_readback_and_retired_lifetime_over_socket、shared_client_resyncs_real_dropped_frame_and_duplicate_without_provider_replay，后者 proxy 真丢 TextDelta/重复 Started，Provider 次数保持 1。重启 direct run.read 需要完整 source 初始化展示已修复。新增 stamp 使旧测试 helper 误判 compact 为 session，现按完整 compact DTO 逐字段比对，未放宽旧断言。Web 新 4 项连同原 31 项通过。

门禁均退出0：cargo test --locked --workspace --all-targets --all-features 359 项含24真实daemon（/tmp/control-waves-wave5-tests-final-corrected.log）；严格 Clippy（clippy-final-corrected）、MSRV1.88（msrv-final）、release（release-final）、fmt（fmt-final）、diff（diff-final）；Web35（web-final）、JS语法。cargo deny --offline check --hide-inclusion-graph 四项通过（deny-final），仅已有缓存，未在线刷新。API docs/view-reducer-api.md。

Wave6–8尚未完成；真实商业 Provider/MCP、Linux、生产慢消费者和 CI 未验证；未提交/推送/部署。


## Wave 6 设计补充（实施前）

Provider 发送前只使用 agent-context 的纯 assembler；同一输入同时产生规范化消息/工具、ContextEnvelope、分区计量、能力降级与请求摘要。SQLite v20 前进迁移保存完整 exact owner 的不可变 request capture 与 SHA；包括该次调用的 source revision/generation、冻结完整 catalog、实际小工具面、route/provider capability、校准与 policy。captured retrieved/overlay 保存为当时材料，不在只读路径重新启动 Git/MCP/Provider/skill 搜索/memory ingest。

context.readback 在同一只读事务核对 current lifetime、owner、capture SHA、source 和冻结 snapshot，再以同一个 assembler 重建。输出明确区分当时可重建的 capture 和当前不可重放的动态环境；不声称其等于后来状态。无 capture 的旧 run 报明确 unavailable，而非空上下文。默认仅元数据/摘要，显式本地诊断才返回经脱敏的完整材料；memory 分区不返回原文，当前权限变化禁止完整诊断。损坏 fail closed。


## Wave 6 完成：实际请求 capture 与零写共享重建（2026-10-06）

状态 owner 为 SQLite v20 provider_requests；保存模型/fallback/自动与手动摘要的发送前不可变材料，SHA与ContextEnvelope同事务。实际 Provider 发送与只读 context.readback 都使用 agent-context::assemble_request；删掉 loop_engine 原独立 envelope 计量与媒体降级路径。ContextManager 摘要也接入相同 assembler，避免旁路。cron 新准入保存真正冻结 RunSnapshot，旧 usage fixture 改 canonical 准入，原安全/重启/预算断言保留。完整瞬时 DTO 在 core，不建立 context 私有历史 owner；架构测试未放宽。

capture 明确是当时可重建材料，当前动态环境/skill/plan/memory不重放。默认只返回类型化 envelope/digest，显式 local 诊断保守隐藏凭据/memory/媒体/工具参数/摘要原文；权限变化或凭据不可核对拒绝。读取同一 readonly 事务核对 full owner/life、冻结政策、catalog、source prefix、projection与 SHA，无新 Provider/MCP/runtime/compact/ingest。旧历史无 capture 明确 unavailable，不补身份/空请求。CLI/TUI/ACP v1/WebSocket/HTTP共用 handler；Web 只读不造 chat/run 终态。API docs/provider-context-api.md。

真实 RED /tmp/control-waves-wave6-red.log 缺 context.readback 方法。新纯 assembler 2 项、storage 2 项双后端/迁移/fault/损坏、协议 strict DTO 1项、真实daemon 1项和Web1项；既有compact合同补实际摘要capture与下一chat generation。真实wire消息/小工具SHA与返回一致，三入口及HTTP逐字段相同，读回前后clock/event不变，kill/restart调用次数仍1，权限变化/损坏拒绝。测试 helper 识别新增 DTO，保持完整 JSON 精确比对；child16K预算与route32K取冻结有效最小值，原断言保留。中间失败日志保留，未通过降低测试换绿。

最终门禁全部退出0：Rust365（含25真实daemon）/tmp/control-waves-wave6-tests-final-complete.log；严格Clippy、MSRV1.88、release分别 clippy/msrv/release-final-complete.log；fmt/diff；Web36 web-final-complete.log、JS语法。cargo deny --offline check 四项通过 deny-final.log，仅已有缓存，未刷新在线公告。无新外部依赖，Cargo.lock未修改。Wave7–8未完成；商业Provider、真实MCP/用户hooks、Docker/Keychain、Linux与CI未验证；未提交/推送/部署。


## Wave 7 设计补充（实施前）

ACP 默认仍运行 SDK 稳定 v1；editor --acp-v2 显式选择 SDK Agent.v2()/V2ConnectionTo，并仅接受物理连接初始化协商出的 v2。连接保存的只有不可变能力/版本指纹，不保存 plan/session/compact 第二事实。SDK v2 当前没有标准 execute/discard/compact-start 请求，使用命名空间扩展方法，通知优先标准 PlanMarkdown、CompactionUpdate、StateUpdate、RequestPermission。扩展 metadata 严格 schema/fingerprint/完整生命周期及 exact owner；未协商、重复初始化、未知身份字段在 mutation 前拒绝。

prompt 准入后及时响应 accepted，后台消费共享 DaemonClient reducer 的 canonical 事件，终态只来自 durable readback/run；长 prompt 不阻塞控制请求。resume 先单事务 sessions.read，再按 durable active owner/interaction 恢复，不切换业务 session、不重放 Provider；断线 detach。plan/compact/管理命令委托既有 daemon owner，exact cancel 不使用客户端 reservation ID。优先保持原始数据库 schema v20，无新增业务事实或持久迁移。


## Wave 7 完成：真实 opt-in ACP v2 与有界传输（2026-10-07）

稳定 v1 默认路径保留，--acp-v2 使用 SDK Agent.v2/V2ConnectionTo/标准 v2 类型；物理连接一次严格协商 schema/fingerprint/capability。未协商、未知身份、版本/重复/热变更在 mutation 前拒绝。计划 Markdown/execute/discard、compact 持久 Started/真实结算/exact cancel、pending interaction 恢复、session readback/new/list/resume/close/delete 共用 daemon 命令；SDK 缺标准请求的操作采用命名空间扩展，未伪造 v1 metadata 为 v2。

prompt 的 durable Started -> accepted response -> 后台 stream 不阻塞 inbound；标准 permission popup Cancelled 不提交决策。canonical message_ids 从真实 batch/native turn 派生，readback/page/replay 一致；foreign batch fail closed。close 使用现有 lifecycle_receipts，取消 captured exact owners 并等待真实结算后发布一次 SessionEnd，保持 lifetime/transcript；重复 receipt 不取消新工作。schema 保持 v20。

v1/v2 物理预算 256 帧/8MiB、4MiB 单帧、128 tasks；v2 请求32。真实慢 stdout 合同首次 RED 发现 Tokio 阻塞 stdin 关闭问题，改为有界独立 I/O 桥接，stderr 测试夹具单独排空；budget detach 不改 canonical，不取消 run，不重放 Provider。严格默认 UI Cancelled 保留 pending。新合同包含严格协商、三入口计划、提前 compact/长 prompt/close、interaction 断线恢复和真实慢消费者。未删除/放宽既有测试。

最终门禁全部退出0：Rust376（30真实daemon）/tmp/control-waves-wave7-tests-accepted.log；严格Clippy/MSRV1.88/release 分别 clippy/msrv/release-accepted.log；fmt/JS/diff static-accepted.log；Web36 web-accepted.log；离线 cargo deny 四项通过 deny-accepted.log，仅已有公告缓存，未在线刷新。Cargo.lock 未变，无新增外部包。API docs/acp-v2-api.md。真实 IDE/商业Provider/MCP/Docker/Keychain/Linux/CI 未验证。Wave8 尚未完成；未提交/推送/部署。


## Wave 8 设计补充（实施前，2026-10-07）

在 Wave1–7 门禁已闭合后依次迁移 sandbox -> runtime -> daemon -> entry-support -> CLI/TUI -> ACP -> Gateway/Web。每个 crate 迁移后先 workspace 编译与目标测试，再进行下一步。根 binary 只组合 host 服务、参数和启动；共享 DTO/SQLite owner 不复制。runtime 不反向依赖 daemon/入口，sandbox 接收已冻结 image/owner 和取消端口，不取 repository。生命周期启动/配置组合通过真实 host 端口注入 Gateway，入口生产依赖仅协议/client/共享展示支持，测试可使用 daemon fixture。已有 facade 在迁移期间短暂使用，完成全部删除。

保持 SQLite v20、配置/socket/binary/HTTP/WS/CLI。安全系统调用改用已锁定 rustix 的 safe fd API，保持 O_NOFOLLOW/openat/目录句柄/CAS/原子 rename 与进程组 kill；不退化成路径 reopen。根进程加载环境仅在多线程 runtime 建立前完成，库内移除 unsafe 环境写入。增加真实物理路径/manifest/AST 依赖与 unsafe 护栏，原断言保留迁移后的对应代码，禁止用 src facade 掩盖 owner。Cargo.lock 只允许新增内部 package 及已锁依赖的使用边，不刷新第三方版本。回滚为工作区代码恢复，持久 schema 不做倒退。


## Wave 8 完成与最终验收（2026-10-07）

七个新库按 sandbox→runtime→daemon→entry-support→CLI/TUI→ACP→Gateway/Web 顺序迁移，每个均先 workspace check 与目标测试。根仅 main.rs/bootstrap.rs；旧根业务模块及 client/storage/protocol facade 全部删除。唯一 owner、实际物理边与非默认 fixture features 由 12 项架构检查保护。库 forbid unsafe，文件能力使用已锁 rustix safe fd API 保留 no-follow/目录句柄/CAS/原子 rename，环境加载在根多线程 runtime 前执行。配置/binary/socket/HTTP/WS/CLI/schema v20 保持，Cargo.lock 第三方身份/checksum 与基线完全一致。

最终审计补真实 v1 permission RED：SDK 容错解码吞入 v2 owner metadata 并错误执行文件。共享 raw Value strict decoder 在审批前拒绝根/selected 的 my-agent v2 控制字段，保留标准其他 namespace；GREEN 验证 canonical pending 不变、无写文件/Provider 重放，v2 重连可精确恢复批准。日志 /tmp/control-waves-wave8-v1-permission-red.log 与 green-fixed.log。独立 crate 的 policy SHA 序列化、safe fd、锁损坏和测试时间构造问题均修复，原行为断言保持。

修复后所有门禁退出0：Rust379（31真实daemon/入口、12架构）/tmp/control-waves-wave8-tests-audited-final.log；strict Clippy/MSRV1.88/release 对应 clippy/msrv/release-audited-final.log；fmt/JS/diff static-audited-final.log，文档收尾再检查 delivery-static.log；Web36 web-accepted.log。cargo deny --offline check --hide-inclusion-graph 四项通过 deny-accepted.log，仅已有公告缓存，未在线刷新。完整逐项 owner、迁移、删旧路径、兼容、失败、测试和实际环境边界见 docs/changes/governed-runtime-final-report.md；物理 API docs/runtime-crates.md。

八波实现与本地门禁全部完成。商业 Provider/真实 IDE v2/MCP/用户 hook/Docker成功隔离/真实Keychain/Linux/CI仍未验收；不声称这些生产环境已通过。没有提交、推送或部署。


# 企业升级执行记录（2026-10-07）

- 读取用户附件完整规格，核对 git HEAD/status、workspace、现有架构测试、物理 crate 清单、ADR 与 CI。
- 应用 planning-with-files：保留既有规划历史并追加新任务。当前进行 Wave 0 基线审计和可执行门禁。

- 启动完整基线 Rust 回归，输出 docs/enterprise/evidence/baseline-tests.log；生成基线物理依赖/测试名索引。

- 真实 RED（exit 101）见 docs/enterprise/evidence/wave0-red-quality.log：旧 AST 没有检测 std::println/core::todo/std::unimplemented。修复宏路径识别；新增 owner、Cargo alias/条件/继承、channel 构造站点护栏与夹具。四生产无界站点 + 一测试 feature 例外精确登记，尚未宣称有界化。

- 首次新增护栏编译发现 syn::Error 不支持 PartialEq；整数判断改为 parse().ok() == Some(0)，生产代码未受影响。日志 wave0-architecture-green.log 保留后续另存结果。

- Wave 0 指定九项命令退出0，机器报告 docs/enterprise/evidence/wave0-gates.json；Rust 387/0失败/0忽略，其中20架构/31真实daemon，Web36。cargo deny 在线命令四类通过，保留已有 duplicate warnings。开始隔离副本真注入三类违规，原工作区不变。

- 隔离副本实际三次违规均被架构门禁拒绝（exit101 且断言命中，非编译失败）；报告 wave0-red-fixtures.json。逐表对账纠正文档计数为48，所有 DDL 表均在 schema-v20.md，无漏表；schema-v20-inventory.json 保存列表。

- 企业 Wave 0 locally verified 完成，报告 docs/enterprise/wave0-report.md。原 schema v20/所有业务路径/锁文件未改；Wave1–10仍未实现，不复用旧个人波次完成状态。文档收尾后再做静态/架构检测。

- 收尾更强 owner 登记/例外去重检查后，九项完整门禁再次全0；Rust仍387/Web36。三真注入 RED 全命中；新文件无尾随空格、文档链接存在、Cargo.lock 与 HEAD 字节完全相同。

## 企业 Wave1-S1 实施中
真实 daemon identity.readback RED 返回 -32601，日志 evidence/wave1-s1-red.log。新增 core 企业DTO、纯 identity/policy 库、v21身份/授权迁移与同一准入事务、中央 RPC 策略 gate、CLI/ACP v2/HTTP投影。首次编译发现测试字符串推导歧义，已改显式String；首轮全测试发现8个旧schema降级fixture保留了v21表，正修复fixture真实删除新schema而非让生产迁移忽略重复表。新的事务合同正在验证策略freeze、revocation、故障回滚和并发幂等；尚未验收Wave1。

Wave1-S1验证修复：新授权合同的stale-policy用例误用了另一个request ID，先触发合法ProtocolViolation；保留ID绑定约束，测试改为同ID的旧策略请求。/identity放入slash末尾，保留原TUI菜单首项和键盘顺序。真实parity测试初版未创建session，被原生命周期准入正确拒绝；补显式创建而不放宽准入。此前失败日志全部保留。

Wave1-S1真实parity已通过（5.07秒）。首次九门禁的test/clippy/MSRV被新增opaque ID测试的try_from模糊.into阻断，已改.to_owned保留类型校验；runner继续执行其余门禁并保留失败记录，不把部分绿视为验收。

全workspace实际运行后发现 delegated/hook child 已经在 canonical 准入事务创建，旧幂等读回缺授权被新检查拒绝。现扩展原 child 准入事务继承经完整父owner/epoch/SHA核验的授权与更窄原snapshot；不能从请求补造历史授权。升级前 Local 旧run仍在当前可信授权后可幂等读回（snapshot=null），不对未知/外域主体开放。真实compact合同的schema断言20随实际migration更新为21，其余副作用/读回断言保留。失败记录 tests-reviewed.log 保留。

## 企业 Wave1-S1 本地验收完成

完整门禁 evidence/wave1-s1-final-gates.json 九项全部exit0，402 Rust（包括20架构、32真实daemon、7新增真实SQLite授权合同），36 Web，在线cargo deny通过。真实入口/故障/restart证据已写 docs/enterprise/wave1-s1-report.md、identity-api.md、schema-v21.md、ADR0004。第三方lock所有package条目与HEAD逐字段比较完全相同。完整Wave1仍未完成，Wave2–10尚未实施；没有提交/推送/部署。


### 企业 Wave2-S1：事务发布协调（2026-10-07）

- 已写 core DTO/lease reducer、两独立 adapter、v22/schema v1、same-transaction outbox、本地 publisher/receipt/ack、六种入口投影。
- 已保留真实 RPC RED 与 PG worker regression/55P03 RED；修复异步驱动交接后 PG6项通过。最终九门禁和完整域 FK 复验尚在汇总。
- 原 Local/native owners 保留；enterprise_ready=false。没有提交、推送或部署；未声明完整 Wave2–10 完成。


### Wave2-S2 文件 artifact 实施

真实 symlink RED 已保留。固定目录句柄/no-follow/CAS/原子 NOREPLACE/fsync、摘要和原 tool receipt 读回核验已接线；8 文件合同与真实 daemon 六入口/重启合同通过。正在跑九门禁。发现原 store_owned_output→finish_tool 的引用提交窗口可能被跨进程 GC 删除；下一切片改原 tool receipt 事务发布，不建第二业务状态机，schema 保持 v22。PostgreSQL lane 已再次真实通过6项，证据按切片独立保存；脚本新增 --wave 以避免覆盖早期证据。


### Wave2-S2 门禁与 S3 File 两阶段发布

S2 九门禁全退出0：420 Rust、6 PG 显式 ignored（独立真实 lane六项通过）、36 Web；34真实daemon。S3新增 core ToolArtifactCompletion、私有 staged File 能力和原 tool receipt 的 BEGIN IMMEDIATE 发布事务；生产 runtime 已替换先存文件再独立 finish_tool 的路径。13 文件合同通过；真实第二数据库连接 Barrier/写锁合同、真实子进程在 finalize 后 commit 前 kill 并重启合同通过。Clippy发现 test module不在文件末尾和未用变量，已修结构和变量，未放宽 lint。此前 sessions_read 测试方法名错误与缺 trait import 已修，不作为 RED。


### Wave2-S3 验收记录

九门禁 wave2-s3-file-final-gates.json 均退出0：427 Rust/6 PG ignored/36 Web/34真实daemon/20架构；独立真实PG六项通过。新增对象根目录时父目录 fsync 审阅修复后，重新跑 reviewed 全部门禁。File正式对象、原 tool receipt 和实际摘要不一致均 fail closed。未实现 S3-compatible backend、全部业务PG、native writer epoch 或离线迁移；完整Wave2和Wave3–10不能标完成。未提交、推送、部署。


Wave2-S3 reviewed 最终验收：9项门禁全exit0、Rust427/Web36、独立真实PG6。完整报告 docs/enterprise/wave2-s3-file-report.md；原所有权/旧Local兼容和未完成范围如实保留。没有把File切片编号S3误标为S3对象存储后端；完整Wave2/Wave3–10仍未完成。


## 最新请求：完成完整企业 Wave2–10

继续既有工作区，不清理前序未提交变更。完整Wave2前置缺口仍为业务PG、native writer lease、S3与离线迁移。先实施S3-compatible artifact纵向切片：异步对象端口、有实际runtime caller、网络不持DB事务、原tool receipt继续唯一引用/终态owner、真实对象存储合同/故障/重启、多入口读回与九门禁。不得把PG发布协调或File切片伪称完整Wave2。


## Wave2-S4 最终复核与继续 S5

S4 真正 S3-compatible 后端已生产接线；SQLite v23 原工具收据/opaque ref、prepared 元数据、实际 GET SHA/size 与完整 receipt payload 摘要核验。payload-reviewed 九门禁全部 exit0：436 Rust/36 Web，9 项外部环境测试显式 ignored；独立真实 TLS MinIO 三项及真实 PG 六项通过。 SDK 固定 object_store0.14.2/reqwest0.13.5/quick-xml0.41.0，修复两项新 XML 公告，无 advisory ignore；仅新增 webpki-root-certs1.0.9 的已有同类证书数据许可例外，全文保存。旧第三方包身份未改变，新增43项。报告 docs/enterprise/wave2-s4-s3-report.md。

用户再次指示“继续”。下一 S5 为 native session writer lease：租约与原 admission/迟到 mutation 同事务检查，不能把 S1 delivery lease 冒充 native writer。Local 默认不依赖外部服务，显式 mode 引入租约；DB 中已存在租约时未持 proof 的 Store 不可绕过。第二活 daemon 不得执行全局 recovery 把首个活 run 标 unknown。native PG 全业务迁移仍是完整 Wave2 后续，Wave3–10不伪称完成。


## Wave2-S5 进行中

SQLite v24 native writer 与原 admission/terminal/transcript/tool/provider/resource/Hook/lifecycle 事务接线。真实两活 daemon RED 为首个 run 被第二实例改 unknown；两个 DB 连接 Barrier、旧 owner 迟到原调用、DB 到期接管、v24 迁移备份/故障和未来版本测试已落盘。启动/运行三秒受管续租，TTL十五秒；默认无 lease 的 Local 保持兼容，已有 native 行的未配置连接不能继续写入。诊断六入口新增共享 writer 摘要，初测暴露 bind-before-ready 指纹竞态，已修原子 marker + 有限启动等待，正在重跑。完整九门禁、S3 和 PG 发布回归尚未完成；全业务PG/PG writer/离线迁移及完整Wave2–10仍未完成，禁止提升 readiness。


S5 最终代码九门禁全通过（wave2-s5-writer-final-gates.json）：451 Rust、36 Web、9 外部环境 ignored；真实 TLS MinIO 三项回归通过。对象 late writer 新 StaleWriter(-32063/409)拒绝，legacy 活 owner 首次开启 Conflict、两次 commit 原 SQL 到期故障同事务回滚、ready 双 socket barrier 通过。独立 PG 发布六项回归正在执行，不声称 PG native writer完成。发现原 Cron runner 创建新的未配置 RunStore，native 模式安全拒绝该后台写入；需要下一切片复用 daemon canonical repository/proof 和对象 backend，并验证真实 Cron caller，不能用前台 lease 测试代替。

S6 fixture 修正：未知工具最初使用 deferred 名称，没有 discovery，实际工具收据为空/文件未写，不能作为未知副作用证据。改为冻结 core write_file 名称的受控 effect 后，runtime Cron 8项全通过，文件一次、native unknown、attempt1；未放宽终态断言。补真实定时触发续租与正常/kill恢复测试，独立S3与九门禁待跑。

S6 真定时触发合同发现禁用未来schedule后 active Cron未计入daemon persistent background，空闲退出在续租观测期间中止当前run。process.log为超时初诊，process-diagnostic.log为真实EOF失败；DB证据为原running被空闲recover改unknown，非provider回包。keeps_daemon_alive增加原running集合判断，保留现有调度owner。

S6 第一轮完整门禁 tests失败：acp_v2_plan_controls入口对等测试预选并释放TCP端口58976后，Web进程bind撞并发端口，os48 Address in use；其余37真实daemon测试通过。保留cron-final日志，不能记全通过。修fixture由实际Web服务bind端口0，并读取真实监听地址，避免TOCTOU而不是改串行或跳测试；完整九门禁须新reviewed前缀。

S6 Web fixture修正期间Rustfmt检测预选端口删除后残留空白，已清理；新WebSocket-only caller未用port提示已改丢弃绑定，无lint降级。服务实际local_addr用于原启动stdout，8个真实Web子进程从自己的endpoint文件读取port0真实地址，不预留并释放端口。首轮九门禁除端口竞态tests之外其余8项exit0。

S6 reviewed九门禁456Rust/36Web全部exit0，但审阅后新增Provider receipt数据库主键与JSON身份一致性RED（provider-corrupt-red.log）实际发现source JSON可指向target attempt。恢复/finish now核主键、round、candidate、run、Started；新contract确保两run/全部attempt/event零变更。最终审阅代码需重新九门禁并跑真实环境，不将旧456结果代表新增防护。

S6 implemented / locally verified：最终wave2-s6-cron-audited九门禁全部exit0，457Rust/36Web/10外部ignored；独立TLSMinIO四项和PG发布六项全通过。nativeCron同物理repo/proof/backend/冻结Provider，未知收口/no retry，定时续租/退出/kill恢复、六入口、SQL fault/损坏JSON身份零写闭合。报告wave2-s6-cron-report.md。完整Wave2和Wave3–10仍未完成；下一S7统一已接线async端口的SQLite有界阻塞隔离，保持schema24。

S7 cancel-budget目标测试首次编译失败：agent-storage单包缺Tokio test宏feature（全workspace统一feature不能替代单包正确声明）。增加仅dev-dependencies macros，复用已有锁定Tokio，无外部包版本变化，生产rt/sync/time保持。错误日志保存，非功能RED。

S7 最终九门禁 wave2-s7-adapter-final全部exit0；459Rust/36Web/10外部ignored。SQLite发布8项、storage取消/共享预算1项、真实daemon38项均通过；独立S3四项与PG发布六项回归进行中，不能提前记完整本地验收。所有native业务asyncports/nativePG/离线迁移仍pending。

S7 最终implemented / locally verified：459Rust/36Web/10外部ignored、九门禁、真实TLS S3四项及PG发布六项全部通过。统一三组端口的有界SQLite隔离及取消/过载，schema仍v24。报告wave2-s7-adapter-report.md；完整Wave2–10仍未完成。后续必须完整native业务asyncports、全业务PG/租户键/原事务、audit/quota/offline迁移，不得把协调端口当作全部事实源。

本轮交付复核：S6 audited与S7 final两组九门禁及各自S3/PG JSON均全部exit0；最后git diff --check通过，无后台gate/cargo任务遗留。全部改动仍在同一未提交工作区；未提交/推送/部署。后续继续完整Wave2的全native领域异步端口、全部canonical PostgreSQL/schema与tenant域、native PG writer、audit/quota及离线count/digest/切换/回滚；这些未实现，不能进入并宣称Wave3–10完成。

Wave2-S8：四项有效RED与七项storage GREEN；schema25永久life归属、只读scopedasyncports与原daemoncaller、HTTP/CLI新增只读投影、客户端错误归约和ACPtypederror已实施。真实六入口/重启测试正在重跑，随后完整九门禁和两个真实外部lane。完整Wave2–10未完成，继续按顺序推进。

S8最终implemented / locally verified：467Rust/36Web/10外部ignored、九门禁全exit0，真实TLS MinIO四项和真实PG发布六项全过。schema25及rollback/隔离证据报告wave2-s8-readback-report.md，完整Wave2–10尚未完成；继续S9中心身份/授权async端口。

Wave2-S9：中心身份/冻结授权async端口与纯core安全错误已接线，schema25保持。真正Connection占有timer RED/GREEN，新增scope/oldowner/revocation/proof/故障零写合同，身份九项通过。真实六入口中心SQL故障与S8 scoped重启运行中，完整门禁/外部回归未验收。完整Wave2–10未完成，继续。

S9 最终九门禁全部 exit0：471 Rust / 36 Web / 40 真实 daemon，10 项外部测试按环境显式 ignored。真实 TLS MinIO 四项与 PG 发布六项独立回归进行中；S10 已准备事件隔离/字节分页 RED 和实际端口/六入口方案，尚未实施，完整 Wave2–10 未完成。

S9 implemented / locally verified：471 Rust / 36 Web / 40 真实 daemon、九门禁全 exit0，独立真实 TLS MinIO 四项和真实 PG 发布六项全通过。报告 wave2-s9-identity-report.md；继续 S10 事件读回的实际 RED/修复，完整 Wave2–10 未完成。

S10 已实施：RunEventPage/RunEventRepository、SQLite scope 单事务及行/字节分页、原事件/stamp共用解码、daemon run.events/订阅恢复和 CLI/ACP1/ACP2/HTTP/WS 实际投影。两项有效 RED、六项 storage GREEN 与六入口真实 daemon 合同通过；订阅恢复、共享预算/竞争与完整九门禁/真实环境回归尚待收口，schema25/enterprise_ready=false。

S10 最终九门禁 wave2-s10-events-cursor-final 全 exit0：480 Rust / 36 Web / 42 真实 daemon / 10 外部 ignored；两次实施/旧行为回归已修复并保留失败证据。独立 S3/PG 回归中；S11 lifecycle scoped async 实施草稿仍 proposed，完整 Wave2–10 未完成。

S10 implemented / locally verified：480 Rust / 36 Web / 42 真实 daemon、九门禁及独立 TLS MinIO 四项/PG 发布六项全 exit0。报告 wave2-s10-events-report.md；事件 provenance 后续缺口如实记录，完整 Wave2–10 未完成。继续 S11 lifecycle scope/async 真 RED。

S11：七项 lifecycle 合同/79 storage 单测通过。新增 result_json 错误 key/lifetime 真实 RED，原共用 decoder 校验永久 owner；取消发生在原 commit 后仍保持实际 blocking permit。真实 daemon scope RED 为缺失会话清空的 -32602 与越域 Forbidden 不同；删除前置同步查询，原事务统一授权。daemon fixture 方法名错误单独保留，不计 RED；完整门禁与真实环境回归待收口。

S11 完整九门禁全exit0：489Rust/36Web/43真实daemon，外部10项明确ignored；独立真实TLS S3四项全过，PG发布六项回归中。S12 run.read/tools/provider原同步查询的scope/单事务/async缺口审阅及两个真实RED草稿已准备，尚未实施。完整Wave2–10仍未完成。

S11 implemented / locally verified：489Rust/36Web/43真实daemon，完整九门禁及独立TLS MinIO四项/PG发布六项全部exit0。继续S12原run事实查询async隔离；完整Wave2–10未完成，enterprise_ready=false。

S12 implemented：两个真实baseline RED与额外continuation故障RED、七项readback GREEN/storage80项通过。三新async端口实际RPC与slash caller共用原SQL/decoder/Connection，scope在同读TX内，完整字节/行预算。Provider JSON/SQL身份校验和未知拼写兼容；六入口新投影实际进程合同验收中。重复已有/run接线编译错误保留，已扩建原registry，不计RED；完整九门禁/真实环境待收口。

S12七项storage合同/storage80项/真实六入口三视图、driver故障、scope与重启合同均通过；原/run默认摘要和compact纯formatter兼容。ACP2严格字段白名单遗漏已修，实施失败保留。完整九门禁wave2-s12-run-facts-final开始，之后S3/PG严格串行。完整Wave2–10仍未完成。

S12 implemented / locally verified：498 Rust/36 Web/44真实daemon、九门禁 wave2-s12-run-facts-lint-final 全 exit0，独立真实 TLS MinIO 四项/PG 发布六项全过。相邻 if Clippy 失败保留、修后完整重跑，无 lint 例外。继续 S13 原准入/队列/终态真实 caller，schema25与全Wave2–10 pending/readiness=false保持。

继续 S13：准入/队列/终态原事务异步端口草稿已准备，开始原同步生产路径真实 RED。S12 已完整本地验收，完整 Wave2–10 未完成。

S13 async 真实 foreground/cancel 已接线，owner 同准入 TX 捕获贯穿工具审计/Hook/终态；三个有效 RED、八项 execution GREEN/storage82/真实前台迟到结果与条件重启合同通过。新增编码预算合同待全套验证；夹具假设/修正编译失败保留。完整九门禁 wave2-s13-execution-final 进行中，S3/PG 后续严格串行。全Wave2–10未完成。

首次完整门禁暴露实施回归：ACP 原 standard_acp_streams_updates_and_round_trips_permission 在默认栈发生 overflow；原用两个 async wrapper 将大执行 Future 重复内联。改为在普通函数入口 Box::pin 原执行 Future，唯一 task-local helper 持 heap pointer，未扩大栈、未 ignore 原合同。原 ACP 测试已在默认栈通过；新增256KiB执行Future/上下文小于4KiB/原owner保留的两种caller合同通过。Clippy explicit-auto-deref 修为自然借用，无 allow。失败日志 wave2-s13-execution-final-tests.log / clippy.log 保留；接着完整九项门禁重跑。

最终本地验收：wave2-s13-execution-stack-final 九项全部exit0，511Rust/36Web/45真实daemon，外部10项明确ignored。执行合同9/storage82/runtime131均通过，原ACP标准与前台generation故障合同保持，未加栈或跳过测试。真实TLS MinIO四项与PostgreSQL发布协调六项独立回归全部exit0，见 wave2-s13-execution-regression-s3.json / postgres.json。原失败门禁和夹具失败均保留；未CI/生产验证、未提交。全企业Wave2尚未完成，Wave3–10 pending，enterprise_ready=false。继续原Provider attempt/未知结果和实际runtime caller，不能称所有ports或全部PG事实已迁移。

继续S14原Provider attempt：S13全验收结束；两个真实业务RED开始，分别验证终态不能改写原provider身份、已persist OutcomeUnknown的attempt不能导致run假成功。后续才应用同事务async端口及实际runtime caller，schema25保持，全Wave2–10未完成。

S14推进：五项原事务RED已修复，八项scoped合同GREEN。真实HTTP完整接收后断连有效RED观察到3次请求/run Failed；新增驱动内部dispatch证据，只有确证未建立连接或明确4xx拒绝才允许安全重试，5xx/丢失响应/现场超时取消保守Unknown。wire GREEN一次调用/OutcomeUnknown/typed -32002/重启不重放。native新attempt冻结route缺口有效RED（profile维度截停，其余维度待GREEN验证），同原Immediate TX增加running/candidate/profile/API/model校验。新的timer及完整合同/共享预算/九门禁与真实lane仍待验收，未提升全企业readiness。

S14 implemented / locally verified：wave2-s14-attempt-audited-final 九项全部exit0，526Rust/36Web/48真实daemon、10外部环境项明确ignored。12项attempt存储合同/131 runtime单测/3真实HTTP接受后断连取消超时全部GREEN。独立TLS MinIO四项业务与PostgreSQL发布协调六项全部exit0；PG runner五步骤不等于五业务合同。首次依赖清单/Clippy遗漏及两项旧取消断言失败保留，未放宽架构护栏、未加栈/跳过测试。schema25，不新增第三方包，未提交推送部署；完整Wave2–10未完成，enterprise_ready=false。继续原工具事务async/固定owner，后续完整PG/租户键/nativewriter/auditquota/offline仍必须闭合。


S15 implemented / 局部验收：原工具六异步端口接实际前台 caller，同原 Connection/四 permit/固定准入 owner；四项有效旧业务 RED 修复。原未知工具后继续执行后续 wave 的有效 RED 修复（后续 effect1→0，模型请求均1），原 typed unknown/no replay。11工具合同、83storage单测、真实工具发现回填合同通过；File已commit后取消等待者仍持permit/收据持久合同通过。保存batch SQL字节预检与十二项扩展合同正在跑，九门禁/S3/PG待最终验收。schema25/readiness=false，完整Wave2–10未完成；详见 docs/enterprise/wave2-s15-tool-report.md。

S15 全量 Rust 门禁已 exit0：540通过/0失败/10外部环境ignored，39个test executables；runtime132/storage83/工具integration12/真实daemon48均纳入。其余八门禁仍进行，真实TLS S3四业务和PG发布六业务之后严格串行，暂不标最终验收。


S15 implemented / locally verified：wave2-s15-tool-audited-final 九项全部exit0，540 Rust / 36 Web / 48真实daemon，39个test executables、10外部环境项明确ignored；12项工具存储合同、83 storage单测、132 runtime单测通过。真实TLS MinIO四业务与PostgreSQL发布协调六业务回归全部exit0。原工具六async端口/固定owner/原File两阶段receipt事务及未知工具停止后续wave已接实际前台caller，无新表/状态机/第三方依赖，schema25。原功能RED、编译/夹具失败证据保留，不计虚假RED；未放宽超时/栈/架构。Cron兼容和其余业务ports/fullPG/租户键/nativePGwriter/完整outbox/auditquota/offline仍欠；完整Wave2–10未完成，enterprise_ready=false。详见 docs/enterprise/wave2-s15-tool-report.md。

S16 implemented / 局部验收中：原turn/transcript/计划六异步端口已接实际foreground runtime及daemon start-plan，同一Connection/四permit/固定准入envelope。六项原越域RED已修，15项扩展合同通过；另确认已结算无决策run仍返回计划执行成功和receipt.run_id错误仍执行两个有效RED，原helper增加running及SQL/JSON身份校验，16项最终合同在跑。Message临时借用编译失败、旧计划直接SQL插入clock变化和执行夹具误用Queue均保留、均不计功能RED。schema25/无新状态表/无第三方库；完整九门禁/S3/PG尚待验收，全Wave2–10仍pending/readiness=false。

S16完整九门禁运行中；生产源冻结。初test编译给出run_turn_contract中多余RunRepository import警告，只移除该无效test import（无生产或测试行为修改）；Clippy仍将核最终源码。SQL身份新增断言等行为从九门禁开始前已固定。S17仅/tmp request/context异步审阅草稿，未实施，不提升完整Wave状态。

S16完整Rust门禁exit0：556通过/0失败/10外部ignored，40个executables；实际daemon48/原计划审阅执行六入口与重启恢复全部保持。Clippy exit0，未增加例外或栈；MSRV/release/Web/deny/diff进行中，随后TLS S3四业务和PG发布六业务严格串行。


S16 implemented / locally verified：wave2-s16-turn-audited-final 九项全部exit0，556 Rust / 36 Web / 48真实daemon，40个test executables、10外部环境项明确ignored；16项turn/transcript/计划integration合同、83 storage/132 runtime单测通过。独立真实TLS MinIO四业务和PostgreSQL发布协调六业务全部exit0。原六async端口接实际前台/PlanStore/daemon start-plan，同一Connection/四permit/固定准入envelope；六越域RED、已结算无决策仍给执行许可RED、receipt错误run_id仍执行RED修复。JSON四维和SQL四维身份GREEN，不把第一个截停RED夸成多项RED；故障回滚/CAS/幂等/撤销/过期和真实daemon原计划六入口/恢复合同保持，无新表/第三方包、schema25。编译/夹具失败及测试无效import警告保留，不计功能RED；未放宽超时/栈/护栏。全业务ports/nativePG/租户键/nativePGwriter/outbox/auditquota/offline仍欠，完整Wave2–10未完成，enterprise_ready=false。详见 docs/enterprise/wave2-s16-turn-report.md。


2026-10-08 持续执行：用户要求直到Wave10完成再停。S16已完整本地验收；S17请求捕获/上下文异步迁移in_progress，先复现原请求行SQL/JSON完整合法错配及scope缺口，随后唯一helper/固定准入envelope/实际模型及摘要await。全企业Wave2–10尚未完成，readiness=false。继续使用planning-with-files已有记录，不重建计划。

S17四项有效功能RED（scope来源读、capture写、只读重建，以及完整合法JSON保留原digest但SQLcapture_id错配）均独立失败，修后四项GREEN。原helper唯一body/同TXscope/JSONSQLrun/capture/round/candidate核、SQLBLOB预算和完整canonical前缀超预算明确拒绝已实施。三个native async端口接线进行中；摘要回调改owned消息+boxed await，compact使用已捕获owner进入同原task-local上下文。完整compact准入/结算仍sync欠账，不能称全async。旧GREEN session已结束后write_stdin返回Unknownprocess，日志证明4passed，不重复运行冒充新增证据。

S17扩展首次12合同11pass/1fail：capture中life维度在原入参Protocol验证提前转换为Conflict，未得到固定owner的StaleOwner。移除冗余提前life检查，native同TX先验证scope/fixedowner/冻结fence，再核captureowner并执行原source_check；不放宽测试断言。幂等重复提交增加原SQLround/candidate/byte及savedraw核对，损坏事实不因digest相同冒充幂等成功。check期间无效import警告为当时未加终态扩展合同，现在已使用该trait，不计功能RED。

S17 reviewed编译失败：SQLidentity元组在长SQL macro附近保留无空格格式，文本替换未命中u32→i64，比较因此类型不匹配；精确修正元组类型，无测试放宽。writer lease夹具列名在运行前审阅发现应session_key，已修正。两者均实施错误，不计功能RED。

S17额外route有效RED：合法冻结catalog/policy及正确摘要仍接受与冻结Provider候选不一致的route/provideridentity，实测Ok写入（wave2-s17-request-route-red.log）；原helper增加候选index、model与原candidate序列化SHA身份核对，读回复用同helper，不新增路由账本。撤销夹具首次失败为对未结算legacy运行开启writer被原安全activation拒绝，先正常终态结算原夹具run再启用writer，不修改原activation保护。

S17请求合同14项全部GREEN；三个端口的满额合同和异步摘要发送前校验单测已补，库/真实普通模型及compact-summary回归串行进行。S18原事件provenance两个RED草稿已写/tmp，尚未应用；考虑在同Wave2剩余实现批次后统一完整九门禁和真实环境回归，避免把每个小端口切片冒充完整Wave。全Wave2–10未完成。

S17库回归发现真实兼容回归：两个未知副作用合同在发送前capture失败，Cron按原安全未发送逻辑尝试4次，未执行外部副作用。新envelopecatalog检查错误混用了两种历史合法编码：RunSnapshot哈希typed ToolSpec serialization，ContextEnvelope哈希serde_json::Value（字段排序）。改为各自按原canonical表示核同一catalog，不改历史摘要或弱化合同。原失败log wave2-s17-libraries-local.log保留，完整库及真实daemon回归以新prefix重跑。

S17 reviewed-local库步骤exit0：133 runtime / 83 storage，包括三个请求端口统一满额、异步摘要先await拒绝不发送，以及原未知工具/Provider/Cron合同。真实daemon普通请求读回与compact-summary两步继续串行，不改生产源；随后S18真实RED开始。

S18两个真实RED均有效：原native事件stamp删除后分页返回无_my_agent_view的成功事件，直接stamp读取返回None，均不允许降legacy。应用schema26原events永久provenance/native缺stampCorrupt/已发布stamp删除保护/先SQL字节预算；旧fixture准确撤26后再撤25，future更新27。旧迁移故障版本仍原值不放宽。S17普通模型与compact-summary真实daemon两步均exit0；其完整九门禁与真实lane跟本批收口统一执行，不冒充最终验收。

S18扩展编译漏导入MaintenanceRepository导致health_report方法不可见；补正确trait import，无行为变更，不计功能RED。两个scoped RED修后8原事件合同已GREEN；新增来源/迁移备份/回滚/future27/预算合同继续验收。

S18事件12项合同及storage83单测全部GREEN，schema26已实施但完整门禁尚未收口。S19Cron审阅proposed：真实背景运行仍with_tool_audit重读owner、finish(runid)重取终态owner、sync创建/准入/请求；拟同canonicalports/固定准入上下文与独立workload主体，policy verifier从daemon注入，原create_in与admit_inner同TX复用，不新增runtime→policy依赖。先真实RED再实施。

S18五组迁移回归全部exit0（enterprise identity/object/publication/readback/writer；外部环境测试显式ignored，未冒充真实lane）。S19两个业务RED有效：缺独立冻结workload0记录，以及晚到callback把新generation状态改failed；锁场景第三测试先被旧Deferred升级SQLITE_BUSY失败截停，未达到timer断言，不夸async timer RED。已实施schema27 Local固定workload/developer角色、原同TX SessionCreate+Prompt校验及create_in/admit body/fixedowner，实际Cron接原asyncports；policy verifier仅由daemon注入，无新增runtime基础设施依赖。首次check缺std::sync::Arc限定已修，开发依赖policy加入原测试注入用途，Cargo.lock将更新后locked门禁验证。

S19 Cron初11合同9pass/2fail：两个原持久history验收在独立新连接上调用旧session_snapshot时由views::commit强制writer校验而StaleLease。测试改用原S8已经实现的只读read_session(Canonical)端口，在同一个独立新连接验证相同消息/持久session断言，不重新获取writer或降低保护。两个有效业务RED、native暂时写锁后成功/timerGREEN及原未知工具no retry已通过。

S17–S19 收口：当前schema27。补 source 总 batch 行数与 message revision 预算，避免小字节大量合法空行的分配放大；首次局部命令使用 --exact 配部分测试名导致0项运行，日志保留、不计RED，下一次使用完整测试名。一次查找不存在schema.sql已改用实际session_migration.sql。S18/S19中文实施报告已保存，完整九门禁与真实S3/PG尚待本批统一验收。

S17新增有效row-budget RED：32769个合法空batch使原source成功返回32770个batch_ranges，修前真实失败证据 wave2-s17-request-row-budget-functional-red.log；原同TX source预算增加batch行数及总messages≤32768，修后request15/event12已GREEN，workload7继续运行。S20 compact仅只读预审，原settle在fence失败后写rejected/terminal为关键风险，native必须零晚写；计划/tmp/agent-rust-wave2-s20-compact-plan.md，尚未实施。

S17–S19 reviewed合同request15/event12通过，workload6/7通过；foreign夹具改scope时原已有Local writer FK拒绝UPDATE，未达到准入断言，不计业务RED。夹具用保持FK的删除旧lease→修改scope→插入foreign lease构造真实foreign session，未关闭FK，前后lease/clock/hooks零写断言保持。

S17–S19宽门禁进行中：runtime136/storage83/架构20已通过，未完整结束。S20仅/tmp草稿，没有生产更改；发现原RunKind由linkage推导，拟复核原request id/input防合法link跨run错配，避免影子kind。另发现原Canonical/Model snapshot_in仍缺总量/行预算，拟共享唯一sessions预算helper，保全量或明确拒绝。

S17–S19首次完整九门禁8项通过、Clippy101：LOCAL_CRON_SUBJECT生产常量放在test module后触发items_after_test_module。移动到test module前，不新增lint允许。Rust588/42 executables/外部10ignored、Web36、daemon48已通过，原失败prefix wave2-s17-s19-audited-final全部保留；先Clippy完整scope预检，随后最终验收收口。

Clippy原生产常量顺序已修，完整workspace/alltargets/allfeatures/-Dwarnings预检exit0（wave2-s17-s19-clippy-reviewed.log），无新增lint例外。最终九门禁改用wave2-s17-s19-reviewed-final全项重跑，生产源码重新冻结，原失败门禁完整保留。实际TLS S3/PG需在该完整结果后串行。S20仍仅/tmp，不冒充实施。


S17–S19 implemented / locally verified：wave2-s17-s19-reviewed-final 九项全部exit0，588 Rust / 36 Web / 48真实daemon，42个test executables、10外部环境项明确ignored；runtime136/storage83、request15/event12/workload7合同全部通过。独立真实TLS MinIO四组业务与PostgreSQL发布协调六项全部exit0（wave2-s17-s19-reviewed-regression-s3.json / postgres.json，PG runner五步骤不等于五合同）。S17三请求端口实际模型/摘要/读回await；S18原events不可变来源及native缺stampCorrupt；S19原同TX后台准入、固定owner及Local独立workload。schema27，不新增第三方包。首轮Clippy常量放testmodule后失败、foreign夹具writerFK失败及0tests过滤错误均保留，不计功能RED；移动常量后完整九项重跑，未放宽lint/超时/栈/架构。全企业Wave2、Wave3–10仍未完成，enterprise_ready=false，未提交推送部署。继续S20 compact实际RED与原唯一helper迁移。

S20首个旧路径RED运行被夹具漏导入TurnRepository编译错误截停（wave2-s20-compact-functional-red.log），不计功能RED；加正确trait import保留原commit_turn业务，新增原canonical只读大量合法空batch预算复现，共七项准备运行。S17–S19九门禁及真实S3/PG已全exit0，当前无验收后台命令；S20开始实际代码。

S20七个有效业务RED全部复现（wave2-s20-compact-audited-functional-red.log）：foreign admit/fact/begin/settle均Ok，合法linkage移动到原Chat仍Ok NoGain，旧generation结算revision33→37，canonical读回32769合法空batch成功。首次夹具缺TurnRepository修后复现，不计编译失败为RED。抽取脚本在begin_compact单行签名处断言失败：已写ports/source、尚未写context及以后；修泛化闭括号插入策略，正在保留部分树首次check失败日志后继续剩余抽取，不重跑已写parts。

S20抽取余下helpers/actual callers已应用。闭括号适配首次误命中Result<(),RuntimeError>的unit括号，begin_compact_scoped签名被格式器拒绝；精确修复实际函数声明，后续脚本限定arrow前寻找args闭括号。两次parser/wiring失败保留，不计业务RED；不得重跑已应用extract以覆新源码。

S20恢复编译发现已有daemon字段为readback_store/event_store，草稿误用session_read_store/run_event_store；改回原同Arc端口，不添加第二字段。source预算聚合限前32769行：达到阈值明确拒绝，未超阈值时是完整集，避免百万坏空行全表CPU扫描且不截断合法历史。既有Source和Canonical共用唯一预算helper。新check继续，未放宽编译/test断言。

S20 native夹具误用不存在SessionCommand::Clear导致编译失败（compact-native-green.log），修为原End{delete:false}生命周期命令；未修改生产lifecycle。审阅发现专用Compact run若begin另一个operation可能结算原run，增加独立业务RED；SourceCAS竞争改为普通Chat下两个implicit intent，避免用错误专用run操作伪造并发。

S20第8项有效业务RED：专用Compact native run begin另一个operation真实返回Ok(())（wave2-s20-compact-operation-red.log），原唯一helper增加linkage operation绑定，无第二业务账本；正常Chat两implicit intent独立CAS合同避免混淆run领域。修后14context合同待运行。

S20 native reviewed首轮10/14通过，四项夹具SQL误用events.kind（真实列event），修查询及故障trigger为event，不计这些夹具失败为业务RED。增加8context+exactOwner全部9方法共享四permit满额拒绝断言，不绕过实际预算。

S20 corrected native十四合同全部exit0（wave2-s20-compact-native-corrected-green.log），包括8个有效功能RED对应修复、全scope/owner维度、专用operation绑定、正常Chat SourceCAS零terminal、原terminal故障回滚、清空后历史读取、撤销/过期/大source与300ms真实锁等待下25ms timer/取消awaiter后worker真实提交。完整门禁仍待运行。

S20库runtime136/storage83和真实daemon五项compact合同全通过（libraries-reviewed.log / daemon-compact-reviewed.log）；同批新端口及exactOwner共享预算断言在storage测试内已过。S21开始原即时事件投影真实RED，schema27，预审/tmp/agent-rust-wave2-s21-event-projection-plan.md。S20/S21统一本地完整门禁收口，仍非全Wave2完成。

S21两个有效RED（latest-event-functional-red.log）：foreign helper返回私密terminal与封存stamp，合法2MiB event返回成功。共用原decoder/scoped TX/SQL CASE字节预检、async latest端口及daemon实际caller已接线；首check闭包Err.into类型推断失败，显式Result<StoredEvent,RuntimeError>；委派父run使用真实字段run_fact_store（readback_store只SessionRead），无新增字段或第二事实。

S21 reviewed check因daemon原imports显式列表缺新增Core TenantContext/CoordinationError截停；加真实显式imports（未放宽架构依赖）。追加精确child/原stamp相等、四scope+absent统一Forbidden、退休life零writer/clock及合法JSON错digest拒绝合同；最新事件current_thread真实连接锁timer/取消预算回收库合同。

S21首native 16/17通过；退休life后的夹具试改events被原stale fact owner保护拒绝，未到decoder断言；改为合法ViewStamp JSON更改snapshot_revision但保留旧digest，验证原stamp SHA拒绝，不把events payload无digest误称已验证。S20审阅又发现begin直接取无界run_snapshots、settle不再复核frozen只读政策；新增两真实旧路径RED。

S20第9项有效业务RED：原无冻结授权legacy兼容run的合法>1MiB冻结snapshot，经native begin真实返回Ok（compact-frozen-reviewed-functional-red.log）。原begin改复用requests::frozen_in唯一SQL长度+JSONdecoder，缺snapshot仍保持legacy兼容；native授权fence check_snapshot_in取原snapshot前增加SQL长度预检，继续核原SHA。readonly改动本就被原SHA拒绝Forbidden，不新增第二政策状态机，不计为RED。

S20/S21 context16/event17已全GREEN（context-event-reviewed-green.log）。Clippy预检截停于原请求source helper放testmodule后及format!未内联变量；移动helper到测试模块前、改直接格式变量，无lint例外。latest child SQL CASE先字节预算，再json_valid，再json_extract；坏/巨大候选明确拒绝，不解析无界JSON或跳过损坏候选返回更旧事件。追加该合同后统一宽门禁。

S20/S21 implemented，完整本地验证进行中：原上下文/compact八async端口与exactOwner端口、即时事件投影async端口已接真实caller，scope/fixedOwner/CAS/原stamp/预算在唯一原事务复核。context16/event17局部通过，runtime136/storage83及五项真实daemon compact通过；新增child SQL预算和锁合同在统一门禁中验证。schema27，无新第三方依赖，未提交推送部署；全业务async/nativePG/租户键/PGwriter/auditquota/offline与完整Wave2–10未完成。详见wave2-s20-context-report.md及wave2-s21-event-projection-report.md。

S20/S21首次完整门禁8/9通过，仅workspace tests101：真实daemon central_async_authorization_storage_fault... ACP stdout EOF，47/48实测daemon通过（另2外部ignored）。故障夹具principals仍存在、1session/0run、无web诊断，故障发生于ACP启动/初始化阶段而非授权故障注入之后。原helper丢stderr到sink无法诊断；追加64KiB有界stderr及method/真实ExitStatus，仍持续drain，不改合同断言、timeout、栈或test并行度；独立复核该合同，再处理根因。原8项成功不覆盖失败test，总验收未完成。

ACP有界诊断版本独立重跑原central_async_authorization_storage_fault... exit0（acp-failure-diagnostic.log），不能据此称根因已修；原并发完整daemon合同正在同样原配置重跑（daemon-bounded-diagnostic.log），未设置test_threads/额外TOKIO_WORKER_THREADS/栈/超时。首次失败完整证据保留，当前无九门禁最终合格声明。

ACP原并发复核完成：48 passed / 0 failed / 2 ignored（50总项），daemon-bounded-diagnostic.log exit0；首次EOF仍未定位，不称已修根因。S20新增第10个有效业务RED：source_json合法JSON改为不同lifetime后read_compaction_result实际返回Ok(Some(0))，证据compact-result-identity-functional-red.log。原唯一事务增加JSON源lifetime/起点/整数范围与committed+1/no_gain原generation一致性；健康revision冲突及summary_failed以Conflict拒绝重放，损坏以Corrupt拒绝。新增三项读回合同正在验证。首次改动闭包Err.into推断失败，显式返回Result<ProjectionGeneration, RuntimeError>修复，编译失败日志保留、不计业务RED。全Wave2–10仍未完成。

S20新增来源身份修复后的context19/event18均通过（result-corrected-green.log），原首次closure类型推断编译失败日志保留。wave2-s20-s21-reviewed-final完整九项门禁已启动；生产源码冻结，S22只/tmp预审与合同草稿，不称已实施。

最终九门禁仍运行：workspace中runtime136/storage84/context19/event18及真实daemon48均过；原central_async_authorization_storage_fault合同此次原并发也通过，runtime suite 48 passed/2ignored（36.57s）。首次ACP EOF仍无根因，不因后续过而删除失败证据；真实S3/PG继续待当前完整门禁结束后串行。

S20/S21最终本地九门禁全部exit0（wave2-s20-s21-reviewed-final-gates.json）：614 Rust /36 Web，43执行体，10外部环境项ignored；context19/event18/runtime136/storage84/daemon48已计入。真实TLS MinIO四组业务回归已启动（reviewed-regression），PG发布六项合同待其结束后串行；生产源码仍冻结。首次九门禁tests101/ACP EOF与初闭包类型推断失败完整保留，后续通过不解释为已定位修复根因。

2026-10-09 S20/S21 implemented / locally verified：wave2-s20-s21-reviewed-final 九项全部exit0，614 Rust /36 Web /48实测daemon，43测试执行体、10外部环境项明确ignored；runtime136/storage84/context19/event18全部通过。独立真实TLS S3四组业务及PostgreSQL发布协调六项全部exit0（wave2-s20-s21-reviewed-regression-s3.json / postgres.json），自己的MinIO容器与PG隔离DB已清理。第10项有效compact来源身份RED已修，同原事务核JSON lifetime/起点/结果generation，正常revision和摘要失败返回Conflict，损坏Corrupt。首次九门禁tests101/ACP初始化EOF保留；独立与原并发daemon复核、最终完整门禁均未复现，根因未定位，64KiB有界诊断并持续drain保留，未修改超时/栈/并行度。schema27，无新第三方包。全企业Wave2、Wave3–10仍未完成，enterprise_ready=false；未提交推送部署。继续S22实际Live事件mutation，再S23发现端口，统一本批验收。

S22两个有效业务RED已复现（live-events-functional-red.log）：foreign旧compatibility append以及无冻结授权旧run的late callback都Ok(EventSeq(2))。已有frozen SHA的native owner保护不能称被绕过；复现明确授权记录0、合法同步更新run及head generation。原helper保留唯一events/interactions/stamp/outbox事务；native live新async入口与五个实际chat caller固定准入env、shared4budget、状态/allowlist/object/字节预算已接线，foreground初始历史改await原execution_context。check workspace/alltargets/allfeatures已exit0。新增typed scope/owner/终态/预算/rollback/两Connection seq及取消awaiter仍实际worker提交合同待GREEN，尚未最终验收；S23继续待实施。extract脚本已应用，不得重跑覆盖新源码。

S22首次GREEN命令错误：-p agent-storage搭配根package的--test run_event_contract，Cargo拒绝目标（未执行任何合同，不计业务失败或RED）。改为两个真实package命令串行：storage --lib，再根run_event_contract；保留原错误日志，不改测试断言。

S22 storage首轮84/85通过，取消awaiter新合同失败不是timer/预算失效：原Fixture.admit默认Queue，worker正确以Conflict拒绝queued事件，所以没有提交。新live合同明确try_start_queued先进入Running，保留原timer/permit/提交一次断言，不改生产guard、不放宽status；三个新native事件夹具及并发夹具同样真实start，原已有18只读夹具保持不改。并发cursor用start后的真实last_seq，不能用准入时旧seq。最初late-owner旧路径RED是Queued兼容run，后续需Running原helper再复现，避免夸可执行callback证据。

S22纠正Running夹具后storage85全过（storage-corrected-green.log），25ms timer/四permit占有及worker取消后单次提交原断言通过。原callback helper在真正Running且head/run generation一致更新后再次真实复现成功EventSeq(3)（live-running-owner-functional-red.log），不是Queued不合法状态的混合证明；恢复测试调用线上实际native append_run_live_event固定env，预期不变，原legacy runid helper保留维护语义。完整23事件合同继续GREEN验收；本批尚未九门禁和真实环境验收。

S22局部verified：storage85/event23全部通过，含满额四permit、真实连接锁timer/awaiter取消后worker真实提交及interaction/event/stamp/outbox故障rollback、两独立连接seq/stamp/outbox。真实daemon48回归正在运行（daemon-reviewed-local.log）。报告wave2-s22-live-events-report.md，尚未最终九门禁/真实环境收口，S23接下来同批迁移。

S23首轮5个有效业务RED：foreign publish/discovered/read/cancel真实Ok，合法>1MiB保存snapshot执行许可真实Ok(true)（discovery-functional-red.log）。第6历史夹具先End仍Running被原领域拒绝，未到历史读回，不计功能RED；修为原finish canonical Completed再End，然后独立复现历史读取。S22真实daemon48全部通过（2外部S3ignored），新批九门禁待S23合并收口。

S23 runtime/storage实际接线check已过。RunDiscoveryRepository四端口共享4permit；原discovery事务复用完整scope/fixedowner/writer/frozen授权，历史仅永久归属与receipt校验。daemon run.discovery和/tools已await，取消只捕获一次原owner贯穿queued/running事务，ToolRegistry发现许可await且副作用前一次冻结descriptor供原批次和输出预算使用。新增合同首轮8/10通过：两个Connection校验stamp列误用event_seq（真实seq），夹具修正；保存receipt错误catalog当前回Conflict，改唯一read helper将保存的generation冲突映射Corrupt，caller generation冲突仍Conflict；失败日志保留，不把夹具失败计业务RED。追加原授权撤销/租约过期/退休重启/累计入库预算及四真实锁合同，等待局部GREEN，再同批S22/S23完整门禁与真实环境验收。schema27，无新第三方依赖，全Wave2–10未完成。

S23 corrected14发现合同全部通过（discovery-reviewed-corrected-green.log）。stamp真实seq夹具已修，前次12/13仍为已编译旧查询执行体，失败证据保留；不是生产修复。保存catalog冲突已归Corrupt，合法caller冲突保留Conflict；进一步将健康已取消执行许可明确Conflict，并将129行预算夹具改每条真实匹配operation/SHA的合法receipt，不以多处损坏混淆预算证明，统一完整门禁验证最终源。新增native搜索worker取消与四实际Connection锁/awaiter取消合同；storage/runtime库正在运行。S22/S23完整九门禁和真实S3/PG仍待收口，Wave2–10未完成。

S23库全部通过（wave2-s23-libraries-reviewed-green.log）：runtime137/storage86，包括新增native搜索worker取消、共享四permit满额拒绝、四端口真实Connection锁25ms timer与awaiter取消后原worker恰好完成一次。S22/S23统一本地九门禁已启动wave2-s22-s23-reviewed-final；生产源码冻结，后续发布scope只在/tmp预审不修改生产。原六发现RED已修，14root发现合同局部通过；最终全量仍运行，真实S3/PG待九门禁结束后串行。

S22/S23首轮完整门禁8/9通过（wave2-s22-s23-reviewed-final）：636 Rust/44执行体/10外部ignored、36 Web、MSRV/release/deny/diff全部通过，仅Clippy101：新发现合同5处对Copy的RepositoryError调用expected.clone()。去掉5处多余clone，不改断言/超时/栈/并行度、不加lint例外；首次失败完整保留，不计功能RED。修正后的完整九门禁wave2-s22-s23-reviewed-corrected-final重新验收，生产源码保持不变，真实S3/PG待完成后串行。

S22/S23修正后的完整九门禁wave2-s22-s23-reviewed-corrected-final全部exit0：636 Rust/36 Web/48实测daemon，44执行体，10外部ignored；runtime137/storage86/context19/discovery14/event23全部通过。首次8/9仅Clippy多余Copy.clone失败的证据保留，无lint例外、无测试/栈/并发/超时放宽。真实TLS S3四组业务回归已启动wave2-s22-s23-reviewed-regression；PG发布六项待S3后串行。生产源码仍冻结，全Wave2–10未完成。

2026-10-09 S22/S23 已实现并完成本地验收：wave2-s22-s23-reviewed-corrected-final九项全部exit0，636 Rust/36 Web/48实测daemon，44执行体、10外部ignored；runtime137/storage86/context19/discovery14/event23全部通过。真实TLS S3四组业务及PostgreSQL发布六项全部exit0（wave2-s22-s23-reviewed-regression-s3.json / postgres.json）；自己的MinIO容器与PG独立DB已清理。原实时事件caller固定准入env、发现四端口及取消一次捕获owner已接actualawait；原事实/SQL/同TX不复制。首次九门禁Clippy5处Copy.clone失败及发现夹具错误完整留，不计业务RED，不放宽lint/断言/超时/栈/并行度。schema27，无新第三方包，全Wave2/Wave3–10仍未完成，enterprise_ready=false；未提交推送部署。后续S24发布完整key先SQL/列绑定与预算实际合同，预审与草稿在/tmp，仅未执行时不计RED。

S24已进入实际合同：原SQLite发布8项通过，新增两个功能RED真实失败（wave2-s24-publication-sqlite-functional-red.log）：六维组织/租户/项目/agent/session/lifetime外域租约的ACK和inbox共12次均先返回Corrupt而非Forbidden；仅改保存SQL域列、原JSON和SHA合法，六维read_publications均Ok(1)返回原域envelope。全部在隔离fixture显式移除immutable触发器注入保存事实损坏，不声称正常业务写可以绕过触发器。真实PG原六项+新两项合同已启动wave2-s24-publication-functional-red，待其完成再修改原两个adapter与唯一decoder。S22/S23此前636/九门禁/真实S3PG全部验收通过，当前新增RED不覆盖前批事实，也不计全Wave2完成。

S24真实PG原六项+新两项功能RED复现（publication-functional-red-postgres.log）：12个六维ACK/inbox均Corrupt，SQL组织/tenant/project/agent/session/life列不同但JSON+SHA合法时六维均Ok(1)。两后端共有四个有效功能RED合同（按独立合同计，维度次数不夸成24漏洞）。原record已改WHERE完整KEY+event_id，原claim/ACK及claim_owner查询同key，唯一decode_record增加只读OutboxRowBinding核原SQL列与JSON；未复制业务SQL状态机，无新事实表/schema/deps。SQL CASE抑制超4MiB payload、错误长度id/digest与超大身份列；SQLite按行原decoder，不先collect所有raw。check workspace全targets/features通过；SQLite发布10合同全部通过（sqlite-reviewed-green.log），真实PG八合同正在GREEN验证。pending_delivery_keys元数据仍需预算复核，不声称本批全部方法已预算闭合。

S24 scoped/row-binding修复后真实PG八合同全通过（publication-reviewed-green-postgres.log），包含原六+两新增。另实际SQLite pending_delivery_keys预算RED：合法UTF8的>1MiB保存session_key返回Ok(1)（pending-budget-sqlite-functional-red.log）；测试首case即失败，尚未执行lifetime，不夸成两个RED。原READ/CLAIM codec虽已SQL预算，pending_keys仍裸字段，继续相同实际PG合同，然后改原元数据投影；并非整批最终验收完成。

S24原发布完整key/保存列binding与SQL预算已实施，最终局部SQLite12/真实PG10全部通过（publication-sqlite-final-corrected-green.log / publication-final-green-postgres.log），自己的独立DB已删。三类问题分别在两后端六独立合同有效RED；新增run/seq夹具首次FK拒绝未达decoder，改真实events引用目标并保留FK，失败不计RED。大合法JSON拒绝与零claim/inbox/ACK/clock为回归，旧decoder后置预算已有拒绝，不伪称RED/独立内存测试。schema27/PG发布schema1不变，无新依赖。报告wave2-s24-publication-report.md；完整九门禁即将启动，生产冻结，后续Hook仅/tmp预审，全Wave2–10仍未完成。

2026-10-09 S24已实现并本地验收：wave2-s24-reviewed-final九项全exit0，640 Rust/36 Web/48实测daemon，44执行体、14外部ignored；SQLite12与真实PG10项发布合同通过。真实TLS S3四组业务与PG发布10项回归全部通过（wave2-s24-reviewed-regression-s3.json / postgres.json），自己的容器与独立DB已清理。原完整key先SQL、保存列与唯一JSON decoder绑定、字段预算已接实际两adapter，无新schema/deps。全业务PG/其余async与完整Wave2–10仍欠，enterprise_ready=false；未提交推送部署。继续S25原Hook观察读回/待发布/ACK实际合同，执行actor取消仍单独欠账。

S25原Hook观察三个async端口已实施，真实RPC、/hooks、startup积压查询与lifecycle ACK已接await；原Connection/helper/四permit，无新schema/deps。六项原路径有效RED已复现，原只读clock合同已通过不能算RED（views::commit无无条件clock写，预审推断已纠正）。14项局部观察合同全过；退休夹具Clear不存在与误用End(false)失败留，不计RED。原JSON有界身份JOIN原scope，过滤在LIMIT64前；保存outcome预算/原operation及ownerSQL绑定，ACK先scope。storagecurrent_thread/共享预算与实际observer进程daemon合同正在验证，完整九门禁和真实环境回归待；Hook claim/settle及受管取消仍欠，完整Wave2–10未完成。

2026-10-09 工作流变更：用户明确要求“先开发完，再测试，再修复”，停止逐切片 RED/GREEN/九门禁/真实环境回归。S25 的最后已验源已有九项全 exit0、656 Rust/36 Web/49实际daemon、45执行体、14外部ignored，真实TLS S3四组通过；本批PG最终回归未启动，不能称完整双lane验收。S26 continuation三端口、S27 Hook claim/settle/快照/interaction受管执行与关闭drain、S28记忆读取/写入/曝光/反馈/摄入及实际RPC/上下文/工具/Cron/维护接线已开发，均尚未编译和测试。cargo fmt仅格式化成功，不是行为验证。当前树不是656项已验源；集中验证安排在开发阶段完成后。完整Wave2–10仍未完成，enterprise_ready=false，schema27，未提交/推送/部署。

开发追加 S29–S31（全部未编译、未测试）：S29资源八端口接RPC、/resources、后台执行/状态/日志工具和取消子树清理，原资源状态机/游标/进程身份唯一；S30交互四端口接read/list/respond/stamp，原Broker只交付等待者，回答处理注册原daemon协调器effect_tasks句柄集合，客户端Drop不能detach已接受的claim，shutdown统一join效果任务；S31将原DelegationRequest/Record移至core，八端口接实际spawn/read/list/wait/cancel/result reservation/RPC/工具/启动结束路径。child session复用原sessions::create_in与原授权继承；原继承目前仅Local，native明确拒绝NonLocal准入，不能称完整企业委派完成。三领域复用原Connection/four-permit/原事务，不新增业务事实副本。只进行开发格式化，未执行测试、cargo check、Clippy或九门禁，集中验证留待开发完成。完整Wave2–10仍未完成，enterprise_ready=false。

开发追加 S32–S36（尚未编译、未测试）：S32队列五端口接queue RPC、重启32条稳定游标恢复、订阅/取消/关闭终态读取；旧同步SDK队列和request查找共用同一scoped helper。父委派冻结cwd、request_approval、只读工具和预算复核，child也消费原1024队列总预算。S33会话控制三端口接运行装配、trace元数据、偏好选择和旧transcript导入，选择/导入在原事务核lifetime；全域偏好键和旧文件导入尚仅Local。S34维护四端口、计划管理四端口接doctor、启动/关闭恢复、GC、人工核验、RPC/斜线计划和旧计划导入，重启恢复及writer统计按SQL完整scope过滤；原Hook恢复校验绑定并保存unknown。S35观察三端口和旧记忆导入一端口接canonical诊断、context摘要、legacy artifact读取及旧文件导入；canonical批次digest/长度复用唯一decoder，历史artifact只核永久归属，无需writer。S36生命周期Hook由至多一个受管发布批次定时排空，存储失败保留待发而非ACK；Cron通过daemon弱引用任务委派器接固定执行HookScope及终态观察，不新建结果状态；idle探针受管，不在listener分支等待数据库。上述都是源码实施，不是并发或故障合同通过的证据。只有cargo fmt开发格式化；未执行cargo check/test/Clippy或门禁。当前schema27，全量PG事实和完整Wave2–10仍欠，enterprise_ready=false。

开发追加 S37–S38（尚未编译、未测试）：启动采用async bootstrap，同一事实源读取session元数据/偏好/旧历史并由唯一assemble_prepared构建daemon。native执行新的with_native_execution不携带旧ControlRepository，上下文、记忆、Provider捕获、Exec冻结配置均从async领域端口与固定owner读取；旧同步接口仅留SDK兼容，DaemonState生产字段已移除同步聚合。工厂目前仍接受具体RunStore，全量PG装配尚欠。静态复核同时补Hook恢复身份字段SQL预算、writer启用对损坏Hook JSON的拒绝及context摘要先永久归属核验。仅开发格式化，不运行编译/测试/门禁。

开发追加 S39（尚未编译、未测试）：新PgCanonicalStore复用原PG发布连接/driver，独立canonical schema v1（现有coordination仍v1）；已写现有业务事实及writer/audit/quota的完整四维域DDL、永久lifetime外键、scope RLS、不可变事实和scope版本时钟。native writer读/获取/续租/释放和绑定句柄维护已实现，mutation在原TX核固定proof并提交前复核数据库时钟。PG会话lifecycle Create/End/Close/Fork、元数据/偏好/Local旧历史导入与实际管理CLI已接，原Hook lifecycle publication同事务。PG事件append/read/page、版本封存和canonical outbox在原TX实现；NOTIFY仅唤醒。完整scope身份读取/冻结授权检验、初始化域CLI、审计hash-chain读取/幂等append及配额账户配置/预占/结算亦为真实SQL实现。当前PG runtime尚未可装配：RunExecution等其余PG端口、远程TLS、完整离线迁移仍欠，不称全业务PG或完整Wave2完成。依赖仅新增已在lock中的serde/sha2直依赖；cargo fmt开发格式化成功，未cargo check/test/Clippy/真实PG或门禁。完整Wave3–10尚未进入，enterprise_ready=false。

开发追加 S40（尚未编译、未测试）：PG RunExecution五方法已写真实普通/独立workload准入、固定owner和冻结授权、请求/计划receipt幂等、scope队列预算、稳定run计数、队列启动及TurnCommit事务。工具未确认/Provider未确认/已启动副作用取消共用core normalize_run_terminal_status（原SQLite caller也调用同函数）；开放tool exchange结算unknown并闭合原transcript，Provider attempt归unknown，usage/planCAS/终态/委派result原事实同TX。新增PG队列五端口、完整域游标读取和元数据查询，实际控制CLI沿原scope仍enterprise_ready=false。PG plans原document/version/identity/decision helper已写，仍欠完整计划端口、原context/compact/工具/请求等PG适配与Daemon PG装配、离线迁移。静态读取发现grant提取中残留tx变量、队列恢复limit原端口为usize/请求lookup返回RunRecord，已按原声明修正；没有运行编译或测试，不把静态修复算验证证据。

开发追加 S41（尚未编译、未测试）：PG canonical/model/omitted会话读回及列表由同一REPEATABLE READ事务构建，包括原history/队列/交互/终态/计划/上下文与固定scope；canonical message IDs移入core由SQLite/PG共用。PG Hook观察三端口与执行四端口真实实现，claim保存原running并ACK原publication，settle保留unknown不重播；Hook合法性移至core共用。历史Hook只核永久归属，运行Hook核固定授权及writer，ownerless生命周期核当前head proof，已删除head只可协调，不能执行旧run。静态复核发现SessionSnapshot.prefix_digest原为按batch raw拼接哈希，PG改回同语义；transcript/Hook添加原rowid等价的storage_row_id稳定顺序，lifecycle退休proof退出续租集。原RunQueue接口签名已按usize/RunRecord修正。只有cargo fmt，未执行cargo check/test/Clippy/门禁/数据库。离线工具和其余PG端口、PG daemon装配仍欠，全Wave2–10未完成。

### S42–S43 开发记录（尚未验证）

按用户“先按规划开发，完成后测试再修复”继续：新增 PostgreSQL RunFact（4）、Provider attempt（4）、interaction（4）、RunTurn（6）、discovery/cancellation（4）、RunTool（6）、对象 artifact 发布（3）的真实领域实现。身份、永久 lifetime、原 SQL 绑定和字段预算先于正文，mutation 携带已绑定 writer proof 并在提交前复核数据库时钟；开始/终态事件与 outbox 同事务，网络对象 I/O 在事务外。

两种后端共用 Provider attempt 身份解码、终态调用身份、实时事件许可集合、interaction 正文规则、计划 stage/摘要/Markdown 验证、冻结目录发现许可、artifact receipt/completion 验证。原 no-follow 文件发布实现移动到 agent-artifact 共用，SQLite 原 API 通过重导出保留；PG 的宿主文件发布必须显式配置且只允许 Local，四 permit 包含取消后仍在执行的文件 worker。PG 工具收据与对象 published 同一原事务，没有第二套工具终态。

补了当前角色与冻结授权角色的比较；canonical DDL 添加 JSON 正文约束、原事件不可改写、published stamp 保护、Hook/对象发布单调标记及稳定 Provider/discovery 插入序号。DDL未运行。仅 cargo fmt 格式化成功，不代表编译或行为正确，S25旧验收不覆盖本树。请求/context/compact、记忆、资源、委派、管理/维护、runtime装配、远程TLS及离线迁移等仍待开发；完整Wave2及Wave3–10均未完成。

### 2026-10-09 敏捷续开发

已阅读提示词、基线/架构/进度及源码改动，补 PG 后台资源八端口与两后端共享规则，修复前批遗留导入/借用/类型/Arc接线编译问题。局部 check 通过。第一次 SQLite 局部测试编译发现 flywheel trait缺导入和新增夹具误用不存在的in_memory，均已修正，保留为编译失败而非业务RED。首轮 PG 真实资源合同通过，最终源码局部测试仍在收尾；没有跑完整测试或门禁。详见 docs/enterprise/wave2-resource-continuation-report.md。

最终结果：agent-daemon + agent-storage-postgres 局部 locked/offline check exit0，无告警；core资源3、SQLite资源2、真实PostgreSQL资源1全部通过。真实PG18.6合同包含事件插入故障下资源/时钟/events/stamp/outbox回滚，重建连接读回永久事实；未运行实际后台进程，不称完整远程worker验收。自己的临时测试数据库已删除。开发格式化与diff检查通过，未跑全量测试/九门禁；未提交/推送/部署。资源端口切片完成，完整PG runtime与Wave2–10仍未完成。

### 2026-10-09 计划、诊断、续跑和装配

PG PlanControl4/RuntimeObservation3/HookContinuation3已实现，新增文件与原lib模块真实接线；共用原admit_in/commit_terminal_in及唯一transcript/关系/计划receipt decoder。计划丢弃最终write/receipt之后seal，snapshot/stamp/outbox一并提交。DaemonRepositories为同一adapter构造26端口，实际Local启动已使用；兼容SDK委托同一工厂。首次装配check发现SessionRead不提供metadata，两处改为原SessionControl端口；修复后生产最终locked/offline check exit0无告警。只有局部编译、格式化和diff检查，本轮未执行测试/实库/完整门禁。完整PG runtime与Wave2–10未完成。详见docs/enterprise/wave2-plan-observation-continuation-report.md。

最终test-support局部cargo check exit0；保留两组兼容测试字段/资源helper dead_code告警。最终生产check无告警，git diff --check通过。本轮没有运行测试或真实数据库合同。


### 2026-10-09 PG 记忆、委派、维护与本机启动

补齐同源 PG 记忆/维护/委派领域端口，并将 daemon/Cron/导入/artifact 接到完整26端口装配；默认SQLite保留，postgres-local显式Unix socket与writer、空Local域human和独立workload初始化，PG文件目录绑定database/schema。canonical迁移v2实际执行，遗忘不破坏历史反馈幂等；共享摘录/凭据过滤和授权conditions。恢复、核验及文件GC使用原事实事务，GC取消后仍受管并排空。实库冒烟发现并修正resource session_id关联错误、同owner过期预留、异步Provider预算遗漏；实际启动发现PG UUID与trace文件校验不兼容，保留32hex兼容并接受严格UUID。局部生产编译和test-support内部测试源码编译通过，真实PG18.6合同1项通过。真实binary双后端启动/假模型chat/重启去重通过，每个后端仅调用模型一次，PG未生成SQLite事实库；未跑完整测试或门禁，完整Wave2–10未完成。详见docs/enterprise/wave2-native-runtime-report.md。

最终补正：真实PG chat暴露执行future与事件消费者等待同一连接的闭环；改为join并行驱动，已接收事件完成及剩余队列排空后才提交终态，未引入第二执行状态。最新binary的两个后端冒烟均exit0，使用本机假模型及独立临时工作区。
# 本批收尾验证

PostgreSQL schema v2 下资源生命周期/日志/作用域/事务回滚局部合同通过。自行创建的临时 PostgreSQL 数据库已删除；双后端冒烟 daemon 和临时工作区已清理。本轮未执行全量测试及完整门禁。

### 持续开发：TLS与配额

已实现PgConnectionConfig严格文件配置、CA/hostname校验TLS、可选mTLS、受保护password/key文件、超时和断连零重放；connect_local保留，PgCanonicalStore共用原迁移，daemon新增postgres装配。TLS局部生产cargo check通过（/tmp/agent-wave2-tls-check.log）。新增SQLite schema28审计hash-chain/配额和同源读写端口；两后端runs额度原事务预占/终态结算，Local无配置继续原体验；RPC/CLI audit/quota接线。ledger局部编译首次发现MutexGuard借用与新Ledger错误match缺口，正在修复，尚未称合同通过。完整Wave1/2–10仍未完成。

### 2026-10-10 持续开发：TLS、账本与离线迁移

已实现远程PG严格CA/hostname TLS及可选mTLS配置（真实远程握手待验收）；SQLite schema28和PG原准入事务的runs配额预占/终态结算、审计hash-chain、audit/quota RPC与CLI均已接线，装配29个领域端口。同一ledger合同SQLite与真实PostgreSQL18.6分别通过；第一次include-ignored因未设置测试DB环境变量启动失败，不计行为失败。

全canonical离线工具prepare/dry-run/copy/verify/switch/rollback已接真实PG管理入口，保留原源库、artifact store_id、backup证据和daemon切换marker。自有fixture 68表52行（artifact0）完成真实dry-run、提交、逐表逐行对账、切换与回滚；回滚后正确格式PG迟到mutation返回Conflict。首轮deferred FK触发器未清空导致ALTER拒绝，已先SET CONSTRAINTS ALL IMMEDIATE修复；回滚等待栅栏前固定RR快照风险已改READ COMMITTED。上述局部没有证明大artifact、远程PG、切换后真实daemon和迁移故障矩阵全部通过。

当前正在接企业RuntimeIdentity受保护启动配置与全daemon scope路由，并修复PG普通policy/role发布错误撤销在途run的语义；尚待必要编译与局部身份验证。完整Wave1/2仍部分，Wave3–10仍待实现，enterprise_ready=false。未全量测试/门禁，未提交/推送/部署。

### 2026-10-10 企业域与身份治理续开发

当前 SQLite schema29、PG canonical v3、daemon 同源30领域端口。真实代码已接企业 RuntimeIdentity、会话事务授权、身份注册/角色/策略/撤销 RPC+CLI、冻结策略与双人审批。双后端 identity_governance_contract 两项通过（含撤销旧票、幂等与越域拒绝），PG ordinary_publish_preserves_run_but_explicit_revocation_fences 一项实库通过。生产 daemon/storage-postgres/cli 局部 check 和真实 binary build 成功。双租户 daemon 冒烟首次因夹具会话名不符合原格式被拒，已修夹具待重跑。Wave3 WebSocket128帧/8MiB/64并发、connect/write/idle deadline 和慢消费 detach 已接真实入口，局部check通过；完整慢消费/断线验收未运行。Wave1/2完整验收与Wave3剩余身份/directory/共享限额仍欠，Wave4–10尚待实现，enterprise_ready=false。未提交、推送或部署。

企业scope真实binary冒烟最终通过：两个租户同一PG schema、同project/agent/session/request ID，各自run-1，重启幂等，模型总2calls，quota各消费1，审计scope一致，未创建SQLite事实库。夹具首次错误已修并保留诊断记录。证据见docs/enterprise/evidence/20261010/agent-wave1-scope-smoke.json。Wave3已新增jsonwebtoken9.3.1有限JWKS验签、可信scope/subject/epoch transport envelope、Gateway/daemon双端接线，正在局部编译；尚无OIDC行为验收。HTTP审批改为pending/detach，禁止因HTTP不能交互而伪造拒绝。新增依赖后--locked拒绝未更新依赖边，正在一次offline更新lock，不算行为测试失败。

### 2026-10-10 Wave3 Access Plane实际接线

新增jsonwebtoken9.3.1（固定lock，Rust1.88），有限JWKS缓存（32keys/128KiB/TTL/5秒刷新冷却）、HTTPS/DNS pin/禁代理与重定向、RS256/ES256/EdDSA允许集、issuer/audience/exp/nbf/iat/短期access用途及可信scope/subject/epoch校验。BearerCredential Debug脱敏，不进入params或canonical facts；Gateway/daemon独立验签，原身份/角色/策略事务作最终授权。SQLite schema30/PG canonical4新增session_directory和Access租约/计数，daemon同源31端口。Directory创建复用原session create helper并同TX审计；路由不可变，真实HTTP /enterprise/sessions、/enterprise/rpc只公开opaque session_id。租户/主体/endpoint三层数据库时钟固定窗口和并发预算，PG同tenant advisory lock；请求先获取共享lease，流body持有lease，Drop异步release、过期收敛，不生成第二run。HTTP30s首响应、240s总流与15s idle预算，WS256本机连接、64并发、128帧/8MiB以及240s企业连接预算。上一批局部cargo check通过；最新WS目录接线、共享合同与OIDC mock合同正在必要编译/验证。仍欠登录nonce/state/PKCE持久化、APIkey Argon2id/服务凭据、共享限额配置/完整用量成本、全网关多runtime路由；Wave3未完成，Wave4–10尚待实施，enterprise_ready=false。

### 2026-10-10 Wave3 访问面与凭据纵向切片

SQLite schema31 / PG canonical v5，daemon 同后端装配32领域端口。API key仅落Argon2id PHC，首发行返回明文，同operation重读不再返回；撤销同事务递增主体epoch。Gateway使用固定服务主体+Gateway角色的独立凭据证明入口channel，外部params不能决定授权属性。

局部验证：OIDC最小JWKS签名/域/audience/expiry/未知kid合同1项通过；Access+凭据共享合同SQLite/真实PG各1项通过，含24连接竞争只16个准入、凭据幂等及撤销。真实binary HTTP→UDS→PG冒烟通过：opaque目录幂等、用户chat、跨租户401、假模型只调用1次。先前fixture audit limit128超过64，改fixture后通过；不能将此前失败记为通过。证据在evidence/20261010。

仍在开发：进程内token bucket和目录current lifetime额外复核已写入，尚待本切片编译；登录state/nonce/PKCE、真正轮转/stale矩阵、multi-endpoint路由及细粒度费用账本未完成。完整套件、Clippy、release、MSRV和真实远程OIDC/集群验收未运行，enterprise_ready=false。

### 2026-10-10 登录与访问面续开发

新增SQLite schema32、PG v6一次性登录事务，Gateway /auth/login 与 /auth/callback 接原daemon端口。S256 PKCE、HttpOnly/SameSite/Secure cookie、issuer/state/nonce/ID-token subject/audience/azp 检验已实现；消费CAS发生在授权码兑换之前，未知兑换结果不重放。32端口保持同后端，OAuth不是第二业务session。OpenAPI3.1静态导出已接/openapi.json。

真实HTTP授权码登录冒烟通过：错误state不消费，合法回调返回短期token（no-store），回调重放401、token端点只调用1次，原opaque session可读。双后端一次性消费/错误verifier/重放拒绝合同各1项通过。身份合同含nonce/主体绑定1项通过，第二把真实Ed25519钥匙轮转、刷新冷却和过期stale拒绝合同1项通过；TTL测试调整私有缓存时钟，未实际等待30秒。

进程内三层token bucket和current lifetime保护已写入且生产编译过；targeted bucket unit尚待执行。持久目录多端点路由正在接线，受保护端点map、运行实例endpoint+scope校验、按原operation查已发布目录已写；最新路由树尚待编译/双端点真实冒烟。细粒度input/output/cache/tool/sandbox费用账本和分层预算尚未完成，Wave3仍进行中。credential.status已受统一RPC准入核验，未发现原先假设的独立JWT绕过；显式重复查询已删除。

- 2026-10-10 续开发：修JSON-RPC result:null存在性，局部protocol合同1项pass；真实双端点HTTP冒烟越过directory/chat后在restart run.read403，定位平铺DTO投影，修复待重跑。费用policy、双backend管理员端口、前向33/v7迁移、准入/Provider/tool/resource账本和冻结预算限额正在接线；最新必要check运行，未标完成。下一项：补局部双backend费用合同、实库迁移/双端点smoke，再完成Wave3后继续Wave4–10。
- 双backend billing_contract5共2项通过（实PG两连接竞争）；schema33/PGv7实库迁移验证。发现原四permit背压应由合同承认，未放宽并发。继续细分共享endpoint lease（34/v8前向迁移）、重建真实binary并跑双端点重启+登录+费用链路，随后补工具/resource局部预算合同和多scope入口路由。
- 真实双端点Gateway重启+PKCE登录+费用HTTP组合冒烟pass；max_tokens128真实发送、cost30各层原TX结算、1modelcall。修Provider先attempt后capture引起预算冲突：capture先保存，原attempt事务预算后才派发。证据已归档evidence/20261010，继续多scope Gateway受保护配置和路由，然后Wave4。

- 多scope Gateway编译发现Query字段移动错误，改取内部DTO；WS首帧无显式HTTP别名时允许受保护JWT scope选路，OAuth cookie按配置别名隔离。新增工作区连接缓存256上限。正在必要编译与真实多租户冒烟。

### 2026-10-10 多企业 Gateway 纵向切片已局部验证，开始 Wave4

当前 schema SQLite34 / PostgreSQL canonical8，daemon33同后端领域端口。单 Gateway 受保护 AGENT_GATEWAY_SCOPES 配置最多16个完整scope，JWT未经验证的scope只用于有限配置选路，选路后Gateway和daemon分别完整验签/原事务授权。API key以X-Agent-Scope或WS首帧scope选路；错误/跨scope均401。OAuth cookie按别名隔离，额外OAuth回调必须包含配置别名；客户端连接缓存上限256。

必要cargo check/build通过；真实HTTP Gateway + 三daemon + 同PG schema的两个tenant冒烟通过：同名主体/Agent/operation的opaque目录互不访问，B的API key仅在B配置可用；原A双运行端点/Gateway重启/授权码一次消费/输出上限128/费用30微单位/模型仅1次调用仍通过。证据evidence/20261010/agent-wave3-multi-scope-smoke.json。未测试真实远程IdP/HA/多Gateway压力、工具与resource费用专项仍待验收；Wave3功能主要入口实现，不据此宣称完整验收。Wave4注册表/配置/签名代际正在实施，Wave5–10尚未实现，enterprise_ready=false。

- Wave4注册表签名扫描移到数据库事务外；原TX只核expected revision/current trust digest、不可变generation和operation receipt。运行中session绑定固定代，canary/rollback只影响后续新lifetime；原Local未安装session固定None。SQLite35/PG9新迁移尚待实库合同，未宣称Wave4完整。

- Wave4注册/config/签名纵向切片：SQLite35/PG9实库共享合同各1项通过；34同后端daemon端口。CLI/HTTP/RPC发布与读回、原session绑定/准入TX/原run snapshot/LoopEngine真实接线。最新MCP来源/Hook信任/根历史/CLI NOFOLLOW改动必要binary build进行中，随后真实HTTP固定V1而current V2模型请求冒烟。完整验收与Wave5–10仍未完成，不退出任务。

- Wave4真实HTTPsmoke首次在registry请求前共享Access准入503，未生成注册业务事实；定位valid_access_endpoint缺新路由白名单，补registry/generation与后续固定workflow/A2A endpoint后必要重构建。旧PG/SQLite注册合同通过，不能将该HTTP失败算成功。

- Wave4真实入口组合smoke通过：同Gateway两tenant/三个daemon/实PG9，先发布签名V1并创建原目录会话，再发布V2+100%canary，旧会话实际模型请求含V1 prompt且无tools，重启仍读原run；费用30微单位、输出128、1modelcall和PKCE一次消费保持通过。证据agent-wave4-registry-runtime-smoke2.json。注册/config/发行主要纵向切片已实现并局部验证；完整并发/归档CLI安全矩阵/三入口parity待验；Wave5九类持久DAG/native引用/A2A正在开发，Wave6–10仍待实施，继续不停。

- Wave5持久DAG继续：SQLite36/PG10前向迁移与共用事务编排已接真实后端，35领域端口、严格RPC/CLI/HTTP入口、有界独立workload调度、AgentRun原run同TX关联、SubWorkflow同TX创建与关联。必要入口check2通过；原生工具/Approval/Webhook和snapshot恢复语义正在继续接线，最新编译未完成。尚未运行36/10实库合同，不能标Wave5完成；Wave6–10仍未实施。本任务继续推进，不退出。

- Wave5必要契约：agent-wave5-workflow-sqlite3.log 1项pass；agent-wave5-workflow-postgres2.log实PG1项pass（此前首轮漏关联失败单独保留）。生产a2a/compensation check通过；最新结果幂等/Compensating、真实双backend HTTP/A2A冒烟binary构建中。新增scripts/smoke-workflow-a2a.py尚未执行，不能记为通过。下一步补artifact引用并跑真实后台+原tool+SSE断流+重启，再补人工/补偿/未知reconcile；Wave6–10仍在原任务范围内，不能停。


## 2026-10-11 远端开发检查点

用户明确要求提交当前修改到云端。正在整理真实实现与状态记录，保留全部已有改动，不重写历史、不生产部署。Wave5 session binding 最新 binary 构建通过；派生 run 清除父 workflow 执行指令的安全修复正在局部验证。A2A 前三轮失败没有计作成功，最终入口冒烟仍待验。Wave5 仍部分，Wave6–10 未实现，完整验收后置。远端与本地原 HEAD 一致；准备在当前 main 上提交并普通推送，无强制推送。

提交前最新局部验证：`cargo test --locked --test workflow_contract sqlite_workflow_native_fence` 通过（1 passed）；包含最新会话绑定及派生指令清除源码的编译。日志已归档 evidence/20261011/checkpoint-workflow-contract.log。完整套件及 Wave5 最终真实 HTTP 冒烟未运行；历史日志原样保留，源码/迁移的 git diff --check 通过。
