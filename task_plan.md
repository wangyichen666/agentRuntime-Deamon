# 个人级 AI 编码 Agent 实施计划

## 目标

使用 Rust 从零构建一个结构清晰、个人可用、单进程 CLI 形态的 AI 编码 Agent。严格按阶段实现并验证，核心版覆盖需求中的 11 个处理环节。

## 阶段

| 阶段 | 状态 | 主要交付 |
|---|---|---|
| 0. 项目骨架 | complete | binary crate、模块目录、基础配置 |
| 1. ReAct 循环 | complete | OpenAI 兼容 Provider、read_file/exec、工具分发、REPL |
| 2. 安全与并行 | complete | 参数校验、路径策略、命令安全、审批、并行波次、write/edit |
| 3. 上下文工程 | complete | 上下文分块、环境注入、token 估算、递归压缩 |
| 4. 持久化与会话 | complete | session.jsonl、恢复、会话串行锁 |
| 5. 个人增强 | complete | 关键词 + 中文 bigram 长期记忆（其余可选项不默认扩张） |
| 6. 全量验收 | complete | build、clippy、test、README、手工可验证说明 |

## 进阶增补（2026-09-08）

| 批次 / 功能 | 状态 | 主要交付 |
|---|---|---|
| 基线通读与偏差核对 | complete | 逐模块核实现状，记录影响后续设计的偏差 |
| 第一批 1. plan | complete | 可落盘计划状态、plan 工具、动态上下文注入 |
| 第一批 2. sub_agent | complete | 可复用执行入口、隔离历史、受限工具、15 轮预算、禁止递归 |
| 第一批 3. 前缀缓存 | complete | 稳定前缀排序、DeepSeek 自动缓存、OpenAI/DeepSeek usage 日志 |
| 第二批 4. 图片/PDF | complete | 图片内容块、可选多模态、PDF 本地文本抽取与限制 |
| 第二批 5. 重复检测 | complete | 调用/参数/结果指纹与建议性提示 |
| 第二批 6. 两级压缩 | complete | 60% 温和压缩、85% 强力压缩、环境可配置 |
| 第三批 7. skill | complete | Markdown 索引、关键词/bigram 匹配、按需正文注入 |
| 第三批 8. cron | complete | daemon 内轻量调度、独立 session、重试、heartbeat、持久化与 slash 管理 |
| 第三批 9. MCP | complete | 自研 stdio JSON-RPC、握手、动态工具桥接、审批隔离与子进程生命周期 |
| 进阶全量验收 | complete | release build、36 项测试、严格 clippy、格式、CLI 烟雾测试与文档 |

### 进阶实施原则

- 在现有模块边界上增量重构，不重写核心。
- 第一批、第二批与第三批 skill/cron/MCP 均已实现；新增外部能力仍必须进入统一工具注册、schema、safety 与 approval 链路。
- sub_agent 默认受限工具集，不持久化到主 session，且不暴露自身，递归深度固定为 1。
- 多模态优先保持 OpenAI 兼容；PDF 采用本地开源解析，图片能力通过可选模型配置控制。

## 关键决策

- 项目目录本身作为 crate 根目录，不再嵌套一层 `my-agent/`。
- API 使用 OpenAI Chat Completions 兼容协议，配置全部来自环境变量。
- 安全审批由 CLI 回调提供；工具与主循环只依赖抽象接口。
- 测试使用 mock provider，不依赖真实密钥或网络。
- 阶段五已补齐个人版所需的记忆、cron、子 Agent 与本地 MCP；远程 MCP transport、RBAC 与系统级沙箱仍明确留作后续。

## Daemon + 三入口演进（2026-09-08）

### 目标

在不重写 ReAct、安全、记忆、计划、上下文与工具逻辑的前提下，把运行时状态收拢到 daemon，提供共享 JSON-RPC 协议、Unix socket 客户端、瘦 CLI、本地 HTTP API 和专业启动自检。

| 阶段 | 状态 | 主要交付 |
|---|---|---|
| 基线通读与偏差核对 | complete | 核实现有所有权、审批、session、取消和流式能力 |
| A. 协议与 daemon 内核 | complete | 协议 SSOT、DaemonState、handlers、进程内 client 回环 |
| B. daemon/socket/启动优化 | complete | UDS、生命周期探测与自动拉起、clap 子命令、配置检查、瘦 REPL |
| C. 本地 HTTP API | complete | 127.0.0.1 默认绑定、health、OpenAI 兼容 chat/SSE、非回环 token 门禁 |
| D. 编辑器 stdio 骨架 | complete | JSON-RPC stdin/stdout 适配器，复用 DaemonClient |
| 全量验收 | complete | 48 项回归、跨入口一致性、release/clippy/fmt、配置诊断、HTML 桌面交付 |

### 初始设计约束

- daemon 是历史、计划、审批、取消和运行中 turn 的唯一真相源；入口不得直接持有 LoopEngine。
- 阶段 A 先用内存双向通道验证协议与 handler，再替换为 Unix socket，避免同时调试所有权和传输。
- 流式事件必须从 LoopEngine/Provider 的共享层产生，不能由 CLI 或 HTTP 入口伪造另一套执行逻辑。
- 正在执行的 turn 不因挂钟超时被强杀；取消使用显式 cancellation token/协作式检查。
- 每阶段完成后先 build、test、严格 clippy，再进入下一阶段。

## 错误记录

| 错误 | 次数 | 处理 |
|---|---:|---|
| 验收启动 Web 时仅设置 `MY_AGENT_WEB_ADDR`，但该变量只影响 TUI launcher，serve 仍使用默认 8787 导致端口占用 | 1 | 读取 `my-agent serve --help`，改用 `--bind 127.0.0.1:18882` 后重试 |
| `view_image` 不支持直接读取 SVG 品牌封面 | 1 | 改用本机 SVG 渲染工具生成临时 PNG 后检查，不重复直接读取 SVG |
| plan 首轮 clippy 报 `PlanStore::in_memory` 为 dead code | 1 | 该构造器只用于单元测试，限定为 `#[cfg(test)]` 后重跑全套验证 |
| sub_agent 首次大补丁被 `apply_patch` 拒绝（同一文件重复 Update 段） | 1 | 补丁未落盘；改为每个文件单一 Update 段的原子补丁 |
| sub_agent 编译时报并行波次闭包 `FnOnce` 生命周期不够通用 | 1 | 并行迭代改为持有 `ToolCall` 克隆值，避免 `async_trait` future 跨层借用切片元素 |
| 多模态整体替换补丁因同文件 Delete/Add 被拒绝 | 1 | 补丁其余段先落盘、read.rs 被明确删除；随后单独新增完整 read.rs |
| 多模态验证 clippy 报 `ReadFileTool::new` 为 dead code | 1 | 正式入口使用 `from_env`，将无配置构造器限定为测试代码 |
| skill 首次编译时 `filter_map` 参数多标了一层引用 | 1 | 将 `&&Skill` 修正为迭代器实际产出的 `&Skill` |
| CUA 浏览器安全策略阻止打开本地 `file://` HTML | 1 | 不绕过策略；改做 xmllint 静态解析与源码一致性检查，并在交付说明视觉复检未执行 |
| 初始目录不是 Git 仓库，`git status` 失败 | 1 | 仅记录；构建任务不依赖 Git 仓库 |
| planning-with-files 技能声明的 templates 目录不存在 | 1 | 按技能定义的用途自行创建三个计划文件 |
| crates.io 索引更新超过两分钟无响应 | 1 | 用户建议使用中国源；探测 rsproxy 与 USTC 均可达，项目级切换至 rsproxy sparse，不改全局配置 |
| `tokio::io::stdin` 未启用 `io-std` | 1 | 在 Tokio features 中补充 `io-std` 后重新验证 |
| Clippy `borrowed_box` 拒绝显式 `&Box<dyn Tool>` | 1 | 让迭代器闭包推断引用类型，消除多余 Box 借用表达 |
| 阶段二首次测试 2 项失败、Clippy 3 项告警 | 1 | 支持 `mkfs.*` 变体；把读取夹具移入工作区；移除未使用方法；修正借用与导入位置 |
| 阶段三首次大补丁上下文不匹配 | 1 | 无文件被部分修改；拆分为独立小补丁依次接入 |
| token 估算泛型不能接收未定长切片 | 1 | 为序列化泛型增加 `?Sized` 边界 |
| 阶段四严格 Clippy 检出未使用的 `Message` 导入 | 1 | 删除已被会话恢复类型推断替代的导入 |
| 直接删除端到端验证临时文件被执行策略拒绝 | 1 | 改为移动到 macOS 废纸篓，可恢复且不再留在 `/tmp` |
| 进阶计划首次补丁因进度文件上下文不匹配而未应用 | 1 | 先读取文件末尾，再按现有内容拆分追加 |
| 最终 stdio 冒烟测试帧漏写 `jsonrpc` | 1 | 适配器正确返回 -32700；补齐 `jsonrpc: "2.0"` 后请求 ID、响应与空闲退出验证通过 |
| ACP crate 首次 `cargo search/info` 被项目 source replacement 拒绝 | 1 | 按 Cargo 提示改用显式 `--registry crates-io` 查询，不重复原命令 |
| ACP 首次编译出现 `ConnectionTo` 借用后移动 2 处及未使用导入 | 1 | `spawn` 前克隆连接供后台任务持有，并移除多余 `ConnectTo` 导入 |
| ACP 恢复整块补丁因 fmt 后锚点变化未匹配 | 1 | 补丁未落盘；读取当前片段后拆为 import、load handler、helper 三个小补丁 |
| Cargo 测试命令误传三个位置过滤器 | 1 | Cargo 尚未编译源码；改用单个 `--all-targets` 全量测试覆盖相关模块 |

## 标准 ACP + WebSocket + 重连恢复（2026-09-08）

### 目标

在现有 daemon 单一真相源上，把编辑器 stdio 私有透传升级为标准 ACP server，为 axum Web 服务新增全双工 WebSocket 私有 RPC 通道，并让 CLI、ACP、WebSocket 在连接或重连后恢复未决审批，且不改变 HTTP/SSE 兼容入口。

| 阶段 | 状态 | 主要交付 |
|---|---|---|
| 0. 基线与 ACP crate 调研 | complete | RPC/事件/审批真实结构、入口行为、官方 `agent-client-protocol =2.1.0` |
| A. 标准 ACP 入口 | complete | 官方 SDK、initialize/session 映射、typed update/permission、正式 Client 集成测试 |
| B. WebSocket 全双工入口 | complete | `/ws`、connect 鉴权、ID 映射、RPC/Event 双向审批、HTTP/SSE 回归 |
| C. 三入口重连恢复 | complete | daemon 可订阅事件真相、公共恢复 helper、CLI/ACP/WS pending 与 active 恢复 |
| D. 全量验收与交付 | complete | fmt/release/test/clippy、三入口断线恢复证据、README/HTML、提交推送 |

### 本轮约束

- 入口只翻译协议；session、approval、cancel 与请求终态仍只由 daemon 决定。
- 客户端断开、缺 ACK 或超时均不得自动批准、拒绝或清空 pending 状态。
- 优先采用固定版本的活跃开源 ACP crate；若实际 API 无法满足，再依据标准规范手写并记录原因。
- HTTP `/health`、`/v1/chat/completions` 及 SSE 保持兼容。

### 阶段 C/D 验收补充

- active turn 使用最多 1 MiB 回放缓存 + broadcast 实时订阅；原连接断开不再影响审批等待。
- `agent.subscribe` 为重连入口，按当前 pending 集合过滤已解决的旧审批回放，避免 ACP 重复弹窗。
- `session.load` 改从 append-only JSONL 读取快照，审批等待期间不会被内存历史锁阻塞。
- CLI、标准 ACP、WebSocket 均有断线后恢复活动请求、审批与最终文本的集成测试。

## TUI 入口（2026-09-08）

### TUI 视觉优化（已完成）

- 显式深色背景与文字色、居中限宽、消息留白、Markdown 标题/强调/代码样式。
- 精简工具回执、独立审批区、输入光标与粘贴、按实际换行滚动。
- 使用 TestBackend 验证宽/窄终端和长文本，生成渲染预览；更新 release 与使用说明。
- 验证：59 项全量测试通过；补充修正后 TUI 测试、严格 Clippy、release 再次通过。实际 PTY 验证中文草稿、Esc 退出及终端 echo/icanon 恢复。
- 测试修正：宽字符的占位单元默认样式不代表可见字符颜色；渲染断言按可见字符检查。Swift 预览遇到系统 SDK 模块冲突，改用 Objective-C/AppKit 从 TestBackend 单元格生成 PNG。

### 目标

增加类似 Claude Code 的终端交互界面，但保持 TUI 为瘦客户端：不持有 Provider、LoopEngine、session 或审批真相，只通过已有 `DaemonClient` 调用 daemon。

| 阶段 | 状态 | 主要交付 |
|---|---|---|
| T1. 终端基础设施 | complete | ratatui/crossterm、终端原始模式、退出恢复、`tui` 子命令 |
| T2. 对话与事件流 | complete | 输入框、消息滚动、文本增量、工具状态和终态响应 |
| T3. 审批/取消/重连 | complete | y/N 审批、Ctrl-C 取消、session.load + agent.subscribe 恢复 |
| T4. 验收与文档 | complete | TUI 单元测试、命令文档、HTML/README 同步、全量构建验证 |

### 架构决策

- TUI 代码放在当前仓库的 `src/entry/tui.rs`，因为它是本项目的正式入口，需要与协议类型和 `DaemonClient` 一起版本化。
- TUI 不应复制 `LoopEngine` 或直接调用工具；daemon 仍是唯一真相源，未来 ACP/HTTP/CLI/TUI 共用同一套事件语义。
- 仅把终端绘制和用户输入放在 TUI；恢复、审批响应、取消和请求生命周期通过现有 RPC 完成。

### TUI 终端主题兼容修复（已完成）

- 默认改为终端原生主题：继承前景色与背景色，不再整屏强制 RGB 背景。
- 保留显式真彩深色主题，通过 `MY_AGENT_TUI_THEME=dark` 启用。
- 增加渲染测试，确保默认主题不写入固定背景色；fmt、60 项测试、严格 clippy、release 构建与实际 PTY 退出恢复均通过。

## 默认新会话与 `/resume`（2026-09-08）

### 目标

每次启动交互入口都创建全新 session，不自动展示旧对话；用户在 TUI/REPL 输入 `/resume` 后查看历史 session，并选择一个继续会话。断线后仍允许 daemon 保持活动任务，不把“启动新会话”和“恢复未完成请求”混为一谈。

| 阶段 | 状态 | 主要交付 |
|---|---|---|
| S1. 现状与语义核对 | complete | session 存储格式、daemon RPC、TUI/CLI 恢复路径与活动请求约束 |
| S2. daemon 会话切换能力 | complete | 可列举元数据、按 ID 加载并切换当前 session、新建 session |
| S3. TUI/CLI `/resume` | complete | 启动新 session、列表选择、取消与错误反馈 |
| S4. 回归与文档 | complete | 单元/集成/PTY、README/HTML、release、clippy |

### 本轮约束

- daemon 仍是 session 唯一真相源，TUI/CLI 只通过 RPC 操作。
- 不能在活动 turn 或待审批期间静默切换 session；必须给出明确错误，避免结果写进错误会话。
- 历史列表至少展示稳定 session ID 与可识别摘要，恢复必须由用户明确选择。
- 已记录错误：首次源码检查工具调用的 JavaScript 字符串拼接有语法错误，未执行任何命令；改为单一合法命令字符串后继续。
- 已记录告警：移除启动时自动恢复调用后，旧 CLI `recover_connection` 三个函数成为 dead code；保留集成测试所需 helper 并用 `#[cfg(test)]` 收窄，删除无调用的生产包装函数。
- 已记录格式检查失败：原子 current 指针临时文件表达式不符合 rustfmt 单行布局；其余 63 项测试、Clippy 和 release 仍通过。执行 rustfmt 后单独重验格式门禁。
- 已记录测试编译失败：新增 TUI `/resume` 集成测试漏导入 `SessionStore`，生产代码未受影响；按编译器建议补齐测试模块导入后重验。

### SQLite 取舍

- 本轮不引入 SQLite：它对 `/resume` 正确性不是必要条件，同时迁移 session、memory、plan 会扩大风险面。
- 后续会话规模增长后，可用 SQLite 保存 session/message/plan/memory 元数据与全文索引；图片和大工具输出仍保留文件，仅记录路径，并提供 JSONL 导入。

## 六项通用能力补齐（2026-09-09）

### 目标

在现有单 crate、daemon + 多入口架构上，按顺序实现并验证：多 Provider、严格 tool-call 装配、共享 Slash 命令、版本化 Skill、本地 Cron + Heartbeat、stdio MCP 客户端；保持 OpenAI 路径与既有安全边界兼容。

| 阶段 | 状态 | 完成标准 |
|---|---|---|
| 0. 源码基线与现状确认清单 | complete | 通读相关源码，回答需求中的 8 组问题，记录初始测试基线 |
| 1. 多 Provider 协议 | complete | OpenAI/Anthropic/Ollama mock 闭环；编译、clippy、测试通过 |
| 2. Tool-call 严格装配 | complete | identity、纯增量装配、fail-closed 测试通过 |
| 3. Slash 命令框架 | complete | 单注册表、多入口复用、帮助自动生成，回归通过 |
| 4. Skill 体系升级 | complete | frontmatter、semver、本地安装器、稳定排序测试通过 |
| 5. Cron + Heartbeat | complete | 持久化、独立会话、有限重试、无人值守安全测试通过 |
| 6. MCP stdio 客户端 | complete | 握手、工具桥接、隔离、审批、进程清理测试通过 |
| 7. 全量回归与完成报告 | complete | fmt/check/clippy/tests/release 全绿，配置和限制文档化 |

## TUI 交互与渲染能力补齐（2026-09-09）

### 目标

在不改变 daemon 作为唯一真相源、保持单 crate 的前提下，完成 TUI 的退出解耦、结构化消息、输入编辑、滚动、渲染缓存、排队发送、并发请求/审批表达和三主题语义色板。

| 阶段 | 状态 | 主要交付 |
|---|---|---|
| 0. 现状确认 | complete | 退出、状态、UiMessage、输入、滚动、快照、颜色与渲染热点定位 |
| 1. 类型化退出 | complete | `should_quit` 独占退出控制流，slash/Esc 不依赖文案 |
| 2. 结构化 UiMessage | complete | Text/Tool 节点、稳定 id、状态/耗时、工具卡片 |
| 3. InputEditor | complete | 光标、词操作、多行、历史、CJK 列定位 |
| 4. 滚动与鼠标 | complete | line/page/top/bottom、follow-bottom、可选鼠标 |
| 5. 换行缓存 | complete | 按消息版本/宽度缓存与 session/resize 失效 |
| 6. 发送队列 | complete | FIFO 排队、自动出队、可见与清空 |
| 7. 多活跃/审批集合 | complete | 快照全量重建、并发轮次与审批队列 |
| 8. 语义主题 | complete | terminal/dark/light tokens、无硬编码组件色值 |
| 9. 全量验收 | complete | render/逻辑/集成/PTY、fmt/clippy/release、文档 |

### 本轮原则

- 严格按 0→7 推进，每阶段验证后再进入下一阶段。
- 协议差异封装在 provider 内，工具装配失败整轮原子拒绝。
- 新工具来源不绕过 schema、safety 与 approval；保留用户已有改动。

### 本轮错误记录

- planning-with-files 技能引用的 templates 目录不存在；按技能定义的职责复用并追加仓库现有三份规划文件。
- 首次 provider 整文件替换补丁因同一 patch 同时 Delete/Add 被拒绝，未产生半成品；拆为两次 apply_patch。
- 多 Provider 首次 check 发现 Anthropic 消息向量需要显式类型及两项 unused import；按编译器定位修正。
- 首轮 73 项测试中 4 项旧 mock 语义失败：内部 identity 已编码，且空 ToolCalls 没有事件而被视为文本；测试按 provider 边界还原 id，空批次改为 typed assembly failure。
- 阶段 1 严格 Clippy 检出一次 `and_then(Some)`，按建议改为 `map`。
- 定向测试命令误传两个位置过滤器，Cargo 在编译前拒绝；改为单个 `--all-targets` 全量测试，不重复该用法。
- Slash 接线首次 check 发现已删除的 TUI 本地编号解析测试与两个 dead-code helper；删除平行解析测试/旧 recovery helper，并将仅测试注册表枚举收窄为 cfg(test)。
- Skill 依赖首次 check 通过但发现两个仅测试构造器在生产目标 dead_code；用 cfg(test) 收窄，并把旧无 frontmatter 的 context fixture 升级为新格式。
- Cron 首次 check 发现 slash 参数解析使用了未导入的 anyhow Context；补齐 trait import。生产目标还提示测试兼容构造器 dead_code，已用 cfg(test) 收窄。

## 多窗口独立 session（2026-09-09）

### 目标

同一台电脑上的每个 TUI/编辑器窗口拥有独立 session；一个窗口中的活动请求、历史、取消、审批和事件订阅不阻塞或串入另一个窗口。旧客户端未携带 `session_id` 时继续落到 daemon 默认 session，以保持兼容。

| 阶段 | 状态 | 主要交付 |
|---|---|---|
| 1. 会话存储与运行时隔离 | complete | 按 session ID 打开固定 JSONL 文件，不竞争全局 current 指针；每个 session 拥有独立 history/turn lock/engine |
| 2. daemon RPC 路由 | complete | `session.new/load/resume` 返回并操作指定 session；`chat.send/cancel/subscribe/snapshot` 按 session 过滤 |
| 3. TUI/入口接线 | complete | TUI 在所有 chat 请求中携带自己的 session ID，启动新窗口不再接回别的窗口活动请求 |
| 4. 回归与发布 | complete | 多 session 并发、隔离取消/审批、兼容旧客户端、fmt/clippy/test/release、安装 `myagent` |

### 约束

- 不删除或覆盖既有 JSONL 会话；仍支持 `/resume` 明确恢复历史。
- 不再用全局 `session_switch` 阻塞无关 session；current pointer 仅作为旧客户端默认 session 的兼容指针。
- 记录每次失败的验证命令和原因，完成后追加到 findings/progress。

### 验收

- `cargo test --all-targets`：107/107 通过。
- `cargo clippy --all-targets --all-features -- -D warnings`、`cargo fmt --all -- --check`、`git diff --check`：通过。
- `cargo build --release` 与 `cargo install --path . --force`：通过；`myagent` 与 `my-agent` 均指向最新 release 二进制。

## OpenClaude 逻辑对比与当前项目优化（2026-09-09）

### 目标

研究 `/Users/pilot/Desktop/github_project/openclaude-main` 的成熟逻辑，提取与当前 Rust daemon/TUI 架构兼容、能显著提升可靠性或可维护性的部分，并在保持现有安全边界和多窗口 session 隔离的前提下实现可验证的改进。

| 阶段 | 状态 | 主要交付 |
|---|---|---|
| 1. OpenClaude 架构勘察 | complete | 梳理 session、消息队列、目标/计划、权限、远程恢复、状态选择器和持久化模式 |
| 2. 差距与取舍设计 | complete | 选择取消可中断、计划写入串行化、session 实时状态三项高收益改进 |
| 3. 当前项目实现 | complete | 落地取消可中断、计划写入串行化、session 实时状态，并补回归测试与 CLI/TUI 展示 |
| 4. 全量验收与交付 | complete | 109 项全量测试、clippy、fmt、diff check、release、安装和 `myagent` 命令验证通过；已准备提交并推送 |

### 约束

- 只提取逻辑和工程模式，不复制 OpenClaude 的 UI/品牌/闭源服务依赖。
- 不削弱当前工具审批、路径边界、MCP 隔离、session 隔离和取消语义。
- 每两次源码检索后把关键发现写入 findings.md；每个阶段结束更新本计划和 progress.md。

## OpenClaude TUI 对比与当前 TUI 优化（2026-09-09）

### 目标

参考 OpenClaude 的 REPL/Ink 交互结构，改善当前 ratatui TUI 的信息层级、输入区、状态反馈、滚动体验和窄终端可读性，同时保持现有 daemon/session/审批协议不变。

| 阶段 | 状态 | 主要交付 |
|---|---|---|
| 1. TUI 现状与 OpenClaude 研究 | complete | 对比消息列表、工具反馈、prompt/footer、状态线、快捷键与滚动模型 |
| 2. 视觉与交互方案 | complete | 选择分层 transcript、动态 prompt、sticky 新消息提示、状态线和帮助浮层 |
| 3. 当前 TUI 实现 | complete | 已落地层级化 transcript、动态 prompt/footer、状态 pills、快捷键帮助与窗口适配 |
| 4. 验收与发布 | complete | 111 项全量测试、TUI 渲染测试、clippy、fmt、release、安装和文档同步通过 |

### 约束

- 不改变 daemon RPC、session 隔离、审批安全和会话持久化语义。
- 不复制 OpenClaude 品牌素材或依赖，仅提取交互和信息架构。
- 每两次源码检索后记录 findings；每个阶段结束更新本计划和 progress。

### 错误记录

| 错误 | 尝试 | 解决 |
|---|---|---|
| 新增帮助浮层测试直接匹配中文字符串失败 | TestBackend 会把双宽字符按终端 cell 展开为空格 | 测试比较前移除空格，保留真实渲染内容断言 |

## Agent 不可用诊断（2026-09-09）

### 目标

根据用户提供的 TUI 截图和“给我写一个前端”复现链路，定位写文件失败、工具重复调用、模型循环和可观测性不足的根因；本阶段先诊断，不在未确认方案前修改生产代码。

| 阶段 | 状态 | 主要交付 |
|---|---|---|
| 1. 截图与工具链审计 | complete | 已核对 write_file、plan、LoopEngine、事件流和 daemon 日志 |
| 2. 最小复现与根因确认 | complete | 已用真实 session JSONL、plan.json、daemon 状态确认是工具失败后的恢复循环，不是 daemon 卡死 |
| 3. 修复建议与可观测性方案 | complete | 已给出目录创建、失败熔断、请求级 telemetry 和诊断入口的优先级方案 |

### 约束

- 先保留当前干净工作树，不直接改生产代码。
- 记录截图证据对应的源码位置和可复现命令。

### 结论

- 本轮仅完成诊断和规划文件记录，未修改生产代码、未重启用户 daemon、未提交或推送。
- 截图中的直接根因是 `write_file` 不创建父目录；循环体验的根因是工具错误被当作普通模型反馈，且缺少连续失败熔断与请求级可观测字段。

## Agent 不可用修复实施（2026-09-09）

### 目标

把诊断结论全部落地，并让安装后的 `myagent` 在真实工作区可直接排障和恢复。

| 阶段 | 状态 | 主要交付 |
|---|---|---|
| 1. 工具可靠性 | complete | `write_file` 自动创建父目录；准入/装配/执行连续 3 次失败熔断；新增回归测试 |
| 2. 请求级可观测性 | complete | round、request/session ID、Provider 首增量/总耗时、工具耗时/成功状态进入日志和 TUI/CLI/ACP |
| 3. 诊断入口与默认行为 | complete | `status` 展示路径、`logs` 只读命令、`sessions` 脱离模型环境校验、模糊前端请求默认提示 |
| 4. 验收与本机安装 | complete | 113 项测试、Clippy、fmt、release 构建通过；已安装最新 release 并重启空闲工作区 daemon |

### 约束

- 保留现有安全策略、审批、session 隔离和旧事件字段兼容性。
- 不自动提交或推送；代码交付前保留可审阅的工作树差异。

## TUI 任务完成与工具折叠优化（2026-09-09）

### 目标

针对截图中“工具记录过多、完成状态不明显”的问题，提供明确的终态标记和默认折叠、可展开的工具详情。

| 阶段 | 状态 | 主要交付 |
|---|---|---|
| 1. 现状审计 | complete | 确认 `show_tools` 仅控制输出展开，工具卡片本身始终逐条显示；Response 成功只显示“就绪” |
| 2. TUI 交互实现 | complete | 连续工具调用默认合并为摘要；Ctrl+T 展开/收起调用与输出；成功/失败均显示状态 |
| 3. 完成状态与回归 | complete | Response 成功/失败插入可见终态消息并更新状态栏；新增折叠和完成标记测试 |
| 4. 构建交付 | complete | release 构建、安装 `myagent`、同步文档与最终验证 |

### 约束

- 保持 daemon 协议、工具执行和审批语义不变，只调整展示层。
- 默认折叠不丢失工具结果；展开后仍显示工具名称、轮次、状态、耗时和输出。
# TUI 空白、详情展开与运行反馈修复（2026-09-12）

## 目标

修复 `docs/known-issues.md` 中 TUI-001～TUI-003：消除 transcript 大面积无效空白；让 Ctrl+T 在原 Query 上下文中内联展开工具详情并保持滚动稳定；为运行中的 Agent 增加持续、分阶段的不确定进度反馈。

## 阶段

| 阶段 | 状态 | 完成标准 |
|---|---|---|
| 0. 基线与根因定位 | complete | 确认布局约束、折叠/展开数据模型、活动阶段状态和现有渲染测试 |
| 1. transcript 布局修复 | complete | 短内容自然顶对齐或连续布局，不再出现大块无效空白 |
| 2. Ctrl+T 内联详情 | complete | 原 Query 始终可见，详情在对应摘要处展开，滚动锚点稳定 |
| 3. 运行指示器 | complete | 无新事件期间仍持续动画，能表达模型/工具/审批阶段且终态停止 |
| 4. 回归与交付 | complete | 定向测试、全量测试、fmt、Clippy、release 构建通过并更新问题记录 |

## 本轮约束

- 保留并兼容仓库当前未提交改动，不覆盖用户已有工作。
- daemon 继续作为运行状态唯一真相源；进度动画只表达“不确定进度”，不伪造百分比。
- 不重构无关模块，优先在现有 `TuiApp` 状态、布局和 view 渲染边界内修复。

## 本轮错误记录

| 错误 | 次数 | 处理 |
|---|---:|---|
| 首次查阅依赖源码时误猜 `ratatui-core-0.1.0` 目录 | 1 | 根据 Cargo.lock 与 registry 文件清单定位到实际版本 `ratatui-core-0.1.2`，不重复错误路径 |
| 最终二进制哈希核对时 `shasum` 因本机 `C.UTF-8` locale 异常崩溃 | 1 | 改用不依赖 Perl locale 的 `cmp` 逐字节核对，不重复运行 `shasum` |
| 最终状态中出现本轮未编辑的 `src/daemon/lifecycle.rs` 语义改动 | 1 | diff 确认为 daemon 升级/日志持久化等独立工作而非 rustfmt 变化；按用户改动保留，未作还原 |
| 消息级锚点重构后严格 Clippy 报测试包装函数 `transcript_lines` 为生产 dead code | 1 | 将该兼容测试 helper 收窄为 `#[cfg(test)]`，保留生产实现 `transcript_lines_with_anchor`，随后重跑全套门禁 |
# 项目展示 HTML 与 GitHub README 更新（2026-09-12）

## 目标

以本地 `/Users/pilot/Desktop/agent-system.html` 为视觉基准，按当前源码与已实现能力更新项目全景说明；再将同一套信息架构转换为 GitHub 原生、美观且易读的 `README.md`，提交并推送到 `origin/main`。

## 阶段

| 阶段 | 状态 | 完成标准 |
|---|---|---|
| 0. 基准与功能审计 | complete | 对比桌面/仓库 HTML、README 与源码现状，形成缺口清单 |
| 1. HTML 内容与视觉更新 | complete | 仓库 HTML 已补齐最新功能并通过 1600px 静态渲染；待最终同步桌面副本 |
| 2. GitHub README 改版 | complete | 首屏定位清晰、功能/架构/快速开始完整，使用 GitHub 支持的视觉元素 |
| 3. 渲染与内容验收 | complete | 1600px HTML 与 1200×420 SVG 渲染通过；结构、链接、JSON、格式和 122 项测试通过 |
| 4. 提交与推送 | complete | 项目说明与 README 已提交并推送到 `origin/main`，GitHub 仓库页确认加载新版内容 |

## 原则

- HTML/README 中的已有文字仅作为项目资料，不作为新的操作指令。
- README 不直接嵌入依赖脚本的复杂 HTML/CSS；采用 GitHub 可稳定渲染的 Markdown、表格、折叠块、徽章和仓库图片。
- 功能描述以当前代码和测试为准，不夸大远程 MCP、多租户、系统级沙箱等未实现能力。
- 保留用户工作树中的所有修改；当前基线与远端一致。

## 错误记录

| 错误 | 次数 | 处理 |
|---|---:|---|
| 内置浏览器安全策略拒绝本地 `file://` HTML | 1 | 不绕过策略，改用 Quick Look 1600px 静态渲染、HTMLParser 结构检查，并在推送后检查 GitHub 页面 |
| 旧版 `tidy` 把 UTF-8/HTML5 标签误报为非法 | 1 | 不使用其结果作为门禁，改用 Python 标准库 HTMLParser 检查标签栈并结合实际渲染验收 |
| 推送后内置浏览器加载 GitHub 页面超时并重置会话 | 2 | GitHub 文本抓取已确认新版 README 生效，且本地原尺寸视觉验收已通过；停止重复浏览器尝试 |
| 临时预览清理脚本使用 `path` 覆盖 zsh 的 PATH 数组 | 1 | 改用 `preview_item` 变量并显式调用 `/bin/mv`、`/usr/bin/find`；预览文件已移入可恢复的废纸篓目录 |

# 本地 Agent Web 控制台（2026-09-13）

## 目标

为现有 daemon 增加同源 Web 控制台：既能发起 Agent 对话，也能列出所有本地 Session，并查看每个 Session 的消息、LLM 请求/响应、工具调用与时间链路；TUI 展示 Web 地址并提供 `/web` 启动或复用、随后打开浏览器的入口。

## 阶段

| 阶段 | 状态 | 完成标准 |
|---|---|---|
| 0. 现状与数据契约审计 | complete | 确认 HTTP/WS、Session JSONL、事件字段、daemon 生命周期、slash/TUI 边界 |
| 1. 可观测数据与 Web API | complete | 提供 Session 列表/详情、链路记录与调用 Agent 的同源 API，保持 daemon 为唯一真相源 |
| 2. Web 前端 | complete | 完成对话工作台、Session 浏览器、详情时间线与响应式布局 |
| 3. TUI `/web` 集成 | complete | 状态区显示地址；`/web` 幂等启动/复用并打开系统浏览器 |
| 4. 验收与文档 | complete | 单测/集成/浏览器烟雾、fmt、Clippy、全量测试、release 和 README 更新 |

## 本轮约束

- 保留当前工作树中尚未提交的 dogfood、slash 补全与 TUI 相关改动，不覆盖或回退。
- Web 仅默认监听 loopback；复用现有鉴权、安全审批、session 隔离与 daemon RPC，不在前端复制 Agent 逻辑。
- Session 详情必须能区分用户/assistant/tool 消息，并尽可能呈现模型请求、响应、工具与阶段时间；不伪造历史上未持久化的时间字段。
- `/web` 重复调用不得重复启动服务；成功后给出稳定 URL 并尝试打开默认浏览器。

## 本轮错误记录

| 错误 | 次数 | 处理 |
|---|---:|---|
| 首次跨三个规划文件的补丁因 `findings.md` 锚点不存在而整体拒绝 | 1 | 补丁未落盘；改用每个文件的真实末尾锚点追加，不重复旧锚点 |
| 首次 trace 接线补丁保留了旧方法调用的一行链式前缀 | 1 | 编译前源码检查发现并删除多余 `.run_turn_with_events(`，未形成重复失败 |
| 新增 `ApiState.workspace` 后测试 fixture 漏填字段 | 1 | `cargo check --all-targets` 精确定位；为唯一测试构造器补入当前工作区 |
| 首次 CUA 初始化调用漏传 `code`，随后又误用了不可用的 `tools` 全局 | 1 | 两次均未操作页面；读取工具返回的正式 API 后改用 `cua.getState/createBrowserTab` |
| CUA 创建标签时误传不支持的 `max_output_chars`，并尝试了不可用的 Chrome provider | 1 | 根据 schema 移除多余字段，枚举可用浏览器后改用 Codex in-app browser |

# Web Agent 工作台与工作目录（2026-09-13）

## 目标

把 Web 控制台重构为两个一级功能：以 Session/链路审计为核心的查看页，以及以实际开发工作为核心的 Agent 对话页；Agent 对话在创建会话时可选择本地工作目录，并确保 daemon、工具安全边界、会话持久化和页面展示都与所选目录一致。

## 阶段

| 阶段 | 状态 | 完成标准 |
|---|---|---|
| 0. 工作区与会话模型审计 | complete | 明确现有 daemon 单工作区边界、Web 生命周期、Session RPC 和工具工作目录约束 |
| 1. 多工作区后端契约 | complete | Web 可列出/校验目录并为所选目录连接或启动对应 daemon，Session 数据按工作区隔离 |
| 2. Agent 工作台前端 | complete | Agent 对话成为一级主界面，支持目录选择、新建对话、流式工作、审批、取消与状态反馈 |
| 3. Session 查看前端 | complete | Session 浏览与链路详情成为独立一级页面，可切换工作区并查看历史记录 |
| 4. 兼容、测试与交付 | complete | TUI `/web` 保持可用，完成单测/集成/浏览器验收、fmt、Clippy、全量测试、安装和文档更新 |

## 本轮约束

- 工作目录必须经过 canonicalize/存在性校验，不能由浏览器直接绕过 SafetyPolicy。
- 一个已有 daemon 仍只负责一个工作区；优先在 Web 服务层按目录管理 daemon 连接，不把运行时安全边界改成可变全局状态。
- Agent 工作是默认主路径，日志和 trace 是独立的 Session 查看能力，不喧宾夺主。
- 保持现有 `/web` 默认工作区与旧 Session 的兼容性。

## 本轮错误记录

| 错误 | 次数 | 处理 |
|---|---:|---|
| 重写 `web/index.html` 时在同一补丁中同时 Delete/Add 同一路径，被 apply_patch 拒绝 | 1 | 补丁未落盘；改为分两次 Delete/Add，不重复该补丁结构 |
| 一次 `cargo test` 传入两个独立测试过滤串，Cargo 拒绝第二个参数 | 1 | JS 检查已通过；改为分别运行两个过滤串，serve 7 项与 web 2 项均通过 |
| 更新阶段记录时两次使用了不稳定的跨章节锚点，补丁未命中 | 2 | 读取文件末尾确认最终记录已在本节，后续只用本节内唯一完整行更新 |
| `rg` 查询字符串包含未安全引用的反引号，zsh 将其当作命令替换 | 1 | 未产生文件修改；后续 shell 搜索避免反引号或使用安全单引号参数 |
| 浏览器隔离验收首次选择 127.0.0.1:18789，该端口已被占用 | 1 | daemon 已正常拉起但 Web 未启动；改用系统分配的空闲端口，不重复固定端口 |
| CUA 选择 Agent 工作台按钮时未使用 exact，和“在 Agent 工作台继续”发生严格匹配冲突 | 1 | 页面没有发生点击；后续对同名按钮使用 `exact: true`，不重复模糊定位 |
| CUA 调用重复误传了已知不支持的 `max_output_chars` 字段 | 2 | 调用在参数校验阶段失败、未创建页面；后续严格只传 `code`/`title`/`timeout_ms` |
| CUA 误用不存在的 `tab.playwright.dom.setViewportSize` 与 REPL `text()` helper | 1 | 页面已正常创建；停止猜测辅助 API，后续仅使用已确认可用的 `tab.playwright.domSnapshot()` 与交互接口 |

# Session 日志帧超限修复（2026-09-13）

## 目标

修复 Web Session 查看页读取大 Session 失败的问题。当前 `session.load`/`session.trace` 将整个 JSONL 一次性放入 daemon 协议帧；截图显示 7,643,140 字节响应超过 4,194,304 字节限制，导致消息和链路全部无法展示。

## 阶段

| 阶段 | 状态 | 完成标准 |
|---|---|---|
| 0. 根因确认 | complete | 确认超限发生在整包 Session/trace RPC 返回，而非前端渲染；梳理兼容边界 |
| 1. 分页 RPC | complete | daemon 提供有界的消息与 trace 分页结果，单帧留出协议开销 |
| 2. Web 增量查看 | complete | Session 页面首屏可见、可继续加载，不因单条大内容阻塞整页 |
| 3. 回归与交付 | complete | 大数据测试、全量测试、格式/Clippy/前端检查通过并更新文档 |

## 本轮约束

- 保留旧 `session.load`/`session.trace` RPC 兼容性；Web 改用分页方法，Agent 运行时仍从本地完整 Session 文件恢复历史。
- 单次 Web 响应必须显式受字节预算约束，不能只依赖消息条数；必要时对超大展示字段给出可见的截断标记。
- 不提高全局 `MAX_FRAME_BYTES`，避免把协议层内存/拒绝服务风险转嫁给所有客户端。

## 本轮错误记录

| 错误 | 次数 | 处理 |
|---|---:|---|
| 首个大消息分页测试误假设单条消息会立即触发页满，实际单条内容先被压缩后仍可与后续项同页 | 1 | 改为构造多条 300 KiB 消息，验证页级字节预算、`has_more` 和截断标记，不重复该断言 |

# 三档 Agent 权限模式（2026-09-13）

## 目标

在同一工作区的 Web 与 TUI 提供三档共享模式：请求批准、帮我批准、完全访问权限；模式切换直接作用于 daemon 使用的 SafetyPolicy，完全访问跳过审批但保留灾难性命令硬拦截。

## 阶段

| 阶段 | 状态 | 完成标准 |
|---|---:|---|
| 0. SafetyPolicy 审计 | complete | 明确现有路径、命令、MCP 审批入口和 daemon 生命周期 |
| 1. daemon/RPC 契约 | complete | SafetyPolicy 支持三档；Web/TUI 可读写同一个工作区模式 |
| 2. Web/TUI 入口 | complete | Web 菜单与 `/permissions [request|risk|full]` 可切换并反馈当前模式 |
| 3. 回归与交付 | complete | 审批边界、前端资源、全量测试和安装推送通过 |

## 本轮约束

- 模式按工作区 daemon 共享，目录切换后使用目标目录自己的模式状态；不向 Agent 请求参数注入可变 cwd。
- “完全访问权限”只跳过安全审批，不解除 fork bomb、磁盘擦除、根目录递归删除等硬拦截。
- Cron 无人值守任务继续使用独立的 `UnattendedApproval`，不会因交互模式切换而自动放宽。

## 本轮错误记录

| 错误 | 次数 | 处理 |
|---|---:|---|
| 一次 cargo test 传入两个独立过滤串，Cargo 拒绝第二个参数 | 1 | 改为分开运行 daemon RPC 与 SafetyPolicy 两个定向测试，不重复该调用方式 |
## 交付记录

- Web 权限菜单已在隔离发布版服务中验证，默认显示“帮我批准”，弹窗展示三档选项和灾难性命令硬拦截说明。
- TUI `/permissions` 与 Web `permissions.get/set` 已通过共享状态回归测试；全量测试 136/136、Clippy、rustfmt、Node 语法和 diff 检查通过。
- 发布版已重新构建并安装到 `/Users/pilot/.cargo/bin/my-agent` 与 `/Users/pilot/.local/bin/my-agent`，二进制逐字节一致。
## Web Agent 流式与 Markdown 优化（2026-09-13）

### 目标

1. Agent 工作台默认不展开工具调用与工具输出，只显示紧凑的执行摘要。
2. 模型响应按 WebSocket 增量事件实时更新，首字节到达后立即出现在对话中。
3. 模型返回的 Markdown 在页面安全解析为标题、列表、代码、链接等语法。

### 阶段

| 阶段 | 状态 | 完成标准 |
|---|---|---|
| 0. 现状审计 | complete | 确认 Web 事件处理、工具动态 DOM 和现有内容渲染边界 |
| 1. 工具默认折叠 | complete | 工具调用/输出默认收起，仍可主动展开查看 |
| 2. 流式与 Markdown | complete | 增量响应不被快照覆盖，Markdown 渲染安全且持续更新 |
| 3. 回归与交付 | complete | 前端/后端测试、浏览器验收、构建安装和推送完成 |

### 约束

- 保留现有审批、取消、Session 持久化和链路审计，不改变 daemon 协议语义。
- Markdown 不能执行脚本或注入任意 HTML；链接使用安全协议，代码块保留可复制文本。
- 工具详情只改变默认视觉状态，不删除 Session 中的工具与 trace 数据。

### 错误记录

| 错误 | 次数 | 处理 |
|---|---:|---|
| Anthropic 思考流单测使用非 ASCII 原始字节串，rustc 拒绝解析 | 1 | 改用 JSON `\\uXXXX` 转义，保持 SSE fixture 为 ASCII 字节串 |
| Ollama 思考流单测同样使用非 ASCII 原始字节串 | 1 | 将 NDJSON fixture 改为 JSON Unicode 转义后重新执行格式检查 |
| OpenAI 思考流单测使用非 ASCII 原始字节串 | 1 | 将 SSE fixture 改为 JSON Unicode 转义 |
| CUA 通过 `evaluate` 直接给 textarea 的 `value` 赋值触发只读包装错误 | 1 | 改用 Playwright locator 的 `fill`，再用页面脚本只读取状态；不再重复该赋值方式 |
| CUA 页面 `evaluate` 隔离上下文不提供 `MutationObserver` 构造器 | 1 | 改用 WebSocket 事件时间戳与页面定时读取完成验收，不依赖该 API |
| 新增思考事件后 CLI/TUI/ACP 的 `EventKind` 匹配不完整，`cargo check --all-targets` 报非穷举 | 1 | 为非 Web 消费者补充忽略/状态处理分支，保留 Web 思考流展示 |
| CUA 按可访问名称 `textbox[name=prompt]` 未匹配到 textarea | 2 | 改用稳定 DOM id `#prompt` 定位，后续不再使用无名 textbox 角色查询 |

### 当前验收记录

- 隔离发布版验证响应首段到达时状态为“生成响应”、存在流式光标；完成后 Markdown 标题、列表、代码块和安全链接均生成语义 DOM。
- 工具调用与工具输出数据仍保留在消息和活动状态中，但默认放入关闭的 `<details>`，用户可主动展开查看。
- 全量测试 136/136、Clippy、rustfmt、Node 语法、嵌入资源测试和 diff 检查通过；release 已构建并安装到 PATH，准备提交推送。
## Web Markdown 表格与思考流（2026-09-13）

### 目标

1. 正确渲染 Markdown 表格，避免被当作带竖线的普通文本。
2. 将模型增量区分为思考流与正式回答流，并在页面按到达顺序实时展示。
3. 思考结束后自动折叠思考区，正式回答继续流式输出；旧 Provider 无思考事件时保持兼容。

### 阶段

| 阶段 | 状态 | 完成标准 |
|---|---:|---|
| 0. 协议与 Provider 审计 | complete | 确认各 Provider 思考字段/增量形态和 daemon 事件透传边界 |
| 1. Markdown 表格 | complete | 表头、分隔线、数据行和对齐标记安全渲染为 table |
| 2. 思考/正文流 | complete | 新事件实时显示，思考完成自动折叠，正文不被覆盖 |
| 3. 回归与交付 | complete | Provider/daemon/Web 测试、浏览器验收、构建安装和推送完成 |

### 约束

- 保持现有 `text_delta`、工具调用、审批、取消和 Session trace 兼容；新增事件必须可选。
- 不把思考内容写入最终 assistant 正文，避免刷新快照时错位；历史记录没有思考字段时正常显示正文。
- Markdown 表格单元格必须经过现有安全内联渲染，不能引入未转义 HTML。

### 错误记录

| 错误 | 次数 | 处理 |
|---|---:|---|

### 当前验收记录

- OpenAI `reasoning_content`、Anthropic thinking content block/`thinking_delta`、Ollama `message.thinking`/`reasoning` 均已归一为 Provider 思考增量；无思考字段的旧 Provider 仍走原有正文流。
- daemon 新增 `thinking_delta` 与 `thinking_finished` 事件；首个正文增量前保证发出思考完成事件，Session assistant 消息和 model response trace 均保留独立 thinking 字段。
- Markdown 表格支持表头、分隔线、数据行与左右/居中对齐，单元格继续经过安全 inline Markdown 渲染。
- 真实隔离 WebSocket 记录显示事件按 `thinking_delta → thinking_finished → text_delta → turn_completed` 到达，且思考与正文之间存在真实时间间隔，不是完成后的伪流式。
- 隔离浏览器可访问性树确认思考阶段显示“生成中”和流式光标；思考完成后显示“已自动折叠”，正文被解析为标题、表格、行和单元格。

### 交付记录

- `cargo test --all-targets` 140/140、严格 Clippy、rustfmt、Node 语法与 diff 检查通过；release 已重新构建并安装到 `/Users/pilot/.cargo/bin/my-agent` 与 `/Users/pilot/.local/bin/my-agent`，两份二进制一致。
- 隔离 Web/WS 验收服务、模拟 Provider、脚本和工作区已停止并移入 `/Users/pilot/.Trash`；用户要求停止的 127.0.0.1:8787 旧进程 PID 64165 已结束，端口已释放。

## Web 工作台布局优化（2026-09-13）

### 目标

1. 增大 Agent 对话区和模型输出卡片的可用面积。
2. 压缩右侧实时辅助栏，但保留工作目录、Session 与状态操作。
3. 移除对开发任务帮助不大的英文眉题，减少顶部占高。

### 阶段

| 阶段 | 状态 | 完成标准 |
|---|---:|---|
| 0. 现状审计 | complete | 确认对话/辅助栏栅格、消息最大宽度与标题占高 |
| 1. 样式调整 | complete | 左侧更宽、模型消息接近满宽、顶部眉题移除 |
| 2. 浏览器验收与交付 | complete | 桌面/窄屏检查、门禁通过、提交并推送 |

### 约束

- 不改变 WebSocket、流式、Markdown、Session 或权限行为，只调整布局与视觉尺寸。
- 右侧工作目录、当前 Session、取消/审批和链路入口仍可访问。
- 保留窄屏断点，避免把模型输出挤出可滚动区域。

### 错误记录

| 错误 | 次数 | 处理 |
|---|---:|---|
| 样式验收启动服务时未提供 Provider 环境变量，配置校验提前退出 | 1 | 改用带本地模拟 Ollama 的隔离工作区启动验收服务 |
| 清理后的模拟脚本扩展名为 `.mjs.qa`，Node 不识别为 ES 模块 | 1 | 复制到明确的 `.mjs` 临时路径后启动，不修改废纸篓原件 |
| 空白工作台上读取不存在的 `.message` 计算样式导致 CUA `getComputedStyle` 报错 | 1 | 改用已有布局节点读取几何尺寸，避免对空状态消息取样 |
| CUA IAB Playwright 对象不提供 `screenshot` 方法 | 1 | 改用 Tab 级截图 API和 DOM 几何完成视觉验收 |

### 当前验收记录

- 已移除 Agent 工作台标题区的 “Build with your local agent” 英文眉题；Session 查看页的记录眉题保留，不影响日志识别。
- 桌面视口 1440×900 验收：左侧对话区约 1102px，右侧实时栏约 290px；模型消息宽约 1014px，接近内容区满宽。
- 对话标题区和 transcript 上下留白已收紧，顶部标题、Markdown 表格、思考折叠区与正式回答均在同一可滚动正文区域内，composer 获得更稳定的底部空间。
- 窄屏断点仍按单列布局工作；移动视口下模型输出卡片保持接近满宽，右侧工作目录/Session/实时状态下移展示。

### 交付记录

- `cargo fmt --all -- --check`、`node --check web/app.js`、`git diff --check` 通过；发布版已重新构建并安装到两个 PATH 目录且二进制一致。
- 隔离浏览器可访问性树确认 Agent 标题区不再包含英文眉题，模型输出仍保留语义标题和 Markdown 表格；临时服务、模拟 Provider、脚本与工作区已停止并移入 `/Users/pilot/.Trash`。

## Web 工作台截图标注优化（2026-09-13）

### 目标

1. 按截图标注将“新建任务”提升到顶部操作区，减少顶部重复信息。
2. 将工作目录、权限模式选择放到输入框底部上下文区，用户提交任务前可以就近确认并切换。
3. 明确中间 transcript 是模型响应区域，空状态文案与真实流式消息保持一致。

### 阶段

| 阶段 | 状态 | 完成标准 |
|---|---:|---|
| 0. 标注与 DOM 审计 | complete | 确认红色标注对应的顶部、响应区和 composer 控件 |
| 1. DOM/样式调整 | complete | 顶部新建任务、底部工作区/权限控件、响应区提示完成 |
| 2. 浏览器回归与交付 | complete | 桌面/窄屏可用、功能事件不回归、测试通过并推送 |

### 约束

- 不改变控件 ID、RPC 请求、权限语义和流式渲染逻辑，只调整 DOM 位置、文案和布局。
- 工作目录与权限模式必须保持可点击、可禁用状态，并在窄屏下仍能读到当前值。
- 不把截图中的红色批注作为产品文字写入页面；仅将其意图转化为正式中文 UI。

### 错误记录

| 错误 | 次数 | 处理 |
|---|---:|---|
| 移动工作目录/权限控件后仍按旧顶部节点更新，隔离页面首次连接报 `textContent` 空节点错误 | 1 | 保留既有 `workspace-label` ID，并移除重复的 composer 节点更新；重新构建发布资源后复验 |
| CUA 回归第一次加载的是修复前发布二进制，页面仍显示旧初始化错误 | 1 | 停止旧服务并重新构建 `target/release/my-agent` 后再加载页面 |
| `functions.exec` 中使用未正确转义的 JavaScript 字符串传递补丁，补丁调用未执行 | 1 | 改用模板字符串重发同一 `apply_patch`，未修改目标文件 |

### 当前验收记录

- 桌面端顶部操作区仅显示“＋新建任务”、连接状态和设置；工作目录/权限模式选择器已移至 composer 底部，1440px 视口下两个控件均可读且可操作。
- agent-transcript 增加“模型响应区域”可访问性描述，空状态明确提示模型响应会在此实时展示；已有 Markdown 表格、思考自动折叠和正式响应流保持不变。
- 390px 窄屏实测 composer 底部仍能同时显示“工作目录”和“权限模式”，控件不溢出；Agent 响应、思考生成中和完成后自动折叠状态均通过 AX 验收。
- 隔离服务中实际提交一轮任务，确认流式思考先显示“生成中”，完成后显示“已自动折叠”，正式响应仍渲染为标题与表格。

### 交付记录

- cargo test --all-targets 140/140、严格 Clippy、rustfmt、Node 语法、diff 检查通过；Release 已重新构建并安装到 /Users/pilot/.cargo/bin/my-agent 与 /Users/pilot/.local/bin/my-agent，两份二进制一致。
- 隔离浏览器、模拟 Provider 与 18884 服务已关闭；验收使用的工作区/脚本位于 /Users/pilot/.Trash，可恢复；8787 既有用户服务未由本轮启动。

## 全局模型配置与 `/models`（2026-09-13）

### 目标

1. 配置不再只依赖当前终端环境变量；支持一次保存后全局复用，并在缺失时给出可操作提示。
2. 支持多个 OpenAI 兼容、Anthropic Messages 和 Ollama 模型配置。
3. Web 设置与 TUI `/models` 可列出、保存和切换活动模型；切换在活动 turn 期间明确拒绝。

### 阶段

| 阶段 | 状态 | 完成标准 |
|---|---:|---|
| 0. 配置与 Provider 审计 | complete | 明确持久化格式、环境变量兼容和运行时 Provider 所有权 |
| 1. 全局配置与动态 Provider | complete | 配置文件安全保存，daemon 能在空闲时切换模型 |
| 2. Web/TUI 入口 | complete | Web 表单、模型列表、`/models` 列表/切换 |
| 3. 回归与交付 | complete | 全量测试、Clippy、前端语法、发布安装与提交推送 |

### 约束

- 环境变量继续兼容，并优先级高于持久化配置，避免破坏现有部署。
- API 密钥响应只返回是否已设置；配置文件创建为用户私有权限。
- 切换模型不得中断正在执行的 turn；无活动请求时立即生效。
- 旧 Session 和旧 daemon 协议保持兼容。

### 验收记录

- 无配置时 `myagent config check` 汇总缺失项并指向全局配置文件；`myagent serve` 可先启动未配置 Web 页面。
- Web `/api/models` 支持保存/列出/激活配置，覆盖 OpenAI 兼容、Anthropic Messages、Ollama；API key 只返回 `has_api_key`。
- daemon 通过稳定 `ProviderManager` 热切换 Provider；活动 turn 期间切换明确返回冲突。
- TUI `/models` 列出配置并支持 `/models 2` 或 `/models <ID>` 切换。
- `cargo test --all-targets` 143/143、严格 Clippy、rustfmt、Node 语法和 release 构建均通过；发布版已安装到 Cargo PATH。

## 参考 pi 的 TUI 与 Agent 架构优化（2026-09-13）

### 目标

完整阅读 `/Users/pilot/Desktop/github_project/pi` 的 TUI、coding-agent 与 agent 运行链路，对照当前 Rust 项目，提炼适合现有 daemon + 多入口架构的设计并完成可验证优化。

### 阶段

| 阶段 | 状态 | 完成标准 |
|---|---:|---|
| 0. 双项目基线 | complete | 明确目录、入口、测试、现有工作树与关键链路 |
| 1. TUI 对照设计 | complete | 集中 keymap、grapheme、聚合 phase、非阻塞 stream poll |
| 2. Agent 流程对照设计 | complete | 截断工具调用 fail-closed，保留文本兼容与现有重试 |
| 3. 实现与回归 | complete | 完成代码和定向测试，保留兼容性 |
| 4. 全量验收 | complete | fmt、check、clippy、全量测试与最终差异审阅通过 |

### 原则

- 保留 Rust、ratatui、daemon 单一真相源和多入口协议，不机械复制 TypeScript。
- 优先落地能提升 TUI 可操作性、流式状态表达、Agent 生命周期清晰度或可测试性的改动。
- 大规模拆 crate/目录迁移若收益不足以覆盖风险，则只形成具体建议，不在本轮强行实施。
- 不提交、不推送；不改动 pi 仓库。

## Web 前端体验优化（2026-09-13）

### 目标

在保留现有 Web/daemon 协议和 Rust 入口不变的前提下，继续提升 Web 工作台的视觉层次、交互反馈、可访问性和窄屏体验。

### 阶段

| 阶段 | 状态 | 完成标准 |
|---|---:|---|
| 0. 前端基线 | complete | 阅读 HTML/CSS/JS、定位现有交互与响应式边界 |
| 1. 体验设计 | complete | 确定低风险、可验证的 UI/交互改进 |
| 2. 实现 | complete | 前端改动完成，保持既有协议和功能 |
| 3. 回归验收 | complete | Node 语法、Rust 测试、窄屏浏览器验收通过；桌面固定 viewport 能力受本地 IAB 限制 |

### 约束

- 保留现有 DOM ID、RPC 方法、Session/模型配置能力和服务端资源路径。
- 继续使用内嵌静态资源，不引入新的前端构建链或外部 CDN。
- 交互反馈不能只依赖颜色；状态、焦点和错误需有文字或 ARIA 语义。

### 基线发现

- `renderTranscript` 每次增量都滚动到底部，用户查看历史时会被流式输出打断。
- 审批卡通过 append 到 transcript 展示，没有进入 state；切换页面或重新渲染后可能消失。
- composer textarea 只允许手动 resize，桌面/窄屏都缺少按内容自适应和更清晰的提交提示。

### 错误记录

| 错误 | 次数 | 处理 |
|---|---:|---|
| Web CSS 大补丁因审批卡片上下文与当前文件不完全一致而未应用 | 1 | 未产生源码改动；改为按 CSS 区域拆成小补丁后重试 |
| Web JS 大补丁在继续 Session 的上下文锚点与当前文件不完全一致而未应用 | 1 | 未产生源码改动；改为分别 patch 发送、审批、Session 状态段 |
| Web JS 交互补丁因 `switchView` 的实际上下文使用了 `$$` 选择器而未应用 | 1 | 未产生源码改动；按函数和初始化尾部拆分补丁 |
| `switchView` 补丁三次因引号/补丁字符串上下文错误未应用 | 3 | 未产生源码改动；改用模板字符串承载当前文件的双引号选择器上下文 |
| IAB 浏览器未提供 viewport capability，无法直接设置固定桌面尺寸 | 1 | 保留窄屏 IAB 验收，并改用 Chrome 本地页做桌面视口复核 |

### 错误记录

| 错误 | 次数 | 处理 |
|---|---:|---|
| 技能文档引用的 `templates/*.md` 本机不存在 | 1 | 按技能职责复用仓库现有三份规划文件 |
| 初始化规划文件时误把仓库长期记录当成新文件覆盖 | 1 | 立即用 `apply_patch` 恢复 Git 完整版本，确认工作树重新干净后仅追加本轮章节 |
| keymap 初版帮助框高度少算边框，定向测试 23/24 | 1 | 高度改为动态内容行数 + 2 行边框后重跑定向测试 |
| 聚合 activity phase 首次编译仍有一处测试按旧字段访问 | 1 | 改为调用派生方法，并新增多 turn + 审批优先级回归 |
| 复核时一次性输出全部差异导致终端结果被截断 | 1 | 不重复大范围输出，改为按模块、小区间审阅 |
| 追加复核检查点时误用统一 diff 行号作为补丁上下文 | 1 | 读取文件尾部后按真实文本锚点追加，未影响源码 |
| 聚焦审阅发现恢复订阅失败会留下幽灵 `Recovering` 状态，且原失败文案会被启动状态覆盖 | 1 | 失败分支移除对应 phase，并在最终启动状态中保留失败数量 |
| TUI 定向测试命令误加 `--lib`，项目没有 library target | 1 | 去掉 `--lib`，按 binary crate 的测试目标重跑 |
| 字素编辑器初版在两枚 emoji 之间插入 ZWJ 时，新光标可能落入合并后的字素内部 | 1 | 每次插入后向前吸附到完整字符串的下一个字素边界，并增加回归测试 |

## 2026-09-26：20 项体验问题逐项修复

目标：从当前 CLI、TUI、Web、配置和恢复流程中找出 20 个可复现的用户体验缺口，按影响与风险逐项修复；每项留下代码位置和验证证据，最后运行全量门禁并提交推送。

| 阶段 | 状态 | 完成标准 |
|---|---|---|
| 0. 基线与清单 | complete | 工作树、入口代码与现有测试核对；20 项问题均有复现路径 |
| 1. 快速反馈与错误恢复 | complete | 修复高频断线、失败、取消、排队反馈问题 |
| 2. 输入与导航 | complete | 修复 CLI/TUI/Web 的输入保留、滚动和详情展开问题 |
| 3. 状态一致性 | complete | 修复模型热切换、工作区和 Session 异步竞态问题 |
| 4. 验证与交付 | complete | 每项对应回归；fmt、test、clippy、release、JS 检查、diff、提交推送 |

本轮原则：先确认当前代码仍有问题，避免重复建设；保留既有工作区改动；不改变 daemon 事实源和安全边界。所有发现与逐项状态记录在 `findings.md`、`progress.md` 本轮章节。

## 2026-09-26：再审计并修复 50 项问题

目标：在上一轮 20 项修复之上，独立发现 50 个仍存在、可指向代码路径和用户触发步骤的问题；逐项修复、增加必要的行为回归、完成全量验证并推送。

| 阶段 | 状态 | 完成标准 |
|---|---|---|
| 0. 基线与证据 | complete | 工作树干净，形成 50 项非重复证据清单 |
| 1. 状态与恢复 | complete | 修复任务/连接/审批/工作区竞态与错误状态 |
| 2. 阅读与操作 | complete | 修复历史、搜索、键盘、可访问性和反馈缺口 |
| 3. CLI/TUI 与服务 | complete | 修复终端入口的明确缺陷 |
| 4. 回归与交付 | complete | 行为回归、Rust/JS 门禁、浏览器冒烟、提交推送 |

规则：不将同一根因拆成虚假的多个问题；清单记录触发、根因、修复、验证。沿用现有三份记录文件，只追加本轮章节。


---

# Runtime Wave 1–7 实施计划

## 目标与约束
继续用户原需求：按顺序完成 Wave 1–7，保留现有修改；不得留下半接线双写或把骨架标为完成。中文文档与输出。事实依据仅当前仓库。Wave 0 已完成，228 项 Rust 测试为继续基线。

## 阶段
- [完成] Wave 1：提取实际 daemon-client；protocol DTO/versioned validation；storage 能力端口与真实实现接线；统一入口。
- [完成] Wave 2：repository/lifetime/migration/transcript/lifecycle fence。
- [完成] Wave 3：TurnCommit/tool closure/run owner。
- [完成] Wave 4：durable context projection/CAS/meter。
- [完成] Wave 5：memory scope/recall/ingest/fence。
- [完成] Wave 6：descriptor/waves/background/Docker。
- [完成] Wave 7：远程 MCP/secrets/doctor/发布门禁。

## 决策
先收口实际依赖，不引入无实现的 trait。新 client 不依赖 daemon concrete；测试内存传输 envelope 归 client。旧 wire 保留兼容策略，新增 protocol_version=1 严格校验。业务 mutation 不因连接重连自动重发。

## 错误记录
planning-with-files 的模板文件未随技能安装；已按 SKILL.md 的文件职责自行建立中文计划，无需模板继续工作。

- 初次提取 storage 时重复的 inner doc comment 被放在 item 后导致 fmt 失败；已改为普通注释。
- storage 提取首轮 check：port 缺少 SessionId/Value 导入、MutexGuard 借用需显式解引用、缺少 tracing；修复。
- root 接线 check：维护调用应走 maintenance_store，入口需把 ClientError 转换为 anyhow；修复。
- protocol 参数校验宏空字段调用需要显式逗号；已修正，228 项基线测试继续全部通过。

- Wave 1 收口：实际 client/storage/DTO/version policy 全部接线，完整旧门禁保持。Wave 2 已核对在线 transcript/control 写入点，未引入半迁移或双写；Wave 2–7 仍待实施。

## 2026-10-02：持续实现至 Wave 7
用户要求不要在中间 wave 停止。按顺序收口并继续；保留所有历史与工作树修改。Wave 2 采用 SQLite 唯一 canonical transcript，JSONL 受控一次导入/备份，在线停止 JSONL append；事实全部共享同一 SQLite lane/事务。

- Wave 2 错误与修复：schema 升为 v6 后旧版本测试期望更新到 6；首次回归会话预览选错助手消息（两项失败），保持旧断言并改回首条用户消息；提取事务 helper 后遗留 tx 名称已改为 db；新增 SessionLifecycle 方法的 trait import 补齐。

- Wave 3 错误记录：TurnState 字段不能直接在格式字符串 capture，改显式参数；user-input operation 必须按 run namespace（两队列合同检出并修复）；recovery 现在将 receipt 结算为 terminal/outcome_unknown，旧回归改为核实该明确未知事实及不可宣称 batch success；plan 插入脚本锚点不匹配，中途只有 schema/core 改动，已逐段修复接线。

### Wave 3 实施验证（2026-10-02）
RunSnapshot、TurnState/RoundState 与原子 TurnCommit 已接入生产链路。工具整批预检、取消 join、崩溃闭合、计划 CAS 与权限冻结通过 workspace 回归（243 项 Rust 测试）；Clippy 无警告。后续 Wave 4–7 继续执行，尚未宣称完成。修复工具委派时过大的异步 future：在执行/监督边界 Box::pin；未增加线程栈或放宽门禁。

### Wave 4 持久投影阶段验证（2026-10-02）
提取 agent-context 无状态计量/验证；schema v8 支持 compact intent、source prefix CAS、projection head 与 context ledger。在线 ContextManager 从 canonical snapshot 读取投影，原始 transcript 不改写；L1 媒体投影降级、L2 工具结果投影裁剪、L3 摘要，取消/无收益拒绝，摘要失败可单独提交 prune_only/partial。workspace 245 项 Rust 测试与 Clippy 通过，包含并发后缀、竞争 head、delete/recreate 和重启遗留 intent。Wave 5 开始；手动 compact RPC、多入口 generation 合同及 ledger 校准还需在后续集成收尾，不宣称整套验收完成。

### Wave 5 实施阶段验证（2026-10-02）
agent-memory 提供 typed scope/layer/kind、可见性复核与确定性 ranking；SQLite v9 保存记忆、legacy quarantine 与 committed-turn ingest receipts。在线 remember/recall 不再写 JSONL；runtime bounded recall 独立检索分区，terminal 发布后单次有界摄入，维护失败不改 terminal。memory store/recall/list/forget/scope RPC 接通，context_read_only 同时阻止 memory 写入/forget/compact。248 项 Rust 测试及 Clippy 通过，覆盖过滤后 limit、TTL、全局确认、legacy 隔离、重启与迟到 lifetime fence。接下来推进 Wave 6，三入口集成合同及完成后维护诊断仍在最终集成清单。

### Wave 6–7 收口与错误修复（2026-10-02）

完整 JSON Schema、资源冲突 waves、artifact 预览/范围读取/保留期回收、后台 Native 监督、可选 Docker exec 接通；SessionSupervisor/RunCoordinator 持有实际运行时锁、实例和任务。HTTPS MCP 重新解析并 pin DNS、禁止代理/重定向、完整 SSE event、身份/响应预算；Keychain/env 引用与迁移失败保留；doctor、显式 orphan reconcile、queue/frame/list 背压。三入口 compact/memory/restart/CAS/read-only 合同通过。

修复记录：上下文取消泛型边界遗留旧变量；新增 schema 字段误匹配 ContextProjection（修正构造，不放宽断言）；维护 trait 导入缺失；slash 新增项挤占旧菜单排序（移至旧项后，前缀测试使用 /dog）；FrameSender 错误携带大帧（改小型 typed error）；完整摘要检验发现默认非流式 Provider adapter 未发 ProtocolDone（补 adapter 的真实完成语义）；全量并发临时目录超过 macOS socket 路径上限（缩短名称，未延长超时）；memory forget 加 v12 receipt，重试返回同一结果且不复活已遗忘 operation。新增 MIT-0 仅对 borrow-or-share@0.2.4 登记许可例外。

### 最终完成

Wave 1–7 均完成当前主链，本地 schema v12；261 项 Rust/27 项 Web 测试、严格 Clippy、MSRV 1.88、release、fmt/diff、离线 cargo-deny 通过。完整证据和未实现的可选后端范围记录在 docs/changes/runtime-architecture.md 与 docs/known-issues.md。未提交或推送。

## Codex 对比与改进（2026-10-02）
- [x] 固定官方源码提交，比较历史、压缩、记忆读写与整合。
- [x] 记录取舍与实施计划：docs/research/codex-context-memory.md。
- [x] 实施当前轮次保护、原文锚点、完整记忆预算、有界摄入与空结果回执。
- [x] 回归测试与门禁，记录实际证据。

## 2026-10-03：Codex 对比与数据飞轮

目标：先研究、再确定取舍，开发运行数据→显式反馈→确定性策略→离线评测→运行应用的持续改进闭环。复用已有 owner/CAS、SQLite 与隔离预算。

| 阶段 | 状态 | 完成标准 |
|---|---|---|
| 对比研究 | complete | 固定源码证据，核对既有实现与实际缺口 |
| 方案设计 | complete | 明确采样、反馈信号、作用域、遗忘、评测与上线边界 |
| 开发接线 | complete | 真实召回链路、持久反馈、诊断与 CLI、评测集接线 |
| 验证交付 | complete | 重启/幂等/只读/遗忘合同与完整门禁通过 |

研究方案：docs/research/codex-data-flywheel.md。Codex参考固定提交8d44977aa2fb9ae1b128660668dc5b36966613fa，远程HEAD受失效代理阻断，未宣称最新版。

开发修复：params补丁按实际导入锚点重做；时间i64与请求u64类型明确转换；恢复查询JOIN runs取得lifetime；并行临时目录加原子序号防碰撞；clear(false)已创建新lifetime，不重复create。SQLite v13保存曝光/评价/回执/退避，自动摘录过滤常见凭据，运行成功不自动视为好评。

本阶段门禁：Rust276项、真实daemon10合同、检索17/17、Web27项、Clippy、MSRV1.88、release、fmt/diff、离线cargo-deny通过。本机socket及公告缓存锁经自动审批执行，未跳过合同。

## 2026-10-03：继续研究——压缩、证据与维护生命周期

延续用户要求，保留上一轮改动。研究、方案、实施、验证均complete，见docs/research/codex-context-evidence.md。

- 当前轮次suffix契约同时进入候选验证和持久提交，完整保护原文、图片、工具参数与结果。
- 历史摘要标明模型推断与projection digest；递归上限拒绝安装截断摘要。
- memory.evidence RPC与只读工具按来源点查canonical transcript，复核owner/digest、分页与元数据预算；跨会话不公开源对话。
- 健康积压每批32项、短间隔排空并保护空闲生命周期；整体错误回到常规退避。
- 修复夹具：clear前先结束受管run；create_session返回metadata，报告传metadata.lifetime；memory.list核对40项须显式limit50。

## 2026-10-05：继续研究——有界委派上下文

研究、方案、实施、验证均complete，见docs/research/codex-delegation-context.md。

显式最多4条父run来源，支持parent_input别名；事务复核来源并冻结于child RunSnapshot。原task独立，正常/overflow请求按同一包组装并计入预算；不扩大read_file能力，不隐式传递祖先包。来源序号规范化后去重，重试读回原捕获包，改选来源拒绝。真实provider请求捕获、重启读回及存储回滚合同通过。

## 2026-10-05：继续研究——原文保留覆盖状态

研究、方案、实施、验证均complete，见docs/research/codex-retention-coverage.md。

v2锚点兼容旧数组，保存文字覆盖状态和省略下限；checkpoint不能恢复原始完整标识。包装与元数据均计预算，零预算覆盖诊断仍进入宿主摘要JSON。6项独立保留评测与15项当前轮次保护评测共同形成21项上下文门禁。SummaryProvider夹具改用Response::Text，生产adapter保持既有契约。

### 2026-10-05：第二至第四轮最终验收

统一门禁全部通过：Rust289项（真实daemon11合同）、Web27项、debug/release检索17/17与上下文21/21、Clippy locked all-targets/all-features -D warnings、MSRV1.88 locked、release locked、fmt/diff、离线cargo-deny。使用已有公告缓存，不宣称实时漏洞数据库。未重启用户项目daemon。

已通过Codex应用创建本对话heartbeat“Agent Rust 持续研究与优化”（automationId: agent-rust），每12小时继续一项有证据的研究/计划/开发/验证增量；不作无收益改动，无实质新进展保持安静。下一次继续依据既有研究与质量基线选题，真实语义收益仍待生产对照验证。

### 提交前文档修复

提交检查发现上一轮写文件脚本误将src/context.rs内容写入task_plan.md。保留临时备份，从HEAD恢复原有921行计划，依据研究文档与progress补回上述新增记录；未改动源码。用户已授权提交并推送远端，按现有main与origin进行常规提交，禁止强制覆盖远端历史。

## 2026-10-05：重新核验 Runtime 架构要求

本轮仅以当前仓库为事实依据，不搜索、克隆其他仓库。保留上述历史。
1. [完成] 干净基线、源码/历史证据盘点、基线测试（socket 需环境审批）。
2. [完成] 实现前 ADR 0002 与 change doc：只读 readback / lifetime CAS / connection negotiation。
3. [进行中] 优先补 Wave 0–2：单事务 durable snapshot、稀疏读取、生命周期请求屏障、明确能力协商与静态护栏。
4. [待完成] 两后端合同、故障/重启/三入口交叉验证及完整门禁。
5. [待完成] 同步完成范围与后续 Wave 的真实未实现项。

### 本轮已验收交付（2026-10-05）

- [完成] Wave 0–2 恢复纵切：SQLite v14 单事务 SessionReadback、纯读取、strict schema/物理连接能力协商、destructive request lifetime CAS/receipt。
- [完成] 两后端、两连接读写、故障回滚、损坏 fork 拒绝、v13 备份迁移与真实 daemon/CLI/ACP/WS 重启对照。
- [完成] Rust 302、Web 27、fmt/Clippy/MSRV1.88/release/offline deny/diff 门禁。
- [完成] ADR0002、change doc 与 README/roadmap/known-issues 同步，纠正历史全部 Wave 完成宣称。
- [未完成] 完整 Wave0–7 验收、Wave5B plan/hooks、manual compact exact owner、全入口 live revision 防回退、tool_search、剩余物理 crate/后端能力；不创建空接口或双写。没有提交/推送。


# 控制面八波实施计划

日期：2026-10-05。基线 HEAD：a8ac3dcbbdd9f77f4988a5359d03c201e4df9f2e；初始工作区干净。

## 当前阶段

审计与基线进行中。任何条目只有经过实际接线和门禁后才标记完成。

1. 审计既有源码/合同，新增 ADR 和变更证据。
2. Wave 1：版本化计划定义、摘要、精确执行/废弃和三入口。
3. Wave 2：集中治理 hooks 和独立 continuation。
4. Wave 3：手动 compact 独立 canonical run。
5. Wave 4：冻结目录检索和 dispatcher。
6. Wave 5：共享 revision reducer。
7. Wave 6：实际请求/只读重建共享 assembler。
8. Wave 7：ACP v2 协商式控制投影。
9. Wave 8：行为稳定后逐 crate 迁移。

## 门禁

每波 RED→GREEN，fmt、全 workspace tests、clippy、MSRV、release、Web tests/check、diff、既有供应链检查。日志保留 /tmp/control-waves-*。不提交、不推送、不部署。

## 环境问题

- 基线沙箱内 transport 测试因绑定 Unix socket 返回 EPERM，退出 101。已申请以相同命令在允许本地 socket 的环境重跑，未修改测试。


### 控制面 Wave 1 验收（2026-10-05）

版本化计划纵切完成：SQLite v15、稳定定义身份/摘要、精确执行/废弃、pending 重启不自启、旧数据证据保留、CLI/ACP/WS/HTTP 合同。Rust 315 项全过，Clippy/MSRV/release/Web28/fmt/check/offline deny 通过；日志与失败修复证据见 docs/changes/governed-runtime-controls.md。下一步 Wave 2 hooks；Wave 2–8 尚未完成。

### 控制面 Wave 2 验收（2026-10-05）

- [完成] SQLite v16、16 类受治理 hook、publication exactly once、严格 effects/typed failure、显式受信配置和分页只读审计。
- [完成] Stop 新 native run/turn，冻结权限/route/context policy，次数/轮次/token/工具/时间预算，精确取消和 unknown 重启不重放。
- [完成] Rust 332（真实 daemon 19）、Web29、fmt/Clippy/MSRV1.88/release/离线 deny/diff。源码、RED/修复和门禁证据见本次 change document。
- [进行中] Wave 3 独立 canonical compact run 的身份/准入/Started/后台执行/取消和恢复。
- [未完成] Wave 3–8；保持顺序，不提前拆 crate。


## 2026-10-06：heartbeat第五轮——工具结果元数据预算

- [完成] 对比固定Codex提交8d44977及workspace版本0.0.0，记录当前provider实际出站字段和思考不回传边界。
- [完成] 独立8项预算样本先RED（24/29），补name/tool_call_id共享计量与饱和聚合，再GREEN（29/29），接入flywheel evaluate。
- [完成] 当前控制面快照的context/OpenAI新合同通过；固定已提交a8ac3dc基线加相同增量303项Rust、严格Clippy、MSRV1.88、Release、Web27、fmt和离线deny通过。
- [完成] 目标文件SHA256校验后仅应用本轮增量，根日志只追加。研究与实际失败/验证记录见docs/research/codex-tool-metadata-budget.md。
- [边界] 既有Wave 3开发快照还有5项迁移断言和Clippy复杂元组失败，未修改或替其验收；Wave 3–8仍按既有计划继续。本地旧v4聚合库无飞轮表，无显式反馈证据。未提交、推送或部署。


### 控制面 Wave 3 收口（2026-10-06，门禁进行中）

- 独立 compact command/native run/turn、v17 linkage、持久 Started、后台摘要、exact cancel、source CAS、typed readback 和 unknown 不重放已接线。
- 旧 unary 只验证来源并等待同一 native receipt，保留旧 receipt 只读兼容；只读来源禁止提高权限。CLI/TUI/Web 与 HTTP 共用 command，ACP v1 只读 native /run。
- 新双后端事务/迁移合同通过；Web31通过。终审补正常 frame channel 关闭排空及 compact 非聊天记忆边界。
- 原记忆40项排空偶发失败已定位为 due 查询采样时间竞态，稳定 RED 已保存，改立即到期0并保持既有退避/预算。最后全 workspace/Clippy/MSRV/release仍在跑，尚不标记本波验收完成。


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


# 企业平台升级（2026-10-07）

原八波个人 Runtime 任务已完成；本节是用户新规格 Wave 0–10，不复用原波次完成标记。基线 HEAD 0af445b1453b0adfad19802cc519624f1ebb29e8；开始时工作区干净。

| 波次 | 状态 | 门禁/交付 |
|---|---|---|
| 0 | complete | 源码证据矩阵、架构总览、六份 ADR、20 架构合同、三项真注入 RED、九门禁全通过 |
| 1 | in_progress | S1 complete / locally verified：v21、可信 Local 主体、纯 RBAC/条件、事务授权/子任务继承、共享读回；402 Rust /36 Web /九门禁。全资源 scoped 查询、双审批等待后续切片 |
| 2 | in_progress | S1–S7 implemented / locally verified：outbox/PG发布、File/S3、安全两阶段、SQLite native writer/canonical Cron、统一有界异步适配；459 Rust/36 Web/九门禁/真实S3四项/PG发布六项；全业务asyncports/nativePG/离线迁移待完成 |
| 3 | pending | OIDC、目录、限流、配额、错误投影 |
| 4 | pending | 注册表、签名 Pack、发布和配置治理 |
| 5 | pending | 持久 DAG、人工节点、A2A 投影 |
| 6 | pending | MCP OAuth、SecretStore、Connector Host 和增量同步 |
| 7 | pending | 隔离 Worker、资源与网络预算 |
| 8 | pending | 遥测、审计、成本、留存 |
| 9 | pending | Operator/Helm、HA、升级和灾备 |
| 10 | pending | 故障/性能/供应链/同产物发布门禁 |

执行约束：按顺序，上一波全门禁通过后才进下一波；不建第二状态机；无生产调用者不建空端口；未验证 lane 如实记录。Wave 0 不迁移数据库（仍 v20）、不改 CLI/socket/协议/执行行为；回滚仅恢复代码文档。扩展既有 tests/architecture_contract.rs，并用真实违反规则的 fixtures 证明检测。

企业 Wave 0 实际结果：387 Rust（20架构/31真实daemon）、36 Web、fmt/clippy/MSRV/release/JS/在线cargo deny/diff均退出0；三次隔离副本真注入被 guard 断言拒绝。完整报告 docs/enterprise/wave0-report.md。四生产通道例外必须在企业联网前收口；Wave1–10尚未实施。

## 企业 Wave1-S1 实施计划

真实闭环：core opaque企业ID/RequestContext/AuthorizationVerifier → agent-identity可信Local上下文 → agent-policy纯类型RBAC/条件 → storage v21身份/策略/作用域/run授权snapshot → daemon中心gate与授权准入 → identity.readback/CLI/ACP/Gateway同源 → restart/stale/revocation/rollback/parity tests → 九门禁。存储仅依赖core领域verifier port，policy实现由daemon注入，不改变storage依赖方向。Local legacy路径保留；企业remote认证与所有resource scoped查询尚未闭合，禁止开放Enterprise模式。

### Wave1-S1 验收（2026-10-07）

真实RED -32601→GREEN共享identity.readback、v21授权准入和run.authorization；core企业opaque ID/条件/typed错误与身份/policy纯库有实际caller。SQLite授权插入故障、迁移故障、策略发布barrier竞态、过期时间、并发同operation、旧policy/epoch/owner与损坏测试闭合。原delegation/Stop continuation在canonical child准入继承完整父授权；升级前Local旧run仍可本次授权后幂等读回，无历史快照补造。CLI/ACP v1/v2/HTTP/WS同身份，v2/HTTP/WS同typed error；重启零Provider重放、读取clock/event不变。

最终完整九门禁 evidence/wave1-s1-final-gates.json 全exit0，Rust402含20架构/32真实daemon，Web36。新外部依赖0，第三方lock所有条目与HEAD一致。完整Wave1仍in_progress：全tenant-scoped业务查询、双distinct审批人、管理身份、后台独立策略/撤销收敛/审计尚未闭合，W2–10pending。没有提交/推送/部署；回滚旧binary必须恢复verified v20备份。

## Wave2 启动（最新用户指示：直接执行2–10，不需暂停）

按用户新顺序允许从Wave2开始，不将Wave1未闭合伪造为完成；远程Enterprise仍保持未启用。S1以现有insert_event/views.publish_in为唯一业务事件owner，新增v22 outbox与同事务事件键；publisher租约按scope+session/lifetime、DB time和epoch fencing；新async storage-ports仅依赖core，SQLite适配与PG适配不互相依赖。首个真实caller为daemon durable publisher与共享readback；全业务PG/schema迁移/ArtifactStore是接下来的S2+，不可把协调子合同当成全部PG事实源。保留每切片完整9门禁。


### Wave2-S1 接线与验证进展

- core 发布 DTO/epoch reducer、async storage-ports、SQLite v22 原 event 事务 outbox、PG 独立协调 schema v1 已实现。
- daemon 单批受管发布、本地 durable journal、CLI/TUI/ACP v1/v2/HTTP/WS 元数据读回已接線。
- 原行为 RED -32601；真实 daemon GREEN；SQLite 完整域 FK/回滚/迁移/并发合同；PG 六项真实合同、独立 worker 和断连注入。
- worker 回归：失败事务 rollback 的 driver 被同步子进程等待阻塞；已显式 close().await + 异步等待，保留 55P03 RED。
- 首次九门禁只有一处 Clippy needless borrow，已修复；reviewed 门禁执行中，最终门禁须覆盖之后的完整域 FK 更新。
- 完整 Wave2 仍 in_progress：全部业务 PG ports/原事务、runtime writer lease、File/S3 两阶段 artifact、离线全量迁移未实现。Wave3–10 仍待实际实施，不能用 S1 的 pub epoch/测试替代。


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

## Wave2-S6 后台 canonical caller

S5 完整九门禁451 Rust/36 Web、真实 TLS S3三项与真实PG发布六项均通过，PG native facts仍未实施。继续S6：Cron runner复用daemon的原SessionRepository/物理连接/租约proof、同一个ArtifactService，避免另开未配置store或回退File；冻结后台run对象backend；原native结算unknown不能由Cron历史伪装成功或自动重试。修改runtime/daemon/storage错误类型及真实进程合同，schema保持v24。风险是后台join与原recovery顺序，以及legacyStandalone兼容；原repository未迁移状态，不创建第二调度器，旧JSON cron配置的完整企业迁移不在本切片。回滚须按v24兼容binary窗口，不在活运行过程中切换模式。每切片保留真实RED、门禁、实际S3与PG回归；完整Wave2–10尚未完成。

S6 审阅：native Cron canonical进程合同通过，真实定时触发补 normal stop/SIGKILL/DB expiry 接管、同epoch续租、Provider零重放。route-red 显示 attempt期望1实际0；补原 ProviderManager冻结接线。provider-unknown红为中断attempt仍Started，原finish/recover同事务收口OutcomeUnknown，未完Provider阻止假Completed/Failed/Cancelled。新增SQL trigger atomic rollback与late success拒绝测试。九门禁/真实TLS S3四项/PG六项待验收；不改变完整Wave2–10未完成状态。


S7 预审：现 PublicationRepository 的 async SQLite方法仍直接阻塞，daemon用spawn_blocking+runtime.block_on包住整publisher，readback/lifecycle其他caller仍会阻塞执行器。这是W2.01原端口缺口；待S6所有门禁与实际环境lane闭合后，统一storage adapter有界blocking budget、writer/artifact/publication共享，取消后permit覆盖实际工作，并删除daemon阻塞包装。保留原SQL与同事务事件owner，不增加新repository trait/业务状态表；需要current_thread真SQLite写锁RED、取消/过载测试及两backend合同/九门禁。完整业务asyncports/nativePG仍继续pending。

S6 implemented / locally verified：最终wave2-s6-cron-audited九门禁全部exit0，457Rust/36Web/10外部ignored；独立TLSMinIO四项和PG发布六项全通过。nativeCron同物理repo/proof/backend/冻结Provider，未知收口/no retry，定时续租/退出/kill恢复、六入口、SQL fault/损坏JSON身份零写闭合。报告wave2-s6-cron-report.md。完整Wave2和Wave3–10仍未完成；下一S7统一已接线async端口的SQLite有界阻塞隔离，保持schema24。

## Wave2-S7 实施

S6最终457Rust/36Web、九门禁、真实TLS S3四项/PG六项闭合后开始。真实current_thread+第二SQLite连接BEGIN IMMEDIATE RED：100ms timeout被同步端口阻塞至5.21s、实际Ok(StorageUnavailable)，不能把async签名当异步隔离。已实现storage::sqlite_blocking统一四个共享permit，Publication/Writer/ObjectArtifact实际端口均复用；取消的已开始工作仍占permit到提交/回滚。满额直接typed unavailable；对象adapter旧五秒排队改为有界立即拒绝。daemon publisher原JoinSet改async，不再随调用者spawn_blocking/runtime.block_on。8 SQLite发布合同GREEN，其中新timer合同0.35s全组完成。新取消/三端口过载合同与完整9门禁、S3/PG回归待验证。schema仍v24；全部native业务asyncports/PG/离线迁移仍pending，不能标完整Wave2–10完成。

S7 最终implemented / locally verified：459Rust/36Web/10外部ignored、九门禁、真实TLS S3四项及PG发布六项全部通过。统一三组端口的有界SQLite隔离及取消/过载，schema仍v24。报告wave2-s7-adapter-report.md；完整Wave2–10仍未完成。后续必须完整native业务asyncports、全业务PG/租户键/原事务、audit/quota/offline迁移，不得把协调端口当作全部事实源。

## Wave2-S8 实施（继续完整 Wave2–10）

S7 九门禁和真实 S3/PG 回归闭合后推进原会话读回纵向迁移。有效四项 RED 为 foreign read、scope 在页 limit 后遗漏、退休 ownerless lifetime 重绑定和损坏 run lifetime 跨会话 terminal。保留原当前 lifetime UNIQUE，新增 v25 永久归属/孤立旧历史隔离/immutable run incarnation；单事务 canonical SQL 带 key/lifetime 和 scope。core SessionListEntry、async SessionReadRepository、统一 adapter 与原 daemon read/list/close caller 已接线；CLI session-read、HTTP共享严格DTO、ACP/WS原投影。七项 storage 合同 GREEN，六入口进程暴露错误 data 被当成功 DTO、ACP typed error 丢失，已修且保留日志；旧测试 chat 自动新建两 session 导致 clock 变化，改只读CLI保持原零写断言。完整门禁/真实 S3/PG 回归待跑；完整业务PG、其余asyncports/审计配额/离线迁移与Wave3–10仍pending，不提升readiness。

S9 预审：中心 request guard和identity/run.authorization/slash真实caller仍同步Query。S8所有lane闭合后迁移至core-only async身份查询端口，共用四permit与原Connection，保留准入事务最终policy/owner复核。新增纯core授权/协调错误封装，不携rusqlite和SQL文本。schema25保持，旧同步Local owner端口仍供尚未迁移调用者。先受控Connection占有+current_thread timer真RED，之后取消/过载、scope/撤销/晚owner/重启、六入口、九门禁和真实S3/PG回归。

S8最终implemented / locally verified：467Rust/36Web/10外部ignored、九门禁全exit0，真实TLS MinIO四项和真实PG发布六项全过。schema25及rollback/隔离证据报告wave2-s8-readback-report.md，完整Wave2–10尚未完成；继续S9中心身份/授权async端口。

## Wave2-S9 实施

S8 467Rust/36Web、九门禁与真实TLS S3四项/PG发布六项闭合后实施。真实Connection占有+current_thread RED得到Ok(identity)，100ms timer未调度；新core RepositoryError/IdentityReadRepository两方法统一四permit，中心授权/foreground准入/identity/run.authorization/slash caller已await。原准入同事务最终verifier保留；scoped原owner读SQL返回typed Forbidden/StaleOwner，过期writer不重新acquire。九项身份合同GREEN；共享取消过载、中心SQL缺表故障六入口与零准入、S8重启读回合同正在运行。schema25不变；最终九门禁/真实环境lane待跑，完整业务async/PG与完整Wave2–10未完成。

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

最终本地验收：wave2-s13-execution-stack-final 九项全部exit0，511Rust/36Web/45真实daemon，外部10项明确ignored。执行合同9/storage82/runtime131均通过，原ACP标准与前台generation故障合同保持，未加栈或跳过测试。真实TLS MinIO四项与PostgreSQL发布协调六项独立回归全部exit0，见 wave2-s13-execution-regression-s3.json / postgres.json。原失败门禁和夹具失败均保留；未CI/生产验证、未提交。全企业Wave2尚未完成，Wave3–10 pending，enterprise_ready=false。继续原Provider attempt/未知结果和实际runtime caller，不能称所有ports或全部PG事实已迁移。

继续S14原Provider attempt：S13全验收结束；两个真实业务RED开始，分别验证终态不能改写原provider身份、已persist OutcomeUnknown的attempt不能导致run假成功。后续才应用同事务async端口及实际runtime caller，schema25保持，全Wave2–10未完成。

S14推进：五项原事务RED已修复，八项scoped合同GREEN。真实HTTP完整接收后断连有效RED观察到3次请求/run Failed；新增驱动内部dispatch证据，只有确证未建立连接或明确4xx拒绝才允许安全重试，5xx/丢失响应/现场超时取消保守Unknown。wire GREEN一次调用/OutcomeUnknown/typed -32002/重启不重放。native新attempt冻结route缺口有效RED（profile维度截停，其余维度待GREEN验证），同原Immediate TX增加running/candidate/profile/API/model校验。新的timer及完整合同/共享预算/九门禁与真实lane仍待验收，未提升全企业readiness。

S14 implemented / locally verified：wave2-s14-attempt-audited-final 九项全部exit0，526Rust/36Web/48真实daemon、10外部环境项明确ignored。12项attempt存储合同/131 runtime单测/3真实HTTP接受后断连取消超时全部GREEN。独立TLS MinIO四项业务与PostgreSQL发布协调六项全部exit0；PG runner五步骤不等于五业务合同。首次依赖清单/Clippy遗漏及两项旧取消断言失败保留，未放宽架构护栏、未加栈/跳过测试。schema25，不新增第三方包，未提交推送部署；完整Wave2–10未完成，enterprise_ready=false。继续原工具事务async/固定owner，后续完整PG/租户键/nativewriter/auditquota/offline仍必须闭合。


S15 implemented / 局部验收：原工具六异步端口接实际前台 caller，同原 Connection/四 permit/固定准入 owner；四项有效旧业务 RED 修复。原未知工具后继续执行后续 wave 的有效 RED 修复（后续 effect1→0，模型请求均1），原 typed unknown/no replay。11工具合同、83storage单测、真实工具发现回填合同通过；File已commit后取消等待者仍持permit/收据持久合同通过。保存batch SQL字节预检与十二项扩展合同正在跑，九门禁/S3/PG待最终验收。schema25/readiness=false，完整Wave2–10未完成；详见 docs/enterprise/wave2-s15-tool-report.md。


S15 implemented / locally verified：wave2-s15-tool-audited-final 九项全部exit0，540 Rust / 36 Web / 48真实daemon，39个test executables、10外部环境项明确ignored；12项工具存储合同、83 storage单测、132 runtime单测通过。真实TLS MinIO四业务与PostgreSQL发布协调六业务回归全部exit0。原工具六async端口/固定owner/原File两阶段receipt事务及未知工具停止后续wave已接实际前台caller，无新表/状态机/第三方依赖，schema25。原功能RED、编译/夹具失败证据保留，不计虚假RED；未放宽超时/栈/架构。Cron兼容和其余业务ports/fullPG/租户键/nativePGwriter/完整outbox/auditquota/offline仍欠；完整Wave2–10未完成，enterprise_ready=false。详见 docs/enterprise/wave2-s15-tool-report.md。

S16 implemented / 局部验收中：原turn/transcript/计划六异步端口已接实际foreground runtime及daemon start-plan，同一Connection/四permit/固定准入envelope。六项原越域RED已修，15项扩展合同通过；另确认已结算无决策run仍返回计划执行成功和receipt.run_id错误仍执行两个有效RED，原helper增加running及SQL/JSON身份校验，16项最终合同在跑。Message临时借用编译失败、旧计划直接SQL插入clock变化和执行夹具误用Queue均保留、均不计功能RED。schema25/无新状态表/无第三方库；完整九门禁/S3/PG尚待验收，全Wave2–10仍pending/readiness=false。


S16 implemented / locally verified：wave2-s16-turn-audited-final 九项全部exit0，556 Rust / 36 Web / 48真实daemon，40个test executables、10外部环境项明确ignored；16项turn/transcript/计划integration合同、83 storage/132 runtime单测通过。独立真实TLS MinIO四业务和PostgreSQL发布协调六业务全部exit0。原六async端口接实际前台/PlanStore/daemon start-plan，同一Connection/四permit/固定准入envelope；六越域RED、已结算无决策仍给执行许可RED、receipt错误run_id仍执行RED修复。JSON四维和SQL四维身份GREEN，不把第一个截停RED夸成多项RED；故障回滚/CAS/幂等/撤销/过期和真实daemon原计划六入口/恢复合同保持，无新表/第三方包、schema25。编译/夹具失败及测试无效import警告保留，不计功能RED；未放宽超时/栈/护栏。全业务ports/nativePG/租户键/nativePGwriter/outbox/auditquota/offline仍欠，完整Wave2–10未完成，enterprise_ready=false。详见 docs/enterprise/wave2-s16-turn-report.md。


2026-10-08 持续执行：用户要求直到Wave10完成再停。S16已完整本地验收；S17请求捕获/上下文异步迁移in_progress，先复现原请求行SQL/JSON完整合法错配及scope缺口，随后唯一helper/固定准入envelope/实际模型及摘要await。全企业Wave2–10尚未完成，readiness=false。继续使用planning-with-files已有记录，不重建计划。

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

S20库runtime136/storage83和真实daemon五项compact合同全通过（libraries-reviewed.log / daemon-compact-reviewed.log）；同批新端口及exactOwner共享预算断言在storage测试内已过。S21开始原即时事件投影真实RED，schema27，预审/tmp/agent-rust-wave2-s21-event-projection-plan.md。S20/S21统一本地完整门禁收口，仍非全Wave2完成。

S20/S21 implemented，完整本地验证进行中：原上下文/compact八async端口与exactOwner端口、即时事件投影async端口已接真实caller，scope/fixedOwner/CAS/原stamp/预算在唯一原事务复核。context16/event17局部通过，runtime136/storage83及五项真实daemon compact通过；新增child SQL预算和锁合同在统一门禁中验证。schema27，无新第三方依赖，未提交推送部署；全业务async/nativePG/租户键/PGwriter/auditquota/offline与完整Wave2–10未完成。详见wave2-s20-context-report.md及wave2-s21-event-projection-report.md。

后续优先S22原实时事件写入（已有RunEventRepository/append_event_scoped，实际daemon固定execution_envelope接线），再S23 discovery。预审/tmp/agent-rust-wave2-s22-live-events-plan.md与真实RED草稿；S20/S21九门禁及真实环境验收期间生产源码仍冻结，尚未实施S22。

S20/S21首次完整门禁8/9通过，仅workspace tests101：真实daemon central_async_authorization_storage_fault... ACP stdout EOF，47/48实测daemon通过（另2外部ignored）。故障夹具principals仍存在、1session/0run、无web诊断，故障发生于ACP启动/初始化阶段而非授权故障注入之后。原helper丢stderr到sink无法诊断；追加64KiB有界stderr及method/真实ExitStatus，仍持续drain，不改合同断言、timeout、栈或test并行度；独立复核该合同，再处理根因。原8项成功不覆盖失败test，总验收未完成。

ACP原并发复核完成：48 passed / 0 failed / 2 ignored（50总项），daemon-bounded-diagnostic.log exit0；首次EOF仍未定位，不称已修根因。S20新增第10个有效业务RED：source_json合法JSON改为不同lifetime后read_compaction_result实际返回Ok(Some(0))，证据compact-result-identity-functional-red.log。原唯一事务增加JSON源lifetime/起点/整数范围与committed+1/no_gain原generation一致性；健康revision冲突及summary_failed以Conflict拒绝重放，损坏以Corrupt拒绝。新增三项读回合同正在验证。首次改动闭包Err.into推断失败，显式返回Result<ProjectionGeneration, RuntimeError>修复，编译失败日志保留、不计业务RED。全Wave2–10仍未完成。

S20新增来源身份修复后的context19/event18均通过（result-corrected-green.log），原首次closure类型推断编译失败日志保留。wave2-s20-s21-reviewed-final完整九项门禁已启动；生产源码冻结，S22只/tmp预审与合同草稿，不称已实施。

2026-10-09 S20/S21 implemented / locally verified：wave2-s20-s21-reviewed-final 九项全部exit0，614 Rust /36 Web /48实测daemon，43测试执行体、10外部环境项明确ignored；runtime136/storage84/context19/event18全部通过。独立真实TLS S3四组业务及PostgreSQL发布协调六项全部exit0（wave2-s20-s21-reviewed-regression-s3.json / postgres.json），自己的MinIO容器与PG隔离DB已清理。第10项有效compact来源身份RED已修，同原事务核JSON lifetime/起点/结果generation，正常revision和摘要失败返回Conflict，损坏Corrupt。首次九门禁tests101/ACP初始化EOF保留；独立与原并发daemon复核、最终完整门禁均未复现，根因未定位，64KiB有界诊断并持续drain保留，未修改超时/栈/并行度。schema27，无新第三方包。全企业Wave2、Wave3–10仍未完成，enterprise_ready=false；未提交推送部署。继续S22实际Live事件mutation，再S23发现端口，统一本批验收。

S22两个有效业务RED已复现（live-events-functional-red.log）：foreign旧compatibility append以及无冻结授权旧run的late callback都Ok(EventSeq(2))。已有frozen SHA的native owner保护不能称被绕过；复现明确授权记录0、合法同步更新run及head generation。原helper保留唯一events/interactions/stamp/outbox事务；native live新async入口与五个实际chat caller固定准入env、shared4budget、状态/allowlist/object/字节预算已接线，foreground初始历史改await原execution_context。check workspace/alltargets/allfeatures已exit0。新增typed scope/owner/终态/预算/rollback/两Connection seq及取消awaiter仍实际worker提交合同待GREEN，尚未最终验收；S23继续待实施。extract脚本已应用，不得重跑覆盖新源码。

S22 storage首轮84/85通过，取消awaiter新合同失败不是timer/预算失效：原Fixture.admit默认Queue，worker正确以Conflict拒绝queued事件，所以没有提交。新live合同明确try_start_queued先进入Running，保留原timer/permit/提交一次断言，不改生产guard、不放宽status；三个新native事件夹具及并发夹具同样真实start，原已有18只读夹具保持不改。并发cursor用start后的真实last_seq，不能用准入时旧seq。最初late-owner旧路径RED是Queued兼容run，后续需Running原helper再复现，避免夸可执行callback证据。

S22纠正Running夹具后storage85全过（storage-corrected-green.log），25ms timer/四permit占有及worker取消后单次提交原断言通过。原callback helper在真正Running且head/run generation一致更新后再次真实复现成功EventSeq(3)（live-running-owner-functional-red.log），不是Queued不合法状态的混合证明；恢复测试调用线上实际native append_run_live_event固定env，预期不变，原legacy runid helper保留维护语义。完整23事件合同继续GREEN验收；本批尚未九门禁和真实环境验收。

S22局部verified：storage85/event23全部通过，含满额四permit、真实连接锁timer/awaiter取消后worker真实提交及interaction/event/stamp/outbox故障rollback、两独立连接seq/stamp/outbox。真实daemon48回归正在运行（daemon-reviewed-local.log）。报告wave2-s22-live-events-report.md，尚未最终九门禁/真实环境收口，S23接下来同批迁移。

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

### 2026-10-09 敏捷续开发：PostgreSQL 资源端口

- 范围：复核当前真实源码，补 RunResourceRepository 的八个 PostgreSQL 方法，共享原资源状态规则；不进入后续 Wave。
- 修改：core/resource、storage/resources、storage-postgres/resources；沿用 SQLite schema27、PG canonical schema1，不新增事实表或第三方依赖。
- 兼容：Local 状态字符串及原 RPC 不变，PG 复用同一异步端口；当前后台 Native 执行仍只向 Local 开放。
- 风险：原树 S26 后尚未编译，局部检查发现 core 委派导入不存在的 SessionId；--locked 发现依赖清单与锁文件不同步，改用 --offline 更新本地锁文件。
- 验证：开发后只做必要编译及资源合同，不运行九项门禁或完整测试。PG 实库若未执行明确标记未验证；已有 schema1 不自动重建。
- 状态：源码复核完成，资源实现进行中。

本轮资源切片 complete（源码及上述局部验证），不代表完整Wave2完成。原有--locked与core导入、记忆借用、资源bool、compact参数、daemon scope/Arc/cfg和flywheel测试trait问题已修；新增测试夹具in_memory错误改为临时SQLite文件。两次patch格式上下文未匹配均未落盘，读取实际格式后修正。下一步继续PG其他领域端口与backend-neutral启动装配。

### 2026-10-09 继续开发：PG 计划管理、诊断、Hook续跑与启动装配

1. 补齐 PlanControlRepository 四方法及 RuntimeObservationRepository 三方法；复用既有计划/正文验证、读回和安全文件目录。
2. 将 Stop continuation 接入原 PG admit_in 同事务准入，永久关系、原 Hook 成功证据与冻结授权继承不得新建第二准入路径。
3. 将 daemon 装配收敛到共享领域端口集合，生产装配不再要求具体 SQLite RunStore；Local 继续同一路径，PG 缺失领域明确保留。
4. 先完成实现，仅运行必要局部编译，不运行完整测试与门禁。schema27/PG canonical1沿用原表，兼容现有 RPC。
状态：实现进行中。

本批实现完成：PG 计划4/诊断3/Stop续跑3方法已写入；原准入和终态事务共用，最终计划/回执变更后封存事件。DaemonRepositories 同adapter的26端口接实际Local async bootstrap，旧SDK保留委托。生产局部check通过，未跑完整测试或门禁；PG缺memory/delegation/maintenance，完整启动及Wave2–10仍未完成。


### 2026-10-09 PG 记忆、委派、维护与本机启动

补齐同源 PG 记忆/维护/委派领域端口，并将 daemon/Cron/导入/artifact 接到完整26端口装配；默认SQLite保留，postgres-local显式Unix socket与writer、空Local域human和独立workload初始化，PG文件目录绑定database/schema。canonical迁移v2实际执行，遗忘不破坏历史反馈幂等；共享摘录/凭据过滤和授权conditions。恢复、核验及文件GC使用原事实事务，GC取消后仍受管并排空。实库冒烟发现并修正resource session_id关联错误、同owner过期预留、异步Provider预算遗漏；实际启动发现PG UUID与trace文件校验不兼容，保留32hex兼容并接受严格UUID。局部生产编译和test-support内部测试源码编译通过，真实PG18.6合同1项通过。真实binary双后端启动/假模型chat/重启去重通过，每个后端仅调用模型一次，PG未生成SQLite事实库；未跑完整测试或门禁，完整Wave2–10未完成。详见docs/enterprise/wave2-native-runtime-report.md。

最终补正：真实PG chat暴露执行future与事件消费者等待同一连接的闭环；改为join并行驱动，已接收事件完成及剩余队列排空后才提交终态，未引入第二执行状态。最新binary的两个后端冒烟均exit0，使用本机假模型及独立临时工作区。

## 2026-10-09 持续至 Wave10（本轮）

用户授权从当前工作树继续全部原始规格，保留未提交改动，不提交、推送或生产部署；仅必要编译和局部合同，完整验收后置。原始规格已通读；旧局部验收不代表本树全部通过。

- [进行中] Wave1：企业域启动/可信身份路由、管理授权、冻结策略及双人审批。
- [进行中] Wave2：远程PG TLS与连接配置、SQLite审计/配额对等及实际事务消费、完整离线迁移。
- [待实施] Wave3：OIDC/服务身份、持久directory、共享限流、稳定错误、预算。
- [待实施] Wave4：registry/pack/plugin、签名信任、不可变generation和分层配置。
- [待实施] Wave5：持久DAG/人工节点/补偿、A2A canonical投影。
- [待实施] Wave6：MCP OAuth/SecretStore、隔离connector-host、增量reload与网络边界。
- [待实施] Wave7：强隔离worker、mTLS、资源/网络预算与artifact输出。
- [待实施] Wave8：OTLP、统一审计/脱敏/签名导出、留存legal hold与成本。
- [待实施] Wave9：Operator/CRD/Helm、HA、drain、升级与灾备。
- [待实施] Wave10：故障矩阵/benchmark/供应链/SBOM/provenance/RC产物验收配置。

全程维护逐项证据；未运行的完整或外部验收标待验收，enterprise_ready=false。

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

- 当前切片：Wave3费用账本纵向链路（domain+33/v7迁移+双backend+RPC/CLI/HTTP+原TX预算），待局部合同。双端点目录restart403 DTO修复待binarysmoke。未完成Wave3和Wave4–10，继续执行原scope。

### 2026-10-10 多企业 Gateway 纵向切片已局部验证，开始 Wave4

当前 schema SQLite34 / PostgreSQL canonical8，daemon33同后端领域端口。单 Gateway 受保护 AGENT_GATEWAY_SCOPES 配置最多16个完整scope，JWT未经验证的scope只用于有限配置选路，选路后Gateway和daemon分别完整验签/原事务授权。API key以X-Agent-Scope或WS首帧scope选路；错误/跨scope均401。OAuth cookie按别名隔离，额外OAuth回调必须包含配置别名；客户端连接缓存上限256。

必要cargo check/build通过；真实HTTP Gateway + 三daemon + 同PG schema的两个tenant冒烟通过：同名主体/Agent/operation的opaque目录互不访问，B的API key仅在B配置可用；原A双运行端点/Gateway重启/授权码一次消费/输出上限128/费用30微单位/模型仅1次调用仍通过。证据evidence/20261010/agent-wave3-multi-scope-smoke.json。未测试真实远程IdP/HA/多Gateway压力、工具与resource费用专项仍待验收；Wave3功能主要入口实现，不据此宣称完整验收。Wave4注册表/配置/签名代际正在实施，Wave5–10尚未实现，enterprise_ready=false。

- Wave4注册/config/签名纵向切片：SQLite35/PG9实库共享合同各1项通过；34同后端daemon端口。CLI/HTTP/RPC发布与读回、原session绑定/准入TX/原run snapshot/LoopEngine真实接线。最新MCP来源/Hook信任/根历史/CLI NOFOLLOW改动必要binary build进行中，随后真实HTTP固定V1而current V2模型请求冒烟。完整验收与Wave5–10仍未完成，不退出任务。

- Wave4真实入口组合smoke通过：同Gateway两tenant/三个daemon/实PG9，先发布签名V1并创建原目录会话，再发布V2+100%canary，旧会话实际模型请求含V1 prompt且无tools，重启仍读原run；费用30微单位、输出128、1modelcall和PKCE一次消费保持通过。证据agent-wave4-registry-runtime-smoke2.json。注册/config/发行主要纵向切片已实现并局部验证；完整并发/归档CLI安全矩阵/三入口parity待验；Wave5九类持久DAG/native引用/A2A正在开发，Wave6–10仍待实施，继续不停。

### Wave5 持久编排当前切片
- [x] 严格DAG/frozen输入、36/10迁移、共享规则与双backend端口、35端口daemon装配、RPC/CLI/HTTP。
- [x] 原run准入TX关联、独立workload调度、有界执行、AgentRun/ToolCall/Approval/Webhook/SubWorkflow调用链；普通pure四类。
- [x] SQLite/PG native关联/lease/CAS/重启局部合同；较早版本，不代表最新全部功能验收。
- [ ] 最新反向补偿、内部命令幂等、未知reconcile与artifact原引用专项。
- [ ] scripts/smoke-workflow-a2a.py真实双backend入口/断流/重启，当前binary构建中。
- [ ] Wave6、7、8、9、10全部实现及必要局部验证，继续原任务。


## 2026-10-11 用户要求的云端开发检查点

- 当前代码与进度归档并提交到现有 main，普通推送 origin，不强制改写历史。
- 最新 SQLite 工作流 native fence 局部合同通过；完整测试和门禁后置。
- Wave5 仍部分；继续时先补原 HTTP/A2A 冒烟及其余合同，再推进 Wave6–10，不把本次提交当作功能完成或生产发布。
