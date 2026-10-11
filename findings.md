# 实施发现

- 当前目录 `/Users/pilot/Documents/myproject/agent-rust` 为空，且不是 Git 仓库。
- 用户蓝图要求一个 binary crate、约 8 个职责模块、统一 tokio 异步运行时，以及逐阶段 build/clippy 验证。
- 非测试代码不得使用 `unwrap()` / `expect()`；应用边界使用 anyhow，领域错误使用 thiserror。
- 真实 LLM 验证受环境变量与外部服务可用性影响，因此自动化验收需要 mock provider 覆盖工具调用闭环。
- 2026-09-07 探测 `rsproxy.cn` 与 USTC crates.io 镜像均返回 HTTP 200；选用 rsproxy sparse 并限制在项目级配置。
- 阶段一采用 SSE 行增量解析，将分片的文本和工具名称/arguments 分别累积；工具 arguments 完整后再解析 JSON。
- assistant 的工具调用消息与 tool 结果都进入历史，tool 结果通过 `tool_call_id` 严格配对。
- 最终实现保持一个 binary crate 和单进程 CLI；核心由 8 个职责模块构成，阶段五只增加 JSONL 关键词记忆。
- 上下文压缩仅改变内存中的请求视图；session.jsonl 保留原始 append-only 对话，重启后会在需要时重新压缩，兼顾可审计性和实现简单度。
- 工作区外只读直接拒绝，工作区外写入/编辑走审批；该选择避免并行只读波次同时争抢终端审批输入。
- DeepSeek 的 OpenAI 兼容端点 `https://api.deepseek.com/chat/completions` 与 `deepseek-v4-flash` 已在 2026-09-08 实测支持本项目的流式工具调用格式。

## 进阶基线核对与实现结论

- Cargo 仍是单 binary crate；依赖均为开源 crate，Rust 2024 edition，最低 Rust 1.85。
- `main.rs` 确实只提供 CLI REPL，启动时组装安全、6 个工具、上下文、session 和记忆。
- `provider.rs` 的 `Message.content` 目前只能表达纯文本，图片内容块需要扩展消息模型；SSE 已能累积文本和 function arguments 分片。
- provider 已解析 `prompt_tokens_details.cached_tokens` 并用 tracing debug 输出，但当前请求未发送显式 cache_control，工具定义也作为顶层 `tools` 字段而不是上下文消息。
- `loop_engine.rs` 确实实现 50 轮 ReAct、连续只读最多 8 路并行、副作用串行、tool_call_id 回填与逐消息持久化。
- `context.rs` 当前只有 80% 单闸门；稳定消息顺序为系统提示→AGENTS.md，随后历史与动态环境。工具 schema 是 Provider 顶层字段，无法由 ContextManager 直接插在两个系统块之间。
- `safety.rs` 是文件路径与命令的唯一决策点；工作区外读拒绝、外写/编辑审批，灾难命令硬拒、高风险命令审批，与基线一致。
- `session.rs` 是 append-only JSONL，能忽略崩溃残缺末行并用 Tokio Mutex 提供无超时 RAII turn 锁；`memory.rs` 是 TTL JSONL + 关键词/中文 bigram，需模型主动调用。
- 工具注册表当前存 `Box<dyn Tool>` 且不可克隆/筛选。sub_agent 要复用工具实例并创建受限子集，需要改为 `Arc<dyn Tool>` 或增加共享子集视图。
- `LoopEngine` 当前把主 session 持久化与 turn 锁写死在执行入口中。sub_agent 需要抽出不持久化、独立历史、可配最大轮次的内部运行入口。
- `read_file` 对 PNG/JPG/WebP/PDF 等均只返回降级说明；消息 content 只有 String，图片多模态需要向后兼容地扩展内容块表达。
- 基线描述与真实代码总体一致；主要偏差是“稳定前缀中的工具位置”受 OpenAI 顶层字段协议约束，以及缓存 usage 已记录但没有显式断点。
- DeepSeek 官方文档确认上下文缓存对所有用户自动启用、不需要代码或接口变更，只匹配从第 0 token 开始的相同前缀；返回字段为 `prompt_cache_hit_tokens` 与 `prompt_cache_miss_tokens`。因此没有向 OpenAI 兼容请求添加非标准 cache_control，而是稳定排序工具 schema 并补齐两种 usage 形状。
- 图片按 DeepSeek/OpenAI 兼容协议使用 user 消息的 `text` + `image_url` 内容块；工具回执仍保持字符串和 tool_call_id 配对。base64 只存在于当前 turn 的临时消息，不写入 session。
- PDF 采用开源 `lopdf` 在 `spawn_blocking` 中本地抽取，限制 16 MiB、50 页与约 512K 字符；不做视觉渲染。
- 两级压缩默认水位为 60%/85%；温和模式只摘要可压缩旧历史的最老三分之一，强力模式保留最近配置数量。
- skill 从 `.my-agent/skills/*.md` 建立标题/摘要索引，英文关键词与中文 bigram 匹配，最多加载 3 个正文；目录不存在时无感禁用。
- cron 与 MCP 属第三批明确可停项，本轮在完成 skill 后停止扩张，留待独立设计。
- `lopdf` 已关闭默认的 chrono/jiff/rayon/time features；本地 PDF 生成与抽取回归测试仍通过，减少了不必要的依赖面。
- 当前工具执行环境未配置 `OPENAI_API_KEY` / `OPENAI_BASE_URL` / `MODEL_NAME`，故进阶功能以 mock Provider、真实本地文件和 CLI 烟雾测试完成验收；先前基础版本的 DeepSeek 工具闭环实测记录仍保留。

## Daemon 架构基线核对与最终结论

- 附件要求一个工作区对应一个 daemon，CLI/HTTP/编辑器入口全部通过同一 JSON-RPC 方法访问，不允许入口复制业务逻辑。
- 当前 `main.rs` 直接创建 Provider、SafetyPolicy、8 个工具、MemoryStore、PlanStore、ContextManager、SessionStore 与 LoopEngine，并直接持有 `Vec<Message>` 进入阻塞式 stdin REPL；它确实是单进程直连。
- 当前 `LoopEngine` 持有 Provider、ToolRegistry、ContextManager 和可选 SessionStore；主入口用 `Some(session)`，sub_agent 使用 `None` 的 ephemeral 模式。这为 daemon 收拢提供了复用入口，但主历史仍由调用者以 `&mut Vec<Message>` 传入。
- Provider 当前只在完成整个 SSE 后返回 `Response`，尚未向上游发出文本增量或工具开始/结束事件；阶段 A 的“流式 chat.send”需要给共享执行层增加事件 sink，而不是由入口模拟。
- `SessionStore` 的 turn 锁在 `LoopEngine::run_turn` 内部获取；daemon 若同时持有 history 锁与该锁，需要固定锁顺序以避免死锁。
- 现有审批实现是同步终端 `y/N` 回调，daemon 化后必须改为可挂起的审批请求状态，并由 `approval.respond` 唤醒；入口不能直接读 daemon 的 stdin。
- 现有代码尚无取消 token、session 清单元数据、会话 ID、socket 生命周期、HTTP server、clap 子命令或配置对象。
- `PlanStore` 已支持 set/update/add/show、原子临时文件提交和动态上下文注入，适合作为 daemon 持有的共享状态，不应在各入口重复创建。
- `MemoryStore` 与 skill 库均以工作区路径为作用域；daemon 化后应只初始化一次并由所有入口共享。
- `ReadFileTool` 已支持图片临时内容块及 PDF 本地抽取；daemon 事件协议应避免把大型 base64 工具中间内容重复广播给客户端。
- `ToolRegistry` 内部使用 `Arc<dyn Tool>` 且可 clone/subset，适合直接迁入 `DaemonState` 的单实例装配。
- `Cargo.toml` 尚无 `clap`、HTTP server、socket 辅助或哈希依赖；阶段 A 可仅用现有 Tokio `mpsc/oneshot` 完成内存回环，阶段 B/C 再按需引入依赖。
- Provider 的 SSE 解析入口集中在 `parse_sse`/`consume_sse_line`，可用可选 `mpsc::UnboundedSender<ProviderEvent>` 向上转发真实文本分片，同时让现有 mock 仅实现 `chat` 并通过 trait 默认方法保持兼容。
- 阶段 A 采用轻量自研 `CancellationToken(Arc<AtomicBool>)`，在模型请求和工具波次外层用 `tokio::select!` 响应取消，无需新增依赖。
- `SafetyPolicy` 只依赖 `Arc<dyn Approval>`，因此可用 daemon `ApprovalBroker` 无侵入替换 `TerminalApproval`；审批表与活动请求表必须使用独立锁，避免等待审批时阻塞 `approval.respond`/`agent.cancel`。
- 当前 session 仅有一个活跃 JSONL 与轮换备份；阶段 A 的 `session.list` 将如实枚举当前文件及同目录备份，而不虚构多会话数据库。

## 阶段 B 设计结论

- Unix 客户端需要保持一条持久连接并按 request_id 多路复用；若每个 RPC 新建连接，“最后客户端断开退出”会导致 REPL 两轮之间 daemon 被反复拉起。
- socket server 将每个请求交给同一 `DaemonState::handle_request`，连接层只负责 NDJSON 解码/编码和 4 MiB 限制，不复制 RPC 业务逻辑。
- 运行目录采用系统临时目录下 `my-agent/<稳定工作区哈希>`，同时允许 `MY_AGENT_RUNTIME_DIR` 覆盖；目录权限设为 0700，避免 socket/PID/ready 在不同工作区冲突。
- daemon 空闲退出只在“至少连接过客户端、当前客户端为 0、没有活动 turn”同时成立后触发；活动任务绝不由空闲计时器强杀。
- 已抽出 `daemon::runtime::build_daemon_state`，后续 daemon 进程成为 Provider/工具/安全/历史的唯一装配点，CLI 不再装配运行时。

## 阶段 C 与生命周期补充

- HTTP 普通与 SSE 路径都直接消费 `DaemonClient` 的同一事件流；HTTP 无交互审批能力时采用安全默认值“自动拒绝”，避免危险操作被静默放行或请求永久挂起。
- OpenAI 兼容入口只提取请求中最后一条非空 user 文本；对话真相仍来自 daemon session，避免客户端重复上传的全量历史被再次持久化。
- HTTP `serve` 进程持有一条持久 daemon 连接，因此服务存活期间 daemon 不会触发“最后客户端断开”退出。
- 烟雾测试用 PTY Ctrl-C 停止 `serve` 时发现 daemon 残留 pid/ready；根因推断为自动拉起的子进程继承前台进程组并同时收到 SIGINT。修复方向：daemon 子进程独立 process group，且 server 自身监听 Ctrl-C 做清理。
- 上述修复复测后 pid/ready/socket 均正常消失；运行目录只保留 daemon.log，`status` 稳定返回 stopped。
- stdio 编辑器入口采用并发转发：stdin 持续读取请求，按原 request_id 启动独立转发任务，stdout 单写协程串行输出，因此 chat 等待审批时仍能接收 `approval.respond` 或 `agent.cancel`。
- HTTP 失败烟雾测试会先持久化 user 消息再遇到上游错误，这是正确的 Agent 语义但会污染开发工作区；已将本次测试创建的两行 session 文件移入废纸篓，后续烟雾测试需显式设置临时 SESSION_PATH。
- README 与功能总览 HTML 仍描述“单进程、单 CLI、无 daemon/多入口、启动询问恢复”，已经与新实现相反；最终验收必须整体更新架构、请求首段、模块表、运行命令和能力边界。
- 桌面 HTML 的核心工具/记忆/安全内容仍有效，可保留视觉样式，重点重写入口→daemon→ReAct 的前半链路并新增 daemon/client/entry 模块说明。
- 最终形态由一个按工作区隔离、按需自动拉起的 daemon 持有全部运行时真相；CLI、HTTP 与 stdio 编辑器入口都只使用 `DaemonClient` 和统一 JSON-RPC 协议。
- 并发审批必须使用 task-local 请求上下文；全局可变“当前事件出口”会让排队请求覆盖正在等待的审批路由。对应双请求回归测试已固定该约束。
- 最终 48 项测试、release、fmt 与严格 Clippy 全绿；隔离 stdio 实进程测试确认协议响应和 daemon 空闲退出，HTTP/UDS 生命周期测试也已覆盖。

## 标准 ACP + WebSocket + 重连恢复：阶段 0 发现

- daemon 私有 RPC 真实方法为 `chat.send`、`session.load`、`session.list`、`session.new`、`approval.respond`、`agent.cancel`、`daemon.stop`。
- `chat.send` 参数是 `{message: String}`；`approval.respond` 是 `{approval_id: String, approved: bool}`；`agent.cancel` 是 `{request_id: RequestId}`。后两者当前没有“作用域”字段，审批只有本次允许/拒绝。
- `session.load` 返回 `{messages, pending_approvals, active_requests}`；pending 项实际结构为 `{id, request_id, prompt}`，没有拆分后的动作名、命令或选项字段；active request 是字符串或数字 request id 数组。
- daemon EventKind 实际为 `turn_started`、`text_delta`、`tool_started`、`tool_finished`、`approval_required`、`turn_completed`。工具事件含 call id/name，结束事件另含 output；审批事件 data 为 `{approval: PendingApprovalInfo}`。
- 当前 `ApprovalBroker::request` 在审批事件接收端断开时会删除 pending 并返回错误；这与本轮“连接断开不能决定业务终态”的硬约束冲突，阶段 C 前必须将审批 truth 与单连接事件发送解耦。
- 编辑器入口 `run_stdio_adapter` 确认是纯私有 JSON-RPC 透传：解析项目自己的 `JsonRpcRequest`，把 method/params 原样交给 `DaemonClient`，并把 `ServerFrame` 原样写 stdout；没有 ACP initialize/session 方法或 server notification/request。
- Web 入口确实是 axum 0.8，现有路由仅 `/health` 与 `/v1/chat/completions`；鉴权落在 `is_authorized(HeaderMap, Option<&str>)`，HTTP/SSE 遇到审批会安全地自动拒绝。
- CLI 连接后不会自动 `session.load`；`/status` 只显示 history/active/pending 数量，审批交互仅发生在当前 `chat.send` 流收到 `approval_required` 时。
- `DaemonClient` 在单条 Unix 连接上按 request id 多路复用，但只把 Event 发给“发起该 request id 的本连接 pending channel”；新连接无法订阅既有 active request。要满足重连继续收流，需要 daemon 侧持久的请求事件广播/重放机制和一个订阅 RPC，而不能由入口伪造。
- `serve_unix_connection` 为每个连接创建独立 frame sender；客户端 EOF 后该 sender 最终关闭。当前 `chat.send` 与连接 sender 生命周期耦合，进一步确认断线恢复需要解耦请求执行与连接输出。
- WebSocket 可直接基于现有 axum 增加 `ws` feature 与 `WebSocketUpgrade`；握手后仍可复用 `DaemonClient::request_with_id`，但需并发读写以允许审批/cancel 与长 chat 同时进行。
- ACP 官方组织明确提供 Rust 实现 `agent-client-protocol`，当前稳定 wire protocol 为 v1；官方仓库包含 agent/client 示例。搜索结果显示旧单仓库曾发布 0.13.x，而官方项目后来又拆出 `rust-sdk`，因此必须以 crates.io 当前元数据和实际下载源码为准选定固定版本，不能仅凭搜索摘要猜版本。
- 项目把 crates.io 替换为 rsproxy sparse，首次裸跑 `cargo search/info` 被 Cargo 拒绝并提示指定 `--registry crates-io`；下一次查询使用该明确修正。
- crates.io 当前正式 crate 是 `agent-client-protocol 2.1.0`（Apache-2.0、官方 `agentclientprotocol/rust-sdk`），但 MSRV 为 Rust 1.88；项目当前声明 Rust 1.85，当前机器编译器为 1.98.1。采用 2.1.0 就必须明确把项目 rust-version 提升到至少 1.88，或调研是否有仍可获取且满足协议能力的 1.x 版本。
- `agent-client-protocol 2.0.0` 同样要求 Rust 1.88；搜索摘要中的旧 0.13.3 已无法通过当前 crates.io 索引获取，不能作为可靠选项。
- 已确认官方 2.1.0 是完整 SDK 而非只有 schema：导出 `Agent`/`Client` role、`Builder`、`ConnectionTo`、`Stdio`、`Lines`、typed request/notification、session helper 与权限请求能力；默认使用稳定 ACP v1，草案 v2 需显式 feature，本项目不应开启不稳定 v2。
- 官方 2.1.0 README 明确提供 `simple_agent` 示例并把 stdio/连接构建纳入 crate；因此选择官方 crate 路径优于手写 ACP 帧。项目可把 `rust-version` 从 1.85 提升到 1.88（当前工具链 1.98.1），并精确 pin `=2.1.0`。
- 已从下载后的 crate 源码核对：标准 agent 入口形态是 `Agent.builder().on_receive_request(...).connect_to(Stdio::new())`，不需要 `tokio_util::compat`；crate 自带 stdio 传输。
- SDK 的稳定 v1 session helper会在 `session/new` 后动态安装 session 消息 handler；`PromptRequest` 对应终态 `PromptResponse`，流式内容经 `SessionNotification`/`SessionUpdate::AgentMessageChunk` 主动发送。权限往返有 typed `RequestPermissionRequest/Response`，可直接实现标准方法而非私有帧。
- ACP v1 schema 的必要映射已核对：`NewSessionRequest` 要求绝对 cwd，`NewSessionResponse` 要求 SessionId；`LoadSessionRequest` 含 sessionId/cwd；`CancelNotification` 只带 sessionId；`RequestPermissionRequest` 必须关联一个 `ToolCallUpdate` 并给出 options；稳定选项种类包含 allow_once/allow_always/reject_once/reject_always。本项目 daemon 仅支持一次性 bool，故只广告 allow_once 与 reject_once，不能伪装持久授权。
- ACP 稳定 `SessionUpdate` 原生支持 agent message chunk、tool call、tool call update 和 plan；本项目当前事件没有独立 plan 事件，若未来出现无对应事件才降级文本，本轮不伪造 plan update。
- ACP request handler 会阻塞同连接后续入站分发；`session/prompt` 和 `session/load` 若在 handler 内直接等待 daemon/权限响应会死锁。正确模式是用 connection context `spawn` 后台任务并立即返回，让 responder 在任务终态响应。
- `ConnectionTo` 原生支持 typed `send_notification`、`send_request`；权限请求可在后台任务中无超时等待 client response。工具状态可准确映射为 Pending/InProgress/Completed/Failed。
- ACP prompt 的基线内容要求 Text 与 ResourceLink；本适配器会把文本直接拼接，把 ResourceLink 以名称+URI 注入文本，不广告尚未完成入站转换的 image/audio/embeddedContext 能力。
- SDK 的 `ConnectionTo::spawn` 任务生命周期与 ACP 连接绑定，适合让 prompt handler 立即返回而后台消费 daemon 流；真正跨 ACP 进程断线继续执行仍依赖阶段 C 的 daemon 事件订阅，而不是依赖该 task 存活。
- 官方 SDK 支持 `Channel::duplex()` 与 `Client.builder().connect_with(agent, ...)`，因此阶段 A 集成测试可全程进程内使用正式 ACP client/server 两侧和 mock daemon，无需依赖真实密钥或脆弱的手写 JSON。
- 危险审批集成测试可注册一个测试工具，通过真实 `ApprovalBroker` 请求授权；mock Provider 先返回该工具调用、再返回文本，由 ACP Client 的 typed `RequestPermissionRequest` handler 选择 allow_once，完整覆盖通知、权限往返和继续执行。
- ACP 适配器已按 SDK 推荐结构拆成可测试的 `build_acp_agent` 组件；正式入口连接 `Stdio`，单元集成测试可让官方 Client 直接 `connect_with` 该组件。
- daemon 重连采用新增私有 RPC `agent.subscribe {request_id}`：active turn 在 daemon 内维护取消令牌、最多 1 MiB 事件回放与 broadcast 实时流；订阅 RPC 把回放/实时事件改绑到订阅请求 ID，因此能被新 `DaemonClient` 正确路由。
- 已解决审批重放重复响应问题：订阅时按 `ApprovalBroker` 当前 pending 集合过滤已经被明确处理的旧 `approval_required` 回放；新产生的审批事件仍实时投影。
- 阶段 C 新发现：`chat.send` 在 `LoopEngine` 完成前持有 daemon history Mutex；若模型等待审批，重连端的 `session.load` 会被永久阻塞。恢复快照现从 append-only session JSONL 读取（每条消息 append 后 flush），并保留内存历史供活动 turn 使用。
- WebSocket 重连时原外部 request id 映射可能不存在，恢复事件使用 daemon active request id；客户端应以 recovery snapshot 中的 active request id 订阅/取消，测试已覆盖该语义。
- TUI 调研结论：当前 `main.rs` 默认进入普通 stdin REPL，代码中没有终端绘制层。适合新增 `src/entry/tui.rs` 作为瘦入口，通过 `DaemonClient`/现有 RPC 消费事件；不应把 LoopEngine、Provider 或安全逻辑复制到 TUI。
- 依赖选择：`ratatui 0.30.2`（MIT，MSRV 1.88，默认 crossterm backend）+ `crossterm 0.29.0`（MIT）；当前工具链为 Rust 1.98.1，满足要求。TUI 需要处理 raw mode、alternate screen、输入框、事件流、审批 y/N、Ctrl-C 和退出清理。
- TUI 优化核对：原界面默认颜色继承终端荧光绿；原滚动按原始行计数且使用 usize::MAX 哨兵，导致中文换行和 PageUp 不正确。改为显式 RGB 主题、统一按显示列宽换行、距底部行数滚动。视图独立于 RPC 层，工具结果默认折叠，Markdown 仅处理标题/强调/代码，不改会话内容。
- 终端主题兼容结论：不能把“支持 RGB 类型”当作“当前会话会正确呈现真彩色”。在 `TERM=dumb`、`NO_COLOR=1` 或终端调色板重映射环境中，固定 RGB 可能被抑制或错误降级。可靠默认值应为 `Color::Reset` 前景/背景并用 Bold/Dim 建立层级；内置 RGB 深色主题只应显式选择。
- session 现状：`SessionStore` 仅持有固定 `.my-agent/session.jsonl`，`session.new` 会把它重命名为 `.bak-时间戳`，而 `session.load` 永远只读固定活动文件。备份 session 没有切换/恢复 API，恢复后也无法保持稳定 ID。
- TUI 旧对话根因：`run_tui` 启动第一步直接调用 `recovery::load_snapshot(session.load)` 并把全部消息转成 UI 消息；CLI `chat` 入口还会运行 `recover_connection` 主动打印历史。因此“重开程序”等同于自动恢复当前 daemon session。
- 会话切换并发约束：`chat.send` 会先注册 active request，再锁 history 并在 LoopEngine 内取 turn lock。新建/恢复需要持有 active map 锁确认空闲，再持有 history 锁并切换 SessionStore 当前路径，才能阻止新 turn 在切换窗口内插入并写入错误 session。
- 设计选择：每个 session 使用稳定独立 JSONL 文件，SessionStore 保存一个 current 指针；`session.resume {session_id}` 只接受列表内安全文件名。TUI/REPL 启动显式调用 `session.new`，`/resume` 再由用户选择历史，不再自动加载旧对话。
- SQLite 评估：对于大量 session 的时间/标题/关键词检索、跨表事务和未来全文搜索，SQLite 优于遍历 JSONL；但当前单用户 daemon 下不是 `/resume` 的必要依赖。适合后续作为结构化索引与元数据层，原始大内容仍文件化，并通过迁移工具导入现有 JSONL。
- session 并发审计补充：仅用 active/history 锁仍会让 `session.load` 与 new/resume 在极窄窗口返回“旧历史 + 新 ID”的混合快照；DaemonState 增加专用 `session_switch` 互斥后，load/new/resume 的路径与 ID 观察保持一致。

## 六项通用能力补齐：基线审阅（2026-09-09）

- Provider 基线：`Provider` 仅要求 `chat`，默认 `chat_stream` 只为纯文本补发 `TextDelta`；`ProviderEvent` 当前只有 `TextDelta`；`OpenAiProvider` 在 `parse_sse`/`consume_sse_line` 内同时做 wire 解码、按 index 累积 id/name/arguments、JSON 解析并直接产出 `Response::ToolCalls`。
- 当前 tool-call 装配所有权在 provider，而非 agent。OpenAI arguments 使用 `push_str` 纯顺序拼接，没有长度优先或合法 JSON 快照替换；但没有 Started/Delta/Completed 生命周期、identity 分域、重复完成/EOF 不完整检测。
- `LoopEngine::run_turn_with_events` 收到完整 `Response::ToolCalls` 后先持久化 assistant 调用，再进入 `execute_in_waves`；只读连续段用 `buffered(8)`，写类串行。`ToolRegistry::execute` 先用自研 JSON Schema 子集校验，再调用工具；路径/命令 safety 与 approval 位于具体工具持有的 `SafetyPolicy` 中。
- 当前装配异常发生在 provider 请求返回阶段，因此不会执行本轮工具；但不存在可折回模型的 typed assembly error，错误会直接终止 turn。
- tool result 通过 `Message::tool_result` 原样使用 `ToolCall.id` 回填，provider 原始 id 同时也是内部执行 key，尚未做协议域隔离。
- `Config` 目前直接检查 `OPENAI_API_KEY`、`OPENAI_BASE_URL`、`MODEL_NAME`，没有 `API_TYPE`；daemon runtime 直接构造 `OpenAiProvider::from_env()`。
- Slash 基线：CLI `run_repl` 与 TUI `submit_input` 各有一套硬编码 `match`，帮助文本也各自维护且已发生漂移（CLI 广告 sessions/cancel，TUI 帮助未完整列出）；HTTP/OpenAI 入口和 ACP 目前不解析 slash。
- 既有命令最终都通过 daemon RPC 或公共 recovery helper 操作 session/cancel，适合把“解析 + 命令描述 + RPC 意图/统一响应”下沉，入口只保留终端特有交互渲染。
- Skill 基线：`SkillLibrary::context_for` 每次在 blocking 线程扫描目录；索引项为文件 stem/title/summary/body，标题从 `# ` 猜测、摘要从 `summary:`/`摘要：`或首段猜测；ASCII term + 中文 bigram 交集计数，substring 加 100，按 id 稳定打破平分，最多载入 3 个正文。当前索引并非常驻且读文件时正文已全部载入内存。
- daemon 请求通过 `DaemonState::handle_request` 集中分派，后台 turn、审批与 shutdown 由 Tokio task/channel/cancellation 管理；`LoopEngine::ephemeral` 是 sub-agent 的隔离历史执行入口。
- `PlanStore::persist_state` 使用同目录 `.tmp-<pid>` 写入后 `tokio::fs::rename` 原子提交，可复用于 cron store。
- `ToolRegistry` 已是 `HashMap<String, Arc<dyn Tool>>`，支持 clone、stable specs 与 subset；`register` 当前会静默覆盖同名项，MCP 注册需显式检测冲突并加稳定前缀。
- daemon runtime 的唯一装配点是 `build_daemon_state`；`DaemonState` 持有 engine/history/session/approval/active/shutdown。Unix server 的主 `select!` 监听连接、空闲、显式 shutdown、Ctrl-C，退出前等待 active turn，然后清理运行标记；cron scheduler 与 MCP manager 应由 state 持有并在 shutdown 分支显式停止。
- 现有 daemon 会在最后客户端断开且无 active turn 后约 2 秒退出，这与“常驻 cron”语义冲突；启用 cron/heartbeat 时需要让后台工作成为 daemon 存活条件，或调整生命周期为有 enabled job 时不空闲退出。
- `SubAgentTool` 用 `LoopEngine::ephemeral` + 新 `Vec<Message>` + memory-only plan 实现隔离历史，但它是模型工具且会选择受限工具子集；cron 可复用同一隔离 runner 思路，不应通过当前用户 session 的 `LoopEngine::new`。
- `ContextManager` 当前无 provider capability 判断；总是传完整 specs，多模态由 `ReadFileTool::from_env` 与 transient image message 控制。能力降级最合适放在统一 Provider trait/factory 或 LoopEngine 请求边界，不能在入口分叉。
- tracing 在 `main::init_tracing` 初始化；当前 usage 只在 OpenAI SSE 的 `consume_sse_line` 内以 debug 字段记录，无独立 metrics store。
- SessionStore 是 append-only JSONL，独立 session 由文件/指针管理；cron 不应切换该全局 current session，可直接使用 ephemeral engine 或单独的 session 文件/runner。
- 初始质量门禁：`cargo test --all-targets` 64/64 通过；严格 Clippy 与 rustfmt check 均通过。

## 项目一与项目二完成结论（2026-09-09）

- 新 Provider 契约把 wire 解码归一为 TextDelta/ToolCallStarted/ToolCallDelta/ToolCallCompleted/typed failure；OpenAI、Anthropic、Ollama 分别封装 SSE/SSE/NDJSON 差异。
- execution identity 使用 `tc1:<协议域>:<id|pos>:<值>`，wire id 用 URL-safe base64 可逆编码；OpenAI/Anthropic 出站回放只发送还原的原始 id，Ollama idless 调用仅在完整调用出现时分配流内 position。
- canonical assembler 独占 arguments：Append 永远逐字节追加，只有显式 AuthoritativeSnapshot 才覆盖；重复开始/完成/快照、匿名或迟到 delta、EOF 未完成、非法 JSON 都形成 typed failure。
- LoopEngine 在任何执行前对整批调用做存在性与 schema admission；assembly 与 admission 错误类型分离，并作为 system feedback 折回模型。本轮任一错误均不执行任何工具。
- capability 由 Provider trait 暴露；不支持工具时不注入 specs，不支持图片时统一降级为文字提示，入口/Context 不按 api_type 分叉。
- 三个本地 TCP mock 分别验证工具调用、agent 装配、tool result 协议回填和后续文本响应；全量 79/79 测试、严格 Clippy、fmt 通过。

## 项目三完成结论（2026-09-09）

- `src/slash.rs` 是命令名、alias、usage、参数规格、help 与 action 的唯一注册表；`/help` 完全由注册表生成，新增 `/ping` 只注册一次。
- daemon 新增 `slash.execute`，session 状态查询/新建/恢复等副作用仍由 DaemonState 执行；CLI/TUI 只解析统一 SlashResponse 并做各自渲染，不再保留命令 match 或平行帮助。
- ACP prompt 复用同一注册表，明确只允许 help/status/sessions/ping 四个只读命令；会话切换继续使用标准 ACP session 方法。WebSocket 可直接调用相同 daemon RPC；OpenAI HTTP prompt 不广告 slash。
- TUI 集成与正式 ACP Client 测试均断言 `/ping` 返回 pong；全量 80/80、严格 Clippy、fmt 通过。

## 项目四完成结论（2026-09-09）

- Skill 必须使用 YAML frontmatter 声明 name/version/description/keywords/scope；无效文件按项 warn 并跳过，不拖垮索引。
- SkillLibrary 以共享 RwLock 常驻元数据索引，正文仅在命中后重新读取；排序权重为精确 keyword ≫ 归一化 metadata term/bigram ≫ 正文 term/bigram，并按 version 降序、name 升序稳定打破平分。
- 本地安装器把源限制在工作区内、目标固定为 `.my-agent/skills`，用临时文件 + rename 提交；高版本覆盖、同版 skip、降级需 `--force`，remove 必须 `--confirm`。
- `/skill list/install/update/remove` 通过共享 slash + daemon 持有的 SkillLibrary 执行；坏 frontmatter、版本冲突、稳定排序和 slash 主路径均有测试。
- 新增固定依赖 `serde_yaml_ng =0.10.0`（MIT）与 `semver =1.0.28`（MIT OR Apache-2.0）；84/84、严格 Clippy、fmt 通过。

## 项目五完成结论（2026-09-09）

- `CronStore` 原子持久化 `.my-agent/cron.json`，支持 interval 与标准五段 cron、启停、删除、有限历史及重启恢复；坏文件降级为空 store，不阻断 daemon。
- `CronManager` 后台 tick 支持轻量 stagger、有限指数退避、可关闭 heartbeat；heartbeat 只做 store 自检，不调用模型。enabled job 或 heartbeat 会阻止 daemon 空闲退出，显式停止时后台任务可 join。
- `AgentCronRunner` 每次使用全新历史和独立 `.my-agent/cron-sessions` 文件；无人值守工具注册表使用独立 `UnattendedApproval`，所有需审批动作默认拒绝并写入最终运行记录。
- `/cron list/add/enable/disable/run-now/remove` 由共享 Slash 注册表与 daemon 真相执行，删除要求 `--confirm`。持久化、到点触发、失败重试、审批拒绝、五段表达式和 slash 主路径均有测试。
- 新增固定依赖 `chrono =0.4.45`、`cron =0.17.0`，两者均为 MIT OR Apache-2.0；89/89、严格 Clippy、fmt 通过。

## 项目六完成结论（2026-09-09）

- `src/mcp.rs` 是自研 stdio JSON-RPC 客户端：支持换行与 Content-Length framing，完成 `initialize → notifications/initialized → tools/list`，并把 `tools/call` 结果转换为普通工具文本。
- `.my-agent/mcp.json` 做文件级与 server 级双层隔离；非法 JSON/顶层结构只禁用 MCP，单个 server 配置/握手失败不影响其它 server。仅在 `args`、env value、cwd 展开 `${...}`，`command` 与 env key 保持原样；相对 cwd 必须落在工作区内。
- 工具以 `mcp__<server>__<tool>` 稳定前缀动态注册，schema 进入现有整批 admission；reload 原子替换动态 snapshot，`/mcp list/status/reload` 通过共享 Slash/daemon 执行。
- MCP 调用进入 `SafetyPolicy::authorize_external_action`：默认视为副作用并走审批，参数中的灾难命令硬拒，路径边界提示进入审批；不引入第三方 MCP SDK。daemon shutdown/reload 会关闭 stdin、kill/wait 子进程并清理 pending 请求。
- 测试覆盖合法/非法 server 共存、占位符边界、两种 framing、模型 tool-call→MCP→工具结果回填、默认拒绝、子进程清理与 slash 主路径。

## 最终全量验收（2026-09-09）

- `cargo fmt --all -- --check` 通过。
- `cargo build --release` 通过。
- `cargo test --all-targets`：97/97 通过。
- `cargo clippy --all-targets --all-features -- -D warnings` 通过且无 warning。
- 文档已同步 README 与 `docs/agent-system.html`；明确新增配置、slash、MCP stdio 限制和安全边界。

## TUI 交互与渲染能力补齐：现状确认（2026-09-09）

- 退出控制目前错误地复用状态文案：`run_event_loop` 第 216 行以 `state.status == "退出"` 结束；Esc（244）与 SlashResponse::Exit（407）写入相同字符串。其余“退出”仅是 UI 文案或终端清理错误上下文。
- `UiMessage` 只有 `role/content`（23-26），流式文本由 `append_assistant`（119）与最后 assistant 合并；ToolFinished（335-347）把工具名和输出拼成一个带换行的字符串，`view::message_lines`（279-295）再按行拆回。没有工具 call id、状态、耗时、消息 id 或 usage 容器。
- 输入是 `String`；`handle_key` 只支持 Esc、审批 Y/N/Enter、PageUp/Down、Ctrl+T、Ctrl+U、Ctrl+C、Backspace、字符末尾追加、Alt+Enter、且只有 `active.is_none()` 的 Enter 才会提交（304-315）。Paste 也只追加末尾（211-213）。
- 滚动为“距底部行数”：PageUp/Down 固定 ±8（256-262），view 用 `start=max_scroll-scroll`（161-166）；审批独占另一滚动值，固定 ±4（248-254）。没有 Home/End、逐行、鼠标或 follow-bottom 字段。
- `wrap_lines` 定义于 `view.rs:390`，每次 draw 都包裹输入、审批和所有消息（89、100、158-161），当前无缓存。输入光标始终按最后一行宽度计算，无法定位到编辑中的中间字符。
- 快照重建在 `from_snapshot`（73-74）和 `replace_snapshot`（111-112）均只取 active/pending 的 `.first()`，因此会静默丢失并发请求和审批。
- 主题仅有 terminal/dark；`view.rs:13-69` 的 Theme 直接携带具体色，组件引用字段尚未有 success/error/info/diff 等语义 token。硬编码 `Color` 仅集中在该映射和测试；组件还有若干直接 `Style::default` 背景组合。

### 阶段 1：类型化退出（已完成）

- `TuiState::should_quit` 是唯一循环终止条件；`request_quit` 统一供 Esc 与 SlashResponse::Exit 使用，status 仅显示“再见”。
- 新增回归测试证明任意 status 文案不会改变退出信号；TUI 定向测试与严格 Clippy 通过。

### 阶段 2：结构化 UiMessage（已完成）

- `UiMessage` 现在带稳定自增 id、创建时间、可选 token usage、内容版本；内容以 `Text` 或 `Tool(UiToolCall)` 区分。
- 工具节点保留 `tool_call_id`、名称、面向用户的中文标题、运行/成功/失败状态、输出行、展开状态与开始/结束时间；`工具执行错误:` 结果会明确标为失败，视图直接渲染卡片，不再拼接后拆分文本。
- 修正 TUI 对 daemon 事件字段的读取为 `tool_call_id`（此前误读 `call_id`）；同名工具调用现在按调用 ID 归属。
- 定向 TUI 测试与严格 Clippy 均通过。

### 阶段 3–8：交互、缓存、并发与主题（已完成）

- `InputEditor` 使用 `Vec<char>` 保存 Unicode 标量与光标，支持左右、行首尾、词级移动/删除、前向删除、多行粘贴、历史浏览；视觉坐标按 `unicode-width` 计算，CJK 不再以字节偏移定位。
- 聊天滚动保留“距底部”语义并加入 follow-bottom；PageUp/Down 翻页，Ctrl+上下逐行，Ctrl+Home/End 首尾。`MY_AGENT_TUI_MOUSE=1` 才启用鼠标滚轮捕获，默认不改变终端鼠标行为。
- 消息渲染缓存键为 `message_id + content_version + width + tool 展开态 + theme`，以 512 项为上限；会话快照替换清空缓存，内容版本变化和宽度变化自然失效。
- 普通输入在当前 turn/审批期间进入 FIFO `queued_turns`，Response 后自动启动下一条；Ctrl+K 清空尚未发送的队列。
- 活动流改为 `Vec<ActiveTurn>`、审批改为 `VecDeque`；恢复快照完整保留每个 active request 并逐一订阅，流式文本和工具卡片按请求 ID 分离，避免并发串流。
- 主题扩展为 terminal/dark/light；Theme 以 info/success/error/warm/code/diff-add/diff-remove 等语义 token 供组件使用，终端模式继续只用 Reset 色。
- 新增输入 CJK、缓存失效、浅色语义色及并发 request/审批队列回归测试。

## 多窗口独立 session（2026-09-09）

- 原 daemon 的 `history`、`SessionStore` turn lock、`LoopEngine` 和 `active` map 都是全局单例；`session.new` 在任意活动请求存在时直接返回 `-32001`，因此多个窗口无法独立运行。
- 运行时现改为 `SessionRuntime`：每个 session ID 对应固定 JSONL 路径、独立内存 history、turn lock 和 engine；共享的 provider/tools/context 只读复用。
- 活动请求键改为 `(session_id, request_id)`，快照、取消、订阅和待审批按 session 过滤；TUI、REPL、ACP 在 chat/slash/subscribe/cancel 请求中显式携带 session ID。
- 为兼容旧客户端保留无 session ID 的“legacy session”选择；新窗口不再接回其它窗口的活动请求，也不等待其它 session 的 turn lock。
- 首次验证发现新 session 创建若复用 workspace current pointer，会在另一个 session 活动时改变共享写入路径；已改为隔离创建并仅更新 daemon 内存中的 legacy session 选择，避免串写。
- 首次多 session 并发测试直接断言历史条数时遇到 append 尚未 flush 的时序；改为短轮询快照后再断言，生产代码未增加等待。
- 一次定向验证误把两个过滤器同时传给 `cargo test`，Cargo 在编译前拒绝；随后按项目既有规则改跑单个过滤器/全量 `--all-targets`，未影响生产代码。

## OpenClaude 对比研究（2026-09-09，初步）

- OpenClaude 把“前台会话”和“后台任务”明确分成两类：后台任务有独立的本地子进程、名称/状态/日志/终态记录和 `ps/logs/kill/attach` 控制面；这比单纯保留 daemon 内存 active map 更适合长任务恢复。
- OpenClaude 的 `QueryEngine`/`messageQueueManager`/`queueProcessor` 将用户输入队列、停止/中断、重试与消息提交拆开，说明当前项目可进一步统一请求生命周期状态机，而不是让 TUI 自己维护一套队列语义。
- OpenClaude 的 goal 服务把目标状态、持久化、评估器、控制器和 prompt instructions 分层，并有状态机测试；当前项目已有 PlanStore，但缺少对“目标/下一步/完成判定”的持久化控制层。
- OpenClaude 的远程 session 管理、permission bridge 和 websocket 恢复都围绕稳定 session ID 做事件重放与权限关联；这一点与当前刚完成的 `(session_id, request_id)` 隔离方向一致，可继续提取为通用 session lifecycle/status API。
- OpenClaude 的 goal 状态转换是纯函数，明确记录 `turnCount`、`lastEvaluatedMessageUuid`、最大轮次、暂停/完成原因，并把 evaluator 失败当作可持久化状态而不是异常退出；这是当前 PlanStore 最值得借鉴的可靠性模式。
- OpenClaude 的 queue manager 使用“不可变快照 + 订阅通知 + 优先级 dequeue + 过滤器”，同时保留非匹配命令，能避免 React/异步消费者因队列变化丢消息；当前 TUI 只有局部 FIFO，需要抽成 daemon 可观测的队列模型。

## OpenClaude 对比落地（2026-09-09）

- 排队请求取消：`LoopEngine` 获取 session turn lock 时同时监听 `CancellationToken`。这样同一 session 的第二个窗口/请求在前一个 turn 长时间运行时可以立即返回“请求已取消”，不会把取消请求卡在锁等待上；新增 PendingProvider 回归测试覆盖该时序。
- 计划写入串行化：`PlanStore` 的 `set/update/add` 共用 mutation mutex，保证“读当前状态→校验→原子持久化→替换内存状态”是单写者临界区，避免两个工具调用并发时后写入覆盖先写入的步骤更新；新增并发更新测试。
- session 实时状态：`SessionInfo` 增加 `status`（idle/running/waiting）、`active_requests` 和兼容性 `updated_at` 字段。daemon 从活动请求表和审批 broker 实时派生状态，session.list、CLI `/sessions`、TUI 恢复选择均展示活动状态；旧 JSONL/旧 session 清单因 serde default 保持可读取。
- 取舍：没有直接复制 OpenClaude 的后台子进程控制面或完整 goal evaluator，因为当前项目已有 cron/MCP/daemon 生命周期，先优先修复会直接影响多窗口交互可靠性的三个临界区；后台任务控制面可作为后续独立阶段。

## OpenClaude TUI 对比研究（2026-09-09）

- OpenClaude 的主 REPL 把消息区、底部 prompt、prompt footer/status line、通知/快捷键提示分层；prompt 底部槽位有最大高度约束，避免多行输入把 transcript 挤没。当前 TUI 也有五段布局，但状态提示和快捷键全部挤在一行 footer，输入区缺少明确的模式/队列/审批层级。
- OpenClaude 的消息呈现以“用户/助手/工具”三种视觉角色为核心：用户消息有清晰的输入标记，助手输出保留 Markdown/流式位置，工具调用以紧凑的 spinner/结果行呈现，详细输出可展开；当前 TUI 已有结构化 UiMessage/工具卡片，但每条消息前后额外空行较多，工具与正文的视觉层级仍不够紧凑。
- OpenClaude 的滚动模型支持 sticky bottom、离底后显示新消息分隔/跳到底部入口，并对长 transcript 使用虚拟列表；当前 TUI 已有 follow_bottom、缓存和 PgUp/Dn，但离底时只有“距底部 N 行”文字，没有明显的“新消息/回到底部”交互提示。
- OpenClaude 的 footer 会按终端宽度隐藏/折叠可选信息，把状态线、快捷键、模式提示和队列提示分别处理；当前 TUI 虽按宽度裁剪 footer，但窄终端会直接丢失状态和取消/审批提示。
- 可迁移方案：保留现有 ratatui 和协议，增加 compact transcript markers、sticky-bottom 新消息 pill、结构化 status bar、prompt 内队列/审批提示、可切换 help overlay 和更强的窄终端降级。暂不引入 OpenClaude 的 React 虚拟 DOM、远程对话框或品牌 Logo。

## OpenClaude TUI 对比落地（2026-09-09）

- transcript 现在以更紧凑的“角色标题 → 内容 → `·` 结束标记”呈现，助手正文使用 `│` 延续线，代码块使用 `┌─/└─`，工具调用保留状态色和可展开输出；这对应 OpenClaude 的 MessageResponse/工具行层级，同时不改变消息数据。
- footer 拆成状态线和快捷键线：状态线显示当前操作、活动请求、排队数量或待审批数；快捷键线按宽度降级，避免把所有信息塞进一个长字符串。
- sticky-bottom 体验增加 unread counter：用户滚离底部时新消息会累计，transcript 下方显示“X 条新消息 · 回到底部”；回到底部会清零。现有 Ctrl+End、PgUp/Dn 和 follow_bottom 语义保持不变。
- 新增 F1/Ctrl+/ 帮助浮层，集中说明发送、多行、历史、滚动、工具、队列、取消和退出操作；Esc 在帮助打开时只关闭帮助，不会误退出。
- prompt 高度改为随终端高度动态调整，最多占用一半内容区且保留状态/footer；窄终端继续使用最小布局和短提示。

## Agent 不可用诊断：第一轮源码证据（2026-09-09）

- 截图中的直接错误不是 Provider 超时，而是 `write_file` 对多个目标返回 `No such file or directory (os error 2)`；当前 `src/tools/write.rs` 只调用 `tokio::fs::write(&path, content)`，没有在写入前创建父目录。模型选择了 `src/main/resources/static/...`，该目录不存在时每个文件都会失败。
- `LoopEngine::execute_in_waves` 会把工具错误转成普通 `ToolOutput::text("工具执行错误: ...")` 并折回模型上下文；模型可以继续生成下一轮工具调用。当前最大 ReAct 轮数是 50，单次错误没有立即失败或暂停机制。
- 重复检测只在“工具名 + 参数哈希 + 结果哈希”完全一致连续三次时发送提醒，不会停止循环；截图中重复的 `plan` 调用很可能参数在变化，或模型在不同调用间反复规划，因此不会触发现有提醒。
- TUI 能看到工具事件和错误文本，但当前没有 request ID、ReAct round、Provider 首包/总耗时、最后一次模型请求状态、连续失败计数或当前 daemon log 路径；所以用户只能看到“工具执行”，无法判断卡在 Provider、工具、审批还是模型循环。
- 当前 daemon active 状态只能告诉 session 有活动请求，`session_snapshot` 能列出 active request/审批，但没有按 request 的 round/tool failure 统计；需要结合 `.my-agent/runtime/*/daemon.log`、session JSONL 和新增结构化 telemetry 才能定位长循环。

## Agent 不可用诊断：真实会话证据（2026-09-09）

- 目标工作区 `/Users/pilot/Desktop/test/test01` 的 daemon 当前是 `ready · pid=7513`，session 列表显示该 session 已 `idle`、0 个活动请求、55 条消息；因此这次不是 daemon 仍在执行，而是执行过程中 TUI 缺少及时的中间态/失败熔断反馈。
- `/var/folders/.../T/my-agent/a2ce7645ea393d41/daemon.log` 只记录了 4 条 `write_file` WARN，全部是目录不存在；默认 `RUST_LOG=warn` 没有 request/round/provider/tool 参数摘要，诊断信息不足。
- session JSONL 还原出完整链路：模型先并行调用 4 次 `write_file`，全部因 `src/main/resources/static/{css,js}` 父目录不存在失败；随后调用 `exec mkdir -p ...`，再次写入成功，最终执行验证并返回完成文本。也就是说模型最终自我修复了，但用户在失败循环阶段看不到“正在恢复/重试/还剩几轮”的明确状态。
- 该 session 的 `plan.json` 最终为 3/3 done；`target/classes/static` 也已有四个资源文件，说明产物实际生成。截图截取的是失败批次附近，不是最终终态。
- 另一个可观测性问题：`myagent sessions` 入口先执行 `config::validate_environment()`，当当前 shell 没有 API 环境变量时会直接报配置错误，无法查看已有 daemon/session 状态；使用 `API_TYPE=ollama MODEL_NAME=qwen3 myagent ... sessions` 才能读到 idle 状态。这会让排障更困难。

## Agent 不可用修复实施勘察（2026-09-09）

- `AgentEvent` 当前只有 turn/text/tool/complete 五类事件；协议层 `EventKind` 也只投影这些事件，适合新增 `RoundStarted`、`Telemetry` 或给 ToolStarted/Finished 增加 round/duration/failed 字段，但要保持旧客户端可反序列化。
- `LoopEngine::execute_one` 当前把所有工具异常包装为普通 `ToolOutput`，无法让外层区分成功/失败；应在 `ToolExecution` 保留 `failed` 与错误摘要，回合结束时根据连续失败数决定继续还是返回明确失败。
- `myagent status` 已经不依赖模型配置，但只输出 pid/socket；`RuntimePaths` 已有稳定 `daemon.log` 路径，可以直接展示 log、ready、session 根目录。
- `sessions` 的 daemon 连接路径本身不需要 provider 配置；应移除环境校验，同时允许已有 daemon 直接查询，daemon 不存在时再给出不依赖模型的启动/状态提示。

## Agent 不可用修复设计（2026-09-09）

- 采用向现有 `ToolStarted/ToolFinished` 事件追加 `round`、`duration_ms`、`success`、`error` 字段的兼容方案，不新增事件种类，避免 ACP/HTTP/旧 TUI 必须处理新枚举分支。
- `LoopEngine` 按工具执行结果统计连续失败；达到 3 次立即返回明确错误，成功工具会重置计数。写文件自动创建父目录后，正常前端脚手架不会触发该熔断。
- Provider telemetry 使用 tracing 记录每轮总耗时、首个流式增量耗时与响应类型；默认日志级别从 warn 提升到 info，`status` 输出 daemon.log 路径，新增只读 `logs` 命令便于现场排障。
- 系统提示补充“模糊前端请求的默认技术栈/目录创建/失败恢复”约束，减少模型把可恢复的文件系统错误当作长循环入口。

## Agent 不可用修复验证（2026-09-09）

- `cargo check --all-targets` 已通过。
- `cargo test --all-targets` 已通过，当前 111 项全绿；正在补充本轮新增行为的定向回归测试与文档。

## Agent 不可用修复结果（2026-09-09）

- 新增回归后 `cargo test --all-targets` 为 113/113，`cargo clippy --all-targets --all-features -- -D warnings`、`cargo fmt --all -- --check`、`git diff --check` 均通过。
- `cargo build --release` 与 `cargo install --path . --force` 通过；`/Users/pilot/.local/bin/myagent`、`my-agent` 均已验证为 0.1.0 最新 release。
- 真实验证：空模型环境下 `myagent --workspace /Users/pilot/Desktop/test/test01 sessions` 可直接读取本地 session 快照；`status` 和 `logs` 可在 daemon 停止时工作，不再要求 API 环境变量。
- 用户工作区的旧 daemon 已确认 idle 后停止，避免旧进程继续使用修复前二进制；下一次带有效模型配置运行 `myagent` 会自动拉起新版本。

## TUI 任务完成与工具折叠优化（2026-09-09）

- 原实现 `show_tools=false` 只隐藏工具输出，仍逐条绘制每个工具卡片；截图中的 10+ 条工具行因此占满消息区。现改为连续工具节点默认合并成一行摘要，摘要包含次数、工具名、轮次范围、状态和总耗时。
- Ctrl+T 现在控制“工具调用与输出详情”整体展开/收起；默认折叠时不再自动展开失败工具输出，避免异常时再次淹没正文。工具结果仍保留在 UI 数据和 session 中。
- Response 成功/失败会向 transcript 写入 `✓ 任务完成` / `✗ 任务未完成`，并在状态栏显示 request ID；CLI 终态额外打印 `[任务完成] request_id=...`。
- 新增 TUI 回归：默认摘要不展示工具输出，Ctrl+T 展开后可见；Response 成功后显示任务完成标记。

## Mac 快捷键提示修正（2026-09-09）

- 运行时原本同时支持 F1 与 Ctrl+/，但标题、footer 和帮助优先显示 F1；Mac 用户通常没有独立 F1 键，现已将所有用户可见提示统一为 `Ctrl+/`，F1 兼容处理仍保留。
# 2026-09-12 TUI 三项问题修复

- 用户已确认开始修复 `docs/known-issues.md` 中的 TUI-001～TUI-003。
- 仓库当前有多处未提交修改，包括 TUI、daemon、上下文和既有规划文件；本轮必须在这些改动上增量工作。
- README 描述当前 TUI 已有结构化 transcript、Ctrl+T 全局展开/折叠、活动请求状态与完成标记，因此修复重点应是现有布局与状态投影，而不是新建另一套界面。
- 附件现象已经文字化保存：短 transcript 被推到底部形成巨幅上方空白；详情模式像替换页面并丢失原 Query；运行时只有数字变化、缺少持续动画反馈。
- Ctrl+T 当前只翻转全局 `show_tools`；`draw_ui` 随后重算 transcript 行数，而 `follow_bottom=true` 会强制 `scroll=0` 并从新的 `max_scroll` 开始显示。工具输出展开新增大量行后，原 Query 因此被直接顶出视口，形成“换到详情页”的感受。
- 当前事件循环仅在键盘/鼠标事件或 daemon frame 到达时把 `dirty` 设为 true；16ms poll 超时本身不触发重绘，所以运行状态无法呈现持续的 spinner/不确定进度动画。
- 工具详情已经按消息序列内联生成，问题不是缺少内联数据结构，而是全局展开后的滚动锚点丢失；优先增加稳定的视口锚点，而不是另建详情页。
- 当前 TUI 定向基线为 16/16 通过，但现有测试只断言展开内容存在，没有验证“展开前后原 Query 仍位于视口”或滚动锚点，因此未捕获用户截图中的跳屏。
- 项目使用 `ratatui 0.30.2`；`Terminal::new` 的 viewport 默认行为仍需从 `ratatui-core` 实现核对，排除终端初始化导致首屏从当前光标位置开始的可能性。
- `ratatui-core 0.1.2` 文档明确说明 Fullscreen viewport 的 `Frame::area` 从 `(0,0)` 覆盖整个 backend；默认 `TerminalOptions` 使用默认 viewport，因此现有大空白并非有意采用 Inline viewport。
- 当前手工初始化与 Ratatui 官方初始化流程一样先进入 alternate screen，但没有显式 `terminal.clear()`；需继续确认 Terminal 首绘是否保证清屏，以及用户终端是否可能保留主屏内容。
- Ratatui 的 `Terminal` 实现拆分在 `terminal/init.rs`、`render.rs` 等子模块，前一次只读主模块未看到构造器不是实现缺失；下一步直接核对这些文件。
- `Terminal::new` 初始化的是两个“空白”缓冲区，首帧只输出相对空缓冲发生变化的单元格；如果某个终端的 alternate screen 没有清除旧内容，未被绘制的空白单元不会主动覆盖旧屏幕。初始化后显式 `terminal.clear()` 能消除这类残留空白/旧主屏内容。
- 已实现第一版：进入 TUI 后显式清屏；Ctrl+T 保存当前 transcript 顶部行并关闭 follow-bottom 后再展开；新增 ActivityPhase 与 90ms 动画 tick，状态栏显示无百分比的往返式进度条。
- 第一版修改 `cargo check --all-targets` 通过；仍需新增回归测试验证 query 可见性、首行位置、动画变化与终态停止。
- 新增 3 项 view 回归：短 transcript 标题/Query 靠近屏幕顶部；80 行工具输出展开后原 Query 和首行详情同时可见且不 follow-bottom；活动状态显示会随 tick 改变的不确定进度条，审批状态改为静态菱形。
- 完成响应测试补充断言 ActivityPhase 回到 Idle，确保终态不会继续动画。
- `cargo fmt --all` 后 TUI 定向测试 19/19 通过（原 16 项 + 新增 3 项）。
- 实现 diff 审查确认修改仅落在 TUI 状态/渲染/初始化和测试；`git diff --check` 通过。
- 严格 Clippy（all-targets、all-features、`-D warnings`）通过，无新增告警。
- 全量测试已通过：122/122（包含新增 TUI 回归），无失败或忽略项。
- 仓库没有保留把 TestBackend JSON 转为 PNG 的脚本，只有旧 `docs/tui-preview.png`；视觉验收优先使用 TestBackend 行位置/内容断言和真实 PTY 启动。
- `cargo fmt --all -- --check` 与 `cargo build --release` 均通过。
- 隔离 PTY 启动捕获到 `ESC[2J ESC[1;1H`，证明初始化显式清屏生效；标题绘制在第 2 行、引导内容从第 5 行开始，不再下沉到底部。
- PTY 中发送 Esc 后正常输出退出 alternate-screen、关闭 bracketed paste 与显示光标序列，进程状态 0 结束，终端恢复链路正常。
- PTY 测试只在隔离目录生成一个 daemon 日志；测试目录已整体移到 `/Users/pilot/.Trash/my-agent-tui-fix.sFXqlN`，可恢复，未触碰用户工作区 session。
- README 与 `docs/known-issues.md` 已同步修复后的 Ctrl+T、运行指示器和验收状态。
- `cargo install --path . --force` 已成功替换 `/Users/pilot/.cargo/bin/my-agent`；还需核对用户实际调用的 `myagent` 是否为同一二进制/链接。
- 命令解析核对：`myagent` 是指向当前仓库 `target/release/my-agent` 的符号链接；PATH 中 `my-agent` 优先解析到 `/Users/pilot/.local/bin/my-agent`，因此又用 `cargo install --root /Users/pilot/.local --path . --force` 更新该实际命令。
- 进一步审阅 `src/daemon/lifecycle.rs` 后确认它包含 daemon 指纹升级和日志落盘等语义改动，不可能由本轮 rustfmt 产生；这是本轮未编辑的独立用户工作，已完整保留。
- 使用 `cmp` 核对三份可执行文件：仓库 release、`/Users/pilot/.local/bin/my-agent`、`/Users/pilot/.cargo/bin/my-agent` 逐字节一致，且都包含本轮 Ctrl+T 修复文案。
- 最终审查将 Ctrl+T 锚点从单纯的顶部行偏移增强为“最近一条用户消息 ID + 行偏移回退”；多轮对话中会优先定位本轮原 Query。
- 增强后 TUI 定向测试仍为 19/19 通过。
- Query 锚点回归现包含“上一轮用户/Agent 消息 + 当前 Query + 80 行详情”，并断言锚点行号非零时当前 Query 仍可见；该定向测试通过。
- 将测试包装函数收窄后，最终质量门禁全部通过：122/122 全量测试、严格 Clippy、格式检查、release 构建和 `git diff --check` 均为绿色。
- 最终版已重新安装到 `/Users/pilot/.cargo/bin/my-agent` 与 `/Users/pilot/.local/bin/my-agent`；两者与仓库 `target/release/my-agent` 经 `cmp` 确认逐字节一致，`myagent`/`my-agent --version` 均正常返回 0.1.0。
# 2026-09-12 项目展示 HTML 与 README

- 用户希望先更新本地项目全景 HTML，再将其内容以美观、GitHub 友好的方式落到仓库 README，并上传 GitHub。
- 实际找到两个文件：桌面 `/Users/pilot/Desktop/agent-system.html` 与仓库 `/Users/pilot/Documents/myproject/agent-rust/docs/agent-system.html`；文件名均为小写 `agent-system.html`。
- 开始时 `main` 与 `origin/main` 一致，工作树干净，基准提交为 `60de04d`。
- README 视觉实现需要遵守 GitHub 渲染限制：以 Markdown 和静态图片为主，不依赖 HTML 文件里的 CSS/JavaScript。
- 桌面 HTML 与仓库 HTML 已不一致：仓库版已更新“主任务无固定轮次上限、每 50 轮进度检查、连续 10 次完全相同调用才熔断”；桌面版仍是旧的 50 轮硬上限。
- 仓库 HTML 当前已有完整的 8 大版块和成熟视觉 CSS，但尚未写入 2026-09-12 的 TUI 显式清屏、Query 锚点内联详情、不确定进度动画，以及 daemon 二进制指纹/工作区日志等最新说明。
- 当前 README 功能信息较完整，但首屏只有标题和长段落，缺少徽章、视觉封面、快速价值说明与可扫描导航；功能列表过长、架构和用法层级偏平，末尾还有多余的 `# agent-daemon-`。
- README 应保留准确技术细节，同时重构为“品牌首屏 → 截图 → 为什么/能力矩阵 → 架构 → 快速开始 → 进阶能力 → 安全边界/限制 → 开发验证”的阅读路径。
- 已视觉检查 `docs/tui-preview.png`：深色终端风格、蓝色品牌色、清晰的用户/Agent 层级和输入框，适合作为 GitHub README 首屏主视觉；但截图是较早版本，底栏仍显示旧的 `Ctrl+T 工具` 简写，未呈现最新动态进度条。
- README 可直接引用仓库内 `docs/tui-preview.png`，GitHub 会稳定展示；HTML 则继续使用现有深色玻璃卡片/蓝青渐变视觉语言。
- CLI 现有正式入口为默认 TUI、`chat`、`tui`、`serve`、`editor`、`status`、`stop`、`sessions`、`logs`、`config`；`logs` 支持按 session/request/行数过滤。
- 共享 Slash 注册表包含 `/help`、`/status`、`/sessions`、`/resume`、`/new`、`/cancel`、`/skill`、`/cron`、`/mcp`、`/ping`、`/exit`，README 应避免只列旧命令子集。
- HTML 当前采用暖色纸张背景、teal/amber 状态色、sticky 目录、卡片网格、时间线、终端代码块和响应式断点；视觉已经成熟，适合做内容增补而非彻底换肤。
- HTML footer 日期仍为 2026-09-08，使用说明中的 TUI 文案未包含 Query 锚点、显式清屏和运行进度动画；“Cron / MCP”卡片却标为“可选项未做”，与正文“均已实现”矛盾，应改为已实现。
- Cargo 元数据确认项目版本 0.1.0、Rust 2024 edition、MSRV 1.88；仓库暂无 LICENSE、GitHub Actions、CHANGELOG 或 CONTRIBUTING，因此 README 不应展示虚构的 license/CI 徽章。
- README 徽章采用可核验的静态信息（Rust 1.88+、macOS/Linux、OpenAI/Anthropic/Ollama、ACP v1、MCP stdio），避免易过期的测试数量或不存在的 CI 状态。
- HTML 已补齐最新功能，README 已完成整体改版并新增 `docs/readme-hero.svg`；直接用 `view_image` 读取 SVG 失败，需先渲染为临时 PNG 做视觉检查。
- `xmllint` 验证 `readme-hero.svg` 为合法 XML。Quick Look 临时 PNG 显示暖灰纸张背景、teal 品牌胶囊、醒目的 my-agent 标题和指标卡片，中文字体清晰、视觉风格与 HTML 一致。
- Quick Look 生成的是正方形缩略图并采用放大裁切，右侧指标卡未完整进入预览；这是缩略器行为，不代表 SVG viewBox 越界，仍需用浏览器按原始 1200×420 画布复核。
- Codex 内置浏览器的安全策略禁止访问本地 `file://` 页面，因此无法直接打开 `docs/agent-system.html` 做浏览器交互预览；改用系统静态渲染与结构检查完成本地验收，并在推送后检查 GitHub 的真实渲染页面。
- Quick Look 以 1600px 渲染 `docs/agent-system.html` 成功：首屏标题、说明、三个行动按钮、五项指标和 sticky 导航均完整可见，无明显横向溢出或文字遮挡；暖色纸张、teal 强调色与卡片层级保持一致。
- HTMLParser 检查确认 `docs/agent-system.html` 标签完整闭合；README 的 3 个本地图片/文档引用均存在，MCP 示例 JSON 可被 `jq` 正常解析。
- 系统自带 `tidy` 版本过旧，按非 UTF-8/非 HTML5 语义解析中文与 `<header>` 等标签，产生误报；不作为本轮质量门禁。
- 使用 `sips` 将 `docs/readme-hero.svg` 按原始 1200×420 比例渲染后复核：标题、副标题、装饰线和四项指标全部位于画布内，中文清晰，无裁切、重叠或越界。
- 推送后 GitHub 仓库页已抓取到新版 README：中文定位、导航、核心能力、架构、快速开始和完整说明链接均来自最新提交。内置浏览器连续两次加载 GitHub 超时，因此不再重试；远端内容生效由 GitHub 页面抓取确认，视觉由本地原尺寸渲染确认。

# 2026-09-13 本地 Agent Web 控制台

- 仓库当前已有未提交改动，集中在 dogfood 日志导出、共享 slash/TUI 补全和 daemon 辅助能力；本轮必须在这些改动之上增量开发。
- 项目已存在本地 HTTP API、WebSocket 私有 RPC、独立 Session JSONL、daemon 日志和 TUI slash 注册表，可作为 Web 控制台地基。
- 当前尚未确认 HTTP 服务是否随 daemon 默认启动、Session JSONL 是否带逐消息时间，以及浏览器打开逻辑的现有实现；阶段 0 将据源码确定。
- `my-agent serve` 目前是独立前台进程：先确保 daemon，再监听 `127.0.0.1:8787`；daemon 自身只监听 Unix socket，因而 `/web` 需要单独管理 Web 进程的幂等生命周期。
- 现有 HTTP Router 只有 `/health`、OpenAI 兼容 `/v1/chat/completions` 和 `/ws`，WebSocket 已能透传任意 daemon JSON-RPC，并带 connect 鉴权与活动请求恢复。
- daemon ready marker 只记录 daemon PID/版本/工作区；RuntimePaths 尚无 Web PID/ready 信息。现有依赖中未发现浏览器打开 crate 或系统 `open` 封装。
- 当前工作树的 dogfood 能导出原始 Session JSONL 与按 session_id 过滤的 daemon 日志，这与 Web 详情需求互补，但不应要求前端解析纯文本导出。
- `Message` 目前仅持久化 role/content/tool_calls/tool_call_id/name/image_urls，没有 created_at、request_id 或 round；文件 mtime 只能给出 Session 级更新时间，无法还原每条历史消息的精确时间。
- `session.load` 已支持传入 session_id 且从 append-only 文件即时读取，`session.list` 已返回状态、活动请求数、消息数、预览和更新时间；Web Session 浏览器可直接基于这两个 RPC，无需切换 daemon 的 legacy/current session。
- `chat.send` 可显式传 session_id，事件已包含 turn/tool 阶段耗时，daemon 日志包含 request/session、轮次、Provider 和工具 telemetry；但网页若只靠现有 snapshot，历史链路无法结构化关联。
- Provider 出站消息由各 wire adapter 手动构造，因此可以给本地 `Message` 增加 `#[serde(default)]` 的审计元数据而不污染发给模型的 wire payload；旧 JSONL 可保持向后兼容。
- TUI 的 slash 输入最终集中到 `submit_input`/daemon `slash.execute`；浏览器启动属于入口本地副作用，更适合由 TUI 在识别 `/web` 后调用共享 Web 生命周期 helper，而不是让 daemon 执行桌面打开动作。
- Trace 设计确定为 `session-….jsonl.trace`，避免被现有 `is_session_name` 规则误识别成会话；记录类型覆盖 turn/model/tool 的 start/finish，并以 request_id、round 和毫秒时间关联。
- Web UI 可完全通过既有同源 `/ws` 调用 `session.new/list/load`、`chat.send`、审批和取消；只需新增 `session.trace` RPC，不需要复制一套 REST 会话协议。
- TUI 页眉第二行有稳定空间显示 `/web` 与默认地址；`TuiState` 已保存 workspace 字符串，入口可复用它调用 Web launcher。
- `DaemonClient` 克隆共享同一个 Unix transport，Web 健康轮询可安全复用 clone；daemon 断开时所有 pending 请求会收到明确的 -32000 终态。
- `my-agent stop` 会让现有 `serve` 进程失去 daemon，但当前 HTTP server 只监听 Ctrl-C，不会自动退出；需要为 Web server 增加 daemon 连接存活监视，避免留下占用 8787 的僵尸前端。
- 现有 Session 测试都使用独立临时文件并显式清理，trace 回归应同时清理 `.trace` 文件，确保列表逻辑不会把 trace 当成 Session。
- Web server 现在可每秒探测 daemon；daemon 断开时会优雅退出并释放端口。launcher 能区分健康服务、同工作区正在退出的旧服务、无服务以及被其他工作区占用四种状态。
- 定向回归确认 `session.trace` 返回 turn → 完整 model request → 完整 model response → turn completed，并且 TUI 的 21 项交互/渲染测试在增加 Web 地址和 `/web` 后全部通过。
- 静态前端不需要额外 crate 或 npm：HTML/CSS/JS 由 `include_str!` 嵌入二进制，调用同源 WebSocket，支持新建/选择 Session、流式对话、审批、取消、搜索和链路筛选。
- 隔离进程浏览器验收通过：页面自动连上 daemon、能新建 Session；用不可达的本地 Ollama 端点触发失败后，UI 清晰展示用户输入、失败 toast，以及 turn/model request/model response/turn failed 共 4 条 trace。
- 浏览器可访问性树确认链路记录含精确毫秒时间、provider、完整消息数/工具数、首字延迟入口与可展开 payload；浏览器控制台无 error/warning。
- 840px 视口下采用双栏对话 + 下方链路面板，完整页面可滚动；大屏使用三栏。移动端不再隐藏 Session，而是纵向排列，功能仍可达。
- 隔离 TUI 实测页眉显示 `Web http://127.0.0.1:18787 · /web 打开`；输入 `/web` 返回“已在运行，已打开”，进程检查确认仍只有一个 serve 实例。

# 2026-09-13 Web Agent 工作台与工作目录

- 当前架构是“一工作区一个 daemon”：RuntimePaths、SessionStore、SafetyPolicy、MCP、Memory、Plan、Cron 都在 daemon 启动时绑定 `--workspace`。
- 当前 Web 服务只持有一个 `DaemonClient` 和一个固定 workspace；因此仅在现有 `chat.send` 参数里增加 cwd 会绕过/破坏安全边界，不能满足真实的跨目录开发。
- 正确方向是保留 daemon 的单工作区隔离，让 Web 层针对所选 canonical 工作目录连接或启动对应 daemon，再把 RPC 路由到该工作区客户端。
- 当前页面虽能聊天，但三栏布局以 Session/trace 为视觉中心；需要改成“Agent 工作台 / Session 查看”两个一级视图，并把 Agent 工作台设为默认入口。
- WebSocket 当前在握手后永久绑定 `ApiState.client`；最小安全改造是让 connect 帧携带 canonical workspace，由 Web 服务的工作区路由器为该目录复用或启动对应 daemon，随后整条 socket 只绑定这一工作区。
- 浏览器原生目录选择器不会把绝对路径交给服务端，无法直接用于本地 Agent cwd；需要由同源、鉴权后的本地 API 提供只读目录浏览，并在服务端再次 canonicalize 与目录类型校验。
- Web 服务应从“单 daemon 附属进程”升级为多工作区入口：默认工作区继续服务 OpenAI 兼容 API，而 WebSocket 可按连接选择工作区；页面切换目录时重连，天然避免请求跨工作区串线。
- RuntimePaths 已用 canonical workspace 哈希隔离 socket/PID/ready/log，现有 `ensure_daemon` 可直接被 Web 工作区路由器复用；无需把 daemon 本体改为多租户。
- `/web` launcher 当前把 8787 上的健康服务限定为同一工作区，且 URL 不携带目录；需改为复用任意健康的 my-agent Web 服务，并用 `?workspace=<绝对路径>` 告知页面本次希望打开的工作区。
- 页面当前只有一个三栏视图。重构时保留既有 transcript、审批和 trace 渲染逻辑，但分别挂到默认的 Agent 工作台与独立 Session 页面，减少回归面。
- 现有 WebSocket 集成测试使用内存 DaemonClient；工作区路由器需要保留注入默认 client 的构造路径，使旧审批/断线恢复测试无需启动真实 daemon。
- `/health` 没有既有状态断言，可调整为 Web 服务自身健康状态；目录浏览复用已有 Bearer 鉴权，WebSocket 另外校验同源 Origin，避免本地页面被跨站 WebSocket 滥用。
- 首次真实浏览器加载确认默认页是“Agent 工作台”而非日志页；顶部显示 project-a 工作目录，正文突出 Agent 开发任务，右侧仅保留实时执行动态，并提供独立“Session 查看”入口。
- WebSocket 返回的 canonical 路径为 `/private/tmp/.../project-a`，页面 URL 与工作目录展示同步更新，说明 query workspace → 服务端 canonicalize → connected workspace 的链路生效。
- 目录选择器真实读取 project-a 后只显示其 `.my-agent` 子目录；点击“上一级”后准确显示同级 project-a、project-b 与 runtime，证明浏览器拿到的是服务端目录结构而非伪造的前端列表。
- 选择器同时提供默认工作区、用户目录和最近目录快捷项；路径输入、上一级与“选择当前目录”形成完整的绝对目录选择流程。
- 浏览器从 project-a 切换到 project-b 后，页眉、Agent 说明、composer 上下文和 URL 同步切换为 canonical project-b，连接状态恢复为“Agent 已连接”。
- 进程与运行文件核对显示 project-a/project-b 分别拥有独立 daemon PID、ready marker 和 `.my-agent/daemon.log`；Web 服务仍只有一个，验证了“一个 Web 入口、多工作区 daemon、安全隔离”的目标结构。
- project-b 的 Agent 请求真实到达该工作区 daemon；不可达 Ollama 触发失败后，右侧“Agent 动态”展示任务提交与明确失败原因，工作目录始终保持 project-b。
- 独立 Session 查看页显示 project-b 仅有自身 1 个 Session、1 条消息和 4 条 trace（Turn start、LM request、LM response failed、Turn failed），与 project-a 数据隔离。
- 浏览器验收发现失败系统消息会先加入对话，随后被 finally 中的磁盘快照刷新覆盖；右侧仍有失败记录，但应避免对话内错误提示瞬间消失，并为失败状态补红色视觉语义。
- “在 Agent 工作台继续”可从只读 Session 页带回同一 Session，并自动聚焦任务输入；浏览器控制台无 error/warning。
- 已修正失败后无条件重载快照的问题：成功时同步持久化快照，失败时保留对话内系统错误，同时给 Agent 状态使用红色失败语义。
- README 与系统说明已改为“Agent 工作台默认首页 + Session 独立查看 + 每目录独立 daemon”的真实模型；WebSocket 示例同步加入 workspace 字段。
- 当前差异只覆盖 Web 工作台、serve/workspace launcher、文档和规划记录；静态差异检查、rustfmt 与 JS 语法检查通过。
- 最新二进制回归确认失败系统消息会持续保留在 Agent 对话中，右侧失败状态与动态同步，浏览器控制台仍无 error/warning。
- 同一回归暴露一个空 assistant 占位：模型在首个增量前失败时草稿节点没有内容但仍被渲染为 Agent 标签；应在失败分支移除空草稿，若已有部分增量则保留。
- 空 assistant 草稿已按对象身份在失败分支移除；已有部分流式内容时不会删除，可保留模型中途失败前的有效输出。
- 浏览器创建三个空 Session 后发现它们的 `updated_at` 为 null；这类刚创建的 Session 应归入“今天”，否则用户无法折叠/展开今天分组。

# 2026-09-13 全局模型配置需求

- 当前 Provider 仅从 `API_TYPE`、`OPENAI_API_KEY`、`OPENAI_BASE_URL`、`MODEL_NAME` 环境变量构造；`main` 在启动 TUI/serve/daemon 前直接校验，因此新终端不会继承一次性 shell 配置。
- 当前 daemon 在 `build_daemon_state` 中只创建一个固定 `Arc<dyn Provider>`，LoopEngine、ContextManager、SubAgent 和 Cron 均共享该实例；Web/TUI 没有配置 RPC。
- `DaemonState.active` 已能判断活动 turn；切换模型若不想中断工作，应在有活动请求时返回冲突，再替换共享 Provider。
- 每个工作区只有一个 daemon，但 daemon 内 `sessions: HashMap` 明确支持多个独立 Session 并行；同一 Session 由 SessionStore/LoopEngine 的 turn lock 串行。
- `/models` 最小兼容方案是 slash 列出配置并接受编号/ID，避免为 TUI 增加新的交互状态；Web 通过独立 RPC 提供下拉与编辑表单。
- 配置无效或缺失时 Web `serve` 不能依赖 daemon 启动；因此新增无 daemon 的设置页启动路径，`/api/models` 先落盘配置，再按所选工作区启动/切换 daemon。
- Web 模型配置 API 需要携带当前 WebSocket 所选工作区，否则切换跨目录页面时会误应用到 serve 默认目录；请求现带 `workspace` 并由 WorkspaceRouter 再次 canonicalize。
- `ConfigStore::upsert` 对空 API key 保留原 profile 的已保存 key，避免编辑远程模型时清空密码；所有响应只返回 `has_api_key`。
- 日期分组按 ISO 日键降序排列；无时间戳的新 Session 回退到当天，真正无法解析的历史值仍归入“日期未知”。
- 最新嵌入资源的浏览器回归确认三个新建空 Session 均归入“今天”，今天分组默认展开并正确显示数量；点击日期标题后 Session 条目全部隐藏，日期标题保留。

# 2026-09-13 Session 日志帧超限修复

- 用户反馈 Web 页面无法查看 Session 日志和链路；截图错误为“协议帧大小 7643140 字节，超过 4194304 字节限制”。
- 根因确认：前端 `inspectSession` 并行调用 `session.load` 与 `session.trace`，后端 handler 分别把完整消息数组/完整 trace 数组序列化为单个 JSON-RPC 响应；daemon 协议层 `MAX_FRAME_BYTES` 固定为 4 MiB，因此任一大响应都会让 `Promise.all` 失败并阻断整页。
- `SessionStore::load`/`load_trace` 当前会完整读入内存；新增分页 RPC 时应先限制响应帧，再视需要优化文件读取，不能通过提高全局帧上限掩盖问题。

# 2026-09-13 三档 Agent 权限模式

- 当前所有生产工具的审批入口集中在 `SafetyPolicy`：文件读/写/编辑、shell 命令和 MCP 外部动作均经同一实例；子 Agent 复用工具 Arc，因此切换模式可自然覆盖 Agent 工作。
- 现有默认行为等同“帮我批准”：工作区内读写放行，工作区外写入、风险命令和 MCP 外部动作需要审批，工作区外读取拒绝；灾难性命令始终硬拦截。
- 新增模式语义：`request_approval` 对工作区内写/编辑以及可识别的联网命令也询问；`risk_approval` 保持现有风险边界；`full_access` 放行工作区外读取、风险命令和 MCP 审批，但仍拒绝灾难性命令。
- SafetyPolicy 模式以共享 `AtomicU8` 存在于 daemon 生命周期内，Web RPC 与 TUI slash 通过同一 daemon 即时同步；Cron 使用独立安全策略，不受交互模式影响。
- Web 顶部权限菜单采用用户截图的三项文案和选中态；TUI 新增 `/permissions [request|risk|full]`，`/permission`、`/mode` 为别名。
- 当前 WebSocket 的 `chat.send` 已通过 `text_delta` 增量事件驱动前端；问题在于前端每个增量都重建整个 transcript，且 `renderMessage` 使用 `escapeHtml`，所以 Markdown 不会解析。
- Agent Session 快照中的 `tool_calls` 与 `tool` 角色消息会被直接渲染；右侧 `activities` 也会展示工具状态。本轮应保留数据和状态，但默认通过折叠容器隐藏详情，审批卡仍保持可见。
- 项目没有前端打包/npm 依赖；应在 `web/app.js` 内实现无外部依赖的安全 Markdown 渲染器，白名单 HTML 标签并限制链接协议，避免引入供应链或 XSS 风险。
- 流式响应不能依赖“完成后重新加载快照”才能显示；当前 `text_delta` 已更新草稿对象，需确保渲染不会因快照刷新覆盖增量，并对长文本减少不必要的滚动/重绘开销。
- 当前 `ProviderEvent` 只有 `TextDelta` 和工具调用事件，OpenAI/Anthropic/Ollama 的思考字段均未透传：OpenAI delta 只读 `content`，Anthropic 只处理 `text_delta`，Ollama 只读 `message.content`。
- 可兼容地新增 `ThinkingDelta`：OpenAI 读取 `reasoning_content`/`reasoning`/`thinking`，Anthropic 读取 `thinking` content block 与 `thinking_delta`，Ollama 读取 `message.thinking`/`message.reasoning`。
- 思考内容需写入 assistant Message 的可选字段，才能在 `chat.send` 完成后的快照刷新和历史 Session 查看中保留；Provider 出站序列化会忽略该本地字段。
- daemon 事件应新增 `thinking_delta` 与 `thinking_finished`；LoopEngine 在首个正文增量前发出 finished，前端据此自动关闭思考折叠区。
- OpenAI Chat Completions 的增量思考字段可从 `delta.reasoning_content`（兼容 `reasoning`/`thinking`）读取；Anthropic 在 thinking content block 起始和 `thinking_delta` 中分别携带片段；Ollama 在 NDJSON `message.thinking` 或 `message.reasoning` 中携带片段。
- LoopEngine 的 provider 事件消费必须在同一异步 select 中转发增量，不能只在 provider future 完成后读取；`thinking_finished` 在正文首片段前或 provider 结束时补发，确保错误/工具调用也不会留下打开状态。
- Session Message 增加可选 `thinking` 字段并带 serde 默认值，老 JSONL 无需迁移；各 provider 出站 wire serializer 不发送该本地审计字段。
- Web 自定义 Markdown renderer 在识别代码围栏前识别“表头 + 分隔线”结构，表格单元格使用同一安全 inline renderer；分隔线至少三个连字符并支持冒号对齐标记。
- 真实隔离 WebSocket 流测得思考首片段约 30ms 到达、第二片段约 1.5s、`thinking_finished` 与正文首片段约 3.0s、最终完成约 5.4s，证明页面可以观察到真实增量时序。
- 隔离浏览器可访问性快照在思考阶段显示“思考过程 生成中”和“正在生成思考/正在生成”光标；后续快照显示“思考过程 已自动折叠”、标题和语义 `<table>`，表格不再以竖线文本呈现。

# 2026-09-13 Web 工作台布局优化

- Agent 工作台标题区原先额外显示英文眉题，占据首屏垂直空间；仅在 Agent 视图移除，Session 记录页的上下文眉题继续保留。
- `.agent-layout` 改为左侧 `minmax(0, 1fr)`、右侧 `minmax(250px, 290px)`，右栏继续承载工作目录、Session 入口和实时活动；桌面 1440px 视口实测左侧约 1102px、右侧约 290px。
- assistant 与 tool details 消息覆盖默认 86% 最大宽度，模型输出卡片和工具折叠条可利用完整 transcript 内容宽度；用户消息仍保持右对齐的紧凑气泡。
- 标题区与 transcript padding 收紧，减少工具/消息之间的空白，同时保留 820px 单列断点，窄屏下模型输出仍占据主要可视区域。

# 2026-09-13 Web 工作台截图标注优化

- 截图红色标注对应三处正式 UI 意图：顶部操作区使用“＋新建任务”；工作目录与权限模式靠近 composer；中间 transcript 明确作为模型响应区域。
- 保留 workspace-label、permission-label 和两个 trigger 的既有 DOM ID，移动节点不会影响 setAgentControls、目录切换、权限 RPC 或禁用状态。
- 顶部选择器移动后，composer context 在桌面横向排列，在 820px 以下自动换行；390px 视口仍能显示“工作目录 / 权限模式”标签和当前值。
- 通过隔离模拟 Ollama 实际发送一轮任务，页面在思考增量期间显示“生成中”，随后自动折叠思考并显示 Markdown 表格响应，证明本轮只改变布局和提示文案。

# 2026-09-13 参考 pi 的 TUI 与 Agent 架构优化

- pi 是多 package TypeScript monorepo；本轮重点目录为 `packages/tui`（终端组件/输入/渲染）、`packages/coding-agent`（交互会话与应用编排）和 `packages/agent`（模型-工具循环）。
- 当前项目是单 Rust binary crate，TUI 位于 `src/entry/tui.rs`、`src/entry/tui/*`，Agent 主循环位于 `src/loop_engine.rs`，daemon 负责 session、审批、取消与活动请求真相。
- pi 根级 `AGENTS.md` 要求宽泛改动前完整阅读文件；后续读取相关目标文件全文，不依据零散搜索片段直接设计。
- 当前工作树在恢复长期规划文件后为干净状态；pi 仓库只读。
- pi 将通用 TUI 库与 coding-agent 应用层分离：`packages/tui` 提供差分渲染、布局、Editor、ScrollView、overlay、keybinding 等基础能力；`packages/coding-agent/modes/interactive` 负责 Agent 会话投影与交互编排；`packages/agent` 独立承载模型/工具状态机。
- 当前 Rust 项目仍是单 crate，相关模块体量已经较大：`entry/tui.rs` 1383 行、`entry/tui/view.rs` 1252 行、`loop_engine.rs` 1823 行、`daemon/handlers.rs` 1771 行。是否拆模块应依据职责耦合和测试边界，而不是照搬 monorepo。
- pi 的 TUI 核心实现规模也很大，需优先阅读 `tui.ts`、main/alt screen、Editor/ScrollView 和 coding-agent 的薄适配层；Agent 流程优先阅读 `agent-loop.ts`、`agent.ts`、types 与 runtime service，避免被 6000+ 行 interactive mode 的细节淹没。
- pi TUI 自述定位是“差分渲染的终端 UI 库”；当前 Rust 项目使用 ratatui/crossterm，差分缓冲已由依赖提供，因此本轮关注上层状态/组件/交互设计，不重复造底层 renderer。

## 待确认

- pi TUI 的组件树、增量重绘、宽字符输入、快捷键和 overlay 设计中，哪些能力当前项目仍缺失。
- pi agent loop 的事件模型、消息变换、工具调度、取消/错误与 steering/follow-up 流程，哪些适合映射到 daemon 协议。

## 当前 Rust TUI 基线

- `TuiState` 同时持有领域投影（消息、工具、turn、审批）、视图状态（scroll、overlay、theme、render cache）和输入/快捷键状态；`tui.rs` 还同时承担终端生命周期、事件泵、RPC frame reducer、slash 与提交队列，职责耦合明显。
- 已有优点不应重做：`UiMessage`/`UiToolCall` 结构化投影、稳定 message id/content version、按宽度/主题缓存、follow-bottom/unread、队列发送、多活动 turn、审批队列、终端 Drop 恢复和 bracketed paste 都已实现。
- 当前事件泵每 16ms 调用同步 `crossterm::event::poll`，随后最多 128 次对每个 RPC stream 做 1ms timeout 轮询；这使终端输入、网络流和动画依赖固定轮询，空闲时也持续唤醒。若 pi 使用失效调度/统一事件源，这是高价值对照点。
- 快捷键散落在 `handle_key` 的条件分支中（F1/Ctrl+/、Ctrl+T/U/K/C、编辑、滚动、审批），缺少动作级 keymap；pi 根规则明确要求默认快捷键进入集中 keybinding 表，这很可能适合移植为 Rust `Action` 映射。
- `ActivityPhase` 目前是单个全局枚举，而状态允许多个 active turn；任何一个 ToolFinished 都会把 phase 改回 WaitingModel，无法准确表达并发 turn 的综合活动状态。后续需检查 daemon 是否实际串行以及视图是否只需要聚合状态。
- ThinkingDelta 当前只更新状态文案，不把思考内容投影为 TUI message；这是 Web 已支持而 TUI 尚未展示的能力，但是否本轮实现要结合 pi 的 loader/thinking 交互。
- `view.rs` 已有响应式布局、窄屏兜底、审批/slash/help overlay、三主题语义色、CJK 列宽换行和 TestBackend 回归，视觉基础比简单 demo 完整。
- 每次绘制会 `clone()` 整个 `messages`，再对每条缓存行进行 clone；流式输出时最后一条消息的 content_version 每片段变化，而历史消息仍被整体复制。`wrap_lines` 又把每个 Unicode 字符拆成独立 `Span`，长会话会造成明显分配/克隆压力。
- `draw_ui` 为处理锚点会直接 `take()` 并修改 scroll/follow-bottom/unread/cache 等状态，模型投影与布局计算难以独立测试。适合逐步引入 pi 风格的组件/视图模型边界，但不宜一轮重写所有渲染。
- 渲染缓存 key 覆盖 message/version/width/tools/theme，正确性较好；容量达到 512 时整表清空，旧 content_version 项在阈值前保留。可考虑以 message id 定向失效或用稳定组件缓存替代全表清空。
- 帮助文案与 `handle_key` 是两份手工维护的快捷键真相；集中 `Action -> bindings -> help` 可同时降低漂移和提高可配置性。

## 当前 Rust 输入与 Agent 循环基线

- `InputEditor` 使用 `Vec<char> + scalar cursor`，能避免 UTF-8 字节切割并正确计算 CJK 显示列；但中部插入/删除为 O(n)，只支持左右/行首尾/空白分词，缺少多行上下移动、撤销/重做、kill ring。是否补齐要对照 pi Editor 的能力与复杂度，优先选择用户可感知且测试容易的子集。
- `LoopEngine` 已具备不少成熟设计：会话 turn 锁、协作取消、流式 thinking/text、严格 tool-call 装配与整批准入、连续只读并行/副作用串行、重复/失败熔断、长任务检查点、图片瞬态上下文和结构化 trace。
- Agent 生命周期事件是闭合但较粗的 `AgentEvent`：TurnStarted、thinking/text delta、tool started/finished、TurnCompleted；错误和取消没有独立终态事件，由上层 future/RPC error 表达。TUI reducer 因此要同时解释事件流与 response。
- 当前 Agent loop 把编排、事件生成、持久化、trace、provider 流装配、工具调度、重复检测集中在 1823 行单文件中；生产实现约 900 行，后半主要是高质量回归测试。拆分的合理方向是内部职责模块，而不是改变对外 `LoopEngine` API。
- TUI 的排队消息只存在客户端内存，Agent 内核没有 pi 式 steering/follow-up 概念；活动任务期间的输入不会进入当前模型循环，只会在完成后作为新 turn。需对照 pi 的明确语义后决定是否值得扩协议。
- 当前 `execute_in_waves` 仅并行“相邻的只读调用”，用 `buffered(8)` 保持结果顺序；这比无序并行更利于 tool_call_id 回填，应该保留。

## pi TUI：快捷键、滚动与输入

- pi 用声明式 `Keybinding -> defaultKeys + description` 注册表和 `KeybindingsManager` 解析默认/用户覆盖、去重与冲突；组件只匹配语义动作。这个设计可在当前项目先落一个无配置的 Rust 精简版：集中定义 `TuiAction` 与匹配/帮助元数据，再让 `handle_key` 消费动作，后续配置化不必再改业务逻辑。
- pi 的 `ScrollView` 明确区分 follow-end、用户在末尾抑制 follow、content/viewport 尺寸、overscroll chain/contain，并让 `scrollBy` 返回未消费行数；当前项目的 `scroll + follow_bottom` 已覆盖主要语义，但布局计算散在 `draw_ui`。现阶段可借鉴状态不变量与测试，暂不需要复制 scrollbar/timer。
- pi 的 `EditorComponent` 是小而稳定的可替换接口，使应用层可以注入 Vim/Emacs 等编辑器；当前项目没有扩展系统，直接引入 trait 收益暂不足，但应把 InputEditor 保持为独立模块并避免 TUI 应用逻辑进入其中。
- pi 输入组件按 grapheme cluster 移动/删除，当前 Rust 按 Unicode scalar，面对组合字符、ZWJ emoji 会拆坏用户可见字符。这是明确的正确性差距，Rust 可通过 `unicode-segmentation` 修复并补回归。
- pi 已实现 undo coalescing、delete word forward、delete to line start/end、kill/yank/yank-pop；当前项目的 Ctrl+U 是整框 clear、Ctrl+K 被占作清空发送队列。可优先补“按字素删除/移动”和多行编辑常用键，避免直接改变现有 Ctrl+K 队列语义造成兼容问题。
- pi 词导航使用 `Intl.Segmenter` 并单独处理标点，优于当前只按 whitespace 分词；Rust 若引入复杂分词会扩大依赖/语义差异，本轮更适合先修字素安全。

## pi TUI：核心渲染与焦点

- pi 把所有界面单元抽象为 `Component(render/invalidate/optional input/mouse)`，`Container` 只做组合；overlay 有独立栈、focus owner、pre-focus 恢复、可见性与 bounds。当前项目只有单个 help bool + 固定审批/slash 区，短期无需完整 overlay framework，但可把 help/approval 输入优先级显式化为模式/action reducer。
- pi 的关键性能策略不是盲目循环，而是 `requestRender` 合并失效、16ms 最大帧率节流；键盘输入走 immediate render 降低延迟。当前项目 `dirty` 也避免每圈重绘，但 RPC 读取仍用每 stream 的 1ms timeout 轮询，可能把一轮延迟叠加到活动流数量上。
- pi 终端输入、resize 与组件 `requestRender` 都是事件驱动；当前可先将 RPC stream 的“等待 1ms”改成单次非阻塞 poll，避免最多 128ms 的串行超时成本，再评估引入 crossterm EventStream 的完整 select。
- pi 集中处理终端 query 回复、key release、鼠标坐标转换、IME 硬件光标和 overlay focus；ratatui/crossterm 已替当前项目承担不少底层工作，不应复制 ANSI compositor。
- pi 的 overlay 与 component 系统代码复杂度很高（仅 `tui.ts` 1456 行），说明“拆组件”本身不是免费收益；当前项目适合先抽纯状态 reducer/keymap，并用已有 ratatui Widget 继续渲染。

## pi TUI：屏幕与聊天布局

- pi 的 main-screen 自研行级 diff、同步输出、超大写入分块、Kitty 图片与 scrollback 对齐；当前项目运行在 alternate screen 且 ratatui 已维护前后 buffer，因此不应迁移这段终端 renderer。
- coding-agent 的 `createChatViewport` 非常薄：transcript 是唯一 grow 区，pending/status/editor/footer 组成可收缩 dock。当前 `draw_ui` 已基本采用同一布局原则，但将所有区块计算写在一个函数里；可抽 layout 计算纯函数以提高边界测试，不需要引入组件框架。
- pi fullscreen 把 transcript search、滚动条、文本选择、OSC8 链接等放在通用 TUI 层，而应用层只提供主题和回调。这些是长期演进方向；本轮最值得低成本借鉴的是“按上一/下一用户 prompt 跳转”的语义滚动，因为当前已有稳定 message id/锚点，可用集中 keybinding 接入。
- pi fullscreen 的 PageUp/PageDown 留 4 行重叠、滚轮/逐行/半页动作明确；当前固定 PageUp 8 行在不同终端高度下体验不一致。可改为按 transcript viewport 高度减重叠滚动，但需要把上次 viewport height 放入状态或由事件处理计算。
- pi 能在退出 alternate screen 时把最终文档重绘回主屏；当前项目退出后清空 TUI，不保留会话输出。这是体验差异，但会改变既有 alternate-screen 语义，优先级低于编辑正确性和事件循环。

## pi Agent loop：前半

- pi 明确分离 `AgentMessage` 与 provider `Message`，只在模型调用边界做 transform/convert；当前项目的 `provider::Message` 同时是会话领域模型和 wire 近似模型，导致 thinking/image/tool 字段需要各 provider 小心忽略。长期可引入领域消息层，但本轮全面迁移风险较大。
- pi 事件生命周期更正交：agent_start/end、turn_start/end、message_start/update/end、tool_execution_start/update/end；事件携带完整 partial message，TUI/其他消费者只做投影。当前事件较少但够用；最明显缺口是 error/aborted 没有显式事件终态。
- pi 在流式开始时把 partial assistant message 放入 context，后续就地替换，最终 `message_end`；当前项目只在 Provider 完成后记录 assistant message，崩溃/断开时已流式展示的正文不会进 session。若要提高恢复一致性，应设计“草稿事件/trace”，不能简单对每 delta append JSONL。
- pi 把 steering（下一次模型调用前注入）与 follow-up（Agent 原本将停止后再启动下一 turn）分开，并在耗时 `prepareNextTurn` 后重新取 steering；当前 TUI 只有 follow-up 队列且在客户端，语义应至少命名清楚，若扩展 steering 需 daemon 成为队列真相源。
- pi 有 `prepareNextTurn`、`shouldStopAfterTurn`、transformContext 等 hook，使 compaction/模型切换/策略可插拔；当前 Rust 直接调用 ContextManager 和固定循环。可以先抽内部 `TurnStep`/终态 helper，而不为了扩展性预先引入大量 trait。
- pi 对 provider stopReason=`length` 的整批工具调用 fail-closed，避免截断 arguments 被 salvage 后误执行；当前严格 assembler 能拒绝无效 JSON，但若截断后 JSON 恰好合法且缺失字段，schema admission 可能拦截，仍应核对 provider 是否保留 finish_reason 并作整批拒绝。
- pi 工具调度默认并行，任一工具声明 sequential 则整批串行；当前“只读并行、副作用串行”安全策略更保守，更适合本项目，不建议改成 pi 默认。

## pi Agent loop：工具与类型契约

- pi 把工具处理拆为 prepare（查找、参数适配、schema 验证、before hook）、execute（支持 partial update）、finalize（after hook）、message artifact 四步；当前项目 ToolRegistry 已负责查找/准入/执行，但 `LoopEngine::execute_one` 同时做事件、trace、错误归一化、fingerprint。可抽内部 helper/模块，降低主循环认知负担。
- pi 的事件 sink 可异步等待，从而严格保证 start/update/end 顺序和下游落盘完成；当前用 unbounded channel，不会反压 Agent，但慢消费者可能积压。对本地单用户 Agent 可接受，避免贸然让 TUI 速度阻塞模型；trace 继续由 LoopEngine 直接落盘更可靠。
- pi 工具支持 partial result update 与 structured details；当前工具只在完成时产生 `ToolOutput`，长命令的 TUI 无实时 stdout。这个能力价值高但涉及 Tool trait、exec 子进程读取、daemon 协议与多入口，适合后续独立批次而非顺手扩张。
- pi `AgentState` 明确定义 isStreaming、streamingMessage、pendingToolCalls、errorMessage 等派生运行态；当前 TUI/daemon 通过多个集合和单个 ActivityPhase 自行推断。可在 TUI 先用纯函数从 active turns + approvals + running tools 派生聚合 phase，避免事件顺序覆盖状态。
- pi 的 AgentTool 有 replay safe/never 与 per-tool executionMode，反映持久恢复与并行策略属于工具元数据；当前仅 `is_read_only`。若未来做崩溃后工具恢复，应新增 replay policy，而不是把工具默认重放。
- pi 将 stopReason=error/aborted 编码在 final assistant message，`agent_end` 仍正常闭合；当前 Rust 用 `Result` 返回失败更惯用，但协议事件可以补 `TurnFailed { cancelled, error }`，让所有入口不必从 RPC error 反推终态。

## 当前 Provider 停止原因核对

- 全仓 `finish_reason/stop_reason` 搜索表明当前三个 provider 都未把停止原因传入 `ProviderEvent`/`Response`；OpenAI 兼容 SSE 的 `finish_reason`、Anthropic `message_delta.stop_reason`、Ollama `done_reason` 均会丢失。
- 因此当前确实无法实现 pi 的“输出因 token 上限截断时整批工具调用 fail-closed”：如果截断恰好形成合法 JSON 且满足 schema，工具可能执行。这是比纯结构拆分更优先的 Agent 流程安全改进。
- 合理的最小设计是新增 provider 层统一停止原因事件/枚举，在 assembler 完成前把 `length/max_tokens` 转成 `ToolCallFailed(code=output_truncated)`；若本轮只有文本而无工具调用，仍允许返回已生成文本并在 trace/日志标记，避免无谓破坏现有行为。
- 实施前仍需完整读取 `tool_calls.rs` 和三个 provider 的流解析文件，确认事件顺序及各协议停止字段。

## Provider/Assembler 完整阅读结论

- `ToolCallAssembler` 已是严格单一装配点，并在任何 `ToolCallFailed` 后保留已收到文本但最终返回 assembly failure；新增 `OutputTruncated` 事件最适合由 assembler 在 `finish()` 时根据 `calls.is_empty()` 决定是否失败，provider 无需感知完整调用状态。
- OpenAI `StreamChoice` 当前只反序列化 `delta`，可增加 `finish_reason: Option<String>`；当值为 `length` 时发 `OutputTruncated`。事件通常在 tool delta 后、`[DONE]` 前到达，随后 `complete_calls` 再发 Completed，assembler 可保留首个截断失败。
- Anthropic `message_delta` 当前只记录 usage，实际停止原因位于 `delta.stop_reason`；值 `max_tokens` 应发截断事件。tool block 往往已 content_block_stop，因此不能依赖 `StreamState.tools` 判断是否曾有工具，交给 assembler 判断正确。
- Ollama `OllamaChunk` 已解析 `done`，可增加 `done_reason`；值 `length` 发截断事件。工具调用在同一/之前 chunk 已进入 assembler。
- 对无工具的截断文本，assembler 应继续返回 `Response::Text`，保持 pi 的语义；对任意已开始/完成工具调用则返回 `ToolAssemblyFailed(code=output_truncated)`，LoopEngine 现有“回填系统纠错消息并有限重试”路径可直接复用。
- `ProviderEvent` 是内部非序列化 enum，新增变体不会破坏 daemon wire 协议；需要同步 `emit_legacy_response`、assembler match、LoopEngine `consume_provider_event` 的穷尽匹配及三 provider 单测。

## pi Agent 状态包装与当前基线

- pi 的 `Agent` 将低层 loop 包成唯一 activeRun，集中拥有 abort、waitForIdle、steering/follow-up queue，并用 event reducer 维护 streamingMessage/pendingToolCalls/errorMessage；异常也合成为完整 message/turn/agent 终态。当前 daemon 已承担 active request/取消/会话真相，因此不应再在 `LoopEngine` 外复制一个 Agent wrapper，但 TUI 应从集合派生聚合运行态。
- pi 的 session runtime 在切换/新建/fork 前先 `abort()` 并等待旧 turn settle，再 teardown/rebind，避免结果写入新 session。当前 daemon 已禁止活动 turn 时切换 session，这个策略更简单安全，应保留。
- pi 本地 provider 映射再次确认：OpenAI `finish_reason=length`、Anthropic `stop_reason=max_tokens` 都统一为 stopReason `length`，支持本轮映射设计。
- 当前全量基线为 `cargo test --all-targets` 143/143，通过且耗时约 1.36s。

## 本轮选定落地范围

- TUI：新增集中式语义 `TuiAction` keymap，让处理逻辑和帮助文案共享注册表；不在本轮做用户配置文件，先消除双真相。
- TUI：输入编辑改为 grapheme cluster 安全，覆盖组合音标和 ZWJ emoji；保留现有快捷键语义。
- TUI：每个 active turn 独立记录 phase，顶部状态从 pending approvals/running tools/active turns 派生，修复并发事件互相覆盖；RPC stream 轮询改为单次非阻塞 poll，消除每流 1ms 累加等待。
- Agent：Provider 统一发出输出截断事件；assembler 只在截断响应包含工具调用时整批 fail-closed，无工具的截断文本保持兼容；LoopEngine 复用既有有限纠错重试。
- 结构：新增 `entry/tui/keybindings.rs`，保持 provider 协议适配分文件与 assembler 单一边界；不做单 crate→workspace 或大规模搬文件，因为当前 daemon/入口共享内部类型，机械拆 crate 会增加公开 API 面而缺少直接收益。
- `unicode-segmentation 1.13.3` 已由 ratatui 间接锁定在现有 `Cargo.lock`，将其声明为直接精确依赖不会引入新下载或锁文件版本漂移。

## 实施与最终结论

- 集中式 `TuiAction` 注册表已成为按键解析和帮助弹层的共同真相源；保留原快捷键并增加 Alt+左右、Ctrl+J、Ctrl+D、Alt+Backspace 等 pi 风格别名。后续若要支持用户自定义，只需替换绑定来源，不再改业务 reducer。
- `InputEditor` 已改为 UTF-8 字符串和字素边界光标；组合字符、ZWJ emoji 及中部插入后边界吸附均有测试，末尾输入保持 O(1) 快路径。
- TUI phase 已从“最后一个事件覆盖全局枚举”改为 per-turn facts + 聚合优先级；审批、工具执行、流式、等待和恢复不会再被并发 turn 相互覆盖。RPC stream 改用公平轮转的非阻塞 poll，移除每流 1ms 串行等待。
- OpenAI `finish_reason=length`、Anthropic `stop_reason=max_tokens`、Ollama `done_reason=length` 已统一为 `OutputTruncated`；canonical assembler 对含工具的截断响应整批拒绝，LoopEngine 走原有有限重试，纯文本截断保持兼容。端到端回归证明合法 JSON 的副作用调用也不会执行。
- 结构上只抽出高内聚的 `entry/tui/keybindings.rs`，没有照搬 pi 的 monorepo 或自研 ANSI renderer。现有 ratatui、daemon 单一真相源与 provider/assembler 分层继续保留。
- 后续若继续演进，优先级建议为：① 将 `loop_engine.rs` 的工具波次/trace/provider round 拆成内部模块；② 把客户端排队升级为 daemon-owned follow-up/steering；③ 为长工具增加 partial output 事件；④ 最后再评估领域 Message 与 provider wire Message 分离。上述项目均涉及公共协议或恢复语义，不适合在本轮低风险优化中顺带改动。
- 最终门禁：`cargo fmt --all -- --check`、`cargo check --all-targets`、严格 Clippy、`cargo test --all-targets`（153/153）、`cargo build --release`、`git diff --check` 全部通过；pi 仓库保持未修改。

## Web 前端体验优化结论（2026-09-13）

- 现有 Web 页面已经具备完整的 Agent、Session、模型、权限和目录能力，本轮不改变 RPC 或 DOM ID，只改善状态连续性与反馈层。
- 流式 transcript 现在区分“用户仍在底部”与“用户正在查看历史”：前者自动跟随，后者保持滚动位置并提供“回到最新”入口。
- 审批请求进入前端 state，重绘、切换视图和流式增量不会丢失审批卡；提交审批时按钮会锁定，失败可恢复。
- composer 增加自适应高度、字数提示、明确的 ARIA 描述；连接失败提供可见的“重试连接”；状态 pill 增加文字之外的动态指示，并支持 reduced-motion。
- 浏览器静态回归确认窄屏 684px 下 `scrollWidth=684`，无横向溢出；控制台无 error/warning。前端改动通过 Node 语法检查，Rust 嵌入资源通过全量测试和 release 构建。

## 2026-09-26：20 项体验问题审计

- 起点：`main` 工作树干净；上一轮 P4 已推送。仓库已有 `task_plan.md`、`findings.md`、`progress.md` 长期记录，本轮只追加新章节。
- 待完成：扫描 CLI、TUI、Web、配置与恢复的实际交互，建立 20 项有源码证据的清单，再实施。
- Web 初审发现可复现候选：连接握手超时未关闭 socket、握手失败需等待满 8 秒、连接后附属数据读取失败被误判为断线；运行中模型切换仍被前端阻止；Session 快照竞态可用旧请求错误覆盖新选择；全文重绘会丢失 `<details>` 展开状态和文本选择；Session 翻页重绘会跳到底部；“回到最新”在任务结束后消失。待与后半段事件处理及 HTML 核对后定稿。
- 后半段确认：`sendPrompt` 在等待创建 Session 时无提交锁，重复提交可并发；输入被提前清空，失败不恢复；WebSocket 断开会把未确认 run 直接显示为“任务失败”，丢失恢复入口；停止按钮用 socket 请求 ID，缺 run ID；同一轮可以重复发送停止。目录选择在连接成功前清空旧工作区，目录请求和 Session 请求都缺过期结果防护。trace 摘要仅统计已加载页，却显示为全量总数。输入“字数”按 UTF-16 长度计。CLI REPL 的 `run_chat` 错误直接结束交互循环。下一步核对协议与 CSS，再固定 20 项。

### 本轮固定清单（按用户操作路径排序）

| # | 可复现的体验问题 | 修复目标 |
|---:|---|---|
| 1 | WebSocket 握手超过 8 秒只 reject、不关闭连接 | 超时关闭 socket，迟到帧不能反转状态 |
| 2 | 握手前 socket 关闭时 Promise 悬挂 | 立即报告连接失败 |
| 3 | 已连接后 Session 列表读取失败被误报为连接失败 | 连接状态与辅助读取分别处理 |
| 4 | 运行中断线只能手工点重试 | 自动有限重连并显示状态 |
| 5 | 断线把仍在 daemon 执行的任务标成失败 | 按 run/session 恢复事实状态 |
| 6 | 首次发送等待新建 Session 时可重复提交 | 提交锁从函数入口生效 |
| 7 | 发送失败后用户输入已清空 | 恢复尚未确认提交的草稿 |
| 8 | 停止任务依赖本 socket 的请求 ID | 优先用稳定 run ID 精确取消 |
| 9 | 停止按钮可重复点并重复发请求 | 停止请求中禁用并明确反馈 |
| 10 | 运行中“使用模型”静默无效 | 热切换并提示生效范围 |
| 11 | 运行中保存模型被前端旧限制阻拦 | 允许保存并提示生效范围 |
| 12 | 切换工作区失败仍清空当前 Session | 连接成功后才切换可见状态 |
| 13 | 快速切换目录时慢响应覆盖新目录 | 只接受最后一次请求结果 |
| 14 | 快速切换 Session 时旧请求错误覆盖新 Session | 所有成功/失败路径都做会话一致性检查 |
| 15 | 查看历史 Session 翻页后跳到底部 | 保留滚动锚点 |
| 16 | 流式重绘折叠已展开的消息详情 | 保留详情展开状态 |
| 17 | 任务完成后“回到最新”按钮消失 | 只按滚动位置决定显示 |
| 18 | Emoji 的“字数”按 UTF-16 双算 | 按字素簇统计 |
| 19 | CLI 一次聊天错误就退出 REPL | 显示错误并继续下一条输入 |
| 20 | TUI 任务已提交但队列查询失败却报操作失败 | 队列计数作为尽力读取 |

### 逐项修复与验证

- 1–4：`RpcSocket` 对超时、握手前关闭和迟到帧只结算一次，超时主动关闭 socket；工作区握手与附属数据读取解耦；活动连接断开后最多重连 5 次。前端回归覆盖超时、关闭、列表失败和重连计时。
- 5：前端保留活动 run ID 与事件序号，断线后读取当前 Session 的活动请求并以 `agent.subscribe(after_seq)` 续接；若请求已结束，重新读取持久化 Session。断线只显示“确认任务状态”，不会误报为任务失败；回归覆盖恢复与列表刷新竞态。
- 6–9：发送入口在 `session.new` 前加提交锁；socket 未接受时保留输入并撤销乐观消息；停止优先以 `run_id + session_id` 精确定位，停止请求中禁用按钮。回归覆盖重复提交、失败输入、精确取消和重复点击。
- 10–11：允许运行中激活与保存模型配置，提示“当前任务沿用原模型，后续任务使用新模型”；回归覆盖两个操作。
- 12–14：连接新工作区成功后才重置旧视图；目录浏览只接收最新请求；Session 快照与错误分支均核对所选 ID。回归覆盖连接失败保留旧状态、目录竞态和 Session 竞态。
- 15–18：只读历史重绘保留滚动位置，详情展开状态跨流式重绘保存，“回到最新”由实际滚动位置决定，字数按 Unicode 字素统计。回归覆盖滚动、详情、按钮和组合 emoji。
- 19–20：CLI 单次聊天错误打印后继续 REPL；TUI 任务已提交后队列查询失败只提示队列状态不可用，不再把任务判为操作失败。通过 Rust 全量测试与严格 Clippy 验证编译和现有契约。
- 本机真实浏览器复核：在仅用于进程的 Ollama 占位配置下，Web 工作台成功连接并读取历史 Session，输入可用；初始无模型配置时仍会清楚显示连接失败和重试入口。测试后已关闭临时服务、浏览器页并请求 daemon 停止。未运行真实模型推理或外部 Provider。
- 验证：`node --test web/app.test.cjs` 16/16；`node --check web/app.js`；`cargo fmt --all -- --check`；`cargo clippy --all-targets --all-features -- -D warnings`；`cargo test --all-targets` 200/200；`cargo build --release`；`git diff --check` 全部通过。

## 2026-09-26：再次审计 50 项

- 基线：`main` 工作树干净；上一轮 20 项已提交。此轮从未覆盖的路径重新审计，避免重复记数。
- 下表逐项给出触发路径与源码位置；修复按路径归组，验证记录见表后。

### 已确认的 50 项独立问题（本轮）

| # | 触发与当前表现 | 位置 |
|---:|---|---|
| 1 | 自动重连倒计时期间无法手动立即重试 | `web/app.js:setConnection` |
| 2 | 恢复快照读取失败后输入长期锁定 | `recoverAgentTurn` |
| 3 | 同一 Session 多个活动任务时恢复长期锁定 | `recoverAgentTurn` |
| 4 | 已知 run 与唯一活动请求不匹配时误订阅别人的任务 | `recoverAgentTurn` |
| 5 | 断线后失败或取消的 run 都显示成泛泛的“已结束” | `recoverAgentTurn` |
| 6 | 断线后终态快照只取最早 60 条，末尾结果可能不可见 | `recoverAgentTurn` |
| 7 | 订阅返回 `subscribed:false` 时显示“待确认”，却放开输入 | `recoverAgentTurn` |
| 8 | 并发恢复入口可能对同一 run 发起两个订阅 | `connect` / `sendPrompt` |
| 9 | 恢复时多个待审批项只显示第一项 | `recoverAgentTurn` |
| 10 | WebSocket 断开后审批按钮仍可点击并报 RPC 错误 | `setAgentControls` / `renderApprovalCard` |
| 11 | 正常打开长 Agent 会话也只显示最早 60 条 | `loadAgentSession` |
| 12 | 从 Session 查看继续会话时复制分页片段，误称完整记录 | `continueInspectedSession` |
| 13 | 每次切回 Session 页都会重新读取并清除已翻页内容 | `switchView` |
| 14 | 同一 Session 连续检查时旧请求仍可覆盖新请求 | `inspectSession` |
| 15 | 切换 Session 后旧对话仍显示在“正在读取”标题下 | `inspectSession` |
| 16 | 切换 Session 后旧链路仍显示在新 Session 标题下 | `inspectSession` |
| 17 | 链路摘要以已加载页统计，却看起来是全部记录 | `renderTrace` |
| 18 | 链路筛选或加载更多会折叠已展开的记录 | `renderTrace` |
| 19 | 搜索后数量徽标仍显示全部 Session 数量 | `renderSessions` |
| 20 | 缺失时间戳的会话被错误归类为“今天” | `sessionDayKey` |
| 21 | 快速连点“新建任务”可产生多个 Session | `newAgentSession` |
| 22 | Agent 运行时输入框不可编辑，不能预写下一条任务 | `setAgentControls` |
| 23 | 非 macOS 页面仍提示 `⌘ Enter`，与实际 Ctrl 快捷键不符 | `web/index.html` |
| 24 | 中文输入法组合期间按发送快捷键会意外提交 | `web/app.js` 键盘事件 |
| 25 | 链路筛选按钮只有颜色变化，没有可读选中状态 | `web/index.html` / 筛选事件 |
| 26 | 当前 Session 列表项只有视觉高亮，没有选中语义 | `renderSessionItem` |
| 27 | 运行/审批状态图标只有颜色，没有会话级文字 | `renderSessionItem` |
| 28 | Session 搜索仅靠 placeholder，缺少控件名称 | `web/index.html` |
| 29 | 非法链路时间戳让 `Intl.DateTimeFormat` 抛异常 | `formatTime` |
| 30 | “几分钟前”只在整页重绘时更新 | `renderSessionItem` |
| 31 | 快速连续激活模型时，较早响应可覆盖后一次选择 | `useModelProfile` |
| 32 | 保存模型成功后 API key 仍留在输入 DOM 中 | `saveModelProfile` |
| 33 | 模型列表刷新失败会清空先前已读的可用配置 | `loadModels` |
| 34 | 权限模式刷新失败会清空先前已知状态 | `loadPermissionMode` |
| 35 | 权限切换响应晚于工作区切换时污染新工作区状态 | `selectPermissionMode` |
| 36 | 选择当前权限模式会静默返回且对话框不关闭 | `selectPermissionMode` |
| 37 | 打开权限对话框后任务开始，点选模式静默无反馈 | `selectPermissionMode` |
| 38 | 浏览器禁用 localStorage 时保存 Token 抛异常，无法重连 | `#reconnect` 事件 |
| 39 | 编辑配置后缺少“新增配置”入口，旧 ID 容易被覆盖 | 模型设置对话框 |
| 40 | 无效或负数耗时会在界面显示 `NaN`/负值 | `formatDuration` |
| 41 | 目录读取失败后仍保留上次可选择的旧路径 | `browseDirectory` |
| 42 | 手动输入新路径后直接点“选择”实际选择旧目录 | `selectBrowsedWorkspace` |
| 43 | 切换工作区后 Session 标题和对话仍显示旧目录内容 | `resetWorkspaceState` |
| 44 | 新工作区继承旧工作区搜索条件，容易误以为无会话 | `resetWorkspaceState` |
| 45 | 新工作区继承旧目录折叠状态，可能隐藏全部 Session | `resetWorkspaceState` |
| 46 | 新工作区继承链路过滤条件，可能隐藏记录 | `resetWorkspaceState` |
| 47 | CLI `/` 命令单次失败会退出整个 REPL | `src/entry/cli.rs:run_repl` |
| 48 | TUI 提交失败时输入已从编辑器取走且不可重试 | `src/entry/tui.rs:handle_key` |
| 49 | CLI 取消 RPC 返回 `cancelled:false` 仍打印“正在取消” | `src/entry/cli.rs:cancel_request` |
| 50 | TUI 流意外关闭只要求退出重开，不尝试恢复订阅 | `src/entry/tui.rs:run_event_loop` |

### 修复与验证映射

| 编号 | 修复与验证 |
|---|---|
| 1–10 | 重连期间保留立即重试；恢复读取异常与歧义任务均释放输入锁；按稳定 run 核对 request 身份、终态和消息末页；订阅失效明确报错；同一 RPC 避免重复恢复；审批队列逐项显示且断线禁用。Web 回归覆盖快照失败、run 不匹配、重复恢复及多审批。 |
| 11–20 | Agent 默认载入最新消息页；继续会话时重新读取末页；历史页保留分页；同 ID 并发检查用代次淘汰旧响应；切换时清旧消息与链路；部分摘要标注范围；详情展开状态跨筛选保留；搜索计数、未知日期与相对时间显示修正。Web 回归覆盖长会话与并发检查。 |
| 21–30 | 新建 Session 加提交锁，运行时允许准备下条输入；快捷键标识按平台更新并跳过输入法组合；筛选与 Session 选中补语义，状态补文字、搜索补名称；无效时间戳不抛异常。Web 回归覆盖重复新建；浏览器实测 Agent 与 Session 页面。 |
| 31–40 | 模型激活串行化；保存后清空密钥；刷新失败保留已知模型和权限；权限响应按 RPC 身份检查，并对相同模式和忙碌状态给反馈；Token 在禁用存储时保留页内值；新增空白配置入口；异常耗时显示占位。Web 回归覆盖激活顺序、耗时，浏览器实测配置入口与清空行为。 |
| 41–46 | 目录失败时失效旧选中路径；手输路径选择前先核实；切换工作区时清除旧标题、对话、搜索、折叠和链路筛选。Web 回归覆盖失败目录及工作区状态清理。 |
| 47–50 | CLI 单次斜杠命令失败留在 REPL，取消结果为 false 时报告失败；TUI 提交失败恢复草稿与原会话显示，任务流断开有限重新订阅。Rust 新增订阅失效不可误报完成回归，既有 daemon 断流重连测试继续通过。 |

完整门禁与提交信息见 `progress.md` 本轮记录。浏览器冒烟使用独立端口和临时 Ollama 环境配置，未触发模型推理；结束后关闭临时服务与页面。


---

# 发现

- HEAD 仍为 2f204df；Wave 0 修改未提交，全部保留。
- 当前 client 仅依赖协议，测试模式反向依赖 daemon::server::InMemoryEnvelope；应迁移 envelope 到 client 测试传输能力。
- handler 中 20 余请求 DTO 尚未归 protocol；ModelSaveParams 的 ProviderProfile 为配置值，可用 generic DTO 避免协议依赖 provider 实现。
- storage 现存 RunStore 可直接实现 query/mutation 能力端口；不能仅放置未来 SessionRepository 空接口。
- 稳定状态仍是 SQLite v5 与 JSONL；本轮不得把提取 client/端口误称为 lifetime 集成。

- SQLite 和 client 已各自形成 workspace crate；控制面与工具审计改用 ControlRepository trait object，人工 reconcile/recover 使用独立 MaintenanceRepository。
- ClientError 保留 RPC/resync typed data，提供显式重连与有界 cursor 读回；不会重放 mutation。
- 已移出 handler 请求 DTO，protocol_version=1 拒绝未知字段，旧格式经兼容归一化再走同一 handler。

### Wave 1 最终事实与下一阶段边界

237 项 Rust 合同覆盖显式重连无 mutation 重放、run/event owner、稳定 cursor 连续分页、typed resync snapshot、v1 未知字段/嵌套 profile/未知版本拒绝、旧协议兼容、真实 daemon 重启与三个入口一致性。schema 仍 v5，业务消息 owner 未改变。

Wave 2 盘点：daemon 的 session_runtime/session_snapshot/session_infos 与 LoopEngine::record 仍直接使用 JSONL；SessionRuntime.history 是长期执行输入，ContextManager::prepare 原地压缩它。下一 wave 必须在受控备份/导入之后同时切换准入身份、持久 snapshot、transcript 写入和生命周期事务 fence；仅 ALTER TABLE 增加 lifetime 不能提供隔离。当前这些 Wave 2 保证均 **未实现**。

### Wave 2 实际接线

旧 sessions 表保留兼容，session_heads 持有 incarnation；runs 的内部 idempotency key 由 lifetime + 原 request id 编码，wire request id 单独保存，不把内部 lifetime 发往旧公开 DTO。相同公开 key 重建可重用 request id，但 run id/generation 不复用。SQLite triggers 守护 event/interaction/tool/attempt/queue/delegation 的迟到写入，transcript/生命周期显式事务 fence。clear 也开启新 incarnation，原始 transcript 留在旧 lifetime 审计，不删除 project/global 数据。在线和 cron 均停止 JSONL append，legacy append 只留测试 fixture。

## Wave 2–7 最终事实与边界

全部主链本地验收通过。真正防止“删后迟到回调”的是同一 SQLite lane 内的 exact lifetime/owner CAS，运行时 task map 和超时不构成授权。compact 的 candidate、source proof、receipt/generation 各自持久化，原始 transcript 不改写。memory 先在 SQL 过滤 scope/TTL/global 确认，再 bounded ranking 与分区注入；forget 的 v12 receipt 保护重试和避免复活。usage 校准只用成功 committed turn、相同 provider identity/model/generation 的 ledger。

最终完整测试 261 Rust + 27 Web。Docker daemon 存在但缺 alpine:3.21，强隔离不可用时拒绝已验证；没有把容器成功隔离、真实 Keychain/远端服务/在线公告或 GitHub CI 宣称已验证。三份规划与最终实施文档保留各 wave 历史证据。

## Codex 对比发现
官方参考8d44977aa2fb9ae1b128660668dc5b36966613fa。Codex retained user inputs、独立有界memory fragment、租约式两阶段提炼值得参考；本项目 CAS/SQLite/scoped receipts 保留。本轮发现压缩可能切当前轮、记忆包装漏计、Episode总量无界。详见docs/research/codex-context-memory.md。
# 2026-10-03 数据飞轮研究

已有基线：266 项 Rust 回归，SQLite v12；上一轮 Codex 对比的压缩当前轮次保护、原文锚点、完整注入预算、有界摄入已实现。本轮无需重复实现。

官方参考固定为 8d44977aa2fb9ae1b128660668dc5b36966613fa（/tmp/codex-architecture-reference），非实时 HEAD。state/src/runtime/memories.rs 提供 cited usage、usage/recency retention 与租约；memories/write/src/phase1.rs 包含上传前 secret redaction。仅参考机制，独立实现。

当前缺口：render_context 返回文本而无入选 ID；没有实际注入记录或显式反馈；按关键词/置信度/新近度排序，无法基于用户纠正改进；已有 provider/context ledger 与 terminal，不需要另建完整轨迹库。官方 compaction 文档说明 opaque state 必须原样传递；本项目支持多 provider，继续使用受验证的宿主摘要投影，不强行绑定 Responses API。

本轮落地：独立评测17/17；lexical-feedback-v1 不用曝光数或业务成功自举标签。负反馈绑定 memory ID+digest，本会话 lifetime 使用；无关/有用仅调整同相关度排序。评测是人工预期的合成检索样本，不能推导真实模型成功率或成本改善。曝光为 context_prepared/tool_result_prepared，不宣称模型采用。学习表不保存query/content；forget FK CASCADE 和 clear/delete 清学习数据。保持 canonical transcript、投影 CAS、scope 信任不变。

有界候选与负反馈复核：评价读取与同作用域最新1000个候选集合对齐，避免全体最新1000评价把仍在候选集中的旧负反馈挤出。恢复依赖既有 turn_commits，业务 terminal 先发布、维护独立；每批32、60秒退避、3次上限，失败不阻塞同批健康 turn。

## 2026-10-03 第二轮研究发现

- Codex `core/src/context_manager/history_user_authorization.rs` 把原始/继承/checkpoint 输入及 complete 标志分开；checkpoint 不构成原始授权。`core/src/context/compaction_summary.rs` 把摘要标作 contextual fragment（user role + compaction.summary kind），与原始指令不同。本项目摘要目前直接形成 System 字符串，只有自由文本提示，没有结构化来源/未知声明。
- 本项目 `agent_context::validate_candidate` 只检查收益、非空和工具闭合，未验证当前用户轮次字节级保留；存储 `settle_compact` 只检查工具闭合，可能安装删除当前输入但合法闭合的候选。这是可验证的框架缺口，应把当前轮次保护提升为宿主和持久提交的共同契约。
- 摘要递归到 depth=8 时直接截断字符串并返回成功，可能将截断 checkpoint 当完整摘要。本轮应允许无收益/失败降级，不静默损失。
- 记忆有 source_message_ids（lifetime:seq）但无有界 evidence API；用户/模型无法从召回条目按来源核实 raw transcript。本项目已有 canonical batches 可复用，不需要 Codex 文件型索引库或复制原文。
- 上轮恢复每60秒推进32个，但daemon空闲2秒会退出；积压大于32时只处理首批，剩余要等下次启动。需要研究与既有受管后台生命周期协调的可完成排空机制，不能无限占用空闲daemon。

第二轮证据接口使用当前记忆声明的 lifetime:seq 点查 canonical batch，并复核 batch digest 与 run owner；跨会话的已确认记忆不公开原对话。维护整体失败回到60秒间隔，不让立即到期查询触发100毫秒永久忙循环。

第三轮：Codex在fork前flush持久history，并区分完整继承与最近轮次；本项目已有SQLite原子委派和冻结能力，缺口是task之外可核实背景。选择显式source IDs与冻结有界包，复用run_snapshots JSON；不默认整段fork，不新增存储后端。

第四轮发现：字符串数组锚点无法暴露预算省略；Codex的original/checkpoint completeness可借鉴。以文字锚点覆盖状态和省略下限补齐，不把摘要短、重压缩成功或有digest当作原始历史完整。

交接复核：来源序号规范化后检测重复，防止0与0000绕过同来源去重；证据页对请求ID和元数据也作硬预算，避免无来源/跨会话返回分支绕过32KiB页上限。

## 2026-10-05 主链复核

现有六个 workspace 库，SQLite v13；源码证实在线 transcript 不双写，但文档宣称 Wave0–7 全完成过宽。只读 load 会构造 runtime；公开快照缺 durable control/lifetime；删除请求没有客户端 lifetime CAS；连接无 capability intersection。Wave5B plan identity/digest/execute/discard/hooks、手动 compact exact owner、tool_search 尚未实现。详见 ADR0002 与 runtime-readback-fences change doc。初次基线 client socket EPERM 为环境限制，经环境审批同命令通过。

本轮实现收口：snapshot revision 采用全库持久单调游标，元数据/控制/投影在同一 read transaction 对齐；读取不使用 runtime/approval 缓存拼事实。旧 JSONL 启动迁移已在 composition root 受控存在，公共 readback 无需兼容导入。缺 expected_lifetime 的 destructive legacy 请求不能安全授权，所有版本统一拒绝；原方法 alias 与 Rpc(-32602) 错误合同保留。严格 RunRecord 与兼容 snapshot 附加字段通过集中 typed decoder 处理。新 readback 的 plan digest 不冒充未实现的 plan execution identity。


# 审计发现

- 已有 SQLite v14、ExactOwner、plan stage/TurnCommit 和单事务 SessionReadback。
- src/plan.rs 的计划只含 steps，进度与定义共用 stage_plan 的 revision+1；无稳定身份、定义摘要、review/decision receipt。
- src/daemon/handlers.rs 的 session.compact 复用旧聊天 owner，并持 session writer 等待 unary 结果。
- 原变更记录明确 tool_search、共享 live reducer、完整只读请求重建和剩余 crate 提取未完成，须逐项源码核对。
- 入口架构测试禁止 storage/runtime concrete；共享控制必须经 daemon-client/protocol。


### 控制面 Wave 1 验收（2026-10-05）

版本化计划纵切完成：SQLite v15、稳定定义身份/摘要、精确执行/废弃、pending 重启不自启、旧数据证据保留、CLI/ACP/WS/HTTP 合同。Rust 315 项全过，Clippy/MSRV/release/Web28/fmt/check/offline deny 通过；日志与失败修复证据见 docs/changes/governed-runtime-controls.md。下一步 Wave 2 hooks；Wave 2–8 尚未完成。

### Wave 2 收口与 Wave 3 审计（2026-10-05）

Wave 2 门禁332 Rust/Web29通过。continuation 不能复用会改写 context policy 的 for_delegation；专用构造器只限制执行预算，snapshot 不改写。恢复 outbox 允许旧 lifetime Start 的唯一依据是原 publication；伪造历史 Start 拒绝。Hook readback cursor 是插入游标，结算推进 snapshot_revision，刷新必须重读对应页。

Wave 3 当前事实：SessionCompactParams 强制 owner_run_id/expected_revision，handler 借旧聊天 owner，在 session writer 下等 unary；compact_operations 已保存 intent/source/receipt，但不是独立 run。复用原 admission、queued/native run、ExactOwner、TurnCommit/Started/事件回放和 compact source CAS；新增独立 kind/linkage，不能以客户端 reservation 替代 owner。保留旧 unary 为同一 command owner 的等待适配。


## 2026-10-06：heartbeat第五轮取舍

OpenAI工具结果实际发送name与tool_call_id，共享TokenEstimator此前漏计；Codex固定8d44977（workspace 0.0.0，非最新）对FunctionCallOutput计入这些字段。独立样本复现4项字段漏计与1项闭合压缩候选无收益被误接受，旧24/29，修后29/29。只借鉴输入字段覆盖，保留当前中文估算、工具配对/轮次保护和provider路由。thinking只存储不回传，继续排除；关联ID可能重映射，仍是启发式而非精确tokenizer。

本地聚合主库schema v4且缺飞轮表，不能把缺表当成零反馈或把运行成功当记忆好评；未读取/外传正文、迁移生产库或调整记忆排序。固定已提交a8ac3dc加增量303项Rust全过，但开发快照Wave 3原有v17迁移断言和type_complexity失败必须分别记录，不能宣称当前全工作区已验收。证据见docs/research/codex-tool-metadata-budget.md。


### Wave 3 终审发现（2026-10-06）

- compact intent/receipt 与 native terminal 必须同事务；否则 projection 已安装但 run 仍未知。generic chat finish/reconcile 不得改写该 run kind。独立 compact 不能写伪 user/assistant transcript，也不能进入聊天记忆摄入队列。
- watch.changed 的正常 channel 关闭不表示 overflow；应继续排空最后 response，真实256帧溢出仍立即断开。
- 记忆维护原 next_memory_ingest_due 对首次任务返回查询当时now，调度器比较较早采样now，毫秒交错可能停止第二批。稳定RED：control-waves-wave3-maintenance-race-red.log；改首次due=0，原重试时间仍不变。
- 同 workspace heartbeat 的context/OpenAI预算改动及research文档已审计并保留，未替其重写历史记录。


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


# 企业升级基线（2026-10-07）

- HEAD 与用户基线一致，13 个物理职责库、根组合 binary，工作区干净。
- 既有 AST 护栏为 tests/architecture_contract.rs；目前检查入口/Provider/storage 依赖、消息 owner、库 unsafe、物理 facade、根组合，但尚未覆盖全部 channel、run/approval owner 和未来企业库质量。
- 既有 task_plan.md 的 Wave 0–8 指个人 Runtime 重构，不能作为本次企业 Wave 0–10 完成证据。
- 企业新增能力必须注明 proposed，不能以新增 ADR 或 DTO 宣称生产接线完成。

- 生产 unbounded 构造器：gateway/serve::websocket_connection、daemon/handlers::execute_chat（确切函数继续核对）、runtime/loop_engine::call_provider_streaming、runtime/tool_calls::canonical_output；test-transport 另有隔离测试通道。文件/内存 SQLite 合同不能宣称 PostgreSQL。

- 补充基线差异：CronStore 仍由 .my-agent/cron.json + RwLock<CronState> 管理作业定义，SQLite 只管理 cron 引发的 native run；MCP/Skill/Provider 配置也仍是本地文件。这些不能写成 SQLite v20 已覆盖所有管理面事实。

- 环境实际探测：Docker Server 29.2.0 可响应、127.0.0.1:5432 可接连接、Kubernetes 保存当前 context kind-cloude-course。只做只读发现，未改已有 DB/cluster；不把 CLI/context 存在当企业 PG/OIDC/K8s 验收。

- Wave1复核：admit_run_inner已有同事务准入、plan/compact/hook边界；授权应嵌入该transaction而非新增run owner。当前server唯一trusted域是Local；客户端params不能指定主体/tenant。新授权快照独立引用现有run与ExactOwner，老run不补伪造principal历史。

Wave1-S1最终发现：SQLite WAL外部policy发布与已开始读取身份的准入事务并发，旧事务写升级必须失败；barrier合同验证零提交。ABAC时间必须在取锁/进入事务后使用可信当前时间，否则排队前条件可过期。旧delegation/hook child在handler请求前已准入，授权只能嵌入原child transaction继承父fence，而不能在入口补造历史。完整全tenant ports与双人审批尚未实现，enterprise_ready固定false。

Wave2复核：事件只在RunStore::insert_event/views.publish_in和commit事务成为事实。outbox应挂同一个publication事务；旧stamp不追补发布历史。旧sync ControlRepository与rusqlite transaction密切绑定，不能为PG随处spawn_blocking或另造run state。当前psql/docker可执行，需真实连接验证再记录PG lane。


## Wave2-S1 持久发布与真实 PostgreSQL

- 真实 RED 为未知 RPC outbox.readback (-32601)，不是构建问题。v22 原 views::commit 封存 outbox，同事务 FK 指向真实 events。
- publisher 的 epoch 只保护 transport claim/receipt/ack；原 run terminal 仍由既有 ExactOwner/authorization fence，尚无 HA writer epoch。
- PG v1 不含第二套 run/session/approval；独立 worker 为实际 production caller。Unix socket /tmp、本机 PG18.6 真 lane，新建随机隔离 DB，finally 只清理自身 DB。
- 真实 worker 捕获 55P03：async driver 的 queued rollback 未运行，而测试同步等待子进程；修复 close().await 与 async output，不靠 sleep。
- event payload 按递归排序 JSON 序列化并校验实际存储字节 SHA；inbox FK 绑定完整域/event ID/digest。列表最多64元数据，无正文。
- 16个新增第三方包用于PG驱动；旧第三方版本/source/checksum零变化，digest/tokio-util新增feature依赖边；锁文件差异证据独立保存。


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

S6 后台原 snapshot.route=None 让 LoopEngine走无 ProviderAttempt 的兼容路径；真实 route-red 为期望attempt1/实际0。daemon现将原ProviderManager注入Cron，每次native准入前冻结路由/配置代，Context和LoopEngine用相同primary与route，原provider_attempts记录不复制。定时合同补中断attempt outcome_unknown核验。

S6 reviewed九门禁456Rust/36Web全部exit0，但审阅后新增Provider receipt数据库主键与JSON身份一致性RED（provider-corrupt-red.log）实际发现source JSON可指向target attempt。恢复/finish now核主键、round、candidate、run、Started；新contract确保两run/全部attempt/event零变更。最终审阅代码需重新九门禁并跑真实环境，不将旧456结果代表新增防护。

S6 implemented / locally verified：最终wave2-s6-cron-audited九门禁全部exit0，457Rust/36Web/10外部ignored；独立TLSMinIO四项和PG发布六项全通过。nativeCron同物理repo/proof/backend/冻结Provider，未知收口/no retry，定时续租/退出/kill恢复、六入口、SQL fault/损坏JSON身份零写闭合。报告wave2-s6-cron-report.md。完整Wave2和Wave3–10仍未完成；下一S7统一已接线async端口的SQLite有界阻塞隔离，保持schema24。

S7 原异步PublicationRepository直接锁Connection/同步SQLite，真实DB busy可阻塞单线程tokio的100ms定时器五秒。统一adapter不移动或复制SQL状态机，只将已存在sync事务安排到受四个permit限制的阻塞池；内存permit是预算不是业务事实。取消不提供回滚证明，调用者须原事实读回；SQL仍核原DB time/epoch/owner。Generic typed错误映射不暴露SQL。测试Condvar加RAII释放，避免断言失败时留住阻塞任务阻止runtime退出。

S7 最终implemented / locally verified：459Rust/36Web/10外部ignored、九门禁、真实TLS S3四项及PG发布六项全部通过。统一三组端口的有界SQLite隔离及取消/过载，schema仍v24。报告wave2-s7-adapter-report.md；完整Wave2–10仍未完成。后续必须完整native业务asyncports、全业务PG/租户键/原事务、audit/quota/offline迁移，不得把协调端口当作全部事实源。

S8：源码证实 session_heads.lifetime 已 UNIQUE，但只覆盖当前 head。退休 ownerless transcript 无永久session归属，可被另一head重绑定；v25保存原life→session并隔离不可证明旧life。原 readback run/queue/interaction/terminal/ledger多处只life查询，现在 SQL 同时key+life。page scope原未过滤会导致foreign先占limit；新scoped查询先DB过滤。六入口真进程还证实 client reducer会解码通用error.data成成功会话DTO，ACP v2 read_snapshot把RpcError丢成Internal，均需保留typed安全错误而不是伪造新投影。新cli session-read真正只读，旧chat有意新建session不能用于全局零写断言。

S9：中心 authorize_local_request原调用同步identity_readback，可能在每个Tokio请求线程等Connection。真实current_thread/受控原Connection通知RED为timeout100ms实际Ok(identity)，原任务约0.56s；改新实际端口后及时timeout。core错误封装保留Authorization/Coordination并脱敏Persistence。身份等待后采样timestamp，原准入TX继续复核策略/epoch/时间，不能信任前置授权。原授权读取此前对badowner可能Protocol→Internal；新的完整scope SQL owner fence返回StaleOwner，保留原native proof/frozen snapshot/revocation。SQL故障fixtureDROPprincipals先被FK拦截，不是真功能RED；只在测试连接关闭FK再注入缺表，应用连接仍保持FK。

S9 最终九门禁全部 exit0：471 Rust / 36 Web / 40 真实 daemon，10 项外部测试按环境显式 ignored。真实 TLS MinIO 四项与 PG 发布六项独立回归进行中；S10 已准备事件隔离/字节分页 RED 和实际端口/六入口方案，尚未实施，完整 Wave2–10 未完成。

S9 implemented / locally verified：471 Rust / 36 Web / 40 真实 daemon、九门禁全 exit0，独立真实 TLS MinIO 四项和真实 PG 发布六项全通过。报告 wave2-s9-identity-report.md；继续 S10 事件读回的实际 RED/修复，完整 Wave2–10 未完成。

S10 已实施：RunEventPage/RunEventRepository、SQLite scope 单事务及行/字节分页、原事件/stamp共用解码、daemon run.events/订阅恢复和 CLI/ACP1/ACP2/HTTP/WS 实际投影。两项有效 RED、六项 storage GREEN 与六入口真实 daemon 合同通过；订阅恢复、共享预算/竞争与完整九门禁/真实环境回归尚待收口，schema25/enterprise_ready=false。

S10 审阅后续缺口：原事件 decoder 对缺少 stamp 的历史仍保留 legacy fallback；现有表没有不可删除的 native/legacy provenance 标记，不能仅凭时间戳猜测新旧来源。损坏已存在 stamp 的 SHA/owner 合同已通过，完整事件 provenance/历史防篡改仍需后续真实 RED 与原表迁移，不能夸大为全部事件防篡改完成。

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

S16完整Rust门禁exit0：556通过/0失败/10外部ignored，40个executables；实际daemon48/原计划审阅执行六入口与重启恢复全部保持。Clippy exit0，未增加例外或栈；MSRV/release/Web/deny/diff进行中，随后TLS S3四业务和PG发布六业务严格串行。


S16 implemented / locally verified：wave2-s16-turn-audited-final 九项全部exit0，556 Rust / 36 Web / 48真实daemon，40个test executables、10外部环境项明确ignored；16项turn/transcript/计划integration合同、83 storage/132 runtime单测通过。独立真实TLS MinIO四业务和PostgreSQL发布协调六业务全部exit0。原六async端口接实际前台/PlanStore/daemon start-plan，同一Connection/四permit/固定准入envelope；六越域RED、已结算无决策仍给执行许可RED、receipt错误run_id仍执行RED修复。JSON四维和SQL四维身份GREEN，不把第一个截停RED夸成多项RED；故障回滚/CAS/幂等/撤销/过期和真实daemon原计划六入口/恢复合同保持，无新表/第三方包、schema25。编译/夹具失败及测试无效import警告保留，不计功能RED；未放宽超时/栈/护栏。全业务ports/nativePG/租户键/nativePGwriter/outbox/auditquota/offline仍欠，完整Wave2–10未完成，enterprise_ready=false。详见 docs/enterprise/wave2-s16-turn-report.md。


2026-10-08 持续执行：用户要求直到Wave10完成再停。S16已完整本地验收；S17请求捕获/上下文异步迁移in_progress，先复现原请求行SQL/JSON完整合法错配及scope缺口，随后唯一helper/固定准入envelope/实际模型及摘要await。全企业Wave2–10尚未完成，readiness=false。继续使用planning-with-files已有记录，不重建计划。

S17四项有效功能RED（scope来源读、capture写、只读重建，以及完整合法JSON保留原digest但SQLcapture_id错配）均独立失败，修后四项GREEN。原helper唯一body/同TXscope/JSONSQLrun/capture/round/candidate核、SQLBLOB预算和完整canonical前缀超预算明确拒绝已实施。三个native async端口接线进行中；摘要回调改owned消息+boxed await，compact使用已捕获owner进入同原task-local上下文。完整compact准入/结算仍sync欠账，不能称全async。旧GREEN session已结束后write_stdin返回Unknownprocess，日志证明4passed，不重复运行冒充新增证据。

S17额外route有效RED：合法冻结catalog/policy及正确摘要仍接受与冻结Provider候选不一致的route/provideridentity，实测Ok写入（wave2-s17-request-route-red.log）；原helper增加候选index、model与原candidate序列化SHA身份核对，读回复用同helper，不新增路由账本。撤销夹具首次失败为对未结算legacy运行开启writer被原安全activation拒绝，先正常终态结算原夹具run再启用writer，不修改原activation保护。

S17库回归发现真实兼容回归：两个未知副作用合同在发送前capture失败，Cron按原安全未发送逻辑尝试4次，未执行外部副作用。新envelopecatalog检查错误混用了两种历史合法编码：RunSnapshot哈希typed ToolSpec serialization，ContextEnvelope哈希serde_json::Value（字段排序）。改为各自按原canonical表示核同一catalog，不改历史摘要或弱化合同。原失败log wave2-s17-libraries-local.log保留，完整库及真实daemon回归以新prefix重跑。

S18两个真实RED均有效：原native事件stamp删除后分页返回无_my_agent_view的成功事件，直接stamp读取返回None，均不允许降legacy。应用schema26原events永久provenance/native缺stampCorrupt/已发布stamp删除保护/先SQL字节预算；旧fixture准确撤26后再撤25，future更新27。旧迁移故障版本仍原值不放宽。S17普通模型与compact-summary真实daemon两步均exit0；其完整九门禁与真实lane跟本批收口统一执行，不冒充最终验收。

S18事件12项合同及storage83单测全部GREEN，schema26已实施但完整门禁尚未收口。S19Cron审阅proposed：真实背景运行仍with_tool_audit重读owner、finish(runid)重取终态owner、sync创建/准入/请求；拟同canonicalports/固定准入上下文与独立workload主体，policy verifier从daemon注入，原create_in与admit_inner同TX复用，不新增runtime→policy依赖。先真实RED再实施。

S18五组迁移回归全部exit0（enterprise identity/object/publication/readback/writer；外部环境测试显式ignored，未冒充真实lane）。S19两个业务RED有效：缺独立冻结workload0记录，以及晚到callback把新generation状态改failed；锁场景第三测试先被旧Deferred升级SQLITE_BUSY失败截停，未达到timer断言，不夸async timer RED。已实施schema27 Local固定workload/developer角色、原同TX SessionCreate+Prompt校验及create_in/admit body/fixedowner，实际Cron接原asyncports；policy verifier仅由daemon注入，无新增runtime基础设施依赖。首次check缺std::sync::Arc限定已修，开发依赖policy加入原测试注入用途，Cargo.lock将更新后locked门禁验证。

S17–S19 收口：当前schema27。补 source 总 batch 行数与 message revision 预算，避免小字节大量合法空行的分配放大；首次局部命令使用 --exact 配部分测试名导致0项运行，日志保留、不计RED，下一次使用完整测试名。一次查找不存在schema.sql已改用实际session_migration.sql。S18/S19中文实施报告已保存，完整九门禁与真实S3/PG尚待本批统一验收。

S17新增有效row-budget RED：32769个合法空batch使原source成功返回32770个batch_ranges，修前真实失败证据 wave2-s17-request-row-budget-functional-red.log；原同TX source预算增加batch行数及总messages≤32768，修后request15/event12已GREEN，workload7继续运行。S20 compact仅只读预审，原settle在fence失败后写rejected/terminal为关键风险，native必须零晚写；计划/tmp/agent-rust-wave2-s20-compact-plan.md，尚未实施。

S17–S19宽门禁进行中：runtime136/storage83/架构20已通过，未完整结束。S20仅/tmp草稿，没有生产更改；发现原RunKind由linkage推导，拟复核原request id/input防合法link跨run错配，避免影子kind。另发现原Canonical/Model snapshot_in仍缺总量/行预算，拟共享唯一sessions预算helper，保全量或明确拒绝。


S17–S19 implemented / locally verified：wave2-s17-s19-reviewed-final 九项全部exit0，588 Rust / 36 Web / 48真实daemon，42个test executables、10外部环境项明确ignored；runtime136/storage83、request15/event12/workload7合同全部通过。独立真实TLS MinIO四组业务与PostgreSQL发布协调六项全部exit0（wave2-s17-s19-reviewed-regression-s3.json / postgres.json，PG runner五步骤不等于五合同）。S17三请求端口实际模型/摘要/读回await；S18原events不可变来源及native缺stampCorrupt；S19原同TX后台准入、固定owner及Local独立workload。schema27，不新增第三方包。首轮Clippy常量放testmodule后失败、foreign夹具writerFK失败及0tests过滤错误均保留，不计功能RED；移动常量后完整九项重跑，未放宽lint/超时/栈/架构。全企业Wave2、Wave3–10仍未完成，enterprise_ready=false，未提交推送部署。继续S20 compact实际RED与原唯一helper迁移。

S20七个有效业务RED全部复现（wave2-s20-compact-audited-functional-red.log）：foreign admit/fact/begin/settle均Ok，合法linkage移动到原Chat仍Ok NoGain，旧generation结算revision33→37，canonical读回32769合法空batch成功。首次夹具缺TurnRepository修后复现，不计编译失败为RED。抽取脚本在begin_compact单行签名处断言失败：已写ports/source、尚未写context及以后；修泛化闭括号插入策略，正在保留部分树首次check失败日志后继续剩余抽取，不重跑已写parts。

S20 native夹具误用不存在SessionCommand::Clear导致编译失败（compact-native-green.log），修为原End{delete:false}生命周期命令；未修改生产lifecycle。审阅发现专用Compact run若begin另一个operation可能结算原run，增加独立业务RED；SourceCAS竞争改为普通Chat下两个implicit intent，避免用错误专用run操作伪造并发。

S20十四context合同已GREEN；S21只读预审发现daemon四即时投影helper同步且latest_view_event无scope/字节预算。预审及两真实旧路径RED草稿在/tmp，当前库/真实daemon回归未结束前不修改生产源码。实际迁移复用原events decoder/同TX scope，不用分页扫描历史求latest。

S21两个有效RED（latest-event-functional-red.log）：foreign helper返回私密terminal与封存stamp，合法2MiB event返回成功。共用原decoder/scoped TX/SQL CASE字节预检、async latest端口及daemon实际caller已接线；首check闭包Err.into类型推断失败，显式Result<StoredEvent,RuntimeError>；委派父run使用真实字段run_fact_store（readback_store只SessionRead），无新增字段或第二事实。

S20 frozen初运行两失败实际均Authorization Forbidden而不是写入：原authorization_fence已核原run_snapshot_digest，合法改readonly也被拒，先前“settle不复核readonly”判断不成立，不计有效RED。保留新typedForbidden/零clock/零terminal合同，无生产重复政策检查。大snapshot读取仍有代码证据的预分配预算缺口；改为原有无授权legacy兼容run通过新端口复现begin真实成功，必须先预算再取原JSON。

S20第9项有效业务RED：原无冻结授权legacy兼容run的合法>1MiB冻结snapshot，经native begin真实返回Ok（compact-frozen-reviewed-functional-red.log）。原begin改复用requests::frozen_in唯一SQL长度+JSONdecoder，缺snapshot仍保持legacy兼容；native授权fence check_snapshot_in取原snapshot前增加SQL长度预检，继续核原SHA。readonly改动本就被原SHA拒绝Forbidden，不新增第二政策状态机，不计为RED。

S20/S21 context16/event17已全GREEN（context-event-reviewed-green.log）。Clippy预检截停于原请求source helper放testmodule后及format!未内联变量；移动helper到测试模块前、改直接格式变量，无lint例外。latest child SQL CASE先字节预算，再json_valid，再json_extract；坏/巨大候选明确拒绝，不解析无界JSON或跳过损坏候选返回更旧事件。追加该合同后统一宽门禁。

S22实际源码预审：ToolRegistry为runtime concrete类型，可async化target/descriptor/admit_all/scheduled_waves而无需改core Tool trait；原discovery receipt read也调用writer fence，需分只读永久scope与运行许可。cancel_one只从ActiveRequest取token/run_id，再run_owner可采用后来的generation，需原准入fixedowner证明。计划/tmp/agent-rust-wave2-s22-discovery-plan.md，当前生产源码冻结等待S20/S21九门禁，S22未实施。

S22预审约束补充：原authorization_fence已有冻结owner与run_snapshot_digest保护，不能只凭cancel_one重读owner就宣称当前native已有晚写绕过。需要真实验证无授权兼容运行能到所述路径，若已拒则保GREEN，不捏造RED。

S22额外真实caller：foreground在start_run_plan.await后仍同步session_snapshot(key)作为初始history，已有S20 read_execution_context(envelope)可直接替换，避免无scope原读取与Tokio阻塞；无需新DTO/表/端口。S20/S21宽门禁仍进行中，不改冻结源码。

S20/S21冻结审阅追加待核：compact_result_scoped虽SQL限定operation/run/lifetime，但JSON ContextSource尚未核source.lifetime与owner一致；预期revision不一致的健康请求目前Protocol统一映射Corrupt，应区分Conflict。红合同草稿/tmp/agent-rust-wave2-s22-compact-result-red.rs，未运行不计RED；放S22原结果解码修复一起，不在本轮门禁中改冻结生产树，不宣称该不变量已验证。

ACP原并发复核完成：48 passed / 0 failed / 2 ignored（50总项），daemon-bounded-diagnostic.log exit0；首次EOF仍未定位，不称已修根因。S20新增第10个有效业务RED：source_json合法JSON改为不同lifetime后read_compaction_result实际返回Ok(Some(0))，证据compact-result-identity-functional-red.log。原唯一事务增加JSON源lifetime/起点/整数范围与committed+1/no_gain原generation一致性；健康revision冲突及summary_failed以Conflict拒绝重放，损坏以Corrupt拒绝。新增三项读回合同正在验证。首次改动闭包Err.into推断失败，显式返回Result<ProjectionGeneration, RuntimeError>修复，编译失败日志保留、不计业务RED。全Wave2–10仍未完成。

2026-10-09后续预审（未实施）：S22临时接线脚本只覆盖chat五个publish_update caller及原append_event_scoped；Provider事件也用同helper，不能被live七名称误拒。S23 cancel_one原已显式检查params.exact_owner，问题是检查后再次读取owner，不能称所有显式cancel已有绕过；descriptor在副作用后重查必须移到副作用前。额外发布协调SQL预审发现SQLite/PG record按全局event_id先读payload再check_claim判断key，尚未运行真实RED，记录/tmp/agent-rust-wave2-publication-scope-audit.md，后续必须在SQL按scope/key先过滤。S20/S21真实TLS S3四业务全过且本次容器已删除，PG发布六项正在串行验收；生产源仍冻结。

2026-10-09 S20/S21 implemented / locally verified：wave2-s20-s21-reviewed-final 九项全部exit0，614 Rust /36 Web /48实测daemon，43测试执行体、10外部环境项明确ignored；runtime136/storage84/context19/event18全部通过。独立真实TLS S3四组业务及PostgreSQL发布协调六项全部exit0（wave2-s20-s21-reviewed-regression-s3.json / postgres.json），自己的MinIO容器与PG隔离DB已清理。第10项有效compact来源身份RED已修，同原事务核JSON lifetime/起点/结果generation，正常revision和摘要失败返回Conflict，损坏Corrupt。首次九门禁tests101/ACP初始化EOF保留；独立与原并发daemon复核、最终完整门禁均未复现，根因未定位，64KiB有界诊断并持续drain保留，未修改超时/栈/并行度。schema27，无新第三方包。全企业Wave2、Wave3–10仍未完成，enterprise_ready=false；未提交推送部署。继续S22实际Live事件mutation，再S23发现端口，统一本批验收。

S20报告校正：fact/operation/request历史读取只核永久归属、不取writer；read_compaction_result与execution source则使用check_compact_owner/check_execution固定准入owner+当前head/fence，是执行链读取，不能把它们称为退休后可无proof读回。历史compact status由原fact端口承担，未放宽生产授权。traceability W2.01改为当前明确范围/最终614验收与剩余backend，旧过程证据仍各切片报告和日志保存。

S22两个有效业务RED已复现（live-events-functional-red.log）：foreign旧compatibility append以及无冻结授权旧run的late callback都Ok(EventSeq(2))。已有frozen SHA的native owner保护不能称被绕过；复现明确授权记录0、合法同步更新run及head generation。原helper保留唯一events/interactions/stamp/outbox事务；native live新async入口与五个实际chat caller固定准入env、shared4budget、状态/allowlist/object/字节预算已接线，foreground初始历史改await原execution_context。check workspace/alltargets/allfeatures已exit0。新增typed scope/owner/终态/预算/rollback/两Connection seq及取消awaiter仍实际worker提交合同待GREEN，尚未最终验收；S23继续待实施。extract脚本已应用，不得重跑覆盖新源码。

S22 storage首轮84/85通过，取消awaiter新合同失败不是timer/预算失效：原Fixture.admit默认Queue，worker正确以Conflict拒绝queued事件，所以没有提交。新live合同明确try_start_queued先进入Running，保留原timer/permit/提交一次断言，不改生产guard、不放宽status；三个新native事件夹具及并发夹具同样真实start，原已有18只读夹具保持不改。并发cursor用start后的真实last_seq，不能用准入时旧seq。最初late-owner旧路径RED是Queued兼容run，后续需Running原helper再复现，避免夸可执行callback证据。

S22纠正Running夹具后storage85全过（storage-corrected-green.log），25ms timer/四permit占有及worker取消后单次提交原断言通过。原callback helper在真正Running且head/run generation一致更新后再次真实复现成功EventSeq(3)（live-running-owner-functional-red.log），不是Queued不合法状态的混合证明；恢复测试调用线上实际native append_run_live_event固定env，预期不变，原legacy runid helper保留维护语义。完整23事件合同继续GREEN验收；本批尚未九门禁和真实环境验收。

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

### 2026-10-09 源码复核

当前分支仍为基线 0af445b，企业化改动全部在工作树。已有 baseline/ADR/异步端口和 PG canonical 实现。文档截至 S43，但 PG requests.rs/compacts.rs 实际已实现请求捕获及九个上下文/压缩方法；以源码为准，不重复开发。PG resources 适配缺失，daemon/runtime 已通过 RunResourceRepository 使用 SQLite 实现。原资源有效状态为 starting/running/stopping/completed/failed/stopped/orphaned；未知进程由 orphaned 承载，保留 Local 合同，不在本切片提前新增 worker 状态机。

本轮核验结果：PG资源SQL及当前daemon/storage-postgres依赖库可编译；共6个定向合同通过。单纯64KiB字节预算不能限制空日志行数量，已把空文本和不足一完整字符定义为不推进cursor；SQLite/PG共享同一规则。核验证据前缀亦占4096字节字段预算，写入前必须计入。PG资源callbacks额外显式复核session_heads.run_generation，读历史仅核永久lifetime。测试数据库隔离并已清理，全量企业功能仍欠。

### 2026-10-09 计划、诊断与续跑复核

PG原表足以实现计划/旧文件证据、diagnostics和hook_continuations，不需要schema变更。终态helper若提前seal，则丢弃计划后snapshot_clock增长而终态stamp未反映最终事务revision，已移到最外层command最终seal。保存计划回执必须绑定SQL identity/session/lifetime/run列及永久owner，queued取消共用同一个decoder。26端口generic构造器保证同一adapter事实源；生产Local bootstrap已实际调用，PG缺三类能力不能通过完整构造器。生产局部check通过，行为合同未执行。


### 2026-10-09 PG 记忆、委派、维护与本机启动

补齐同源 PG 记忆/维护/委派领域端口，并将 daemon/Cron/导入/artifact 接到完整26端口装配；默认SQLite保留，postgres-local显式Unix socket与writer、空Local域human和独立workload初始化，PG文件目录绑定database/schema。canonical迁移v2实际执行，遗忘不破坏历史反馈幂等；共享摘录/凭据过滤和授权conditions。恢复、核验及文件GC使用原事实事务，GC取消后仍受管并排空。实库冒烟发现并修正resource session_id关联错误、同owner过期预留、异步Provider预算遗漏；实际启动发现PG UUID与trace文件校验不兼容，保留32hex兼容并接受严格UUID。局部生产编译和test-support内部测试源码编译通过，真实PG18.6合同1项通过。真实binary双后端启动/假模型chat/重启去重通过，每个后端仅调用模型一次，PG未生成SQLite事实库；未跑完整测试或门禁，完整Wave2–10未完成。详见docs/enterprise/wave2-native-runtime-report.md。

最终补正：真实PG chat暴露执行future与事件消费者等待同一连接的闭环；改为join并行驱动，已接收事件完成及剩余队列排空后才提交终态，未引入第二执行状态。最新binary的两个后端冒烟均exit0，使用本机假模型及独立临时工作区。

### 本轮续作基线

原规格全文已读。当前daemon通过DaemonRepositories组合相同adapter的26异步端口；PG canonical v2/coordination v1。connect_local固定/tmp + NoTls，daemon仅sqlite/postgres-local；PG quota已有API但未接run准入/结算，SQLite尚缺audit/quota对等。企业域受信任DB CLI可初始化，但运行工厂/上下文仍Local。必须复用这些真实owner扩建，不建立第二状态机。用户禁止子agent与逐切片完整门禁。

### TLS、审计配额与离线迁移续作

两后端共享ledger合同已在SQLite和真实PG18.6通过；第一次include-ignored忘记设置PG临时库变量，属于测试启动失败，随后显式AGENT_RESOURCE_TEST_DB/USER合同通过。SQLite审计读回原使用resource JSON字符串比较，serde preserve_order导致合法同值JSON被Corrupt；改为有界Value比较，PG也补resource保存列binding。

离线迁移新增scripts/migrate-sqlite-postgres.py和PG offline模块/CLI，原schema预留offline_migration_receipts被实际调用。schema28全canonical逐表、未知列拒绝、完整备份/dry-run、逐行PG读回/计数/摘要/artifact校验，switch/rollback marker正在本机实测。回滚通过schema共享/独占advisory事务栅栏，目标改变则拒绝，不删除新事实。Local PG原artifact目录hash保持旧二元组；远程新增host/port避免跨库对象回收。尚未称迁移合同通过。

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

- JSON-RPC result:null反序列化为Option<Value>的None会错误触发v1响应shape失败并关闭UDS。自定义deserialize保持Some(Null)，缺字段用defaultNone；strict验证同时检查wire字段存在性，拒error:null。目录find的合法空结果暴露了该问题。
- daemon run.read现有projection为平铺RunRecord + _my_agent_view，Gateway不能假定RunFactReadback内部嵌套DTO。保持原协议并在受保护目录中核对session_key/lifetime，绝不移除授权检查。
- 费用charge operation必须包含run身份，否则不同run相同tool call_id会撞scope唯一约束；改用canonical[run,operation]SHA256派生。费用预占属于财务事实，不创建新运行状态；unknown Provider/tool/resource费用禁止释放或重放。
- 真实财务冒烟暴露Provider attempt开始原先早于capture；账本需要原发送材料，所以改request_model_once在assemble_provider_request/capture成功后登记原attempt，网络dispatch仍在登记之后。preflight/start失败不调用不存在attempt的finish；若start ACK未知，原Started由native终态/恢复保守收敛unknown，不重放。
- Billing input.images表示adapter支持图片，不代表输入有图片；上界检查只检查实际image_urls，避免拒绝所有正常OpenAI文本请求。局部合同加入images capability=true且无图片场景。

- 多scope Gateway编译发现Query字段移动错误，改取内部DTO；WS首帧无显式HTTP别名时允许受保护JWT scope选路，OAuth cookie按配置别名隔离。新增工作区连接缓存256上限。正在必要编译与真实多租户冒烟。

- Wave4注册表签名扫描移到数据库事务外；原TX只核expected revision/current trust digest、不可变generation和operation receipt。运行中session绑定固定代，canary/rollback只影响后续新lifetime；原Local未安装session固定None。SQLite35/PG9新迁移尚待实库合同，未宣称Wave4完整。

- Wave4真实HTTPsmoke首次在registry请求前共享Access准入503，未生成注册业务事实；定位valid_access_endpoint缺新路由白名单，补registry/generation与后续固定workflow/A2A endpoint后必要重构建。旧PG/SQLite注册合同通过，不能将该HTTP失败算成功。


## 2026-10-11 Wave5 派生执行与会话绑定

原 SessionStore 的文件名必须符合当前 session base stem；固定 workflow 前缀使 SQL session 已创建后入口拒绝。已改用原 session_key_for_operation，独立 workload 的内部命令在节点 TX 首次绑定并禁止更换。原 RunSnapshot.workflow 代表具体节点执行指令；委派和 Stop continuation 继承冻结配置时必须清除此字段，并在 continuation 的父快照比较规则中同步清除，否则 ToolCall/Subagent/Webhook 可能重复执行父节点。原 scope、owner、generation、工具子集与授权仍保留。
