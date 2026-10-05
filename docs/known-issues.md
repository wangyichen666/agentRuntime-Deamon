# 已知问题

## Runtime 部署与验证边界（2026-10-02）

当前 schema v14。2026-10-05 源码复核更正“全部 Wave 完成”的历史结论：本轮只补齐早期恢复读取/协议/lifetime 屏障纵切，302 项 Rust 和 27 项 Web 测试通过；完整 Wave 0–7 验收**未完成**。证据见 [最新实施记录](./changes/runtime-readback-fences.md)。

- Wave 5B：stable plan identity、digest CAS execute/discard、pending_execution/executing、受治理 boundary hooks **未实现**。readback 的 plan_digest 只供展示。
- 手动 compact 独立 canonical run/Started/exact cancel **未实现**，仍复用历史 run owner。
- 所有入口 live event/readback 的统一 revision 防回退 **未实现**；领域比较 helper 已提供但未统一接线。
- generation-scoped tool_search 与 runtime/daemon/sandbox/cli/acp/gateway 最终物理 crate 提取 **未实现**。
- 新 model readback 只返回已安装模型历史 projection+suffix，完整四层 Provider 请求只读重建**未实现**；`/context` 保留既有读取路径。
- clear/delete/fork 所有版本现在要求 expected_lifetime；兼容方法名保留，缺身份的旧客户端需先 readback 后发请求。恢复读取不切换 preferred session。

- Native 仍是软边界，不能阻止同 UID shell 访问宿主。Docker foreground exec 已接通；后台 Docker resource **未实现**，强隔离后台请求 fail closed。
- 本机 Docker daemon 可连接，但缺少预装 `alpine:3.21`，未运行真实容器隔离成功路径；已验证不可用时拒绝请求。部署需要自行准备合适镜像，不会自动 pull。
- 远程 MCP 本地 TLS/JSON/SSE 合同已验证；真实远端服务、OAuth 和跨服务互操作验收 **未实现**。目前授权使用环境 token 引用。
- macOS Keychain 适配器已编译，自动化故障测试使用测试 secret store，未修改真实 Keychain。其他平台 native secret store **未实现**，使用 `env:NAME`。
- token meter 是确定性估算器，使用同 lifetime/route/projection generation 的成功 usage 校准；不宣称等同各厂商 tokenizer。各候选 route 使用显式冻结的配置预算。
- 已运行本地 workspace/MSRV/release/Web/离线依赖审计，GitHub Actions 与在线漏洞公告刷新未运行。

> 本文件仅记录待处理问题；在用户明确要求“开始修复/开发”前，不修改相关实现。

处理状态：已于 2026-09-12 修复并通过回归测试。

## TUI 交互与运行反馈

记录日期：2026-09-12

### TUI-001：对话内容区域存在大面积无效空白

状态：已修复。

- 现象：进入 TUI 或显示对话时，可视区域上方出现大面积空白，实际内容集中在窗口底部，空间利用率很低。
- 影响：首屏信息密度低，阅读割裂，并容易让用户误以为界面没有正常加载。
- 预期：对话内容应按合理的顶部/底部布局连续展示，减少没有用途的大块空白；窗口尺寸变化时也应保持自然、稳定的排版。
- 参考截图：`codex-clipboard-29d0e6e0-b7d3-48fd-93b7-30a9be35f324.png`。

### TUI-002：Ctrl+T 展开详情后丢失原 Query 上下文

状态：已修复。

- 现象：按 Ctrl+T 查看工具调用详情后，界面只显示详情，原始 Query/对话上下文不可见，同时详情视图仍存在空白区域。
- 影响：用户需要在“原问题”和“执行详情”之间进行割裂式切换，无法边看原 Query 边核对 Agent 的执行过程，体验像打开了另一个详情页。
- 预期：Ctrl+T 应在原 Query/对话内容的基础上原位展开或折叠工具调用详情；原 Query 始终保留可见，滚动位置和上下文连续，交互表现应类似内联展开，而不是页面替换。
- 验收要点：
  - 展开详情前后，原 Query 不消失。
  - 详情插入对应消息/工具摘要附近，并能再次折叠。
  - 展开、折叠不会制造新的大面积无效空白。
  - 展开、折叠后滚动位置稳定，不发生突兀跳屏。
- 参考截图：`codex-clipboard-ad4a910b-2b35-4897-a153-a973c0bf1fc2.png`。

### TUI-003：Agent 运行中缺少明确、持续的加载反馈

状态：已修复。

- 现象：运行期间只能观察到工具调用次数、轮次、耗时等数字发生变化，没有加载条、旋转指示器或其他持续动态反馈。
- 影响：当一段时间没有文本输出或工具事件时，用户无法确认 Agent 仍在运行、正在等待，还是已经卡住。
- 预期：任务执行期间提供持续可见的运行状态反馈，例如不确定进度的动画指示器/脉冲式进度条，并配合当前阶段文案；任务完成、失败或取消后立即停止动画并展示明确终态。
- 验收要点：
  - 从请求开始到完成/失败/取消，始终存在可感知的运行指示。
  - 即使暂无新 token 或工具事件，指示仍持续更新。
  - 能区分“思考/等待模型”“执行工具”“等待审批”等主要阶段。
  - 不能用虚假的百分比暗示可精确计算的进度；无法估算时采用不确定进度样式。
  - 动画刷新不应造成明显闪烁、布局跳动或高 CPU 占用。
- 参考截图：`codex-clipboard-5833f9f6-ce08-4ed7-be7d-17591aa1359f.png`。

## 后续处理边界

- TUI-001：进入 alternate screen 后显式清屏，并用渲染回归保证短 transcript 从屏幕顶部开始。
- TUI-002：Ctrl+T 展开/折叠时保存 transcript 顶部锚点，详情继续在原消息流内渲染，不再自动跳到底部。
- TUI-003：新增运行阶段状态和不确定进度动画；模型等待、流式输出、工具执行期间持续更新，审批、完成、失败或中断时停止动画并显示对应终态。
- 验证：TUI 定向测试、全量测试、严格 Clippy、格式检查、release 构建及隔离 PTY 烟雾测试均通过。

## P4 Provider 韧性剩余边界

- 熔断状态仅在当前 daemon 进程内维护；重启会重新从 closed 开始。route 与 attempt 已持久化，但已开始的 LLM future 不会自动恢复。
- OpenAI 兼容服务需要接受 `stream_options.include_usage` 才能返回结构化用量；拒绝该字段的服务会按 `InvalidRequest` 失败，需针对该服务调整配置或适配器。真实商业 API 端到端兼容性尚未用密钥测试。
- `ContextOverflow` 只对本轮输入副本进行一次现有摘要式压缩；摘要生成失败或压缩未缩小时保留原输入并返回类型化溢出错误。持久 compact projection 留待 P7。
- queued run 在 daemon 重启后仍按已持久 route 恢复；若配置 ID、模型或 URL 摘要已改变，按 fail closed 结束，避免静默切换。API key 轮换可用相同配置 ID 和路由元数据恢复。

## P0 CI 远端验证边界（2026-09-26）

RustSec 公告库已可访问，完整 `cargo deny check` 在依赖升级后通过。新 CI 工作流尚未在 GitHub Actions 实际运行，Linux/macOS 托管 runner 的结果仍待首次提交验证。
P5 本轮重新执行在线检查时 GitHub 443 连接超时；离线使用本地缓存公告库的四项检查通过。远端公告库的最新变化仍待网络恢复后核对。

## P5 子 Agent 剩余边界（2026-09-26）

- 子 Agent 当前只继承 `read_file`。`exec`、写入、MCP 等能力会在准入时拒绝；未来若扩大能力，需先冻结独立权限与 cwd 执行上下文并验证副作用恢复。
- `wait_subagents` 的 `after_seq` 表示子 run 事件游标前进；P7 的持久上下文 checkpoint 尚未实现。等待本身不预留结果，断线或超时不会改变 child 状态。
- 结果 reservation 为 30 秒租约。客户端收到 reserve 后需调用 commit 或 release；断线时无需服务端推断业务终态，租约过期后可重领。
- token 上限依据已报告的 Provider usage 和本地估算在响应后检查；Provider 单次超额输出无法事前完全阻止。活动 child 的 steer、terminal revival 尚未实现。
- 父 run 若先于 child 结束，其原有流已经关闭；child 终态仍可经 `list_subagents`、`read_subagent` 与 child session 订阅读取。真实商业 Provider 的异步委派兼容性尚未用密钥验证。
