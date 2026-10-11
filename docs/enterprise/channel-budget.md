# 通道预算、例外与收口顺序

状态：基线已审计；Wave 0 冻结遗留例外，尚未完成全生产队列有界化。不能据此启用 Enterprise 多租户远程部署。

| 通道 | 真实构造位置 | 当前容量 | 当前溢出/断连语义 | 后续改造 |
|---|---|---|---|---|
| UDS server frames | daemon/frames::frame_channel | 256 帧，protocol 单帧 4 MiB | try_send 满后 watch 通知 detach；不 cancel | 增加累计字节预算和指标 |
| UDS client requests | daemon-client::connect | 64 请求 | send 等待背压，传输关闭不重发 | 企业并发/超时准入预算 |
| client input/projector | daemon-client::request_raw_with_id / project_stream | 各 256 帧 | 满时 detach/resync，通过 durable cursor 恢复 | 累计字节预算 |
| ACP reader/writer bridge | acp/acp_transport | 8 输入帧、1 输出帧，另有 256 帧/8 MiB 协议预算 | I/O 超限 detach；popup Cancelled 不提交 interaction | 指标 |
| Web 浏览器展示 | web/app.js | 256 帧/8 MiB | 本地 detach 后 readback | 服务器出口仍须有界 |
| Gateway WS writer | gateway/serve::handle_websocket | **无界**，单帧有上限 | send 忽略已关闭；无累计满语义 | 独立有界出口与 overflow 信号；终止连接 jobs，不 cancel native run；真实慢 socket 故障测试 |
| daemon AgentEvent | daemon/handlers::handle_chat_send_inner | **无界** | 转发 canonical events，消费者/任务共同 select | 有界 event port，必须保留 terminal transaction 与 unknown 语义；不可静默丢影响事实的消息 |
| ProviderEvent | runtime/loop_engine::request_model_once | **无界** | request/receiver 同一 select 轮询，结束后 drain | wire producer 支持背压；满时 typed 错误/unknown，整批工具 fail closed；避免同 task 阻塞 send 的死锁 |
| summary ProviderEvent | runtime/tool_calls::collect_response | **无界** | select + drain；摘要要求 ProtocolDone/未截断 | 与 ProviderEvent 同切片改造；不允许把不完整摘要安装为 compact |
| in-memory fixture bridge | daemon-client::request_raw_with_id，test-transport | 无界，仅非默认测试 feature | downstream 256 帧 | 可保留测试例外；生产不得启用 test feature |

精确例外登记在 [channel-exceptions.toml](channel-exceptions.toml)，每项包含文件、函数、构造数、理由和真实合同测试。`enterprise_channels_only_allow_exact_documented_legacy_sites_and_no_new_unbounded` 扫描源码，新增/挪用/重复构造或删除后未清理例外都会失败；零容量 mpsc 也拒绝。RED fixture 覆盖 fully-qualified、函数 alias、std 零参数无界 channel 与零容量 Tokio channel。

这些合同证明现有审批、重连、不重放等语义和门禁有效，**不证明遗留队列已经有界**。Wave 0 采用用户允许的“书面例外与测试”分支，目的是先冻结业务行为和精确技术债；在 Enterprise 暴露流量前必须移除四项生产例外。不能只将 `.send` 改成忽略错误的 `.try_send`，否则会丢 ProtocolDone 或工具参数，使 transport failure 混入 canonical outcome。

收口顺序：Gateway 输出独立切片 → Provider wire/assembler 同时改背压 → runtime AgentEvent/daemon 桥 → 字节预算/metrics。每个切片同时保留真实断线、重启、工具整批失败和未知结果合同。首选 bounded sender async 背压；若 producer/consumer 同 task select，生产 future 的发送等待必须允许 receiver 获得轮询。内部事件丢失不能仅作为展示 resync 处理。

本轮没有新增生产 channel。oneshot 是单值通道，watch 只有最新值；broadcast 自带固定容量（lag 必须从持久游标恢复），不作为业务事实源。
