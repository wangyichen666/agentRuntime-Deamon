# 共享视图归约与恢复

schema v19 将持久事件的完整 ExactOwner、snapshot/metadata/transcript revision、projection generation、event seq、前一可见 seq 与 interaction revision 封存于 event_view_stamps。事件与最终事务版本同时提交；SHA 校验失败即报错。旧审计事件缺版本保持明确缺失，不补当前版本。返回帧上的保留元数据由 repository 构造，不信任原 data 中的同名字段。

agent_core::reduce_view 是唯一新旧比较规则，Rust DaemonClient 直接调用；Web 的串行展示适配调用同一个纯 views.reduce RPC。输入严格区分 readback/page/run/event/replay；输出 accepted/ignored/resync、展示游标与同版本补充标记。游标没有执行权限。CLI/TUI、ACP 和 Gateway/Web 共享完整身份屏障；Gateway 的协议适配消费者不持有浏览器展示 ACK。

版本倒退、重复事件和退休 lifetime 被忽略；独立 run 以完整 owner 和持久 seq 合并。真缺口先从单事务 session.load_page 建立基线，再以最多 32 条的持久页补齐仍活动 owner 的内容。ViewResynced 只是展示通知；真实 terminal 只能来自事务快照中的 canonical RunRecord。断线、归约失败、读取失败均不改变业务状态。

Rust 接收/展示队列最多 256 帧，溢出 detach 后从 durable cursor 恢复。Web 串行队列同时限制 256 帧和 8 MiB。独立展示消费者使用 fork_view；重连保留自身游标。嵌套 slash session_changed 和 compact_started 也通过同一归约；被忽略的响应只结束传输，不显示旧状态。

协议兼容：v1 additive _my_agent_view 元数据，typed RunRecord 解码先严格验证再移除传输字段；原始 transcript/审计 JSON 不改写。历史无戳事件要求持久基线，不伪造身份。ACP v1 既有入口保持；opt-in ACP v2 也使用同一 DaemonClient reducer，见 [ACP v2 API](acp-v2-api.md)。

验证覆盖领域乱序/interaction/并行 run/生命周期、双后端封存/回滚/损坏/迁移、真实 socket 丢帧与重复帧、重启和三入口同事实，以及 Web 嵌套响应/串行归约/慢消费者。具体命令见实施记录。
