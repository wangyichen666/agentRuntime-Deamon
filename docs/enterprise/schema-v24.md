# SQLite v24：原 session 的 native writer 凭据

v23→v24 在 BEGIN IMMEDIATE 内增加 `session_writer_leases`；全部原 run、turn、tool、interaction、resource、hook、transcript 状态仍由原表拥有。租约不是第二业务状态机，delivery publisher 租约不能代替 native writer。

| 字段或约束 | 事实 |
|---|---|
| organization/tenant/project/agent/session/lifetime | 完整域复合主键，独立于发布协调表 |
| organization/tenant | FK tenants 的完整组织域 |
| tenant/project/agent/session | FK 原 session_scopes，禁止跨域行 |
| owner_id | 随机进程实例 nonce，PID 不是所有权证明 |
| epoch | 正整数；到期或释放后接管递增，同 owner 有效续租保持 epoch |
| expires_at_ms | SQLite DB 时间计算；不相信请求携带的时钟 |
| expiry index | 完整租户域前缀；不按全局 session ID 猜授权 |

两个复合唯一索引为 FK 提供准确父键。旧 lifetime 的 lease 留存；claim 复核当前 session head 的 lifetime，不能凭历史行取得新会话写入权。已删除 head 仍保护生命周期 Hook 的历史收据。重新创建、clear、fork 在原事务校验 source/旧 lifetime 的 proof 后取得新 lifetime。

`temp.writer_context` 和 `temp.writer_grants` 只缓存本连接已经获得的随机实例与 epoch proof，不保存业务状态。RunStore clone 共享同一物理连接，不能把它复制成另一个 owner。过期 proof 不在原进程自动 acquire 新 epoch。未启用 writer 的连接在数据库已有 native lease 后不得继续 mutation/recovery，也不能通过另一个 session 绕过。

Local 写锁顺序是连接 mutex→BEGIN IMMEDIATE/原事务→session scope/head→native lease→原领域状态→event/ViewStamp/outbox→提交前再次检查 proof。网络 I/O 不持此锁。对象存储 I/O 前后分别在 SQLite 事务复核。实际对象已写入而 writer 失效时，原 prepared/native receipt 保持证据，不能补造成功或重放工具。

未持 native lease 的活 run/resource/Hook 存在时首次启用返回 Conflict，恢复事务内再次预检；不能通过配置切换猜测活旧 owner 已崩溃。默认 Local 没有任何租约行时保持原行为。显式 `AGENT_SESSION_WRITER=1` 为本地工作区启用保护；启动恢复按稳定 session_key 顺序取得所有当前 head（包括 tombstone），最大 1024 个。缓存最多 1024 当前 proof；clear/recreate 移除旧缓存而保留 DB 历史行。该边界是 SQLite 单工作区恢复保护，尚不是 PostgreSQL 多 worker HA。不能将 v24 标成全业务 PG schema。

升级前产生并校验 `runtime.v23.backup.sqlite3` 与 SHA marker；迁移插入故障回滚表和索引。schema >24 拒绝。v23 备份可由新版恢复后再前向升级。回退旧 binary 必须停进程并恢复升级前 v23 DB 与匹配的 File/S3 对象；已产生 v24 业务写之后不能直接恢复旧备份继续服务，否则丢失这些写入。不能通过删除租约行或 unset flag 宣称安全回滚。
