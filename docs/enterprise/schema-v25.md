# SQLite v25：永久 lifetime 归属

状态：implemented；本机验收见 [S8 报告](wave2-s8-readback-report.md)。不代表完整 PostgreSQL schema 已完成。

`session_heads.lifetime` 原本已经有 UNIQUE，保证当前 head 不重名。v25 新增 `session_lifetime_owners`，永久记录 lifetime 对应的原 session。它是原 transcript/plan 键的归属证据，不保存第二份会话、run 或终态。归属由 head 创建/更新的同一事务记录，禁止 UPDATE/DELETE。历史行不会随 clear/delete 被重新分配。

迁移在 BEGIN IMMEDIATE 内从 heads、runs、lifecycle receipts 的原 metadata 并集提取归属。一个 lifetime 存在两个不同 session 时整个迁移失败。无 run 的旧 transcript/plan 已退休且缺少可证明 owner 时保留原内容，记录 session_id=NULL / provenance=legacy_orphan，禁止自动绑定或暴露内容。该证据可供离线审查，不能通过普通读回自动修复。

head 插入或改变 lifetime 时先检查永久归属，再 INSERT OR IGNORE 本 session 的记录。原 run incarnation 初始化仍可把空 lifetime 写成当前 head；已经初始化的 run 禁止改 session_id/lifetime。原事件、transcript 和工具事务不变。读取在同一事务内先用 organization/tenant/project/agent/session SQL 取得 metadata，再核永久归属；transcript/plan 查询 JOIN 原 lifetime owner，run/queue/interaction/ledger 查询同时限制 session 与 lifetime。

锁序：SQLite adapter 的共享四 permit → 原 Connection mutex → 单个只读或业务事务。读回没有 view publication，不推进 snapshot_clock。列表的 scope、游标和 limit 均在 SQL 内执行，摘要和预览在同一事务中读取，不逐条拼接另一份 readback。取消只 detach 等待，实际 SQLite 工作结束前不释放 permit。

升级前使用原 VACUUM INTO 备份及 integrity/version/SHA-256 校验。v24→v25 故障零新表、零 schema marker、零原业务事实修改；拒绝 schema >25。兼容窗口为停准入 → 收敛并备份 → 前向迁移 → v25 binary 启动。旧 v24 binary 必须拒绝 v25；回退要停机恢复 v24 已校验备份，不能删除 migration 行或在活库移除触发器。新增 Provider OutcomeUnknown 枚举的兼容限制仍适用。

这是 Local 现有全局 session key 的前向硬化。它没有把全部旧业务表改成可跨租户复用同一 session key 的复合主键，也没有实现全业务 PG、PG native writer、审计/配额账本或 SQLite→PG 离线迁移；Enterprise 仍未开放。
