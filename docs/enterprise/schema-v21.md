# SQLite v21：Local 企业身份与授权

这是 Wave1-S1 的前进迁移，v20 的全部业务表继续是事实源。新增表由 storage/enterprise.rs 和原 RunStore 连接持有，没有新 session/run owner。

| 表 | 事实与键 | 事务边界 |
|---|---|---|
| organizations | opaque organization ID | 一次 v21 migration 初始化 Local |
| tenants | organization 下的 opaque tenant | FK organization |
| projects | tenant 下 project | tenant/project 唯一与 FK |
| agents | tenant/project 下 AgentUid | 完整域 FK |
| principals | tenant/subject、类型、revocation epoch | 准入/owner fence 事务重读 |
| role_bindings | tenant/project/agent/subject/role | 完整域查询，不在内存过滤 |
| identity_policies | 域/version、严格 PolicyDocument | 版本不可 UPDATE，准入读取 |
| identity_policy_heads | 域/current version | 指向同域存在的版本 |
| session_scopes | 既有 session 的不可变域映射 | 迁移回填 Local；创建 trigger 固定 Local |
| run_authorizations | 既有 run、scope、subject、epoch、完整快照/SHA | 与原 run/turn/route/snapshot/queue/event 一起提交 |

run snapshot 保存 permission mode、Provider route、tools/catalog、sandbox、context policy 等；AuthorizationSnapshot 保存原 snapshot 的 SHA、EnterpriseOwnerEnvelope、RequestContext、roles、完整策略与 digest、准入属性与 action。后台历史 run 不伪造主体，snapshot=null 明确表示缺少授权快照。

读取 identity 和 run.authorization 使用同一只读事务，不发布 event 或 snapshot_clock。run.authorization 的 scope 来自可信服务器；RPC 参数只有完整 ExactOwner，没有 tenant/principal 选择器。角色列表以数据库排序稳定返回。

普通策略发布只更新 head，不改旧 run 快照；显式 principal epoch 变化使旧 run callback 被拒绝。当前没有对外发布/撤销管理 API。政策条件支持 scope/workspace/channel/tool effect/time/network；channel 本身不产生角色。请求参数中自报身份字段，即使 legacy JSON-RPC，也会在 decoder 阶段拒绝。

迁移前沿用 VACUUM INTO、integrity_check、版本及 SHA 备份校验。v21 migration 单独 BEGIN IMMEDIATE/COMMIT；任何 DDL/种子/version 写入故障全部回滚。程序拒绝 schema>21。回滚到旧程序必须恢复备份，不能删除 schema version 或对 v21 库运行 v20 binary。

当前只运行保留 Local 域。原全资源 repository ports 仍需后续逐项升级；未提供 Enterprise listener，也未声称双租户 session/artifact 查询已经隔离。
