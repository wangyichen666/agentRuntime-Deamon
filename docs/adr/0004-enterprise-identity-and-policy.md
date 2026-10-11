# ADR 0004：企业身份与事务授权边界

日期 2026-10-07。状态 **partially implemented**；Wave1-S1 接入 Local 身份、纯 RBAC/条件判定和 v21 准入授权事务。完整 Enterprise 租户治理仍 proposed。基线 [企业证据表](../enterprise/baseline.md)。

## 问题与决策

现有 ExactOwner 只有 session/lifetime/run/generation/turn，SafetyPolicy 是工作区文件/命令安全模式，不是主体授权。新增 agent-identity/agent-policy 必须在首个纵向切片就有 daemon/repository 调用者。opaque OrganizationId/TenantId/ProjectId/SubjectId/ServiceAccountId/AgentUid/PolicyVersion 与 slug 分离；不能将 session filename、workspace path、memory.project 或 namespace 名用作租户主键。

层级 org→tenant→project→agent→session→run/artifact，角色 org_admin/tenant_admin/project_admin/agent_operator/developer/auditor/viewer，动作以 resource/action 类型表示。权限判定固定次序：可信主体/scope→hard deny→绑定角色 action→条件 tenant/project/agent/workspace/channel/tool effect/time/network→sandbox/secret/path hard constraints。channel 标签只参与条件，永不赋权；缺条件信息默认拒绝。

Server 创建 `AuthenticatedPrincipal + TenantContext + RequestId + TraceContext`，不把请求 params 中的 tenant/principal 直接塞入。Local UDS 由当前 server workspace 建立保留 local org/tenant/project/agent 和本地主体；不要求外部身份服务，不允许普通远程请求自行选择 Local 超级权限。Enterprise listener 单独 config/认证，未配置信任链不能降级到 Local。

## 持久事实与真实调用链要求

身份/role bindings/policy versions 与 existing session/run facts 在选定 canonical DB。session scope 在创建/准入 transaction 固定；repository query 参数是已认证 scope，SQL WHERE/JOIN 和所有 unique/FK 包含 tenant/project/agent 域。旧本地数据前进 migration 明确映射保留 local scope，不能在查询时用 default 兜底缺失的企业身份。

入口认证→daemon dispatch centralized policy→scoped repository command transaction→复核当前 owner/lifetime/generation/revision/policy/授权 snapshot→mutation+audit→commit。读取也必须在数据库 tenant scope 限定后才返回，不能 global ID 查出后内存过滤。跨域与不可见/不存在返回同外部 code/message，内部 audit 记录不同原因。

run admission 保存不可变 AuthorizationSnapshot（principal、role set、policy version/digest、scope、permission mode），再保留现有 route/catalog/sandbox/context snapshot。中途 policy 更新只影响新 run；显式 revocation 有持久记录和当前执行体可核对 token，旧 snapshot 回调需按 revocation fence 零写入，不能把“冻结”理解为永久免撤销。

审批复用 interactions owner+revision CAS，审批人必须拥有 interaction.respond 和被批准 action；高风险可配置 separation-of-duties，多 distinct SubjectId 投票由同一 interaction state 扩展，不能新建私有 approval owner。断线/timeout 不能补第二票或自动拒绝。

## 并发、失败与兼容

锁序沿用 session control barrier→repository transaction；policy 查验和 freeze 在同事务，不跨服务先验后写。policy version/owner/revision 任一 stale 即 typed conflict/stale 零写入；auth 拒绝也在系统 audit lane 落地，不依赖被拒租户数据权限。Local legacy DTO 保持；Enterprise 字段采用新协商能力/专用可信 transport envelope，不放宽 deny_unknown_fields。

首次 schema 版本由 slice 声明，v20 原表不能随意重建破坏 transcript/receipt；切换前备份与 dry-run，旧 binary 对未来 schema 拒绝。无 DB 降级回滚：恢复事先备份或使用兼容新 schema 的新 binary。

## 必需证据

双租户同名 agent/session/artifact 在 scoped SQL list/read/mutate 不可见；旧 lifetime/generation/policy/revision 和跨域回包零写；授权策略表驱动/role hard deny/工具权限双审批；三入口一致错误和审计；重启冻结 snapshot；未知字段拒绝。S1 已有 Local 真实三入口/restart、策略冻结、撤销 owner fence、并发 operation 与事务/迁移故障合同，见 [S1 报告](../enterprise/wave1-s1-report.md)。双租户全资源隔离、双人审批和授权拒绝系统审计仍 **未实现/未验证**，不可用 S1 或旧 ExactOwner 测试替代。

## Wave1-S1 实际事务与范围

core 拥有企业 DTO 和同步 `AuthorizationVerifier` 领域端口；identity 仅绑定可信 Local 连接，policy 实现拒绝优先的纯判定。storage 仍仅依赖 core，daemon 将 `PolicyEngine` 注入已存在的 `admit_run_inner`。数据库先 scoped 查询 principals/role bindings/policy head；同事务复核 expected policy、principal epoch、Local scope，创建既有 run/turn/snapshot 后插入不可变 run_authorizations。没有新的 run/approval 状态 owner。Snapshot SHA 引用原 RunSnapshot（权限/route/tools/sandbox）并冻结完整 ExactOwner、身份、角色和策略。

SQLite 为 WAL 单连接锁→事务→纯 verifier；不得在生产 verifier 中做 I/O、await 或反向获取仓储锁。外部连接在读事务期间发布新策略会令旧事务升级写锁失败，整体零写入；使用两组 barrier 的真实并发合同证明，不靠 sleep。普通 policy head 发布不重写旧授权，revocation_epoch 变更使旧 owner 在队列启动、event publication、turn fence 和工具 receipt 事务拒绝。撤销的管理 API、活动执行体收敛与审计留待后续切片，不能称已实现完整 revocation 管理。

当前只能服务器保留的 Local scope；请求中的主体/tenant/project 字段被 strict decoder 拒绝。`enterprise_ready=false` 是实际边界，不存在 Enterprise fallback。旧 v20 session 前进迁移为 Local；旧 run 授权读回返回 snapshot=null，不补造主体历史。delegation/Stop continuation 已在原事务继承父授权；MCP/cron/compact 的完整后台授权、全资源 scoped ports 与双人批准仍待后续切片。
