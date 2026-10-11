# 企业 Wave1-S1：可信 Local 身份与准入授权

状态：**implemented / locally verified**，S1 九门禁全部退出 0。**Wave1 尚未完成，Wave2–10 未开始**。

首个纵向切片新增 agent-identity/agent-policy，core 拥有 opaque IDs、RequestContext、角色/action、条件、授权错误和 EnterpriseOwnerEnvelope；storage 仍仅依赖 core。daemon 中央 gate 在 RPC handler 前判定，chat.send 使用原 admit_run_inner 的同一事务重读身份/角色/策略并冻结授权，与原 run/turn/queue/event/RunSnapshot 一起提交。身份和 run.authorization 可从 durable DB 读回，没有第二个业务 owner。

SQLite v20→v21 新增十张身份/策略/作用域/授权表。旧 session 明确映射 Local；旧 run 没有伪造 principal。授权 snapshot SHA 绑定已冻结 permission mode、Provider route、tools、sandbox；普通策略发布不改变旧运行授权；delegation/Stop continuation 在原 child 准入事务继承核验后的父授权，工具/预算继续由原链缩窄。显式 epoch fence 接入原 turn owner、事件发布、队列启动和工具 receipt。独立工具 receipt 写入替换为同事务 fence+更新。

真实调用链：CLI identity 或 /identity、TUI slash、ACP v1 slash、ACP v2 readback extension、HTTP /api/identity、WS RPC → DaemonClient → strict normalization → central gate → scoped IdentityRepository；chat.send → authorize_local_request → RunRepository::admit_authorized_run → core AuthorizationVerifier/PolicyEngine → 同一准入事务 → immutable run_authorizations。

## RED 与合同

真实 RED：[原方法不存在](evidence/wave1-s1-red.log)，进程返回 -32601 并使行为断言失败。

- opaque_ids_reject_empty_paths_and_oversized_slugs；enterprise_owner_checks_scope_before_all_exact_owner_dimensions。
- identity::local_transport_does_not_accept_foreign_or_workload_identity。
- policy::hard_deny_and_every_condition_override_admin_roles；channel_never_grants_roles_and_versions_and_revocation_are_typed。
- protocol::legacy_and_versioned_requests_cannot_claim_principal_or_tenant；authorization_readback_requires_full_owner_and_rejects_unknown_identity_fields。
- tests/enterprise_identity_contract.rs：旧策略拒绝、普通发布/重启不改快照、跨域与不存在统一拒绝、revocation、损坏、授权插入故障整体回滚、并发同 operation 幂等、v20迁移故障/未来版本拒绝。
- policy_publish_during_admission_cannot_commit_stale_authorization：两组 barrier 控制真实外部 WAL 写入；旧读事务不能升级写事务提交旧授权，零 run/session/auth 写入。
- 真实 daemon enterprise_identity_is_canonical_and_survives_daemon_restart：CLI/ACP v1/v2/HTTP/WS 同身份；ACP v2/HTTP/WS 同 error code/data；fake tenant 被拒绝；读取不推进 snapshot clock/event；重启快照相同，Provider 次数不增。

首次测试失败及修复日志保留：旧迁移夹具必须删除新增 v21 表以真实回到旧版；opaque ID 测试改显式 String；stale-policy 用例保留正确 request ID；/identity 放 slash 末尾保留 TUI 顺序；daemon 合同显式创建 session，未放宽原准入。

## 边界与回滚

这是 Local 身份与授权事务切片，不是完整 Enterprise 安全模型。完整租户资源 ports、双租户同名资源 list/read/mutate、独立 Admin 身份、双人批准、compact/cron 等后台授权、撤销管理/执行体收敛与系统审计尚待实现。Enterprise listener 未提供，enterprise_ready=false；所有 Wave1 需求行仍保留 proposed 并写明 partial 证据。

无新第三方依赖，Cargo.lock 只增加两个内部包和依赖边。没有提交/推送/部署。新程序拒绝未来 schema；回滚旧 binary 必须恢复经过 SHA 校验的迁移前备份，不对 v21 库做伪降级。真实 IdP/PostgreSQL/容器/K8s/商业Provider尚未用于本切片验证。

## 最终门禁

[evidence/wave1-s1-final-gates.json](evidence/wave1-s1-final-gates.json) 记录九条完整命令、退出码和耗时，全部为 0。Rust 402 项（20 架构、32 真实 daemon、7 新 SQLite 授权合同）与 Web36；fmt、严格 Clippy、MSRV1.88、release、JS、在线 cargo-deny、git diff 均通过。所有早期失败日志仍保留，未修改旧安全/副作用断言换绿。

新事务时间合同证明不使用请求中过期的时间声明：纯策略对旧 timestamp 可允许，canonical 准入取锁后用当前服务器时间重判并拒绝零写入。所有第三方 package/source/checksum/dependency 条目与 HEAD 锁文件完全相同，只增加内部 identity/policy 包和依赖边。

新根 chat 准入读取当前策略；derived delegation/Stop continuation 继承父冻结授权与既有缩窄规则，尚未完成独立企业子任务策略发布。撤销管理 API、后台取消/unknown 收敛、拒绝审计、全租户查询与双审批需后续 Wave1 切片；不能进入 Wave2 验收。
