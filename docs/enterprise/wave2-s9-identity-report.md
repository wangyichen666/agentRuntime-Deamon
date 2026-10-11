# Wave2-S9：中心身份与冻结授权的异步端口

日期：2026-10-08。状态：implemented / locally verified。完整 Wave2–10 尚未完成，enterprise_ready=false。

## 范围与兼容

修改 core/storage-ports/storage/daemon 和原合同；schema 保持 v25，不新增业务表/主体/策略 owner，不引入外部包。新增纯 core RepositoryError，仅封装原 AuthorizationError/CoordinationError，不包含 rusqlite、SQL、内部路径或凭据。IdentityReadRepository 两个方法均复用原 SQLite 单事务与共享四 permit adapter；预算/驱动错误保留 StorageUnavailable，不改成 Forbidden/Internal。

daemon 的中心 request guard、foreground 准入前身份、identity.readback、run.authorization、/identity 和 /artifact 的授权路径全部 await 原同一 RunStore 实例。async 前置授权不能替代准入事务：原 admit_authorized_run 中 verifier、expected_policy/epoch/时间复核、冻结 snapshot、ExactOwner/native writer fence均继续执行。身份 timestamp 在 await 之后采样，排队时间不能变成陈旧时间声明。

旧同步 IdentityRepository 保留给尚未迁移的 Local/维护调用者和原事务内实现；在线 daemon 身份读路径不再调用它。完整 native async ports/全部 PG 合同仍在后续。

原授权读回强化为 scoped owner SQL JOIN：org/tenant/project/agent/session/run 在数据库内约束，不存在/越域统一 Forbidden，generation/turn/lifetime 及当前 head 不一致是 typed StaleOwner，原 writer proof 仍检查。返回原 immutable AuthorizationSnapshot，普通 policy 发布不改旧 run，revocation_epoch 变化拒绝旧 run。它不生成第二份授权事实。

## RED、合同与调用证据

`wave2-s9-identity-red.log` 在 current_thread runtime 中，由真实原 Connection 持有者发送已占有通知；旧同步身份查询阻塞执行线程，100ms timeout 得到 Ok(identity)，RED成立。GREEN 改用实际新端口，timer 可以到期，取消后的真实工作保持 permit 到结束；主动释放连接后等待预算恢复，并验证下一次真实身份读取成功。持有线程有有界失败保护，不用 sleep 猜竞争。

共享取消/过载合同补 IdentityReadRepository 两个方法：四个真实 adapter 工作在异步 caller 取消后仍占满预算，新 identity/authorization 返回 core StorageUnavailable，直到实际工作退出。保持原 Publication/Writer/Artifact/SessionRead 端口的同一预算测试。

新增 async_identity_and_frozen_authorization_preserve_scope_epoch_and_exact_owner_errors 与 async_authorization_uses_shared_native_writer_proof_and_never_reacquires_expired_epoch：完整 scope 四维、不存在、exact generation/turn/lifetime、policy 新旧两代、revocation、实际 SQL 故障脱敏、clone 共享 proof、过期不重新 acquire、event/clock 零写。原策略发布 barrier、时间过期、同 operation 并发、授权插入故障、备份/迁移合同均保留；九项身份合同通过。

故障 fixture 最初 DROP principals 被 SQLite 外键拒绝，该失败不算功能 RED。修复只在隔离测试连接关闭 foreign_keys 后注入缺表，应用连接仍保留外键，验证 SQL 缺表转换为稳定 StorageUnavailable。

真实进程 central_async_authorization_storage_fault_preserves_typed_error_and_zero_admission 验证中心 guard 在实际数据库故障时拒绝 chat/session/run authorization：RPC/CLI/ACP v1/v2/HTTP/WS 同 code/data，HTTP503，SQL内部表名不可见，零 run/零新会话/clock不变/Provider调用零次。S8 scoped 六入口与重启、原身份六入口/冻结授权重启继续回归。

## 门禁、运维与限制

最终九门禁 [wave2-s9-identity-final-gates.json](evidence/wave2-s9-identity-final-gates.json) 全部 exit0：471 Rust、36 Web、10 项外部环境测试显式 ignored；包括 40 项真实 daemon 合同。独立真实 TLS MinIO 四项全部通过：[S3 回归证据](evidence/wave2-s9-identity-regression-s3.json)。独立真实 PG 发布六项全部通过：[PG 回归证据](evidence/wave2-s9-identity-regression-postgres.json)。这些证据不代表全业务 PostgreSQL 已实现或验收。没有提交、推送或部署；不声称 CI/production verified。原可复现 scripts 保留。

schema25兼容窗口/备份恢复遵循 schema-v25：旧 binary 不可原地写升级库。本切片无新的 migration，错误分类/接口只影响当前 binary，回滚仍需支持 schema25 与 Provider OutcomeUnknown 的兼容 binary 或停机恢复备份。

这不是完整企业错误分类、OIDC、服务账号禁用、审计/配额实现，也不是全业务 native PG/完整 Wave2。其余 synchronous runtime/lifecycle/resource 等调用者、PG facts/复合租户键/原事务/PG writer、审计配额账本与离线迁移仍待实施；Wave3–10 依完整 Wave2 门禁顺序继续，禁止提升 readiness。
