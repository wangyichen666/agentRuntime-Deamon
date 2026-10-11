# Wave2-S11：原会话生命周期事务的异步隔离端口

日期：2026-10-08。状态：implemented / locally verified。完整 Wave2–10 尚未完成，enterprise_ready=false。

## 当前证据与实施边界

原 SessionCommand 和 SessionMetadata 已属于纯 core，无需另造 DTO 或事实。原 execute_lifecycle 事务拥有 create/close/end/fork 和 lifecycle_receipts；切片前 daemon 四类控制 caller 同步调用该事务。原 close_receipt 及 mutation 在 operation receipt 查询前没有完整 scope 校验，已存在 receipt 可能先于请求资源授权返回。两项真实 RED 已确认越域提交和越域历史返回，证据保留。

已新增 SessionLifecycleRepository async port：commit_session_command 和 read_close_receipt，每个方法都复用原 SQL/业务 owner，经共享四 permit adapter执行。原 execute_lifecycle/close_receipt 提取 scoped helper，同一 TX 在查 receipt 前验证 command source/已存在 target 的 organization/tenant/project/agent 归属。不存在的 create 仅允许当前受支持的 Local scope；完整企业资源复合键未闭合前不能借此创建远程租户或提升 readiness。

daemon create/close/end/fork 原 control_lock、cancel/等待结算、Hook publication、当前 head/lifetime CAS 和原 native writer fence 保持；只将真实 DB 调用 await 新 port。不可把异步前置查询当作提交事务最终 scope 校验，不能复制 lifecycle 状态机。新端口错误不携带 rusqlite/路径；授权、Conflict/StaleLease 与 StorageUnavailable 不靠字符串分类。

已有业务 mutation 的取消不等于回滚：开始的 SQLite 工作继续持有 permit 至真实事务结束，operation_id 的原 receipt 提供重试/重启读回。须用实际写锁/可控 barrier、故障触发器、两连接同 operation 与重启证明一次提交，不能以 mock 或 sleep 猜测。

## 兼容与验收计划

schema 保持 v25，沿用原 command/receipt JSON 和旧 Local 数据兼容规则，不新建账本，不新增依赖。原 set_preferred_session、直接维护 create/end/fork helper 和其它 native runtime 端口尚未全部 async 化，后续继续。回滚遵循 v25 与 OutcomeUnknown 的已声明兼容窗口，不原地降级旧 binary。

需包含越域/不存在、历史 receipt 授权、完整 scope 四维、operation 相同/冲突、故障整笔回滚、clone writer proof/旧 epoch、caller 取消后结果幂等与零重复 Hook、真实 daemon 控制和适用六入口读回。最终必须完整九门禁、真实 S3 四项和 PG 发布六项；本报告没有通过或完成声明。

预审补充：commit 使用 BEGIN IMMEDIATE，使两独立连接先串行取得原写事务，再判定 operation receipt；相同 operation/payload 必须一次事实，两 caller 同结果。原 RPC 的生命周期 Conflict 继续映射 -32001，StorageUnavailable/StaleLease 等保留已有 typed core 分类；不能把所有错误都降成旧 Conflict。已有 receipt 的只读重试不重新取得 writer epoch；取消后未知响应应按原 operation 读回，不能假定事务撤销。

S11 只迁移既有 mutation caller 和补事务内资源 scope，不新增远程管理 API。原中心 request guard 的 Local policy/epoch 校验保持；完整生命周期管理授权在最终事务复核、管理 audience、独立后台主体和审计仍需后续闭合，不能将本切片的 scope 合同宣称为全部企业授权。

两项有效 RED 已保留 wave2-s11-lifecycle-red.log：越域 Close 提交和已有 operation 结果越域读回。异步 commit/read_close_receipt 和原 scoped helper 已接线；首次编译接线误触两个同步 compact 查询，已恢复原路径，green 编译错误日志保留，不算功能 RED。

## 实际损坏、取消与范围证据

新增真实 result_json 损坏 RED：旧 decoder 返回其它 session 的 metadata。commit replay/read_close_receipt 现在共用 lifecycle_result_in，校验 command 输出 key 和永久 lifetime owner；不要求历史结果等于当前 head，保留退休 lifetime 的合法重试。七项生命周期合同全部通过（wave2-s11-lifecycle-result-green.log）。

storage 79 项单测通过（wave2-s11-lifecycle-storage-green.log），新增实际提交后的调用者取消合同：原 Create 已提交，blocking 工作仍被受控 barrier 保持，取消等待者不归还 permit；同 operation 重试读原 receipt，snapshot_clock 不变，Hook 一份，真实工作退出才释放预算。共享四 permit 满额同时覆盖两项 lifecycle 方法。

真实 daemon fixture 首次误用 sessions.close，正确已有方法为 session.close；首次失败不是功能 RED。修正后真实 scope RED 暴露 sessions.clear 在事务前返回“session 不存在”，与越域 Forbidden 不同；移除这条同步前置读取，使原事务统一做 scope 校验。故障 BEFORE INSERT receipt 导致完整生命周期 head/owner/Hook/receipt 回滚，返回 typed StorageUnavailable 且不泄露触发器文本。

## 本地完整门禁证据

真实 daemon 新合同 lifecycle_ports_reject_foreign_and_absent_commands_and_survive_fault_and_restart 已通过，故障 head/receipt/Hook 一笔回滚，原四类 mutation 的越域与不存在同 Forbidden，重启 close receipt 读回同 metadata，零 Provider 调用。原生命周期/六入口/取消/重启合同在完整 workspace 回归中全部通过。

wave2-s11-lifecycle-final-gates.json 完整九项均 exit0：489 Rust、36 Web、43 真实 daemon，10 项外部环境测试明确 ignored。真实 TLS MinIO 四业务项已 exit0（wave2-s11-lifecycle-regression-s3.json）；真实 PG 发布六项回归正在执行。ignored 不作为真实环境通过，PG发布协调不等于全业务PG。

最终真实环境验证闭合：[完整九门禁](evidence/wave2-s11-lifecycle-final-gates.json)、[真实 TLS S3 四项](evidence/wave2-s11-lifecycle-regression-s3.json)、[真实 PG 发布六项](evidence/wave2-s11-lifecycle-regression-postgres.json)全部 exit0。仅 locally verified；未宣称 CI/production verified，未提交、推送或部署。完整 Wave2–10、全 native async/PG、完整租户键、审计配额与离线迁移仍未完成。
