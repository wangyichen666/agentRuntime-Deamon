# Wave2-S22 原实时事件写入异步化

状态：已实现并完成本地验收。S22/S23统一九门禁全部通过，636 Rust/36 Web/48实测daemon，44执行体、10外部环境项明确ignored；runtime137/storage86/context19/discovery14/event23全部通过。真实TLS S3四组业务、PostgreSQL发布协调六项全部通过，自己的容器及独立DB已清理。schema27，完整Wave2–10未完成，enterprise_ready=false。

最终证据：`wave2-s22-s23-reviewed-corrected-final-gates.json` 与 `wave2-s22-s23-reviewed-regression-s3.json` / `postgres.json`。首次九门禁8/9，仅新发现合同5处多余Copy.clone导致Clippy失败；去掉冗余调用，原断言及lint强度保持，首次失败保留。

`RunEventRepository.append_run_live_event` 已接 daemon 五个原 publish_update caller，传入原准入事务捕获的固定 envelope。原 BEGIN IMMEDIATE 内复核完整 scope、ExactOwner、head/lifetime、writer proof 和冻结授权；live 事件只允许 running/waiting_interaction。事件、interaction、stamp 和 outbox 仍由原 append_event_scoped/insert_event/views.commit 同事务承担。七个实际事件名称、object 与字节预算在进入共享四permit前检查；留16KiB给原封存stamp和响应元数据，超过预算明确拒绝。原Provider路由事件同helper继续保留其专用端口，未错误套用live allowlist。

原前台开始执行计划后读取历史已改 await 原 read_execution_context，核固定准入身份。publish_update 在 active mutex 外等待数据库，读回原精确 seq/stamp；更新活动投影前再次核 run_id 等于捕获 owner。Approval观察Hook也沿用捕获 owner，但Hook领域本身仍同步，不声称所有链路已异步。

原兼容同步 append_event 先按Local scope在SQL拒绝跨域。其run_id API保留维护语义，不能据此提供调用方的旧generation证明；线上caller已迁移到有固定envelope的端口。原冻结授权SHA已有保护，不声称这些合同绕过了该SHA。

有效RED：live-events-functional-red.log复现foreign兼容写入成功EventSeq(2)，以及Queued旧run的generation缺口；后者不能冒充真实Running执行callback。live-running-owner-functional-red.log明确先真实start，再一致更新run/head generation，原Local兼容helper仍成功EventSeq(3)。GREEN以线上实际fixed-envelope端口验证相同拒绝和零写入，不改变拒绝预期。

局部GREEN：storage-corrected-green.log 85单测、events-reviewed-green.log 23事件合同；九个scope/owner维度的typed错误、terminal/非法名称/非object/字节预算、原interaction及event/stamp/outbox故障完全回滚、两独立Connection受控屏障写入唯一连续seq且原stamp/outbox逐条一致。真实连接被占有时current_thread25ms timer<150ms；取消awaiter后worker保留一个许可，释放连接后恰好提交一条事件并归还全部许可。新方法也进入原共享满额四permit合同。

保留失败：初GREEN命令将根测试目标放在agent-storage package，Cargo未执行任何合同；原锁夹具默认Queue被正确拒写，修为真实start，未修改生产status guard、timer、预算、栈、超时或并行度。日志均保留，不计这些夹具/命令错误为功能RED。

本批无新schema或第三方包。全业务async/native PostgreSQL facts/租户键/PG writer/audit/quota/offline迁移继续待完成。回滚需停止准入、join执行并保留校验备份，schema不变不意味着旧binary具备本批owner保护；未提交、推送或部署。
