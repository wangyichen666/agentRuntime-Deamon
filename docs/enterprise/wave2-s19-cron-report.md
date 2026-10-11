# Wave2-S19 Cron 固定准入 owner 与独立后台主体

状态：implemented / locally verified。完整 Wave2–10 尚未完成，enterprise_ready=false，当前 SQLite schema27。

schema27 在原 principals 和 role_bindings 建立 Local Cron workload 主体，使用已有 developer 角色，不借用交互用户身份，不声称是专门的最小权限角色或远程 workload token。daemon 从同一个 canonical RunStore 注入执行、身份、尝试、工具、turn、请求端口及策略 verifier。独立 runner 必须显式绑定这些依赖，不能在运行时另开未配置的 SQLite。

admit_workload_run 复用唯一原准入 body 和四 permit。原 SessionCreate 与 SessionPrompt 授权、session/lifetime/SessionStart/writer、run、冻结授权、快照及固定 owner 在一个 BEGIN IMMEDIATE 事务内提交；现有会话先核完整 scope。运行开始、模型捕获、工具/计划和终态都携带准入时的 envelope，不重取晚到结果对应的 owner。未知副作用仍返回 typed unknown，不自动重放。

两个有效业务 RED 见 wave2-s19-cron-functional-red.log：旧 Cron 迟到结果把新 generation 结算为 failed，后台运行没有自己的冻结授权记录。锁场景原实现先因 Deferred 升级产生 SQLITE_BUSY，未达到 timer 断言，不计异步 timer RED。修后上述两项及真实写锁下 current_thread timer/原未知副作用合同通过。

七项 workload 存储合同已通过（wave2-s19-workload-contract-green.log），覆盖双 action 校验、冻结授权故障全事务回滚、身份维度、同请求幂等、撤销零写、schema27 备份/回滚/未来28拒绝；新增 foreign session 合同也通过。Cron 初次11项中两个独立连接持久历史读取因旧同步 snapshot 要求 writer 而失败，改用已有真正只读 canonical SessionReadRepository 保留原消息、独立 occurrence 和持久化断言，完整门禁复验通过。首次 Arc 限定遗漏编译错误单独保留，不计功能 RED。

当前仅 Local 后台身份及原运行链路闭合；SessionStore 的本地文件构造、其他 compact/memory/Hook/delegation/resource 端口仍有同步欠账。全部 native PostgreSQL facts/租户键/writer/outbox/audit/quota/offline 迁移与 Wave3–10 均未完成。


S17–S19 implemented / locally verified：wave2-s17-s19-reviewed-final 九项全部exit0，588 Rust / 36 Web / 48真实daemon，42个test executables、10外部环境项明确ignored；runtime136/storage83、request15/event12/workload7合同全部通过。独立真实TLS MinIO四组业务与PostgreSQL发布协调六项全部exit0（wave2-s17-s19-reviewed-regression-s3.json / postgres.json，PG runner五步骤不等于五合同）。S17三请求端口实际模型/摘要/读回await；S18原events不可变来源及native缺stampCorrupt；S19原同TX后台准入、固定owner及Local独立workload。schema27，不新增第三方包。首轮Clippy常量放testmodule后失败、foreign夹具writerFK失败及0tests过滤错误均保留，不计功能RED；移动常量后完整九项重跑，未放宽lint/超时/栈/架构。全企业Wave2、Wave3–10仍未完成，enterprise_ready=false，未提交推送部署。继续S20 compact实际RED与原唯一helper迁移。
