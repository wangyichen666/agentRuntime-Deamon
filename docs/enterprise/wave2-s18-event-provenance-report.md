# Wave2-S18 原事件来源与损坏校验

状态：implemented / locally verified。完整 Wave2–10 尚未完成，enterprise_ready=false。

schema26 在原 events 增加永久 view_provenance。迁移时原 stamp 或 outbox 任一证据存在的行标为 native，其余为 legacy；不能据此识别迁移前已被删除全部证据的 native 行。新事件只允许 native，来源不可修改，已发布 stamp 不可删除。不新增事件账本，原业务事务、outbox 和 publisher 保持同一个事实源。

读取在同一事务核来源、SQL 行身份、发布状态、字节预算、JSON owner 和摘要。native 缺 stamp 返回 typed Corrupt，不能降级为没有企业 view 的成功事件。legacy 无 stamp 及不存在事件保留原兼容语义。测试损坏解码时显式撤删除保护，避免把数据库拒绝 DELETE 冒充 decoder 证据。

两个有效功能 RED 保留于 wave2-s18-event-provenance-red.log：分页在删除 native stamp 后仍成功，直接 stamp 读取返回 None。修后12事件合同全部通过（wave2-s18-event-provenance-reviewed-green.log），包括不可变来源、stamp 字节预算、v25 三种迁移来源、备份 SHA/字节及完整性、迁移中断零半写和未来 schema 拒绝。83 storage 单测及五组旧迁移合同通过。一次测试漏导入 MaintenanceRepository 的编译错误单独保留，不计功能 RED。

该迁移是 SQLite 原事实的保护；PostgreSQL 仍仅完成发布协调端口，不能证明全部业务事实、writer 或离线迁移完成。后续 S19 使当前程序 schema 升至27，旧版本测试明确撤除相应迁移，不伪造当前 schema。


S17–S19 implemented / locally verified：wave2-s17-s19-reviewed-final 九项全部exit0，588 Rust / 36 Web / 48真实daemon，42个test executables、10外部环境项明确ignored；runtime136/storage83、request15/event12/workload7合同全部通过。独立真实TLS MinIO四组业务与PostgreSQL发布协调六项全部exit0（wave2-s17-s19-reviewed-regression-s3.json / postgres.json，PG runner五步骤不等于五合同）。S17三请求端口实际模型/摘要/读回await；S18原events不可变来源及native缺stampCorrupt；S19原同TX后台准入、固定owner及Local独立workload。schema27，不新增第三方包。首轮Clippy常量放testmodule后失败、foreign夹具writerFK失败及0tests过滤错误均保留，不计功能RED；移动常量后完整九项重跑，未放宽lint/超时/栈/架构。全企业Wave2、Wave3–10仍未完成，enterprise_ready=false，未提交推送部署。继续S20 compact实际RED与原唯一helper迁移。
