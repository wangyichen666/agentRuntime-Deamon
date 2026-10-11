# ADR 0008：统一审计、遥测、成本与留存

日期 2026-10-07。状态 **proposed**；现有 tracing、tool/provider/hook receipts 仅局部证据，不是统一 audit/OTLP/费用账本。

## 事实与接口

跨 Gateway→control-plane→daemon→runtime→provider/tool/MCP/worker 传递标准 trace context/request_id；OTel/OTLP 为可替换开放端口，Prometheus/Loki/Tempo/Jaeger 是部署示例。span 只含 ID/digest/分类，不含 prompt/token/完整工具参数。exporter bounded、背压/丢弃指标与超时明确；telemetry 故障不伪造业务 terminal。

AuditEvent 含 actor/scope/resource/action/decision/policy version/request/trace/exact owner/before+after digest/outcome/time。allow/deny 都写 append-only canonical audit；拒绝事件走系统审计 lane，不能以租户不可见为理由遗漏。mutations、audit/export outbox、quota/usage reservation 同事务；不能在响应发出后再异步补最关键证据。

敏感字段递归脱敏并限制总大小/层深，再持久；管理员也不能从 audit API 恢复 secret。query 在 SQL 限定 tenant/action/subject/resource/time，稳定 cursor/有限页，audit.export 是独立资源权限。trace/logs/redaction 共享规则与注入假 token oracle。

## 防篡改与一致性

批次 canonical serialization→prev_hash+events_hash→Ed25519 signature→可检验导出；批次范围/排序/hash 与 signing key version 持久。outbox 至少一次导出按 batch_id/digest 去重，重复写不是新 audit event。WORM 通过公开 Object Lock/兼容接口部署；本地文件只提供篡改可检测，不能声称管理员无法删除。

锁序 tenant ledger/audit head→业务 mutation receipt→audit append→export outbox；不能在 transaction 内等 OTLP/S3。多实例分配稳定 audit 序列/hash head 必须数据库 CAS，不让各副本内存独立链。签名异步任务复核 immutable batch digest/version，密钥不进入普通 DB DTO/log。

## Quota、usage、删除

Provider usage/可标记估算与 sandbox资源生成 canonical cost ledger，按 tenant/project/agent/model 聚合。hard quota admission 在事务预留，终态结算；soft quota 告警，缓存/UI 不决定准入。未知 usage 保留估算等级，不能假造精确商业账单。

tenant retention 分 transcript/artifact/audit，legal hold 优先；删除/匿名化需审批/tombstone/证据，不能破坏保留 audit chain。密文/分离内容和删除证明的设计要能说明保留 digest 的来源；对象 GC 不可跳过 hold。Local schema/profile 默认保持现有证据，不自动清理用户历史。

## 失败、回滚与证据

audit 写失败导致受审计 mutation 拒绝/回滚，exporter 暂停只积累有界 outbox/lag alarm，不能丢已提交 audit。预算/retention policy 更新版本化只影响新 decision，删除无法用“roll back”伪装复原；导出重试幂等，不执行第二业务副作用。

必须验证同 request/trace 串联 full owner/provider/tool/artifact/terminal，假 API key/Bearer/password 在日志/trace/audit 全部脱敏；改历史记录 hash-chain 检出；双租户 query/export 无泄漏；quota race 无超卖；hold/deletion 证据。实际 OTLP/真实 WORM/商业费用/法务流程 **未验证**，文档不做生产保证。
