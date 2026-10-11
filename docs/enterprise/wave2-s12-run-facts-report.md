状态：implemented / locally verified。完整 Wave2–10 尚未完成，enterprise_ready=false。

# Wave2-S12：原 run 控制投影的异步隔离端口

run.read 当前先run_view_readback再多次独立snapshot/compact/Hook查询，不在同一事务，且无完整scope；run.tools和run.provider_attempts同样同步读全局run_id。此为实际生产caller和canonical事实，必须迁移。

core纯RunFactReadback/RunProviderReadback；RunFactRepository三方法实际daemon caller await同RunStore四permit，原SQL/decoder抽共用connection helper，无复制状态机。原View/Turn/Run同步维护接口继续委托相同helper。scope gate复用唯一check_run_scope_in(SQL org/tenant/project/agent)。读回保留退休life永久owner验证，不要求当前head/writer，不重新租约，不写clock/outbox。run fact/冻结snapshot/compactlinkage/continuation在一只读TX内，原wire shape保持。

每响应受1MiB/最多1000row约束；数据库先按BLOB长度核验原raw数据，再decode/serialize总预算，超限typedInvalidRequest，不静默丢记录或推进cursor。原 JSON/attempt身份/永久owner损坏failCorrupt，driver/共享budget typedUnavailable；errors不泄 SQL。

必须实际run越域RED、源helper单事务/重启/退休lifetime/四维scope/损坏/预算/SQLiteConnectioncurrentthreadtimer/取消过载GREEN、真实六入口既有run读回/Provider零重放、九门禁及TLS S3/PG回归。未迁移discovery读取/原admission/terminal/其余mutation不可称全nativeasync；完整PG/schema/tenant键/audit/quota/offline继续主线。S10 provenance缺口尚未闭合，另作真实原表迁移，不能宣称全部事件防篡改。

审阅补充：三个原RPC必须保留wire字段；纯core端口用结构DTO，daemon仅序列化投影。provider decoder要校验SQL attempt_id/run_id/round/candidate_index/status与JSON一致，避免内层JSON伪造另一run。continuation两端均须同scope和永久life归属，否则Corrupt；预算应包括所有实际返回的字符串列，数据库预检后再序列化总预算。discovery原读取当前head fence，与退休不可变历史语义有差异，先如实保留其未迁移范围，不借此绕过原许可校验。

实际 RED 两项为越域 run 私密输出、Provider JSON round 与 SQL 不同仍被返回；已转 GREEN。额外 continuation 故障 RED 为当前新 port 返回另一 scope 的 child run id，现已校验关联两端范围、永久 lifetime、缺失和自环并拒绝。七项合同全部通过。storage 80 项全部通过，新增真实 Connection 占有/current_thread 100ms timer/取消后 permit 保持，三新方法共享四 permit 满额合同。

实现保持 schema25，不新增依赖。run.read、run.tools、run.provider_attempts 和原 slash /run 均经相同纯core port；CLI run-read --view fact/tools/provider、HTTP POST /api/runs/read/tools/provider、ACP2 _my_agent/run/read/tools/provider 加入 readback capability，ACP1 /run 显式视图和原WS/RPC调用同daemon handler。原 /run 默认摘要必须保留，compact默认原投影也需共用纯序列化helper。接线首次重复新增已有 /run registry/enum 导致编译失败，已改扩建原命令，该失败保留而不计功能RED。

Provider共用decoder校验原SQL和JSON的attempt_id/run_id/round/candidate/status；新finish写入正确serde拼写 outcome_unknown，对原Debug小写产生的 outcomeunknown 只做明确legacy兼容读取，不重写历史、不伪造结果。core DTO不包含数据库或SQL。完整管理授权/所有native mutation/PG schema/审计配额/offline迁移仍pending，不能把三个读回方法当成整波完成。

六入口真实进程合同已通过（wave2-s12-run-facts-daemon-reviewed-green.log）：三种视图读取原真实工具和两项Provider attempts；外域/不存在RPC、ACP1/2、HTTP403、WS和CLI一致；未知tenant HTTP422/ACP2-32602；原表临时改名注入真实driver故障，六入口 typedUnavailable/HTTP503，恢复后结果相同。整个读回clock零写，重启Provider保持两次原调用不重放。ACP2第一次遗漏严格run_id白名单产生真实实施失败，已补齐只读字段，日志保留。原/run默认摘要保持且在真实测试断言；compact默认复用原formatter。完整九门禁开始执行，真实S3/PG回归待后续，不计整波完成。

最终本地验收：`wave2-s12-run-facts-lint-final-gates.json` 九项全部 exit0，498 Rust / 36 Web / 44 真实 daemon；外部 10 项按环境明确 ignored。第一次完整门禁中 Clippy 发现相邻 if 格式歧义，修复格式后完整九项重跑，未 suppress lint，`wave2-s12-run-facts-final-clippy.log` 失败保留。独立真实 TLS MinIO 四项及 PostgreSQL 原生发布协调六项回归全部通过，见 `wave2-s12-run-facts-regression-s3.json` / `wave2-s12-run-facts-regression-postgres.json`；PG 仅发布协调，不代表全部业务事实迁移。

继续 S13 原准入、队列启动/取消和终态异步端口。已有有授权 run 的原 frozen snapshot fence 仍有效；原无授权 legacy run 的越域及 owner 重读风险需要真实合同。全企业 Wave2 尚未验收，Wave3–10 pending，未提交、未 CI 或生产验证。
