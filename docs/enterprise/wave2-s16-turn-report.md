状态：implemented / locally verified。完整 Wave2–10 未完成，enterprise_ready=false。

# Wave2-S16：原 turn、transcript 和执行计划的异步事务

RunTurnRepository 六项实际端口由 foreground runtime/PlanStore/daemon await：同事务计划读回、计划 stage、执行许可、工具拒绝交换、助手 stage、transcript append。原 ToolAuditContext 复用同一份准入 envelope，所有 wrapper 保持入口 Box::pin；未重读后来 owner、未新增 mutable owner/map/状态表。原同步 trait 为兼容薄包装，事务体和 SQL 仅一份。SQLite 共享四 permit 和 canonical Connection，mutation 同事务核完整 scope、固定 ExactOwner、当前 lifetime/永久归属、writer proof、冻结授权；网络与模型调用在事务外。

六项分别有效原业务 RED：外域 legacy 会话的 assistant stage、transcript append、工具拒绝交换、计划读取、计划 stage、计划执行。证据 wave2-s16-turn-six-red.log（6各自失败）；早期四项 wave2-s16-turn-functional-red.log 保留。修后六项 scoped GREEN；并覆盖 scope四维和owner四维、六方法授权撤销/租约过期零新epoch、两个Connection计划CAS/同operation transcript幂等。

补充有效 RED：已结算且无计划决策的 run 原 start-plan 返回Ok（wave2-s16-plan-terminal-red.log）；原决策 receipt.run_id 错误仍启动并修改原计划（wave2-s16-plan-receipt-functional-red.log，第一个run_id断言截停，不称四个独立RED）。原 helper native running核验、收据与 SQL operation/session/lifetime/identity/run 完整一致后才可执行；JSON及SQL身份维度在 GREEN覆盖。原计划决策、版本、stage和最终 turn commit继续唯一事实源。

新 RunPlanReadback 只投影同事务 snapshot和原legacy计划存在性，避免原PlanStore先读stage再读base的跨时刻拼接。已暂存新版本可合法覆盖旧只读定义；该原业务规则未改变。native stage 相同内容及 assistant 相同payload零新clock；原transcript operation/digest幂等/CAS复用。拒绝工具批次保留原 not_executed 收据与完整交换，重复批次 typed Conflict，不制造准备或副作用。原helper terminal故障回滚保留。

预算：计划输入512KiB、持久计划JSON/decision receipt/identity1MiB、助手/transcript编码4MiB−8192、批次256 calls、拒绝理由64KiB且整批最坏嵌套转义预算先拒。输入预算用已有流式JSON计数，保存文本先SQL BLOB长度检查，损坏事实typed Corrupt。助手stage拒绝tool-call locator；数据库/SQL/路径不进入端口错误。真实SQLite写锁300ms期间current_thread25ms timer保持响应，全部六端口加入共享满额合同。

局部最终16项工具/turn合同GREEN（wave2-s16-turn-plan-audited-green.log）；storage83项通过（wave2-s16-storage-local.log）。覆盖最终turn发布plan/output/transcript一次、重启、四类写故障rollback、原plan execution故障回滚。最后扩展的SQL身份断言和实际 daemon计划审阅/执行/恢复/六入口以及runtime132原合同均已通过完整门禁。

失败保留：Message临时借用的编译错误、旧计划直接SQL插入改变clock的夹具断言、计划执行夹具误用Queue均非功能RED。各自修正后重跑，未放宽超时/栈、未ignore原合同。完整九门禁、真实TLS S3四业务/PG发布协调六业务均通过，结果见末尾最终记录。

schema25保持，没有新第三方依赖；原 legacy/Cron和context/request capture/Hook/MCP/resource/delegation/管理/recovery等尚未全部迁移。PG仍仅发布协调，完整v20全业务facts/正确租户唯一键/nativePGwriter/完整outbox/auditquota/离线迁移待实施。旧二进制不具备新scope和计划收据约束，不能凭schema不变称生产回滚安全。未提交、推送或部署。

S16 implemented / locally verified：wave2-s16-turn-audited-final 九项全部exit0，556 Rust / 36 Web / 48真实daemon，40个test executables、10外部环境项明确ignored；16项turn/transcript/计划integration合同、83 storage/132 runtime单测通过。独立真实TLS MinIO四业务和PostgreSQL发布协调六业务全部exit0。原六async端口接实际前台/PlanStore/daemon start-plan，同一Connection/四permit/固定准入envelope；六越域RED、已结算无决策仍给执行许可RED、receipt错误run_id仍执行RED修复。JSON四维和SQL四维身份GREEN，不把第一个截停RED夸成多项RED；故障回滚/CAS/幂等/撤销/过期和真实daemon原计划六入口/恢复合同保持，无新表/第三方包、schema25。编译/夹具失败及测试无效import警告保留，不计功能RED；未放宽超时/栈/护栏。全业务ports/nativePG/租户键/nativePGwriter/outbox/auditquota/offline仍欠，完整Wave2–10未完成，enterprise_ready=false。详见 docs/enterprise/wave2-s16-turn-report.md。
