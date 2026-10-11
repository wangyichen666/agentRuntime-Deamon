# Wave2-S23 原发现许可与取消事务异步化

状态：已实现并完成本地验收。S22/S23统一九门禁全部通过，636 Rust/36 Web/48实测daemon，44执行体、10外部环境项明确ignored；runtime137/storage86/context19/discovery14/event23全部通过。真实TLS S3四组业务、PostgreSQL发布协调六项全部通过，自己的容器及独立DB已清理。schema27，无新第三方依赖，完整Wave2–10未完成，enterprise_ready=false。

最终证据：`wave2-s22-s23-reviewed-corrected-final-gates.json` 与 `wave2-s22-s23-reviewed-regression-s3.json` / `postgres.json`。首次九门禁8/9，Clippy发现5处Copy类型的冗余clone；仅去掉多余调用，不改断言、不放宽lint，原失败日志保留。

`RunDiscoveryRepository` 四个实际端口复用原 Connection、SQL、收据和事务，并共用原四permit。前台、独立compact和原Cron通过 `NativeExecutionPorts` 传入同一个 canonical Store 的实际能力引用；这里不保存第二份业务状态。`SearchCatalog.execute` 发布、`ToolRegistry` 嵌套调用的许可检查都实际 await。原调用批次在副作用前取得descriptor，并在分组、原receipt和输出预算中使用同一份descriptor。纯Tool descriptor仍为同步计算。

原发布、执行许可与取消helper在同事务中先SQL核完整scope，再核固定准入ExactOwner、当前head/lifetime、原writer proof及冻结授权。取消入口只捕获一次scoped owner，贯穿queued/running路径；原取消事件事务提交成功后才取消执行token。`run.discovery` 和 `/tools` 读回实际 await，原历史read TX只核scope、永久lifetime归属和原收据完整性，不重新获取writer，不要求当前head或重新授予执行许可。

保存snapshot复用原SQL长度预检及唯一decoder，最多1MiB。保存receipt读取前SQL聚合检查最多128行/累计1MiB；发布新记录前把新raw、operation与digest一起纳入累计预算，超限拒绝且不破坏已有历史读取。原SHA、JSON schema、owner、operation、冻结工具集合和typed目录SHA继续核验。保存目录generation错误返回Corrupt，合法调用者传错generation及已取消执行许可返回Conflict。operation幂等读取先限定本run，外域全局主键碰撞只返回Conflict，不先读取外域私有payload。

有效功能RED共六项：foreign发布、许可、历史、取消返回成功；合法超大冻结snapshot仍获执行许可；合法完成并退休lifetime无法读原发现收据。见 `wave2-s23-discovery-functional-red.log`（五个有效；第六夹具仍Running，未到读回）和 `wave2-s23-discovery-retired-functional-red.log`（修夹具先原finish后复现第六）。首轮修复合同8/10通过：stamp序号列测试误用event_seq（真实seq），不计业务RED；保存错误目录的Conflict分类为实际缺口，原helper修Corrupt。第二次12/13仍运行已编译旧序号查询的执行体；修正后的 `wave2-s23-discovery-reviewed-corrected-green.log` 14项全部通过，原失败日志保留。

合同覆盖完整scope/owner维度、原event/stamp/outbox故障整体回滚、收据SHA/身份/目录损坏、SQL行/字节预算、累计入库预算、两独立Connection并发唯一收据和取消事件、撤销/租约过期拒绝执行但保留scoped历史、退休后重启零新lease、外域损坏operation碰撞。库新增四端口真实Connection锁等待时Tokio timer不被阻塞，以及awaiter取消后worker仍持permit、原发布/取消恰好完成一次；新增native搜索worker取消合同与原legacy合同共同验证计算worker不能迟到发布许可。这些库合同、每条operation/SHA正确的合法129行预算夹具及取消typed Conflict分类均已纳入最终九门禁通过。

兼容同步API仍保留，并薄委托同一原helper。Hook/MCP/memory/resource/delegation/interaction/管理等剩余链路、全业务native PostgreSQL facts、租户唯一键、PGwriter、audit/quota及离线迁移仍欠。PG发布协调回归不等于完整业务PG后端。未提交、推送或部署。
