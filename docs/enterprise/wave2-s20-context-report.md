# Wave2-S20 原上下文与压缩事务异步化

状态：implemented / locally verified，完整九项门禁及真实环境回归通过。schema27保持；完整Wave2–10未完成，enterprise_ready=false。

`RunContextRepository` 扩展原请求端口，八个实际方法承担执行上下文读取、压缩结果读取、开始、结算、专用准入及三个不可变读回。SQLite经同一个四permit、原Connection和原事务body执行；原同步compact/context接口薄委托，不增加第二份压缩、session或run状态。`RunContextReadback`与`RunCompactReadback`只投影同事务原事实。`RunFactRepository.read_exact_run_owner`由旧`session.compact`适配实际调用，核永久归属，不能从当前head或最后stamp猜旧运行owner。

native mutation在BEGIN IMMEDIATE内核完整scope、固定准入owner、当前head与永久lifetime、writer proof、冻结授权和running状态。专用Compact run只能处理原linkage绑定的operation；普通Chat可创建多个隐式intent，由原source/generation/prefix/CAS决定是否安装。native迟到fence/CAS失败返回错误且不写rejected/terminal；legacy原source失败结算兼容行为保留，但旧owner不得写新generation。安装仍通过原context_heads/projection generation/compact_operations和原terminal事务；terminal写故障使所有事实同事务回滚。

专用压缩准入在原run准入事务冻结SessionPrompt授权并返回ExactOwner。daemon、普通自动压缩、显式摘要及独立命令真实await新端口；task-local与原入口Box::pin保留，未知副作用不自动重放。fallback同样使用准入的固定envelope，不重新读取后来generation。只读fact/operation/request允许永久归属的历史读回，不取得或续租writer；执行链使用的source与压缩结果读取仍核固定准入owner、当前head及原执行fence。

原requests与Canonical/Model读取现在共用唯一`sessions::snapshot_budget_in`，在加载/分配消息前核原表字节和行数：16MiB历史、4MiB当前投影、32768条消息及32768个batch。SQL聚合最多32769行，达到阈值明确拒绝；未达阈值时即完整集，不截断历史。compact请求/intent16KiB、replacement4MiB/32768消息、reason1024字节；原保存source在解码前16KiB检查，readback1MiB，run冻结快照1MiB。原begin复用requests唯一冻结快照SQL/decoder；原授权fence取snapshot前增加长度检查，原冻结SHA继续拒绝合法JSON的权限变更，没有第二套政策状态。

有效RED证据：`wave2-s20-compact-audited-functional-red.log`七项分别复现foreign准入/fact/begin/settle成功、合法linkage移动到Chat仍成功、旧generation结算改clock、32769个合法空batch成功读取；`wave2-s20-compact-operation-red.log`复现专用run接受第二operation；`wave2-s20-compact-frozen-reviewed-functional-red.log`复现无冻结授权的旧兼容run接受合法超大snapshot。不能把两次原冻结SHA已返回Forbidden的夹具预期失败称为安全绕过。

十四项初始合同、runtime136/storage83和五项真实daemon压缩合同已通过：自动hooks、手动无收益/拒绝/失败、独立命令准确取消和unknown重启、ACP长prompt/compact、三入口摘要与memory收据重启读回。新增两项冻结合同及三项结果身份/错误分类合同均已纳入最终context19统一验收。包括完整scope/owner维度、无增epoch的撤销/过期/预算拒绝、普通Chat并发CAS零终态、专用operation绑定、原terminal故障回滚、清空后历史读回；真实SQLite300ms写锁期间current_thread25ms timer保持响应，取消awaiter不阻止已启动worker真实提交。新八端口和exactOwner端口全部纳入共享满额四permit合同。

实施和夹具失败保留：不存在Clear枚举、events误用kind列、抽取签名parser、原daemon字段名与source读取模式，均已按真实源码修正，不计为业务RED；未放宽原测试超时、栈、架构、lint或忽略合同。

本批无新schema或第三方依赖。原文件SessionStore构造、hooks/memory/resource/delegation/interaction/恢复及部分管理路径仍同步；S21继续即时事件投影。PG仍只发布协调，全业务事实/租户唯一键/nativePGwriter/完整outbox/auditquota/离线迁移未完成。回滚需停止准入、join执行体并保留校验备份；schema不变不等于旧binary具备本批scope/CAS/预算修复。未提交、推送或部署。

ACP原并发复核完成：48 passed / 0 failed / 2 ignored（50总项），daemon-bounded-diagnostic.log exit0；首次EOF仍未定位，不称已修根因。S20新增第10个有效业务RED：source_json合法JSON改为不同lifetime后read_compaction_result实际返回Ok(Some(0))，证据compact-result-identity-functional-red.log。原唯一事务增加JSON源lifetime/起点/整数范围与committed+1/no_gain原generation一致性；健康revision冲突及summary_failed以Conflict拒绝重放，损坏以Corrupt拒绝。新增三项读回合同正在验证。首次改动闭包Err.into推断失败，显式返回Result<ProjectionGeneration, RuntimeError>修复，编译失败日志保留、不计业务RED。全Wave2–10仍未完成。

2026-10-09 S20/S21 implemented / locally verified：wave2-s20-s21-reviewed-final 九项全部exit0，614 Rust /36 Web /48实测daemon，43测试执行体、10外部环境项明确ignored；runtime136/storage84/context19/event18全部通过。独立真实TLS S3四组业务及PostgreSQL发布协调六项全部exit0（wave2-s20-s21-reviewed-regression-s3.json / postgres.json），自己的MinIO容器与PG隔离DB已清理。第10项有效compact来源身份RED已修，同原事务核JSON lifetime/起点/结果generation，正常revision和摘要失败返回Conflict，损坏Corrupt。首次九门禁tests101/ACP初始化EOF保留；独立与原并发daemon复核、最终完整门禁均未复现，根因未定位，64KiB有界诊断并持续drain保留，未修改超时/栈/并行度。schema27，无新第三方包。全企业Wave2、Wave3–10仍未完成，enterprise_ready=false；未提交推送部署。继续S22实际Live事件mutation，再S23发现端口，统一本批验收。
