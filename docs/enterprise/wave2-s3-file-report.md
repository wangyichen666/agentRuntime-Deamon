# 企业 Wave2-S3：原 tool receipt 的 File 两阶段发布

状态：implemented / locally verified。**完整 Wave2 与 Wave3–10 尚未完成。** 本标题的 S3 是第三个纵向切片，不是 S3 对象存储后端。

范围：core ToolArtifactCompletion；File staged capability；原 storage 工具收据事务；runtime 真实完成路径；已有共享 artifact 协议投影。SQLite 保持 v22，没有复制工具状态机、增加对象数据库或公开第二套完成接口。兼容原 Local 命名/路径和旧 receipt；保留 legacy 内部 helper，但生产 runtime 的 store_owned_output→finish_tool 分离调用被替换。File 本地 I/O 受同一 SQLite 写事务保护；该事务不能直接扩展为跨 S3 网络操作。

RED `evidence/wave2-s3-file-publication-red.log` 是旧 finish_tool 接受错误 digest/size 并提交 terminal 的真实行为失败。新链路先写 private temp 并对实际对象计算 SHA/size → fsync → BEGIN IMMEDIATE → ExactOwner/授权/running tool fence → NOREPLACE finalize/目录 fsync/对象校验 → 原 tool receipt 引用与 terminal → 再次 fence → commit。所有元数据校验失败整体回滚 receipt。新接口要求完整 digest/bytes；既有 legacy receipt 可缺失字段，但存在时必须匹配。

同 `(run, round, call)` 和完整 receipt payload 幂等；不同 payload 冲突。先前 terminal 读回不改 finished_at，也不重放工具。正常失败沿原 fd 清理 private temp；commit 前失败只可能留下未引用正式对象。真实进程 crash 后近期残留暂保留；GC 只按规范临时名、普通文件与身份 CAS 清理一天以上残留，不删除未知名字。

GC 改为同一 DB BEGIN IMMEDIATE 事务。跨 store/跨进程 File finalize→引用 commit 期间，另一连接拿不到 SQLite 写锁；提交后它看见原 tool receipt 引用并保留对象。对象损坏或 unsafe link 拒绝，不降级为零字节成功。artifact API 内部错误不输出 SQL/路径。

## 证据

- 13 文件合同：旧边界合同 + digest/reference 提交 RED→GREEN、两阶段幂等与重启、SQL trigger 故障回滚/孤儿清理、坏 digest/stale owner 无对象发布、模拟 crash temp 在重启后有界清理。
- 两真实数据库连接 + Barrier + zero busy_timeout probe 证明数据库写锁覆盖引用提交；真实 GC 线程最终删0个并能读回对象。
- 真实子进程调用相同生产发布 helper；在 finalize 完成后、原 receipt commit 前观察管道标记并 kill，重开真实 SQLite 后原收据保持 running/无引用，GC 清理孤儿对象。不是用回调异常冒充 kill。
- runtime 生产接线由原真实 read_file 六入口/重启合同覆盖；Provider 使用 mock，未声称商业 Provider 验收。
- 首次 Clippy test module 次序和未用变量、测试方法/trait import 编译错误均保留日志并修复，没有放宽 lint 或原断言。

API 见 [artifact-api.md](artifact-api.md)。schema 不变；回滚需停写、保留 unknown/running tool 证据和对象，不能自动重放外部工具。大输出 receipt.output 改为纯预览，artifact_ref 保持结构字段，模型结果继续包含引用提示。

## 最终门禁

`evidence/wave2-s3-file-final-gates.json` 九项全退出0：427 Rust（含 34 真实 daemon、20 架构合同、13 File 合同和真实进程 kill/Barrier 单元合同）、6 PG 显式 ignored、36 Web。独立真实 PostgreSQL 18.6 lane `evidence/wave2-s3-file-postgres.json/log` 六项全部通过；仅清理本次隔离数据库。

最后审阅补上新建 artifact 根目录时父目录 fsync，重新完整验证 `evidence/wave2-s3-file-reviewed-gates.json` 九项仍全部退出0，Rust 427 / Web 36。锁审计 `evidence/wave2-s3-file-lockfile.json` 证明旧第三方包 name/version/source/checksum 零改变、新增16项 PG 依赖；既有 feature/消歧依赖边有13条文本变化，不冒充 lock 逐字未变。

## 尚未完成

S3-compatible ArtifactStore/短期 capability URL、全部 async business ports/PG facts、runtime writer lease、离线 SQLite→PG 切换工具未完成；Wave3–10 也未实际实施。不能把 File 本地协议或发布协调 PG schema v1 当作完整 Enterprise 后端。Docker/Kubernetes/IdP、CI、production lanes 未验收；未提交、推送或部署。
