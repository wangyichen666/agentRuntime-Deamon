# SQLite v23：原工具收据的对象发布证据

v22 → v23 单一前向事务新增 `artifact_publications`；旧历史不回填、不补造 receipt 或 SHA。升级前保留原在线备份及 SHA/完整性验证。版本 >23 在任何 schema 写入前拒绝。没有声称这是 PostgreSQL 的业务 schema。

| 字段 | 约束/事实 |
|---|---|
| publication_id | 完整 scope、原 ExactOwner、round/call 的摘要；opaque `artifact:v1:` 引用 |
| organization/tenant/project/agent | 所有发布和读回查询复核完整域 |
| run_id/round/call_id | FK 原 tool_executions，原工具 occurrence 唯一 |
| store_id/sha256/bytes | 非秘密后端 ID、实际输入内容身份；≤64 MiB |
| reservation_json/digest | 不可变完整元数据；读取重新校验摘要与全部冗余列 |
| published | 只允许 0→1；与原工具 terminal/ref 更新同一事务 |

该表保存 prepared/发布证据；`tool_executions` 是工具唯一状态与收据 owner。不可变触发器禁止篡改 scope/对象身份；单调触发器禁止退回 prepared。服务恢复不读取 prepared 证据来执行工具。

RunSnapshot 新增可缺省 `artifact_store_id`，旧 None 不序列化，因此旧授权快照摘要保持。新 S3/file-v1 run 在准入冻结 ID，发布事务复核；网络 I/O 后再次复核 owner/授权/native ref。改变配置不把旧 run 转移到新后端。

v22 备份可恢复到新位置再由新 binary 前向升级。若回退旧 binary，须恢复升级前的 v22 DB，并同时保留对应对象及凭据；不能让旧 binary 直接打开 v23。新对象引用不能凭字符串转成本地路径。
