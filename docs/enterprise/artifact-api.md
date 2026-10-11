# Local artifact 共享读回与 File 发布

状态：Wave2-S2/S3 File 与 S4 S3 后端已接线；本地验收见对应报告。S3 使用 opaque `artifact:v1:` 引用和同一分页 API，配置/故障边界见 [S4 报告](wave2-s4-s3-report.md)。远程 capability URL/完整企业租户授权未完成，Enterprise 仍未启用。

所有入口读取原 daemon `artifacts.read`，仅返回分页 base64 数据、offset、cursor 和 has_more。不生成 run，不推进 business clock，不重放工具/Provider。artifact_ref 必须属于原 run 的 tool receipt，读取同时验证 ExactOwner/session lifetime、授权、实际对象 SHA/大小及原 receipt 可用的 metadata。旧 Local receipt 缺少 hash/size 字段不补造。

| 入口 | 形式 |
|---|---|
| 原 RPC / WS | `artifacts.read`，params 为下述严格对象 |
| CLI | `my-agent artifact --session SESSION --run RUN --reference REF --offset 0 --limit 16384` |
| TUI / ACP v1 | `/artifact SESSION RUN REF [offset limit]`；含空格的引用使用下面 JSON 形式 |
| ACP v2 | `_my_agent/artifacts/read`，要求物理连接协商 `readback`；不要求 session-control |
| HTTP | `POST /api/artifacts/read`，body `{ "workspace": "…", "params": {…} }` |

```json
{"session_id":"session-key","run_id":"run-id","artifact_ref":"原收据中的引用","offset":0,"limit":16384}
```

含空格的引用可以通过 `/artifact {"session_id":"…","run_id":"…","artifact_ref":"… …","offset":0,"limit":32}` 传递。JSON 严格拒绝未知字段，不能在请求中声明 tenant/subject 等可信身份。原其他 slash 的参数行为和菜单次序保留。

默认 offset=0、limit=16384；原 Local limit 兼容行为仍钳制在 1–16384。文件总量上限 64 MiB；读取以 64 KiB buffer 流式校验完整对象，再返回有限页，因此读一小页仍可能扫描完整对象。S3 同样完整流式校验再分页；本切片未进行生产性能或远程 capability URL 验收。

| Artifact kind | RPC code | HTTP | 恢复动作 |
|---|---|---|---|
| storage_unavailable | -32060 | 503 | restore_artifact_store |
| corrupt | -32061 | 500 | verify_and_restore_object |
| conflict | -32062 | 409 | reconcile_object_identity |
| stale_writer | -32063 | 409 | stop_old_writer_and_readback |
| invalid_reference | -32602 | 400 | correct_reference |
| forbidden | -32041 | 403 | review_object_access |

RPC/v2/HTTP/WS 的 artifact 错误 data 包含 domain=artifact、kind、retryable、safe_auto_retry、recovery_action。只有只读 storage_unavailable 建议安全自动重试。授权 owner 保留原 AuthorizationError 投影。slash 使用原人类展示错误方式，不声称它提供同样完整的 typed error data。内部 SQL/路径不会作为 artifact API 失败文本返回。

File 实现固定父目录与 artifact 根目录句柄，最终目录/file 均 NOFOLLOW；拒绝非普通文件、hardlink 和 symlink。对象名继续 `sha256-<hash>.txt`，新发布 private temp → fsync → 原目录 NOREPLACE rename → fsync → 实际 SHA/size/身份 CAS 验证。根目录被替换时当前 RunStore 拒绝继续操作。

Wave2-S3 切片中，生产 runtime 使用 `finish_owned_tool_output`：先准备私有临时对象并校验其实际摘要；然后原 SQLite BEGIN IMMEDIATE 事务核验 owner 和 running tool，在同一写锁下 finalize、校验、提交原 tool receipt 引用。它是 File 发布协议，不是 S3 后端实现。receipt 的结构字段保持；大输出的 receipt.output 保存纯预览，完整引用在 artifact_ref 字段，面向模型的工具结果仍含引用提示。

重复相同 `(run, round, call)` 和收据 payload 返回原引用；不同 payload 冲突。失败或 crash 不把 receipt 伪装成功；只可能留下未引用对象。GC 同样取得数据库 BEGIN IMMEDIATE 写锁，避免删掉正在发布的对象；未引用正式对象按 retention 清理，规范临时名的 crash 残留至少一天后清理。近期 temp、未知文件名和被引用证据保留。单次扫描最多 1000 entries，完整稳定 GC cursor/合法留存策略仍属于后续工作。

原 `store_tool_output` / `store_owned_output` 为既有内部兼容端口保留；生产工具完成路径已替换为原子方法。分离调用者必须以 finish_tool 的对象/元数据校验面对发布失败，不能把兼容 helper 返回值当作已发布的 tool receipt。
