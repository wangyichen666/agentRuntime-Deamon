# 企业 Wave2-S2：File artifact 路径与共享读回

状态：implemented / locally verified。SQLite 保持 v22，不新增业务 owner。**完整 Wave2 和 Wave3–10 尚未完成，Enterprise 未启用。** 后续 S3 切片继续强化同一 File 发布链路。

范围：core ArtifactIdentity/ArtifactError、storage fd 文件实现和原 ArtifactRepository、daemon 读回、CLI/TUI/ACP v1/v2/HTTP/WS；不提供 S3 后端或远程下载能力。保留 Local 的引用命名、socket、旧 receipt 和分页字段。风险是坏链接/目录替换原来可能被跟随，现在 fail closed；恢复时停下相关写入并从可信备份核验对象，不能通过删 digest 检查绕过失败。代码回滚不需要数据库降级，但会重新暴露旧路径问题。

真实 RED `evidence/wave2-s2-file-red.log` 证明旧根目录 symlink 会向另一目录写入。旧 std::fs pathname 读写/rename/GC 被固定目录句柄、NOFOLLOW、普通文件/nlink 校验、身份/内容 CAS、原子 NOREPLACE 与 fsync 替换。已有 target 不覆盖；实际 SHA/size 与原 tool receipt metadata 不一致拒绝读回。

8 项真实文件合同覆盖 symlink/hardlink、root replacement、foreign owner、重启分页与篡改、两独立 store 的 Barrier 并发、parent symlink 开库前拒绝、GC 和 DB metadata mismatch。真实 daemon `file_artifact_shared_readback_restart_and_link_denial_preserve_native_tool_receipt` 执行原 read_file，证明六种入口一致、v2/HTTP 稳定 Forbidden、读回零事件/clock/Provider 增加、原 tool receipt 不变、重启恢复。

完整九门禁 `evidence/wave2-s2-file-final-gates.json` 全退出0：420 Rust、6 PG 显式 ignored、36 Web、34 真实 daemon、20 架构合同。独立真实 PostgreSQL lane `evidence/wave2-s2-file-postgres.json/log` 六项通过。本切片沿用已锁定 rustix 1.1.4，没有升级旧第三方包。

API、运维与实现限制见 [artifact-api.md](artifact-api.md)。S3/完整业务 PG/runtime writer lease/离线迁移、全企业 tenant scope 仍待实施；Docker/Kubernetes/商业 Provider/IdP/CI/production 未验收。未提交、推送或部署。
