# 企业 Wave2-S1：事务 outbox 与双后端发布协调

状态：implemented / locally verified。**完整 Wave2 及 Wave3–10 尚未完成。** Wave1 前置约束仍未闭合，enterprise_ready=false。

已实现原事件事务的 outbox、DB 时间发布租约与 epoch CAS、claim、inbox 去重、receipt 后 ack、稳定分页和类型化错误。core DTO/状态转移只有一份；storage-ports 只依赖 core；SQLite 与 PostgreSQL 适配器互不依赖。SQLite 保留原 run/session/approval owner，v22 的 outbox 与原 ViewStamp 同事务提交；PG v1 只是发布协调 schema，未复制另一套业务状态机。

生产调用：原 SQLite views::commit → outbox；daemon 单个受管 publisher → 同一异步 PublicationRepository → durable local-journal receipt → ack。PG 独立 worker 执行同一 publisher，正常退出等待连接 driver 关闭。CLI/TUI/ACP v1/v2/HTTP/WS 读同一个 outbox 元数据；readback 不返回正文或凭据，不推进 business clock/event，不重放 Provider。

## 证据

- 真 RED：`evidence/wave2-s1-red.log`，真实 daemon 未识别 outbox.readback，返回 -32601；不是编译失败。
- 真实 daemon GREEN：`transactional_outbox_is_durable_before_publication_and_shared_after_restart`，六种投影一致、分页、错误 code/data、伪造 tenant 拒绝、读回零业务写、重启收据/Provider 调用不增加。
- SQLite：共享合同、独立连接受控 barrier 竞争、epoch takeover、receipt 后 ack 前恢复、terminal/outbox 同事务故障回滚、immutable payload/tamper 拒绝、v21 备份/迁移不回填旧历史、未来版本拒绝、完整域 FK。
- 真实 PostgreSQL：共享合同、两连接同 key 唯一 winner、同公开 session/run 键跨域隔离、重连/去重、driver/真实 worker 恢复、过期 epoch、未来 schema、损坏、只中断本测试连接的 DB 断连。
- 回归证据：`wave2-s1-postgres-regression.log/json`、`wave2-s1-postgres-lock-red.log` 保留 worker 的 StorageUnavailable/55P03。原因是失败事务排队 rollback 后，同步子进程等待阻塞驱动退出；通过 close().await 与异步子进程等待修复，没有 sleep 放宽时序断言。

## 已替换路径与兼容

原 `views::commit` 新增封存调用，没有旁路第二事件 owner。v22 表缺失/插入失败时不能只提交 terminal。原未发布到 outbox 的历史不伪造补发。未来版本测试从 v22 调整到 v23，旧迁移夹具真实删掉新增表再回放，未放宽原业务安全断言。slash 注册在末尾，保留 TUI 原菜单次序。新方法严格拒绝未知身份字段，即使 legacy RPC。

API 见 [publication-api.md](publication-api.md)，迁移/锁序/回滚见 [schema-v22.md](schema-v22.md)。新程序只前进 schema；旧 binary 必须恢复校验备份，不能删版本号降级。

## 仍需完成

Wave2：全部业务 async ports/PG 原事务、runtime writer lease 和旧 epoch terminal fence、S3-compatible ArtifactStore/企业 capability、完整离线迁移与切换验证。Local File 两阶段发布已在后续 [S2 路径切片](wave2-s2-file-report.md)和 [S3 发布切片](wave2-s3-file-report.md)实现。当前 epoch 只保护发布协调，不能作为 runtime writer 的授权。

Wave3–10：OIDC/服务账号/目录/共享限额、注册表/签名 Pack/配置治理、持久工作流/A2A、OAuth/SecretStore/connector-host、远程 worker/sandbox、统一审计/OTel/retention/账本、Operator/Helm/HA/灾备、性能/soak/SBOM/签名与同产物发布 lanes。原 Local 已有的 Provider/MCP/Hooks/子 Agent 不冒充这些企业交付。

PostgreSQL 18.6 在本机隔离库实测；Docker/Kubernetes/IdP/商业 Provider 未用于本切片验收。没有 CI verified 或 production verified 声明，没有提交/推送/部署。

## 最终门禁

`evidence/wave2-s1-final-gates.json` 九项退出码均为 0：Rust 411 通过、6 项 PostgreSQL 显式 ignored（独立真实 lane 六项均通过）、Web 36。包含 20 项架构合同和 33 项真实 daemon 合同。fmt、严格 Clippy、MSRV 1.88、release、JS、在线 cargo deny、diff 全通过。

锁文件审计 `evidence/wave2-s1-lockfile.json`：旧第三方包 name/version/source/checksum 全部保持；新增 16 项 PostgreSQL 依赖。既有 digest/tokio-util 的 feature dependency edges 增加，不声称 Cargo.lock 逐字不变。
