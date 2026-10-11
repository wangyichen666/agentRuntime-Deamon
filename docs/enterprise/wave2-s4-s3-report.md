# Wave2-S4：实际 S3-compatible 对象与原收据发布

本切片 implemented / locally verified，已接入真实 caller。完整 Wave2 与 Wave3–10 尚未完成，Enterprise 仍未启用。

## 调用链与唯一 owner

启动组合显式选择后端 → LoopEngine 准入 RunSnapshot 冻结非秘密 store ID → 原工具执行 → ArtifactService 计算实际输出 SHA/size → SQLite v23 事务保留 prepared 证据 → ArtifactStore 临时/正式对象 I/O → 实际 GET 重新校验 → SQLite 事务更新原 `tool_executions` terminal/opaque ref 与 published 证据。

生产 runtime、daemon RPC 与全部既有 artifact 投影都经过此调用链。File 默认旧行为保留；显式 file-v1 与 S3 使用同一对象发布合同。数据库锁不跨网络；SQLite 异步对象端口统一在 storage 边界用受管 spawn_blocking（共享4个 permit），Clone 只共享原连接、文件句柄、预算和指标。caller 取消后已经开始的阻塞操作继续持 permit 到结束。

S3 adapter 使用 Apache `object_store` 固定0.14.2（quick-xml0.41.0，修复两项 RustSec XML DoS 公告；不添加 advisory ignore），MIT/Apache-2.0。强制 HTTPS、验证证书，启动解析并固定最多16个地址，禁用代理和重定向；connect5s、单请求20s、发布45s、读20s、并发4、SDK自动重试0。真实服务返回正文最多64MiB，分页最多16384bytes，流式重新计算完整 SHA，不信任 ETag。已存在对象只允许 conditional create；内容不一致拒绝覆盖。

临时与正式 key 均由完整 tenant/project/agent scope 摘要派生，不接收客户端 bucket/key。读回只返回经过原 owner/native receipt 检查的有限 base64 页，网络 I/O 后再次复核授权；无公开 bucket URL。

## 配置与恢复

缺省未设置 `AGENT_ARTIFACT_BACKEND` 保留原 Local File。显式 `file-v1` 使用同合同本地对象；`s3` 必需启动环境：`AGENT_S3_ENDPOINT`（HTTPS）、`AGENT_S3_BUCKET`、`AGENT_S3_REGION`、`AGENT_S3_PREFIX`、`AGENT_S3_ACCESS_KEY`、`AGENT_S3_SECRET_KEY`；自有 CA 用 `AGENT_S3_ROOT_CA` 文件。凭据不进入 Debug/Serialize/普通配置 DTO。错误配置拒绝启动，不静默 fallback。

启动环境凭据仍是本切片的注入机制，**不是 Wave6 SecretStore/OAuth 生命周期**。历史冻结 store ID 需要相同非秘密配置；更换 endpoint/bucket/prefix 不会重放历史工具。原 receipt/元数据损坏、对象实际摘要不符、旧 owner、越域都 fail closed。

上传/读回失败后尝试在5s内删除临时对象；网络分区/进程强杀不能证明清理成功，prepared 元数据保留为 repair evidence。最终对象已写但数据库提交失败时 ref 不可读，工具保持 running；只有明确的原 owner reconcile 可以完成引用提交，不能把网络重试解释为重放工具。当前尚无受管 S3 orphan/TTL/合法留存 GC，不能声称所有强杀残留都自动清理。

## RED、合同与证据

真实 daemon RED `wave2-s4-s3-red.log`：S3 配置被旧 runtime 忽略，返回 Local 文件引用。首次错误 fixture/编译失败日志单独保留，不作 RED。接线后隔离 MinIO 实际 TLS GET/PUT/DELETE、篡改、不可信证书、跨租户 key、commit故障、原收据六入口读回与重启通过。

`scripts/enterprise-s3-lane.py` 使用固定镜像 digest、本次隔离容器、临时测试 CA/数据、随机凭据、127.0.0.1随机端口，结束只清理该容器/目录。现有镜像可用性是 lane 前置，脚本不下载镜像，不接触其他服务。测试只标 locally verified，不作生产性能/HA保证。

证据：`evidence/wave2-s4-s3-payload-reviewed-s3.json` 与 log；九门禁报告 `evidence/wave2-s4-s3-payload-reviewed-gates.json` 全 exit0：436 Rust / 36 Web；普通测试9项外部环境合同显式 ignored。PG 回归 `evidence/wave2-s4-s3-regression-postgres.json` 真实6项通过。普通 cargo 测试中 S3 与 PG 外部环境测试显式 ignored，独立真实 lane 必须分别执行。

## 未完成的企业范围

全业务 PostgreSQL canonical facts、native writer lease/迟到终态 fencing、SQLite→PostgreSQL 离线迁移、完整企业授权、S3合法留存/分页GC/性能验收仍未闭合。Wave3–10 不能据本切片改为 implemented；按规格完整Wave2门禁前不进入下一波。
