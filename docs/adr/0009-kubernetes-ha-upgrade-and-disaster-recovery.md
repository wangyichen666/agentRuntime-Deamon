# ADR 0009：Kubernetes 部署、HA、版本窗口与灾备

日期 2026-10-07。状态 **proposed**；仓库尚无 Operator/CRD/Helm 或企业环境 E2E。

## Owner 与部署

使用公开 Kubernetes/kube-rs、Helm，PG/S3-compatible/OTel Collector 通过接口健康检查对接。AgentRuntime CRD 表达 desired generation/resource/sandbox/replica policy；可选 ConnectorRuntime。Operator reconcile/status conditions/finalizer/backoff，**CRD 不保存 canonical session/run/workflow/audit**。

namespace 是 deployment/business/env/tenant 的运行隔离边界，语义来自 opaque labels/annotations 与 canonical DB，不反解析 namespace 字符串。管理/使用面独立 Service/audience/scope；gateway/control-plane 可多副本，不依赖 sticky memory truth。同 agent 初版一个 active runtime owner；DB lease/fencing 决定 failover，不把 K8s pod Ready 当 writer permit。

chart 显式 NetworkPolicy default deny、PodSecurity non-root/read-only/seccomp/cap drop、ResourceQuota/PDB/requests+limits、可选 ServiceMonitor、Secret 引用和固定 image digest。禁止任意 hostPath；短期 workload capabilities 不等于租户管理员密钥。

## 启停与升级

readiness 只指可接新任务（schema/capability/storage/lease dependency 校验）；liveness 检查进程实际死锁，不因 Provider latency/外部模型超时重启。preStop 停 admission→有界 drain→持久化 receipts/cursor/outbox→撤销新任务分配→退出；强杀后恢复 unknown，不自动重放 prepared/running 外部作用。

expand/migrate/contract：先部署兼容扩展 schema/协议能力→离线/受控 migrate 与验证→新 binary/客户端协商窗口→旧副本 drain→最后 contract。发布产物 manifest 声明 supported schema range/capability version；旧 binary 在不兼容新 schema fail closed。rollback 只能兼容当前 schema 或经离线备份恢复，不能 DB down migration/双写。

K8s 控制器 CAS/reconcile 有界 retry；业务 lease 和 transaction 锁不与 API 网络 I/O 互持。finalizer 保留 work/canonical owner/unknown evidence，先禁止新 work 并精确停止 workload，再清 deployment 资源；不要删除未知副作用证据以使 finalizer 快速结束。

## 灾备与发布证据

PG PITR/artifact versioning/registry+trust roots/signing key 独立备份，密钥隔离加密/权限；每次恢复演练对 sessions/runs/events/artifacts/audit/digests/owner/lease epoch 核验。备份时间点不一致需 repair evidence，不能只“服务启动成功”就宣布恢复完成。

同 checksum-verified release candidate 依次运行 Local/PG/OIDC/container/kind/升级/回滚 lanes；不可各 lane 重编相似 binary 代替。供应链 SBOM/provenance/signature 与镜像固定 digest 是生产发布门禁，CI 静态配置不等于生产验收。

需 kind/k3d E2E 创建租户/Agent→session→tool→approval→kill/restart→cursor 恢复；多 gateway/lease 不超卖；滚动升级 capability 窗口；PG/worker/gateway/operator 各故障；真实备份恢复 count/digest/audit chain。当前环境即使 CLI 存在也必须探测 daemon/cluster，不冒称这些 lanes 通过。没有所需环境时提供可复现配置并标 **未验证**；Local 不受 K8s 缺失影响。
