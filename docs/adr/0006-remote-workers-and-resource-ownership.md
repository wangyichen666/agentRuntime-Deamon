# ADR 0006：远程 worker 的单 writer、隔离与未知结果

日期 2026-10-07。状态 **proposed**；当前只有前台 Native/Docker 与后台 Native 真实执行路径，worker/mTLS/K8s backend 未实现。

## 决策

第一版不做 same-session 多活 ReAct。control-plane 可多副本，session lifetime 同时一个有效 writer lease；runtime worker 只执行冻结任务。复用 existing native run、ExactOwner、tool receipt、ResourceRepository 与 cancellation，不能建 worker 私有 run/session/approval truth。

WorkerBackend 采用 native/container/kubernetes 开源接口，首个 backend port 要有真实 daemon/scheduler 调用者及安全 oracle。RunSnapshot 冻结 requested/effective backend、image **digest**、CPU/memory/pid/disk/network/time/output 预算，当前 image 字符串未等价于已验证 digest。强隔离不具备时 typed SandboxUnavailable；禁止自动回退 Native。

worker transport mTLS+短期 workload token、严格 DTO/能力协商、operation_id/generation/exact owner/scope/lease epoch/policy digest/artifact capabilities。任务不携带长期 Provider/tenant admin keys。workload token scope 限定该 run/action/expiry；worker callback 不覆盖 server principal/scope。

workspace 来自一次性 snapshot/受控 checkout/volume，不允许任意 hostPath；输出 stage artifact 再 canonical publish。egress default deny，显式 domain/IP/port allowlist 与 DNS pin/rebinding 防护；short-lived secret 注入、结束撤销，log/core dump/artifact 递归脱敏。

## 状态与失败时序

调度 transaction 准入冻结 snapshot+prepared dispatch receipt+lease epoch→worker 接收与 backend workload identity 持久化→started→执行→回包/实际 artifact digest→同 epoch 事务结算。terminal 发布先 commit；重复 operation 同 payload 幂等、不同 payload conflict。

前台/后台共享 starting/running/stopping/completed/failed/orphaned/unknown Resource 状态；后端稳定 workload identity 不等于 PID/name。stop/reconcile 必须精确 owner/epoch/identity，不能“同名容器”删除另一任务。输出分页 bounded，日志压力只能截断/脱敏/写证据，不能伪造成功。

heartbeat timeout 只标 suspect，lease 失效才能分配新 epoch；takeover 前对 prepared/running 外部作用 reconcile。worker 执行后未持久回包，必须 unknown；无幂等键/后端证据不可重放。网络恢复的旧 epoch result 零写且留审计；不能因 orphan/timeout 就换一个 worker 再执行同副作用。

## 锁序、回滚与验证

lease/owner 事务与后端 network I/O 分离；数据库规定 scope/session→lease→run/resource→receipt/outbox 顺序。backend cancel 与 terminal settle 并发用 barrier 测试；canonical CAS 决定一次终态，不以任务消失猜执行失败。

rollout 可关闭新 worker 准入并 drain 旧 generation；回滚不迁移现有 task 到 Native，不销毁 unknown 证据。Local 仍使用已冻结本地 backend，不要求容器服务/集群。

要求真实 deny-read/deny-write/deny-network oracle、资源预算和无孤儿、mTLS/过期 token/跨 tenant、网络分区/旧 epoch/未知外部作用不重放。Docker/kind 成功 lane 当前 **未验证**；本地 preflight/mock 只证明 fail closed 和协议合同，不能称强隔离已验收。
