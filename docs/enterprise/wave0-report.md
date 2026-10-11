# 企业升级 Wave 0 交付与证据

日期：2026-10-07。基线 `0af445b1453b0adfad19802cc519624f1ebb29e8`。本波 **implemented / locally verified**；未提交 Git commit、推送或部署。**Wave 1–10 未实现**；六份企业 ADR 和其余需求行均为 proposed。

## 实际交付

- [ARCHITECTURE.md](../../ARCHITECTURE.md)：真实依赖、唯一 facts owner、双模式/双 API 面目标、协议/错误、owner/CAS、未知副作用、安全文件/资源、迁移与故障恢复约束。
- [baseline.md](baseline.md)：27 类能力的 owner/schema/RPC/入口/实际测试/完整或部分或缺失；回答用户的 13 项动手前确认。
- [schema-v20.md](schema-v20.md)：**48 张最终表**的逐表职责与 transaction owner，明确 Cron/config/artifact 文件事实例外。静态 DDL 与文档逐表集合一致，见 [inventory](evidence/schema-v20-inventory.json)。
- [baseline-index.md](baseline-index.md)：13 库真实生产依赖与原基线测试位置索引；[crate-boundaries.toml](crate-boundaries.toml) 登记职责/允许内部边。
- ADR 0004–0009：身份/授权、双存储、远程 worker、pack/plugin/connector trust、审计/留存/成本、K8s/HA/灾备。明确设计尚未实现，锁序、事务、失败/回滚和必须验证的 lanes。
- [traceability.json](traceability.json) / [matrix](traceability.md)：71 个编号需求映射 owner/schema/projection/test/document/status。没有以 mock 或设计冒充企业实现。
- 现有 `tests/architecture_contract.rs` 保留原 12 检查并扩成 **20 项**。新增 qualified placeholder、typed mutable run/approval/message owner（含别名/enum/static）、Cargo renamed/target/build/inherited deps、全成员职责与 fixture feature 审计、通道/精确例外、矩阵完整性检查。
- 新的可复现脚本 `scripts/enterprise-gates.py` 跑用户九项命令；`scripts/enterprise-architecture-red.py` 隔离复制当前源树，真实注入禁止依赖/第二 owner/新增无界通道，检查必须来自目标 guard 的失败（不能只因为无法编译而称 RED）。

没有删除/替换 production owner/执行路径；没有新增 production crate/数据库 migration/RPC；schema 仍 v20、第三方 lock 不变、Local 入口与业务行为不变。Wave 0 的真实实现是可执行架构门禁，后续不能绕过它添加新的基础设施/状态 owner。

## RED / GREEN

旧 checker 对 `std::println!`、`core::todo!`、`std::unimplemented!` 实际漏检；新增 regression 先失败（exit 101），后加强 AST 判定通过。[原始 RED](evidence/wave0-red-quality.log)。

新护栏在隔离源码副本中分别被真实违规触发：

| 注入 | 失败原因 | 结果 |
|---|---|---|
| Gateway 条件生产依赖 agent-daemon | 企业 Cargo guard 检出 concrete 边 | exit101，目标断言命中，[log](evidence/wave0-red-dependency.log) |
| Gateway Mutex<Vec<RunRecord>> 新 owner | 企业 typed state guard 检出长期 mutable facts | exit101，目标断言命中，[log](evidence/wave0-red-owner.log) |
| Gateway 新 unbounded_channel | 精确 channel registry 检出额外站点 | exit101，目标断言命中，[log](evidence/wave0-red-channel.log) |

完整机器报告：[wave0-red-fixtures.json](evidence/wave0-red-fixtures.json)。原工作区未被注入。别名/标准 std 无界构造/零容量、Cargo rename/build/workspace、enum/static/alias、fixture 排除也在常规 GREEN 合同覆盖。

基线 379 全通过；扩展后 387 全通过，0 失败/忽略，包括原 **31 个真实 daemon/入口合同**、**20 架构合同**；Web 36 保留。旧测试与业务断言未削弱。

## 九项完整门禁

[机器结果](evidence/wave0-gates.json) 每项记录 command/exit/time/log；都退出 0。本机验证，不是 GitHub CI 或生产结果。

| 命令 | 日志 |
|---|---|
| cargo fmt --all -- --check | [fmt](evidence/wave0-fmt.log) |
| cargo test --locked --workspace --all-targets --all-features | [387 tests](evidence/wave0-tests.log) |
| cargo clippy --locked --workspace --all-targets --all-features -- -D warnings | [clippy](evidence/wave0-clippy.log) |
| cargo +1.88.0 check --locked --workspace --all-targets --all-features | [MSRV](evidence/wave0-msrv.log) |
| cargo build --locked --release | [release](evidence/wave0-release.log) |
| node --test web/app.test.cjs | [36 Web tests](evidence/wave0-web.log) |
| node --check web/app.js | [JS](evidence/wave0-js.log) |
| cargo deny check | [在线 deny](evidence/wave0-deny.log)；advisories/bans/licenses/sources 通过，已有 duplicate warnings 保留 |
| git diff --check | [diff](evidence/wave0-diff.log) |

脚本复现：

```sh
python3 scripts/enterprise-architecture-red.py
python3 scripts/enterprise-gates.py --wave wave0
```

## 基线修订、书面例外与剩余风险

1. 现有 “both_backends” 是文件/内存 SQLite，**不是 PostgreSQL**。PG 全部 v20 合同、lease/outbox、跨 tenant SQL 尚未实现。
2. CronStore 的 cron.json 仍是作业定义与历史 owner；MCP/Skill/Provider/Hook config 仍是文件配置。原 SQLite 能力不能被扩大解释为已有统一企业管理事实。
3. 四项生产无界队列（Gateway WS、daemon AgentEvent、ProviderEvent、summary assembler）按用户 Wave 0 允许的“书面例外与测试”分支冻结；另有非默认 test-transport 分支例外。[通道预算](channel-budget.md) / [精确 registry](channel-exceptions.toml)。本波没有改这些通道，也不声称所有队列已 bounded；Enterprise 网络开放前必须收口。测试关联与站点计数证明门禁/原业务合同，不能证明内存上限。
4. AST/Cargo 能拒绝已定义的禁止形态，不能证明任意跨文件 type alias、宏展开、字符串 SQL 或动态 owner 没有绕过。必须配合后续领域 transaction/authorization/真实故障合同与 review。
5. Docker server 29.2.0 可响应、Local 5432 可接连接、保存有 Kubernetes context；本轮只读探测，没有操作既有 DB/集群。**PG backend/OIDC/真实强隔离/K8s/外部商业 Provider/MCP/Keychain/CI/生产压力均未验收**，不能将“环境存在”写作验收完成。

回滚 Wave 0 仅恢复 tests/docs/scripts/planning，不影响数据。后续仍须按 W1→W10 逐波实现真实领域/数据库/编排/协议/入口/故障/运维闭环；禁止以本波 ADR/矩阵宣称这些目标已完成。
