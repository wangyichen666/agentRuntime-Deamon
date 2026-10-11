# ADR 0007：Agent/App Pack、插件与连接器的信任和代际发布

日期 2026-10-07。状态 **proposed**；Local Skill/MCP/Hook manager 已实现，企业 registry/pack/plugin/connector-host/OAuth 未实现。

## 决策与 owner

AgentDefinition 不可变版本绑定 agent_uid/scope、prompt/model/tool/skill/MCP/hook/sandbox/resource refs。App Pack manifest 引用每项 canonical digest，semver/schema/大小/文件数严格限制；Ed25519 用已审计开源库，不自研密码算法。信任根由租户管理面版本化，签名有效不代表运行授权。

新增 registry/pack/plugin crate 只在有真实 publish/install/runtime admission 调用链时创建。解析验证与执行分离；builtin/organization-reviewed/project-local/untrusted 是策略输入，tool/hook/network/secret 权限由 policy hard constraints 决定。“安装/配置/执行”三种不同 action；不能在 connector/plugin 中新建 approval/retry/session owner。

出版事务 stage→validate schema→scan archive→verify signature/trust version→immutable store→registry publication/generation CAS。路径穿越、symlink、重复路径、超大解压/zip bomb/嵌套输入、错误 digest/签名全部 reject。scan 只受控读取，不执行包内代码。

## EffectiveConfig 与代际切换

defaults<org<tenant<project<agent<session override 每层校验 schema，产 immutable EffectiveConfig/digest/provenance；secret 仅引用，不出普通 config DTO/审计 diff。稳定冲突规则 project explicit>tenant reviewed>builtin；同层重复唯一 coordinate 明确 conflict/evidence，不静默后写覆盖。

prepare/validate/probe 成功后 publication transaction 更新 current generation；旧 session/run 冻结原代不追改，新 work 选新代；canary 保存持久 rollout 规则。rollback 是发布旧 immutable generation 的新决策，历史不覆写。失败的 prepare 不影响 current。

## Connector Host 与 OAuth

复用 McpManager/标准工具桥；host 独立进程只持 connector 包/transport，daemon 持 generation/lifecycle，tool admission/schema/safety/approval/receipt/retry 仍走同 owner。host crash 有界重启+unknown receipt，不重放 ambiguous MCP call；单 server 失败不阻塞其他已连接实例。

配置可保存不等于可 spawn/connect。spawn 前重查 policy/trust/network/secret/tenant/resources；reload 只准备目标 server，新 generation 探针成功再替换并 drain 旧代，不中断无关 server。不能证明安全的全量 reload 必须说明影响并保留已连接 server，而非复制第二 manager。

OAuth code+PKCE/state/nonce/resource/audience、refresh rotation/concurrency CAS/revocation 绑定 tenant+subject+server+state。token 只 SecretStore reference；Keychain/Secret Service/Credential Manager 或公开 Vault/K8s Secret 不可用即 fail closed，不能 JSON 明文回退。HTTP MCP 继续 HTTPS、DNS pin、redirect/proxy policy 与 response/rate/concurrency budget；私网访问由 tenant policy 明确允许。

## 事务、回滚与证据

DB 不持锁等待签名扫描/进程探针/OAuth 网络；prepared receipt/generation 先持久，结果发布同事务复核 scope/policy/generation/digest，旧回调零写。锁序 registry coordinate→generation→publication/audit；OAuth connection key→refresh generation→secret reference receipt。

验证篡改 archive、错签名/信任根撤销、两代 canary/rollback、跨 tenant token/state/refresh 并发、server 重命名、单 host crash、增量 reload 和 canonical receipts。当前 Local MCP/Skill 合同不能代替上述验收；真实 secret 后端/企业发行包/connector host lanes **未验证**。
