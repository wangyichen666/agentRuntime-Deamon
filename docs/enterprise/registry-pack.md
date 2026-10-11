# 注册表与签名发行代

当前实现：agent-pack负责严格JSON发行物、SemVer、规范JSON/SHA256、Ed25519 verify_strict签名和有界内容扫描；agent-plugin负责四级信任约束和同层coordinate冲突；agent-registry负责六层EffectiveConfig、来源、灰度和显式回滚纯规则。SQLite35/PG9在同canonical数据库增加注册控制、不可变包/安装代、信任根历史、operation receipt与session lifetime绑定。发布和审计同事务，入口没有第二session/run/审批状态。

管理入口：RPC registry.mutate；CLI `my-agent registry --request FILE --workspace WORKSPACE`；HTTP POST /enterprise/registry。mutation必须包含operation_id、expected_revision和严格command；ConfigureTrust只能由IdentityManage授权，Publish需要PluginInstall，Rollout/Rollback需要AgentConfigure。外层RPC鉴权与数据库最终当前主体/角色/策略验证均执行。

读取：registry.readback返回有限代索引，registry.generation返回一个完整不可变代。CLI `registry --generation DIGEST`与HTTP GET /enterprise/registry/generation?digest=…读取同一EffectiveConfig digest和sources。索引不携带全部包文件；文件保存在canonical不可变pack_json，运行不从入口解压或执行其任意路径。

发行物SignedAgentPack包含manifest、contents、key_id和signature_base64。签名消息为UTF8 `agent-pack-v1`加一个NUL字节，加manifest规范JSON。规范JSON递归按键排序、无空白、不使用浮点；可选config patch未设置字段省略。Ed25519签名和32字节公钥用标准base64。SHA256用64位小写hex。外部包不能声明builtin；builtin来自受信binary的原内置工具装配。

限制：256文件、路径仅ASCII相对普通路径（无..、绝对路径、反斜线或重复路径），每文件编码512KiB、总编码2MiB；仅UTF8文本内容，不支持压缩归档、ELF或任意包内进程。ZIP/gzip输入直接拒绝。CLI打开普通文件时使用NOFOLLOW。扫描在数据库事务外，有4个并发预算；原事务复核当前revision与trust digest再发布，失败不更新current rollout。信任根版本不可重写，撤去原信任后旧安装代不能用于新session，需重新发布新SemVer版本；在途固定代不自动重写。

配置层级：defaults < organization < tenant < project < agent < session_override。每层校验，扩展按唯一coordinate稳定覆盖并记录before/after摘要；同层重复直接冲突。secret_refs只接受secret://引用，低信任包不授予network、secret、MCP或Hook能力；安装不代表获准执行。

原会话创建事务固定generation，原run snapshot再冻结它。实际LoopEngine使用固定prompt/skill文本、工具白名单、模型候选及轮次/context/tool预算；重启从原run snapshot恢复。原Provider capture/attempt/usage/未知结果语义不变。Hook只可使用固定代匹配的已受保护配置摘要，MCP工具还需匹配server配置摘要和reviewed信任；完整Connector配置材料化/进程隔离在Wave6继续。

局部SQLite与真实PG合同通过：签名内容篡改拒绝、发布幂等、层级来源、旧session不迁代、canary/rollback只作用新session、跨scope空读。真实HTTP→冻结模型prompt验证通过：current V2而原session模型实际使用V1，无tools，Gateway重启读回同run；证据evidence/20261010/agent-wave4-registry-runtime-smoke2.json。完整并发/故障/CLI-ACP-HTTP parity、供应链及外部部署验收待执行；enterprise_ready=false。
