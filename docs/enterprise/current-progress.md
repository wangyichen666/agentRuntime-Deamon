# 当前企业升级进度

完整 Wave2–10 尚未完成，enterprise_ready=false。下表区分完整 Wave 与 Wave2 内部纵向切片；S15 不是 Wave15。

| 范围 | 当前状态 | 可核验证据与剩余工作 |
|---|---|---|
| Wave0 基线与架构门禁 | 已本地验收 | baseline.md / baseline-index.md / ARCHITECTURE.md；非生产发布验收 |
| Wave1 身份与授权 | 已实现主要切片，待完整验收 | 受保护企业域绑定、持久身份管理、冻结授权与显式 epoch 撤销、原 pending 双人审批；双后端治理局部合同及 PG 冻结策略合同通过 |
| Wave2 存储企业化 | 已实现主要切片，待完整验收 | 全 canonical 端口装配、远程 TLS 配置、同事务 quota、完整表离线迁移及切换回滚；双租户真实启动隔离通过，远程 TLS 握手及大 artifact/fault 矩阵待验 |
| Wave3 访问面 | 已实现主要切片，待完整验收 | 短期JWT/JWKS、Argon2id凭据、PKCE一次消费、持久多端点目录、双tenant单Gateway、共享限额、原TX细粒度费用账本；远程IdP/HA压力/完整故障矩阵及工具resource费用专项待验 |
| Wave4 注册/配置/发行 | 进行中 | pack/plugin/registry、SQLite35/PG9、双backend签名/代际/CAS/灰度/回滚局部合同通过；真实冻结配置模型入口冒烟正在修正共享端点白名单后重跑 |
| Wave5–10 | 待实施 | 后续持续真实实现；完整门禁后置，不执行生产部署 |

最近已验证的 S25 源：656 Rust/36 Web/49实际daemon、45执行体、14外部ignored，九项门禁全 exit0；真实TLS S3四组业务通过，S25最后PG回归未执行。S24此前真实PG发布协调10项通过，仅证明发布后端。详见 [S25报告](wave2-s25-hook-observation-report.md)。

此前开发批次 S26–S43：continuation、Hook受管执行与关闭收敛、记忆、资源、交互及委派、队列、会话控制、维护、计划管理、诊断领域异步端口和真实调用者接线，Cron/生命周期Hook受管排空已写入。2026-10-09 敏捷续开发已完成 daemon/storage-postgres 的局部编译并修复遗留编译错误；此前全量测试仍未重新执行。不能用S25的656项结果证明当前树。

S15 已接原工具六异步端口、固定准入 owner，并在未知工具后停止后续执行；12工具存储合同、83 storage单测、132 runtime单测和48真实daemon合同已纳入最终验收。S16 原turn/transcript/执行计划六端口已接实际前台运行链路；六项越域及两项计划执行RED已修，16项合同/storage83/runtime132和完整九门禁、真实环境回归通过。S17 请求捕获/上下文三端口与实际模型/摘要/读回已实施，六项有效RED和15项存储合同通过；库及真实普通请求/摘要daemon局部回归通过。S18原事件永久来源及缺stamp损坏校验已实施，12合同通过；S19 Cron固定准入owner、同事务后台准入及Local独立workload已实施，局部及统一九门禁/真实环境回归通过。当前schema27；S20原上下文/compact及S21即时事件投影已实施并完成本地验收，context19/event18通过，S22原实时事件写入已实施、storage85/event23/真实daemon48局部通过；S23发现许可与取消四端口已实施、发现14/runtime137/storage86局部通过。已纳入S22/S23修正后最终九门禁及真实环境验收，见上述报告。

S39已进入native PG canonical schema/writer/会话生命周期/事件封存/身份读回/audit/quota开发，尚未编译或测试，管理CLI已写；PG runtime尚未可装配，远程TLS和离线迁移仍待开发。

S42–S43已继续写入PG运行事实、Provider attempts、interaction、RunTurn、发现/取消、工具批次与对象artifact发布；共享原文件发布能力及纯身份/摘要规则，尚未编译或测试。

完整 Wave2 主要剩余：企业租户启动/路由、远程TLS、SQLite audit/quota对等及实际配额消费、可校验和回滚的SQLite→PostgreSQL离线迁移与完整共享合同。本机 Local 兼容身份的 PG adapter 已可装配全部 26 个 daemon 领域端口，不能据此称完整企业多租户 backend 完成。以下较早批次的“PG 端口仍缺”等描述是当时状态，最新实现与验证以本页末尾及 [本批报告](wave2-native-runtime-report.md) 为准。

未提交、推送或部署；本地验证不等于 CI 或生产验证。

### 2026-10-09 敏捷续开发

源码复核发现 PG 请求捕获和上下文/compact 端口已存在，按源码复用而未重复开发。新增 PG RunResourceRepository 八个方法：同域预算锁、writer/current generation/frozen authorization、原事务事件/stamp/outbox、退休 lifetime 读回、64KiB稳定日志及 orphaned 证据核验。SQLite/PG共用状态/日志/证据规则，空输出不分配游标，核验正文含前缀总长有界。修复前批遗留编译错误；agent-daemon 与 agent-storage-postgres 局部 cargo check 已通过，不代表全树测试或完整门禁。最终局部测试结果另记本轮报告。schema不变，完整 PG runtime仍未可装配，enterprise_ready=false。

最终结果：agent-daemon + agent-storage-postgres 局部 locked/offline check exit0，无告警；core资源3、SQLite资源2、真实PostgreSQL资源1全部通过。真实PG18.6合同包含事件插入故障下资源/时钟/events/stamp/outbox回滚，重建连接读回永久事实；未运行实际后台进程，不称完整远程worker验收。自己的临时测试数据库已删除。开发格式化与diff检查通过，未跑全量测试/九门禁；未提交/推送/部署。资源端口切片完成，完整PG runtime与Wave2–10仍未完成。

本轮继续补齐 PG 计划四方法、诊断三方法和 Stop 续跑三方法；daemon 实际 Local 启动已接同一 adapter 提供的 26 个领域端口，原 async SDK 委托同一路径。丢弃计划复用原终态事务，完成计划/回执写入后再统一封存事件版本；SQLite/PG 共用 transcript decoder。最终生产局部 cargo check exit0 无告警，本轮未跑测试和实库合同。详见 [续开发报告](wave2-plan-observation-continuation-report.md)。


### 2026-10-09 PG 记忆、委派、维护与本机启动

已实现 PG 记忆12/维护1/旧记忆导入1、委派8、daemon维护4方法；同原准入/终态/事件事务，canonical迁移v2实际执行。daemon显式sqlite/postgres-local选择已接26个端口，Cron/导入/artifact复用同事实源，独立Local workload保留。修复异步模型请求预算、UUID trace校验及恢复资源归属查询。真实PG18.6局部合同1项通过，生产编译和test-support测试源码编译通过；真实binary双后端启动/对话/重启去重通过，PG未生成SQLite事实库。未跑完整测试或门禁；完整Wave2–10未完成。详见 [本批报告](wave2-native-runtime-report.md)。

最终补正：真实PG chat暴露执行future与事件消费者等待同一连接的闭环；改为join并行驱动，已接收事件完成及剩余队列排空后才提交终态，未引入第二执行状态。最新binary的两个后端冒烟均exit0，使用本机假模型及独立临时工作区。

### 2026-10-10 持续开发：TLS、账本与离线迁移

已实现远程PG严格CA/hostname TLS及可选mTLS配置（真实远程握手待验收）；SQLite schema28和PG原准入事务的runs配额预占/终态结算、审计hash-chain、audit/quota RPC与CLI均已接线，装配29个领域端口。同一ledger合同SQLite与真实PostgreSQL18.6分别通过；第一次include-ignored因未设置测试DB环境变量启动失败，不计行为失败。

全canonical离线工具prepare/dry-run/copy/verify/switch/rollback已接真实PG管理入口，保留原源库、artifact store_id、backup证据和daemon切换marker。自有fixture 68表52行（artifact0）完成真实dry-run、提交、逐表逐行对账、切换与回滚；回滚后正确格式PG迟到mutation返回Conflict。首轮deferred FK触发器未清空导致ALTER拒绝，已先SET CONSTRAINTS ALL IMMEDIATE修复；回滚等待栅栏前固定RR快照风险已改READ COMMITTED。上述局部没有证明大artifact、远程PG、切换后真实daemon和迁移故障矩阵全部通过。

当前正在接企业RuntimeIdentity受保护启动配置与全daemon scope路由，并修复PG普通policy/role发布错误撤销在途run的语义；尚待必要编译与局部身份验证。完整Wave1/2仍部分，Wave3–10仍待实现，enterprise_ready=false。未全量测试/门禁，未提交/推送/部署。

### 2026-10-10 企业域与身份治理续开发

当前 SQLite schema29、PG canonical v3、daemon 同源30领域端口。真实代码已接企业 RuntimeIdentity、会话事务授权、身份注册/角色/策略/撤销 RPC+CLI、冻结策略与双人审批。双后端 identity_governance_contract 两项通过（含撤销旧票、幂等与越域拒绝），PG ordinary_publish_preserves_run_but_explicit_revocation_fences 一项实库通过。生产 daemon/storage-postgres/cli 局部 check 和真实 binary build 成功。双租户 daemon 冒烟首次因夹具会话名不符合原格式被拒，已修夹具待重跑。Wave3 WebSocket128帧/8MiB/64并发、connect/write/idle deadline 和慢消费 detach 已接真实入口，局部check通过；完整慢消费/断线验收未运行。Wave1/2完整验收与Wave3剩余身份/directory/共享限额仍欠，Wave4–10尚待实现，enterprise_ready=false。未提交、推送或部署。

企业scope真实binary冒烟最终通过：两个租户同一PG schema、同project/agent/session/request ID，各自run-1，重启幂等，模型总2calls，quota各消费1，审计scope一致，未创建SQLite事实库。夹具首次错误已修并保留诊断记录。证据见docs/enterprise/evidence/20261010/agent-wave1-scope-smoke.json。Wave3已新增jsonwebtoken9.3.1有限JWKS验签、可信scope/subject/epoch transport envelope、Gateway/daemon双端接线，正在局部编译；尚无OIDC行为验收。HTTP审批改为pending/detach，禁止因HTTP不能交互而伪造拒绝。新增依赖后--locked拒绝未更新依赖边，正在一次offline更新lock，不算行为测试失败。

### 2026-10-10 Wave3 Access Plane实际接线

新增jsonwebtoken9.3.1（固定lock，Rust1.88），有限JWKS缓存（32keys/128KiB/TTL/5秒刷新冷却）、HTTPS/DNS pin/禁代理与重定向、RS256/ES256/EdDSA允许集、issuer/audience/exp/nbf/iat/短期access用途及可信scope/subject/epoch校验。BearerCredential Debug脱敏，不进入params或canonical facts；Gateway/daemon独立验签，原身份/角色/策略事务作最终授权。SQLite schema30/PG canonical4新增session_directory和Access租约/计数，daemon同源31端口。Directory创建复用原session create helper并同TX审计；路由不可变，真实HTTP /enterprise/sessions、/enterprise/rpc只公开opaque session_id。租户/主体/endpoint三层数据库时钟固定窗口和并发预算，PG同tenant advisory lock；请求先获取共享lease，流body持有lease，Drop异步release、过期收敛，不生成第二run。HTTP30s首响应、240s总流与15s idle预算，WS256本机连接、64并发、128帧/8MiB以及240s企业连接预算。上一批局部cargo check通过；最新WS目录接线、共享合同与OIDC mock合同正在必要编译/验证。仍欠登录nonce/state/PKCE持久化、APIkey Argon2id/服务凭据、共享限额配置/完整用量成本、全网关多runtime路由；Wave3未完成，Wave4–10尚待实施，enterprise_ready=false。

### 2026-10-10 Wave3 访问面与凭据纵向切片

SQLite schema31 / PG canonical v5，daemon 同后端装配32领域端口。API key仅落Argon2id PHC，首发行返回明文，同operation重读不再返回；撤销同事务递增主体epoch。Gateway使用固定服务主体+Gateway角色的独立凭据证明入口channel，外部params不能决定授权属性。

局部验证：OIDC最小JWKS签名/域/audience/expiry/未知kid合同1项通过；Access+凭据共享合同SQLite/真实PG各1项通过，含24连接竞争只16个准入、凭据幂等及撤销。真实binary HTTP→UDS→PG冒烟通过：opaque目录幂等、用户chat、跨租户401、假模型只调用1次。先前fixture audit limit128超过64，改fixture后通过；不能将此前失败记为通过。证据在evidence/20261010。

仍在开发：进程内token bucket和目录current lifetime额外复核已写入，尚待本切片编译；登录state/nonce/PKCE、真正轮转/stale矩阵、multi-endpoint路由及细粒度费用账本未完成。完整套件、Clippy、release、MSRV和真实远程OIDC/集群验收未运行，enterprise_ready=false。

### 2026-10-10 登录与访问面续开发

新增SQLite schema32、PG v6一次性登录事务，Gateway /auth/login 与 /auth/callback 接原daemon端口。S256 PKCE、HttpOnly/SameSite/Secure cookie、issuer/state/nonce/ID-token subject/audience/azp 检验已实现；消费CAS发生在授权码兑换之前，未知兑换结果不重放。32端口保持同后端，OAuth不是第二业务session。OpenAPI3.1静态导出已接/openapi.json。

真实HTTP授权码登录冒烟通过：错误state不消费，合法回调返回短期token（no-store），回调重放401、token端点只调用1次，原opaque session可读。双后端一次性消费/错误verifier/重放拒绝合同各1项通过。身份合同含nonce/主体绑定1项通过，第二把真实Ed25519钥匙轮转、刷新冷却和过期stale拒绝合同1项通过；TTL测试调整私有缓存时钟，未实际等待30秒。

进程内三层token bucket和current lifetime保护已写入且生产编译过；targeted bucket unit尚待执行。持久目录多端点路由正在接线，受保护端点map、运行实例endpoint+scope校验、按原operation查已发布目录已写；最新路由树尚待编译/双端点真实冒烟。细粒度input/output/cache/tool/sandbox费用账本和分层预算尚未完成，Wave3仍进行中。credential.status已受统一RPC准入核验，未发现原先假设的独立JWT绕过；显式重复查询已删除。

### 2026-10-10 双端点路由诊断与费用账本接线（开发中）

双daemon在同企业scope/schema启动已成功，NonLocal默认会话按scope+受保护runtime endpoint稳定派生，保留同会话writer排他。发现并修复JSON-RPC合法result:null被Option解码为缺失的协议问题；局部null成功/非法双字段合同1项通过。首次修复后的双端点冒烟已通过目录创建/路由到runtime-two和真实chat，但重启后run.read因Gateway使用错误的嵌套DTO字段返回403；已改为原平铺run与_my_agent_view核对，尚待重建重跑。此前失败没有记作验收成功。

费用账本正在实现：新增BillingPolicy/FrozenBillingPolicy整数微单位价格和tenant/project/agent/model预算、双backend BillingRepository管理员发布/读回/分页用量、schema33/PGv7前向迁移（历史迁移不改）、daemon同backend装配33端口、RPC/CLI/HTTP管理入口、run准入同TX预占、Provider终态同TX原usage计价、未知结果保留预占、冻结provider输出请求上限/轮次/tool次数/输入capture预算、exec/resource wall timeout与用量接线。必要adapter与第一批入口check已通过；最新结算/沙箱树仍在编译，迁移尚未经本切片实库合同验证，不能标为已完成。完整价格/预算/未知结算并发验收未做。

SQLite OAuth过期清理和pending计数已改为完整scope，与PG RLS对齐。Wave3仍开发，Wave4–10尚待实施；enterprise_ready=false。未运行完整测试、Clippy、release/MSRV或生产部署，未commit/push。

费用原事务局部合同已在SQLite和真实PostgreSQL各通过1项（agent-wave3-billing-contract5.log）：不可变价格/operation幂等、同scope多请求拒绝超预算、PG两连接共享预算、旧run继续原价格、重复Provider终态单收费、完成结算、软预算审计、未知Provider终态保留预占、原view terminal保留。SQLite的原四permit背压允许StorageUnavailable拒绝，未改为无限队列。前3次夹具编译错误和首次并发夹具误判已修，未写作行为通过。

schema33/PGv7财务迁移已随合同实库应用验证；新的共享endpoint粒度迁移schema34/PGv8正在构建，保留v1–v7校验和。新增原Access lease endpoint，可信Gateway按实际路由名在原准入预算事务中检查endpoint速率/并发，不再把全部HTTP路径仅合并为request类别。最后binary构建和真实双端点+登录+费用HTTP链路正在运行，尚无最终成功证据。

真实组合入口冒烟通过（agent-wave3-dispatch-order-smoke.json）：同scope两个daemon端点、immutable目录runtime-two选路、Gateway优雅重启后原op/原run读回、OIDC PKCE/nonce一次性兑换、HTTP发布费用策略、Provider wire携带max_tokens=128、原PG用量费用30微单位结算、各层reserved=0/consumed=30、模型只调用1次。原首次费用冒烟暴露运行时先start attempt再capture的顺序问题，已改为capture→原TX start/budget→dispatch；失败未重放原run，重新自有fixture验证。schema34/PGv8和最新Access/Billing局部共享合同共4项通过。最后scope单实例路由扩展/工具resource专项仍在开发，Wave3不标全量完成。

### 2026-10-10 多企业 Gateway 纵向切片已局部验证，开始 Wave4

当前 schema SQLite34 / PostgreSQL canonical8，daemon33同后端领域端口。单 Gateway 受保护 AGENT_GATEWAY_SCOPES 配置最多16个完整scope，JWT未经验证的scope只用于有限配置选路，选路后Gateway和daemon分别完整验签/原事务授权。API key以X-Agent-Scope或WS首帧scope选路；错误/跨scope均401。OAuth cookie按别名隔离，额外OAuth回调必须包含配置别名；客户端连接缓存上限256。

必要cargo check/build通过；真实HTTP Gateway + 三daemon + 同PG schema的两个tenant冒烟通过：同名主体/Agent/operation的opaque目录互不访问，B的API key仅在B配置可用；原A双运行端点/Gateway重启/授权码一次消费/输出上限128/费用30微单位/模型仅1次调用仍通过。证据evidence/20261010/agent-wave3-multi-scope-smoke.json。未测试真实远程IdP/HA/多Gateway压力、工具与resource费用专项仍待验收；Wave3功能主要入口实现，不据此宣称完整验收。Wave4注册表/配置/签名代际正在实施，Wave5–10尚未实现，enterprise_ready=false。

- Wave4注册/config/签名纵向切片：SQLite35/PG9实库共享合同各1项通过；34同后端daemon端口。CLI/HTTP/RPC发布与读回、原session绑定/准入TX/原run snapshot/LoopEngine真实接线。最新MCP来源/Hook信任/根历史/CLI NOFOLLOW改动必要binary build进行中，随后真实HTTP固定V1而current V2模型请求冒烟。完整验收与Wave5–10仍未完成，不退出任务。

- Wave4真实入口组合smoke通过：同Gateway两tenant/三个daemon/实PG9，先发布签名V1并创建原目录会话，再发布V2+100%canary，旧会话实际模型请求含V1 prompt且无tools，重启仍读原run；费用30微单位、输出128、1modelcall和PKCE一次消费保持通过。证据agent-wave4-registry-runtime-smoke2.json。注册/config/发行主要纵向切片已实现并局部验证；完整并发/归档CLI安全矩阵/三入口parity待验；Wave5九类持久DAG/native引用/A2A正在开发，Wave6–10仍待实施，继续不停。

### 2026-10-10 Wave5 持久编排纵向开发

SQLite36 / PostgreSQL canonical10，daemon35同后端领域端口。工作流DAG严格DTO、冻结输入digest、数据库时钟lease epoch、节点CAS、不可变定义/operation/event、原run同TX关联和独立workflow workload已实现。AgentRun复用原chat；ToolCall直接进入原工具预检/审批/收据/交换事务；Approval复用原interaction，Webhook单次公网HTTPS/DNS pin/禁代理重定向/禁自动HTTP retry；SubWorkflow同TX创建与关联。取消/deadline走原run与child控制；反向补偿使用新occurrence，仅针对确认完成节点，不改原收据。

局部证据：SQLite工作流合同1项通过；PG首次合同失败发现新run分支漏关联，修正后实PG合同1项通过，失败夹具已清理。检查包含A2A0.3四方法投影与补偿主体已成功；最后结果幂等/真实binary更新仍在构建。不能将上述较早合同当作最终补偿/全功能验收。A2A只接受user纯文本，不允许媒体、system角色、跨任务引用或传输config覆盖；artifact/status/history来自原workflow，客户端断流不取消。

仍在补：完整补偿/lease故障局部合同、真实HTTP后台执行与A2A重启冒烟、artifact原收据引用、人工待审批状态投影/恢复和全部内部命令operation幂等、未知节点显式reconcile。Wave6–10未实施，完整验收待后置，enterprise_ready=false。未提交/推送/生产部署，继续开发。


### 2026-10-11 用户要求提交当前代码到远端

本次按用户明确要求准备开发检查点提交与推送，覆盖此前未提交的真实代码、迁移、配置、脚本及验证记录；原“未经要求不提交/推送”已获得本次授权。不是 Wave10 完成声明，也不是生产发布。

最新 Wave5 会话绑定构建已成功（agent-wave5-session-binding-build.log）。前三轮真实 A2A 冒烟未通过：首轮 Python 模块名冲突、第二轮缺 capability 映射、第三轮原 SessionStore 文件名前缀不匹配；已分别修正，真实全链路仍待重跑。节点首次 native session key 现在通过原 session_key_for_operation 派生并由内部 workload 在 canonical TX 固定。派生 Subagent/Stop continuation 已清除仅属于父工作流节点的执行指令，保留其余冻结授权，避免再次执行父 ToolCall/Webhook。

Wave1/2 已补多个基础纵向切片，Wave3/4 主要入口已实现并局部验证；这些均不等于完整验收。Wave5 部分实现，人工待审批投影/恢复、未知节点显式 reconcile、完整内部命令幂等及故障合同仍欠；Wave6–10 尚未实现。完整测试、Clippy、release、MSRV、供应链及发布门禁未运行，enterprise_ready=false。下一步继续 Wave5 最新树合同和真实 HTTP 冒烟，再补其剩余功能并推进 Wave6–10。Python 缓存不提交，公开 OIDC mock 测试私钥仅为无生产信任关系的测试夹具。

提交前最新局部验证：`cargo test --locked --test workflow_contract sqlite_workflow_native_fence` 通过（1 passed）；包含最新会话绑定及派生指令清除源码的编译。日志已归档 evidence/20261011/checkpoint-workflow-contract.log。完整套件及 Wave5 最终真实 HTTP 冒烟未运行；历史日志原样保留，源码/迁移的 git diff --check 通过。
