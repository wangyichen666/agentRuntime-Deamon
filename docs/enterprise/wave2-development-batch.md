# Wave2 后续开发批次（未验证）

用户已明确调整为先开发、再统一测试、再修复，不再逐切片运行测试和门禁。以下描述是源码实施记录，不是验收证明。完整 Wave2–10 尚未完成，enterprise_ready=false。

S26：HookContinuationRepository三端口接重复请求读回、前台父关系、Stop准入；复用原同事务准入、父授权继承和continuation事实。读取保留永久归属，mutation持固定准入proof，原Stop成功结果和预算在准入事务内核对。

S27：HookExecutionRepository原claim/settle与有界snapshot/interaction接实际Hook调用。原HookOutcome仍是唯一结果事实；daemon协调器只保存执行句柄。父future Drop只取消token，受管任务等待claim、终止和回收子进程、结算。已启动后取消/超时结果未确认时保存原unknown状态，原claim幂等禁止重播。关闭先停止入口和producer、join Hook/outbox/维护、drain已接受SQL，再恢复事实和释放writer。未知状态、取消竞态及关闭顺序尚待集中验证。

S28：ScopedMemoryRepository提供冻结设置、候选/页/证据/评价/学习读回、保存/遗忘/曝光/反馈/摄入及维护诊断。SQLite复用原业务helper和四permit；source_run JOIN原scope在LIMIT前隔离，保存JSON绑定原列/摘要/owner并限制字段和页预算。原证据脱敏、receipt幂等、read_only、曝光不证明采用、遗忘不复活保留。RPC、模型上下文、Remember/Recall/Evidence工具、Cron固定准入和完成后维护已接await；原定时维护由至多一个受管批次执行，shutdown等待其结束。SQL/schema没有新增记忆副本，schema27未变。

尚未运行编译、测试、Clippy或真实环境回归。一次cargo fmt格式化成功只能证明解析和格式，不能证明类型或行为。后续仍须闭合MCP/管理端口及native PostgreSQL全量事实、租户唯一键、writer、audit/quota和离线迁移；现有PG发布协调不能代替这些能力。

开发追加 S29–S31（全部未编译、未测试）：S29资源八端口接RPC、/resources、后台执行/状态/日志工具和取消子树清理，原资源状态机/游标/进程身份唯一；S30交互四端口接read/list/respond/stamp，原Broker只交付等待者，回答处理注册原daemon协调器effect_tasks句柄集合，客户端Drop不能detach已接受的claim，shutdown统一join效果任务；S31将原DelegationRequest/Record移至core，八端口接实际spawn/read/list/wait/cancel/result reservation/RPC/工具/启动结束路径。child session复用原sessions::create_in与原授权继承；原继承目前仅Local，native明确拒绝NonLocal准入，不能称完整企业委派完成。三领域复用原Connection/four-permit/原事务，不新增业务事实副本。只进行开发格式化，未执行测试、cargo check、Clippy或九门禁，集中验证留待开发完成。完整Wave2–10仍未完成，enterprise_ready=false。

开发追加 S32–S36（尚未编译、未测试）：S32队列五端口接queue RPC、重启32条稳定游标恢复、订阅/取消/关闭终态读取；旧同步SDK队列和request查找共用同一scoped helper。父委派冻结cwd、request_approval、只读工具和预算复核，child也消费原1024队列总预算。S33会话控制三端口接运行装配、trace元数据、偏好选择和旧transcript导入，选择/导入在原事务核lifetime；全域偏好键和旧文件导入尚仅Local。S34维护四端口、计划管理四端口接doctor、启动/关闭恢复、GC、人工核验、RPC/斜线计划和旧计划导入，重启恢复及writer统计按SQL完整scope过滤；原Hook恢复校验绑定并保存unknown。S35观察三端口和旧记忆导入一端口接canonical诊断、context摘要、legacy artifact读取及旧文件导入；canonical批次digest/长度复用唯一decoder，历史artifact只核永久归属，无需writer。S36生命周期Hook由至多一个受管发布批次定时排空，存储失败保留待发而非ACK；Cron通过daemon弱引用任务委派器接固定执行HookScope及终态观察，不新建结果状态；idle探针受管，不在listener分支等待数据库。上述都是源码实施，不是并发或故障合同通过的证据。只有cargo fmt开发格式化；未执行cargo check/test/Clippy或门禁。当前schema27，全量PG事实和完整Wave2–10仍欠，enterprise_ready=false。

开发追加 S37–S38（尚未编译、未测试）：启动采用async bootstrap，同一事实源读取session元数据/偏好/旧历史并由唯一assemble_prepared构建daemon。native执行新的with_native_execution不携带旧ControlRepository，上下文、记忆、Provider捕获、Exec冻结配置均从async领域端口与固定owner读取；旧同步接口仅留SDK兼容，DaemonState生产字段已移除同步聚合。工厂目前仍接受具体RunStore，全量PG装配尚欠。静态复核同时补Hook恢复身份字段SQL预算、writer启用对损坏Hook JSON的拒绝及context摘要先永久归属核验。仅开发格式化，不运行编译/测试/门禁。

开发追加 S39（尚未编译、未测试）：新PgCanonicalStore复用原PG发布连接/driver，独立canonical schema v1（现有coordination仍v1）；已写现有业务事实及writer/audit/quota的完整四维域DDL、永久lifetime外键、scope RLS、不可变事实和scope版本时钟。native writer读/获取/续租/释放和绑定句柄维护已实现，mutation在原TX核固定proof并提交前复核数据库时钟。PG会话lifecycle Create/End/Close/Fork、元数据/偏好/Local旧历史导入与实际管理CLI已接，原Hook lifecycle publication同事务。PG事件append/read/page、版本封存和canonical outbox在原TX实现；NOTIFY仅唤醒。完整scope身份读取/冻结授权检验、初始化域CLI、审计hash-chain读取/幂等append及配额账户配置/预占/结算亦为真实SQL实现。当前PG runtime尚未可装配：RunExecution等其余PG端口、远程TLS、完整离线迁移仍欠，不称全业务PG或完整Wave2完成。依赖仅新增已在lock中的serde/sha2直依赖；cargo fmt开发格式化成功，未cargo check/test/Clippy/真实PG或门禁。完整Wave3–10尚未进入，enterprise_ready=false。

开发追加 S40（尚未编译、未测试）：PG RunExecution五方法已写真实普通/独立workload准入、固定owner和冻结授权、请求/计划receipt幂等、scope队列预算、稳定run计数、队列启动及TurnCommit事务。工具未确认/Provider未确认/已启动副作用取消共用core normalize_run_terminal_status（原SQLite caller也调用同函数）；开放tool exchange结算unknown并闭合原transcript，Provider attempt归unknown，usage/planCAS/终态/委派result原事实同TX。新增PG队列五端口、完整域游标读取和元数据查询，实际控制CLI沿原scope仍enterprise_ready=false。PG plans原document/version/identity/decision helper已写，仍欠完整计划端口、原context/compact/工具/请求等PG适配与Daemon PG装配、离线迁移。静态读取发现grant提取中残留tx变量、队列恢复limit原端口为usize/请求lookup返回RunRecord，已按原声明修正；没有运行编译或测试，不把静态修复算验证证据。

开发追加 S41（尚未编译、未测试）：PG canonical/model/omitted会话读回及列表由同一REPEATABLE READ事务构建，包括原history/队列/交互/终态/计划/上下文与固定scope；canonical message IDs移入core由SQLite/PG共用。PG Hook观察三端口与执行四端口真实实现，claim保存原running并ACK原publication，settle保留unknown不重播；Hook合法性移至core共用。历史Hook只核永久归属，运行Hook核固定授权及writer，ownerless生命周期核当前head proof，已删除head只可协调，不能执行旧run。静态复核发现SessionSnapshot.prefix_digest原为按batch raw拼接哈希，PG改回同语义；transcript/Hook添加原rowid等价的storage_row_id稳定顺序，lifecycle退休proof退出续租集。原RunQueue接口签名已按usize/RunRecord修正。只有cargo fmt，未执行cargo check/test/Clippy/门禁/数据库。离线工具和其余PG端口、PG daemon装配仍欠，全Wave2–10未完成。

### S42–S43 开发记录（尚未验证）

按用户“先按规划开发，完成后测试再修复”继续：新增 PostgreSQL RunFact（4）、Provider attempt（4）、interaction（4）、RunTurn（6）、discovery/cancellation（4）、RunTool（6）、对象 artifact 发布（3）的真实领域实现。身份、永久 lifetime、原 SQL 绑定和字段预算先于正文，mutation 携带已绑定 writer proof 并在提交前复核数据库时钟；开始/终态事件与 outbox 同事务，网络对象 I/O 在事务外。

两种后端共用 Provider attempt 身份解码、终态调用身份、实时事件许可集合、interaction 正文规则、计划 stage/摘要/Markdown 验证、冻结目录发现许可、artifact receipt/completion 验证。原 no-follow 文件发布实现移动到 agent-artifact 共用，SQLite 原 API 通过重导出保留；PG 的宿主文件发布必须显式配置且只允许 Local，四 permit 包含取消后仍在执行的文件 worker。PG 工具收据与对象 published 同一原事务，没有第二套工具终态。

补了当前角色与冻结授权角色的比较；canonical DDL 添加 JSON 正文约束、原事件不可改写、published stamp 保护、Hook/对象发布单调标记及稳定 Provider/discovery 插入序号。DDL未运行。仅 cargo fmt 格式化成功，不代表编译或行为正确，S25旧验收不覆盖本树。请求/context/compact、记忆、资源、委派、管理/维护、runtime装配、远程TLS及离线迁移等仍待开发；完整Wave2及Wave3–10均未完成。
