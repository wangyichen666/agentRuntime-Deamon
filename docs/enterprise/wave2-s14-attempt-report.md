状态：implemented / locally verified。完整 Wave2–10 未完成，enterprise_ready=false。

# Wave2-S14：原 Provider attempt 的异步事务和未知结果

RunAttemptRepository 四项方法由 foreground runtime 真实 await：Started、terminal、累计 usage、circuit/retry/fallback 事件。SQLite 共用四 permit、原 Connection 和原事务体；scope、固定 ExactOwner、当前 head/永久 lifetime、writer proof/冻结授权在 mutation 事务内复核，网络调用始终在事务外。Cron 兼容路径仍使用同一原同步 helper，尚未全部异步化。

原普通 INSERT 和 attempt_id 唯一键已有防覆盖，未虚构 INSERT OR REPLACE 缺口。原 terminal 只核 run/round/candidate，可改 profile、API、model、开始时间；四项各自有效 RED 后，在共用 decoder/helper 核不可变身份。原 run finish 只检查 Started，已 persist OutcomeUnknown 可返回 Completed；真实 RED 后把 native/旧拼写 unknown 纳入原 unknown 收口。相同 payload 重复提交零新 clock/event，冲突拒 Conflict，坏 SQL/JSON 身份拒 Corrupt，driver 故障不泄 SQL。native 新 attempt 还在同一事务核 running 和冻结 route candidate/profile/API/model；有效 RED 的第一个 profile 断言截停，GREEN 覆盖四维。

真实 HTTP 合同先完整读取模型 JSON 请求体，再直接断连。有效 RED 观察 3 次请求和 run Failed。驱动新增内部 dispatch evidence：reqwest 确证连接/构建失败为 NotSent；4xx 为明确拒绝；canonical 完成路径为 Completed。默认、5xx、流故障、现场 deadline/取消为 Unknown；没有收到输出或 Connect 标签均不能证明未发送。Unknown 先 persist attempt OutcomeUnknown，立即返回，不进入 retry/fallback/overflow 重建；原 run/turn/event/outbox 同事务收口 typed -32002。真实断连、已接收后取消、静默 timeout 三合同均一次请求、原 unknown、重启不重放。该证明针对受控真实 HTTP 协议服务，不冒充商业 Provider 验收。

原安全 retry 合同继续覆盖明确429拒绝及 usage 计量；旧模拟503与真实5xx自动重试假设按新保守语义修正，新增真实5xx拒重试断言。Provider error kind/diagnostic 原 wire 保持；dispatch evidence 不从请求 DTO、上游字符串或诊断字段赋权。

12 项 storage integration 合同 GREEN：四项 immutable、persist unknown、四域/owner/absence、原事务故障与重复提交、decoder 损坏、冻结 route、current_thread timer、两个原 Connection terminal 竞争、每项方法撤销/租约过期零新 epoch。原 runtime 131 单测 GREEN；共享 adapter 满额合同已加入四新方法，最终全量门禁待确认。timer 在实际 SQLite 写锁等待期间保持响应，未放宽超时或加栈。

有效失败证据：wave2-s14-attempt-five-red.log / wave2-s14-provider-wire-red.log / wave2-s14-attempt-route-red.log。初 Drop fixture 所有权/遗漏 crate 直接依赖/遗漏 await 编译错误保留，均非功能 RED。局部 GREEN：wave2-s14-attempt-expanded-contracts.log（12）/ wave2-s14-runtime-local.log（131）/ wave2-s14-provider-wire-all-green.log（3）。

schema25保持，无第二状态机、第二事件账本或新第三方依赖。runtime 仅增加既有 workspace storage-ports 直接依赖边；沿用 v25 与 OutcomeUnknown 兼容窗口，旧二进制不能原地写升级库。未提交、推送或部署；完整业务 async、全事实 PostgreSQL/正确租户键/native PG writer、audit/quota、离线迁移和 Wave3–10 仍待完成。

首次完整门禁保留：wave2-s14-attempt-final（tests因依赖清单遗漏、Clippy因clone_on_copy失败；其余7项通过）。已只登记具体runtime→纯core ports边，不放宽禁止依赖与owner护栏；删冗余clone。全daemon回归初两项旧断言把已接受Provider的取消当Cancelled，按明确unknown语义增加attempt及重启不重放断言，原排队未发送取消保持Cancelled；失败日志wave2-s14-runtime-regression保留。修后wave2-s14-runtime-conservative-regression为48通过/2真实S3环境ignored。最终九门禁audited-final进行中。

S14 implemented / locally verified：wave2-s14-attempt-audited-final 九项全部exit0，526Rust/36Web/48真实daemon、10外部环境项明确ignored。12项attempt存储合同/131 runtime单测/3真实HTTP接受后断连取消超时全部GREEN。独立TLS MinIO四项业务与PostgreSQL发布协调六项全部exit0；PG runner五步骤不等于五业务合同。首次依赖清单/Clippy遗漏及两项旧取消断言失败保留，未放宽架构护栏、未加栈/跳过测试。schema25，不新增第三方包，未提交推送部署；完整Wave2–10未完成，enterprise_ready=false。继续原工具事务async/固定owner，后续完整PG/租户键/nativewriter/auditquota/offline仍必须闭合。
