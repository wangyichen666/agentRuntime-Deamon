# Wave2-S10：canonical 事件的异步隔离读回

日期：2026-10-08。状态：implemented / locally verified。完整 Wave2–10 尚未完成，enterprise_ready=false。

## 实施范围与基线

切片前 run.events 和订阅读回使用原同步 RunRepository::events_after，原 SQL 仅 WHERE run_id，错误统一投影 Internal。切片前原实现对行数限制 1000，但没有聚合字节预算，也未返回明确的分页游标/has_more。事件、event_view_stamps 与 transactional outbox 已是唯一事实，不能增加第二事件账本。

本切片在 core 定义纯 RunEventPage，在 storage-ports 定义有真实 daemon caller 的 RunEventRepository，SQLite 适配使用原四 permit blocking helper和原 Connection。数据库内按 organization/tenant/project/agent 与 run/session/lifetime 归属核验，再在同一只读事务读取原事件与原封存 stamp。原 payload/stamp 解码应抽取共用 helper，不能复制状态机或重写历史。

事件 cursor 继续使用原 EventSeq；响应同时限制行数与序列化字节，返回 cursor/has_more。单条合法但超出传输预算的事件 fail closed，不能静默截断或丢弃。scope 来自可信服务端上下文，客户端新增 tenant 字段不能授予权限。不存在与越域统一 Forbidden；驱动/预算不可用维持 StorageUnavailable；损坏 stamp/owner 拒绝读取。

历史读回不取得 writer lease、不要求当前活动 lifetime：永久 lifetime 归属用于核验已退休 run 的历史，而当前 mutation 的 writer fence 保持原事务行为。查询不写 snapshot_clock、事件、outbox、授权或租约。

## 验收与兼容计划

两项真实 RED 已保留在 evidence/wave2-s10-events-red.log：原同步路径返回越域私密事件，四条有限行生成 1,436,235 字节响应。六项存储合同和六入口真实 daemon 合同已通过；覆盖完整 scope 四维、不存在、稳定 cursor、不重不漏、空页、过大单条、损坏 stamp、重启和零写。补共享预算/取消合同与真实 daemon 的事件读回、订阅恢复及适用入口合同。

修改 core/storage-ports/storage/daemon/合同和文档；schema 保持 v25，不新增依赖。旧 events 字段与 EventSeq/_my_agent_view 保留，分页元数据增补在原返回中；旧同步事务维护调用者仍使用原事件解码。订阅遇到未全部读完的页必须返回原 resync_required，不能在缺历史时伪装恢复成功。

实施后完整九门禁、真实 TLS MinIO 四项及 PG 发布六项均需独立新证据。当前局部合同已通过，不能替代待执行的完整门禁。S10 不是全部 native async ports 或全业务 PG；后续全业务复合租户键、PG 原事务/lease、audit/quota、离线迁移仍待实施。

回滚仍遵循 schema-v25 和 OutcomeUnknown 兼容窗口；未提交、推送或部署。

恢复夹具最初误用 request.subscribe，原方法为 agent.subscribe；这两次夹具调用失败保留 resync/resync-fixed 日志，不计功能 RED。恢复完整性按原 finish 返回的 canonical last_seq 校验，不凭手写事件数量猜测原 terminal/assistant_content 事务。

实现锁序和历史归属决策见 ADR 0005 S10。run.events / agent.subscribe await 同一个 daemon event_store；CLI run-events、ACP1 /events、ACP2 _my_agent/run/events（readback capability）、HTTP POST /api/runs/events、WS 原 run.events 都已接线。HTTP/ACP2 拒绝未知 tenant 字段；越域/不存在同 code/data、HTTP403、读取不创建新 session/run。六入口和重启 Provider 只执行原一次；恢复合同 Provider 零调用，clock 零写。原订阅方法修正后合同通过。

首次完整九门禁的 tests 发现真实 TUI 回归：新增 /events 插在 registry 首位，原 / → next → Tab 从 /status 变成 /help。需把新增项移到既有命令后，保留原菜单顺序和原断言；该失败属于实施问题，不能计通过。首轮 evidence/wave2-s10-events-final-* 保留，最终必须新前缀整九门禁。

复核门禁发现原订阅单测固定了未发送 1000 条历史就推进 cursor=1000 的旧行为。新真实进程合同已证明 cursor 必须为最后交付位置（初始为0）。将该原单测升级为从0完整翻页、验证1001条原事件逐条连续/全部交付；不是放宽断言或跳过旧回归。复核失败日志保留，最终需再新前缀整九门禁。

最终九门禁 evidence/wave2-s10-events-cursor-final-gates.json 全部 exit0：480 Rust、36 Web、42 真实 daemon，10 项真实外部环境测试显式 ignored。独立 TLS MinIO 四项与 PG 发布六项正在执行，未用 ignored 充当通过。两轮失败证据全部保留，原 TUI 顺序不变，原订阅回归加强到1001条逐条完整交付。

真实 TLS MinIO 四项回归已通过，证据 evidence/wave2-s10-events-regression-s3.json（仅创建/移除自己的隔离容器）。真实 PG 发布六项正在执行，尚未计完整本地验收。

兼容限制：原无 stamp 历史继续 legacy fallback，不宣称已实现不可删除 native/legacy provenance 或全部事件防篡改。该项已记录 findings，需后续真实 RED/原表迁移；本切片不凭时间戳猜新旧历史。

最终独立外部环境回归全部通过：[真实 TLS MinIO 四项](evidence/wave2-s10-events-regression-s3.json)、[真实 PG 发布六项](evidence/wave2-s10-events-regression-postgres.json)。全部九门禁证据：[cursor-final-gates.json](evidence/wave2-s10-events-cursor-final-gates.json)。implemented / locally verified，不声称 CI/production verified；未提交、推送或部署，完整 Wave2–10 未完成。
