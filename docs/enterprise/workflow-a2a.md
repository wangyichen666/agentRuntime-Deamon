# Wave5 工作流与 A2A

真实工作流通过 daemon `workflow.publish/start/read/control/message`、CLI `workflow --action … --request FILE` 与 HTTP `/enterprise/workflows`、`/read`、`/control` 操作。发布需要 AgentConfigure，启动需要 SessionPrompt，控制需要 SessionManage，读回需要 AgentRead；最终身份、epoch、策略都在 canonical 事务内检查。operation_id 按完整scope与actor保存，不跨主体复用。

工作流定义有9类节点。AgentRun与ToolCall执行原native run；Approval使用原interaction与ExactOwner/revision审批，不因客户端离开而回答。Condition路径相对 `{workflow,node,dependencies}` 输入对象；假分支跳过，Join容纳跳过的分支。WaitUntil使用事务后数据库时钟轮询，不阻塞线程sleep。SubWorkflow在原事务创建并关联，嵌套最多8层。纯规则同源，SQLite36与PG10适配器只有事实读写。

Local自动使用保留主体local-workflow-workload。企业域默认不开工作流调度；启动前注册同域独立workload、绑定必要角色，并设置受保护0600绝对JSON路径 `AGENT_WORKFLOW_IDENTITY`。文件形如原RuntimeIdentity的scope/principal结构。这个身份用于领取/续租/结果CAS，不借用Gateway账号；原任务creator仍按当前epoch和原run冻结授权执行。每daemon最多32节点执行体，每次最多8个领取。lease epoch为数据库事实，旧executor结果被拒绝。

发布的输入digest不可变，节点开始执行时在原run准入TX记录native_run_id。未知Provider/tool/网络结果禁止自动重放。HTTPS Webhook通过原工具receipt派发，拒代理、重定向、私网/linklocal/metadata地址，固定DNS结果并关闭HTTP自动retry；HTTP非2xx或响应中断不能推断无副作用。已确认完成节点的显式补偿按反向拓扑顺序创建新occurrence和原native run，不改原收据。取消先传播到活动原child；unknown保持unknown。

A2A使用[0.3开放规格](https://a2a-protocol.org/v0.3.0/specification/)，入口为 `/a2a`，支持message/send、message/stream、tasks/get、tasks/cancel。Agent Card位于 `/.well-known/agent-card.json`。企业域设置 `AGENT_A2A_PUBLIC_URL=https://可信公开host/a2a`；Local固定loopback端口自动生成URL。URL不取自HTTP Host/X-Forwarded头，公开URL不能带凭据、query或fragment。多个profile继续使用受保护scope配置和X-Agent-Scope。

首次messageId生成原持久workflow，重复相同消息读取同task_id；不同内容复用messageId冲突。只支持role=user和text/plain，最多32parts、总32KiB；不支持媒体、系统角色、push通知、跨任务引用或客户端配置覆盖。返回的Task只投影公开内容、revision、状态历史与artifact摘要；不返回session文件名、snapshot或凭据。SSE使用原持久状态游标，网络断开不取消任务；重启后tasks/get读回相同事实。人工回答当前使用原interaction exact owner/revision入口，A2A不隐式推断“同意”。

当前局部合同证明native原子关联/lease fencing/重启读回；首次PG失败已修后实库通过。最新补偿、人工恢复、全部内部operation幂等、unknown节点reconcile和完整并发/fault matrix仍在开发或待验收，不能据此宣称完整Wave5或企业发布通过。
