//! Black-box daemon contract: a process restart must preserve committed facts.
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UnixStream};
use tokio::process::{Child, Command};
use tokio_tungstenite::tungstenite::Message as WsMessage;

static NEXT: AtomicU64 = AtomicU64::new(0);
struct AcpStdioContract {
    process: Child,
    input: tokio::process::ChildStdin,
    output: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
}
impl AcpStdioContract {
    async fn start(workspace: &Path, runtime: &Path, url: &str, v2: bool) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_my-agent"));
        command.arg("--workspace").arg(workspace).arg("editor");
        if v2 {
            command.arg("--acp-v2");
        }
        let mut process = command
            .env("MY_AGENT_RUNTIME_DIR", runtime)
            .env("MY_AGENT_CONFIG", workspace.join("empty-config.json"))
            .env("API_TYPE", "ollama")
            .env("MODEL_NAME", "mock")
            .env("OPENAI_BASE_URL", url)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut stderr = process.stderr.take().unwrap();
        tokio::spawn(async move {
            let _ = tokio::io::copy(&mut stderr, &mut tokio::io::sink()).await;
        });
        let input = process.stdin.take().unwrap();
        let output = BufReader::new(process.stdout.take().unwrap()).lines();
        Self {
            process,
            input,
            output,
        }
    }
    async fn send(&mut self, frame: Value) {
        self.input
            .write_all(format!("{frame}\n").as_bytes())
            .await
            .unwrap();
    }
    async fn next(&mut self) -> Value {
        serde_json::from_str(
            &self
                .output
                .next_line()
                .await
                .unwrap()
                .expect("ACP 连接提前终止"),
        )
        .unwrap()
    }
    async fn query(&mut self, id: u64, method: &str, params: Value) -> (Value, Vec<Value>) {
        self.send(json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
            .await;
        let mut updates = Vec::new();
        loop {
            let frame = self.next().await;
            if frame["id"] == id && frame.get("method").is_none() {
                return (frame, updates);
            }
            assert!(updates.len() < 512);
            updates.push(frame);
        }
    }
    async fn initialize(&mut self, capabilities: Vec<&str>) -> Value {
        self.query(1,"initialize",json!({"protocolVersion":2,"info":{"name":"contract","version":"1"},"_meta":{"my-agent":{"schema_version":1,"fingerprint":agent_daemon_protocol::acp::ACP_V2_FINGERPRINT,"capabilities":capabilities}}})).await.0
    }
    async fn stop(mut self) {
        self.process.kill().await.unwrap();
        self.process.wait().await.unwrap();
    }
}
fn acp_control(session: &str, life: &Value, extra: Value) -> Value {
    let mut control = json!({"schema_version":1,"fingerprint":agent_daemon_protocol::acp::ACP_V2_FINGERPRINT,"expected_lifetime":life});
    for (key, value) in extra.as_object().unwrap() {
        control[key] = value.clone();
    }
    json!({"sessionId":session,"_meta":{"my-agent":control}})
}

#[tokio::test]
async fn acp_v1_permission_rejects_v2_identity_before_approval_and_v2_can_recover() {
    tokio::time::timeout(Duration::from_secs(45), async {
        let workspace=temp_workspace();let runtime=PathBuf::from(format!("/tmp/ma-acp-v1-ir-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
        let (url,provider,captures)=mock_tool_response_ollama("{\"message\":{\"role\":\"assistant\",\"content\":\"\",\"tool_calls\":[{\"function\":{\"name\":\"write_file\",\"arguments\":{\"path\":\"v1-approved.txt\",\"content\":\"严格批准\"}}}]},\"done\":false}\n{\"done\":true}\n").await;
        let (mut process,socket)=daemon(&workspace,&runtime,&url).await;
        rpc(&socket,"mode","permissions.set",json!({"mode":"request_approval"})).await;
        let key="session-acp-v1-ir.jsonl";let created=rpc(&socket,"create","sessions.create",json!({"session_id":key})).await;let life=created["result"]["session_lifetime_id"].clone();
        let mut v1=AcpStdioContract::start(&workspace,&runtime,&url,false).await;
        assert!(v1.query(1,"initialize",json!({"protocolVersion":1})).await.0.get("error").is_none());
        v1.send(json!({"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":key,"prompt":[{"type":"text","text":"修改文件"}]}})).await;
        let permission=loop{let frame=v1.next().await;if frame["method"]=="session/request_permission"{break frame;}};
        let before=rpc(&socket,"before","sessions.read",json!({"session_id":key})).await;
        v1.send(json!({"jsonrpc":"2.0","id":permission["id"],"result":{"outcome":{"outcome":"selected","optionId":"allow_once"},"_meta":{"my-agent":{"schema_version":1,"owner":{"run_generation":999}}}}})).await;
        let refused=loop{let frame=v1.next().await;if frame["id"]==2 && frame.get("method").is_none(){break frame;}};
        assert!(refused.get("error").is_some(),"v1 不得吞 v2 permission 身份：{refused}");
        assert_eq!(rpc(&socket,"after","sessions.read",json!({"session_id":key})).await["result"],before["result"]);
        assert_eq!(captures.lock().await.len(),1);assert!(!workspace.join("v1-approved.txt").exists());v1.stop().await;
        let mut v2=AcpStdioContract::start(&workspace,&runtime,&url,true).await;assert!(v2.initialize(vec!["interaction","readback"]).await.get("error").is_none());
        let mut resume=acp_control(key,&life,json!({}));resume["cwd"]=json!(workspace);resume["mcpServers"]=json!([]);
        let (resumed,mut frames)=v2.query(2,"session/resume",resume).await;assert!(resumed.get("error").is_none());
        let permission=loop{if let Some(index)=frames.iter().position(|frame|frame["method"]=="session/request_permission"){break frames.remove(index);}frames.push(v2.next().await);};
        v2.send(json!({"jsonrpc":"2.0","id":permission["id"],"result":{"outcome":{"outcome":"selected","optionId":"allow_once"}}})).await;
        loop{let frame=v2.next().await;if frame["params"]["update"]["sessionUpdate"]=="state_update" && frame["params"]["update"]["state"]=="idle"{break;}}
        assert_eq!(std::fs::read_to_string(workspace.join("v1-approved.txt")).unwrap(),"严格批准");assert_eq!(captures.lock().await.len(),2);
        v2.stop().await;process.kill().await.unwrap();process.wait().await.unwrap();provider.abort();std::fs::remove_dir_all(workspace).unwrap();std::fs::remove_dir_all(runtime).unwrap();
    }).await.expect("v1 permission 严格身份/跨版本恢复合同超时");
}

#[tokio::test]
async fn acp_slow_stdio_detaches_without_settling_native_run() {
    tokio::time::timeout(Duration::from_secs(45), async {
        let workspace = temp_workspace();
        let runtime = PathBuf::from(format!("/tmp/ma-acp-slow-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
        let (url,provider,count) = mock_delegation_ollama(vec![]).await;
        let (mut daemon_process,socket) = daemon(&workspace,&runtime,&url).await;
        let key = "session-acp-slow.jsonl";
        let created = rpc(&socket,"create","sessions.create",json!({"session_id":key})).await;
        let life = created["result"]["session_lifetime_id"].clone();
        let mut acp = AcpStdioContract::start(&workspace,&runtime,&url,true).await;
        assert!(acp.initialize(vec!["readback"]).await.get("error").is_none());
        let mut prompt = acp_control(key,&life,json!({}));
        prompt["prompt"] = json!([{"type":"text","text":"保持后台运行"}]);
        let accepted = acp.query(2,"session/prompt",prompt).await.0;
        assert!(accepted.get("error").is_none(),"{accepted}");
        let owner = accepted["result"]["_meta"]["my-agent"]["owner"].clone();
        while count.load(Ordering::SeqCst) == 0 { tokio::task::yield_now().await; }
        let before = rpc(&socket,"before","sessions.read",json!({"session_id":key})).await;
        // 此后不消费 stdout。大字段错误填满物理 pipe；持续输入触发有界预算断开。
        let large = "unknown".repeat(16_384);
        let mut params = acp_control(key,&life,json!({}));
        params[large] = json!(true);
        for id in 3..403 {
            let frame = json!({"jsonrpc":"2.0","id":id,"method":"_my_agent/session/read","params":params});
            if acp.input.write_all(format!("{frame}\n").as_bytes()).await.is_err() { break; }
        }
        drop(acp.input);
        assert!(!acp.process.wait().await.unwrap().success(),"预算溢出必须 detach");
        let after = rpc(&socket,"after","sessions.read",json!({"session_id":key})).await;
        assert_eq!(after["result"],before["result"],"transport detach 不得写 canonical 状态");
        assert_eq!(rpc(&socket,"native","run.read",json!({"run_id":owner["run_id"]})).await["result"]["status"],"running");
        assert_eq!(count.load(Ordering::SeqCst),1);
        assert_eq!(rpc(&socket,"cancel","agent.cancel",json!({"session_id":key,"expected_lifetime":life,"run_id":owner["run_id"],"exact_owner":owner})).await["result"]["cancelled"],true);
        daemon_process.kill().await.unwrap();daemon_process.wait().await.unwrap();provider.abort();
        std::fs::remove_dir_all(workspace).unwrap();std::fs::remove_dir_all(runtime).unwrap();
    }).await.expect("ACP 慢 stdio 消费者合同超时");
}

#[tokio::test]
async fn acp_v2_long_prompt_and_native_compact_allow_exact_control_without_private_terminal() {
    tokio::time::timeout(Duration::from_secs(45),async{
        let workspace=temp_workspace();let runtime=PathBuf::from(format!("/tmp/ma-acp-control-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
        let (url,provider,count)=mock_delegation_ollama(vec![1,2,3,4,7]).await;let(mut process,socket)=daemon(&workspace,&runtime,&url).await;
        let key="session-acp-compact.jsonl";let created=rpc(&socket,"create","sessions.create",json!({"session_id":key})).await;let life=&created["result"]["session_lifetime_id"];
        for index in 0..4{let (_,response)=chat(&socket,&format!("seed-{index}"),key,&"constraint-".repeat(1000),true).await;assert!(response.unwrap().get("error").is_none());}
        let source=rpc(&socket,"source","sessions.read",json!({"session_id":key})).await;
        let mut acp=AcpStdioContract::start(&workspace,&runtime,&url,true).await;assert_eq!(acp.initialize(vec!["compact","readback","management"]).await["result"]["protocolVersion"],2);
        let command=acp_control(key,life,json!({"operation_id":"acp-compact","expected_revision":source["result"]["transcript_revision"],"expected_projection_generation":source["result"]["projection_generation"]}));
        let (started,updates)=acp.query(2,"_my_agent/compact/start",command.clone()).await;assert!(started.get("error").is_none(),"{started}");let native=started["result"]["run_id"].as_str().unwrap().to_owned();let owner=started["result"]["compact"]["owner"].clone();
        assert_eq!(started["result"]["status"],"running");assert!(updates.iter().any(|frame|frame["params"]["update"]["sessionUpdate"]=="compaction_update" && frame["params"]["update"]["status"]=="in_progress" && frame["params"]["update"]["compactionId"]==native),"Started 必须先于被阻塞摘要：{updates:?}");
        while count.load(Ordering::SeqCst)<5 {tokio::task::yield_now().await;}
        let same=acp.query(3,"_my_agent/compact/start",command).await.0;assert_eq!(same["result"]["run_id"],native);
        let observed=acp.query(4,"_my_agent/session/read",acp_control(key,life,json!({}))).await.0;let canonical=rpc(&socket,"same","sessions.read",json!({"session_id":key})).await;assert_eq!(observed["result"],json!(agent_daemon_protocol::decode_session_readback(canonical["result"].clone()).unwrap()));
        let mut wrong=owner.clone();wrong["run_generation"]=json!(999);assert!(acp.query(5,"_my_agent/run/cancel",acp_control(key,life,json!({"owner":wrong}))).await.0.get("error").is_some());assert_eq!(rpc(&socket,"still-running","run.read",json!({"run_id":native})).await["result"]["status"],"running");
        assert_eq!(acp.query(6,"_my_agent/run/cancel",acp_control(key,life,json!({"owner":owner}))).await.0["result"]["cancelled"],true);
        loop{let frame=acp.next().await;if frame["params"]["update"]["compactionId"]==native && frame["params"]["update"]["status"]=="cancelled"{break;}}
        let read=rpc(&socket,"cancelled","run.read",json!({"run_id":native})).await;assert_eq!(read["result"]["status"],"cancelled");let cancelled_compact=read["result"]["compact"].clone();
        let mut prompt=acp_control(key,life,json!({}));prompt["prompt"]=json!([{"type":"text","text":"启动长任务"}]);let (accepted,prior)=acp.query(7,"session/prompt",prompt).await;assert!(accepted.get("error").is_none(),"{accepted}");let chat_owner=accepted["result"]["_meta"]["my-agent"]["owner"].clone();assert!(chat_owner["run_id"].as_str().is_some());assert!(!prior.iter().any(|frame|frame["params"]["update"]["state"]=="idle"));
        let read=acp.query(8,"_my_agent/session/read",acp_control(key,life,json!({}))).await.0;assert!(read.get("error").is_none(),"长 prompt 不得阻塞 read：{read}");assert!(read["result"]["active_runs"].as_array().unwrap().iter().any(|run|run["run_id"]==chat_owner["run_id"]));let before_close_revision=read["result"]["transcript_revision"].clone();
        while count.load(Ordering::SeqCst)<6 {tokio::task::yield_now().await;}
        // 标准 close 必须取消 exact 工作、等待真实结算，并保留同一 lifetime/transcript。
        let closed=acp.query(9,"session/close",acp_control(key,life,json!({"operation_id":"close-active"}))).await.0;assert!(closed.get("error").is_none(),"{closed}");
        let after=rpc(&socket,"after-close","sessions.read",json!({"session_id":key})).await;assert_eq!(after["result"]["session_lifetime_id"],*life);assert_eq!(after["result"]["transcript_revision"],before_close_revision);assert_eq!(rpc(&socket,"chat-cancelled","run.read",json!({"run_id":chat_owner["run_id"]})).await["result"]["status"],"cancelled");
        assert!(acp.query(10,"session/close",acp_control(key,life,json!({"operation_id":"close-active"}))).await.0.get("error").is_none());
        assert!(acp.query(11,"session/close",acp_control(key,&json!("foreign-life"),json!({"operation_id":"close-active"}))).await.0.get("error").is_some());
        let count_before=count.load(Ordering::SeqCst);acp.stop().await;process.kill().await.unwrap();process.wait().await.unwrap();let(mut restarted,socket)=daemon(&workspace,&runtime,&url).await;
        assert_eq!(rpc(&socket,"restart","run.read",json!({"run_id":native})).await["result"]["compact"],cancelled_compact);assert_eq!(count.load(Ordering::SeqCst),count_before);
        restarted.kill().await.unwrap();restarted.wait().await.unwrap();provider.abort();std::fs::remove_dir_all(workspace).unwrap();std::fs::remove_dir_all(runtime).unwrap();
    }).await.expect("ACP 长 prompt/compact/control 合同超时");
}

#[tokio::test]
async fn acp_v2_plan_controls_and_v1_reject_metadata_share_canonical_decisions() {
    tokio::time::timeout(Duration::from_secs(45),async{
        let workspace=temp_workspace();let runtime=PathBuf::from(format!("/tmp/ma-acp-plan-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
        let (url,provider,captures)=mock_tool_catalog_ollama(true).await;let(mut process,socket)=daemon(&workspace,&runtime,&url).await;let key="session-acp-plan.jsonl";
        let created=rpc(&socket,"create","sessions.create",json!({"session_id":key})).await;let life=created["result"]["session_lifetime_id"].clone();let(_,response)=chat(&socket,"author",key,"仅制定计划",true).await;assert!(response.unwrap().get("error").is_none());
        let read=rpc(&socket,"plan","sessions.plan.readback",json!({"session_id":key})).await;let plan=&read["result"]["plan"];let identity=json!({"plan_id":plan["plan_id"],"revision":plan["revision"],"content_digest":plan["content_digest"],"operation_id":"acp-plan-execute"});let count=captures.lock().await.len();
        let mut v1=AcpStdioContract::start(&workspace,&runtime,&url,false).await;assert!(v1.query(1,"initialize",json!({"protocolVersion":1})).await.0["result"].get("capabilities").is_none());
        let mut prompt=acp_control(key,&life,json!({"plan_execution":identity}));prompt["prompt"]=json!([{"type":"text","text":"执行"}]);assert!(v1.query(2,"session/prompt",prompt).await.0.get("error").is_some());assert_eq!(captures.lock().await.len(),count);v1.stop().await;
        let mut acp=AcpStdioContract::start(&workspace,&runtime,&url,true).await;assert!(acp.initialize(vec!["plan","readback"]).await.get("error").is_none());
        let (projected,updates)=acp.query(2,"_my_agent/plan/read",acp_control(key,&life,json!({}))).await;assert_eq!(projected["result"],read["result"]);assert!(updates.iter().any(|frame|frame["params"]["update"]["sessionUpdate"]=="plan_update" && frame["params"]["update"]["plan"]["type"]=="markdown" && frame["params"]["update"]["plan"]["content"]==read["result"]["markdown"]));
        let mut stale=identity.clone();stale["content_digest"]=json!("stale");assert!(acp.query(3,"_my_agent/plan/execute",acp_control(key,&life,json!({"plan_execution":stale}))).await.0.get("error").is_some());assert_eq!(captures.lock().await.len(),count);
        let executed=acp.query(4,"_my_agent/plan/execute",acp_control(key,&life,json!({"plan_execution":identity}))).await.0;assert!(executed.get("error").is_none(),"{executed}");let owner=executed["result"]["_meta"]["my-agent"]["owner"].clone();
        loop{let frame=acp.next().await;if frame["params"]["update"]["sessionUpdate"]=="state_update" && frame["params"]["update"]["state"]=="idle"{break;}}
        let original=format!("执行已确认计划 {} 版本 {} 摘要 {}。",plan["plan_id"].as_str().unwrap(),plan["revision"],plan["content_digest"].as_str().unwrap());let same=rpc(&socket,"cli-command","chat.send",json!({"session_id":key,"expected_lifetime":life,"message":original,"admission_mode":"reject_if_busy","plan_execution":identity})).await;assert_eq!(same["result"]["run_id"],owner["run_id"]);
        let mut discard=identity;discard["operation_id"]=json!("acp-discard");let params=acp_control(key,&life,json!({"plan_execution":discard}));let receipt=acp.query(5,"_my_agent/plan/discard",params.clone()).await.0;assert!(receipt.get("error").is_none(),"{receipt}");assert_eq!(receipt["result"],acp.query(6,"_my_agent/plan/discard",params).await.0["result"]);
        let read=acp.query(7,"_my_agent/plan/read",acp_control(key,&life,json!({}))).await.0;assert_eq!(read["result"]["plan"]["review"],"rejected");assert_eq!(read["result"],rpc(&socket,"shared","sessions.plan.readback",json!({"session_id":key})).await["result"]);
        entry_views(&workspace,&runtime,&url,owner["run_id"].as_str().unwrap(),"完成",None).await;
        acp.stop().await;process.kill().await.unwrap();process.wait().await.unwrap();provider.abort();std::fs::remove_dir_all(workspace).unwrap();std::fs::remove_dir_all(runtime).unwrap();
    }).await.expect("ACP plan/v1 metadata 合同超时");
}

#[tokio::test]
async fn acp_v2_pending_interaction_reconnect_does_not_cancel_or_replay_provider() {
    tokio::time::timeout(Duration::from_secs(45),async{
        let workspace=temp_workspace();let runtime=PathBuf::from(format!("/tmp/ma-acp-ir-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
        let (url,provider,captures)=mock_tool_response_ollama("{\"message\":{\"role\":\"assistant\",\"content\":\"\",\"tool_calls\":[{\"function\":{\"name\":\"write_file\",\"arguments\":{\"path\":\"approved.txt\",\"content\":\"已批准\"}}}]},\"done\":false}\n{\"done\":true}\n").await;
        let(mut process,socket)=daemon(&workspace,&runtime,&url).await;rpc(&socket,"policy","permissions.set",json!({"mode":"request_approval"})).await;
        let key="session-acp-ir.jsonl";let created=rpc(&socket,"create","sessions.create",json!({"session_id":key})).await;let life=created["result"]["session_lifetime_id"].clone();
        let mut first=AcpStdioContract::start(&workspace,&runtime,&url,true).await;assert!(first.initialize(vec!["interaction","readback"]).await.get("error").is_none());
        let mut prompt=acp_control(key,&life,json!({}));prompt["prompt"]=json!([{"type":"text","text":"修改文件"}]);let accepted=first.query(2,"session/prompt",prompt).await.0;assert!(accepted.get("error").is_none(),"{accepted}");let owner=accepted["result"]["_meta"]["my-agent"]["owner"].clone();
        let pending=loop{let frame=first.next().await;if frame["method"]=="session/request_permission"{break frame;}};let interaction=pending["params"]["_meta"]["my-agent"]["interaction"].clone();
        first.send(json!({"jsonrpc":"2.0","id":pending["id"],"result":{"outcome":{"outcome":"cancelled"}}})).await;
        let still_pending=first.query(3,"_my_agent/session/read",acp_control(key,&life,json!({}))).await.0;assert_eq!(still_pending["result"]["pending_interactions"][0],interaction,"popup 关闭不得伪造拒绝");
        first.stop().await;assert_eq!(captures.lock().await.len(),1);
        let snapshot=rpc(&socket,"detached","sessions.read",json!({"session_id":key})).await;assert_eq!(snapshot["result"]["pending_interactions"][0],interaction);assert_eq!(snapshot["result"]["active_runs"][0]["status"],"waiting_interaction");assert!(!workspace.join("approved.txt").exists());
        let mut second=AcpStdioContract::start(&workspace,&runtime,&url,true).await;assert!(second.initialize(vec!["interaction","readback"]).await.get("error").is_none());
        let mut resume=acp_control(key,&life,json!({}));resume["cwd"]=json!(workspace);resume["mcpServers"]=json!([]);let(resumed,mut updates)=second.query(2,"session/resume",resume).await;assert!(resumed.get("error").is_none(),"{resumed}");
        let pending=loop{if let Some(index)=updates.iter().position(|frame|frame["method"]=="session/request_permission"){break updates.remove(index);}updates.push(second.next().await);};assert_eq!(pending["params"]["_meta"]["my-agent"]["interaction"],interaction);assert_eq!(pending["params"]["_meta"]["my-agent"]["owner"],owner);
        // 同连接仍可读与显式严格审批；尚未返回的 permission request 不阻塞 dispatch。
        assert!(second.query(3,"_my_agent/session/read",acp_control(key,&life,json!({}))).await.0.get("error").is_none());
        let result=second.query(4,"_my_agent/interaction/respond",acp_control(key,&life,json!({"owner":owner,"interaction_id":interaction["interaction_id"],"interaction_revision":interaction["revision"],"approved":true}))).await.0;assert!(result.get("error").is_none(),"{result}");
        second.send(json!({"jsonrpc":"2.0","id":pending["id"],"result":{"outcome":{"outcome":"selected","optionId":"allow_once"}}})).await;
        loop{let frame=second.next().await;if frame["params"]["update"]["sessionUpdate"]=="state_update" && frame["params"]["update"]["state"]=="idle"{break;}}
        assert_eq!(captures.lock().await.len(),2,"恢复只续实际未结算调用，不重放已发送 Provider 请求");assert_eq!(std::fs::read_to_string(workspace.join("approved.txt")).unwrap(),"已批准");let run=owner["run_id"].as_str().unwrap();assert_eq!(rpc(&socket,"terminal","run.read",json!({"run_id":run})).await["result"]["status"],"completed");entry_views(&workspace,&runtime,&url,run,"完成",None).await;
        second.stop().await;process.kill().await.unwrap();process.wait().await.unwrap();provider.abort();std::fs::remove_dir_all(workspace).unwrap();std::fs::remove_dir_all(runtime).unwrap();
    }).await.expect("ACP pending interaction 断线恢复合同超时");
}

#[tokio::test]
async fn acp_v2_stdio_negotiates_strict_connection_before_shared_readback() {
    tokio::time::timeout(Duration::from_secs(45), async {
        let workspace=temp_workspace();let runtime=PathBuf::from(format!("/tmp/ma-acp-v2-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
        let (url,provider,count)=mock_ollama().await;let (mut daemon_process,socket)=daemon(&workspace,&runtime,&url).await;
        let mut acp=Command::new(env!("CARGO_BIN_EXE_my-agent")).arg("--workspace").arg(&workspace).arg("editor").arg("--acp-v2")
            .env("MY_AGENT_RUNTIME_DIR",&runtime).env("MY_AGENT_CONFIG",workspace.join("empty-config.json")).env("API_TYPE","ollama").env("MODEL_NAME","mock").env("OPENAI_BASE_URL",&url)
            .stdin(std::process::Stdio::piped()).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
        let mut input=acp.stdin.take().unwrap();let mut output=BufReader::new(acp.stdout.take().unwrap()).lines();
        input.write_all(format!("{}\n",json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":2,"info":{"name":"contract","version":"1"}}})).as_bytes()).await.unwrap();
        let init:Value=serde_json::from_str(&output.next_line().await.unwrap().expect("opt-in v2 应启动真实 SDK stdio server")).unwrap();
        assert_eq!(init["result"]["protocolVersion"],2,"{init}");assert!(init["result"]["capabilities"]["session"].is_object());
        let before=rpc(&socket,"before","sessions.list",json!({})).await;
        input.write_all(format!("{}\n",json!({"jsonrpc":"2.0","id":2,"method":"initialize","params":{"protocolVersion":2,"info":{"name":"contract","version":"1"}}})).as_bytes()).await.unwrap();
        let repeated:Value=serde_json::from_str(&output.next_line().await.unwrap().unwrap()).unwrap();assert!(repeated.get("error").is_some(),"重复/热协商必须拒绝：{repeated}");
        assert_eq!(before["result"],rpc(&socket,"after","sessions.list",json!({})).await["result"]);assert_eq!(count.load(Ordering::SeqCst),0);
        acp.kill().await.unwrap();acp.wait().await.unwrap();daemon_process.kill().await.unwrap();daemon_process.wait().await.unwrap();provider.abort();std::fs::remove_dir_all(workspace).unwrap();std::fs::remove_dir_all(runtime).unwrap();
    }).await.expect("ACP v2 stdio 协商合同超时");
}

#[tokio::test]
async fn provider_context_readback_rebuilds_captured_request_without_replay_or_writes() {
    tokio::time::timeout(Duration::from_secs(45),async {
        let workspace=temp_workspace();let runtime=PathBuf::from(format!("/tmp/ma-envelope-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
        let done="{\"message\":{\"role\":\"assistant\",\"content\":\"完成\"},\"done\":true}\n";
        let (url,provider,captures)=mock_response_sequence_ollama(vec![done],done).await;
        let (mut process,socket)=daemon(&workspace,&runtime,&url).await;let key="session-envelope.jsonl";
        let created=rpc(&socket,"new","sessions.create",json!({"session_id":key})).await;
        let (run,response)=chat(&socket,"envelope-chat",key,"核对请求 Bearer secret-demo-for-test",true).await;assert!(response.unwrap().get("error").is_none());
        let params=json!({"session_id":key,"expected_lifetime":created["result"]["session_lifetime_id"],"run_id":run});
        let read=rpc(&socket,"read","context.readback",params.clone()).await;assert!(read.get("error").is_none(),"{read}");
        let envelope=&read["result"];assert_eq!(envelope["schema_version"],1);assert_eq!(envelope["owner"]["run_id"],run);assert_eq!(envelope["replayability"],"captured");assert!(envelope.get("messages").is_none());assert!(envelope.get("tools").is_none());assert!(!envelope.to_string().contains("secret-demo-for-test"));
        let local=rpc(&socket,"local","context.readback",json!({"local_diagnostics":true,"session_id":key,"expected_lifetime":created["result"]["session_lifetime_id"],"run_id":run})).await;
        assert!(local.get("error").is_none(),"{local}");assert!(!local.to_string().contains("secret-demo-for-test"));assert!(local["result"]["messages"].as_array().unwrap().len()>2);
        assert_eq!(local["result"]["request_digest"],envelope["request_digest"]);
        let sent=captures.lock().await;assert_eq!(sent.len(),1);let captured=&sent[0];
        let sent_messages:Vec<agent_core::Message>=serde_json::from_value(captured["messages"].clone()).unwrap();
        assert_eq!(agent_context::digest(&sent_messages).unwrap(),envelope["message_digest"].as_str().unwrap(),"实际 Provider 收到的消息摘要与重建一致");
        let sent_tools:Vec<agent_core::ToolSpec>=captured["tools"].as_array().unwrap().iter().map(|tool|serde_json::from_value(tool["function"].clone()).unwrap()).collect();
        assert_eq!(agent_context::value_digest(&serde_json::to_value(sent_tools).unwrap()).unwrap(),envelope["provider_tools_digest"].as_str().unwrap());
        assert!(captured["messages"].as_array().unwrap().iter().any(|m|m["content"].as_str().is_some_and(|s|s.contains("secret-demo-for-test"))));drop(sent);
        let db=rusqlite::Connection::open_with_flags(workspace.join(".my-agent/runtime.sqlite3"),rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let clock:u64=db.query_row("SELECT revision FROM snapshot_clock WHERE id=1",[],|row|row.get(0)).unwrap();
        let before:u64=db.query_row("SELECT count(*) FROM events",[],|row|row.get(0)).unwrap();
        let again=rpc(&socket,"again","context.readback",params.clone()).await;
        assert_eq!(clock,db.query_row::<u64,_,_>("SELECT revision FROM snapshot_clock WHERE id=1",[],|row|row.get(0)).unwrap());
        assert_eq!(before,db.query_row::<u64,_,_>("SELECT count(*) FROM events",[],|row|row.get(0)).unwrap());assert_eq!(again["result"],read["result"]);assert_eq!(captures.lock().await.len(),1);
        entry_views(&workspace,&runtime,&url,&run,"完成",None).await;
        process.kill().await.unwrap();process.wait().await.unwrap();let (mut restarted,socket)=daemon(&workspace,&runtime,&url).await;
        let restored=rpc(&socket,"restored","context.readback",params).await;assert_eq!(restored["result"],read["result"]);assert_eq!(captures.lock().await.len(),1);
        let changed=rpc(&socket,"permission","permissions.set",json!({"mode":"full"})).await;assert!(changed.get("error").is_none());
        let denied=rpc(&socket,"denied","context.readback",json!({"session_id":key,"expected_lifetime":created["result"]["session_lifetime_id"],"run_id":run,"local_diagnostics":true})).await;assert!(denied.get("error").is_some(),"权限改变禁止完整诊断");
        let db=rusqlite::Connection::open(workspace.join(".my-agent/runtime.sqlite3")).unwrap();db.execute("UPDATE provider_requests SET capture_json='{}' WHERE run_id=?1",[&run]).unwrap();
        let corrupt=rpc(&socket,"corrupt","context.readback",json!({"session_id":key,"expected_lifetime":created["result"]["session_lifetime_id"],"run_id":run})).await;assert!(corrupt.get("error").is_some());assert_eq!(captures.lock().await.len(),1);
        restarted.kill().await.unwrap();restarted.wait().await.unwrap();provider.abort();std::fs::remove_dir_all(workspace).unwrap();std::fs::remove_dir_all(runtime).unwrap();
    }).await.expect("Provider 请求只读合同超时");
}

#[tokio::test]
async fn shared_client_resyncs_real_dropped_frame_and_duplicate_without_provider_replay() {
    tokio::time::timeout(Duration::from_secs(45), async {
        use agent_core::RunStatus;
        use agent_daemon_protocol::{EventKind, HistoryReadMode, ServerFrame, SessionKey};
        let workspace = temp_workspace();
        let runtime = PathBuf::from(format!(
            "/tmp/ma-view-gap-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let (url, provider, count) = mock_ollama().await;
        let (mut process, socket) = daemon(&workspace, &runtime, &url).await;
        let key = "session-view-gap.jsonl";
        rpc(
            &socket,
            "create",
            "sessions.create",
            json!({"session_id":key}),
        )
        .await;
        let proxy_path = PathBuf::from(format!(
            "/tmp/ma-vproxy-{}-{}.sock",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let listener = tokio::net::UnixListener::bind(&proxy_path).unwrap();
        let upstream_path = socket.clone();
        let (dropped, observed) = tokio::sync::oneshot::channel();
        let proxy = tokio::spawn(async move {
            let (downstream, _) = listener.accept().await.unwrap();
            let upstream = UnixStream::connect(upstream_path).await.unwrap();
            let (mut input, mut output) = downstream.into_split();
            let (reader, mut writer) = upstream.into_split();
            let upload = tokio::spawn(async move {
                tokio::io::copy(&mut input, &mut writer).await.unwrap();
            });
            let mut reader = BufReader::new(reader);
            let mut dropped = Some(dropped);
            loop {
                let mut line = Vec::new();
                if reader.read_until(b'\n', &mut line).await.unwrap() == 0 {
                    break;
                }
                let frame: Value = serde_json::from_slice(&line).unwrap();
                if frame["event"] == "text_delta" && dropped.is_some() {
                    dropped.take().unwrap().send(frame).unwrap();
                    continue;
                }
                output.write_all(&line).await.unwrap();
                if frame["event"] == "turn_started" {
                    output.write_all(&line).await.unwrap();
                }
            }
            upload.abort();
        });
        let client = agent_daemon_client::DaemonClient::connect_unix(&proxy_path)
            .await
            .unwrap();
        client
            .read_session(&SessionKey(key.into()), HistoryReadMode::Omitted)
            .await
            .unwrap();
        let mut stream = client
            .request(
                "chat.send",
                json!({"session_id":key,"message":"验证丢帧恢复"}),
            )
            .await
            .unwrap();
        let mut starts = 0;
        let mut syncs = 0;
        let mut native = None;
        loop {
            match stream.next().await.expect("恢复不能伪造 EOF 终态") {
                ServerFrame::Event(event) if event.event == EventKind::TurnStarted => {
                    starts += 1;
                    native = event.run_id;
                }
                ServerFrame::Event(event) if event.event == EventKind::ViewResynced => {
                    syncs += 1;
                    let (snapshot, terminal) =
                        agent_daemon_protocol::decode_view_sync(event.data).unwrap();
                    assert_eq!(snapshot.session_id, key);
                    let terminal = terminal.unwrap();
                    assert_eq!(Some(terminal.run_id), native);
                    assert_eq!(terminal.status, RunStatus::Completed);
                    assert_eq!(terminal.content.as_deref(), Some("已完成"));
                }
                ServerFrame::Event(_) => {}
                ServerFrame::Response(response) => {
                    assert!(response.error.is_none(), "{response:?}");
                    break;
                }
            }
        }
        assert_eq!(starts, 1, "重复 live 只展示一次");
        assert_eq!(syncs, 1, "真实缺口先以 repository 快照建立基线");
        assert_eq!(observed.await.unwrap()["event"], "text_delta");
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let native = native.unwrap();
        assert_eq!(
            client.read_run(&native).await.unwrap().status,
            RunStatus::Completed
        );
        entry_views(&workspace, &runtime, &url, &native.0, "已完成", None).await;
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "二次入口与恢复不能重发 Provider"
        );
        drop(stream);
        drop(client);
        proxy.abort();
        process.kill().await.unwrap();
        process.wait().await.unwrap();
        provider.abort();
        std::fs::remove_file(proxy_path).unwrap();
        std::fs::remove_dir_all(workspace).unwrap();
        std::fs::remove_dir_all(runtime).unwrap();
    })
    .await
    .expect("真实丢帧、重复、恢复与三入口合同超时");
}

#[tokio::test]
async fn revision_reducer_rejects_delayed_readback_and_retired_lifetime_over_socket() {
    tokio::time::timeout(Duration::from_secs(45), async {
        let workspace=temp_workspace();let runtime=PathBuf::from(format!("/tmp/ma-view-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
        let (url,provider,_)=mock_delegation_ollama(vec![1]).await;
        let (mut process,socket)=daemon(&workspace,&runtime,&url).await;
        let key="session-view.jsonl";
        rpc(&socket,"create","sessions.create",json!({"session_id":key,"operation_id":"view-original"})).await;
        let old=rpc(&socket,"old","sessions.read",json!({"session_id":key,"history_mode":"omitted"})).await;
        chat(&socket,"view-turn",key,"建立新版事实",true).await;
        let fresh=rpc(&socket,"fresh","sessions.read",json!({"session_id":key,"history_mode":"omitted"})).await;
        let folded=rpc(&socket,"fold-fresh","views.reduce",json!({"state":null,"input":{"kind":"readback","snapshot":fresh["result"]}})).await;
        assert!(folded.get("error").is_none(),"共享reducer必须跨真实协议接线：{folded}");
        assert_eq!(folded["result"]["decision"],"accepted");
        let stale=rpc(&socket,"fold-old","views.reduce",json!({"state":folded["result"]["state"],"input":{"kind":"readback","snapshot":old["result"]}})).await;
        assert_eq!(stale["result"]["decision"],"ignored");assert_eq!(stale["result"]["state"],folded["result"]["state"]);
        rpc(&socket,"delete","sessions.delete",json!({"session_id":key,"expected_lifetime":fresh["result"]["session_lifetime_id"],"operation_id":"delete-view"})).await;
        assert!(rpc(&socket,"recreate","sessions.create",json!({"session_id":key,"operation_id":"view-recreated"})).await.get("error").is_none());
        let next=rpc(&socket,"next","sessions.read",json!({"session_id":key,"history_mode":"omitted"})).await;
        let reincarnated=rpc(&socket,"fold-next","views.reduce",json!({"state":folded["result"]["state"],"input":{"kind":"readback","snapshot":next["result"]}})).await;
        assert_eq!(reincarnated["result"]["decision"],"accepted","next={next}; folded={reincarnated}");
        let delayed=rpc(&socket,"retired","views.reduce",json!({"state":reincarnated["result"]["state"],"input":{"kind":"readback","snapshot":fresh["result"]}})).await;
        assert_eq!(delayed["result"]["decision"],"ignored");assert_eq!(delayed["result"]["state"],reincarnated["result"]["state"]);
        process.kill().await.unwrap();process.wait().await.unwrap();provider.abort();std::fs::remove_dir_all(workspace).unwrap();std::fs::remove_dir_all(runtime).unwrap();
    }).await.expect("共享reducer的旧事件/lifetime屏障合同超时");
}

#[tokio::test]
async fn deferred_tools_reject_direct_unfound_fuzzy_and_invalid_nested_schema_without_execution() {
    tokio::time::timeout(Duration::from_secs(45), async {
        const DIRECT:&str="{\"message\":{\"role\":\"assistant\",\"content\":\"\",\"tool_calls\":[{\"function\":{\"name\":\"recall_memory\",\"arguments\":{\"query\":\"约束\"}}}]},\"done\":false}\n{\"done\":true}\n";
        const UNFOUND:&str="{\"message\":{\"role\":\"assistant\",\"content\":\"\",\"tool_calls\":[{\"function\":{\"name\":\"tool_search\",\"arguments\":{\"name\":\"recall_memory\",\"arguments\":{\"query\":\"约束\"}}}}]},\"done\":false}\n{\"done\":true}\n";
        const FUZZY:&str="{\"message\":{\"role\":\"assistant\",\"content\":\"\",\"tool_calls\":[{\"function\":{\"name\":\"tool_search\",\"arguments\":{\"query\":\"select:recall_memor\"}}}]},\"done\":false}\n{\"done\":true}\n";
        const SEARCH:&str="{\"message\":{\"role\":\"assistant\",\"content\":\"\",\"tool_calls\":[{\"function\":{\"name\":\"tool_search\",\"arguments\":{\"query\":\"select:recall_memory\",\"limit\":1}}}]},\"done\":false}\n{\"done\":true}\n";
        const INVALID:&str="{\"message\":{\"role\":\"assistant\",\"content\":\"\",\"tool_calls\":[{\"function\":{\"name\":\"tool_search\",\"arguments\":{\"name\":\"recall_memory\",\"arguments\":{\"query\":42}}}}]},\"done\":false}\n{\"done\":true}\n";
        const DONE:&str="{\"message\":{\"role\":\"assistant\",\"content\":\"拒绝后完成\"},\"done\":false}\n{\"done\":true}\n";
        for (index,responses,reason,discovered,receipts) in [(0,vec![DIRECT],"隐藏工具禁止 direct",0,0),(1,vec![UNFOUND],"尚未在当前 run",0,0),(2,vec![FUZZY],"精确工具名不在冻结授权目录",0,1),(3,vec![SEARCH,INVALID],"参数校验失败",1,1)] {
            let workspace=temp_workspace();let runtime=PathBuf::from(format!("/tmp/ma-discovery-deny-{}-{}-{index}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
            let (url,provider,captures)=mock_response_sequence_ollama(responses,DONE).await;
            let (mut process,socket)=daemon(&workspace,&runtime,&url).await;
            assert!(rpc(&socket,"create","sessions.create",json!({"session_id":"session-discovery-deny.jsonl"})).await.get("error").is_none());
            let (run,response)=chat(&socket,"deny","session-discovery-deny.jsonl","拒绝非法调用",true).await;
            assert!(response.unwrap().get("error").is_none());
            let requests=captures.lock().await;
            assert!(requests.last().unwrap()["messages"].to_string().contains(reason),"{requests:?}");drop(requests);
            let fact=rpc(&socket,"discoveries","run.discovery",json!({"run_id":run})).await;
            assert_eq!(fact["result"]["receipts"].as_array().unwrap().len(),discovered);
            let audit=rpc(&socket,"receipts","run.tools",json!({"run_id":run})).await;
            assert_eq!(audit["result"]["receipts"].as_array().unwrap().len(),receipts,"{audit}");
            assert!(audit["result"]["receipts"].as_array().unwrap().iter().all(|receipt|receipt["name"]=="tool_search"));
            process.kill().await.unwrap();process.wait().await.unwrap();provider.abort();std::fs::remove_dir_all(workspace).unwrap();std::fs::remove_dir_all(runtime).unwrap();
        }
    }).await.expect("非法延迟工具不得执行");
}

#[tokio::test]
async fn deferred_tool_search_roundtrip_freezes_catalog_and_refills_same_dispatcher() {
    tokio::time::timeout(Duration::from_secs(45),async {
        const SEARCH:&str="{\"message\":{\"role\":\"assistant\",\"content\":\"\",\"tool_calls\":[{\"function\":{\"name\":\"tool_search\",\"arguments\":{\"query\":\"select:recall_memory\",\"limit\":1}}}]},\"done\":false}\n{\"done\":true}\n";
        const INVOKE:&str="{\"message\":{\"role\":\"assistant\",\"content\":\"\",\"tool_calls\":[{\"function\":{\"name\":\"tool_search\",\"arguments\":{\"name\":\"recall_memory\",\"arguments\":{\"query\":\"既有约束\",\"limit\":1}}}}]},\"done\":false}\n{\"done\":true}\n";
        const DONE:&str="{\"message\":{\"role\":\"assistant\",\"content\":\"完成\"},\"done\":false}\n{\"done\":true}\n";
        let workspace=temp_workspace();let runtime=PathBuf::from(format!("/tmp/ma-discovery-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
        let (url,provider,captures)=mock_response_sequence_ollama(vec![SEARCH,SEARCH,INVOKE],DONE).await;
        let (mut process,socket)=daemon(&workspace,&runtime,&url).await;
        let key="session-discovery.jsonl";rpc(&socket,"create","sessions.create",json!({"session_id":key})).await;
        let (run,response)=chat(&socket,"discovery",key,"先查找记忆工具再读取既有约束",true).await;
        assert!(response.unwrap().get("error").is_none());
        let requests=captures.lock().await.clone();assert_eq!(requests.len(),4);
        let tools=requests[0]["tools"].as_array().unwrap();
        assert!(tools.iter().any(|tool|tool["function"]["name"]=="tool_search"),"Provider 必须获得常驻 dispatcher：{tools:?}");
        assert!(!tools.iter().any(|tool|tool["function"]["name"]=="recall_memory"),"延迟 schema 不得常驻注入");
        let discovery=requests[1]["messages"].as_array().unwrap().iter().find(|message|message["role"]=="tool").unwrap();
        let result:Value=serde_json::from_str(discovery["content"].as_str().unwrap()).unwrap();
        assert_eq!(result["tools"][0]["name"],"recall_memory");assert!(result["tools"][0]["parameters"]["properties"]["query"].is_object());
        let fact=rpc(&socket,"discovery-readback","run.discovery",json!({"run_id":run})).await;
        assert_eq!(fact["result"]["receipts"].as_array().unwrap().len(),2,"{fact}");
        assert_eq!(fact["result"]["receipts"][0]["result"],result);
        assert_ne!(fact["result"]["receipts"][0]["operation_id"],fact["result"]["receipts"][1]["operation_id"],"跨轮重复 wire id 必须有独立执行身份");
        let receipts=rpc(&socket,"tools","run.tools",json!({"run_id":run})).await;
        assert!(receipts["result"]["receipts"].as_array().unwrap().iter().all(|receipt|receipt["name"]=="tool_search" && receipt["status"]=="terminal"));
        entry_views(&workspace,&runtime,&url,&run,"完成",None).await;
        process.kill().await.unwrap();process.wait().await.unwrap();
        let (mut restarted,socket)=daemon(&workspace,&runtime,&url).await;
        let restored=rpc(&socket,"after-restart","run.discovery",json!({"run_id":run})).await;
        assert_eq!(restored["result"],fact["result"]);
        entry_views(&workspace,&runtime,&url,&run,"完成",None).await;
        assert_eq!(captures.lock().await.len(),4,"恢复不得重放搜索或调用 Provider");
        restarted.kill().await.unwrap();restarted.wait().await.unwrap();provider.abort();std::fs::remove_dir_all(workspace).unwrap();std::fs::remove_dir_all(runtime).unwrap();
    }).await.expect("搜索/nested invoke/回填合同超时");
}

#[tokio::test]
async fn manual_compact_has_immediate_native_started_exact_cancel_and_unknown_restart() {
    tokio::time::timeout(Duration::from_secs(45), async {
        let workspace=temp_workspace();
        install_observer_hooks(&workspace,&[agent_core::HookEvent::PreCompact,agent_core::HookEvent::PostCompact]);
        let runtime=PathBuf::from(format!("/tmp/ma-compact-native-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
        let (url,provider,count)=mock_delegation_ollama(vec![1,2,3,4,6]).await;
        let (mut process,socket)=daemon(&workspace,&runtime,&url).await;
        let key="session-compact-native.jsonl";
        let created=rpc(&socket,"create","sessions.create",json!({"session_id":key})).await;
        let mut chat_owner=String::new();
        for index in 0..4 {
            let (run,response)=chat(&socket,&format!("seed-{index}"),key,&"constraint-".repeat(1000),true).await;
            assert!(response.unwrap().get("error").is_none());chat_owner=run;
        }
        let source=rpc(&socket,"source","sessions.read",json!({"session_id":key})).await;
        let command=json!({"session_id":key,"expected_lifetime":created["result"]["session_lifetime_id"],"operation_id":"native-compact","expected_revision":source["result"]["transcript_revision"],"expected_projection_generation":source["result"]["projection_generation"]});
        let started=rpc(&socket,"compact","compact.start",command.clone()).await;
        assert!(started.get("error").is_none(),"独立 compact 必须立即准入：{started}");
        let compact=started["result"]["run_id"].as_str().expect("native compact owner").to_owned();
        assert_ne!(compact,chat_owner);
        assert_eq!(started["result"]["kind"],"compact");
        let events=rpc(&socket,"events","run.events",json!({"run_id":compact,"after_seq":0})).await;
        assert!(events["result"]["events"].as_array().unwrap().iter().any(|event|event["event"]=="run_started"));
        assert!(!events["result"]["events"].as_array().unwrap().iter().any(|event|event["event"]=="terminal"));
        let stream=UnixStream::connect(&socket).await.unwrap();let (reader,mut writer)=stream.into_split();
        writer.write_all(format!("{}\n",json!({"jsonrpc":"2.0","id":"native-subscribe","method":"agent.subscribe","params":{"session_id":key,"request_id":started["result"]["request_id"],"after_seq":0}})).as_bytes()).await.unwrap();
        let mut live=BufReader::new(reader).lines();let published:Value=serde_json::from_str(&live.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(published["event"],"run_started");assert_eq!(published["run_id"],compact);
        drop(live);drop(writer);
        assert_eq!(rpc(&socket,"detached","run.read",json!({"run_id":compact})).await["result"]["status"],"running","断线只 detach");
        assert_eq!(rpc(&socket,"wrong-cancel","agent.cancel",json!({"session_id":key,"run_id":chat_owner})).await["result"]["cancelled"],false,"旧聊天 owner 不能取消 native compact");
        assert_eq!(rpc(&socket,"duplicate","compact.start",command.clone()).await["result"]["run_id"],compact);
        let mut conflicting=command.clone();conflicting["expected_revision"]=json!(0);
        assert!(rpc(&socket,"conflict","compact.start",conflicting).await.get("error").is_some());
        let mut competing=command.clone();competing["operation_id"]=json!("competing");
        assert!(rpc(&socket,"busy","compact.start",competing).await.get("error").is_some());
        loop {if count.load(Ordering::SeqCst)>=5 {break;} tokio::task::yield_now().await;}
        assert_eq!(rpc(&socket,"cancel","agent.cancel",json!({"session_id":key,"run_id":compact})).await["result"]["cancelled"],true);
        loop {
            let read=rpc(&socket,"cancelled","run.read",json!({"run_id":compact})).await;
            if read["result"]["status"]=="cancelled" {break;}tokio::task::yield_now().await;
        }
        assert_eq!(rpc(&socket,"chat-owner","run.read",json!({"run_id":chat_owner})).await["result"]["status"],"completed");
        let after=rpc(&socket,"after","sessions.read",json!({"session_id":key})).await;
        assert_eq!(after["result"]["transcript_revision"],source["result"]["transcript_revision"],"compact 不得伪造聊天原文");
        assert_eq!(after["result"]["projection_generation"],source["result"]["projection_generation"]);
        assert_eq!(rpc(&socket,"cancelled-retry","compact.start",command).await["result"]["run_id"],compact);
        let (next,response)=chat(&socket,"next",key,"取消压缩后继续聊天",true).await;
        assert!(response.unwrap().get("error").is_none());
        let source=rpc(&socket,"new-source","sessions.read",json!({"session_id":key})).await;
        let command=json!({"session_id":key,"expected_lifetime":created["result"]["session_lifetime_id"],"operation_id":"crash-compact","expected_revision":source["result"]["transcript_revision"],"expected_projection_generation":source["result"]["projection_generation"]});
        let started=rpc(&socket,"crash-start","compact.start",command.clone()).await;
        let uncertain=started["result"]["run_id"].as_str().unwrap().to_owned();
        loop {if count.load(Ordering::SeqCst)>=7 {break;}tokio::task::yield_now().await;}
        process.kill().await.unwrap();process.wait().await.unwrap();
        let (mut restarted,socket)=daemon(&workspace,&runtime,&url).await;
        let unknown=rpc(&socket,"unknown","run.read",json!({"run_id":uncertain})).await;
        assert_eq!(unknown["result"]["status"],"unknown_after_restart");
        assert_eq!(unknown["result"]["kind"],"compact");
        assert_eq!(unknown["result"]["compact"]["outcome"],"unknown");
        agent_daemon_protocol::decode_run_readback(unknown["result"].clone()).unwrap();
        assert_eq!(rpc(&socket,"unknown-retry","compact.start",command).await["result"]["run_id"],uncertain);
        entry_views_with_decision(&workspace,&runtime,&url,&next,"子任务完成",None,Some(&format!("/run {uncertain}"))).await;
        assert_eq!(count.load(Ordering::SeqCst),7,"重启不得重放摘要");
        restarted.kill().await.unwrap();restarted.wait().await.unwrap();provider.abort();
        std::fs::remove_dir_all(workspace).unwrap();std::fs::remove_dir_all(runtime).unwrap();
    }).await.expect("独立 compact 生命周期合同超时");
}

#[tokio::test]
async fn governed_tool_approval_error_and_terminal_hooks_use_durable_owners_and_redacted_payloads()
{
    tokio::time::timeout(Duration::from_secs(45),async {
        let workspace=temp_workspace();install_observer_hooks(&workspace,&agent_core::HookEvent::ALL);
        std::fs::write(workspace.join("bad.txt"),[0xff,0xfe]).unwrap();
        let runtime=PathBuf::from(format!("/tmp/ma-hook-tools-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
        let (url,provider,_)=mock_tool_response_ollama("{\"message\":{\"role\":\"assistant\",\"content\":\"\",\"tool_calls\":[{\"function\":{\"name\":\"read_file\",\"arguments\":{\"path\":\"bad.txt\"}}},{\"function\":{\"name\":\"write_file\",\"arguments\":{\"path\":\"hook-result.txt\",\"content\":\"完成\"}}}]},\"done\":false}\n{\"done\":true}\n").await;
        let (mut process,socket)=daemon(&workspace,&runtime,&url).await;
        let created=rpc(&socket,"create","sessions.create",json!({"session_id":"session-hook-tools.jsonl"})).await;
        assert!(rpc(&socket,"permissions","permissions.set",json!({"mode":"request_approval"})).await.get("error").is_none());
        let stream=UnixStream::connect(&socket).await.unwrap();let (reader,mut writer)=stream.into_split();
        writer.write_all(format!("{}\n",json!({"jsonrpc":"2.0","id":"tools","method":"chat.send","params":{"session_id":"session-hook-tools.jsonl","message":"敏感输入不进入 hook payload","entry_channel":"cli"}})).as_bytes()).await.unwrap();
        let mut frames=BufReader::new(reader).lines();let run=loop {
            let frame:Value=serde_json::from_str(&frames.next_line().await.unwrap().unwrap()).unwrap();
            if frame["event"]=="approval_required" {
                let interaction=&frame["data"]["interaction"];
                let response=rpc(&socket,"approve","interaction.respond",json!({"session_id":"session-hook-tools.jsonl","run_id":frame["run_id"],"interaction_id":interaction["interaction_id"],"revision":interaction["revision"],"approved":true})).await;
                assert!(response.get("error").is_none(),"{response}");
            }
            if frame.get("id").is_some() {assert!(frame.get("error").is_none(),"{frame}");break frame["result"]["run_id"].as_str().unwrap().to_owned();}
        };
        assert_eq!(std::fs::read_to_string(workspace.join("hook-result.txt")).unwrap(),"完成");
        let read=rpc(&socket,"hooks","hooks.readback",json!({"session_id":"session-hook-tools.jsonl","expected_lifetime":created["result"]["session_lifetime_id"]})).await;
        let records=read["result"]["outcomes"].as_array().unwrap();
        for event in ["user_prompt_submit","pre_tool","post_tool","tool_error","approval_requested","approval_resolved","assistant_reply","after_turn","notification","stop"] {
            assert!(records.iter().any(|record|record["payload"]["event"]==event),"缺 {event}: {read}");
        }
        for record in records.iter().filter(|record|record["payload"]["owner"].is_object()) {
            assert_eq!(record["payload"]["owner"]["run_id"],run);assert_eq!(record["payload"]["channel"],"cli");assert_eq!(record["status"],"succeeded","{read}");
        }
        let requested=records.iter().find(|record|record["payload"]["event"]=="approval_requested").unwrap();let resolved=records.iter().find(|record|record["payload"]["event"]=="approval_resolved").unwrap();
        assert_eq!(requested["payload"]["data"]["interaction_status"],"pending");assert_eq!(resolved["payload"]["data"]["interaction_status"],"answered");assert_eq!(resolved["payload"]["data"]["approved"],true);
        assert!(!read.to_string().contains("敏感输入不进入"));assert!(!read.to_string().contains("OPENAI_BASE_URL"));
        entry_views(&workspace,&runtime,&url,&run,"完成",None).await;
        process.kill().await.unwrap();process.wait().await.unwrap();provider.abort();std::fs::remove_dir_all(workspace).unwrap();std::fs::remove_dir_all(runtime).unwrap();
    }).await.expect("tool/approval hook 合同超时");
}

fn install_observer_hooks(workspace: &Path, events: &[agent_core::HookEvent]) {
    use std::os::unix::fs::PermissionsExt;
    let directory = workspace.join(".my-agent");
    std::fs::create_dir_all(&directory).unwrap();
    let executable = directory.join("observer");
    std::fs::write(&executable,"#!/bin/sh\ncat >/dev/null\n[ -z \"$OPENAI_BASE_URL\" ] || exit 19\nprintf '{\"effect\":\"observe\"}'\n").unwrap();
    let config = directory.join("hooks.json");
    std::fs::write(&config,serde_json::to_vec(&json!({"schema_version":1,"hooks":events.iter().map(|event|json!({"event":event,"executable":executable,"timeout_ms":5000})).collect::<Vec<_>>()})).unwrap()).unwrap();
    for (path, mode) in [(&directory, 0o700), (&executable, 0o700), (&config, 0o600)] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }
}

#[tokio::test]
async fn automatic_compact_hooks_follow_real_intents_and_settlements() {
    tokio::time::timeout(Duration::from_secs(45),async {
        let workspace=temp_workspace();install_observer_hooks(&workspace,&[agent_core::HookEvent::PreCompact,agent_core::HookEvent::PostCompact]);
        let runtime=PathBuf::from(format!("/tmp/ma-auto-hooks-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
        let (url,provider,_)=mock_delegation_ollama((1..=50).collect()).await;
        let (mut process,socket)=daemon(&workspace,&runtime,&url).await;
        let created=rpc(&socket,"create","sessions.create",json!({"session_id":"session-auto-hooks.jsonl"})).await;
        let mut last=String::new();
        for index in 0..16 {
            let (run,response)=chat(&socket,&format!("auto-{index}"),"session-auto-hooks.jsonl",&"constraint-".repeat(500),true).await;
            assert!(response.unwrap().get("error").is_none());last=run;
        }
        let read=rpc(&socket,"hooks","hooks.readback",json!({"session_id":"session-auto-hooks.jsonl","expected_lifetime":created["result"]["session_lifetime_id"]})).await;
        let outcomes=read["result"]["outcomes"].as_array().unwrap();assert!(!outcomes.is_empty(),"{read}");
        assert_eq!(outcomes.len()%2,0,"{read}");
        for pair in outcomes.chunks_exact(2) {
            assert_eq!(pair[0]["payload"]["event"],"pre_compact");assert_eq!(pair[1]["payload"]["event"],"post_compact");
            assert_eq!(pair[0]["payload"]["owner"],pair[1]["payload"]["owner"]);
            assert!(matches!(pair[1]["payload"]["data"]["outcome"].as_str(),Some("succeeded"|"no_gain")));
            assert!(pair.iter().all(|record|record["status"]=="succeeded"),"{read}");
        }
        entry_views(&workspace,&runtime,&url,&last,"子任务完成",None).await;
        process.kill().await.unwrap();process.wait().await.unwrap();provider.abort();std::fs::remove_dir_all(workspace).unwrap();std::fs::remove_dir_all(runtime).unwrap();
    }).await.expect("自动 compact hooks 合同超时");
}

#[tokio::test]
async fn manual_compact_hooks_publish_no_gain_rejection_and_summary_failure_after_settlement() {
    tokio::time::timeout(Duration::from_secs(45), async {
        const DONE: &str = "{\"message\":{\"role\":\"assistant\",\"content\":\"完成\"},\"done\":false}\n{\"done\":true}\n";
        const EMPTY: &str = "{\"message\":{\"role\":\"assistant\",\"content\":\"\"},\"done\":true}\n";
        for outcome in ["no_gain", "rejected", "failed"] {
            let workspace=temp_workspace();
            install_observer_hooks(&workspace, &[agent_core::HookEvent::PreCompact, agent_core::HookEvent::PostCompact]);
            if outcome == "rejected" {
                std::fs::write(workspace.join(".my-agent/observer"), "#!/bin/sh\npayload=$(cat)\ncase \"$payload\" in *'\"event\":\"pre_compact\"'*) printf '{\"effect\":\"deny\",\"reason\":\"显式拒绝\"}';; *) printf '{\"effect\":\"observe\"}';; esac\n").unwrap();
            }
            let runtime=PathBuf::from(format!("/tmp/ma-compact-outcome-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
            let turns=if outcome=="failed" {2} else {1};
            let (url, provider, captures)=mock_response_sequence_ollama(vec![DONE; turns], EMPTY).await;
            let (mut process, socket)=daemon(&workspace,&runtime,&url).await;
            let key="session-hook-outcomes.jsonl";
            let created=rpc(&socket,"create","sessions.create",json!({"session_id":key})).await;
            let mut owner=String::new();
            for index in 0..turns {
                let (run,response)=chat(&socket,&format!("seed-{index}"),key,&"约束".repeat(200),true).await;
                assert!(response.unwrap().get("error").is_none());owner=run;
            }
            let source=rpc(&socket,"source","sessions.read",json!({"session_id":key})).await;
            let command=json!({"session_id":key,"owner_run_id":owner,"operation_id":"compact-effect","expected_revision":source["result"]["transcript_revision"]});
            let result=rpc(&socket,"compact","sessions.compact",command.clone()).await;
            assert_eq!(result.get("error").is_some(),outcome!="no_gain","{outcome}: {result}");
            let read=rpc(&socket,"hooks","hooks.readback",json!({"session_id":key,"expected_lifetime":created["result"]["session_lifetime_id"]})).await;
            let records=read["result"]["outcomes"].as_array().unwrap();assert_eq!(records.len(),2,"{read}");
            assert_eq!(records[0]["payload"]["event"],"pre_compact");
            assert_eq!(records[1]["payload"]["event"],"post_compact");
            assert_eq!(records[1]["payload"]["data"]["outcome"],outcome);
            assert_eq!(records[0]["payload"]["owner"],records[1]["payload"]["owner"]);
            assert_eq!(records[1]["status"],"succeeded");
            let after=rpc(&socket,"after","sessions.read",json!({"session_id":key})).await;
            assert_eq!(after["result"]["projection_generation"],0);
            assert_eq!(after["result"]["transcript_revision"],source["result"]["transcript_revision"]);
            assert_eq!(captures.lock().await.len(), turns+usize::from(outcome=="failed"));
            process.kill().await.unwrap();process.wait().await.unwrap();
            let (mut restarted,socket)=daemon(&workspace,&runtime,&url).await;
            let retry=rpc(&socket,"retry","sessions.compact",command).await;
            assert_eq!(retry.get("error").is_some(),outcome!="no_gain","{retry}");
            let restored=rpc(&socket,"restored","hooks.readback",json!({"session_id":key,"expected_lifetime":created["result"]["session_lifetime_id"]})).await;
            assert_snapshot_projection(&restored["result"].to_string(),&read["result"].to_string());
            let native=records[0]["payload"]["owner"]["run_id"].as_str().unwrap();
            assert_ne!(native,owner,"旧 unary 必须分配独立 native owner");
            entry_views_with_decision(&workspace,&runtime,&url,&owner,"完成",None,Some(&format!("/run {native}"))).await;
            restarted.kill().await.unwrap();restarted.wait().await.unwrap();provider.abort();
            std::fs::remove_dir_all(workspace).unwrap();std::fs::remove_dir_all(runtime).unwrap();
        }
    }).await.expect("manual compact hook outcome 合同超时");
}

#[tokio::test]
async fn stop_continuation_has_a_new_native_run_and_exact_cancel_preserves_parent_terminal() {
    tokio::time::timeout(Duration::from_secs(30),async {
        let workspace=temp_workspace();let directory=workspace.join(".my-agent");std::fs::create_dir_all(&directory).unwrap();
        let executable=directory.join("stop-hook");
        std::fs::write(&executable,"#!/bin/sh\ncat >/dev/null\nprintf '{\"effect\":\"continue\",\"prompt\":\"继续核验\"}'\n").unwrap();
        std::fs::write(directory.join("hooks.json"),serde_json::to_vec(&json!({"schema_version":1,"hooks":[{"event":"stop","executable":executable,"timeout_ms":1000}]})).unwrap()).unwrap();
        use std::os::unix::fs::PermissionsExt;
        for (path,mode) in [(&directory,0o700),(&executable,0o700),(&directory.join("hooks.json"),0o600)] {std::fs::set_permissions(path,std::fs::Permissions::from_mode(mode)).unwrap();}
        let runtime=PathBuf::from(format!("/tmp/ma-stop-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
        let (url,provider,_)=mock_ollama().await;let (mut process,socket)=daemon(&workspace,&runtime,&url).await;
        let created=rpc(&socket,"create","sessions.create",json!({"session_id":"session-stop.jsonl"})).await;
        let (parent,response)=chat(&socket,"parent","session-stop.jsonl","完成一次核验",true).await;
        let response=response.unwrap();assert!(response.get("error").is_none(),"{response}");
        let child=response["result"]["continuation_run_id"].as_str().expect("新 continuation native run").to_owned();assert_ne!(child,parent);
        let read=rpc(&socket,"child","run.read",json!({"run_id":child})).await;
        assert_eq!(read["result"]["continuation_parent_run_id"],parent);
        assert!(matches!(read["result"]["status"].as_str(),Some("queued"|"running")));
        let cancel=rpc(&socket,"cancel","agent.cancel",json!({"session_id":"session-stop.jsonl","run_id":child})).await;
        assert_eq!(cancel["result"]["cancelled"],true,"{cancel}");
        loop {
            let read=rpc(&socket,"cancelled","run.read",json!({"run_id":child})).await;
            if read["result"]["status"]=="cancelled" {break;}
            tokio::task::yield_now().await;
        }
        assert_eq!(rpc(&socket,"parent-terminal","run.read",json!({"run_id":parent})).await["result"]["status"],"completed");
        let retry=rpc(&socket,"parent","chat.send",json!({"session_id":"session-stop.jsonl","message":"完成一次核验"})).await;
        assert_eq!(retry["result"]["continuation_run_id"],child);
        entry_views(&workspace,&runtime,&url,&parent,"已完成",None).await;
        process.kill().await.unwrap();process.wait().await.unwrap();
        let (mut restarted,socket)=daemon(&workspace,&runtime,&url).await;
        assert_eq!(rpc(&socket,"restored","run.read",json!({"run_id":child})).await["result"]["status"],"cancelled");
        let outcomes=rpc(&socket,"hooks","hooks.readback",json!({"session_id":"session-stop.jsonl","expected_lifetime":created["result"]["session_lifetime_id"]})).await;
        assert_eq!(outcomes["result"]["outcomes"][0]["payload"]["event"],"stop");
        restarted.kill().await.unwrap();restarted.wait().await.unwrap();provider.abort();
        std::fs::remove_dir_all(workspace).unwrap();std::fs::remove_dir_all(runtime).unwrap();
    }).await.expect("Stop continuation 合同超时");
}

#[tokio::test]
async fn stop_continuation_crash_preserves_unknown_child_without_replaying_hook_or_provider() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let workspace=temp_workspace();
        install_observer_hooks(&workspace,&[agent_core::HookEvent::Stop]);
        std::fs::write(workspace.join(".my-agent/observer"),"#!/bin/sh\ncat >/dev/null\nprintf '{\"effect\":\"continue\",\"prompt\":\"继续核验\"}'\n").unwrap();
        let runtime=PathBuf::from(format!("/tmp/ma-stop-crash-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
        let (url,provider,count)=mock_ollama().await;
        let (mut process,socket)=daemon(&workspace,&runtime,&url).await;
        let key="session-stop-crash.jsonl";
        let created=rpc(&socket,"create","sessions.create",json!({"session_id":key})).await;
        let (parent,response)=chat(&socket,"parent",key,"完成一次核验",true).await;
        let response=response.unwrap();assert!(response.get("error").is_none(),"{response}");
        let child=response["result"]["continuation_run_id"].as_str().unwrap().to_owned();
        loop {
            if count.load(Ordering::SeqCst)>=2 {break;}
            let child_read=rpc(&socket,"child-start","run.read",json!({"run_id":child})).await;
            assert!(!matches!(child_read["result"]["status"].as_str(),Some("failed"|"cancelled"|"completed")),"child 未启动 Provider: {child_read}");
            tokio::task::yield_now().await;
        }
        assert_eq!(count.load(Ordering::SeqCst),2);
        let original=rpc(&socket,"original","hooks.readback",json!({"session_id":key,"expected_lifetime":created["result"]["session_lifetime_id"]})).await;
        assert_eq!(original["result"]["outcomes"].as_array().unwrap().len(),1);
        process.kill().await.unwrap();process.wait().await.unwrap();
        let (mut restarted,socket)=daemon(&workspace,&runtime,&url).await;
        let unknown=rpc(&socket,"child","run.read",json!({"run_id":child})).await;
        assert_eq!(unknown["result"]["status"],"unknown_after_restart");
        assert_eq!(unknown["result"]["continuation_parent_run_id"],parent);
        let retry=rpc(&socket,"parent","chat.send",json!({"session_id":key,"message":"完成一次核验"})).await;
        assert_eq!(retry["result"]["continuation_run_id"],child);
        let restored=rpc(&socket,"restored","hooks.readback",json!({"session_id":key,"expected_lifetime":created["result"]["session_lifetime_id"]})).await;
        assert_snapshot_projection(&restored["result"].to_string(),&original["result"].to_string());
        entry_views(&workspace,&runtime,&url,&parent,"已完成",None).await;
        assert_eq!(count.load(Ordering::SeqCst),2,"未知续跑不能重放 Provider");
        assert_eq!(rpc(&socket,"parent-read","run.read",json!({"run_id":parent})).await["result"]["status"],"completed");
        restarted.kill().await.unwrap();restarted.wait().await.unwrap();provider.abort();
        std::fs::remove_dir_all(workspace).unwrap();std::fs::remove_dir_all(runtime).unwrap();
    }).await.expect("Stop continuation crash 合同超时");
}

#[tokio::test]
async fn governed_session_hooks_follow_publication_once_and_survive_restart() {
    let workspace = temp_workspace();
    let directory = workspace.join(".my-agent");
    std::fs::create_dir_all(&directory).unwrap();
    let executable = directory.join("observe-hook");
    std::fs::write(
        &executable,
        "#!/bin/sh\ncat >/dev/null\nprintf '{\"effect\":\"observe\"}'\n",
    )
    .unwrap();
    std::fs::write(
        directory.join("hooks.json"),
        serde_json::to_vec(&json!({"schema_version":1,"hooks":[
            {"event":"session_start","executable":executable,"timeout_ms":5000},
            {"event":"session_end","executable":executable,"timeout_ms":5000}
        ]}))
        .unwrap(),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::set_permissions(
        directory.join("hooks.json"),
        std::fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    let runtime = PathBuf::from(format!(
        "/tmp/ma-hooks-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let (url, provider, _) = mock_ollama().await;
    let (mut process, socket) = daemon(&workspace, &runtime, &url).await;
    let created = rpc(
        &socket,
        "new",
        "sessions.create",
        json!({"session_id":"session-hook.jsonl","operation_id":"new-hook"}),
    )
    .await;
    assert!(created.get("error").is_none(), "{created}");
    let repeated = rpc(
        &socket,
        "new-again",
        "sessions.create",
        json!({"session_id":"session-hook.jsonl","operation_id":"new-hook"}),
    )
    .await;
    assert!(repeated.get("error").is_none());
    let lifetime = created["result"]["session_lifetime_id"].clone();
    let fork_params = json!({"session_id":"session-hook.jsonl","target_session_id":"session-hook-fork.jsonl","operation_id":"fork-hook","expected_revision":0,"expected_lifetime":lifetime});
    let forked = rpc(&socket, "fork", "sessions.fork", fork_params.clone()).await;
    assert_eq!(forked["result"]["forked"], true, "{forked}");
    assert!(
        rpc(&socket, "fork-again", "sessions.fork", fork_params)
            .await
            .get("error")
            .is_none()
    );
    let fork_meta = rpc(
        &socket,
        "fork-read",
        "sessions.read",
        json!({"session_id":"session-hook-fork.jsonl"}),
    )
    .await;
    let fork_hooks = rpc(&socket, "fork-hooks", "hooks.readback", json!({"session_id":"session-hook-fork.jsonl","expected_lifetime":fork_meta["result"]["session_lifetime_id"]})).await;
    assert_eq!(
        fork_hooks["result"]["outcomes"].as_array().unwrap().len(),
        1
    );
    assert_eq!(
        fork_hooks["result"]["outcomes"][0]["payload"]["event"],
        "session_start"
    );
    assert_eq!(fork_hooks["result"]["outcomes"][0]["status"], "succeeded");
    let close = json!({"session_id":"session-hook.jsonl","expected_lifetime":lifetime,"operation_id":"close-hook"});
    let (first, second) = tokio::join!(
        rpc(&socket, "close1", "sessions.delete", close.clone()),
        rpc(&socket, "close2", "sessions.delete", close)
    );
    assert!(first.get("error").is_none(), "{first}");
    assert!(second.get("error").is_none(), "{second}");
    let read = rpc(
        &socket,
        "read",
        "hooks.readback",
        json!({"session_id":"session-hook.jsonl","expected_lifetime":lifetime}),
    )
    .await;
    let records = read["result"]["outcomes"]
        .as_array()
        .expect("持久 hook outcome");
    assert_eq!(records.len(), 2, "{read}");
    assert_eq!(records[0]["payload"]["event"], "session_start");
    assert_eq!(records[1]["payload"]["event"], "session_end");
    assert!(
        records.iter().all(|record| record["status"] == "succeeded"),
        "{read}"
    );
    process.kill().await.unwrap();
    process.wait().await.unwrap();
    let (mut restarted, socket) = daemon(&workspace, &runtime, &url).await;
    let restored = rpc(
        &socket,
        "restored",
        "hooks.readback",
        json!({"session_id":"session-hook.jsonl","expected_lifetime":lifetime}),
    )
    .await;
    assert_snapshot_projection(&restored["result"].to_string(), &read["result"].to_string());
    restarted.kill().await.unwrap();
    restarted.wait().await.unwrap();
    provider.abort();
    std::fs::remove_dir_all(workspace).unwrap();
    std::fs::remove_dir_all(runtime).unwrap();
}

#[tokio::test]
async fn restarted_pending_plan_can_be_discarded_by_exact_identity_and_release_its_queue() {
    tokio::time::timeout(Duration::from_secs(30),async {
        let workspace=temp_workspace();
        let runtime_dir=PathBuf::from(format!("/tmp/ma-plan-discard-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
        let (url,provider,_)=mock_tool_catalog_ollama(true).await;
        let (mut process,socket)=daemon(&workspace,&runtime_dir,&url).await;
        let created=rpc(&socket,"create","sessions.create",json!({"session_id":"session-discard.jsonl"})).await;
        let lifetime=created["result"]["session_lifetime_id"].clone();
        let (seed,response)=chat(&socket,"author","session-discard.jsonl","制定计划",true).await;
        assert!(response.unwrap().get("error").is_none());
        let read=rpc(&socket,"read","sessions.plan.readback",json!({"session_id":"session-discard.jsonl"})).await;
        let plan=&read["result"]["plan"];
        let mut identity=json!({"plan_id":plan["plan_id"],"revision":plan["revision"],"content_digest":plan["content_digest"],"operation_id":"pending"});
        let snapshot=rpc(&socket,"snapshot","runs.read",json!({"run_id":seed})).await;
        let snapshot:agent_core::RunSnapshot=serde_json::from_value(snapshot["result"]["snapshot"].clone()).unwrap();
        process.kill().await.unwrap();process.wait().await.unwrap();
        let store=agent_storage::RunStore::open(&workspace.join(".my-agent/runtime.sqlite3")).unwrap();
        let admission=agent_core::RunAdmission {session_key:agent_core::SessionKey("session-discard.jsonl".into()),expected_lifetime:Some(serde_json::from_value(lifetime.clone()).unwrap()),request_id:agent_core::RequestId::String("pending".into()),input:"执行".into(),mode:agent_core::AdmissionMode::RejectIfBusy,plan_execution:Some(serde_json::from_value(identity.clone()).unwrap())};
        let agent_core::Admission::New(pending)=store.admit_run(&admission,&snapshot).unwrap() else {panic!("new")};
        drop(store);
        let (mut restarted,socket)=daemon(&workspace,&runtime_dir,&url).await;
        identity["operation_id"]=json!("discard-pending");
        let params=json!({"session_id":"session-discard.jsonl","expected_lifetime":lifetime,"identity":identity});
        let discarded=rpc(&socket,"discard","sessions.plan.discard",params.clone()).await;
        assert_eq!(discarded["result"]["run_id"],pending.run_id.0,"{discarded}");
        assert_eq!(discarded["result"],rpc(&socket,"duplicate","sessions.plan.discard",params).await["result"]);
        let cancelled=rpc(&socket,"cancelled","runs.read",json!({"run_id":pending.run_id})).await;
        assert_eq!(cancelled["result"]["status"],"cancelled");
        let read=rpc(&socket,"read-discard","sessions.plan.readback",json!({"session_id":"session-discard.jsonl"})).await;
        assert_eq!(read["result"]["plan"]["review"],"rejected");
        let (next,response)=chat(&socket,"next","session-discard.jsonl","继续正常聊天",true).await;
        assert!(response.unwrap().get("error").is_none());
        entry_views(&workspace,&runtime_dir,&url,&next,"完成",None).await;
        restarted.kill().await.unwrap();restarted.wait().await.unwrap();provider.abort();
        std::fs::remove_dir_all(workspace).unwrap();std::fs::remove_dir_all(runtime_dir).unwrap();
    }).await.expect("pending plan discard timeout");
}

#[tokio::test]
async fn versioned_plan_readback_execute_discard_and_restart_share_daemon_facts() {
    tokio::time::timeout(Duration::from_secs(45), async {
        let workspace=temp_workspace();
        let runtime_dir=PathBuf::from(format!("/tmp/ma-plan-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
        let (url,provider,captures)=mock_tool_catalog_ollama(true).await;
        let (mut process,socket)=daemon(&workspace,&runtime_dir,&url).await;
        let created=rpc(&socket,"create-plan","sessions.create",json!({"session_id":"session-plan.jsonl"})).await;
        assert!(created.get("error").is_none(),"{created}");
        let lifetime=created["result"]["session_lifetime_id"].clone();
        let (seed,response)=chat(&socket,"author","session-plan.jsonl","只制定计划",true).await;
        assert!(response.unwrap().get("error").is_none());
        let read=rpc(&socket,"read","sessions.plan.readback",json!({"session_id":"session-plan.jsonl"})).await;
        assert_eq!(read["result"]["plan"]["review"],"pending_review","{read}");
        assert!(read["result"]["markdown"].as_str().unwrap().contains("验证持久恢复"));
        entry_views(&workspace,&runtime_dir,&url,&seed,"完成",None).await;
        let plan=&read["result"]["plan"];
        let identity=json!({"plan_id":plan["plan_id"],"revision":plan["revision"],"content_digest":plan["content_digest"],"operation_id":"execute"});
        let mut stale=identity.clone(); stale["content_digest"]=json!("stale");
        let rejected=rpc(&socket,"stale","chat.send",json!({"session_id":"session-plan.jsonl","expected_lifetime":lifetime,"admission_mode":"reject_if_busy","message":"执行","plan_execution":stale})).await;
        assert_eq!(rejected["error"]["code"],-32001,"{rejected}");
        let execution_line=format!("/plan execute {} {} {} {} execute",lifetime.as_str().unwrap(),plan["plan_id"].as_str().unwrap(),plan["revision"],plan["content_digest"].as_str().unwrap());
        let native_input=format!("执行已确认计划 {} 版本 {} 摘要 {}。",plan["plan_id"].as_str().unwrap(),plan["revision"],plan["content_digest"].as_str().unwrap());
        let command=json!({"session_id":"session-plan.jsonl","expected_lifetime":lifetime,"admission_mode":"reject_if_busy","message":native_input,"plan_execution":identity});
        let seed_read=rpc(&socket,"seed-read","runs.read",json!({"run_id":seed})).await;
        let snapshot:agent_core::RunSnapshot=serde_json::from_value(seed_read["result"]["snapshot"].clone()).unwrap();
        process.kill().await.unwrap();process.wait().await.unwrap();
        let store=agent_storage::RunStore::open(&workspace.join(".my-agent/runtime.sqlite3")).unwrap();
        let prepared=store.admit_run(&agent_core::RunAdmission {
            session_key:agent_core::SessionKey("session-plan.jsonl".into()),
            expected_lifetime:Some(serde_json::from_value(lifetime.clone()).unwrap()),
            request_id:agent_core::RequestId::String("execute".into()),input:native_input,
            mode:agent_core::AdmissionMode::RejectIfBusy,
            plan_execution:Some(serde_json::from_value(identity.clone()).unwrap()),
        },&snapshot).unwrap();
        let agent_core::Admission::New(prepared)=prepared else { panic!("new pending execution") };
        drop(store);
        let (replacement,new_socket)=daemon(&workspace,&runtime_dir,&url).await;
        process=replacement;
        let pending=rpc(&new_socket,"pending","sessions.plan.readback",json!({"session_id":"session-plan.jsonl"})).await;
        assert_eq!(pending["result"]["plan"]["review"],"pending_execution");
        let queued=rpc(&new_socket,"queued","runs.read",json!({"run_id":prepared.run_id})).await;
        assert_eq!(queued["result"]["status"],"queued");
        assert_eq!(captures.lock().await.len(),2,"pending_execution 重启不得自动调用 Provider");
        let socket=new_socket;
        let executed=rpc(&socket,"execute","chat.send",command.clone()).await;
        assert!(executed.get("error").is_none(),"{executed}");
        let run=executed["result"]["run_id"].as_str().unwrap();
        let duplicate=rpc(&socket,"new-wire-id","chat.send",command.clone()).await;
        assert_eq!(duplicate["result"]["run_id"],run);
        let mut conflict=command.clone(); conflict["message"]=json!("不同输入");
        assert!(rpc(&socket,"conflict","chat.send",conflict).await.get("error").is_some());
        let after=rpc(&socket,"after","sessions.plan.readback",json!({"session_id":"session-plan.jsonl"})).await;
        assert_eq!(after["result"]["plan"]["review"],"executing");
        assert_eq!(after["result"]["plan"]["revision"],plan["revision"]);
        entry_views(&workspace,&runtime_dir,&url,run,"完成",None).await;
        process.kill().await.unwrap();process.wait().await.unwrap();
        let (mut restarted,socket)=daemon(&workspace,&runtime_dir,&url).await;
        let restored=rpc(&socket,"restored","sessions.plan.readback",json!({"session_id":"session-plan.jsonl"})).await;
        assert_eq!(restored["result"]["plan"],after["result"]["plan"]);
        let mut discard=identity.clone();discard["operation_id"]=json!("discard");
        let params=json!({"session_id":"session-plan.jsonl","expected_lifetime":lifetime,"identity":discard});
        let receipt=rpc(&socket,"discard","sessions.plan.discard",params.clone()).await;
        assert!(receipt.get("error").is_none(),"{receipt}");
        assert_eq!(receipt["result"],rpc(&socket,"discard-again","sessions.plan.discard",params).await["result"]);
        let decision=rpc(&socket,"discard-read","sessions.plan.readback",json!({"session_id":"session-plan.jsonl"})).await;
        assert_eq!(decision["result"]["plan"]["review"],"rejected");
        let discard_line=format!("/plan discard session-plan.jsonl {} {} {} {} discard",lifetime.as_str().unwrap(),plan["plan_id"].as_str().unwrap(),plan["revision"],plan["content_digest"].as_str().unwrap());
        entry_views_with_decision(&workspace,&runtime_dir,&url,run,"完成",None,Some(&discard_line)).await;
        entry_views_with_decision(&workspace,&runtime_dir,&url,run,"完成",None,Some(&execution_line)).await;
        let calls=captures.lock().await.len();
        entry_views(&workspace,&runtime_dir,&url,run,"完成",None).await;
        assert_eq!(captures.lock().await.len(),calls,"readback/三入口读取不得启动 Provider");
        restarted.kill().await.unwrap();restarted.wait().await.unwrap();
        provider.abort();std::fs::remove_dir_all(workspace).unwrap();std::fs::remove_dir_all(runtime_dir).unwrap();
    }).await.expect("计划真实进程合同超时");
}

async fn ready_web_socket(
    child: &mut Child,
    port: u16,
    diagnostics: &Path,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let ready = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                panic!(
                    "Web 服务就绪前退出 {status}: {}",
                    std::fs::read_to_string(diagnostics).unwrap()
                );
            }
            if let Ok((socket, _)) =
                tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/ws")).await
            {
                return socket;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    ready.unwrap_or_else(|_| {
        panic!(
            "Web 服务未就绪: {}",
            std::fs::read_to_string(diagnostics).unwrap()
        )
    })
}

fn temp_workspace() -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "my-agent-restart-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&path).unwrap();
    std::fs::canonicalize(path).unwrap()
}

async fn mock_ollama() -> (String, tokio::task::JoinHandle<()>, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let requests = count.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let served = requests.fetch_add(1, Ordering::SeqCst) + 1;
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buffer = [0u8; 8192];
                loop {
                    let read = stream.read(&mut buffer).await.unwrap();
                    if read == 0 {
                        return;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|part| part == b"\r\n\r\n") {
                        break;
                    }
                }
                if served == 1 {
                    let body = "{\"message\":{\"role\":\"assistant\",\"content\":\"已完成\"},\"done\":false}\n{\"done\":true,\"prompt_eval_count\":2,\"eval_count\":2}\n";
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    stream.write_all(response.as_bytes()).await.unwrap();
                } else {
                    std::future::pending::<()>().await;
                }
            });
        }
    });
    (url, task, count)
}

async fn mock_delegation_ollama(
    completed: Vec<usize>,
) -> (String, tokio::task::JoinHandle<()>, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let requests = count.clone();
    let completed = Arc::new(completed);
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let served = requests.fetch_add(1, Ordering::SeqCst) + 1;
            let completed = completed.clone();
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buffer = [0u8; 8192];
                loop {
                    let read = stream.read(&mut buffer).await.unwrap();
                    if read == 0 {
                        return;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|part| part == b"\r\n\r\n") {
                        break;
                    }
                }
                if !completed.contains(&served) {
                    std::future::pending::<()>().await;
                }
                let body = "{\"message\":{\"role\":\"assistant\",\"content\":\"子任务完成\"},\"done\":false}\n{\"done\":true,\"prompt_eval_count\":2,\"eval_count\":2}\n";
                stream.write_all(format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()).as_bytes()).await.unwrap();
            });
        }
    });
    (url, task, count)
}

async fn mock_tool_spawn_ollama() -> (
    String,
    tokio::task::JoinHandle<()>,
    Arc<tokio::sync::Mutex<Vec<Value>>>,
) {
    mock_tool_catalog_ollama(false).await
}

async fn mock_tool_catalog_ollama(
    plan: bool,
) -> (
    String,
    tokio::task::JoinHandle<()>,
    Arc<tokio::sync::Mutex<Vec<Value>>>,
) {
    let first = if plan {
        "{\"message\":{\"role\":\"assistant\",\"content\":\"\",\"tool_calls\":[{\"function\":{\"name\":\"plan\",\"arguments\":{\"action\":\"set\",\"title\":\"恢复审计\",\"goal\":\"保持唯一事实源\",\"steps\":[{\"id\":\"a\",\"description\":\"验证持久恢复\"}]}}}]},\"done\":false}\n{\"done\":true}\n"
    } else {
        "{\"message\":{\"role\":\"assistant\",\"content\":\"\",\"tool_calls\":[{\"function\":{\"name\":\"spawn_subagent\",\"arguments\":{\"task\":\"核对一个问题\",\"context_source_ids\":[\"parent_input\"]}}}]},\"done\":false}\n{\"done\":true}\n"
    };
    mock_tool_response_ollama(first).await
}

async fn mock_tool_response_ollama(
    first: &'static str,
) -> (
    String,
    tokio::task::JoinHandle<()>,
    Arc<tokio::sync::Mutex<Vec<Value>>>,
) {
    mock_response_sequence_ollama(vec![first], "{\"message\":{\"role\":\"assistant\",\"content\":\"完成\"},\"done\":false}\n{\"done\":true}\n").await
}

async fn mock_response_sequence_ollama(
    responses: Vec<&'static str>,
    fallback: &'static str,
) -> (
    String,
    tokio::task::JoinHandle<()>,
    Arc<tokio::sync::Mutex<Vec<Value>>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let captures = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let captured = captures.clone();
    let responses = Arc::new(responses);
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let served = count.fetch_add(1, Ordering::SeqCst) + 1;
            let captured = captured.clone();
            let responses = responses.clone();
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buffer = [0u8; 8192];
                loop {
                    let read = stream.read(&mut buffer).await.unwrap();
                    if read == 0 {
                        return;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|part| part == b"\r\n\r\n") {
                        break;
                    }
                }
                let offset = request.windows(4).position(|s| s == b"\r\n\r\n").unwrap() + 4;
                let header = String::from_utf8_lossy(&request[..offset]);
                let length = header
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .unwrap()
                    .1
                    .trim()
                    .parse::<usize>()
                    .unwrap();
                while request.len() < offset + length {
                    let read = stream.read(&mut buffer).await.unwrap();
                    if read == 0 {
                        return;
                    }
                    request.extend_from_slice(&buffer[..read]);
                }
                captured
                    .lock()
                    .await
                    .push(serde_json::from_slice(&request[offset..offset + length]).unwrap());
                let body = responses.get(served - 1).copied().unwrap_or(fallback);
                stream.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()).as_bytes()).await.unwrap();
            });
        }
    });
    (url, task, captures)
}

async fn mock_child_outside_read_ollama(
    release: Arc<tokio::sync::Notify>,
) -> (String, tokio::task::JoinHandle<()>, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let requests = count.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let served = requests.fetch_add(1, Ordering::SeqCst) + 1;
            let release = release.clone();
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buffer = [0u8; 8192];
                loop {
                    let read = stream.read(&mut buffer).await.unwrap();
                    if read == 0 {
                        return;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|part| part == b"\r\n\r\n") {
                        break;
                    }
                }
                if served == 1 {
                    std::future::pending::<()>().await;
                }
                let body = if served == 2 {
                    release.notified().await;
                    "{\"message\":{\"role\":\"assistant\",\"content\":\"\",\"tool_calls\":[{\"function\":{\"name\":\"read_file\",\"arguments\":{\"path\":\"/etc/hosts\"}}}]},\"done\":false}\n{\"done\":true}\n"
                } else {
                    "{\"message\":{\"role\":\"assistant\",\"content\":\"已核对\"},\"done\":false}\n{\"done\":true}\n"
                };
                stream.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()).as_bytes()).await.unwrap();
            });
        }
    });
    (url, task, count)
}

async fn daemon(workspace: &Path, runtime_dir: &Path, url: &str) -> (Child, PathBuf) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_my-agent"));
    let hook_config = workspace.join(".my-agent/hooks.json");
    if hook_config.exists() {
        command.env("MY_AGENT_HOOKS_CONFIG", hook_config);
    } else {
        command.env_remove("MY_AGENT_HOOKS_CONFIG");
    }
    let mut child = command
        .arg("--workspace")
        .arg(workspace)
        .arg("daemon")
        .kill_on_drop(true)
        .env("MY_AGENT_RUNTIME_DIR", runtime_dir)
        .env("MY_AGENT_CONFIG", workspace.join("empty-config.json"))
        .env("API_TYPE", "ollama")
        .env("MODEL_NAME", "mock")
        .env("OPENAI_BASE_URL", url)
        .stdout(std::process::Stdio::null())
        .stderr(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(workspace.join("contract-daemon.stderr"))
                .unwrap(),
        )
        .spawn()
        .unwrap();
    for _ in 0..100 {
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "daemon 提前退出 {status}: {}",
                std::fs::read_to_string(workspace.join("contract-daemon.stderr")).unwrap()
            );
        }
        if let Ok(entries) = std::fs::read_dir(runtime_dir) {
            for entry in entries.flatten() {
                let socket = entry.path().join("daemon.sock");
                if socket.exists() && UnixStream::connect(&socket).await.is_ok() {
                    return (child, socket);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("daemon socket did not start");
}

async fn rpc(socket: &Path, id: &str, method: &str, params: Value) -> Value {
    let stream = UnixStream::connect(socket).await.unwrap();
    let (reader, mut writer) = stream.into_split();
    writer
        .write_all(
            format!(
                "{}\n",
                json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut lines = BufReader::new(reader).lines();
    loop {
        let line = lines.next_line().await.unwrap().unwrap();
        let frame: Value = serde_json::from_str(&line).unwrap();
        if frame.get("id").is_some() {
            return frame;
        }
    }
}

async fn chat(
    socket: &Path,
    id: &str,
    session: &str,
    message: &str,
    wait_terminal: bool,
) -> (String, Option<Value>) {
    let stream = UnixStream::connect(socket).await.unwrap();
    let (reader, mut writer) = stream.into_split();
    writer.write_all(format!("{}\n", json!({"jsonrpc":"2.0","id":id,"method":"chat.send","params":{"session_id":session,"message":message}})).as_bytes()).await.unwrap();
    let mut lines = BufReader::new(reader).lines();
    let mut run_id = None;
    loop {
        let line = lines.next_line().await.unwrap().unwrap();
        let frame: Value = serde_json::from_str(&line).unwrap();
        if let Some(id) = frame.get("run_id").and_then(Value::as_str) {
            run_id = Some(id.to_owned());
        }
        if frame.get("id").is_some() {
            return (
                run_id
                    .or_else(|| frame["result"]["run_id"].as_str().map(str::to_owned))
                    .unwrap_or_else(|| panic!("chat 缺少 run 身份：{frame}")),
                Some(frame),
            );
        }
        if !wait_terminal && frame["event"] == "turn_started" {
            return (run_id.unwrap(), None);
        }
    }
}

async fn send_and_disconnect(socket: &Path, id: &str, session: &str, message: &str) {
    let mut stream = UnixStream::connect(socket).await.unwrap();
    stream.write_all(format!("{}\n", json!({"jsonrpc":"2.0","id":id,"method":"chat.send","params":{"session_id":session,"message":message}})).as_bytes()).await.unwrap();
}

fn assert_snapshot_projection(text: &str, expected: &str) {
    fn find(value: &Value, plan: bool) -> Option<Value> {
        if value.get("snapshot_revision").is_some()
            && (if plan {
                value.get("session_key").is_some() && value.get("plan").is_some()
            } else {
                value.get("session_lifetime_id").is_some() && value.get("history_mode").is_some()
            })
        {
            return Some(value.clone());
        }
        match value {
            Value::String(text) => serde_json::from_str::<Value>(text)
                .ok()
                .and_then(|v| find(&v, plan)),
            Value::Object(map) => map.values().find_map(|v| find(v, plan)),
            Value::Array(values) => values.iter().find_map(|v| find(v, plan)),
            _ => None,
        }
    }
    let expected: Value = serde_json::from_str(expected).unwrap();
    if expected["replayability"] == "captured" {
        fn request(value: &Value) -> Option<Value> {
            if value["replayability"] == "captured" && value.get("request_digest").is_some() {
                return Some(value.clone());
            }
            match value {
                Value::String(text) => serde_json::from_str::<Value>(text)
                    .ok()
                    .and_then(|value| request(&value)),
                Value::Object(fields) => fields.values().find_map(request),
                Value::Array(items) => items.iter().find_map(request),
                _ => None,
            }
        }
        let actual = text
            .lines()
            .find_map(|line| {
                let start = line.find('{')?;
                let end = line.rfind('}')?;
                request(&serde_json::from_str::<Value>(&line[start..=end]).ok()?)
            })
            .expect("入口返回完整请求摘要");
        let _: agent_core::ProviderRequestReadback =
            serde_json::from_value(actual.clone()).unwrap();
        assert_eq!(actual, expected, "三入口 envelope 元数据逐字段一致");
        return;
    }
    if expected["kind"] == "compact" {
        fn compact(value: &Value) -> Option<Value> {
            if value["kind"] == "compact" && value.get("compact").is_some() {
                return Some(value.clone());
            }
            match value {
                Value::String(text) => serde_json::from_str::<Value>(text)
                    .ok()
                    .and_then(|value| compact(&value)),
                Value::Object(fields) => fields.values().find_map(compact),
                Value::Array(items) => items.iter().find_map(compact),
                _ => None,
            }
        }
        let actual = text
            .lines()
            .find_map(|line| {
                let start = line.find('{')?;
                let end = line.rfind('}')?;
                compact(&serde_json::from_str::<Value>(&line[start..=end]).ok()?)
            })
            .expect("入口返回完整 compact owner/source/terminal");
        agent_daemon_protocol::decode_run_readback(actual.clone()).unwrap();
        assert_eq!(
            actual, expected,
            "三入口 compact 和封存版本戳必须逐字段一致"
        );
        return;
    }
    if expected.get("outcomes").is_some() {
        fn find_hook(value: &Value) -> Option<Value> {
            if value.get("outcomes").is_some() {
                return Some(value.clone());
            }
            if let Some(content) = value.get("text").and_then(Value::as_str)
                && let Ok(value) = serde_json::from_str::<Value>(content)
                && let Some(found) = find_hook(&value)
            {
                return Some(found);
            }
            match value {
                Value::Object(fields) => fields.values().find_map(find_hook),
                Value::Array(items) => items.iter().find_map(find_hook),
                _ => None,
            }
        }
        let actual = text
            .lines()
            .find_map(|line| {
                let start = line.find('{')?;
                let end = line.rfind('}')?;
                let value: Value = serde_json::from_str(&line[start..=end]).ok()?;
                find_hook(&value)
            })
            .unwrap_or_else(|| panic!("入口未返回 HookReadback: {text}"));
        let _: agent_core::HookReadback = serde_json::from_value(actual.clone()).unwrap();
        for field in [
            "session_key",
            "session_lifetime_id",
            "outcomes",
            "cursor",
            "has_more",
        ] {
            assert_eq!(actual[field], expected[field]);
        }
        assert!(
            actual["snapshot_revision"].as_u64().unwrap()
                >= expected["snapshot_revision"].as_u64().unwrap()
        );
        return;
    }
    let actual = text
        .lines()
        .find_map(|line| {
            let start = line.find('{')?;
            let end = line.rfind('}')?;
            serde_json::from_str::<Value>(&line[start..=end])
                .ok()
                .and_then(|v| find(&v, expected.get("session_key").is_some()))
        })
        .unwrap_or_else(|| panic!("入口未返回 canonical snapshot: {text}"));
    if expected.get("session_key").is_some() {
        let _: agent_core::PlanReadback = serde_json::from_value(actual.clone()).unwrap();
        for field in ["session_key", "lifetime", "plan", "legacy_plan", "markdown"] {
            assert_eq!(
                actual[field], expected[field],
                "三入口 plan 的 {field} 不一致"
            );
        }
        assert!(
            actual["snapshot_revision"].as_u64().unwrap()
                >= expected["snapshot_revision"].as_u64().unwrap()
        );
        return;
    }
    agent_daemon_protocol::decode_session_readback(actual.clone()).unwrap();
    for field in [
        "session_id",
        "session_lifetime_id",
        "transcript_revision",
        "projection_generation",
        "active_owner",
        "queue_rows",
        "pending_interactions",
        "last_durable_terminal",
        "plan_digest",
    ] {
        assert_eq!(
            actual[field], expected[field],
            "三入口 snapshot 的 {field} 不一致"
        );
    }
    assert!(
        actual["snapshot_revision"].as_u64().unwrap()
            >= expected["snapshot_revision"].as_u64().unwrap()
    );
}

async fn entry_views(
    workspace: &Path,
    runtime_dir: &Path,
    url: &str,
    run_id: &str,
    expected_content: &str,
    subagent_parent: Option<&str>,
) {
    entry_views_with_decision(
        workspace,
        runtime_dir,
        url,
        run_id,
        expected_content,
        subagent_parent,
        None,
    )
    .await;
}

async fn entry_views_with_decision(
    workspace: &Path,
    runtime_dir: &Path,
    url: &str,
    run_id: &str,
    expected_content: &str,
    subagent_parent: Option<&str>,
    plan_command: Option<&str>,
) {
    let daemon_socket = std::fs::read_dir(runtime_dir)
        .unwrap()
        .flatten()
        .map(|e| e.path().join("daemon.sock"))
        .find(|p| p.exists())
        .unwrap();
    let read = rpc(
        &daemon_socket,
        "view-owner",
        "run.read",
        json!({"run_id":run_id}),
    )
    .await;
    let session_key = read["result"]["session_id"].as_str().unwrap();
    let mut facts = Vec::new();
    for command in ["memory", "context", "resources", "snapshot", "plan read"] {
        let line = format!("/{command} {session_key}");
        let result = rpc(
            &daemon_socket,
            "view-fact",
            "slash.execute",
            json!({"line":line}),
        )
        .await;
        let content = result["result"]["content"]
            .as_str()
            .unwrap_or_else(|| panic!("{result}"))
            .to_owned();
        facts.push((line, content));
    }
    let plan_fact: Value = serde_json::from_str(&facts.last().unwrap().1).unwrap();
    let lifetime = plan_fact["lifetime"].as_str().unwrap();
    let hook_line = format!("/hooks {session_key} {lifetime}");
    let hook_fact = rpc(
        &daemon_socket,
        "view-hooks",
        "hooks.readback",
        json!({"session_id":session_key,"expected_lifetime":lifetime}),
    )
    .await;
    assert!(hook_fact.get("error").is_none(), "{hook_fact}");
    facts.push((hook_line, hook_fact["result"].to_string()));
    let tools = rpc(
        &daemon_socket,
        "view-tools",
        "run.discovery",
        json!({"run_id":run_id}),
    )
    .await;
    assert!(tools.get("error").is_none(), "{tools}");
    facts.push((format!("/tools {run_id}"), tools["result"].to_string()));
    if let Some(line) = plan_command {
        let result = if line.starts_with("/plan execute ") {
            rpc(
                &daemon_socket,
                "decision",
                "chat.send",
                json!({"session_id":session_key,"message":line}),
            )
            .await
        } else {
            rpc(
                &daemon_socket,
                "decision",
                "slash.execute",
                json!({"line":line}),
            )
            .await
        };
        assert!(result.get("error").is_none(), "{result}");
        facts.push((
            line.to_owned(),
            result["result"]["content"].as_str().unwrap().to_owned(),
        ));
    }
    let mut cli = Command::new(env!("CARGO_BIN_EXE_my-agent"))
        .arg("--workspace")
        .arg(workspace)
        .arg("chat")
        .env("MY_AGENT_RUNTIME_DIR", runtime_dir)
        .env("MY_AGENT_CONFIG", workspace.join("empty-config.json"))
        .env("API_TYPE", "ollama")
        .env("MODEL_NAME", "mock")
        .env("OPENAI_BASE_URL", url)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    cli.stdin
        .take()
        .unwrap()
        .write_all(
            format!(
                "/run {run_id}\n{}{}/exit\n",
                subagent_parent.map_or(String::new(), |parent| format!(
                    "/subagent {parent} {run_id}\n"
                )),
                facts
                    .iter()
                    .map(|(line, _)| if line.starts_with("/plan execute ") {
                        format!("/resume {session_key}\n{line}\n")
                    } else {
                        format!("{line}\n")
                    })
                    .collect::<String>()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let output = cli.wait_with_output().await.unwrap();
    assert!(
        output.status.success(),
        "CLI: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let cli_text = String::from_utf8_lossy(&output.stdout);
    assert!(
        cli_text.contains(&format!("run_id={run_id}"))
            && cli_text.contains("status=completed")
            && cli_text.contains(expected_content)
            && subagent_parent.is_none_or(|_| cli_text.contains(&format!("child_run_id={run_id}"))),
        "{cli_text}"
    );

    for (_, fact) in &facts {
        if fact.contains("snapshot_revision") {
            assert_snapshot_projection(&cli_text, fact);
        } else {
            assert!(
                cli_text.contains(fact),
                "CLI 缺少 durable fact: {fact}; {cli_text}"
            );
        }
    }

    let mut acp = Command::new(env!("CARGO_BIN_EXE_my-agent"))
        .arg("--workspace")
        .arg(workspace)
        .arg("editor")
        .env("MY_AGENT_RUNTIME_DIR", runtime_dir)
        .env("MY_AGENT_CONFIG", workspace.join("empty-config.json"))
        .env("API_TYPE", "ollama")
        .env("MODEL_NAME", "mock")
        .env("OPENAI_BASE_URL", url)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut acp_in = acp.stdin.take().unwrap();
    let mut acp_out = BufReader::new(acp.stdout.take().unwrap()).lines();
    acp_in
        .write_all(
            format!(
                "{}\n",
                json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}})
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let init: Value = serde_json::from_str(&acp_out.next_line().await.unwrap().unwrap()).unwrap();
    assert!(init.get("result").is_some(), "ACP initialize: {init}");
    acp_in.write_all(format!("{}\n", json!({"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":workspace,"mcpServers":[]}})).as_bytes()).await.unwrap();
    let new_session: Value =
        serde_json::from_str(&acp_out.next_line().await.unwrap().unwrap()).unwrap();
    let acp_session = new_session["result"]["sessionId"]
        .as_str()
        .unwrap_or_else(|| panic!("ACP session id: {new_session}"));
    acp_in.write_all(format!("{}\n", json!({"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":acp_session,"prompt":[{"type":"text","text":format!("/run {run_id}")}]}})).as_bytes()).await.unwrap();
    let mut acp_text = String::new();
    loop {
        let line = acp_out
            .next_line()
            .await
            .unwrap()
            .expect("ACP prompt response");
        acp_text.push_str(&line);
        let value: Value = serde_json::from_str(&line).unwrap();
        if value["id"] == 3 {
            assert!(value.get("result").is_some(), "ACP prompt: {value}");
            break;
        }
    }
    assert!(
        acp_text.contains(&format!("run_id={run_id}"))
            && acp_text.contains("status=completed")
            && acp_text.contains(expected_content),
        "{acp_text}"
    );
    if let Some(parent) = subagent_parent {
        acp_in
            .write_all(
                format!(
                    "{}\n",
                    json!({"jsonrpc":"2.0","id":4,
            "method":"session/prompt","params":{"sessionId":acp_session,
            "prompt":[{"type":"text","text":format!("/subagent {parent} {run_id}")}]}})
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut child_text = String::new();
        loop {
            let line = acp_out.next_line().await.unwrap().unwrap();
            child_text.push_str(&line);
            if serde_json::from_str::<Value>(&line).unwrap()["id"] == 4 {
                break;
            }
        }
        assert!(
            child_text.contains(&format!("child_run_id={run_id}")),
            "{child_text}"
        );
    }
    for (index, (line, fact)) in facts.iter().enumerate() {
        let id = 10 + index;
        let target = if line.starts_with("/plan execute ") {
            let load_id = 1000 + index;
            acp_in.write_all(format!("{}\n",json!({"jsonrpc":"2.0","id":load_id,"method":"session/load","params":{"sessionId":session_key,"cwd":workspace,"mcpServers":[]}})).as_bytes()).await.unwrap();
            loop {
                let value: Value =
                    serde_json::from_str(&acp_out.next_line().await.unwrap().unwrap()).unwrap();
                if value["id"] == load_id {
                    assert!(value.get("error").is_none(), "{value}");
                    break;
                }
            }
            session_key
        } else {
            acp_session
        };
        acp_in.write_all(format!("{}\n",json!({"jsonrpc":"2.0","id":id,"method":"session/prompt","params":{"sessionId":target,"prompt":[{"type":"text","text":line}]}})).as_bytes()).await.unwrap();
        let mut text = String::new();
        loop {
            let line = acp_out.next_line().await.unwrap().unwrap();
            text.push_str(&line);
            text.push('\n');
            if serde_json::from_str::<Value>(&line).unwrap()["id"] == id {
                break;
            }
        }
        if fact.contains("snapshot_revision") {
            assert_snapshot_projection(&text, fact);
        } else {
            assert!(
                text.contains(&serde_json::to_string(fact).unwrap()),
                "ACP durable fact 不一致: {fact}; {text}"
            );
        }
    }
    acp.kill().await.unwrap();
    acp.wait().await.unwrap();

    let port_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = port_listener.local_addr().unwrap().port();
    drop(port_listener);
    let web_diagnostics = workspace.join("contract-web.stderr");
    let mut web = Command::new(env!("CARGO_BIN_EXE_my-agent"))
        .arg("--workspace")
        .arg(workspace)
        .arg("serve")
        .arg("--bind")
        .arg(format!("127.0.0.1:{port}"))
        .env("MY_AGENT_RUNTIME_DIR", runtime_dir)
        .env("MY_AGENT_CONFIG", workspace.join("empty-config.json"))
        .env("API_TYPE", "ollama")
        .env("MODEL_NAME", "mock")
        .env("OPENAI_BASE_URL", url)
        .stdout(std::process::Stdio::null())
        .stderr(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&web_diagnostics)
                .unwrap(),
        )
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut socket = ready_web_socket(&mut web, port, &web_diagnostics).await;
    socket
        .send(WsMessage::Text(
            json!({"type":"connect","workspace":workspace})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let connected = socket.next().await.unwrap().unwrap().into_text().unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&connected).unwrap()["type"],
        "connected"
    );
    socket
        .send(WsMessage::Text(
            json!({"jsonrpc":"2.0","id":"web-read","method":"run.read","params":{"run_id":run_id}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let web_result = loop {
        let text = socket.next().await.unwrap().unwrap().into_text().unwrap();
        let value: Value = serde_json::from_str(&text).unwrap();
        if value["id"] == "web-read" {
            break value;
        }
    };
    assert_eq!(web_result["result"]["run_id"], run_id);
    assert_eq!(web_result["result"]["status"], "completed");
    assert_eq!(web_result["result"]["content"], expected_content);
    let http = reqwest::Client::new();
    if let Some((_, fact)) = facts.iter().find(|(_, fact)| {
        serde_json::from_str::<Value>(fact)
            .ok()
            .is_some_and(|value| value["replayability"] == "captured")
    }) {
        let expected: Value = serde_json::from_str(fact).unwrap();
        let response=http.post(format!("http://127.0.0.1:{port}/api/context/readback")).json(&json!({"workspace":workspace,"params":{"session_id":session_key,"expected_lifetime":expected["owner"]["session_lifetime_id"],"capture_id":expected["capture_id"]}})).send().await.unwrap();
        assert!(response.status().is_success());
        let context: Value = response.json().await.unwrap();
        assert_snapshot_projection(&context.to_string(), fact);
    }
    let response = http
        .get(format!("http://127.0.0.1:{port}/api/plans/readback"))
        .query(&[
            ("session_id", session_key),
            ("workspace", workspace.to_str().unwrap()),
        ])
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "HTTP plan readback: {}",
        response.status()
    );
    let http_plan: Value = response.json().await.unwrap();
    let expected = facts
        .iter()
        .find(|(line, _)| line.starts_with("/plan read "))
        .unwrap();
    assert_snapshot_projection(&http_plan.to_string(), &expected.1);
    let http_hooks: Value = http
        .get(format!("http://127.0.0.1:{port}/api/hooks/readback"))
        .query(&[("session_id", session_key), ("expected_lifetime", lifetime)])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_snapshot_projection(&http_hooks.to_string(), &hook_fact["result"].to_string());
    if let Some(line) = plan_command {
        let words = line.split_whitespace().collect::<Vec<_>>();
        if words[0] == "/run" {
            let native = rpc(
                &daemon_socket,
                "compact-http-source",
                "run.read",
                json!({"run_id":words[1]}),
            )
            .await;
            let source = &native["result"]["compact"]["source"];
            let params = json!({"session_id":native["result"]["session_id"],"expected_lifetime":source["lifetime"],"operation_id":native["result"]["compact"]["operation_id"],"expected_revision":source["source_end"],"expected_projection_generation":source["generation"]});
            let response = http
                .post(format!("http://127.0.0.1:{port}/api/compact/start"))
                .json(&json!({"workspace":workspace,"params":params}))
                .send()
                .await
                .unwrap();
            if native["result"]["compact"]["operation_id"] == "crash-compact" {
                assert!(response.status().is_success());
                let result: Value = response.json().await.unwrap();
                assert_eq!(result["run_id"], words[1]);
                assert_eq!(result["compact"], native["result"]["compact"]);
            } else {
                assert!(
                    !response.status().is_success(),
                    "不能省略旧 unary 来源身份冒领 receipt"
                );
            }
        } else if words[1] == "discard" {
            let params = json!({"session_id":words[2],"expected_lifetime":words[3],"identity":{"plan_id":words[4],"revision":words[5].parse::<u64>().unwrap(),"content_digest":words[6],"operation_id":words[7]}});
            let response = http
                .post(format!("http://127.0.0.1:{port}/api/plans/discard"))
                .json(&json!({"workspace":workspace,"params":params}))
                .send()
                .await
                .unwrap();
            assert!(
                response.status().is_success(),
                "HTTP discard {}",
                response.status()
            );
            let receipt: Value = response.json().await.unwrap();
            let expected: Value = serde_json::from_str(&facts.last().unwrap().1).unwrap();
            assert_eq!(receipt, expected);
            let mut invalid = params;
            invalid["identity"]["unknown_lifetime"] = json!("must reject");
            let response = http
                .post(format!("http://127.0.0.1:{port}/api/plans/discard"))
                .json(&json!({"workspace":workspace,"params":invalid}))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
        } else {
            let params = agent_daemon_protocol::normalize_request(
                agent_daemon_protocol::JsonRpcRequest::new(
                    agent_core::RequestId::Number(1),
                    "chat.send",
                    json!({"session_id":session_key,"message":line}),
                ),
            )
            .unwrap()
            .params;
            let response = http
                .post(format!("http://127.0.0.1:{port}/api/plans/execute"))
                .json(&json!({"workspace":workspace,"params":params}))
                .send()
                .await
                .unwrap();
            assert!(
                response.status().is_success(),
                "HTTP execute {}",
                response.status()
            );
            let result: Value = response.json().await.unwrap();
            assert_eq!(result["content"], expected_content);
            assert_eq!(result["run_id"], run_id);
        }
    }
    if let Some(parent) = subagent_parent {
        socket
            .send(WsMessage::Text(
                json!({"jsonrpc":"2.0","id":"web-child",
            "method":"read_subagent","params":{"parent_run_id":parent,"child_run_id":run_id}})
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        let child_view = loop {
            let text = socket.next().await.unwrap().unwrap().into_text().unwrap();
            let value: Value = serde_json::from_str(&text).unwrap();
            if value["id"] == "web-child" {
                break value;
            }
        };
        assert_eq!(
            child_view["result"]["child"]["child_run_id"], run_id,
            "{child_view}"
        );
    }
    for (index, (line, fact)) in facts.iter().enumerate() {
        let id = format!("web-fact-{index}");
        socket
            .send(WsMessage::Text(
                if line.starts_with("/plan execute ") { json!({"jsonrpc":"2.0","id":id,"method":"chat.send","params":{"session_id":session_key,"message":line}}) } else { json!({"jsonrpc":"2.0","id":id,"method":"slash.execute","params":{"line":line}}) }
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        loop {
            let text = socket.next().await.unwrap().unwrap().into_text().unwrap();
            let value: Value = serde_json::from_str(&text).unwrap();
            if value["id"] == id {
                if fact.contains("snapshot_revision") {
                    assert_snapshot_projection(value["result"]["content"].as_str().unwrap(), fact);
                } else {
                    assert_eq!(
                        value["result"]["content"], *fact,
                        "WebSocket durable fact 不一致"
                    );
                }
                break;
            }
        }
    }
    web.kill().await.unwrap();
    web.wait().await.unwrap();
}

#[tokio::test]
async fn committed_terminal_survives_real_daemon_restart_and_uncertain_run_is_not_replayed() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let workspace = temp_workspace();
        let runtime_dir = PathBuf::from(format!(
            "/tmp/ma-runtime-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let (url, server, provider_requests) = mock_ollama().await;
        let (mut child, socket) = daemon(&workspace, &runtime_dir, &url).await;
        let session = rpc(&socket, "new", "session.new", json!({})).await["result"]["session_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let (complete_id, response) = chat(&socket, "complete", &session, "say done", true).await;
        assert_eq!(response.unwrap()["result"]["content"], "已完成");
        let (uncertain_id, _) = chat(&socket, "uncertain", &session, "hold request", false).await;
        for _ in 0..100 {
            if provider_requests.load(Ordering::SeqCst) >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(provider_requests.load(Ordering::SeqCst), 2);
        child.kill().await.unwrap();
        child.wait().await.unwrap();
        let (mut restarted, socket) = daemon(&workspace, &runtime_dir, &url).await;
        let client = agent_daemon_client::DaemonClient::connect_unix(&socket)
            .await
            .unwrap();
        let client = client.reconnect().await.unwrap();
        let typed_run = client
            .read_run(&agent_core::RunId(complete_id.clone()))
            .await
            .unwrap();
        assert_eq!(typed_run.status, agent_core::RunStatus::Completed);
        assert_eq!(typed_run.content.as_deref(), Some("已完成"));
        let mut cursor = agent_core::EventSeq(0);
        let mut found_terminal = false;
        loop {
            let page = client
                .read_events(&typed_run.run_id, cursor, 2)
                .await
                .unwrap();
            found_terminal |= page.events.iter().any(|event| event.event == "terminal");
            assert!(page.events.len() <= 2);
            cursor = page.cursor;
            if !page.has_more {
                break;
            }
        }
        assert_eq!(cursor, typed_run.last_seq);
        assert!(found_terminal);
        let rejected = client
            .request_result(
                "runs.send",
                json!({"session_id":session,"message":"拒绝未知字段","unknown":true}),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(rejected, agent_daemon_client::ClientError::Rpc(error) if error.code == -32602)
        );
        assert_eq!(provider_requests.load(Ordering::SeqCst), 2);
        let complete = rpc(
            &socket,
            "read-complete",
            "run.read",
            json!({"run_id":complete_id}),
        )
        .await;
        assert_eq!(complete["result"]["status"], "completed");
        assert_eq!(complete["result"]["content"], "已完成");
        let provider_ledger = rpc(
            &socket,
            "provider-ledger",
            "run.provider_attempts",
            json!({"run_id":complete_id}),
        )
        .await;
        assert_eq!(
            provider_ledger["result"]["route"]["candidates"][0]["model"],
            "mock"
        );
        assert_eq!(
            provider_ledger["result"]["attempts"][0]["status"],
            "succeeded"
        );
        assert_eq!(provider_ledger["result"]["usage"]["input_tokens"], 2);
        assert_eq!(provider_ledger["result"]["usage"]["output_tokens"], 2);
        assert!(!provider_ledger.to_string().contains("api_key"));
        let unknown = rpc(
            &socket,
            "read-unknown",
            "run.read",
            json!({"run_id":uncertain_id}),
        )
        .await;
        assert_eq!(unknown["result"]["status"], "unknown_after_restart");
        assert_eq!(
            provider_requests.load(Ordering::SeqCst),
            2,
            "未知 run 不能自动重放 Provider 请求"
        );
        let events = rpc(
            &socket,
            "events",
            "run.events",
            json!({"run_id":complete_id,"after_seq":0}),
        )
        .await;
        assert!(
            events["result"]["events"]
                .as_array()
                .unwrap()
                .iter()
                .any(|event| event["event"] == "terminal")
        );
        let duplicate = rpc(
            &socket,
            "complete",
            "chat.send",
            json!({"session_id":session,"message":"say done"}),
        )
        .await;
        assert_eq!(duplicate["result"]["run_id"], complete_id);
        entry_views(&workspace, &runtime_dir, &url, &complete_id, "已完成", None).await;
        restarted.kill().await.unwrap();
        restarted.wait().await.unwrap();
        server.abort();
        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(runtime_dir);
    })
    .await
    .expect("restart contract timed out");
}

#[tokio::test]
async fn daemon_queue_is_durable_and_exact_cancel_does_not_hit_running_run() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let workspace = temp_workspace();
        let runtime_dir = PathBuf::from(format!("/tmp/ma-queue-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        let (url, server, provider_requests) = mock_ollama().await;
        let (mut child, socket) = daemon(&workspace, &runtime_dir, &url).await;
        let session = rpc(&socket, "new", "session.new", json!({})).await["result"]["session_id"].as_str().unwrap().to_owned();
        let _ = chat(&socket, "first", &session, "finish", true).await;
        let (running, _) = chat(&socket, "running", &session, "wait", false).await;
        for _ in 0..100 {
            if provider_requests.load(Ordering::SeqCst) >= 2 { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        send_and_disconnect(&socket, "queued", &session, "queued input").await;
        let queued = loop {
            let listed = rpc(&socket, "list", "queue.list", json!({"session_id":session})).await;
            if let Some(item) = listed["result"]["items"].as_array().and_then(|items| items.first()) { break item.clone(); }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let queued_run = queued["run_id"].as_str().unwrap().to_owned();
        assert_eq!(queued["position"], 1);
        assert_eq!(provider_requests.load(Ordering::SeqCst), 2);
        let duplicate = rpc(&socket, "queued", "chat.send", json!({"session_id":session,"message":"queued input"})).await;
        assert_eq!(duplicate["result"]["run_id"], queued_run);
        let rejected = rpc(&socket, "reject", "chat.send", json!({"session_id":session,"message":"no","admission_mode":"reject_if_busy"})).await;
        assert!(rejected.get("error").is_some());
        let cancelled = rpc(&socket, "cancel", "agent.cancel", json!({"session_id":session,"run_id":queued_run})).await;
        assert_eq!(cancelled["result"]["cancelled"], true);
        let read = rpc(&socket, "read", "run.read", json!({"run_id":queued_run})).await;
        assert_eq!(read["result"]["status"], "cancelled");
        let active = rpc(&socket, "active", "run.read", json!({"run_id":running})).await;
        assert_eq!(active["result"]["status"], "running");
        assert_eq!(provider_requests.load(Ordering::SeqCst), 2);
        let port_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = port_listener.local_addr().unwrap().port();
        drop(port_listener);
        let web_diagnostics = workspace.join("contract-web.stderr");
    let mut web = Command::new(env!("CARGO_BIN_EXE_my-agent"))
            .arg("--workspace").arg(&workspace).arg("serve").arg("--bind").arg(format!("127.0.0.1:{port}"))
            .env("MY_AGENT_RUNTIME_DIR", &runtime_dir)
            .env("MY_AGENT_CONFIG", workspace.join("empty-config.json"))
            .env("API_TYPE", "ollama").env("MODEL_NAME", "mock").env("OPENAI_BASE_URL", &url)
            .stdout(std::process::Stdio::null()).stderr(std::fs::OpenOptions::new().create(true).append(true).open(&web_diagnostics).unwrap()).kill_on_drop(true).spawn().unwrap();
        let mut web_socket = ready_web_socket(&mut web, port, &web_diagnostics).await;
        web_socket.send(WsMessage::Text(json!({"type":"connect","workspace":workspace}).to_string().into())).await.unwrap();
        let _ = web_socket.next().await.unwrap().unwrap();
        web_socket.send(WsMessage::Text(json!({"jsonrpc":"2.0","id":"web-survivor","method":"chat.send",
            "params":{"session_id":session,"message":"resume after crash"}}).to_string().into())).await.unwrap();
        let survivor = loop {
            let listed = rpc(&socket, "list-survivor", "queue.list", json!({"session_id":session})).await;
            if let Some(item) = listed["result"]["items"].as_array().and_then(|items| items.first()) { break item["run_id"].as_str().unwrap().to_owned(); }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        drop(web_socket);
        web.kill().await.unwrap(); web.wait().await.unwrap();
        child.kill().await.unwrap(); child.wait().await.unwrap();
        let (mut restarted, socket) = daemon(&workspace, &runtime_dir, &url).await;
        for _ in 0..100 {
            let run = rpc(&socket, "survivor-read", "run.read", json!({"run_id":survivor})).await;
            if run["result"]["status"] == "running" { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(rpc(&socket, "survivor-read-final", "run.read", json!({"run_id":survivor})).await["result"]["status"], "running");
        assert_eq!(rpc(&socket, "old-read", "run.read", json!({"run_id":running})).await["result"]["status"], "unknown_after_restart");
        assert_eq!(provider_requests.load(Ordering::SeqCst), 3);
        restarted.kill().await.unwrap(); restarted.wait().await.unwrap(); server.abort();
        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(runtime_dir);
    }).await.expect("queue contract timed out");
}

#[tokio::test]
async fn durable_subagent_spawn_wait_restart_and_scoped_cancel() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let workspace = temp_workspace();
        install_observer_hooks(&workspace, &[agent_core::HookEvent::SubagentStart, agent_core::HookEvent::SubagentEnd]);
        let runtime_dir = PathBuf::from(format!("/tmp/ma-child-{}-{}", std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)));
        let (url, server, provider_requests) = mock_delegation_ollama(vec![2]).await;
        let (mut daemon_process, socket) = daemon(&workspace, &runtime_dir, &url).await;
        let session = rpc(&socket, "new", "session.new", json!({})).await["result"]["session_id"]
            .as_str().unwrap().to_owned();
        let (parent, _) = chat(&socket, "parent", &session, "等待", false).await;
        for _ in 0..100 {
            if provider_requests.load(Ordering::SeqCst) >= 1 { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let spawned = rpc(&socket, "spawn", "spawn_subagent", json!({
            "parent_session_id":session,"parent_run_id":parent,"spawn_key":"first",
            "task":"调查一个问题"})).await;
        assert!(spawned.get("result").is_some(), "{spawned}");
        let child = spawned["result"]["child"]["child_run_id"].as_str().unwrap().to_owned();
        let child_session = spawned["result"]["child"]["child_session_id"].as_str().unwrap().to_owned();
        assert_ne!(child_session, session);
        let timed_out = rpc(&socket, "wait-short", "wait_subagents", json!({
            "parent_run_id":parent,"child_run_ids":[child],"timeout_ms":0})).await;
        assert_eq!(timed_out["result"]["timed_out"], true, "{timed_out}");
        let checkpoint = rpc(&socket, "checkpoint", "wait_subagents", json!({
            "parent_run_id":parent,"child_run_ids":[child],"after_seq":0,"timeout_ms":1000})).await;
        assert_eq!(checkpoint["result"]["checkpoints"][0]["child_run_id"], child);
        let completed = rpc(&socket, "wait", "wait_subagents", json!({
            "parent_run_id":parent,"child_run_ids":[child],"timeout_ms":5000})).await;
        assert_eq!(completed["result"]["children"][0]["status"], "completed", "{completed}");
        assert_eq!(completed["result"]["children"][0]["content"], "子任务完成");
        let child_readback=rpc(&socket,"child-meta","sessions.read",json!({"session_id":child_session})).await;
        let child_hooks=loop {
            let read=rpc(&socket,"child-hooks","hooks.readback",json!({"session_id":child_session,"expected_lifetime":child_readback["result"]["session_lifetime_id"]})).await;
            if read["result"]["outcomes"].as_array().is_some_and(|outcomes|outcomes.len()==2 && outcomes.iter().all(|record|record["status"]=="succeeded")) {break read;}
            tokio::task::yield_now().await;
        };
        assert_eq!(child_hooks["result"]["outcomes"][0]["payload"]["event"],"subagent_start");
        assert_eq!(child_hooks["result"]["outcomes"][1]["payload"]["event"],"subagent_end");
        assert!(child_hooks["result"]["outcomes"].as_array().unwrap().iter().all(|record|record["payload"]["owner"]["run_id"]==child && record["payload"]["channel"]=="subagent"));
        assert_eq!(rpc(&socket, "parent-read", "run.read", json!({"run_id":parent})).await["result"]["status"], "running");
        let duplicate = rpc(&socket, "spawn-retry", "spawn_subagent", json!({
            "parent_session_id":session,"parent_run_id":parent,"spawn_key":"first",
            "task":"调查一个问题"})).await;
        assert_eq!(duplicate["result"]["child"]["child_run_id"], child, "{duplicate}");
        let stolen = rpc(&socket, "wrong-scope", "cancel_subagent", json!({
            "parent_run_id":"run-does-not-own","child_run_id":child})).await;
        assert!(stolen.get("error").is_some());
        let elevated_tool = rpc(&socket, "elevate-tool", "spawn_subagent", json!({
            "parent_session_id":session,"parent_run_id":parent,"spawn_key":"bad-tool",
            "task":"越权","tools":["exec"]})).await;
        assert!(elevated_tool.get("error").is_some());
        let elevated_tokens = rpc(&socket, "elevate-budget", "spawn_subagent", json!({
            "parent_session_id":session,"parent_run_id":parent,"spawn_key":"bad-budget",
            "task":"越权","max_tokens":500000})).await;
        assert!(elevated_tokens.get("error").is_some());
        let reserved = rpc(&socket, "reserve", "subagent.result.reserve", json!({
            "parent_run_id":parent,"child_run_id":child,"owner":"reader-a","revision":0})).await;
        assert_eq!(reserved["result"]["child"]["result_state"], "reserved", "{reserved}");
        let reserved_again = rpc(&socket, "reserve-again", "subagent.result.reserve", json!({
            "parent_run_id":parent,"child_run_id":child,"owner":"reader-a","revision":0})).await;
        assert_eq!(reserved_again["result"]["child"]["revision"], 1);
        let conflicted = rpc(&socket, "reserve-conflict", "subagent.result.reserve", json!({
            "parent_run_id":parent,"child_run_id":child,"owner":"reader-b","revision":1})).await;
        assert!(conflicted.get("error").is_some());
        let released = rpc(&socket, "release", "subagent.result.release", json!({
            "parent_run_id":parent,"child_run_id":child,"owner":"reader-a","revision":1})).await;
        assert_eq!(released["result"]["child"]["result_state"], "unconsumed");
        let reserved_b = rpc(&socket, "reserve-b", "subagent.result.reserve", json!({
            "parent_run_id":parent,"child_run_id":child,"owner":"reader-b","revision":2})).await;
        assert_eq!(reserved_b["result"]["child"]["revision"], 3);
        let delivered = rpc(&socket, "commit", "subagent.result.commit", json!({
            "parent_run_id":parent,"child_run_id":child,"owner":"reader-b","revision":3})).await;
        assert_eq!(delivered["result"]["child"]["result_state"], "delivered");
        let second = rpc(&socket, "spawn-second", "spawn_subagent", json!({
            "parent_session_id":session,"parent_run_id":parent,"spawn_key":"second",
            "task":"持续调查"})).await;
        assert!(second.get("result").is_some(), "{second}");
        let second_id = second["result"]["child"]["child_run_id"].as_str().unwrap().to_owned();
        for _ in 0..100 {
            if provider_requests.load(Ordering::SeqCst) >= 3 { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let other_session = rpc(&socket, "new-other", "session.new", json!({})).await
            ["result"]["session_id"].as_str().unwrap().to_owned();
        let (unrelated, _) = chat(&socket, "unrelated", &other_session, "另一个根任务", false).await;
        for _ in 0..100 {
            if provider_requests.load(Ordering::SeqCst) >= 4 { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let cancelled = rpc(&socket, "cancel-root", "agent.cancel", json!({
            "session_id":session,"run_id":parent})).await;
        assert_eq!(cancelled["result"]["cancelled"], true, "{cancelled}");
        for _ in 0..100 {
            let status = rpc(&socket, "second-read", "run.read", json!({"run_id":second_id})).await;
            if status["result"]["status"] == "cancelled" { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(rpc(&socket, "second-final", "run.read", json!({"run_id":second_id})).await["result"]["status"], "cancelled");
        assert_eq!(rpc(&socket, "unrelated-read", "run.read", json!({"run_id":unrelated})).await["result"]["status"], "running");
        let late_spawn = rpc(&socket, "late-spawn", "spawn_subagent", json!({
            "parent_session_id":session,"parent_run_id":parent,"spawn_key":"after-cancel",
            "task":"不能再创建"})).await;
        assert!(late_spawn.get("error").is_some(), "{late_spawn}");
        daemon_process.kill().await.unwrap(); daemon_process.wait().await.unwrap();
        let (mut restarted, socket) = daemon(&workspace, &runtime_dir, &url).await;
        let listed = rpc(&socket, "list", "list_subagents", json!({"root_run_id":parent})).await;
        assert_eq!(listed["result"]["children"][0]["child_run_id"], child);
        assert_eq!(listed["result"]["children"][0]["status"], "completed");
        assert_eq!(listed["result"]["children"][0]["result_state"], "delivered");
        assert_eq!(rpc(&socket, "second-restarted", "run.read", json!({"run_id":second_id})).await["result"]["status"], "cancelled");
        assert_eq!(rpc(&socket, "unrelated-unknown", "run.read", json!({"run_id":unrelated})).await["result"]["status"], "unknown_after_restart");
        assert_eq!(provider_requests.load(Ordering::SeqCst), 4);
        entry_views(&workspace, &runtime_dir, &url, &child, "子任务完成", Some(&parent)).await;
        restarted.kill().await.unwrap(); restarted.wait().await.unwrap(); server.abort();
        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(runtime_dir);
    }).await.expect("subagent contract timed out");
}

#[tokio::test]
async fn running_subagent_becomes_unknown_after_daemon_crash_without_replay() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let workspace = temp_workspace();
        let runtime_dir = PathBuf::from(format!(
            "/tmp/ma-child-crash-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let (url, server, requests) = mock_delegation_ollama(vec![2]).await;
        let (mut daemon_process, socket) = daemon(&workspace, &runtime_dir, &url).await;
        let session = rpc(&socket, "new", "session.new", json!({})).await["result"]["session_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let (parent, _) = chat(&socket, "parent", &session, "等待", false).await;
        for _ in 0..100 {
            if requests.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let first = rpc(
            &socket,
            "first",
            "spawn_subagent",
            json!({
            "parent_session_id":session,"parent_run_id":parent,"spawn_key":"first",
            "task":"完成"}),
        )
        .await;
        let first_id = first["result"]["child"]["child_run_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let _ = rpc(
            &socket,
            "wait-first",
            "wait_subagents",
            json!({
            "parent_run_id":parent,"child_run_ids":[first_id],"timeout_ms":5000}),
        )
        .await;
        let second = rpc(
            &socket,
            "second",
            "spawn_subagent",
            json!({
            "parent_session_id":session,"parent_run_id":parent,"spawn_key":"second",
            "task":"保持运行"}),
        )
        .await;
        let second_id = second["result"]["child"]["child_run_id"]
            .as_str()
            .unwrap()
            .to_owned();
        for _ in 0..100 {
            if requests.load(Ordering::SeqCst) >= 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(requests.load(Ordering::SeqCst), 3);
        daemon_process.kill().await.unwrap();
        daemon_process.wait().await.unwrap();
        let (mut restarted, socket) = daemon(&workspace, &runtime_dir, &url).await;
        let unknown = rpc(
            &socket,
            "read",
            "read_subagent",
            json!({
            "parent_run_id":parent,"child_run_id":second_id}),
        )
        .await;
        assert_eq!(
            unknown["result"]["child"]["status"], "unknown_after_restart",
            "{unknown}"
        );
        assert_eq!(unknown["result"]["child"]["result_state"], "unconsumed");
        assert_eq!(requests.load(Ordering::SeqCst), 3);
        restarted.kill().await.unwrap();
        restarted.wait().await.unwrap();
        server.abort();
        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(runtime_dir);
    })
    .await
    .expect("subagent crash contract timed out");
}

#[tokio::test]
async fn model_spawn_tool_uses_durable_delegation_and_receipt() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let workspace = temp_workspace();
        let runtime_dir = PathBuf::from(format!(
            "/tmp/ma-child-tool-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let (url, server, captures) = mock_tool_spawn_ollama().await;
        let (mut daemon_process, socket) = daemon(&workspace, &runtime_dir, &url).await;
        let session = rpc(&socket, "new", "session.new", json!({})).await["result"]["session_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let (parent, response) = chat(&socket, "parent", &session, "派出子任务\nAPI_KEY=sk-parent-private", true).await;
        assert!(response.unwrap().get("result").is_some());
        let tools = rpc(&socket, "tools", "run.tools", json!({"run_id":parent})).await;
        assert_eq!(
            tools["result"]["receipts"][0]["name"], "spawn_subagent",
            "{tools}"
        );
        let listed = rpc(
            &socket,
            "list",
            "list_subagents",
            json!({"root_run_id":parent}),
        )
        .await;
        assert_eq!(
            listed["result"]["children"].as_array().unwrap().len(),
            1,
            "{listed}"
        );
        let child = listed["result"]["children"][0]["child_run_id"]
            .as_str()
            .unwrap();
        let waited = rpc(
            &socket,
            "wait",
            "wait_subagents",
            json!({
            "parent_run_id":parent,"child_run_ids":[child],"timeout_ms":5000}),
        )
        .await;
        assert_eq!(
            waited["result"]["children"][0]["status"], "completed",
            "{waited}"
        );
        let material=captures.lock().await.iter().find(|request|request["messages"].as_array().unwrap().iter().any(|m|m["role"]=="user" && m["content"]=="核对一个问题")).cloned().expect("应捕获真实child请求");
        let messages=material["messages"].as_array().unwrap();
        let inherited=messages.iter().find(|m|m["content"].as_str().is_some_and(|t|t.starts_with("[retrieved_delegation]"))).expect("child应收到持久交接包");
        let inherited_text=inherited["content"].as_str().unwrap();
        assert!(inherited_text.contains("派出子任务") && inherited_text.contains("凭据行已过滤"));
        assert!(!serde_json::to_string(&material).unwrap().contains("sk-parent-private"));
        assert_eq!(material["tools"].as_array().unwrap().len(),1);
        assert_eq!(material["tools"][0]["function"]["name"],"read_file");
        let fact=rpc(&socket,"captured","run.read",json!({"run_id":child})).await;
        let captured=fact["result"]["snapshot"]["delegation_context"].clone();
        assert_eq!(captured["version"],1);assert_eq!(captured["parent"]["run_id"],parent);
        assert_eq!(captured["sources"].as_array().unwrap().len(),1);
        daemon_process.kill().await.unwrap();
        daemon_process.wait().await.unwrap();
        let (mut restarted,socket)=daemon(&workspace,&runtime_dir,&url).await;
        let fact=rpc(&socket,"captured-restart","run.read",json!({"run_id":child})).await;
        assert_eq!(fact["result"]["snapshot"]["delegation_context"],captured);
        let replay=rpc(&socket,"replay","spawn_subagent",json!({"parent_session_id":session,"parent_run_id":parent,"spawn_key":listed["result"]["children"][0]["spawn_key"],"task":"核对一个问题","context_source_ids":["parent_input"]})).await;
        assert_eq!(replay["result"]["child"]["child_run_id"],child,"{replay}");
        let conflict=rpc(&socket,"replay-conflict","spawn_subagent",json!({"parent_session_id":session,"parent_run_id":parent,"spawn_key":listed["result"]["children"][0]["spawn_key"],"task":"核对一个问题","context_source_ids":[]})).await;
        assert!(conflict.get("error").is_some());
        restarted.kill().await.unwrap();restarted.wait().await.unwrap();
        server.abort();
        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(runtime_dir);
    })
    .await
    .expect("model delegation tool contract timed out");
}

#[tokio::test]
async fn concurrent_children_finish_and_remain_addressable_by_stable_ids() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let workspace = temp_workspace();
        let runtime_dir = PathBuf::from(format!(
            "/tmp/ma-child-parallel-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let (url, server, requests) = mock_delegation_ollama(vec![2, 3]).await;
        let (mut daemon_process, socket) = daemon(&workspace, &runtime_dir, &url).await;
        let session = rpc(&socket, "new", "session.new", json!({})).await["result"]["session_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let (parent, _) = chat(&socket, "parent", &session, "等待", false).await;
        for _ in 0..100 {
            if requests.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let first = rpc(
            &socket,
            "first",
            "spawn_subagent",
            json!({
            "parent_session_id":session,"parent_run_id":parent,"spawn_key":"a",
            "task":"任务 A"}),
        )
        .await;
        let second = rpc(
            &socket,
            "second",
            "spawn_subagent",
            json!({
            "parent_session_id":session,"parent_run_id":parent,"spawn_key":"b",
            "task":"任务 B"}),
        )
        .await;
        let a = first["result"]["child"]["child_run_id"].as_str().unwrap();
        let b = second["result"]["child"]["child_run_id"].as_str().unwrap();
        assert_ne!(a, b);
        let waited = rpc(
            &socket,
            "wait",
            "wait_subagents",
            json!({
            "parent_run_id":parent,"child_run_ids":[a,b],"timeout_ms":5000}),
        )
        .await;
        assert_eq!(waited["result"]["children"][0]["child_run_id"], a);
        assert_eq!(waited["result"]["children"][1]["child_run_id"], b);
        for id in [a, b] {
            for _ in 0..100 {
                let child = rpc(
                    &socket,
                    "read",
                    "read_subagent",
                    json!({
                    "parent_run_id":parent,"child_run_id":id}),
                )
                .await;
                if child["result"]["child"]["status"] == "completed" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let child = rpc(
                &socket,
                "read-final",
                "read_subagent",
                json!({
                "parent_run_id":parent,"child_run_id":id}),
            )
            .await;
            assert_eq!(child["result"]["child"]["content"], "子任务完成");
        }
        assert_eq!(requests.load(Ordering::SeqCst), 3);
        daemon_process.kill().await.unwrap();
        daemon_process.wait().await.unwrap();
        server.abort();
        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(runtime_dir);
    })
    .await
    .expect("parallel delegation contract timed out");
}

#[tokio::test]
async fn child_read_permission_stays_frozen_after_parent_mode_changes() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let workspace = temp_workspace();
        let runtime_dir = PathBuf::from(format!(
            "/tmp/ma-child-mode-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let release = Arc::new(tokio::sync::Notify::new());
        let (url, server, requests) = mock_child_outside_read_ollama(release.clone()).await;
        let (mut daemon_process, socket) = daemon(&workspace, &runtime_dir, &url).await;
        let session = rpc(&socket, "new", "session.new", json!({})).await["result"]["session_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let (parent, _) = chat(&socket, "parent", &session, "等待", false).await;
        for _ in 0..100 {
            if requests.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let spawned = rpc(
            &socket,
            "spawn",
            "spawn_subagent",
            json!({
            "parent_session_id":session,"parent_run_id":parent,"spawn_key":"frozen",
            "task":"核对文件"}),
        )
        .await;
        assert!(spawned.get("result").is_some(), "{spawned}");
        let child = spawned["result"]["child_run_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let changed = rpc(&socket, "mode", "permissions.set", json!({"mode":"full"})).await;
        assert!(changed.get("result").is_some(), "{changed}");
        release.notify_one();
        let completed = rpc(
            &socket,
            "wait",
            "wait_subagents",
            json!({
            "parent_run_id":parent,"child_run_ids":[child],"timeout_ms":5000}),
        )
        .await;
        assert_eq!(
            completed["result"]["children"][0]["status"], "completed",
            "{completed}"
        );
        let receipts = rpc(&socket, "tools", "run.tools", json!({"run_id":child})).await;
        assert_eq!(
            receipts["result"]["receipts"][0]["name"], "read_file",
            "{receipts}"
        );
        assert_eq!(
            receipts["result"]["receipts"][0]["outcome"], "not_executed",
            "{receipts}"
        );
        assert!(
            receipts["result"]["receipts"][0]["started_at_ms"].is_null(),
            "预检拒绝不能启动工具"
        );
        daemon_process.kill().await.unwrap();
        daemon_process.wait().await.unwrap();
        server.abort();
        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(runtime_dir);
    })
    .await
    .expect("child permission contract timed out");
}

#[tokio::test]
async fn session_delete_recreate_and_fork_survive_restart_without_old_history_or_dual_write() {
    tokio::time::timeout(Duration::from_secs(30),async {
        let workspace=temp_workspace();let runtime_dir=std::env::temp_dir().join(format!("ma-life-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
        let (url,server,count)=mock_delegation_ollama(vec![1,2]).await;
        let (mut process,socket)=daemon(&workspace,&runtime_dir,&url).await;
        let key="session-lifetime-test.jsonl";
        let created=rpc(&socket,"create","sessions.create",json!({"session_id":key,"operation_id":"create-original"})).await;
        assert_eq!(created["result"]["session_id"],key);
        let (_,response)=chat(&socket,"same-request",key,"旧输入",true).await;assert!(response.unwrap().get("error").is_none());
        let before=rpc(&socket,"before","sessions.read",json!({"session_id":key})).await;
        assert_eq!(before["result"]["transcript_revision"],2);
        let original_lifetime = before["result"]["session_lifetime_id"].as_str().unwrap().to_owned();
        let fork=rpc(&socket,"fork","sessions.fork",json!({"session_id":key,"target_session_id":"session-forked-test.jsonl","operation_id":"fork-original","expected_revision":2,"expected_lifetime":original_lifetime})).await;assert_eq!(fork["result"]["forked"],true);
        let deleted=rpc(&socket,"delete","sessions.delete",json!({"session_id":key,"operation_id":"delete-original","expected_lifetime":original_lifetime})).await;assert_eq!(deleted["result"]["deleted"],true);
        let missing=rpc(&socket,"deleted-read","sessions.read",json!({"session_id":key})).await;assert!(missing.get("error").is_some());
        let recreated=rpc(&socket,"recreate","sessions.create",json!({"session_id":key,"operation_id":"create-replacement"})).await;assert_eq!(recreated["result"]["created"],true);
        let replacement=rpc(&socket,"replacement-read","sessions.read",json!({"session_id":key,"history_mode":"omitted"})).await;
        let revision = replacement["result"]["snapshot_revision"].clone();
        assert_ne!(replacement["result"]["session_lifetime_id"],json!(original_lifetime));
        for (method,params) in [
            ("sessions.delete",json!({"session_id":key,"operation_id":"late-delete","expected_lifetime":original_lifetime})),
            ("sessions.clear",json!({"session_id":key,"operation_id":"late-clear","expected_lifetime":original_lifetime})),
            ("sessions.fork",json!({"session_id":key,"target_session_id":"session-late-fork.jsonl","operation_id":"late-fork","expected_revision":0,"expected_lifetime":original_lifetime})),
            ("chat.send",json!({"session_id":key,"message":"迟到旧输入","expected_lifetime":original_lifetime})),
        ] {
            let late=rpc(&socket,"late-owner",method,params).await;
            assert!(late.get("error").is_some(),"{late}");
        }
        let current=rpc(&socket,"zero-write","sessions.read",json!({"session_id":key,"history_mode":"omitted"})).await;
        assert_eq!(current["result"]["snapshot_revision"],revision,"迟到请求必须零持久写入");
        let (new_run,response)=chat(&socket,"same-request",key,"新输入",true).await;assert!(response.unwrap().get("error").is_none());
        process.kill().await.unwrap();process.wait().await.unwrap();
        let (mut restarted,socket)=daemon(&workspace,&runtime_dir,&url).await;
        let after=rpc(&socket,"after","sessions.read",json!({"session_id":key})).await;
        assert_eq!(after["result"]["messages"].as_array().unwrap().len(),2);assert_eq!(after["result"]["messages"][0]["content"],"新输入");
        let forked=rpc(&socket,"fork-read","sessions.read",json!({"session_id":"session-forked-test.jsonl"})).await;assert_eq!(forked["result"]["messages"][0]["content"],"旧输入");
        assert!(!workspace.join(".my-agent").join(key).exists());assert_eq!(count.load(Ordering::SeqCst),2);
        entry_views(&workspace,&runtime_dir,&url,&new_run,"子任务完成",None).await;
        restarted.kill().await.unwrap();restarted.wait().await.unwrap();server.abort();let _=std::fs::remove_dir_all(workspace);let _=std::fs::remove_dir_all(runtime_dir);
    }).await.expect("lifetime restart contract timed out");
}

#[tokio::test]
async fn compact_memory_receipts_survive_restart_and_all_three_entries() {
    tokio::time::timeout(Duration::from_secs(30),async {
        let workspace=temp_workspace();
        install_observer_hooks(&workspace,&[agent_core::HookEvent::PreCompact,agent_core::HookEvent::PostCompact]);
        let runtime_dir=std::env::temp_dir().join(format!("ma-cm-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
        let (url,server,_)=mock_delegation_ollama((1..=20).collect()).await;
        let (mut process,socket)=daemon(&workspace,&runtime_dir,&url).await;
        let key="session-context-memory.jsonl";
        rpc(&socket,"create","sessions.create",json!({"session_id":key,"operation_id":"create-context"})).await;
        let invalid=rpc(&socket,"invalid-sandbox","chat.send",json!({"session_id":key,"message":"不能静默忽略 sandbox 政策","sandbox":"unsupported"})).await;
        assert!(invalid.get("error").is_some(),"{invalid}");
        let mut owner=String::new();
        for i in 0..4 {
            let (run,response)=chat(&socket,&format!("turn-{i}"),key,&"constraint-".repeat(1000),true).await;
            assert!(response.unwrap().get("error").is_none());owner=run;
        }
        let command=json!({"session_id":key,"owner_run_id":owner,"operation_id":"compact-once","expected_revision":8});
        let installed=rpc(&socket,"compact","sessions.compact",command.clone()).await;
        assert_eq!(installed["result"]["projection_generation"],1,"{installed}");
        let summary_run=installed["result"]["run_id"].clone();
        let summary_lifetime=installed["result"]["compact"]["source"]["lifetime"].clone();
        let summary=rpc(&socket,"summary-envelope","context.readback",json!({"session_id":key,"expected_lifetime":summary_lifetime,"run_id":summary_run})).await;
        assert!(summary.get("error").is_none(),"{summary}");assert_eq!(summary["result"]["purpose"],"compact_summary");assert_eq!(summary["result"]["envelope"]["source"]["generation"],0);assert_eq!(summary["result"]["envelope"]["source"]["source_end"],8);
        let local_summary=rpc(&socket,"summary-local","context.readback",json!({"session_id":key,"expected_lifetime":summary_lifetime,"run_id":summary_run,"local_diagnostics":true})).await;
        assert!(local_summary.get("error").is_none(),"{local_summary}");assert!(!local_summary.to_string().contains("constraint-"));assert_eq!(local_summary["result"]["tools"],json!([]));
        let remembered=rpc(&socket,"remember","memory.store",json!({"session_id":key,"owner_run_id":owner,"operation_id":"explicit-memory","content":"用户明确要求保留这个约束"})).await;
        assert!(remembered["result"]["entry"]["id"].is_string(),"{remembered}");
        let readonly=rpc(&socket,"readonly","chat.send",json!({"session_id":key,"message":"只读核对","context_read_only":true})).await;
        assert!(readonly.get("error").is_none(),"{readonly}");
        let readonly_run=readonly["result"]["run_id"].as_str().unwrap();
        let source_lifetime=rpc(&socket,"context-life","sessions.read",json!({"session_id":key})).await["result"]["session_lifetime_id"].clone();
        let rebuilt=rpc(&socket,"full-provider","context.readback",json!({"session_id":key,"expected_lifetime":source_lifetime,"run_id":readonly_run})).await;
        assert!(rebuilt.get("error").is_none(),"{rebuilt}");assert_eq!(rebuilt["result"]["envelope"]["source"]["generation"],1);
        assert_eq!(rebuilt["result"]["envelope"]["source"]["source_end"],9,"发送前源不包括之后提交的 assistant");
        let denied=rpc(&socket,"deny-write","memory.store",json!({"session_id":key,"owner_run_id":readonly_run,"operation_id":"denied","content":"不能写入"})).await;
        assert!(denied.get("error").is_some(),"{denied}");
        let denied=rpc(&socket,"deny-readonly-compact","sessions.compact",json!({"session_id":key,"owner_run_id":readonly_run,"operation_id":"readonly-compact","expected_revision":10})).await;
        assert!(denied.get("error").is_some(),"只读来源不能通过 unary 适配提高权限：{denied}");
        let replay=rpc(&socket,"compact-retry","sessions.compact",command.clone()).await;
        assert_eq!(replay["result"]["projection_generation"],1,"{replay}");
        assert_eq!(replay["result"]["replayed"],true);
        let hook_source=rpc(&socket,"hook-source","sessions.plan.readback",json!({"session_id":key})).await;
        let hooks=rpc(&socket,"compact-hooks","hooks.readback",json!({"session_id":key,"expected_lifetime":hook_source["result"]["lifetime"]})).await;
        assert_eq!(hooks["result"]["outcomes"].as_array().unwrap().len(),2,"{hooks}");
        assert_eq!(hooks["result"]["outcomes"][0]["payload"]["event"],"pre_compact");
        assert_eq!(hooks["result"]["outcomes"][1]["payload"]["data"]["outcome"],"succeeded");
        let page=rpc(&socket,"canonical","session.load_page",json!({"session_id":key,"limit":100})).await;
        assert_eq!(page["result"]["transcript_revision"],10,"{page}");
        process.kill().await.unwrap();process.wait().await.unwrap();
        let (mut restarted,socket)=daemon(&workspace,&runtime_dir,&url).await;
        let fresh=rpc(&socket,"after","sessions.read",json!({"session_id":key})).await;
        assert_eq!(fresh["result"]["projection_generation"],1);
        assert_eq!(fresh["result"]["transcript_revision"],10);
        let replay=rpc(&socket,"restarted-retry","sessions.compact",command).await;
        assert_eq!(replay["result"]["replayed"],true,"{replay}");
        let restored_hooks=rpc(&socket,"restored-hooks","hooks.readback",json!({"session_id":key,"expected_lifetime":hook_source["result"]["lifetime"]})).await;
        assert_snapshot_projection(&restored_hooks["result"].to_string(),&hooks["result"].to_string());
        let compact=installed["result"]["run_id"].as_str().unwrap();
        assert_ne!(compact,owner);
        entry_views_with_decision(&workspace,&runtime_dir,&url,&owner,"子任务完成",None,Some(&format!("/run {compact}"))).await;
        let doctor=rpc(&socket,"doctor","runtime.doctor",json!({})).await;
        assert_eq!(doctor["result"]["storage"]["integrity"],"ok");
        assert_eq!(doctor["result"]["storage"]["schema_version"],20);
        rpc(&socket,"clear","sessions.clear",json!({"session_id":key,"operation_id":"clear-context","expected_lifetime":fresh["result"]["session_lifetime_id"]})).await;
        let cleared=rpc(&socket,"clear-read","memory.list",json!({"session_id":key})).await;
        assert_eq!(cleared["result"]["entries"],json!([]));
        let late=rpc(&socket,"late","memory.store",json!({"session_id":key,"owner_run_id":owner,"operation_id":"late-old","content":"迟到旧写入"})).await;
        assert!(late.get("error").is_some());
        restarted.kill().await.unwrap();restarted.wait().await.unwrap();server.abort();
        let _=std::fs::remove_dir_all(workspace);let _=std::fs::remove_dir_all(runtime_dir);
    }).await.expect("compact memory 三入口重启合同超时");
}

#[tokio::test]
async fn memory_flywheel_learns_from_user_feedback_across_restart_and_purges_on_forget() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let workspace=temp_workspace();let runtime_dir=PathBuf::from(format!("/tmp/ma-fw-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
        let (url,server,_)=mock_delegation_ollama((1..=10).collect()).await;
        let (mut process,socket)=daemon(&workspace,&runtime_dir,&url).await;
        let key="session-flywheel.jsonl";
        rpc(&socket,"create","sessions.create",json!({"session_id":key,"operation_id":"create-fw"})).await;
        let (seed,response)=chat(&socket,"seed",key,"初始化",true).await;assert!(response.unwrap().get("error").is_none());
        let stored=rpc(&socket,"store","memory.store",json!({"session_id":key,"owner_run_id":seed,"operation_id":"fact","content":"cargo 约定：构建必须执行 cargo check"})).await;
        let memory=stored["result"]["entry"]["id"].as_str().unwrap().to_owned();
        let (used,response)=chat(&socket,"use",key,"cargo 约定",true).await;assert!(response.unwrap().get("error").is_none());
        let episode=format!("turn:{seed}");
        let evidence=rpc(&socket,"evidence","memory.evidence",json!({"session_id":key,"memory_id":episode,"limit":1})).await;
        assert_eq!(evidence["result"]["evidence"]["sources"][0]["text"],"初始化","{evidence}");
        assert_eq!(evidence["result"]["evidence"]["has_more"],true);
        let report=rpc(&socket,"report","memory.flywheel",json!({"session_id":key})).await;
        assert!(report["result"]["exposures"].as_array().unwrap().iter().any(|e|e["memory_id"]==memory && e["run_id"]==used && e["channel"]=="context_prepared" && e["policy"]=="lexical-feedback-v1"),"{report}");
        let command=json!({"session_id":key,"owner_run_id":used,"operation_id":"user-correction","memory_id":memory,"feedback":"incorrect"});
        let accepted=rpc(&socket,"vote","memory.feedback",command.clone()).await;assert_eq!(accepted["result"]["accepted"],true,"{accepted}");
        let conflict=rpc(&socket,"conflict","memory.feedback",json!({"session_id":key,"owner_run_id":used,"operation_id":"user-correction","memory_id":memory,"feedback":"helpful"})).await;assert!(conflict.get("error").is_some());
        process.kill().await.unwrap();process.wait().await.unwrap();
        let (mut restarted,socket)=daemon(&workspace,&runtime_dir,&url).await;
        let evidence=rpc(&socket,"evidence-restart","memory.evidence",json!({"session_id":key,"memory_id":episode,"after_source":1,"limit":1})).await;
        assert_eq!(evidence["result"]["evidence"]["sources"][0]["role"],"assistant","{evidence}");
        let forget_episode=rpc(&socket,"forget-evidence","memory.forget",json!({"session_id":key,"owner_run_id":used,"memory_id":episode,"revision":0})).await;
        assert_eq!(forget_episode["result"]["forgotten"],true);
        let evidence=rpc(&socket,"evidence-forgotten","memory.evidence",json!({"session_id":key,"memory_id":episode})).await;
        assert!(evidence.get("error").is_some());
        let replay=rpc(&socket,"replay","memory.feedback",command).await;assert_eq!(replay["result"]["accepted"],true);
        let recall=rpc(&socket,"recall","memory.recall",json!({"session_id":key,"query":"cargo 约定"})).await;
        assert!(recall["result"]["entries"].as_array().unwrap().iter().all(|e|e["id"]!=memory),"{recall}");
        let (after,response)=chat(&socket,"after-feedback",key,"cargo 约定",true).await;assert!(response.unwrap().get("error").is_none());
        let report=rpc(&socket,"after-report","memory.flywheel",json!({"session_id":key})).await;
        assert_eq!(report["result"]["feedback"]["incorrect"],1);
        assert!(report["result"]["exposures"].as_array().unwrap().iter().all(|e|e["run_id"]!=after || e["memory_id"]!=memory));
        let forgotten=rpc(&socket,"forget","memory.forget",json!({"session_id":key,"owner_run_id":used,"memory_id":memory,"revision":0})).await;assert_eq!(forgotten["result"]["forgotten"],true);
        let purged=rpc(&socket,"purged","memory.flywheel",json!({"session_id":key})).await;assert_eq!(purged["result"]["feedback"]["incorrect"],0);
        let late=rpc(&socket,"late","memory.feedback",json!({"session_id":key,"owner_run_id":used,"operation_id":"user-correction","memory_id":memory,"feedback":"incorrect"})).await;assert!(late.get("error").is_some());
        restarted.kill().await.unwrap();restarted.wait().await.unwrap();server.abort();let _=std::fs::remove_dir_all(workspace);let _=std::fs::remove_dir_all(runtime_dir);
    }).await.expect("记忆飞轮重启合同超时");
}

#[tokio::test]
async fn memory_maintenance_drains_multiple_batches_before_idle_exit() {
    use agent_core::{Admission, AdmissionMode, RequestId, RunStatus, SessionKey};
    use agent_storage::{RunStore, SessionLifecycle};
    tokio::time::timeout(Duration::from_secs(15), async {
        let workspace = temp_workspace();
        std::fs::create_dir_all(workspace.join(".my-agent")).unwrap();
        let runtime_dir = PathBuf::from(format!(
            "/tmp/ma-drain-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let key = SessionKey("session-drain.jsonl".into());
        {
            let store = RunStore::open(&workspace.join(".my-agent/runtime.sqlite3")).unwrap();
            let lifetime = store.create_session(&key).unwrap();
            for id in 0..40 {
                let Admission::New(run) = store
                    .admit_with_route(
                        key.clone(),
                        RequestId::Number(id),
                        "排空证据",
                        AdmissionMode::Queue,
                        None,
                    )
                    .unwrap()
                else {
                    panic!("new")
                };
                assert!(store.try_start_queued(&run.run_id).unwrap());
                store
                    .finish(&run.run_id, RunStatus::Completed, Some("完成"), None)
                    .unwrap();
            }
            assert_eq!(
                store.flywheel_report(&lifetime.lifetime).unwrap()["pending_ingests"],
                40
            );
        }
        // 不启动模型服务：恢复来自已提交事实，不应发起模型调用。
        let (mut process, socket) = daemon(&workspace, &runtime_dir, "http://127.0.0.1:1").await;
        let mut drained = false;
        for index in 0..30 {
            let report = rpc(
                &socket,
                &format!("report-{index}"),
                "memory.flywheel",
                json!({"session_id":key.0}),
            )
            .await;
            if report["result"]["pending_ingests"] == 0 {
                drained = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            drained,
            "积压40项必须在两个批次排空，不能等60秒或提前空闲退出"
        );
        let listed = rpc(
            &socket,
            "list",
            "memory.list",
            json!({"session_id":key.0,"limit":50}),
        )
        .await;
        assert_eq!(listed["result"]["entries"].as_array().unwrap().len(), 40);
        process.kill().await.unwrap();
        process.wait().await.unwrap();
        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(runtime_dir);
    })
    .await
    .expect("维护积压排空合同超时");
}
