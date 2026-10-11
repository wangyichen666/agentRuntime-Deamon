//! 单次HTTPS派发；未知响应保留原工具unknown收据，不自动重试。
use super::Tool;
use crate::{safety::SafetyPolicy};
use anyhow::{Result,bail};
use serde_json::{Value,json};
use std::{sync::Arc,time::Duration,net::IpAddr};
pub struct WorkflowWebhookTool {pub safety:Arc<SafetyPolicy>}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Args {url:String,body:Value,idempotency_key:Option<String>}
fn parse(value:&Value)->Result<Args>{let args:Args=serde_json::from_value(value.clone())?;let url=reqwest::Url::parse(&args.url)?;if url.scheme()!="https"||!url.username().is_empty()||url.password().is_some()||url.query().is_some()||url.fragment().is_some()||url.host_str().is_none()||args.url.len()>2048||serde_json::to_vec(&args.body)?.len()>65536||args.idempotency_key.as_ref().is_some_and(|k|k.is_empty()||k.len()>256||!k.bytes().all(|b|b.is_ascii_graphic())){bail!("Webhook输入不符合受治理HTTPS边界")}Ok(args)}
fn public_ip(ip:IpAddr)->bool{match ip{IpAddr::V4(v)=>{let o=v.octets();!v.is_private()&&!v.is_loopback()&&!v.is_link_local()&&!v.is_multicast()&&!v.is_unspecified()&&!v.is_broadcast()&&o[0]!=0&&o[0]<240&&!(o[0]==100&&(64..=127).contains(&o[1]))&&!(o[0]==192&&o[1]==0)&&!(o[0]==198&&(o[1]==18||o[1]==19))},IpAddr::V6(v)=>v.to_ipv4_mapped().map(public_ip_v4).unwrap_or_else(||v.segments()[0]&0xe000==0x2000&&!v.is_multicast()&&!v.is_loopback()&&!v.is_unspecified())}}
fn public_ip_v4(ip:std::net::Ipv4Addr)->bool{public_ip(IpAddr::V4(ip))}
#[async_trait::async_trait]
impl Tool for WorkflowWebhookTool{
 fn name(&self)->&str{"workflow_webhook"}
 fn description(&self)->&str{"向已授权公网HTTPS端点单次POST，断连结果未知，不自动重试"}
 fn parameters(&self)->Value{json!({"type":"object","additionalProperties":false,"required":["url","body","idempotency_key"],"properties":{"url":{"type":"string","pattern":"^https://"},"body":{},"idempotency_key":{"type":["string","null"]}}})}
 async fn preflight(&self,args:&Value)->Result<()> {parse(args)?;if let Some(snapshot)=crate::loop_engine::current_run_settings().await?{if snapshot.installed_generation.as_ref().is_some_and(|g|!g.effective.config.sandbox.network){bail!("发行代拒绝网络访问")}}self.safety.authorize_external_action("工作流HTTPS Webhook",args).await}
 fn descriptor(&self,args:&Value)->Result<agent_core::ToolDescriptor>{parse(args)?;let mut descriptor=agent_core::ToolDescriptor::external();descriptor.output_budget=4096;Ok(descriptor)}
 async fn execute(&self,args:Value)->Result<String>{let args=parse(&args)?;let url=reqwest::Url::parse(&args.url)?;let host=url.host_str().ok_or_else(||anyhow::anyhow!("Webhook无host"))?;let port=url.port_or_known_default().ok_or_else(||anyhow::anyhow!("Webhook无port"))?;
  let addresses=tokio::time::timeout(Duration::from_secs(2),tokio::net::lookup_host((host,port))).await??.take(9).collect::<Vec<_>>();if addresses.is_empty()||addresses.len()>8||addresses.iter().any(|v|!public_ip(v.ip())){bail!("Webhook DNS拒绝private/linklocal/metadata地址")}
  let client=reqwest::Client::builder().https_only(true).retry(reqwest::retry::never()).no_proxy().redirect(reqwest::redirect::Policy::none()).resolve_to_addrs(host,&addresses).connect_timeout(Duration::from_secs(3)).timeout(Duration::from_secs(30)).build()?;let mut request=client.post(url).json(&args.body);if let Some(key)=args.idempotency_key{request=request.header("Idempotency-Key",key)}
  let mut response=request.send().await.map_err(|_|agent_core::ToolOutcomeUnknown("Webhook派发未获确认".into()))?;let status=response.status().as_u16();let mut bytes=Vec::new();while let Some(chunk)=response.chunk().await.map_err(|_|agent_core::ToolOutcomeUnknown("Webhook响应未完整确认".into()))?{if bytes.len().saturating_add(chunk.len())>65536{return Err(agent_core::ToolOutcomeUnknown("Webhook响应超过预算，副作用需要reconcile".into()).into())}bytes.extend_from_slice(&chunk);}use sha2::Digest;let digest=format!("{:x}",sha2::Sha256::digest(&bytes));if !(200..300).contains(&status){return Err(agent_core::ToolOutcomeUnknown(format!("Webhook HTTP {status}，body_digest={digest}，不推断无副作用")).into())}Ok(json!({"status":status,"response_digest":digest,"response_bytes":bytes.len()}).to_string())
 }
}
