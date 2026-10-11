//! 信任只收紧能力；签名、安装本身不授予执行授权。
#![forbid(unsafe_code)]
use agent_core::*;
use std::collections::{BTreeMap,BTreeSet};
pub fn validate_config(c:&AgentConfig,trust:PluginTrust)->Result<(),CoordinationError> {
    let invalid=||CoordinationError::InvalidRequest;
    if c.system_prompt.len()>32768 || c.models.is_empty() || c.models.len()>64 || c.tool_allowlist.len()>512 || c.labels.len()>64 || c.secret_refs.len()>64 || c.resources.max_rounds==0 || c.resources.max_rounds>64 || c.resources.max_tool_calls>1024 || !(1024..=16777216).contains(&c.resources.context_tokens) || c.sandbox.backends.is_empty() || c.sandbox.backends.len()>3 {return Err(invalid())}
    for list in [&c.models,&c.tool_allowlist,&c.sandbox.backends] {let mut seen=BTreeSet::new();if list.iter().any(|v|v.is_empty() || v.len()>256 || v.bytes().any(|b|b.is_ascii_control()) || !seen.insert(v)){return Err(invalid())}}
    if c.sandbox.backends.iter().any(|v|!matches!(v.as_str(),"native"|"container"|"kubernetes")) || c.labels.iter().any(|(k,v)|k.is_empty()||k.len()>128||v.len()>1024) || c.secret_refs.iter().any(|(k,v)|k.is_empty()||k.len()>128||v.len()>512||!v.starts_with("secret://")||v.contains(['\n','\r'])){return Err(invalid())}
    for refs in [&c.skill_refs,&c.mcp_refs,&c.hook_refs] {let mut seen=BTreeSet::new();if refs.len()>128 || refs.iter().any(|r|r.coordinate.is_empty() || r.coordinate.len()>256 || r.trust>trust || !seen.insert(&r.coordinate)){return Err(invalid())}}
    if trust<PluginTrust::OrganizationReviewed && (c.sandbox.network || c.sandbox.secrets || !c.secret_refs.is_empty() || !c.mcp_refs.is_empty() || !c.hook_refs.is_empty()){return Err(CoordinationError::InvalidRequest)}
    if trust==PluginTrust::Untrusted && (!c.tool_allowlist.is_empty() || c.sandbox.backends.iter().any(|v|v=="native")){return Err(CoordinationError::InvalidRequest)}
    Ok(())
}
/// 同层coordinate冲突明确拒绝；跨层覆盖保留来源与旧摘要供审计。
pub fn resolve_extensions(layers:&[(String,Vec<ExtensionRef>)])->Result<(Vec<ExtensionRef>,Vec<String>),CoordinationError> {
    let mut result=BTreeMap::<String,ExtensionRef>::new();let mut evidence=Vec::new();
    for (layer,refs) in layers {let mut seen=BTreeSet::new();for r in refs {if !seen.insert(&r.coordinate){return Err(CoordinationError::Conflict)}if let Some(old)=result.insert(r.coordinate.clone(),r.clone()) {evidence.push(format!("{layer}:{}:{}->{}",r.coordinate,old.digest,r.digest));}}}
    Ok((result.into_values().collect(),evidence))
}
