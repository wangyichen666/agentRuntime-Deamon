//! 发布/config纯规则；数据库拥有revision、幂等receipt和session代际绑定。
#![forbid(unsafe_code)]
use agent_core::*;
use std::collections::BTreeMap;
pub use agent_pack::{digest,valid_digest};
pub fn effective(def:&AgentDefinition,trust:PluginTrust)->Result<EffectiveAgentConfig,CoordinationError> {
    let mut config=def.layers.defaults.clone();agent_plugin::validate_config(&config,trust)?;
    let mut sources=BTreeMap::new();let mut refs=[Vec::new(),Vec::new(),Vec::new()];
    for (i,values) in [&config.skill_refs,&config.mcp_refs,&config.hook_refs].into_iter().enumerate(){refs[i].push(("defaults".into(),values.clone()));}
    for field in ["system_prompt","models","tool_allowlist","skill_refs","mcp_refs","hook_refs","sandbox","resources","labels","secret_refs"]{sources.insert(field.into(),"defaults".into());}
    for (name,patch) in [("organization",&def.layers.organization),("tenant",&def.layers.tenant),("project",&def.layers.project),("agent",&def.layers.agent),("session_override",&def.layers.session_override)] {
        macro_rules! field {($($f:ident),*)=>{$(if let Some(v)=&patch.$f {config.$f=v.clone();sources.insert(stringify!($f).into(),name.into());})*}}
        field!(system_prompt,models,tool_allowlist,sandbox,resources,labels,secret_refs);
        for (i,values) in [&patch.skill_refs,&patch.mcp_refs,&patch.hook_refs].into_iter().enumerate(){if let Some(values)=values{refs[i].push((name.into(),values.clone()));sources.insert(["skill_refs","mcp_refs","hook_refs"][i].into(),name.into());}}
        let (a,_)=agent_plugin::resolve_extensions(&refs[0])?;let(b,_)=agent_plugin::resolve_extensions(&refs[1])?;let(c,_)=agent_plugin::resolve_extensions(&refs[2])?;
        config.skill_refs=a;config.mcp_refs=b;config.hook_refs=c;agent_plugin::validate_config(&config,trust)?;
    }
    let mut evidence=Vec::new();for layers in refs {evidence.extend(agent_plugin::resolve_extensions(&layers)?.1);}
    if serde_json::to_vec(&config).map_err(|_|CoordinationError::Corrupt)?.len()>65536{return Err(CoordinationError::InvalidRequest)}
    Ok(EffectiveAgentConfig{digest:digest(&serde_json::json!(config))?,config,sources,evidence})
}
pub fn prepare(pack:&SignedAgentPack,scope:&TenantContext,trust:&RegistryTrust)->Result<InstalledGeneration,CoordinationError> {
    let(pack_digest,trust_digest,scan_digest)=agent_pack::scan(pack,scope,trust)?;let effective=effective(&pack.manifest.definition,pack.manifest.trust)?;
    for refs in [&effective.config.skill_refs,&effective.config.mcp_refs,&effective.config.hook_refs]{for r in refs {if !agent_pack::valid_path(&r.path)||pack.manifest.files.get(&r.path)!=Some(&r.digest){return Err(CoordinationError::InvalidRequest)}}}
    // MCP/hook配置是严格JSON材料；执行准入将在原manager再次核验。
    for r in effective.config.mcp_refs.iter().chain(&effective.config.hook_refs){let f=pack.contents.iter().find(|f|f.path==r.path).ok_or(CoordinationError::Corrupt)?;if f.content_base64.len()>65536{return Err(CoordinationError::InvalidRequest)}}
    let mut skill_materials=BTreeMap::new();let mut bytes=0usize;for r in &effective.config.skill_refs {let text=agent_pack::text_file(pack,&r.path)?;bytes+=text.len();if bytes>65536{return Err(CoordinationError::InvalidRequest)}skill_materials.insert(r.coordinate.clone(),text);}
    let generation=InstalledGeneration{scope:scope.clone(),pack_digest,trust_digest,manifest:pack.manifest.clone(),effective,scan_digest,skill_materials};
    if serde_json::to_vec(&generation).map_err(|_|CoordinationError::Corrupt)?.len()>262144{return Err(CoordinationError::InvalidRequest)}Ok(generation)
}
pub fn rollout(state:&mut RegistryState,stable:&str,canary:Option<&str>,bp:u16)->Result<(),CoordinationError> {
    if !valid_digest(stable) || canary.is_some_and(|v|!valid_digest(v)||v==stable) || bp>10000 || canary.is_none() && bp!=0{return Err(CoordinationError::InvalidRequest)}
    state.rollout=Some(RegistryRollout{stable:stable.into(),canary:canary.map(str::to_owned),canary_basis_points:bp,revision:state.revision});Ok(())
}
pub fn selected<'a>(rollout:&'a RegistryRollout,key:&SessionKey,life:&SessionLifetimeId)->Result<&'a str,CoordinationError> {
    let hash=digest(&serde_json::json!([key.0,life.0,rollout.revision]))?;let bucket=u16::from_str_radix(&hash[..4],16).map_err(|_|CoordinationError::Corrupt)? as u32%10000;
    Ok(if bucket<u32::from(rollout.canary_basis_points){rollout.canary.as_deref().ok_or(CoordinationError::Corrupt)?}else{&rollout.stable})
}
pub fn receipt(state:&RegistryState,generation:Option<InstalledGeneration>)->Result<RegistryReceipt,CoordinationError>{Ok(RegistryReceipt{revision:state.revision,generation,rollout:state.rollout.clone(),state_digest:digest(&serde_json::json!(state))?})}
pub fn command_action(c:&RegistryCommand)->Action {match c {RegistryCommand::ConfigureTrust{..}=>Action::IdentityManage,RegistryCommand::Publish{..}=>Action::PluginInstall,_=>Action::AgentConfigure}}
pub fn validate_mutation(m:&RegistryMutation)->Result<(),CoordinationError> {if m.operation_id.is_empty() || m.operation_id.len()>256 || m.expected_revision>i64::MAX as u64 || serde_json::to_vec(m).map_err(|_|CoordinationError::InvalidRequest)?.len()>3*1024*1024{return Err(CoordinationError::InvalidRequest)}Ok(())}
pub fn apply_prepared(state:&RegistryState,m:&RegistryMutation,scope:&TenantContext,existing:&[InstalledGeneration],prepared:Option<&InstalledGeneration>)->Result<(RegistryState,Option<InstalledGeneration>),CoordinationError> {
    validate_mutation(m)?;if state.revision!=m.expected_revision{return Err(CoordinationError::Conflict)}let mut next=state.clone();next.revision=next.revision.checked_add(1).filter(|n|*n<=i64::MAX as u64).ok_or(CoordinationError::InvalidRequest)?;
    let mut installed=None;
    match &m.command {
        RegistryCommand::ConfigureTrust{trust}=>{agent_pack::validate_trust(trust)?;if state.trust.as_ref().is_some_and(|old|old.version==trust.version && old!=trust){return Err(CoordinationError::Conflict)}next.trust=Some(trust.clone());},
        RegistryCommand::Publish{pack}=>{let trust=next.trust.as_ref().ok_or(CoordinationError::InvalidRequest)?;let generation=prepared.cloned().ok_or(CoordinationError::InvalidRequest)?;
            if generation.scope!=*scope || generation.manifest!=pack.manifest || generation.pack_digest!=digest(&serde_json::json!(pack.manifest))? || generation.trust_digest!=agent_pack::validate_trust(trust)? {return Err(CoordinationError::Conflict)}
            if existing.iter().any(|g|g.manifest.definition.version==generation.manifest.definition.version && *g!=generation){return Err(CoordinationError::Conflict)}
            if existing.len()>=64 && !existing.iter().any(|g|g.pack_digest==generation.pack_digest){return Err(CoordinationError::InvalidRequest)}installed=Some(generation);},
        RegistryCommand::Rollout{stable,canary,canary_basis_points}=>{for d in std::iter::once(stable).chain(canary.iter()){if !existing.iter().any(|g|g.pack_digest==*d){return Err(CoordinationError::InvalidRequest)}}rollout(&mut next,stable,canary.as_deref(),*canary_basis_points)?;},
        RegistryCommand::Rollback{digest}=>{if !existing.iter().any(|g|g.pack_digest==*digest){return Err(CoordinationError::InvalidRequest)}rollout(&mut next,digest,None,0)?;},
    }
    Ok((next,installed))
}

pub fn validate_runtime(g:&InstalledGeneration,s:&RunSnapshot)->Result<(),CoordinationError>{let c=&g.effective.config;
    if g.effective.digest!=digest(&serde_json::json!(c))? || s.tools.iter().any(|t|!c.tool_allowlist.contains(&t.name)) || s.max_tool_calls.is_none_or(|n|n>c.resources.max_tool_calls) || s.context_token_budget as u64>c.resources.context_tokens || !c.sandbox.backends.iter().any(|v|v==&s.sandbox_effective || v=="container" && s.sandbox_effective=="docker") || s.route.as_ref().is_none_or(|r|r.candidates.iter().any(|p|!c.models.contains(&p.model))) {return Err(CoordinationError::InvalidRequest)}Ok(())
}
