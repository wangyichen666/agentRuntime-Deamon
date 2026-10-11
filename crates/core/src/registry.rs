//! 注册表治理配置，不拥有run/session事实；安装代由canonical session lifetime固定。
use crate::*;
use serde::{Deserialize,Serialize};
use std::collections::BTreeMap;
#[derive(Clone,Copy,Debug,PartialEq,Eq,PartialOrd,Ord,Serialize,Deserialize)]
#[serde(rename_all="snake_case")]
pub enum PluginTrust {Untrusted,ProjectLocal,OrganizationReviewed,Builtin}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionRef {pub coordinate:String,pub path:String,pub digest:String,pub trust:PluginTrust}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSandboxPolicy {pub backends:Vec<String>,pub network:bool,pub secrets:bool}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentResourceProfile {pub max_rounds:u32,pub max_tool_calls:u64,pub context_tokens:u64}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    pub system_prompt:String,pub models:Vec<String>,pub tool_allowlist:Vec<String>,
    pub skill_refs:Vec<ExtensionRef>,pub mcp_refs:Vec<ExtensionRef>,pub hook_refs:Vec<ExtensionRef>,
    pub sandbox:AgentSandboxPolicy,pub resources:AgentResourceProfile,
    pub labels:BTreeMap<String,String>,pub secret_refs:BTreeMap<String,String>,
}
#[derive(Clone,Debug,Default,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfigPatch {
    #[serde(default,skip_serializing_if="Option::is_none")] pub system_prompt:Option<String>,#[serde(default,skip_serializing_if="Option::is_none")] pub models:Option<Vec<String>>,#[serde(default,skip_serializing_if="Option::is_none")] pub tool_allowlist:Option<Vec<String>>,
    #[serde(default,skip_serializing_if="Option::is_none")] pub skill_refs:Option<Vec<ExtensionRef>>,#[serde(default,skip_serializing_if="Option::is_none")] pub mcp_refs:Option<Vec<ExtensionRef>>,#[serde(default,skip_serializing_if="Option::is_none")] pub hook_refs:Option<Vec<ExtensionRef>>,
    #[serde(default,skip_serializing_if="Option::is_none")] pub sandbox:Option<AgentSandboxPolicy>,#[serde(default,skip_serializing_if="Option::is_none")] pub resources:Option<AgentResourceProfile>,
    #[serde(default,skip_serializing_if="Option::is_none")] pub labels:Option<BTreeMap<String,String>>,#[serde(default,skip_serializing_if="Option::is_none")] pub secret_refs:Option<BTreeMap<String,String>>,
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigLayers {pub defaults:AgentConfig,pub organization:AgentConfigPatch,pub tenant:AgentConfigPatch,pub project:AgentConfigPatch,pub agent:AgentConfigPatch,pub session_override:AgentConfigPatch}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentDefinition {pub scope:TenantContext,pub name:String,pub version:String,pub layers:ConfigLayers}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackFile {pub path:String,pub digest:String,pub content_base64:String}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackManifest {pub schema_version:u32,pub definition:AgentDefinition,pub files:BTreeMap<String,String>,pub trust:PluginTrust}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedAgentPack {pub manifest:PackManifest,pub contents:Vec<PackFile>,pub key_id:String,pub signature_base64:String}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryTrustRoot {pub key_id:String,pub public_key_base64:String,pub maximum_trust:PluginTrust}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryTrust {pub version:String,pub roots:Vec<RegistryTrustRoot>}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectiveAgentConfig {pub config:AgentConfig,pub digest:String,pub sources:BTreeMap<String,String>,pub evidence:Vec<String>}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledGeneration {pub scope:TenantContext,pub pack_digest:String,pub trust_digest:String,pub manifest:PackManifest,pub effective:EffectiveAgentConfig,pub scan_digest:String,pub skill_materials:BTreeMap<String,String>}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryRollout {pub stable:String,pub canary:Option<String>,pub canary_basis_points:u16,pub revision:u64}
#[derive(Clone,Debug,Default,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryState {pub revision:u64,pub trust:Option<RegistryTrust>,pub rollout:Option<RegistryRollout>}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(tag="action",rename_all="snake_case",deny_unknown_fields)]
pub enum RegistryCommand {
    ConfigureTrust {trust:RegistryTrust},
    Publish {pack:SignedAgentPack},
    Rollout {stable:String,canary:Option<String>,canary_basis_points:u16},
    Rollback {digest:String},
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryMutation {pub operation_id:String,pub expected_revision:u64,pub command:RegistryCommand}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryReceipt {pub revision:u64,pub generation:Option<InstalledGeneration>,pub rollout:Option<RegistryRollout>,pub state_digest:String}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryReadback {pub state:RegistryState,pub generations:Vec<InstalledGeneration>}
