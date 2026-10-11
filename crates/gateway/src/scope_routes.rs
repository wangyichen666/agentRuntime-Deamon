//! 企业scope只能映射到受保护配置；令牌未验签内容只作有限集合中的选路提示。
use agent_identity::{OidcVerifier,oidc::{OidcConfig,read_protected_json}};
use anyhow::{Context,Result};
use serde::Deserialize;
use std::{collections::{BTreeMap,BTreeSet},path::PathBuf,sync::Arc};
#[derive(Clone)]
pub(crate) struct ScopeRoute {
    pub name:String,pub workspace:PathBuf,pub verifier:Arc<OidcVerifier>,
    pub gateway_token:PathBuf,pub runtime_routes:Option<PathBuf>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopeConfig {scopes:BTreeMap<String,ScopeReference>}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopeReference {workspace:PathBuf,oidc_config:PathBuf,gateway_token:PathBuf,#[serde(default)]runtime_routes:Option<PathBuf>}
fn valid_name(name:&str)->bool {!name.is_empty() && name.len()<=64 && name.bytes().all(|v|v.is_ascii_alphanumeric() || matches!(v,b'-'|b'_'))}
pub(crate) async fn load(default:Option<ScopeRoute>)->Result<Arc<BTreeMap<String,Arc<ScopeRoute>>>> {
    let mut routes=BTreeMap::new();if let Some(default)=default {routes.insert("default".into(),Arc::new(default));}
    if let Some(path)=std::env::var_os("AGENT_GATEWAY_SCOPES") {
        anyhow::ensure!(!routes.is_empty(),"多scope Gateway必须启用企业身份");
        let refs:ScopeConfig=tokio::task::spawn_blocking(move||read_protected_json(std::path::Path::new(&path))).await?.map_err(|_|anyhow::anyhow!("scope路由配置无效"))?;
        anyhow::ensure!(refs.scopes.len()<=15,"scope路由超过预算");
        for (name,r) in refs.scopes {
            anyhow::ensure!(valid_name(&name) && name!="default" && r.workspace.is_absolute() && r.oidc_config.is_absolute() && r.gateway_token.is_absolute() && r.runtime_routes.as_ref().is_none_or(|p|p.is_absolute()),"scope路由引用无效");
            let config:OidcConfig=tokio::task::spawn_blocking(move||read_protected_json(&r.oidc_config)).await?.map_err(|_|anyhow::anyhow!("scope身份配置无效"))?;
            let verifier=Arc::new(OidcVerifier::new(config)?);anyhow::ensure!(verifier.gateway_subject().is_some(),"scope缺固定Gateway主体");
            if let Some(oauth)=verifier.oauth_config() {
                // code callback显式带别名，cookie/nonce仍由目标scope事务最终验证。
                let uri=oauth.redirect_uri.parse::<axum::http::Uri>().context("scope回调URI无效")?;
                let hint=axum::extract::Query::<ScopeHint>::try_from_uri(&uri).context("scope回调缺路由提示")?;
                anyhow::ensure!(hint.scope.as_deref()==Some(&name),"scope回调必须绑定受保护别名");
            }
            routes.insert(name.clone(),Arc::new(ScopeRoute{name,workspace:r.workspace,verifier,gateway_token:r.gateway_token,runtime_routes:r.runtime_routes}));
        }
    }
    let mut scopes=BTreeSet::new();for route in routes.values() {anyhow::ensure!(scopes.insert(serde_json::to_string(route.verifier.scope())?),"重复scope路由");}
    Ok(Arc::new(routes))
}
#[derive(Deserialize)]
pub(crate) struct ScopeHint {pub scope:Option<String>}
pub(crate) fn select(routes:&BTreeMap<String,Arc<ScopeRoute>>,token:Option<&str>,name:Option<&str>)->Result<Arc<ScopeRoute>> {
    if let Some(name)=name {return routes.get(name).cloned().context("scope不可用")}
    if let Some(token)=token.filter(|t|!t.starts_with("ak.")) {
        let hint=agent_identity::oidc::unverified_scope_hint(token)?;
        return routes.values().find(|r|*r.verifier.scope()==hint).cloned().context("scope不可用");
    }
    routes.get("default").cloned().context("scope不可用")
}
