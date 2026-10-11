use std::sync::Arc;

use anyhow::{Result, bail};
use sha2::{Digest, Sha256};

use super::circuit::CircuitBreaker;
use super::{Provider, ProviderProfile, build_provider_from_profile};

pub use agent_core::{ContextPolicySnapshot, RouteCandidate, RouteSnapshot, TimeoutPolicy};

pub fn candidate_from_profile(profile: &ProviderProfile) -> RouteCandidate {
    RouteCandidate {
        profile_id: profile.id.clone(),
        api_type: profile.api_type,
        model: profile.model.clone(),
        base_url_sha256: format!("{:x}", Sha256::digest(profile.base_url.as_bytes())),
    }
}

fn candidate_matches(candidate: &RouteCandidate, profile: &ProviderProfile) -> bool {
    candidate.profile_id == profile.id
        && candidate.api_type == profile.api_type
        && candidate.model == profile.model
        && candidate.base_url_sha256 == format!("{:x}", Sha256::digest(profile.base_url.as_bytes()))
}

#[derive(Clone)]
pub struct FrozenRoute {
    pub snapshot: RouteSnapshot,
    pub providers: Vec<Arc<dyn Provider>>,
    pub circuit: Arc<CircuitBreaker>,
}

impl FrozenRoute {
    pub fn restrict_models(mut self,models:&[String])->Result<Self> {
        anyhow::ensure!(self.providers.len()==self.snapshot.candidates.len(),"路由provider映射无效");
        let pairs=self.snapshot.candidates.into_iter().zip(self.providers).filter(|(c,_)|models.contains(&c.model)).collect::<Vec<_>>();
        anyhow::ensure!(!pairs.is_empty(),"注册表无获准模型路由");let(candidates,providers)=pairs.into_iter().unzip();self.snapshot.candidates=candidates;self.providers=providers;Ok(self)
    }
    pub fn primary(&self) -> Arc<dyn Provider> {
        self.providers[0].clone()
    }

    pub fn restore(
        snapshot: RouteSnapshot,
        profiles: &[ProviderProfile],
        circuit: Arc<CircuitBreaker>,
    ) -> Result<Self> {
        let mut providers = Vec::new();
        for candidate in &snapshot.candidates {
            let profile = profiles
                .iter()
                .find(|profile| candidate_matches(candidate, profile))
                .ok_or_else(|| anyhow::anyhow!("原 run 的 Provider 配置不可用或已改变"))?;
            providers.push(Arc::from(build_provider_from_profile(profile)?));
        }
        if providers.is_empty() {
            bail!("route snapshot 没有候选 Provider");
        }
        Ok(Self {
            snapshot,
            providers,
            circuit,
        })
    }
}
