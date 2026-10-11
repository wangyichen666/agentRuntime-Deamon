//! Public client 授权码 + S256 PKCE；授权码兑换不自动重试。
use agent_core::{AuthorizationError, OAuthLoginProof, SecretToken};
use crate::{OidcVerifier, VerifiedToken};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::Duration;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OAuthCodeConfig {
    pub client_id: String,
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    pub redirect_uri: String,
    #[serde(default = "default_scope")]
    pub requested_scope: String,
}
fn default_scope() -> String { "openid".into() }
impl OAuthCodeConfig {
    pub(crate) fn validate(&self, mock: bool) -> Result<(), AuthorizationError> {
        if self.client_id.is_empty() || self.client_id.len()>256 || self.requested_scope.len()>1024
            || !self.requested_scope.split_whitespace().any(|v|v=="openid") {
            return Err(AuthorizationError::Unauthenticated);
        }
        for value in [&self.authorization_endpoint,&self.token_endpoint,&self.redirect_uri] {
            let url=reqwest::Url::parse(value).map_err(|_|AuthorizationError::Unauthenticated)?;
            if value.len()>2048 || !crate::oidc::safe_url(&url,mock) { return Err(AuthorizationError::Unauthenticated); }
        }
        if reqwest::Url::parse(&self.redirect_uri).map_err(|_|AuthorizationError::Unauthenticated)?.path()!="/auth/callback" {
            return Err(AuthorizationError::Unauthenticated);
        }
        Ok(())
    }
}

/// Debug 不实现，避免 cookie/verifier 意外进入日志。
pub struct BrowserLogin { pub state: String, pub nonce: String, pub verifier: String }
impl BrowserLogin {
    pub fn generate() -> Result<Self, AuthorizationError> {
        fn random() -> Result<String, AuthorizationError> {
            let mut bytes=[0u8;32];getrandom::fill(&mut bytes).map_err(|_|AuthorizationError::Unauthenticated)?;
            Ok(URL_SAFE_NO_PAD.encode(bytes))
        }
        Ok(Self {state:random()?,nonce:random()?,verifier:random()?})
    }
    pub fn from_cookie(value: &str) -> Result<Self, AuthorizationError> {
        let parts=value.split('.').collect::<Vec<_>>();
        if parts.len()!=3 || parts.iter().any(|v|v.len()!=43 || !v.bytes().all(|c|c.is_ascii_alphanumeric() || c==b'-' || c==b'_')) {
            return Err(AuthorizationError::Unauthenticated);
        }
        Ok(Self {state:parts[0].into(),nonce:parts[1].into(),verifier:parts[2].into()})
    }
    pub fn cookie_value(&self) -> String {format!("{}.{}.{}",self.state,self.nonce,self.verifier)}
    pub fn proof(&self) -> OAuthLoginProof {
        let digest=|s:&str|format!("{:x}",Sha256::digest(s.as_bytes()));
        OAuthLoginProof {state_digest:digest(&self.state),nonce_digest:digest(&self.nonce),verifier_digest:digest(&self.verifier)}
    }
}
#[derive(Serialize)]
pub struct LoginTokens {pub access_token: SecretToken,pub token_type: &'static str,pub expires_at: u64}
#[derive(Deserialize)]
struct TokenResponse {access_token:String,id_token:String,token_type:String}
#[derive(Deserialize)]
struct IdClaims {sub:String,exp:u64,iat:u64,nonce:String,#[serde(default)]azp:Option<String>,aud:serde_json::Value}

impl OidcVerifier {
    pub fn oauth_config(&self) -> Option<&OAuthCodeConfig> {self.config.oauth.as_ref()}
    pub fn issuer(&self) -> &str {&self.config.issuer}
    pub fn loopback_mock(&self) -> bool {self.config.allow_loopback_mock}
    pub fn authorization_url(&self,login:&BrowserLogin) -> Result<String,AuthorizationError> {
        let config=self.oauth_config().ok_or(AuthorizationError::Unauthenticated)?;
        let mut url=reqwest::Url::parse(&config.authorization_endpoint).map_err(|_|AuthorizationError::Unauthenticated)?;
        url.query_pairs_mut().extend_pairs([
            ("response_type","code"),("client_id",config.client_id.as_str()),("redirect_uri",config.redirect_uri.as_str()),
            ("scope",config.requested_scope.as_str()),("state",login.state.as_str()),("nonce",login.nonce.as_str()),
            ("code_challenge_method","S256"),("code_challenge",URL_SAFE_NO_PAD.encode(Sha256::digest(login.verifier.as_bytes())).as_str()),
        ]);
        Ok(url.into())
    }
    pub async fn exchange_code(&self,code:&str,login:&BrowserLogin)->Result<(LoginTokens,VerifiedToken),AuthorizationError> {
        if code.is_empty() || code.len()>2048 || code.bytes().any(|v|v.is_ascii_control() || v.is_ascii_whitespace()) {return Err(AuthorizationError::Unauthenticated);}
        let config=self.oauth_config().ok_or(AuthorizationError::Unauthenticated)?;
        let url=reqwest::Url::parse(&config.token_endpoint).map_err(|_|AuthorizationError::Unauthenticated)?;
        let host=url.host_str().ok_or(AuthorizationError::Unauthenticated)?;
        let port=url.port_or_known_default().ok_or(AuthorizationError::Unauthenticated)?;
        let addresses=tokio::time::timeout(Duration::from_secs(2),tokio::net::lookup_host((host,port))).await.map_err(|_|AuthorizationError::Unauthenticated)?.map_err(|_|AuthorizationError::Unauthenticated)?.take(9).collect::<Vec<_>>();
        if addresses.is_empty() || addresses.len()>8 || addresses.iter().any(|v|!crate::oidc::allowed_ip(v.ip(),self.config.allow_loopback_mock)) {return Err(AuthorizationError::Unauthenticated);}
        let client=reqwest::Client::builder().no_proxy().https_only(!self.config.allow_loopback_mock).redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(3)).connect_timeout(Duration::from_secs(2)).resolve_to_addrs(host,&addresses).build().map_err(|_|AuthorizationError::Unauthenticated)?;
        let mut response=client.post(url).form(&[("grant_type","authorization_code"),("code",code),("client_id",config.client_id.as_str()),("redirect_uri",config.redirect_uri.as_str()),("code_verifier",login.verifier.as_str())])
            .send().await.map_err(|_|AuthorizationError::Unauthenticated)?;
        if !response.status().is_success() || response.content_length().is_some_and(|v|v>131072) {return Err(AuthorizationError::Unauthenticated);}
        let mut bytes=Vec::new();
        while let Some(chunk)=response.chunk().await.map_err(|_|AuthorizationError::Unauthenticated)? {
            if bytes.len()+chunk.len()>131072 {return Err(AuthorizationError::Unauthenticated);}bytes.extend_from_slice(&chunk);
        }
        let tokens:TokenResponse=serde_json::from_slice(&bytes).map_err(|_|AuthorizationError::Unauthenticated)?;
        if !tokens.token_type.eq_ignore_ascii_case("Bearer") {return Err(AuthorizationError::Unauthenticated);}
        let access=self.verify(&tokens.access_token).await?;
        self.verify_id_token(&tokens.id_token,&login.nonce,&access).await?;
        Ok((LoginTokens {access_token:SecretToken::new(tokens.access_token)?,token_type:"Bearer",expires_at:access.expires_at},access))
    }
    pub async fn verify_id_token(&self,token:&str,nonce:&str,access:&VerifiedToken)->Result<(),AuthorizationError> {
        let config=self.oauth_config().ok_or(AuthorizationError::Unauthenticated)?;
        let (alg,key)=self.signature_key(token).await?;
        let mut validation=jsonwebtoken::Validation::new(alg);
        validation.set_issuer(&[&self.config.issuer]);validation.set_audience(&[&config.client_id]);
        validation.set_required_spec_claims(&["exp","iss","aud","sub"]);validation.validate_nbf=true;validation.leeway=5;
        let claims=jsonwebtoken::decode::<IdClaims>(token,&key,&validation).map_err(|_|AuthorizationError::Unauthenticated)?.claims;
        let now=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_err(|_|AuthorizationError::Unauthenticated)?.as_secs();
        if claims.nonce!=nonce || claims.sub!=access.identity.principal.subject_id.as_str() || claims.exp<=now || claims.exp<=claims.iat || claims.iat>now+5
            || claims.exp-claims.iat>self.config.max_token_lifetime_secs
            || claims.azp.as_ref().is_some_and(|v|v!=&config.client_id)
            || claims.aud.as_array().is_some_and(|v|v.len()>1 && claims.azp.as_deref()!=Some(config.client_id.as_str())) {
            return Err(AuthorizationError::Unauthenticated);
        }
        Ok(())
    }
}
