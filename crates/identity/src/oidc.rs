//! 有限 JWKS 缓存和签名认证。令牌不携带可生效的角色；持久策略仍作最终授权。
use agent_core::{AuthorizationError, RuntimeIdentity, SubjectId, SubjectKind, TenantContext};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header, jwk::JwkSet};
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OidcConfig {
    #[serde(default = "default_endpoint")]
    pub runtime_endpoint: String,
    #[serde(default)]
    pub oauth: Option<crate::oauth::OAuthCodeConfig>,
    #[serde(default)]
    pub gateway_subject: Option<SubjectId>,
    pub issuer: String,
    pub audience: String,
    pub jwks_url: String,
    pub scope: TenantContext,
    pub jwks_ttl_secs: u64,
    pub max_token_lifetime_secs: u64,
    #[serde(default)]
    pub allow_loopback_mock: bool,
}
fn default_endpoint() -> String {"runtime-default".into()}

/// Debug 不包含凭据；所有外部认证失败均投影成同一安全错误。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VerifiedToken {
    pub identity: RuntimeIdentity,
    pub expires_at: u64,
}

#[derive(Deserialize)]
struct AccessClaims {
    sub: SubjectId,
    iss: String,
    aud: serde_json::Value,
    exp: u64,
    nbf: u64,
    iat: u64,
    scope: TenantContext,
    subject_kind: SubjectKind,
    revocation_epoch: u64,
    token_use: String,
}

struct Cache {
    keys: Option<JwkSet>,
    valid_until: Instant,
    refresh_after: Instant,
}
pub struct OidcVerifier {
    pub(crate) config: OidcConfig,
    cache: Mutex<Cache>,
}
impl OidcVerifier {
    pub fn from_environment() -> Result<Option<Self>, AuthorizationError> {
        let Some(path) = std::env::var_os("AGENT_OIDC_CONFIG") else {
            return Ok(None);
        };
        let config = read_protected_json(Path::new(&path))?;
        Self::new(config).map(Some)
    }
    pub fn new(config: OidcConfig) -> Result<Self, AuthorizationError> {
        let issuer =
            reqwest::Url::parse(&config.issuer).map_err(|_| AuthorizationError::Unauthenticated)?;
        let jwks = reqwest::Url::parse(&config.jwks_url)
            .map_err(|_| AuthorizationError::Unauthenticated)?;
        if config.scope == TenantContext::local()
            || config.runtime_endpoint.is_empty() || config.runtime_endpoint.len()>128
            || !config.runtime_endpoint.bytes().all(|v|v.is_ascii_alphanumeric() || matches!(v,b'-'|b'_'|b'.'))
            || config.audience.is_empty()
            || config.audience.len() > 256
            || config.issuer.len() > 2048
            || config.jwks_url.len() > 2048
            || !(30..=3600).contains(&config.jwks_ttl_secs)
            || !(30..=900).contains(&config.max_token_lifetime_secs)
            || !safe_url(&issuer, config.allow_loopback_mock)
            || !safe_url(&jwks, config.allow_loopback_mock)
        {
            return Err(AuthorizationError::Unauthenticated);
        }
        if let Some(oauth) = &config.oauth { oauth.validate(config.allow_loopback_mock)?; }
        let now = Instant::now();
        Ok(Self {
            config,
            cache: Mutex::new(Cache {
                keys: None,
                valid_until: now,
                refresh_after: now,
            }),
        })
    }
    pub fn scope(&self) -> &TenantContext {
        &self.config.scope
    }
    pub fn runtime_endpoint(&self) -> &str {&self.config.runtime_endpoint}
    pub fn gateway_subject(&self) -> Option<&SubjectId> {
        self.config.gateway_subject.as_ref()
    }
    pub async fn verify(&self, token: &str) -> Result<VerifiedToken, AuthorizationError> {
        let (algorithm, key) = self.signature_key(token).await?;
        let mut validation = Validation::new(algorithm);
        validation.set_issuer(&[&self.config.issuer]);
        validation.set_audience(&[&self.config.audience]);
        validation.set_required_spec_claims(&["exp", "nbf", "iss", "aud", "sub"]);
        validation.validate_nbf = true;
        validation.leeway = 5;
        let claims = decode::<AccessClaims>(token, &key, &validation)
            .map_err(|_| AuthorizationError::Unauthenticated)?
            .claims;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| AuthorizationError::Unauthenticated)?
            .as_secs();
        if claims.scope != self.config.scope
            || claims.iss != self.config.issuer
            || claims.token_use != "access"
            || claims.exp <= claims.iat
            || claims.exp - claims.iat > self.config.max_token_lifetime_secs
            || claims.iat > now + 5
            || claims.nbf > claims.exp
            || claims.exp <= now
            || !matches!(
                claims.aud,
                serde_json::Value::String(_) | serde_json::Value::Array(_)
            )
        {
            return Err(AuthorizationError::Unauthenticated);
        }
        Ok(VerifiedToken {
            identity: RuntimeIdentity {
                scope: claims.scope,
                principal: agent_core::AuthenticatedPrincipal {
                    subject_id: claims.sub,
                    kind: claims.subject_kind,
                    revocation_epoch: claims.revocation_epoch,
                },
            },
            expires_at: claims.exp,
        })
    }
    pub(crate) async fn signature_key(&self, token: &str) -> Result<(Algorithm, DecodingKey), AuthorizationError> {
        if token.len() > 16384 || token.is_empty() {
            return Err(AuthorizationError::Unauthenticated);
        }
        let header = decode_header(token).map_err(|_| AuthorizationError::Unauthenticated)?;
        if !matches!(
            header.alg,
            Algorithm::RS256 | Algorithm::ES256 | Algorithm::EdDSA
        ) || header
            .typ
            .as_deref()
            .is_some_and(|v| v != "JWT" && v != "at+jwt")
        {
            return Err(AuthorizationError::Unauthenticated);
        }
        let kid = header
            .kid
            .filter(|v| !v.is_empty() && v.len() <= 128)
            .ok_or(AuthorizationError::Unauthenticated)?;
        let key = {
            let mut cache = self.cache.lock().await;
            let now = Instant::now();
            let fresh = now < cache.valid_until;
            let found = cache.keys.as_ref().and_then(|keys| keys.find(&kid));
            if (!fresh || found.is_none()) && now >= cache.refresh_after {
                cache.refresh_after = now + Duration::from_secs(5);
                let keys = self.fetch_keys().await?;
                cache.valid_until = Instant::now() + Duration::from_secs(self.config.jwks_ttl_secs);
                cache.keys = Some(keys);
            }
            if Instant::now() >= cache.valid_until {
                return Err(AuthorizationError::Unauthenticated);
            }
            let jwk = cache
                .keys
                .as_ref()
                .and_then(|keys| keys.find(&kid))
                .ok_or(AuthorizationError::Unauthenticated)?;
            // 绑定本 issuer 的 key+算法，避免 RSA/EC 混淆及用于加密的 key。
            let raw = serde_json::to_value(jwk).map_err(|_| AuthorizationError::Unauthenticated)?;
            let expected = match header.alg {
                Algorithm::RS256 => "RS256",
                Algorithm::ES256 => "ES256",
                Algorithm::EdDSA => "EdDSA",
                _ => return Err(AuthorizationError::Unauthenticated),
            };
            if raw.get("alg").and_then(|v| v.as_str()) != Some(expected)
                || raw
                    .get("use")
                    .and_then(|v| v.as_str())
                    .is_some_and(|v| v != "sig")
                || raw
                    .get("key_ops")
                    .and_then(|v| v.as_array())
                    .is_some_and(|v| !v.iter().any(|op| op == "verify"))
            {
                return Err(AuthorizationError::Unauthenticated);
            }
            DecodingKey::from_jwk(jwk).map_err(|_| AuthorizationError::Unauthenticated)?
        };
        Ok((header.alg, key))
    }
    async fn fetch_keys(&self) -> Result<JwkSet, AuthorizationError> {
        let url = reqwest::Url::parse(&self.config.jwks_url)
            .map_err(|_| AuthorizationError::Unauthenticated)?;
        let host = url.host_str().ok_or(AuthorizationError::Unauthenticated)?;
        let port = url
            .port_or_known_default()
            .ok_or(AuthorizationError::Unauthenticated)?;
        let resolved = tokio::time::timeout(
            Duration::from_secs(2),
            tokio::net::lookup_host((host, port)),
        )
        .await
        .map_err(|_| AuthorizationError::Unauthenticated)?
        .map_err(|_| AuthorizationError::Unauthenticated)?;
        let addresses = resolved.take(9).collect::<Vec<_>>();
        if addresses.is_empty()
            || addresses.len() > 8
            || addresses
                .iter()
                .any(|a| !allowed_ip(a.ip(), self.config.allow_loopback_mock))
        {
            return Err(AuthorizationError::Unauthenticated);
        }
        let client = reqwest::Client::builder()
            .https_only(!self.config.allow_loopback_mock)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(3))
            .connect_timeout(Duration::from_secs(2))
            .resolve_to_addrs(host, &addresses)
            .build()
            .map_err(|_| AuthorizationError::Unauthenticated)?;
        let mut response = client
            .get(url)
            .send()
            .await
            .map_err(|_| AuthorizationError::Unauthenticated)?;
        if !response.status().is_success() || response.content_length().is_some_and(|n| n > 131072)
        {
            return Err(AuthorizationError::Unauthenticated);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| AuthorizationError::Unauthenticated)?
        {
            if bytes.len() + chunk.len() > 131072 {
                return Err(AuthorizationError::Unauthenticated);
            }
            bytes.extend_from_slice(&chunk);
        }
        let keys: JwkSet =
            serde_json::from_slice(&bytes).map_err(|_| AuthorizationError::Unauthenticated)?;
        if keys.keys.is_empty() || keys.keys.len() > 32 {
            return Err(AuthorizationError::Unauthenticated);
        }
        let mut ids = std::collections::BTreeSet::new();
        for key in &keys.keys {
            if key
                .common
                .key_id
                .as_ref()
                .is_none_or(|id| id.is_empty() || id.len() > 128 || !ids.insert(id.clone()))
            {
                return Err(AuthorizationError::Unauthenticated);
            }
        }
        Ok(keys)
    }
}
pub(crate) fn safe_url(url: &reqwest::Url, mock: bool) -> bool {
    url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
        && url.query().is_none()
        && (url.scheme() == "https"
            || (mock
                && url.scheme() == "http"
                && matches!(url.host_str(), Some("127.0.0.1" | "[::1]" | "localhost"))))
}
pub(crate) fn allowed_ip(ip: std::net::IpAddr, mock: bool) -> bool {
    if ip.is_loopback() {
        return mock;
    }
    match ip {
        std::net::IpAddr::V4(ip) => {
            !ip.is_private()
                && !ip.is_link_local()
                && !ip.is_unspecified()
                && !ip.is_multicast()
                && !ip.is_broadcast()
                && ip.octets()[0] != 0
        }
        std::net::IpAddr::V6(ip) => ip.to_ipv4_mapped().map_or_else(
            || {
                !ip.is_unspecified()
                    && !ip.is_multicast()
                    && (ip.segments()[0] & 0xfe00) != 0xfc00
                    && (ip.segments()[0] & 0xffc0) != 0xfe80
            },
            |v| allowed_ip(v.into(), mock),
        ),
    }
}
pub fn read_protected_json<T: for<'de> Deserialize<'de>>(
    path: &Path,
) -> Result<T, AuthorizationError> {
    use std::{
        io::Read,
        os::unix::fs::{OpenOptionsExt, PermissionsExt},
    };
    if !path.is_absolute() {
        return Err(AuthorizationError::Unauthenticated);
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
        .open(path)
        .map_err(|_| AuthorizationError::Unauthenticated)?;
    let metadata = file
        .metadata()
        .map_err(|_| AuthorizationError::Unauthenticated)?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o077 != 0 {
        return Err(AuthorizationError::Unauthenticated);
    }
    let mut bytes = Vec::new();
    file.take(65537)
        .read_to_end(&mut bytes)
        .map_err(|_| AuthorizationError::Unauthenticated)?;
    if bytes.len() > 65536 {
        return Err(AuthorizationError::Unauthenticated);
    }
    serde_json::from_slice(&bytes).map_err(|_| AuthorizationError::Unauthenticated)
}

#[cfg(test)]
mod cache_contract {
    use super::*;
    use std::sync::{Arc,atomic::{AtomicBool,Ordering}};
    use tokio::io::{AsyncReadExt,AsyncWriteExt};
    #[tokio::test]
    async fn real_key_rotation_and_expired_cache_fail_closed() {
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let issuer=format!("http://127.0.0.1:{}",listener.local_addr().unwrap().port());
        let body=Arc::new(Mutex::new(include_str!("../tests/fixtures/oidc-jwks.json").to_owned()));
        let unavailable=Arc::new(AtomicBool::new(false));
        let (served,failed)=(body.clone(),unavailable.clone());
        let server=tokio::spawn(async move {
            loop {
                let Ok((mut socket,_))=listener.accept().await else {break};
                let mut request=[0u8;8192];socket.read(&mut request).await.unwrap();
                let body=served.lock().await.clone();
                let status=if failed.load(Ordering::SeqCst){"503 Unavailable"}else{"200 OK"};
                socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
        });
        let scope=serde_json::from_value(serde_json::json!({"organization_id":"test-o","tenant_id":"test-t","project_id":"test-p","agent_uid":"test-a"})).unwrap();
        let verifier=OidcVerifier::new(OidcConfig {runtime_endpoint:"runtime-default".into(),oauth:None,gateway_subject:None,issuer:issuer.clone(),audience:"test-api".into(),jwks_url:format!("{issuer}/keys"),scope,jwks_ttl_secs:30,max_token_lifetime_secs:300,allow_loopback_mock:true}).unwrap();
        let now=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        let claims=serde_json::json!({"iss":issuer,"aud":"test-api","sub":"test-user","exp":now+120,"iat":now,"nbf":now,"scope":verifier.scope(),"subject_kind":"human","revocation_epoch":0,"token_use":"access"});
        let sign=|kid:&str,pem:&[u8]| {
            let mut header=jsonwebtoken::Header::new(Algorithm::EdDSA);header.kid=Some(kid.into());
            jsonwebtoken::encode(&header,&claims,&jsonwebtoken::EncodingKey::from_ed_pem(pem).unwrap()).unwrap()
        };
        let old=sign("fixture-key",include_bytes!("../tests/fixtures/oidc-test-private.pem"));
        let new=sign("rotated-test-key",include_bytes!("../tests/fixtures/oidc-rotation-test-private.pem"));
        verifier.verify(&old).await.unwrap();
        *body.lock().await=include_str!("../tests/fixtures/oidc-rotation-jwks.json").into();
        assert!(verifier.verify(&new).await.is_err(),"刷新冷却期不会被新kid绕过");
        // 调整私有缓存时钟，避免测试依赖真实等待30秒；签名和HTTP刷新都真实执行。
        verifier.cache.lock().await.refresh_after=Instant::now();
        verifier.verify(&new).await.unwrap();
        assert!(verifier.verify(&old).await.is_err());
        unavailable.store(true,Ordering::SeqCst);
        verifier.verify(&new).await.unwrap(); // TTL内已签名key仍可用。
        {let mut cache=verifier.cache.lock().await;cache.valid_until=Instant::now()-Duration::from_secs(1);cache.refresh_after=Instant::now();}
        assert!(verifier.verify(&new).await.is_err(),"TTL过期且刷新失败时不能使用stale key");
        server.abort();
    }
}

/// 仅用于选择受保护配置中的 verifier，绝不产生认证身份或授权。
/// 调用方随后必须用该 verifier 完整验签并校验完整 scope。
pub fn unverified_scope_hint(token:&str)->Result<TenantContext,AuthorizationError> {
    use base64::Engine;
    if token.len()>16384 {return Err(AuthorizationError::Unauthenticated)}
    let mut parts=token.split('.');let _header=parts.next().ok_or(AuthorizationError::Unauthenticated)?;
    let payload=parts.next().ok_or(AuthorizationError::Unauthenticated)?;
    if parts.next().is_none() || parts.next().is_some(){return Err(AuthorizationError::Unauthenticated)}
    let bytes=base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload).map_err(|_|AuthorizationError::Unauthenticated)?;
    let value:serde_json::Value=serde_json::from_slice(&bytes).map_err(|_|AuthorizationError::Unauthenticated)?;
    serde_json::from_value(value.get("scope").cloned().ok_or(AuthorizationError::Unauthenticated)?).map_err(|_|AuthorizationError::Unauthenticated)
}
