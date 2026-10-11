//! 本机最小JWKS mock：签名/域/撤销epoch只是主体事实，最终授权仍在存储事务。
use agent_core::*;
use agent_identity::{OidcConfig, OidcVerifier};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[tokio::test]
async fn verified_signature_scope_and_fresh_keys_fail_closed() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let mock = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let mut input = [0_u8; 8192];
            let _ = stream.read(&mut input).await;
            let body = include_str!("fixtures/oidc-jwks.json");
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body).as_bytes()).await.unwrap();
        }
    });
    let scope: TenantContext = serde_json::from_value(
        serde_json::json!({"organization_id":"o","tenant_id":"t","project_id":"p","agent_uid":"a"}),
    )
    .unwrap();
    let issuer = format!("http://127.0.0.1:{port}");
    let verifier = OidcVerifier::new(OidcConfig {
        runtime_endpoint:"runtime-default".into(),
        oauth: Some(agent_identity::oauth::OAuthCodeConfig {client_id:"login-client".into(), authorization_endpoint:format!("{issuer}/authorize"),token_endpoint:format!("{issuer}/token"),redirect_uri:format!("{issuer}/auth/callback"),requested_scope:"openid".into()}),
        gateway_subject: None,
        issuer: issuer.clone(),
        audience: "agent-api".into(),
        jwks_url: format!("{issuer}/jwks"),
        scope: scope.clone(),
        jwks_ttl_secs: 30,
        max_token_lifetime_secs: 300,
        allow_loopback_mock: true,
    })
    .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut claims = serde_json::json!({"iss":issuer,"aud":"agent-api","sub":"registered-subject","subject_kind":"human","revocation_epoch":7,"scope":scope,"token_use":"access","iat":now,"nbf":now,"exp":now+120});
    let key = EncodingKey::from_ed_pem(include_bytes!("fixtures/oidc-test-private.pem")).unwrap();
    let mut header = Header::new(Algorithm::EdDSA);
    header.kid = Some("fixture-key".into());
    let sign = |claims: &serde_json::Value| jsonwebtoken::encode(&header, claims, &key).unwrap();
    let token = sign(&claims);
    let verified = verifier.verify(&token).await.unwrap();
    assert_eq!(verified.identity.scope, scope);
    assert_eq!(verified.identity.principal.revocation_epoch, 7);
    let mut id_claims=serde_json::json!({"iss":issuer,"aud":"login-client","sub":"registered-subject","exp":now+120,"iat":now,"nonce":"expected-nonce"});
    verifier.verify_id_token(&sign(&id_claims),"expected-nonce",&verified).await.unwrap();
    assert!(verifier.verify_id_token(&sign(&id_claims),"wrong-nonce",&verified).await.is_err());
    id_claims["sub"]=serde_json::json!("different-subject");
    assert!(verifier.verify_id_token(&sign(&id_claims),"expected-nonce",&verified).await.is_err());
    let browser=agent_identity::oauth::BrowserLogin::generate().unwrap();
    assert_eq!(agent_identity::oauth::BrowserLogin::from_cookie(&browser.cookie_value()).unwrap().proof(),browser.proof());
    assert!(verifier.authorization_url(&browser).unwrap().contains("code_challenge_method=S256"));
    claims["aud"] = serde_json::json!("other-api");
    assert!(verifier.verify(&sign(&claims)).await.is_err());
    claims["aud"] = serde_json::json!("agent-api");
    claims["scope"]["tenant_id"] = serde_json::json!("foreign");
    assert!(verifier.verify(&sign(&claims)).await.is_err());
    claims["scope"] = serde_json::json!(scope);
    claims["exp"] = serde_json::json!(now - 1);
    assert!(verifier.verify(&sign(&claims)).await.is_err());
    claims["exp"] = serde_json::json!(now + 120);
    claims["nbf"] = serde_json::json!(now + 60);
    assert!(verifier.verify(&sign(&claims)).await.is_err());
    claims["nbf"] = serde_json::json!(now);
    claims["token_use"] = serde_json::json!("id");
    assert!(verifier.verify(&sign(&claims)).await.is_err());
    let forged = format!("{}x", token);
    assert!(verifier.verify(&forged).await.is_err());
    let mut missing = Header::new(Algorithm::EdDSA);
    missing.kid = Some("unknown-key".into());
    let missing = jsonwebtoken::encode(&missing, &claims, &key).unwrap();
    assert!(verifier.verify(&missing).await.is_err());
    mock.abort();
}
