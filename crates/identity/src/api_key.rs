//! API凭据仅落Argon2id PHC；Secret不写日志、receipt或审计。
use agent_core::AuthorizationError;
use argon2::{
    Algorithm, Argon2, Params, PasswordHasher, PasswordVerifier, Version,
    password_hash::{PasswordHash, SaltString},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
fn hasher() -> Result<Argon2<'static>, AuthorizationError> {
    let params =
        Params::new(19456, 2, 1, Some(32)).map_err(|_| AuthorizationError::Unauthenticated)?;
    Ok(Argon2::new(Algorithm::Argon2id, Version::V0x13, params))
}
pub fn generate() -> Result<(String, String, String), AuthorizationError> {
    let mut id = [0_u8; 16];
    let mut secret = [0_u8; 32];
    let mut salt = [0_u8; 16];
    getrandom::fill(&mut id).map_err(|_| AuthorizationError::Unauthenticated)?;
    getrandom::fill(&mut secret).map_err(|_| AuthorizationError::Unauthenticated)?;
    getrandom::fill(&mut salt).map_err(|_| AuthorizationError::Unauthenticated)?;
    let id = id.iter().map(|v| format!("{v:02x}")).collect::<String>();
    let secret = URL_SAFE_NO_PAD.encode(secret);
    let salt = SaltString::encode_b64(&salt).map_err(|_| AuthorizationError::Unauthenticated)?;
    let hash = hasher()?
        .hash_password(secret.as_bytes(), &salt)
        .map_err(|_| AuthorizationError::Unauthenticated)?
        .to_string();
    Ok((id, secret, hash))
}
pub fn parse(token: &str) -> Result<(&str, &str), AuthorizationError> {
    let parts = token.split('.').collect::<Vec<_>>();
    if parts.len() != 3
        || parts[0] != "ak"
        || parts[1].len() != 32
        || !parts[1].bytes().all(|v| v.is_ascii_hexdigit())
        || parts[2].len() != 43
        || !parts[2]
            .bytes()
            .all(|v| v.is_ascii_alphanumeric() || matches!(v, b'-' | b'_'))
    {
        return Err(AuthorizationError::Unauthenticated);
    }
    Ok((parts[1], parts[2]))
}
pub fn verify(secret: &str, phc: &str) -> Result<(), AuthorizationError> {
    if secret.len() != 43 || phc.len() > 256 {
        return Err(AuthorizationError::Unauthenticated);
    }
    let hash = PasswordHash::new(phc).map_err(|_| AuthorizationError::Unauthenticated)?;
    if hash.algorithm.as_str() != "argon2id"
        || hash.version != Some(19)
        || hash.params.get_decimal("m") != Some(19456)
        || hash.params.get_decimal("t") != Some(2)
        || hash.params.get_decimal("p") != Some(1)
        || hash.hash.as_ref().is_none_or(|output| output.len() != 32)
    {
        return Err(AuthorizationError::Unauthenticated);
    }
    hasher()?
        .verify_password(secret.as_bytes(), &hash)
        .map_err(|_| AuthorizationError::Unauthenticated)
}
