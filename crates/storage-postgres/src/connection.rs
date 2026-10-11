//! 连接配置只接受文件引用；无含凭据DSN、verify-none或明文TCP回退。
use crate::{PgPublicationStore, identifier, unavailable};
use agent_core::CoordinationError;
use serde::{Deserialize, Serialize};
use std::{
    io::Read,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PgConnectionConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub schema: String,
    pub root_ca_file: PathBuf,
    pub password_file: Option<PathBuf>,
    pub client_certificate_file: Option<PathBuf>,
    pub client_key_file: Option<PathBuf>,
    pub connect_timeout_ms: u64,
}
fn file(path: &Path, secret: bool) -> Result<Vec<u8>, CoordinationError> {
    // 配置由操作员管理；拒绝symlink与非普通文件，凭据必须仅当前用户可读。
    let meta = std::fs::symlink_metadata(path).map_err(|_| CoordinationError::InvalidRequest)?;
    if !meta.is_file() || meta.len() > 1024 * 1024 || !path.is_absolute() {
        return Err(CoordinationError::InvalidRequest);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        if secret && meta.mode() & 0o077 != 0 {
            return Err(CoordinationError::InvalidRequest);
        }
        // libc的O_NOFOLLOW在各Unix平台不同；使用rustix共享文件设施而非裸数值。
        let mut options = std::fs::OpenOptions::new();
        options
            .read(true)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32);
        let mut handle = options
            .open(path)
            .map_err(|_| CoordinationError::InvalidRequest)?;
        let opened = handle
            .metadata()
            .map_err(|_| CoordinationError::InvalidRequest)?;
        if meta.dev() != opened.dev()
            || meta.ino() != opened.ino()
            || (secret && opened.mode() & 0o077 != 0)
        {
            return Err(CoordinationError::InvalidRequest);
        }
        let mut bytes = Vec::new();
        handle
            .by_ref()
            .take(1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| CoordinationError::InvalidRequest)?;
        if bytes.len() > 1024 * 1024 {
            return Err(CoordinationError::InvalidRequest);
        }
        Ok(bytes)
    }
    #[cfg(not(unix))]
    {
        if secret {
            return Err(CoordinationError::InvalidRequest);
        }
        std::fs::read(path).map_err(|_| CoordinationError::InvalidRequest)
    }
}
impl PgConnectionConfig {
    pub fn from_file(path: &Path) -> Result<Self, CoordinationError> {
        let config: Self = serde_json::from_slice(&file(path, true)?)
            .map_err(|_| CoordinationError::InvalidRequest)?;
        config.validate()?;
        Ok(config)
    }
    pub fn from_env() -> Result<Self, CoordinationError> {
        let path =
            std::env::var_os("AGENT_POSTGRES_CONFIG").ok_or(CoordinationError::InvalidRequest)?;
        Self::from_file(Path::new(&path))
    }
    pub fn validate(&self) -> Result<(), CoordinationError> {
        if self.host.is_empty()
            || self.host.len() > 253
            || self.host.starts_with('/')
            || self.host.contains([' ', '/', '\\', '@', '\n', '\r'])
            || self.port == 0
            || !(100..=30000).contains(&self.connect_timeout_ms)
            || ![&self.database, &self.user, &self.schema]
                .into_iter()
                .all(|s| identifier(s))
            || self.schema == "public"
            || self.schema.starts_with("pg_")
            || self.client_certificate_file.is_some() != self.client_key_file.is_some()
        {
            return Err(CoordinationError::InvalidRequest);
        }
        Ok(())
    }
    fn prepare(&self) -> Result<(tokio_postgres::Config, rustls::ClientConfig), CoordinationError> {
        self.validate()?;
        let ca = file(&self.root_ca_file, false)?;
        let mut roots = rustls::RootCertStore::empty();
        let mut count = 0;
        for cert in rustls_pemfile::certs(&mut ca.as_slice()) {
            roots
                .add(cert.map_err(|_| CoordinationError::InvalidRequest)?)
                .map_err(|_| CoordinationError::InvalidRequest)?;
            count += 1;
        }
        if count == 0 {
            return Err(CoordinationError::InvalidRequest);
        }
        let builder = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|_| CoordinationError::InvalidRequest)?
        .with_root_certificates(roots);
        let tls = match (&self.client_certificate_file, &self.client_key_file) {
            (Some(cert), Some(key)) => {
                let cert = file(cert, false)?;
                let key = file(key, true)?;
                let certs = rustls_pemfile::certs(&mut cert.as_slice())
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|_| CoordinationError::InvalidRequest)?;
                let key = rustls_pemfile::private_key(&mut key.as_slice())
                    .map_err(|_| CoordinationError::InvalidRequest)?
                    .ok_or(CoordinationError::InvalidRequest)?;
                builder
                    .with_client_auth_cert(certs, key)
                    .map_err(|_| CoordinationError::InvalidRequest)?
            }
            (None, None) => builder.with_no_client_auth(),
            _ => return Err(CoordinationError::InvalidRequest),
        };
        let mut pg = tokio_postgres::Config::new();
        pg.host(&self.host)
            .port(self.port)
            .dbname(&self.database)
            .user(&self.user)
            .application_name("agent-runtime")
            .ssl_mode(tokio_postgres::config::SslMode::Require)
            .connect_timeout(Duration::from_millis(self.connect_timeout_ms));
        if let Some(path) = &self.password_file {
            let password = file(path, true)?;
            if password.is_empty() || password.len() > 4096 || password.contains(&0) {
                return Err(CoordinationError::InvalidRequest);
            }
            pg.password(password);
        }
        Ok((pg, tls))
    }
}
impl PgPublicationStore {
    pub async fn connect_remote(config: &PgConnectionConfig) -> Result<Self, CoordinationError> {
        let cloned = config.clone();
        let (pg, tls) = tokio::task::spawn_blocking(move || cloned.prepare())
            .await
            .map_err(|_| CoordinationError::StorageUnavailable)??;
        let (client, connection) = tokio::time::timeout(
            Duration::from_millis(config.connect_timeout_ms),
            pg.connect(tokio_postgres_rustls::MakeRustlsConnect::new(tls)),
        )
        .await
        .map_err(|_| CoordinationError::StorageUnavailable)?
        .map_err(unavailable)?;
        let driver = tokio::spawn(async move {
            if connection.await.is_err() {
                tracing::warn!("PostgreSQL连接已断开，未重放事务");
            }
        });
        Self::initialize(client, driver, &config.schema).await
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_unsafe_config_and_partial_mtls() {
        let mut c = PgConnectionConfig {
            host: "database.example".into(),
            port: 5432,
            database: "agent".into(),
            user: "agent".into(),
            schema: "canonical".into(),
            root_ca_file: "/ca.pem".into(),
            password_file: None,
            client_certificate_file: None,
            client_key_file: None,
            connect_timeout_ms: 5000,
        };
        assert!(c.validate().is_ok());
        c.host = "/tmp".into();
        assert!(c.validate().is_err());
        c.host = "database.example".into();
        c.client_key_file = Some("/key.pem".into());
        assert!(c.validate().is_err());
        c.client_key_file = None;
        c.schema = "public".into();
        assert!(c.validate().is_err());
    }
}
