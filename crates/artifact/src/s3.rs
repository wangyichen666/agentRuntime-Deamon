use agent_core::*;
use agent_storage_ports::ArtifactStore;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use object_store::client::{HttpClient, HttpConnector};
use object_store::{
    ClientOptions, ObjectStore, ObjectStoreExt, PutMode, RetryConfig, aws::AmazonS3Builder,
    path::Path,
};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

// 无 Debug/Serialize：凭据从启动进程传入，不能进入日志或普通配置 DTO。
pub struct S3ArtifactConfig {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub prefix: String,
    pub access_key: String,
    pub secret_key: String,
    pub root_ca_pem: Option<Vec<u8>>,
}
#[derive(Debug)]
struct PinnedConnector(reqwest::Client);
impl HttpConnector for PinnedConnector {
    fn connect(&self, _: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(self.0.clone()))
    }
}
pub struct S3ArtifactStore {
    store: Arc<dyn ObjectStore>,
    id: String,
    prefix: String,
    budget: Semaphore,
}
fn error(_: object_store::Error) -> ArtifactError {
    ArtifactError::StorageUnavailable
}
fn component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
}
impl S3ArtifactStore {
    pub async fn new(config: S3ArtifactConfig) -> Result<Self, ArtifactError> {
        let endpoint =
            url::Url::parse(&config.endpoint).map_err(|_| ArtifactError::InvalidReference)?;
        if endpoint.scheme() != "https"
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || endpoint.path() != "/"
            || !component(&config.bucket)
            || !component(&config.prefix)
            || !component(&config.region)
            || config.access_key.is_empty()
            || config.secret_key.is_empty()
        {
            return Err(ArtifactError::InvalidReference);
        }
        let host = endpoint.host_str().ok_or(ArtifactError::InvalidReference)?;
        let port = endpoint
            .port_or_known_default()
            .ok_or(ArtifactError::InvalidReference)?;
        let addresses = tokio::time::timeout(
            Duration::from_secs(5),
            tokio::net::lookup_host((host, port)),
        )
        .await
        .map_err(|_| ArtifactError::StorageUnavailable)?
        .map_err(|_| ArtifactError::StorageUnavailable)?
        .take(16)
        .collect::<Vec<_>>();
        if addresses.is_empty() {
            return Err(ArtifactError::StorageUnavailable);
        }
        let mut client = reqwest::Client::builder()
            .https_only(true)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(20))
            .resolve_to_addrs(host, &addresses);
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        if let Some(pem) = &config.root_ca_pem {
            use rustls::pki_types::pem::PemObject;
            let mut certificates = 0;
            for certificate in rustls::pki_types::CertificateDer::pem_slice_iter(pem) {
                roots
                    .add(certificate.map_err(|_| ArtifactError::InvalidReference)?)
                    .map_err(|_| ArtifactError::InvalidReference)?;
                certificates += 1;
            }
            if certificates == 0 {
                return Err(ArtifactError::InvalidReference);
            }
        }
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|_| ArtifactError::InvalidReference)?
        .with_root_certificates(roots)
        .with_no_client_auth();
        client = client.use_preconfigured_tls(tls);
        let client = client
            .build()
            .map_err(|_| ArtifactError::StorageUnavailable)?;
        let id = serde_json::to_vec(&(
            &config.endpoint,
            &config.bucket,
            &config.region,
            &config.prefix,
        ))
        .map_err(|_| ArtifactError::InvalidReference)?;
        let store = AmazonS3Builder::new()
            .with_endpoint(&config.endpoint)
            .with_bucket_name(&config.bucket)
            .with_region(&config.region)
            .with_access_key_id(config.access_key)
            .with_secret_access_key(config.secret_key)
            .with_virtual_hosted_style_request(false)
            .with_http_connector(PinnedConnector(client))
            .with_retry(RetryConfig {
                max_retries: 0,
                ..Default::default()
            })
            .build()
            .map_err(error)?;
        Ok(Self {
            store: Arc::new(store),
            id: format!("{:x}", Sha256::digest(id)),
            prefix: config.prefix,
            budget: Semaphore::new(4),
        })
    }
    fn keys(&self, reservation: &ObjectArtifactReservation) -> Result<(Path, Path), ArtifactError> {
        if !reservation.valid() || reservation.store_id != self.id {
            return Err(ArtifactError::Forbidden);
        }
        let scope = serde_json::to_vec(&reservation.envelope.scope)
            .map_err(|_| ArtifactError::InvalidReference)?;
        let scope = format!("{:x}", Sha256::digest(scope));
        Ok((
            Path::from(format!(
                "{}/temporary/{}/{}",
                self.prefix, scope, reservation.publication_id
            )),
            Path::from(format!(
                "{}/objects/{}/{}",
                self.prefix, scope, reservation.identity.sha256
            )),
        ))
    }
    async fn inspect(
        &self,
        key: &Path,
        expected: &ArtifactIdentity,
        offset: u64,
        limit: usize,
    ) -> Result<ArtifactPage, ArtifactError> {
        let response = self.store.get(key).await.map_err(error)?;
        if response.meta.size != expected.bytes || response.meta.size > MAX_ARTIFACT_BYTES {
            return Err(ArtifactError::Corrupt);
        }
        let mut stream = response.into_stream();
        let mut hasher = Sha256::new();
        let mut position = 0u64;
        let mut data = Vec::new();
        let end = offset.saturating_add(limit.clamp(1, 16384) as u64);
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(error)?;
            let next = position
                .checked_add(chunk.len() as u64)
                .filter(|n| *n <= expected.bytes)
                .ok_or(ArtifactError::Corrupt)?;
            hasher.update(&chunk);
            let start = offset.max(position);
            let stop = end.min(next);
            if start < stop {
                data.extend_from_slice(
                    &chunk[(start - position) as usize..(stop - position) as usize],
                );
            }
            position = next;
        }
        let identity = ArtifactIdentity {
            sha256: format!("{:x}", hasher.finalize()),
            bytes: position,
        };
        if identity != *expected {
            return Err(ArtifactError::Corrupt);
        }
        Ok(ArtifactPage { identity, data })
    }
    async fn put_once(&self, key: &Path, body: Bytes) -> Result<(), ArtifactError> {
        match self
            .store
            .put_opts(key, body.into(), PutMode::Create.into())
            .await
        {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. })
            | Err(object_store::Error::Precondition { .. }) => Ok(()),
            Err(e) => Err(error(e)),
        }
    }
}
#[async_trait]
impl ArtifactStore for S3ArtifactStore {
    fn store_id(&self) -> String {
        self.id.clone()
    }
    async fn publish_object(
        &self,
        reservation: &ObjectArtifactReservation,
        body: &str,
    ) -> Result<ArtifactIdentity, ArtifactError> {
        let _permit = tokio::time::timeout(Duration::from_secs(5), self.budget.acquire())
            .await
            .map_err(|_| ArtifactError::StorageUnavailable)?
            .map_err(|_| ArtifactError::StorageUnavailable)?;
        let (temporary, published) = self.keys(reservation)?;
        if body.len() as u64 != reservation.identity.bytes
            || format!("{:x}", Sha256::digest(body.as_bytes())) != reservation.identity.sha256
        {
            return Err(ArtifactError::Corrupt);
        }
        let body = Bytes::copy_from_slice(body.as_bytes());
        let result = async {
            self.put_once(&temporary, body.clone()).await?;
            self.inspect(&temporary, &reservation.identity, 0, 1)
                .await?;
            self.put_once(&published, body).await?;
            let verified = self
                .inspect(&published, &reservation.identity, 0, 1)
                .await?;
            self.store.delete(&temporary).await.map_err(error)?;
            Ok(verified.identity)
        };
        let outcome = tokio::time::timeout(Duration::from_secs(45), result)
            .await
            .unwrap_or(Err(ArtifactError::StorageUnavailable));
        if outcome.is_err() {
            // 清理失败/分区后仍保留原 prepared 元数据，后续只能 reconcile，不能重放工具。
            let _ =
                tokio::time::timeout(Duration::from_secs(5), self.store.delete(&temporary)).await;
        }
        outcome
    }
    async fn read_object(
        &self,
        reservation: &ObjectArtifactReservation,
        offset: u64,
        limit: usize,
    ) -> Result<ArtifactPage, ArtifactError> {
        let _permit = tokio::time::timeout(Duration::from_secs(5), self.budget.acquire())
            .await
            .map_err(|_| ArtifactError::StorageUnavailable)?
            .map_err(|_| ArtifactError::StorageUnavailable)?;
        let (_, published) = self.keys(reservation)?;
        tokio::time::timeout(
            Duration::from_secs(20),
            self.inspect(&published, &reservation.identity, offset, limit),
        )
        .await
        .map_err(|_| ArtifactError::StorageUnavailable)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_storage_ports::reserve_object_artifact;
    fn config(root_ca: bool) -> S3ArtifactConfig {
        let required = |name| std::env::var(name).expect("必须由隔离 TLS S3 lane 提供环境");
        S3ArtifactConfig {
            endpoint: required("AGENT_S3_ENDPOINT"),
            bucket: required("AGENT_S3_BUCKET"),
            region: required("AGENT_S3_REGION"),
            prefix: required("AGENT_S3_PREFIX"),
            access_key: required("AGENT_S3_ACCESS_KEY"),
            secret_key: required("AGENT_S3_SECRET_KEY"),
            root_ca_pem: root_ca.then(|| std::fs::read(required("AGENT_S3_ROOT_CA")).unwrap()),
        }
    }
    fn reservation(store: &S3ArtifactStore, tenant: &str, body: &str) -> ObjectArtifactReservation {
        let mut scope = TenantContext::local();
        scope.tenant_id = tenant.to_owned().try_into().unwrap();
        let owner = ExactOwner {
            session_key: SessionKey("s3-wire".into()),
            session_lifetime_id: SessionLifetimeId("lifetime".into()),
            run_id: RunId("run".into()),
            run_generation: RunGeneration(1),
            turn_id: TurnId("turn".into()),
        };
        let completion = ToolArtifactCompletion {
            round: 1,
            call_id: "call".into(),
            outcome: "success".into(),
            receipt: serde_json::json!({"output_sha256":format!("{:x}",Sha256::digest(body.as_bytes())),"output_bytes":body.len()}),
        };
        reserve_object_artifact(
            EnterpriseOwnerEnvelope { scope, owner },
            &completion,
            ArtifactIdentity {
                sha256: format!("{:x}", Sha256::digest(body.as_bytes())),
                bytes: body.len() as u64,
            },
            store.store_id(),
        )
        .unwrap()
    }
    #[tokio::test]
    #[ignore = "真实隔离 TLS S3 lane"]
    async fn s3_actual_bytes_scope_isolation_tamper_and_certificate_failure() {
        let store = S3ArtifactStore::new(config(true)).await.unwrap();
        let body = "SHA 必须来源于实际读取的 S3 对象";
        let a = reservation(&store, "wire-tenant-a", body);
        let b = reservation(&store, "wire-tenant-b", body);
        assert_ne!(store.keys(&a).unwrap().1, store.keys(&b).unwrap().1);
        store.publish_object(&a, body).await.unwrap();
        store.publish_object(&b, body).await.unwrap();
        let key = store.keys(&a).unwrap().1;
        // 实际对象被覆盖；同长度正文篡改也必须被 GET 后的 SHA 检出，而非相信 ETag。
        let poisoned = vec![b'x'; body.len()];
        store
            .store
            .put(&key, Bytes::from(poisoned.clone()).into())
            .await
            .unwrap();
        assert_eq!(
            store.read_object(&a, 0, 100).await,
            Err(ArtifactError::Corrupt)
        );
        assert_eq!(
            store.publish_object(&a, body).await,
            Err(ArtifactError::Corrupt)
        );
        assert_eq!(
            store
                .store
                .get(&key)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            poisoned.as_slice()
        );
        assert_eq!(
            store.read_object(&b, 0, 100).await.unwrap().data,
            body.as_bytes()
        );
        let bad_tls = S3ArtifactStore::new(config(false)).await.unwrap();
        let c = reservation(&bad_tls, "wire-tenant-c", body);
        assert_eq!(
            bad_tls.publish_object(&c, body).await,
            Err(ArtifactError::StorageUnavailable)
        );
        store
            .store
            .delete(&store.keys(&b).unwrap().1)
            .await
            .unwrap();
        assert_eq!(
            store.read_object(&b, 0, 100).await,
            Err(ArtifactError::StorageUnavailable)
        );
    }
}
