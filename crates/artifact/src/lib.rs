//! 对象 I/O 与发布协调。没有工具/run/session 的第二状态机。
#![forbid(unsafe_code)]
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::print_stdout)
)]
mod file;
mod s3;
use agent_core::*;
use agent_storage_ports::{ArtifactPublicationRepository, ArtifactStore, reserve_object_artifact};
pub use file::{FileArtifactDirectory, StagedFile};
pub use s3::{S3ArtifactConfig, S3ArtifactStore};
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub struct ArtifactService {
    repository: Arc<dyn ArtifactPublicationRepository>,
    objects: Arc<dyn ArtifactStore>,
    scope: TenantContext,
}
impl ArtifactService {
    pub fn new(
        repository: Arc<dyn ArtifactPublicationRepository>,
        objects: Arc<dyn ArtifactStore>,
        scope: TenantContext,
    ) -> Self {
        Self {
            repository,
            objects,
            scope,
        }
    }
    pub fn store_id(&self) -> String {
        self.objects.store_id()
    }
    pub async fn complete(
        &self,
        owner: &ExactOwner,
        completion: &ToolArtifactCompletion,
        body: &str,
    ) -> Result<String, ArtifactError> {
        if body.len() as u64 > MAX_ARTIFACT_BYTES {
            return Err(ArtifactError::InvalidReference);
        }
        let identity = ArtifactIdentity {
            sha256: format!("{:x}", Sha256::digest(body.as_bytes())),
            bytes: body.len() as u64,
        };
        let proposed = reserve_object_artifact(
            EnterpriseOwnerEnvelope {
                scope: self.scope.clone(),
                owner: owner.clone(),
            },
            completion,
            identity,
            self.objects.store_id(),
        )?;
        let reserved = self
            .repository
            .reserve_object_artifact(&proposed, completion)
            .await?;
        if !reserved.published {
            let verified = self.objects.publish_object(&reserved, body).await?;
            self.repository
                .commit_object_artifact(&reserved, completion, &verified)
                .await?;
        }
        Ok(reserved.reference())
    }
    pub async fn read(
        &self,
        owner: &ExactOwner,
        reference: &str,
        offset: u64,
        limit: usize,
    ) -> Result<ArtifactPage, ArtifactError> {
        let envelope = EnterpriseOwnerEnvelope {
            scope: self.scope.clone(),
            owner: owner.clone(),
        };
        let reserved = self
            .repository
            .read_object_artifact(&envelope, reference)
            .await?;
        if reserved.store_id != self.objects.store_id() {
            return Err(ArtifactError::StorageUnavailable);
        }
        let page = self.objects.read_object(&reserved, offset, limit).await?;
        if page.identity != reserved.identity {
            return Err(ArtifactError::Corrupt);
        }
        // 网络 I/O 后再次核验原 owner/授权/引用；撤销或 lifetime 退役时不返回正文。
        if self
            .repository
            .read_object_artifact(&envelope, reference)
            .await?
            != reserved
        {
            return Err(ArtifactError::Conflict);
        }
        Ok(page)
    }
}
