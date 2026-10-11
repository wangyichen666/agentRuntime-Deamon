//! 离线canonical全表导入：只进入空schema，任何列/摘要/约束不一致整事务回滚。
use crate::canonical::{set_scope, unsigned};
use crate::{PgCanonicalStore, SCOPE, identifier, parameters, scope_values, unavailable};
use agent_core::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, io::Read, path::Path};
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OfflineTable {
    pub name: String,
    pub columns: Vec<String>,
    pub count: u64,
    pub chunks: Vec<OfflineChunk>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OfflineChunk {
    pub file: String,
    pub sha256: String,
    pub rows: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OfflineManifest {
    pub version: u32,
    pub source_schema: u32,
    pub migration_id: String,
    pub source_digest: String,
    pub scope: TenantContext,
    pub tables: Vec<OfflineTable>,
    pub artifacts: Vec<ArtifactIdentity>,
}
fn read(path: &Path, max: u64) -> Result<Vec<u8>, CoordinationError> {
    use std::os::unix::fs::OpenOptionsExt;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
        .open(path)
        .map_err(|_| CoordinationError::InvalidRequest)?;
    if !file
        .metadata()
        .map_err(|_| CoordinationError::InvalidRequest)?
        .is_file()
    {
        return Err(CoordinationError::InvalidRequest);
    }
    let mut bytes = Vec::new();
    file.take(max + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| CoordinationError::InvalidRequest)?;
    if bytes.len() as u64 > max {
        return Err(CoordinationError::InvalidRequest);
    }
    Ok(bytes)
}
impl PgCanonicalStore {
    pub async fn offline_shape(&self) -> Result<serde_json::Value, CoordinationError> {
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        let rows=tx.query("SELECT table_name,column_name,data_type,is_nullable,column_default,is_identity FROM information_schema.columns WHERE table_schema=current_schema() AND table_name NOT IN ('canonical_migrations','coordination_migrations','offline_migration_receipts') ORDER BY table_name,ordinal_position",&[]).await.map_err(unavailable)?;
        let mut tables: BTreeMap<String, Vec<serde_json::Value>> = BTreeMap::new();
        for r in rows {
            let table: String = r.try_get(0).map_err(unavailable)?;
            tables.entry(table).or_default().push(serde_json::json!({"name":r.try_get::<_,String>(1).map_err(unavailable)?,"type":r.try_get::<_,String>(2).map_err(unavailable)?,"nullable":r.try_get::<_,String>(3).map_err(unavailable)?=="YES","default":r.try_get::<_,Option<String>>(4).map_err(unavailable)?,"identity":r.try_get::<_,String>(5).map_err(unavailable)?=="YES"}));
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(serde_json::json!({"canonical_schema":9,"tables":tables}))
    }
    pub async fn import_offline(
        &self,
        directory: &Path,
        dry_run: bool,
    ) -> Result<serde_json::Value, CoordinationError> {
        let root = directory.to_owned();
        let manifest =
            tokio::task::spawn_blocking(move || read(&root.join("manifest.json"), 1024 * 1024))
                .await
                .map_err(|_| CoordinationError::StorageUnavailable)??;
        let m: OfflineManifest =
            serde_json::from_slice(&manifest).map_err(|_| CoordinationError::InvalidRequest)?;
        if m.version != 1
            || !matches!(m.source_schema, 28 | 29 | 30 | 31 | 32 | 33 | 34 | 35)
            || m.scope != TenantContext::local()
            || !identifier(&m.migration_id)
            || m.tables.len() > 128
            || m.source_digest.len() != 64
            || m.artifacts.len() > 65536
        {
            return Err(CoordinationError::InvalidRequest);
        }
        let shape = self.offline_shape().await?;
        let shapes = shape["tables"]
            .as_object()
            .ok_or(CoordinationError::Corrupt)?;
        if m.tables.len() != shapes.len() {
            return Err(CoordinationError::InvalidRequest);
        }
        let mut names = std::collections::BTreeSet::new();
        let mut files = std::collections::BTreeSet::new();
        for t in &m.tables {
            if !identifier(&t.name)
                || !names.insert(&t.name)
                || !shapes.contains_key(&t.name)
                || t.columns.is_empty()
                || t.columns.len() > 64
                || t.count > 10000000
                || t.chunks.len() > 65536
            {
                return Err(CoordinationError::InvalidRequest);
            }
            let allowed = shapes[&t.name]
                .as_array()
                .ok_or(CoordinationError::Corrupt)?;
            let mut columns = std::collections::BTreeSet::new();
            for c in &t.columns {
                if !identifier(c) || !columns.insert(c) || !allowed.iter().any(|v| v["name"] == *c)
                {
                    return Err(CoordinationError::InvalidRequest);
                }
            }
            if t.chunks
                .iter()
                .try_fold(0u64, |n, c| n.checked_add(c.rows))
                .ok_or(CoordinationError::InvalidRequest)?
                != t.count
            {
                return Err(CoordinationError::InvalidRequest);
            }
            for c in &t.chunks {
                if c.file != format!("{}.json", c.file.trim_end_matches(".json"))
                    || !identifier(c.file.trim_end_matches(".json"))
                    || !files.insert(&c.file)
                    || c.rows > 128
                    || c.sha256.len() != 64
                {
                    return Err(CoordinationError::InvalidRequest);
                }
            }
        }
        // 校验对象正文，保留原目录store_id；不是将引用改写成新的store。
        let artifact_dir = directory.join("artifacts");
        for artifact in &m.artifacts {
            if !artifact.valid() {
                return Err(CoordinationError::InvalidRequest);
            }
            let artifact = artifact.clone();
            let path = artifact_dir.join(format!("sha256-{}.txt", artifact.sha256));
            tokio::task::spawn_blocking(move || {
                let bytes = read(&path, 67108864)?;
                if bytes.len() as u64 != artifact.bytes
                    || format!("{:x}", Sha256::digest(&bytes)) != artifact.sha256
                {
                    return Err(CoordinationError::Corrupt);
                }
                Ok(())
            })
            .await
            .map_err(|_| CoordinationError::StorageUnavailable)??;
        }
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &m.scope).await?;
        tx.batch_execute("SET LOCAL statement_timeout='60s'; SET LOCAL idle_in_transaction_session_timeout='120s'; LOCK TABLE organizations IN EXCLUSIVE MODE; SET CONSTRAINTS ALL DEFERRED").await.map_err(unavailable)?;
        let exists: bool = tx
            .query_one("SELECT EXISTS(SELECT 1 FROM organizations)", &[])
            .await
            .map_err(unavailable)?
            .try_get(0)
            .map_err(unavailable)?;
        if exists {
            return Err(CoordinationError::Conflict);
        }
        // 只暂时关闭用户派生trigger；FK与CHECK仍执行。rollback会恢复trigger定义。
        for t in &m.tables {
            tx.batch_execute(&format!("ALTER TABLE \"{}\" DISABLE TRIGGER USER", t.name))
                .await
                .map_err(unavailable)?;
        }
        let order = [
            "organizations",
            "tenants",
            "projects",
            "agents",
            "principals",
            "role_bindings",
            "identity_policies",
            "identity_policy_heads",
            "sessions",
            "session_scopes",
            "session_lifetime_owners",
        ];
        let mut tables = m.tables.iter().collect::<Vec<_>>();
        tables.sort_by_key(|t| {
            (
                order
                    .iter()
                    .position(|n| *n == t.name)
                    .unwrap_or(order.len()),
                t.name.clone(),
            )
        });
        let mut evidence = Vec::new();
        for t in tables {
            let columns = t
                .columns
                .iter()
                .map(|c| format!("\"{c}\""))
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "INSERT INTO \"{}\"({columns}) SELECT {columns} FROM jsonb_populate_record(NULL::\"{}\",$1::text::jsonb)",
                t.name, t.name
            );
            for c in &t.chunks {
                let path = directory.join(&c.file);
                let bytes = tokio::task::spawn_blocking(move || read(&path, 32 * 1024 * 1024))
                    .await
                    .map_err(|_| CoordinationError::StorageUnavailable)??;
                if format!("{:x}", Sha256::digest(&bytes)) != c.sha256 {
                    return Err(CoordinationError::Corrupt);
                }
                let rows: Vec<serde_json::Value> =
                    serde_json::from_slice(&bytes).map_err(|_| CoordinationError::Corrupt)?;
                if rows.len() as u64 != c.rows {
                    return Err(CoordinationError::Corrupt);
                }
                for row in rows {
                    let object = row.as_object().ok_or(CoordinationError::Corrupt)?;
                    if object.len() != t.columns.len()
                        || t.columns.iter().any(|k| !object.contains_key(k))
                    {
                        return Err(CoordinationError::Corrupt);
                    }
                    for (key, value) in scope_values(&m.scope).into_iter().zip([
                        "organization_id",
                        "tenant_id",
                        "project_id",
                        "agent_uid",
                    ]) {
                        if object
                            .get(value)
                            .is_some_and(|v| v != &serde_json::Value::String(key))
                        {
                            return Err(CoordinationError::Forbidden);
                        }
                    }
                    let raw =
                        serde_json::to_string(&row).map_err(|_| CoordinationError::Corrupt)?;
                    tx.execute(&sql, &[&raw]).await.map_err(unavailable)?;
                    let matches:i64=tx.query_one(&format!("SELECT count(*) FROM \"{}\" r WHERE to_jsonb(r) @> $1::text::jsonb",t.name),&[&raw]).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
                    if matches != 1 {
                        return Err(CoordinationError::Corrupt);
                    }
                }
            }
            let count: i64 = tx
                .query_one(&format!("SELECT count(*) FROM \"{}\"", t.name), &[])
                .await
                .map_err(unavailable)?
                .try_get(0)
                .map_err(unavailable)?;
            if unsigned(count)? != t.count {
                return Err(CoordinationError::Corrupt);
            }
            evidence.push(
                serde_json::json!({"table":t.name,"count":count,"chunks":t.chunks,"verified":true}),
            );
            // 恢复identity sequence，防止运行后id与迁移rowid冲突。
            for col in shapes[&t.name]
                .as_array()
                .ok_or(CoordinationError::Corrupt)?
                .iter()
                .filter(|c| {
                    c["identity"] == true
                        || c["default"]
                            .as_str()
                            .is_some_and(|d| d.starts_with("nextval("))
                })
            {
                if dry_run {
                    continue;
                }
                let name = col["name"].as_str().ok_or(CoordinationError::Corrupt)?;
                let table = &t.name;
                tx.query_one(&format!("SELECT setval(pg_get_serial_sequence($1,$2),GREATEST(COALESCE((SELECT max(\"{name}\") FROM \"{table}\"),0)+1,1),false)"),&[&table,&name]).await.map_err(unavailable)?;
            }
        }
        tx.batch_execute("SET CONSTRAINTS ALL IMMEDIATE")
            .await
            .map_err(unavailable)?;
        for t in &m.tables {
            tx.batch_execute(&format!("ALTER TABLE \"{}\" ENABLE TRIGGER USER", t.name))
                .await
                .map_err(unavailable)?;
        }
        let mut v = scope_values(&m.scope);
        v.extend([
            m.migration_id.clone(),
            m.source_digest.clone(),
            String::from_utf8(manifest).map_err(|_| CoordinationError::Corrupt)?,
            crate::now(&tx).await?.to_string(),
        ]);
        tx.execute("INSERT INTO offline_migration_receipts VALUES($1,$2,$3,$4,$5,$6,$7,'verified',$8::text::bigint)",&parameters(&v)).await.map_err(unavailable)?;
        if dry_run {
            tx.rollback().await.map_err(unavailable)?;
        } else {
            tx.commit().await.map_err(unavailable)?;
        }
        Ok(
            serde_json::json!({"migration_id":m.migration_id,"dry_run":dry_run,"tables":evidence,"artifact_count":m.artifacts.len(),"verified":true}),
        )
    }
}
impl PgCanonicalStore {
    /// 无写入核验；rollback只有目标仍等于离线快照时才可授权，不回滚新业务事实。
    pub async fn verify_offline(
        &self,
        directory: &Path,
    ) -> Result<serde_json::Value, CoordinationError> {
        self.verify_or_rollback_offline(directory, false).await
    }
    pub async fn rollback_offline(
        &self,
        directory: &Path,
    ) -> Result<serde_json::Value, CoordinationError> {
        self.verify_or_rollback_offline(directory, true).await
    }
    async fn verify_or_rollback_offline(
        &self,
        directory: &Path,
        rollback: bool,
    ) -> Result<serde_json::Value, CoordinationError> {
        let path = directory.join("manifest.json");
        let manifest = tokio::task::spawn_blocking(move || read(&path, 1024 * 1024))
            .await
            .map_err(|_| CoordinationError::StorageUnavailable)??;
        let m: OfflineManifest =
            serde_json::from_slice(&manifest).map_err(|_| CoordinationError::InvalidRequest)?;
        if m.version != 1
            || !matches!(m.source_schema, 28 | 29 | 30 | 31 | 32 | 33 | 34 | 35)
            || m.scope != TenantContext::local()
            || !identifier(&m.migration_id)
            || m.tables.len() > 128
        {
            return Err(CoordinationError::InvalidRequest);
        }
        let shape = self.offline_shape().await?;
        let mut client = self.inner.client.lock().await;
        let tx = client
            .build_transaction()
            // 回滚等待独占栅栏后必须看到刚提交的事实；RepeatableRead会在等待
            // advisory lock前固定快照，可能误把新事实当作未发生。
            .isolation_level(if rollback {
                tokio_postgres::IsolationLevel::ReadCommitted
            } else {
                tokio_postgres::IsolationLevel::RepeatableRead
            })
            .read_only(!rollback)
            .start()
            .await
            .map_err(unavailable)?;
        if rollback {
            tx.query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended(current_schema(),55))",
                &[],
            )
            .await
            .map_err(unavailable)?;
        }
        set_scope(&tx, &m.scope).await?;
        let mut v = scope_values(&m.scope);
        v.push(m.migration_id.clone());
        let saved=tx.query_opt(&format!("SELECT source_digest,manifest_json FROM offline_migration_receipts WHERE {SCOPE} AND migration_id=$5 AND state='verified'"),&parameters(&v)).await.map_err(unavailable)?.ok_or(CoordinationError::Forbidden)?;
        if saved.try_get::<_, String>(0).map_err(unavailable)? != m.source_digest
            || serde_json::from_str::<serde_json::Value>(
                &saved.try_get::<_, String>(1).map_err(unavailable)?,
            )
            .map_err(|_| CoordinationError::Corrupt)?
                != serde_json::from_slice::<serde_json::Value>(&manifest)
                    .map_err(|_| CoordinationError::Corrupt)?
        {
            return Err(CoordinationError::Corrupt);
        }
        let mut evidence = Vec::new();
        for t in &m.tables {
            if !identifier(&t.name)
                || shape["tables"].get(&t.name).is_none()
                || t.count > 10000000
                || t.chunks.len() > 65536
            {
                return Err(CoordinationError::InvalidRequest);
            }
            let count: i64 = tx
                .query_one(&format!("SELECT count(*) FROM \"{}\"", t.name), &[])
                .await
                .map_err(unavailable)?
                .try_get(0)
                .map_err(unavailable)?;
            if unsigned(count)? != t.count {
                return Err(CoordinationError::Conflict);
            }
            for c in &t.chunks {
                if !identifier(c.file.trim_end_matches(".json"))
                    || c.file != format!("{}.json", c.file.trim_end_matches(".json"))
                    || c.rows > 128
                {
                    return Err(CoordinationError::InvalidRequest);
                }
                let path = directory.join(&c.file);
                let bytes = tokio::task::spawn_blocking(move || read(&path, 32 * 1024 * 1024))
                    .await
                    .map_err(|_| CoordinationError::StorageUnavailable)??;
                if format!("{:x}", Sha256::digest(&bytes)) != c.sha256 {
                    return Err(CoordinationError::Corrupt);
                }
                let rows: Vec<serde_json::Value> =
                    serde_json::from_slice(&bytes).map_err(|_| CoordinationError::Corrupt)?;
                if rows.len() as u64 != c.rows {
                    return Err(CoordinationError::Corrupt);
                }
                for row in rows {
                    let raw =
                        serde_json::to_string(&row).map_err(|_| CoordinationError::Corrupt)?;
                    let count:i64=tx.query_one(&format!("SELECT count(*) FROM \"{}\" r WHERE to_jsonb(r) @> $1::text::jsonb",t.name),&[&raw]).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
                    if count != 1 {
                        return Err(CoordinationError::Conflict);
                    }
                }
            }
            evidence.push(serde_json::json!({"table":t.name,"count":count,"verified":true}));
        }
        if rollback {
            let active:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM runs WHERE status IN ('queued','running','waiting_interaction')) OR EXISTS(SELECT 1 FROM session_writer_leases WHERE expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint)",&[]).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
            if active {
                return Err(CoordinationError::Conflict);
            }
            tx.execute(&format!("UPDATE offline_migration_receipts SET state='rolled_back' WHERE {SCOPE} AND migration_id=$5 AND state='verified'"),&parameters(&v)).await.map_err(unavailable)?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(
            serde_json::json!({"migration_id":m.migration_id,"source_digest":m.source_digest,"tables":evidence,"verified":true,"rollback_safe":true,"rolled_back":rollback}),
        )
    }
}

impl PgCanonicalStore {
    pub async fn validate_cutover_receipt(
        &self,
        migration: &str,
        digest: &str,
    ) -> Result<(), CoordinationError> {
        if !identifier(migration) || digest.len() != 64 {
            return Err(CoordinationError::InvalidRequest);
        }
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, &TenantContext::local()).await?;
        let mut v = scope_values(&TenantContext::local());
        v.extend([migration.into(), digest.into()]);
        let exists:bool=tx.query_one(&format!("SELECT EXISTS(SELECT 1 FROM offline_migration_receipts WHERE {SCOPE} AND migration_id=$5 AND source_digest=$6 AND state='verified')"),&parameters(&v)).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
        if !exists {
            return Err(CoordinationError::Forbidden);
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }
}
