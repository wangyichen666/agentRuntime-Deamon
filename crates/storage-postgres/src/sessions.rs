//! 原session lifecycle/receipt/历史批次的PostgreSQL实现；writer proof与事实在同一事务复核。
use crate::canonical::{
    PgCanonicalStore, field, grant_in, key_values, permanent_life, proof_key, set_scope, unsigned,
};
use crate::{SCOPE, now, parameters, scope_values, unavailable};
use agent_core::*;
use agent_storage_ports::{SessionControlRepository, SessionLifecycleRepository};
use sha2::{Digest, Sha256};
use tokio_postgres::{GenericClient, Transaction};
fn valid_key(key: &SessionKey) -> bool {
    !key.0.is_empty() && key.0.len() <= 1024
}
pub(crate) fn encode<T: serde::Serialize + ?Sized>(
    value: &T,
    budget: usize,
) -> Result<String, CoordinationError> {
    struct Bytes {
        remaining: usize,
        bytes: Vec<u8>,
    }
    impl std::io::Write for Bytes {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            if data.len() > self.remaining {
                return Err(std::io::Error::other("JSON超出预算"));
            }
            self.remaining -= data.len();
            self.bytes.extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut output = Bytes {
        remaining: budget,
        bytes: Vec::new(),
    };
    serde_json::to_writer(&mut output, value).map_err(|_| CoordinationError::InvalidRequest)?;
    String::from_utf8(output.bytes).map_err(|_| CoordinationError::Corrupt)
}
pub(crate) async fn metadata_in<C: GenericClient + Sync>(
    db: &C,
    scope: &TenantContext,
    key: &SessionKey,
    lock: bool,
) -> Result<Option<SessionMetadata>, CoordinationError> {
    let mut values = scope_values(scope);
    values.push(key.0.clone());
    let suffix = if lock { " FOR UPDATE" } else { "" };
    let row=db.query_opt(&format!("SELECT CASE WHEN octet_length(lifetime) BETWEEN 1 AND 128 THEN lifetime END,deleted,revision,updated_at_ms,legacy_imported FROM session_heads WHERE {SCOPE} AND session_id=$5{suffix}"),&parameters(&values)).await.map_err(unavailable)?;
    let value = row
        .map(|row| {
            Ok(SessionMetadata {
                key: key.clone(),
                lifetime: SessionLifetimeId(field(&row, 0)?),
                deleted: row.try_get(1).map_err(unavailable)?,
                revision: TranscriptSeq(unsigned(row.try_get(2).map_err(unavailable)?)?),
                updated_at_ms: row.try_get(3).map_err(unavailable)?,
                legacy_imported: row.try_get(4).map_err(unavailable)?,
            })
        })
        .transpose()?;
    if let Some(meta) = &value {
        permanent_life(
            db,
            &SessionWriterLeaseKey {
                scope: scope.clone(),
                session_key: key.clone(),
                session_lifetime_id: meta.lifetime.clone(),
            },
        )
        .await?;
    }
    Ok(value)
}
async fn lifetime(db: &Transaction<'_>) -> Result<SessionLifetimeId, CoordinationError> {
    Ok(SessionLifetimeId(
        db.query_one("SELECT gen_random_uuid()::text", &[])
            .await
            .map_err(unavailable)?
            .try_get(0)
            .map_err(unavailable)?,
    ))
}
async fn publish_lifecycle(
    db: &Transaction<'_>,
    scope: &TenantContext,
    meta: &SessionMetadata,
    event: HookEvent,
) -> Result<(), CoordinationError> {
    let event_label = serde_json::to_string(&event).map_err(|_| CoordinationError::Corrupt)?;
    let publication = HookPublication {
        session_key: meta.key.clone(),
        session_lifetime_id: meta.lifetime.clone(),
        event,
        operation_id: format!("lifecycle:{event_label}:{}", meta.lifetime.0),
    };
    let mut values = scope_values(scope);
    values.extend([
        publication.operation_id.clone(),
        encode(&publication, 16384)?,
    ]);
    db.execute("INSERT INTO hook_publications(organization_id,tenant_id,project_id,agent_uid,operation_id,payload_json,dispatched) VALUES($1,$2,$3,$4,$5,$6,FALSE) ON CONFLICT DO NOTHING",&parameters(&values)).await.map_err(unavailable)?;
    Ok(())
}
pub(crate) async fn create_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    key: &SessionKey,
) -> Result<SessionMetadata, CoordinationError> {
    let old = metadata_in(db, scope, key, true).await?;
    if old.as_ref().is_some_and(|meta| !meta.deleted) {
        return Err(CoordinationError::Conflict);
    }
    let at = now(db).await?;
    let mut values = scope_values(scope);
    values.extend([key.0.clone(), at.to_string()]);
    db.execute("INSERT INTO sessions(organization_id,tenant_id,project_id,agent_uid,id,created_at_ms) VALUES($1,$2,$3,$4,$5,$6::text::bigint) ON CONFLICT DO NOTHING",&parameters(&values)).await.map_err(unavailable)?;
    db.execute(
        "INSERT INTO session_scopes VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING",
        &parameters(&values[..5]),
    )
    .await
    .map_err(unavailable)?;
    let life = lifetime(db).await?;
    let mut binding = scope_values(scope);
    binding.extend([life.0.clone(), key.0.clone()]);
    db.execute(
        "INSERT INTO session_lifetime_owners VALUES($1,$2,$3,$4,$5,$6,'canonical')",
        &parameters(&binding),
    )
    .await
    .map_err(unavailable)?;
    let mut head = scope_values(scope);
    head.extend([key.0.clone(), life.0.clone(), at.to_string()]);
    if old.is_some() {
        db.execute(&format!("UPDATE session_heads SET lifetime=$6,deleted=FALSE,revision=0,projection_generation=projection_generation+1,legacy_imported=TRUE,updated_at_ms=$7::text::bigint WHERE {SCOPE} AND session_id=$5"),&parameters(&head)).await.map_err(unavailable)?;
    } else {
        db.execute("INSERT INTO session_heads(organization_id,tenant_id,project_id,agent_uid,session_id,lifetime,updated_at_ms,legacy_imported) VALUES($1,$2,$3,$4,$5,$6,$7::text::bigint,TRUE)",&parameters(&head)).await.map_err(unavailable)?;
    }
    let meta = metadata_in(db, scope, key, false)
        .await?
        .ok_or(CoordinationError::Corrupt)?;
    crate::registry::bind_in(db,scope,key,&meta.lifetime).await?;
    publish_lifecycle(db, scope, &meta, HookEvent::SessionStart).await?;
    Ok(meta)
}
pub(crate) async fn ensure_idle(
    db: &Transaction<'_>,
    scope: &TenantContext,
    meta: &SessionMetadata,
) -> Result<(), CoordinationError> {
    let key = SessionWriterLeaseKey {
        scope: scope.clone(),
        session_key: meta.key.clone(),
        session_lifetime_id: meta.lifetime.clone(),
    };
    let values = key_values(&key);
    let busy:bool=db.query_one(&format!("SELECT EXISTS(SELECT 1 FROM runs WHERE {SCOPE} AND session_id=$5 AND lifetime=$6 AND status IN ('queued','running','waiting_interaction')) OR EXISTS(SELECT 1 FROM resources WHERE {SCOPE} AND lifetime=$6 AND state IN ('starting','running','stopping'))"),&parameters(&values)).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;
    if busy {
        return Err(CoordinationError::Conflict);
    }
    Ok(())
}
pub(crate) async fn transcript_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    meta: &SessionMetadata,
) -> Result<(Vec<Message>, Vec<(u64, u64)>), CoordinationError> {
    let mut values = scope_values(scope);
    values.push(meta.lifetime.0.clone());
    let budget=db.query_one(&format!("SELECT count(*)::bigint,COALESCE(sum(octet_length(messages_json)),0)::bigint,COALESCE(sum(end_seq-start_seq),0)::bigint FROM (SELECT messages_json,start_seq,end_seq FROM transcript_batches WHERE {SCOPE} AND lifetime=$5 LIMIT 32769) b"),&parameters(&values)).await.map_err(unavailable)?;
    let count: i64 = budget.try_get(0).map_err(unavailable)?;
    let bytes: i64 = budget.try_get(1).map_err(unavailable)?;
    let messages: i64 = budget.try_get(2).map_err(unavailable)?;
    if count > 32768 || bytes > 16 * 1024 * 1024 || !(0..=32768).contains(&messages) {
        return Err(CoordinationError::Corrupt);
    }
    let rows=db.query(&format!("SELECT start_seq,end_seq,CASE WHEN octet_length(messages_json)<=16777216 THEN messages_json END,CASE WHEN octet_length(digest)=64 THEN digest END FROM transcript_batches WHERE {SCOPE} AND lifetime=$5 ORDER BY start_seq,storage_row_id LIMIT 32768"),&parameters(&values)).await.map_err(unavailable)?;
    let mut output = Vec::new();
    let mut ranges = Vec::new();
    let mut previous = 0;
    for row in rows {
        let start = unsigned(row.try_get(0).map_err(unavailable)?)?;
        let end = unsigned(row.try_get(1).map_err(unavailable)?)?;
        let raw = field(&row, 2)?;
        if start != previous {
            return Err(CoordinationError::Corrupt);
        }
        let batch =
            agent_storage_ports::decode_transcript_batch(&raw, &field(&row, 3)?, start, end)?;
        output.extend(batch);
        ranges.push((start, end));
        previous = end;
    }
    if previous != meta.revision.0 {
        return Err(CoordinationError::Corrupt);
    }
    Ok((output, ranges))
}
pub(crate) async fn insert_batch(
    db: &Transaction<'_>,
    scope: &TenantContext,
    meta: &SessionMetadata,
    operation: &str,
    run: Option<&RunId>,
    messages: &[Message],
) -> Result<TranscriptSeq, CoordinationError> {
    if messages.len() > 32768 || operation.is_empty() || operation.len() > 2048 {
        return Err(CoordinationError::InvalidRequest);
    }
    let raw = encode(messages, 16 * 1024 * 1024)?;
    let digest = format!("{:x}", Sha256::digest(raw.as_bytes()));
    let mut lookup = scope_values(scope);
    lookup.extend([meta.lifetime.0.clone(), operation.into()]);
    if let Some(old)=db.query_opt(&format!("SELECT digest,end_seq FROM transcript_batches WHERE {SCOPE} AND lifetime=$5 AND operation_id=$6"),&parameters(&lookup)).await.map_err(unavailable)?{if old.try_get::<_,String>(0).map_err(unavailable)?!=digest{return Err(CoordinationError::Conflict);}return Ok(TranscriptSeq(unsigned(old.try_get(1).map_err(unavailable)?)?));}
    let current = metadata_in(db, scope, &meta.key, true)
        .await?
        .ok_or(CoordinationError::Forbidden)?;
    if current.deleted || current.lifetime != meta.lifetime {
        return Err(CoordinationError::StaleLease);
    }
    let start = current.revision.0;
    let end = start
        .checked_add(messages.len() as u64)
        .filter(|n| *n <= i64::MAX as u64)
        .ok_or(CoordinationError::Conflict)?;
    let mut values = lookup;
    values.extend([start.to_string(), end.to_string(), raw, digest]);
    let run_text = run.map(|run| run.0.as_str());
    let mut params = parameters(&values);
    params.push(&run_text);
    db.execute("INSERT INTO transcript_batches(organization_id,tenant_id,project_id,agent_uid,lifetime,operation_id,start_seq,end_seq,messages_json,digest,run_id) VALUES($1,$2,$3,$4,$5,$6,$7::text::bigint,$8::text::bigint,$9,$10,$11)",&params).await.map_err(unavailable)?;
    let mut head = scope_values(scope);
    head.extend([
        meta.key.0.clone(),
        end.to_string(),
        now(db).await?.to_string(),
    ]);
    db.execute(&format!("UPDATE session_heads SET revision=$6::text::bigint,updated_at_ms=$7::text::bigint WHERE {SCOPE} AND session_id=$5"),&parameters(&head)).await.map_err(unavailable)?;
    Ok(TranscriptSeq(end))
}
impl PgCanonicalStore {
    async fn lifecycle_with_authorization(
        &self,
        scope: &TenantContext,
        command: &SessionCommand,
        expected: Option<&SessionLifetimeId>,
        operation: &str,
        authorization: Option<(&AuthorizationRequest, &dyn AuthorizationVerifier)>,
    ) -> Result<SessionMetadata, RepositoryError> {
        if operation.is_empty() || operation.len() > 256 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (source, target) = match command {
            SessionCommand::Create { key }
            | SessionCommand::Close { key }
            | SessionCommand::End { key, .. } => (key, None),
            SessionCommand::Fork { source, target, .. } => (source, Some(target)),
        };
        if !valid_key(source) || target.is_some_and(|key| !valid_key(key) || key == source) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let raw = encode(&(command, expected), 16384)?;
        let mut proofs = self.proofs.lock().await;
        if proofs.len() > 1021 {
            return Err(CoordinationError::Conflict.into());
        }
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        if let Some((request, verifier)) = authorization {
            if request.context.scope != *scope || request.action != session_command_action(command)
            {
                return Err(AuthorizationError::Forbidden.into());
            }
            let identity =
                crate::identities::identity_in(&tx, scope, &request.context.principal.subject_id)
                    .await?;
            let mut current = request.clone();
            current.attributes.timestamp_ms = now(&tx).await?;
            verifier.verify(&current, &identity)?;
        }
        let lock = encode(&(scope, source), 4096)?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,42))",
            &[&lock],
        )
        .await
        .map_err(unavailable)?;
        let mut params = scope_values(scope);
        params.push(operation.into());
        if let Some(row)=tx.query_opt(&format!("SELECT command_json,CASE WHEN octet_length(result_json)<=65536 THEN result_json END FROM lifecycle_receipts WHERE {SCOPE} AND operation_id=$5"),&parameters(&params)).await.map_err(unavailable)?{
            if row.try_get::<_,String>(0).map_err(unavailable)?!=raw{return Err(CoordinationError::Conflict.into());}
            let result:SessionMetadata=serde_json::from_str(&field(&row,1)?).map_err(|_|CoordinationError::Corrupt)?;
            let result_key=target.unwrap_or(source);if &result.key!=result_key{return Err(CoordinationError::Corrupt.into());}
            permanent_life(&tx,&SessionWriterLeaseKey{scope:scope.clone(),session_key:result.key.clone(),session_lifetime_id:result.lifetime.clone()}).await?;
            tx.commit().await.map_err(unavailable)?;return Ok(result);
        }
        let original = metadata_in(&tx, scope, source, true).await?;
        let mut grants = Vec::new();
        if let Some(meta) = &original {
            if expected.is_some_and(|life| life != &meta.lifetime) {
                return Err(CoordinationError::Conflict.into());
            }
            let key = SessionWriterLeaseKey {
                scope: scope.clone(),
                session_key: source.clone(),
                session_lifetime_id: meta.lifetime.clone(),
            };
            let grant = grant_in(&tx, &key, &self.writer_owner, 30_000).await?;
            grants.push(grant);
        } else if expected.is_some() {
            return Err(CoordinationError::Conflict.into());
        }
        let result = match command {
            SessionCommand::Create { key } => create_in(&tx, scope, key).await?,
            SessionCommand::Close { .. } => {
                let meta = original
                    .as_ref()
                    .filter(|meta| !meta.deleted && expected == Some(&meta.lifetime))
                    .ok_or(CoordinationError::Conflict)?;
                ensure_idle(&tx, scope, meta).await?;
                publish_lifecycle(&tx, scope, meta, HookEvent::SessionEnd).await?;
                meta.clone()
            }
            SessionCommand::End { key, delete } => {
                let meta = original
                    .as_ref()
                    .filter(|meta| !meta.deleted && expected == Some(&meta.lifetime))
                    .ok_or(CoordinationError::Conflict)?;
                ensure_idle(&tx, scope, meta).await?;
                let mut values = key_values(&SessionWriterLeaseKey {
                    scope: scope.clone(),
                    session_key: key.clone(),
                    session_lifetime_id: meta.lifetime.clone(),
                });
                values.push(now(&tx).await?.to_string());
                tx.execute(
                    "INSERT INTO session_tombstones VALUES($1,$2,$3,$4,$6,$5,$7::text::bigint)",
                    &parameters(&values),
                )
                .await
                .map_err(unavailable)?;
                // 仅清理原session派生记忆；保留永久历史及所有业务receipt。
                let mut memory_values = scope_values(scope);
                memory_values.push(meta.lifetime.0.clone());
                for table in [
                    "memory_exposures",
                    "memory_assessments",
                    "memory_feedback_receipts",
                ] {
                    tx.execute(
                        &format!("DELETE FROM {table} WHERE {SCOPE} AND lifetime=$5"),
                        &parameters(&memory_values),
                    )
                    .await
                    .map_err(unavailable)?;
                }
                tx.execute(&format!("DELETE FROM memories WHERE {SCOPE} AND lifetime=$5 AND data_json::jsonb->>'kind'='turn_summary'"),&parameters(&memory_values)).await.map_err(unavailable)?;
                publish_lifecycle(&tx, scope, meta, HookEvent::SessionEnd).await?;
                let new_life = if *delete {
                    meta.lifetime.clone()
                } else {
                    lifetime(&tx).await?
                };
                if !*delete {
                    let mut binding = scope_values(scope);
                    binding.extend([new_life.0.clone(), key.0.clone()]);
                    tx.execute(
                        "INSERT INTO session_lifetime_owners VALUES($1,$2,$3,$4,$5,$6,'canonical')",
                        &parameters(&binding),
                    )
                    .await
                    .map_err(unavailable)?;
                }
                let mut update = scope_values(scope);
                update.extend([
                    key.0.clone(),
                    new_life.0,
                    delete.to_string(),
                    now(&tx).await?.to_string(),
                ]);
                tx.execute(&format!("UPDATE session_heads SET lifetime=$6,deleted=$7::text::boolean,revision=0,projection_generation=projection_generation+1,legacy_imported=TRUE,updated_at_ms=$8::text::bigint WHERE {SCOPE} AND session_id=$5"),&parameters(&update)).await.map_err(unavailable)?;
                metadata_in(&tx, scope, key, false)
                    .await?
                    .ok_or(CoordinationError::Corrupt)?
            }
            SessionCommand::Fork {
                source: _,
                target,
                revision,
            } => {
                let meta = original
                    .as_ref()
                    .filter(|meta| !meta.deleted && meta.revision == *revision)
                    .ok_or(CoordinationError::Conflict)?;
                let (messages, _) = transcript_in(&tx, scope, meta).await?;
                let child = create_in(&tx, scope, target).await?;
                insert_batch(&tx, scope, &child, "fork", None, &messages).await?;
                metadata_in(&tx, scope, target, false)
                    .await?
                    .ok_or(CoordinationError::Corrupt)?
            }
        };
        if !result.deleted {
            let key = SessionWriterLeaseKey {
                scope: scope.clone(),
                session_key: result.key.clone(),
                session_lifetime_id: result.lifetime.clone(),
            };
            grants.push(grant_in(&tx, &key, &self.writer_owner, 30_000).await?);
        }
        params.extend([raw, encode(&result, 65536)?]);
        tx.execute(
            "INSERT INTO lifecycle_receipts VALUES($1,$2,$3,$4,$5,$6,$7)",
            &parameters(&params),
        )
        .await
        .map_err(unavailable)?;
        for grant in &grants {
            Self::before_commit(&tx, grant).await?;
        }
        if let Some((request, _)) = authorization {
            crate::ledgers::append_audit_in(&tx,&AuditCommand {
                scope:scope.clone(),operation_id:format!("lifecycle:{:x}",Sha256::digest(operation.as_bytes())),subject_id:request.context.principal.subject_id.clone(),action:"session.lifecycle".into(),
                resource:serde_json::json!({"key":result.key,"lifetime":result.lifetime,"command":command}),outcome:"committed".into()
            },now(&tx).await?).await?;
        }
        tx.commit().await.map_err(unavailable)?;
        for grant in grants {
            if grant.key.session_key == result.key
                && grant.key.session_lifetime_id != result.lifetime
            {
                proofs.remove(&proof_key(&grant.key)?);
            } else {
                proofs.insert(proof_key(&grant.key)?, grant);
            }
        }
        Ok(result)
    }
}
#[async_trait::async_trait]
impl SessionLifecycleRepository for PgCanonicalStore {
    async fn commit_session_command(
        &self,
        scope: &TenantContext,
        command: &SessionCommand,
        expected: Option<&SessionLifetimeId>,
        operation: &str,
    ) -> Result<SessionMetadata, CoordinationError> {
        self.lifecycle_with_authorization(scope, command, expected, operation, None)
            .await
            .map_err(|error| match error {
                RepositoryError::Coordination(error) => error,
                _ => CoordinationError::Forbidden,
            })
    }
    async fn commit_session_authorized(
        &self,
        request: &AuthorizationRequest,
        command: &SessionCommand,
        expected: Option<&SessionLifetimeId>,
        operation: &str,
        verifier: std::sync::Arc<dyn AuthorizationVerifier>,
    ) -> Result<SessionMetadata, RepositoryError> {
        self.lifecycle_with_authorization(
            &request.context.scope,
            command,
            expected,
            operation,
            Some((request, verifier.as_ref())),
        )
        .await
    }
    async fn read_close_receipt(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        life: &SessionLifetimeId,
        operation: &str,
    ) -> Result<Option<SessionMetadata>, CoordinationError> {
        if !valid_key(key) || operation.is_empty() || operation.len() > 256 {
            return Err(CoordinationError::InvalidRequest);
        }
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        permanent_life(
            &tx,
            &SessionWriterLeaseKey {
                scope: scope.clone(),
                session_key: key.clone(),
                session_lifetime_id: life.clone(),
            },
        )
        .await?;
        let mut values = scope_values(scope);
        values.push(operation.into());
        let row=tx.query_opt(&format!("SELECT command_json,CASE WHEN octet_length(result_json)<=65536 THEN result_json END FROM lifecycle_receipts WHERE {SCOPE} AND operation_id=$5"),&parameters(&values)).await.map_err(unavailable)?;
        let result = if let Some(row) = row {
            let command: (SessionCommand, Option<SessionLifetimeId>) =
                serde_json::from_str(&row.try_get::<_, String>(0).map_err(unavailable)?)
                    .map_err(|_| CoordinationError::Corrupt)?;
            if !matches!(command.0,SessionCommand::Close{key:ref candidate} if candidate==key)
                || command.1.as_ref() != Some(life)
            {
                return Err(CoordinationError::Conflict);
            }
            let meta: SessionMetadata =
                serde_json::from_str(&field(&row, 1)?).map_err(|_| CoordinationError::Corrupt)?;
            if &meta.key != key || &meta.lifetime != life {
                return Err(CoordinationError::Corrupt);
            }
            Some(meta)
        } else {
            None
        };
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
}
#[async_trait::async_trait]
impl SessionControlRepository for PgCanonicalStore {
    async fn read_selected_session(
        &self,
        scope: &TenantContext,
    ) -> Result<Option<SessionKey>, RepositoryError> {
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let row=tx.query_opt(&format!("SELECT CASE WHEN octet_length(value) BETWEEN 1 AND 1024 THEN value END FROM runtime_settings WHERE {SCOPE} AND name='active_session'"),&parameters(&scope_values(scope))).await.map_err(unavailable)?;
        let result = if let Some(row) = row {
            let key = SessionKey(field(&row, 0)?);
            let meta = metadata_in(&tx, scope, &key, false)
                .await?
                .ok_or(CoordinationError::Corrupt)?;
            if meta.deleted { None } else { Some(key) }
        } else {
            None
        };
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
    async fn read_session_metadata(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
    ) -> Result<Option<SessionMetadata>, RepositoryError> {
        if !valid_key(key) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let result = metadata_in(&tx, scope, key, false).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
    async fn select_session(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        life: &SessionLifetimeId,
    ) -> Result<(), RepositoryError> {
        if !valid_key(key) {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let lease_key = SessionWriterLeaseKey {
            scope: scope.clone(),
            session_key: key.clone(),
            session_lifetime_id: life.clone(),
        };
        let mut proofs = self.proofs.lock().await;
        let id = proof_key(&lease_key)?;
        if !proofs.contains_key(&id) && proofs.len() >= 1024 {
            return Err(CoordinationError::Conflict.into());
        }
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let grant = grant_in(&tx, &lease_key, &self.writer_owner, 30_000).await?;
        let mut values = scope_values(scope);
        values.push(key.0.clone());
        tx.execute("INSERT INTO runtime_settings VALUES($1,$2,$3,$4,'active_session',$5) ON CONFLICT(organization_id,tenant_id,project_id,agent_uid,name) DO UPDATE SET value=excluded.value",&parameters(&values)).await.map_err(unavailable)?;
        Self::before_commit(&tx, &grant).await?;
        tx.commit().await.map_err(unavailable)?;
        proofs.insert(id, grant);
        Ok(())
    }
    async fn import_legacy_session(
        &self,
        scope: &TenantContext,
        key: &SessionKey,
        expected: Option<&SessionLifetimeId>,
        messages: &[Message],
    ) -> Result<(), RepositoryError> {
        // 旧文件导入是Local兼容协议；企业迁移使用离线工具，不猜测跨域归属。
        if scope != &TenantContext::local() {
            return Err(AuthorizationError::Forbidden.into());
        }
        if !valid_key(key) || messages.len() > 32768 {
            return Err(CoordinationError::InvalidRequest.into());
        }
        encode(messages, 16 * 1024 * 1024)?;
        let mut proofs = self.proofs.lock().await;
        if proofs.len() >= 1024 {
            return Err(CoordinationError::Conflict.into());
        }
        let mut client = self.inner.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        set_scope(&tx, scope).await?;
        let lock = encode(&(scope, key), 4096)?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,42))",
            &[&lock],
        )
        .await
        .map_err(unavailable)?;
        let existing = metadata_in(&tx, scope, key, true).await?;
        let meta = if let Some(meta) = existing {
            if expected != Some(&meta.lifetime) || meta.deleted {
                return Err(CoordinationError::Conflict.into());
            }
            if meta.legacy_imported {
                tx.commit().await.map_err(unavailable)?;
                return Ok(());
            }
            meta
        } else {
            if expected.is_some() {
                return Err(CoordinationError::Conflict.into());
            }
            let created = create_in(&tx, scope, key).await?;
            let mut values = scope_values(scope);
            values.push(key.0.clone());
            tx.execute(
                &format!(
                    "UPDATE session_heads SET legacy_imported=FALSE WHERE {SCOPE} AND session_id=$5"
                ),
                &parameters(&values),
            )
            .await
            .map_err(unavailable)?;
            created
        };
        let lease_key = SessionWriterLeaseKey {
            scope: scope.clone(),
            session_key: key.clone(),
            session_lifetime_id: meta.lifetime.clone(),
        };
        let grant = grant_in(&tx, &lease_key, &self.writer_owner, 30_000).await?;
        if meta.revision.0 == 0 && !messages.is_empty() {
            insert_batch(&tx, scope, &meta, "legacy-import", None, messages).await?;
        }
        let mut values = scope_values(scope);
        values.extend([key.0.clone(), now(&tx).await?.to_string()]);
        tx.execute(&format!("UPDATE session_heads SET legacy_imported=TRUE,updated_at_ms=$6::text::bigint WHERE {SCOPE} AND session_id=$5"),&parameters(&values)).await.map_err(unavailable)?;
        Self::before_commit(&tx, &grant).await?;
        tx.commit().await.map_err(unavailable)?;
        proofs.insert(proof_key(&lease_key)?, grant);
        Ok(())
    }
}
