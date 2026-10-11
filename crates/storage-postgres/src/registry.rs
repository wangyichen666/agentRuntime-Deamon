//! 注册表事务复用canonical scope/RLS/原审计；会话代际不可变。
use crate::{PgCanonicalStore,SCOPE,parameters,scope_values,unavailable};
use crate::canonical::{field,set_scope};
use agent_core::*;
use agent_storage_ports::RegistryRepository;
use std::sync::Arc;
use tokio_postgres::Transaction;
static PREPARATIONS:std::sync::LazyLock<Arc<tokio::sync::Semaphore>>=std::sync::LazyLock::new(||Arc::new(tokio::sync::Semaphore::new(4)));
fn decode<T:serde::de::DeserializeOwned>(raw:&str,max:usize)->Result<T,RepositoryError>{if raw.len()>max{return Err(CoordinationError::Corrupt.into())}serde_json::from_str(raw).map_err(|_|CoordinationError::Corrupt.into())}
async fn lock_in(tx:&Transaction<'_>,s:&TenantContext)->Result<(),RepositoryError>{tx.query_one("SELECT pg_advisory_xact_lock(hashtextextended($1,71))",&[&serde_json::to_string(s).map_err(|_|CoordinationError::Corrupt)?]).await.map_err(unavailable)?;Ok(())}
async fn read_in(tx:&Transaction<'_>,s:&TenantContext)->Result<RegistryReadback,RepositoryError>{let v=scope_values(s);
    let state=tx.query_opt(&format!("SELECT CASE WHEN octet_length(state_json)<=65536 THEN state_json END FROM registry_control WHERE {SCOPE}"),&parameters(&v)).await.map_err(unavailable)?.map(|r|decode(&field(&r,0)?,65536)).transpose()?.unwrap_or_default();
    let rows=tx.query(&format!("SELECT CASE WHEN octet_length(generation_json)<=262144 THEN generation_json END FROM registry_generations WHERE {SCOPE} ORDER BY version LIMIT 65"),&parameters(&v)).await.map_err(unavailable)?;if rows.len()>64{return Err(CoordinationError::Corrupt.into())}
    let generations=rows.iter().map(|r|decode(&field(r,0)?,262144)).collect::<Result<Vec<_>,_>>()?;Ok(RegistryReadback{state,generations})
}
pub(crate) async fn generation_in(tx:&Transaction<'_>,s:&TenantContext,key:&SessionKey,life:&SessionLifetimeId)->Result<Option<InstalledGeneration>,RepositoryError>{let mut v=scope_values(s);v.extend([key.0.clone(),life.0.clone()]);
    let row=tx.query_opt(&format!("SELECT CASE WHEN octet_length(g.generation_json)<=262144 THEN g.generation_json END,b.digest FROM registry_session_bindings b LEFT JOIN registry_generations g USING(organization_id,tenant_id,project_id,agent_uid,digest) WHERE b.{SCOPE} AND b.session_key=$5 AND b.lifetime=$6"),&parameters(&v)).await.map_err(unavailable)?;
    if let Some(row)=row {let digest:Option<String>=row.try_get(1).map_err(unavailable)?;if digest.is_some(){return Ok(Some(decode(&field(&row,0)?,262144)?))}}Ok(None)
}
pub(crate) async fn bind_in(tx:&Transaction<'_>,s:&TenantContext,key:&SessionKey,life:&SessionLifetimeId)->Result<(),CoordinationError>{
    let read=read_in(tx,s).await.map_err(|e|match e {RepositoryError::Coordination(e)=>e,_=>CoordinationError::Corrupt})?;
    let selected=read.state.rollout.as_ref().map(|r|agent_registry::selected(r,key,life)).transpose()?;
    if let Some(d)=selected {let g=read.generations.iter().find(|g|g.pack_digest==d).ok_or(CoordinationError::Corrupt)?;let trust=read.state.trust.as_ref().ok_or(CoordinationError::InvalidRequest)?;if g.trust_digest!=agent_registry::digest(&serde_json::json!(trust))? {return Err(CoordinationError::InvalidRequest.into())}}
    let v=scope_values(s);tx.execute("INSERT INTO registry_session_bindings VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING",&[&v[0],&v[1],&v[2],&v[3],&key.0,&life.0,&selected]).await.map_err(unavailable)?;Ok(())
}
#[async_trait::async_trait]
impl RegistryRepository for PgCanonicalStore {
    async fn mutate_registry(&self,request:&AuthorizationRequest,m:&RegistryMutation,verifier:Arc<dyn AuthorizationVerifier>)->Result<RegistryReceipt,RepositoryError>{
        agent_registry::validate_mutation(m)?;if request.action!=agent_registry::command_action(&m.command){return Err(CoordinationError::InvalidRequest.into())}
        let s=&request.context.scope;let actor=&request.context.principal.subject_id;
        let prepared=if let RegistryCommand::Publish{pack}=&m.command {let slot=PREPARATIONS.clone().try_acquire_owned().map_err(|_|CoordinationError::StorageUnavailable)?;let trust=self.read_registry(s).await?.state.trust.ok_or(CoordinationError::InvalidRequest)?;let pack=pack.clone();let scope=s.clone();tokio::task::spawn_blocking(move||{let _slot=slot;agent_registry::prepare(&pack,&scope,&trust)}).await.map_err(|_|CoordinationError::StorageUnavailable)?.map(Some)?}else{None};
        let mut db=self.inner.client.lock().await;let tx=db.transaction().await.map_err(unavailable)?;set_scope(&tx,s).await?;
        let identity=crate::identities::identity_in(&tx,s,actor).await?;let mut checked=request.clone();checked.attributes.timestamp_ms=crate::now(&tx).await?;verifier.verify(&checked,&identity)?;lock_in(&tx,s).await?;
        let command_digest=agent_registry::digest(&serde_json::json!(m))?;let mut v=scope_values(s);v.extend([actor.as_str().into(),m.operation_id.clone()]);
        if let Some(row)=tx.query_opt(&format!("SELECT command_digest,receipt_json FROM registry_operations WHERE {SCOPE} AND actor_id=$5 AND operation_id=$6"),&parameters(&v)).await.map_err(unavailable)? {
            if field(&row,0)?!=command_digest{return Err(CoordinationError::Conflict.into())}return decode(&field(&row,1)?,262144)
        }
        if let RegistryCommand::ConfigureTrust{trust}=&m.command {
            let raw=crate::sessions::encode(trust,65536)?;let digest=agent_registry::digest(&serde_json::json!(trust))?;let mut tv=scope_values(s);tv.push(trust.version.clone());
            if let Some(r)=tx.query_opt(&format!("SELECT digest FROM registry_trust_versions WHERE {SCOPE} AND version=$5"),&parameters(&tv)).await.map_err(unavailable)? {if field(&r,0)?!=digest{return Err(CoordinationError::Conflict.into())}}
            let count:i64=tx.query_one(&format!("SELECT count(*) FROM registry_trust_versions WHERE {SCOPE}"),&parameters(&tv[..4])).await.map_err(unavailable)?.try_get(0).map_err(unavailable)?;if count>=64{return Err(CoordinationError::InvalidRequest.into())}
            tv.extend([digest,raw]);tx.execute("INSERT INTO registry_trust_versions VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING",&parameters(&tv)).await.map_err(unavailable)?;
        }
        let read=read_in(&tx,s).await?;let(state,generation)=agent_registry::apply_prepared(&read.state,m,s,&read.generations,prepared.as_ref())?;
        if let Some(g)=&generation {let pack=match &m.command {RegistryCommand::Publish{pack}=>crate::sessions::encode(pack,3*1024*1024)?,_=>return Err(CoordinationError::Corrupt.into())};let raw=crate::sessions::encode(g,262144)?;
            tx.execute("INSERT INTO registry_generations VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING",&[&v[0],&v[1],&v[2],&v[3],&g.pack_digest,&g.manifest.definition.version,&raw,&pack]).await.map_err(unavailable)?;
        }
        let receipt=agent_registry::receipt(&state,generation)?;let raw=crate::sessions::encode(&state,65536)?;
        tx.execute("INSERT INTO registry_control VALUES($1,$2,$3,$4,$5) ON CONFLICT(organization_id,tenant_id,project_id,agent_uid) DO UPDATE SET state_json=excluded.state_json",&[&v[0],&v[1],&v[2],&v[3],&raw]).await.map_err(unavailable)?;
        tx.execute("INSERT INTO registry_operations VALUES($1,$2,$3,$4,$5,$6,$7,$8)",&[&v[0],&v[1],&v[2],&v[3],&actor.as_str(),&m.operation_id,&command_digest,&crate::sessions::encode(&receipt,262144)?]).await.map_err(unavailable)?;
        crate::ledgers::append_audit_in(&tx,&AuditCommand{scope:s.clone(),operation_id:format!("registry:{}",agent_registry::digest(&serde_json::json!([actor,m.operation_id]))?),subject_id:actor.clone(),action:"registry.mutate".into(),resource:serde_json::json!({"state_digest":receipt.state_digest,"revision":receipt.revision,"generation_digest":receipt.generation.as_ref().map(|g|&g.pack_digest)}),outcome:"committed".into()},crate::now(&tx).await?).await?;
        tx.commit().await.map_err(unavailable)?;Ok(receipt)
    }
    async fn read_registry(&self,s:&TenantContext)->Result<RegistryReadback,RepositoryError>{let mut db=self.inner.client.lock().await;let tx=db.transaction().await.map_err(unavailable)?;set_scope(&tx,s).await?;let result=read_in(&tx,s).await?;tx.commit().await.map_err(unavailable)?;Ok(result)}
    async fn read_session_generation(&self,s:&TenantContext,key:&SessionKey,life:&SessionLifetimeId)->Result<Option<InstalledGeneration>,RepositoryError>{let mut db=self.inner.client.lock().await;let tx=db.transaction().await.map_err(unavailable)?;set_scope(&tx,s).await?;
        crate::canonical::permanent_life(&tx,&SessionWriterLeaseKey{scope:s.clone(),session_key:key.clone(),session_lifetime_id:life.clone()}).await?;
        let result=generation_in(&tx,s,key,life).await?;tx.commit().await.map_err(unavailable)?;Ok(result)}
}
