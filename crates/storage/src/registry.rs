//! 同canonical DB的注册表事务，session代际不可变；不持执行状态。
use crate::{RunStore,RuntimeError,enterprise,now_ms};
use agent_core::*;
use agent_storage_ports::RegistryRepository;
use rusqlite::{Connection,OptionalExtension,params};
use std::sync::Arc;
const SCOPE:&str="organization_id=?1 AND tenant_id=?2 AND project_id=?3 AND agent_uid=?4";
fn values(s:&TenantContext)->[&str;4]{[s.organization_id.as_str(),s.tenant_id.as_str(),s.project_id.as_str(),s.agent_uid.as_str()]}
fn decode<T:serde::de::DeserializeOwned>(raw:&str,max:usize)->Result<T,RuntimeError>{if raw.len()>max{return Err(CoordinationError::Corrupt.into())}serde_json::from_str(raw).map_err(|_|CoordinationError::Corrupt.into())}
fn read_in(db:&Connection,s:&TenantContext)->Result<RegistryReadback,RuntimeError>{let v=values(s);
    let state=db.query_row(&format!("SELECT state_json FROM registry_control WHERE {SCOPE}"),params![v[0],v[1],v[2],v[3]],|r|r.get::<_,String>(0)).optional()?.map(|raw|decode(&raw,65536)).transpose()?.unwrap_or_default();
    let mut stmt=db.prepare(&format!("SELECT generation_json FROM registry_generations WHERE {SCOPE} ORDER BY version LIMIT 65"))?;let rows=stmt.query_map(params![v[0],v[1],v[2],v[3]],|r|r.get::<_,String>(0))?;let mut generations=Vec::new();
    for raw in rows {if generations.len()>=64{return Err(CoordinationError::Corrupt.into())}generations.push(decode(&raw?,262144)?);}Ok(RegistryReadback{state,generations})
}
pub(crate) fn generation_in(db:&Connection,s:&TenantContext,key:&SessionKey,life:&SessionLifetimeId)->Result<Option<InstalledGeneration>,RuntimeError>{
    enterprise::check_session_scope_in(db,s,key)?;crate::sessions::lifetime_owner_in(db,key,life)?;let v=values(s);
    let raw:Option<String>=db.query_row(&format!("SELECT g.generation_json FROM registry_session_bindings b LEFT JOIN registry_generations g USING(organization_id,tenant_id,project_id,agent_uid,digest) WHERE b.{SCOPE} AND b.session_key=?5 AND b.lifetime=?6"),params![v[0],v[1],v[2],v[3],key.0,life.0],|r|r.get(0)).optional()?.flatten();
    raw.map(|s|decode(&s,262144)).transpose()
}
pub(crate) fn bind_in(db:&Connection,key:&SessionKey,life:&SessionLifetimeId)->Result<(),RuntimeError>{
    let s:TenantContext=db.query_row("SELECT t.organization_id,s.tenant_id,s.project_id,s.agent_uid FROM session_scopes s JOIN tenants t ON t.id=s.tenant_id WHERE s.session_id=?1",[&key.0],|r|{let values=(r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?);Ok(values)})?.try_into_scope()?;
    let read=read_in(db,&s)?;let selected=read.state.rollout.as_ref().map(|r|agent_registry::selected(r,key,life)).transpose()?;
    if let Some(d)=selected {let g=read.generations.iter().find(|g|g.pack_digest==d).ok_or(CoordinationError::Corrupt)?;let trust=read.state.trust.as_ref().ok_or(CoordinationError::InvalidRequest)?;if g.trust_digest!=agent_registry::digest(&serde_json::json!(trust))? {return Err(CoordinationError::InvalidRequest.into())}}
    let v=values(&s);db.execute("INSERT INTO registry_session_bindings VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT DO NOTHING",params![v[0],v[1],v[2],v[3],key.0,life.0,selected])?;Ok(())
}
trait ScopeTuple{fn try_into_scope(self)->Result<TenantContext,RuntimeError>;}
impl ScopeTuple for(String,String,String,String){fn try_into_scope(self)->Result<TenantContext,RuntimeError>{Ok(TenantContext{organization_id:self.0.try_into()?,tenant_id:self.1.try_into()?,project_id:self.2.try_into()?,agent_uid:self.3.try_into()?})}}
#[async_trait::async_trait]
impl RegistryRepository for RunStore {
    async fn mutate_registry(&self,request:&AuthorizationRequest,m:&RegistryMutation,verifier:Arc<dyn AuthorizationVerifier>)->Result<RegistryReceipt,RepositoryError>{
        agent_registry::validate_mutation(m)?;if request.action!=agent_registry::command_action(&m.command){return Err(CoordinationError::InvalidRequest.into())}
        let(request,m)=(request.clone(),m.clone());self.sqlite_blocking(CoordinationError::StorageUnavailable.into(),move|store|{
            let execute=||->Result<RegistryReceipt,RuntimeError>{
                let prepared=if let RegistryCommand::Publish{pack}=&m.command {let trust={let db=store.lock_connection()?;read_in(&db,&request.context.scope)?.state.trust.ok_or(CoordinationError::InvalidRequest)?};Some(agent_registry::prepare(pack,&request.context.scope,&trust)?)}else{None};
                let mut db=store.lock_connection()?;let tx=db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let s=&request.context.scope;let v=values(s);let actor=&request.context.principal.subject_id;let identity=enterprise::identity_in(&tx,s,actor)?;let mut checked=request.clone();checked.attributes.timestamp_ms=now_ms();verifier.verify(&checked,&identity)?;
                let command_digest=agent_registry::digest(&serde_json::json!(m))?;
                let prior=tx.query_row(&format!("SELECT command_digest,receipt_json FROM registry_operations WHERE {SCOPE} AND actor_id=?5 AND operation_id=?6"),params![v[0],v[1],v[2],v[3],actor.as_str(),m.operation_id],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?))).optional()?;
                if let Some((digest,raw))=prior {if digest!=command_digest{return Err(CoordinationError::Conflict.into())}return decode(&raw,262144)}
                if let RegistryCommand::ConfigureTrust{trust}=&m.command {
                    let raw=serde_json::to_string(trust).map_err(|_|CoordinationError::Corrupt)?;let digest=agent_registry::digest(&serde_json::json!(trust))?;
                    let old=tx.query_row(&format!("SELECT digest FROM registry_trust_versions WHERE {SCOPE} AND version=?5"),params![v[0],v[1],v[2],v[3],trust.version],|r|r.get::<_,String>(0)).optional()?;
                    if old.is_some_and(|d|d!=digest){return Err(CoordinationError::Conflict.into())}
                    let count:u64=tx.query_row(&format!("SELECT count(*) FROM registry_trust_versions WHERE {SCOPE}"),params![v[0],v[1],v[2],v[3]],|r|r.get(0))?;
                    if count>=64{return Err(CoordinationError::InvalidRequest.into())}
                    tx.execute("INSERT INTO registry_trust_versions VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT DO NOTHING",params![v[0],v[1],v[2],v[3],trust.version,digest,raw])?;
                }
                let read=read_in(&tx,s)?;let(state,generation)=agent_registry::apply_prepared(&read.state,&m,s,&read.generations,prepared.as_ref())?;
                if let Some(g)=&generation {let pack=match &m.command {RegistryCommand::Publish{pack}=>serde_json::to_string(pack),_=>return Err(CoordinationError::Corrupt.into())}.map_err(|_|CoordinationError::Corrupt)?;
                    tx.execute("INSERT INTO registry_generations VALUES(?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT DO NOTHING",params![v[0],v[1],v[2],v[3],g.pack_digest,g.manifest.definition.version,serde_json::to_string(g).map_err(|_|CoordinationError::Corrupt)?,pack])?;}
                let receipt=agent_registry::receipt(&state,generation)?;
                tx.execute("INSERT INTO registry_control VALUES(?1,?2,?3,?4,?5) ON CONFLICT(organization_id,tenant_id,project_id,agent_uid) DO UPDATE SET state_json=excluded.state_json",params![v[0],v[1],v[2],v[3],serde_json::to_string(&state).map_err(|_|CoordinationError::Corrupt)?])?;
                tx.execute("INSERT INTO registry_operations VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",params![v[0],v[1],v[2],v[3],actor.as_str(),m.operation_id,command_digest,serde_json::to_string(&receipt).map_err(|_|CoordinationError::Corrupt)?])?;
                crate::ledgers::append_audit_in(&tx,&AuditCommand{scope:s.clone(),operation_id:format!("registry:{}",enterprise::hash(serde_json::to_vec(&(actor,&m.operation_id)).map_err(|_|CoordinationError::Corrupt)?.as_slice())),subject_id:actor.clone(),action:"registry.mutate".into(),resource:serde_json::json!({"state_digest":receipt.state_digest,"revision":receipt.revision,"generation_digest":receipt.generation.as_ref().map(|g|&g.pack_digest)}),outcome:"committed".into()},now_ms())?;
                tx.commit()?;Ok(receipt)
            };execute().map_err(RuntimeError::repository_error)
        }).await
    }
    async fn read_registry(&self,scope:&TenantContext)->Result<RegistryReadback,RepositoryError>{let s=scope.clone();self.sqlite_blocking(CoordinationError::StorageUnavailable.into(),move|store|{let mut db=store.lock_connection().map_err(RuntimeError::repository_error)?;let tx=db.transaction().map_err(RuntimeError::from).map_err(RuntimeError::repository_error)?;read_in(&tx,&s).map_err(RuntimeError::repository_error)}).await}
    async fn read_session_generation(&self,scope:&TenantContext,key:&SessionKey,life:&SessionLifetimeId)->Result<Option<InstalledGeneration>,RepositoryError>{let(s,key,life)=(scope.clone(),key.clone(),life.clone());self.sqlite_blocking(CoordinationError::StorageUnavailable.into(),move|store|{let mut db=store.lock_connection().map_err(RuntimeError::repository_error)?;let tx=db.transaction().map_err(RuntimeError::from).map_err(RuntimeError::repository_error)?;generation_in(&tx,&s,&key,&life).map_err(RuntimeError::repository_error)}).await}
}
