//! 受信任数据库管理CLI与本地发布worker；只读取显式Unix socket数据库配置。
use agent_core::*;
use agent_storage_ports::{
    AuditReadRepository, SessionControlRepository, SessionLifecycleRepository,
    SessionWriterRepository, publish_to_local_inbox,
};
use agent_storage_postgres::{PgCanonicalStore, PgPublicationStore};
use std::io::{Read, Write};
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct InitialDomain {
    scope: TenantContext,
    principal: AuthenticatedPrincipal,
    roles: Vec<EnterpriseRole>,
    policy: PolicyDocument,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionMutation {
    scope: TenantContext,
    command: SessionCommand,
    expected: Option<SessionLifetimeId>,
    operation: String,
}
fn input<T: serde::de::DeserializeOwned>(path: &str) -> Result<T, Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 1024 * 1024 {
        return Err("管理请求超过1MiB".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn output(value: serde_json::Value) -> Result<(), std::io::Error> {
    writeln!(std::io::stdout().lock(), "{value}")
}
async fn canonical_store() -> Result<PgCanonicalStore, Box<dyn std::error::Error>> {
    Ok(if std::env::var_os("AGENT_POSTGRES_CONFIG").is_some() {
        PgCanonicalStore::connect_remote(&agent_storage_postgres::PgConnectionConfig::from_env()?)
            .await?
    } else {
        PgCanonicalStore::connect_local(
            &std::env::var("AGENT_PUBLICATION_DB")?,
            &std::env::var("AGENT_PUBLICATION_USER")?,
            &std::env::var("AGENT_PUBLICATION_SCHEMA")?,
        )
        .await?
    })
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_target(false)
        .without_time()
        .init();
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.first().is_some_and(|a| {
        matches!(
            a.as_str(),
            "--canonical-shape"
                | "--canonical-import"
                | "--canonical-dry-run"
                | "--canonical-verify"
                | "--canonical-rollback"
        )
    }) {
        let store = canonical_store().await?;
        match args.as_slice() {
            [mode] if mode == "--canonical-shape" => {
                output(store.offline_shape().await?)?;
                store.close().await;
                return Ok(());
            }
            [mode, path]
                if matches!(mode.as_str(), "--canonical-import" | "--canonical-dry-run") =>
            {
                output(
                    store
                        .import_offline(std::path::Path::new(path), mode == "--canonical-dry-run")
                        .await?,
                )?;
                store.close().await;
                return Ok(());
            }
            [mode, path] if mode == "--canonical-rollback" => {
                output(store.rollback_offline(std::path::Path::new(path)).await?)?;
                store.close().await;
                return Ok(());
            }
            [mode, path] if mode == "--canonical-verify" => {
                output(store.verify_offline(std::path::Path::new(path)).await?)?;
                store.close().await;
                return Ok(());
            }
            _ => store.close().await,
        }
    }
    match args.as_slice(){
        [mode] if mode=="--once"=>{
            let store=if std::env::var_os("AGENT_POSTGRES_CONFIG").is_some(){PgPublicationStore::connect_remote(&agent_storage_postgres::PgConnectionConfig::from_env()?).await?}else{PgPublicationStore::connect_local(&std::env::var("AGENT_PUBLICATION_DB")?,&std::env::var("AGENT_PUBLICATION_USER")?,&std::env::var("AGENT_PUBLICATION_SCHEMA")?).await?};
            let owner=format!("worker-{}-{}",std::process::id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_nanos());
            let delivered=publish_to_local_inbox(&store,&TenantContext::local(),&owner).await?;store.close().await;
            output(serde_json::json!({"consumer":"local-journal","delivered":delivered}))?;
        },
        [mode,path] if mode=="--canonical-init"=>{
            let domain:InitialDomain=input(path)?;let store=canonical_store().await?;
            store.provision_initial_domain(&domain.scope,&domain.principal,&domain.roles,&domain.policy).await?;
            let audit=store.read_audit_entries(&domain.scope,0,64).await?;store.close().await;
            output(serde_json::json!({"scope":domain.scope,"canonical_schema":9,"audit":audit,"enterprise_ready":false}))?;
        },
        [mode,path] if mode=="--canonical-session"=>{
            let request:SessionMutation=input(path)?;let store=canonical_store().await?;
            let meta=store.commit_session_command(&request.scope,&request.command,request.expected.as_ref(),&request.operation).await?;
            let readback=store.read_session_metadata(&request.scope,&meta.key).await?;
            store.release_bound_writers().await?;store.close().await;
            output(serde_json::json!({"metadata":meta,"readback":readback,"enterprise_ready":false}))?;
        },
        [mode,path] if mode=="--canonical-audit"=>{
            let scope:TenantContext=input(path)?;let store=canonical_store().await?;
            let entries=store.read_audit_entries(&scope,0,64).await?;store.close().await;output(serde_json::json!({"entries":entries,"enterprise_ready":false}))?;
        },
        _=>return Err("用法：--once | --canonical-init 请求.json | --canonical-session 请求.json | --canonical-audit 域.json；显式配置AGENT_PUBLICATION_DB/USER/SCHEMA。管理命令只供受信任数据库操作员。".into())
    }
    Ok(())
}
