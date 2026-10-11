//! 有界JSON发行物：不解压归档，不执行包代码，不访问包声明的宿主路径。
#![forbid(unsafe_code)]
use agent_core::*;
use base64::{Engine,engine::general_purpose::STANDARD};
use ed25519_dalek::{Signature,VerifyingKey};
use sha2::{Digest,Sha256};
use std::collections::{BTreeMap,BTreeSet};
pub fn canonical(value:&serde_json::Value)->Result<Vec<u8>,CoordinationError> {
    fn write(v:&serde_json::Value,out:&mut String,depth:usize)->Result<(),CoordinationError> {
        if depth>32 || out.len()>4*1024*1024{return Err(CoordinationError::InvalidRequest)}
        match v {
            serde_json::Value::Object(map)=>{out.push('{');let ordered=map.iter().collect::<BTreeMap<_,_>>();for (i,(key,value)) in ordered.into_iter().enumerate(){if i>0{out.push(',')}out.push_str(&serde_json::to_string(key).map_err(|_|CoordinationError::Corrupt)?);out.push(':');write(value,out,depth+1)?;}out.push('}');},
            serde_json::Value::Array(values)=>{out.push('[');for (i,v) in values.iter().enumerate(){if i>0{out.push(',')}write(v,out,depth+1)?;}out.push(']');},
            serde_json::Value::Number(n) if !n.is_i64() && !n.is_u64()=>return Err(CoordinationError::InvalidRequest),
            _=>out.push_str(&serde_json::to_string(v).map_err(|_|CoordinationError::Corrupt)?),
        }Ok(())
    }
    let mut output=String::new();write(value,&mut output,0)?;if output.len()>4*1024*1024{return Err(CoordinationError::InvalidRequest)}Ok(output.into_bytes())
}
pub fn digest(value:&serde_json::Value)->Result<String,CoordinationError>{Ok(format!("{:x}",Sha256::digest(canonical(value)?)))}
pub fn valid_digest(s:&str)->bool{s.len()==64 && s.bytes().all(|v|v.is_ascii_digit() || (b'a'..=b'f').contains(&v))}
pub fn valid_path(path:&str)->bool {!path.is_empty() && path.len()<=256 && !path.starts_with('/') && !path.contains(['\\','\0',':']) && path.split('/').all(|p|!p.is_empty() && p!="." && p!=".." && p.bytes().all(|v|v.is_ascii_alphanumeric() || matches!(v,b'-'|b'_'|b'.')))}
pub fn validate_trust(trust:&RegistryTrust)->Result<String,CoordinationError> {
    let mut seen=BTreeSet::new();if trust.version.is_empty() || trust.version.len()>128 || trust.roots.len()>32{return Err(CoordinationError::InvalidRequest)}
    for root in &trust.roots {if root.key_id.is_empty()||root.key_id.len()>128||!seen.insert(&root.key_id)||root.maximum_trust==PluginTrust::Builtin{return Err(CoordinationError::InvalidRequest)}let key=key(&root.public_key_base64)?;if key.is_weak(){return Err(CoordinationError::InvalidRequest)}}
    digest(&serde_json::json!(trust))
}
fn key(encoded:&str)->Result<VerifyingKey,CoordinationError> {
    if encoded.len()!=44{return Err(CoordinationError::InvalidRequest)}let bytes: [u8;32]=STANDARD.decode(encoded).map_err(|_|CoordinationError::InvalidRequest)?.try_into().map_err(|_|CoordinationError::InvalidRequest)?;
    VerifyingKey::from_bytes(&bytes).map_err(|_|CoordinationError::InvalidRequest)
}
pub fn signature_message(manifest:&PackManifest)->Result<Vec<u8>,CoordinationError> {let mut bytes=b"agent-pack-v1\0".to_vec();bytes.extend(canonical(&serde_json::json!(manifest))?);Ok(bytes)}
pub fn scan(pack:&SignedAgentPack,scope:&TenantContext,trust:&RegistryTrust)->Result<(String,String,String),CoordinationError> {
    if pack.manifest.schema_version!=1 || pack.manifest.definition.scope!=*scope || pack.manifest.trust==PluginTrust::Builtin || pack.contents.len()>256 || pack.manifest.files.len()!=pack.contents.len() || pack.manifest.definition.name.is_empty() || pack.manifest.definition.name.len()>256 || semver::Version::parse(&pack.manifest.definition.version).is_err(){return Err(CoordinationError::InvalidRequest)}
    let trust_digest=validate_trust(trust)?;let root=trust.roots.iter().find(|r|r.key_id==pack.key_id).ok_or(CoordinationError::InvalidRequest)?;
    if pack.manifest.trust>root.maximum_trust || pack.signature_base64.len()!=88 {return Err(CoordinationError::InvalidRequest)}
    let signature=Signature::from_slice(&STANDARD.decode(&pack.signature_base64).map_err(|_|CoordinationError::InvalidRequest)?).map_err(|_|CoordinationError::InvalidRequest)?;
    key(&root.public_key_base64)?.verify_strict(&signature_message(&pack.manifest)?,&signature).map_err(|_|CoordinationError::InvalidRequest)?;
    let mut seen=BTreeSet::new();let mut total=0usize;let mut scan=BTreeMap::new();
    for file in &pack.contents {
        if !valid_path(&file.path) || !seen.insert(&file.path) || !valid_digest(&file.digest) || pack.manifest.files.get(&file.path)!=Some(&file.digest) || file.content_base64.len()>512*1024{return Err(CoordinationError::InvalidRequest)}
        total=total.checked_add(file.content_base64.len()).ok_or(CoordinationError::InvalidRequest)?;if total>2*1024*1024{return Err(CoordinationError::InvalidRequest)}
        let bytes=STANDARD.decode(&file.content_base64).map_err(|_|CoordinationError::InvalidRequest)?;
        // 编码归档及可执行文件不属于本发行格式；防止将压缩炸弹隐藏为可执行扩展。
        if bytes.starts_with(b"PK\x03\x04") || bytes.starts_with(b"\x1f\x8b") || bytes.starts_with(b"\x7fELF") || std::str::from_utf8(&bytes).is_err() || format!("{:x}",Sha256::digest(&bytes))!=file.digest{return Err(CoordinationError::InvalidRequest)}
        scan.insert(&file.path,(&file.digest,bytes.len()));
    }
    Ok((digest(&serde_json::json!(pack.manifest))?,trust_digest,digest(&serde_json::json!(scan))?))
}

pub fn text_file(pack:&SignedAgentPack,path:&str)->Result<String,CoordinationError>{let f=pack.contents.iter().find(|f|f.path==path).ok_or(CoordinationError::Corrupt)?;if f.content_base64.len()>49152{return Err(CoordinationError::InvalidRequest)}String::from_utf8(STANDARD.decode(&f.content_base64).map_err(|_|CoordinationError::InvalidRequest)?).map_err(|_|CoordinationError::InvalidRequest)}
