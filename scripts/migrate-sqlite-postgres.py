#!/usr/bin/env python3
"""离线全canonical迁移；输入是SQLite事实库，绝非历史JSONL导入。
prepare持写锁、完整性检查、备份和逐表证据；dry-run/import由PG原事务执行。
"""
import argparse, hashlib, json, os, pathlib, re, sqlite3, stat, subprocess, sys
LOCAL=dict(organization_id="local-organization",tenant_id="local-tenant",project_id="local-project",agent_uid="local-agent")
INFRA={"sqlite_sequence","schema_migrations"}
def encoded(value): return json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode()
def sha(data): return hashlib.sha256(data).hexdigest()
def write(path,data):
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,"wb") as f: f.write(data);f.flush();os.fsync(f.fileno())
def read(path,limit=32*1024*1024):
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
    with os.fdopen(fd,"rb") as f:
        metadata=os.fstat(f.fileno())
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink!=1: raise ValueError("输入文件必须为独立普通文件")
        data=f.read(limit+1)
        if len(data)>limit:raise ValueError("输入文件超预算")
        return data
def admin(binary,*args):
    p=subprocess.run([str(binary),*args],capture_output=True,check=False,text=True)
    # 管理binary只输出类型化错误；不回显SQL、连接凭据或请求正文。
    if p.returncode:raise ValueError("PG管理命令失败："+p.stderr[-2048:].strip())
    return json.loads(p.stdout)
def prepare(args):
    if args.source.is_symlink():raise ValueError("源不能为symlink")
    source=args.source.resolve(strict=True);bundle=args.bundle.absolute()
    if bundle.exists():raise ValueError("bundle必须不存在")
    # listener必须已停止；数据库独占写锁是最终屏障，不依靠这个文件检查授权。
    if (source.parent/"daemon.sock").exists():raise ValueError("请停止daemon后迁移")
    shape=admin(args.binary,"--canonical-shape")["tables"]
    lock=sqlite3.connect(source,timeout=1);lock.execute("BEGIN EXCLUSIVE")
    try:
        if lock.execute("PRAGMA integrity_check").fetchone()[0]!="ok" or lock.execute("PRAGMA foreign_key_check").fetchone():raise ValueError("源完整性检查失败")
        version=lock.execute("SELECT max(version) FROM schema_migrations").fetchone()[0]
        if version not in (28,29,30,31,32,33,34,35):raise ValueError("先用当前Local binary完成前向schema35升级，再停止daemon迁移")
        if lock.execute("SELECT count(*) FROM runs WHERE status IN ('queued','running','waiting_interaction')").fetchone()[0]:raise ValueError("存在未收敛run；先恢复/reconcile，不自动重放")
        if lock.execute("SELECT count(*) FROM resources WHERE state IN ('starting','running','stopping')").fetchone()[0]:raise ValueError("存在活动resource")
        if lock.execute("SELECT count(*) FROM session_scopes WHERE tenant_id<>? OR project_id<>? OR agent_uid<>?",tuple(LOCAL[k] for k in ("tenant_id","project_id","agent_uid"))).fetchone()[0]:raise ValueError("本工具仅迁移原Local域；拒绝猜测其它scope")
        tables={r[0] for r in lock.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if tables-INFRA-shape.keys():raise ValueError("源含未映射事实表："+",".join(sorted(tables-INFRA-shape.keys())))
        bundle.mkdir(mode=0o700,parents=True);(bundle/"artifacts").mkdir(mode=0o700)
        backup=bundle/"source.sqlite3";reader=sqlite3.connect(f"file:{source}?mode=ro",uri=True);target=sqlite3.connect(backup);reader.backup(target);target.close();reader.close();os.chmod(backup,0o600)
        entries=[];source_hasher=hashlib.sha256();artifact_refs={}
        for table,columns in sorted(shape.items()):
            if not re.fullmatch(r"[A-Za-z0-9_]+",table):raise ValueError("非法目标表名")
            source_rows=[]
            if table in tables:
                cursor=lock.execute(f'SELECT rowid AS _source_rowid,* FROM "{table}" ORDER BY rowid');names=[c[0] for c in cursor.description]
                source_rows=(dict(zip(names,row)) for row in cursor)
            elif table=="scope_run_counters":
                max_id=lock.execute("SELECT coalesce(max(rowid),0) FROM runs").fetchone()[0]
                source_rows=[dict(LOCAL,next_id=max_id+1)]
            # 所有源列必须保留到目标列，明确列别名之外不丢弃任何业务数据。
            target_names={c["name"] for c in columns};chunks=[];batch=[];batch_bytes=0;count=0;used_columns=None
            for row in source_rows:
                source_hasher.update(encoded({"table":table,"source":row}));source_hasher.update(b"\n")
                old=row.copy();rowid=row.pop("_source_rowid",None)
                if table=="snapshot_clock":row["slot"]=row.pop("id")
                if "storage_row_id" in target_names:row["storage_row_id"]=rowid
                if row.keys()-target_names:raise ValueError(f"{table}含未映射列：{row.keys()-target_names}")
                for key,value in LOCAL.items():
                    if key in target_names:
                        if key in row and row[key]!=value:raise ValueError("跨域事实拒绝")
                        row[key]=value
                # writer/delivery proof不能在另一个事实源上继续有效，保存更高epoch的退休证据。
                if table in ("session_writer_leases","delivery_leases"):row["expires_at_ms"]=0;row["epoch"]+=1
                for c in columns:
                    name=c["name"]
                    if name not in row and not c["nullable"] and c["default"] is None and not c["identity"]:raise ValueError(f"{table}.{name}缺必需事实")
                    if name in row and c["type"]=="boolean":
                        if row[name] not in (0,1,False,True):raise ValueError("保存boolean无效")
                        row[name]=bool(row[name])
                keys=sorted(row)
                if used_columns is None:used_columns=keys
                elif used_columns!=keys:raise ValueError("同表列不一致")
                raw=encoded(row)
                if len(raw)>20*1024*1024:raise ValueError("单行超预算")
                if batch and (len(batch)>=128 or batch_bytes+len(raw)>24*1024*1024):
                    name=f"{table}_{len(chunks)}.json";data=encoded(batch);write(bundle/name,data);chunks.append(dict(file=name,sha256=sha(data),rows=len(batch)));batch=[];batch_bytes=0
                batch.append(row);batch_bytes+=len(raw);count+=1
                if table=="artifact_publications" and row["published"]:artifact_refs[row["sha256"]]=row["bytes"]
                for name,value in old.items():
                    if isinstance(value,str):
                        for digest in re.findall(r"sha256-([0-9a-f]{64})\.txt",value):artifact_refs.setdefault(digest,None)
            if batch:
                name=f"{table}_{len(chunks)}.json";data=encoded(batch);write(bundle/name,data);chunks.append(dict(file=name,sha256=sha(data),rows=len(batch)))
            entries.append(dict(name=table,columns=used_columns or sorted(target_names),count=count,chunks=chunks))
        artifacts=[];artifact_root=source.with_suffix(".artifacts")
        for digest,expected in sorted(artifact_refs.items()):
            data=read(artifact_root/f"sha256-{digest}.txt",67108864)
            if sha(data)!=digest or (expected is not None and len(data)!=expected):raise ValueError("artifact引用/正文不一致")
            write(bundle/"artifacts"/f"sha256-{digest}.txt",data);artifacts.append(dict(sha256=digest,bytes=len(data)))
        manifest=dict(version=1,source_schema=version,migration_id=args.id,source_digest=source_hasher.hexdigest(),scope=LOCAL,tables=entries,artifacts=artifacts)
        write(bundle/"manifest.json",encoded(manifest));write(bundle/"backup-evidence.json",encoded(dict(source=str(source),backup_sha256=sha(read(backup,1024*1024*1024)),artifacts=str(artifact_root),source_digest=manifest["source_digest"])))
        return dict(prepared=True,tables=len(entries),rows=sum(t["count"] for t in entries),artifact_count=len(artifacts),bundle=str(bundle))
    finally:lock.rollback();lock.close()
def source_digest(db,manifest):
    h=hashlib.sha256();names={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    for t in sorted(manifest["tables"],key=lambda t:t["name"]):
        table=t["name"]
        if table in names:
            cursor=db.execute(f'SELECT rowid AS _source_rowid,* FROM "{table}" ORDER BY rowid');columns=[c[0] for c in cursor.description];rows=(dict(zip(columns,row)) for row in cursor)
        elif table=="scope_run_counters":rows=[dict(LOCAL,next_id=db.execute("SELECT coalesce(max(rowid),0)+1 FROM runs").fetchone()[0])]
        else:rows=[]
        for row in rows:h.update(encoded({"table":table,"source":row}));h.update(b"\n")
    return h.hexdigest()
def cutover(args,rollback=False):
    bundle=args.bundle.absolute();manifest=json.loads(read(bundle/"manifest.json",1024*1024));evidence=json.loads(read(bundle/"backup-evidence.json",1024*1024));source=pathlib.Path(evidence["source"])
    if source.name!="runtime.sqlite3" or source.parent.name!=".my-agent":raise ValueError("切换只允许标准Local事实库路径")
    marker=source.parent/"storage-cutover.json"
    if (source.parent/"daemon.sock").exists():raise ValueError("切换前必须停止daemon")
    lock=sqlite3.connect(source,timeout=1);lock.execute("BEGIN EXCLUSIVE")
    try:
        if source_digest(lock,manifest)!=manifest["source_digest"]:raise ValueError("prepare后源事实发生变化")
        if rollback:
            old=json.loads(read(marker,1024*1024))
            if old["migration_id"]!=manifest["migration_id"] or old["source_digest"]!=manifest["source_digest"] or old["state"]!="postgres":raise ValueError("切换marker不匹配")
            result=admin(args.binary,"--canonical-rollback",str(bundle))
            if not result.get("rolled_back"):raise ValueError("PG回滚栅栏未提交")
            old["state"]="rolled_back";tmp=marker.with_suffix(".next");write(tmp,encoded(old));os.replace(tmp,marker)
        else:
            if marker.exists():raise ValueError("已存在切换marker")
            result=admin(args.binary,"--canonical-verify",str(bundle))
            if not result.get("verified"):raise ValueError("目标未通过对账")
            if os.environ.get("AGENT_POSTGRES_CONFIG"):
                config=json.loads(read(pathlib.Path(os.environ["AGENT_POSTGRES_CONFIG"]),1024*1024));target={k:config[k] for k in ("host","port","database","schema")}
            else:target=dict(host="/tmp",port=5432,database=os.environ["AGENT_PUBLICATION_DB"],schema=os.environ["AGENT_PUBLICATION_SCHEMA"])
            write(marker,encoded(dict(version=1,state="postgres",migration_id=manifest["migration_id"],source_digest=manifest["source_digest"],target=target)))
        fd=os.open(source.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:os.fsync(fd)
        finally:os.close(fd)
        return dict(switched=not rollback,rolled_back=rollback,marker=str(marker))
    finally:lock.rollback();lock.close()
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("mode",choices=["prepare","dry-run","copy","verify","switch","rollback"]);p.add_argument("--source",type=pathlib.Path);p.add_argument("--bundle",type=pathlib.Path,required=True);p.add_argument("--id",default="offline_local_1");p.add_argument("--binary",type=pathlib.Path,default=pathlib.Path("target/debug/agent-storage-postgres"));a=p.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_]{1,63}",a.id):raise ValueError("迁移id非法")
    if a.mode=="prepare":
        if not a.source:raise ValueError("prepare需要source")
        result=prepare(a)
    elif a.mode in ("switch","rollback"):result=cutover(a,a.mode=="rollback")
    else:result=admin(a.binary,{"dry-run":"--canonical-dry-run","copy":"--canonical-import","verify":"--canonical-verify"}[a.mode],str(a.bundle.absolute()))
    sys.stdout.write(json.dumps(result,ensure_ascii=False)+"\n")
if __name__=="__main__":
    try:main()
    except (ValueError,sqlite3.Error,OSError,json.JSONDecodeError) as e:sys.stderr.write("离线迁移失败："+str(e)+"\n");sys.exit(1)
