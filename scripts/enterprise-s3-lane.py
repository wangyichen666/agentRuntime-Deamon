#!/usr/bin/env python3
"""隔离 TLS MinIO 实测；只移除本次创建的容器与临时目录，不下载镜像。"""
import argparse
import json
import os
import secrets
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

IMAGE = 'minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e'

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--wave', default='wave2-s4-s3')
    args = parser.parse_args()
    if not args.wave.replace('-', '').isalnum():
        parser.error('证据名称无效')
    root = Path(__file__).resolve().parent.parent
    evidence = root / 'docs/enterprise/evidence'
    name = 'codex-agent-s3-' + uuid.uuid4().hex[:20]
    results = []
    created = False
    with tempfile.TemporaryDirectory(prefix='agent-s3-lane-') as folder, (evidence / f'{args.wave}-s3.log').open('w') as output:
        base = Path(folder)
        certs = base / 'certs'
        data = base / 'data'
        certs.mkdir(); data.mkdir(); (data / 'artifacts').mkdir()
        env = dict(os.environ, MINIO_ROOT_USER='lane-'+secrets.token_hex(8), MINIO_ROOT_PASSWORD=secrets.token_hex(32))
        def run(label, command, environment=env):
            started = time.monotonic()
            result = subprocess.run(command, cwd=root, env=environment, stdout=output, stderr=subprocess.STDOUT, check=False)
            results.append({'name':label, 'exit_code':result.returncode, 'seconds':time.monotonic()-started})
            output.flush()
            return result.returncode
        try:
            if run('image_digest', ['docker','image','inspect',IMAGE,'--format','{{.Id}}']): return 1
            if run('test_ca', ['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(base/'ca.key'),'-out',str(base/'ca.crt'),'-days','1','-subj','/CN=Agent S3 Lane CA']): return 1
            if run('test_csr', ['openssl','req','-newkey','rsa:2048','-nodes','-keyout',str(certs/'private.key'),'-out',str(base/'server.csr'),'-subj','/CN=localhost']): return 1
            (base/'ext').write_text('subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n')
            if run('test_tls_certificate', ['openssl','x509','-req','-in',str(base/'server.csr'),'-CA',str(base/'ca.crt'),'-CAkey',str(base/'ca.key'),'-CAcreateserial','-out',str(certs/'public.crt'),'-days','1','-extfile',str(base/'ext')]): return 1
            if run('create_isolated_tls_minio', ['docker','run','--pull=never','--detach','--name',name,'-p','127.0.0.1::9000','-e','MINIO_ROOT_USER','-e','MINIO_ROOT_PASSWORD','-v',f'{certs}:/certs:ro','-v',f'{data}:/data',IMAGE,'server','/data','--certs-dir','/certs']): return 1
            created = True
            binding = subprocess.check_output(['docker','port',name,'9000/tcp'],text=True).strip()
            port = binding.rsplit(':',1)[1]
            endpoint = f'https://127.0.0.1:{port}'
            started = time.monotonic(); ready = False
            while time.monotonic()-started < 30:
                probe = subprocess.run(['curl','--noproxy','*','--silent','--fail','--max-time','2','--cacert',str(base/'ca.crt'),endpoint+'/minio/health/ready'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
                if probe.returncode == 0: ready=True; break
                time.sleep(0.25)
            results.append({'name':'tls_health','exit_code':0 if ready else 1})
            if not ready:
                run('container_diagnostic',['docker','logs',name]); return 1
            env.update(AGENT_S3_ENDPOINT=endpoint, AGENT_S3_BUCKET='artifacts', AGENT_S3_REGION='us-east-1', AGENT_S3_PREFIX='lane', AGENT_S3_ACCESS_KEY=env['MINIO_ROOT_USER'], AGENT_S3_SECRET_KEY=env['MINIO_ROOT_PASSWORD'], AGENT_S3_ROOT_CA=str(base/'ca.crt'))
            if run('actual_s3_wire_integrity_tls_and_tenant_scope',['cargo','test','--locked','-p','agent-artifact','s3_','--','--ignored','--nocapture']): return 1
            if run('object_repository_real_contracts',['cargo','test','--locked','--test','object_artifact_contract','s3_','--','--ignored','--nocapture','--test-threads=1']): return 1
            if run('daemon_original_receipt_and_restart',['cargo','test','--locked','--test','runtime_contract','object_artifacts_use_original','--','--ignored','--nocapture']): return 1
            if run('daemon_native_writer_cron_original_receipt',['cargo','test','--locked','--test','runtime_contract','native_writer_cron_s3','--','--ignored','--nocapture']): return 1
            return 0
        finally:
            if created: run('remove_only_created_container',['docker','rm','--force',name])
            (evidence / f'{args.wave}-s3.json').write_text(json.dumps({'backend':'真实 S3-compatible MinIO / TLS / pinned image', 'image':IMAGE, 'results':results, 'verification':'locally verified' if results and all(r['exit_code']==0 for r in results) else 'failed/unverified'},ensure_ascii=False,indent=2)+'\n')

if __name__ == '__main__':
    raise SystemExit(main())
