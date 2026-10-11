#!/usr/bin/env python3
"""在明确命名的隔离数据库执行真实 PostgreSQL 合同；结束只清理本次创建的数据库。"""
import argparse
import json
import os
import re
import subprocess
import time
import uuid
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--user', required=True, help='本机 PostgreSQL 测试用户（需要 CREATEDB）')
    parser.add_argument('--wave', default='wave2-s1', help='证据文件前缀')
    parser.add_argument('--filter', default='postgres_', help='真实合同测试名称过滤器')
    args = parser.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9_]{1,63}', args.user):
        parser.error('测试用户必须是简单标识符')
    if not re.fullmatch(r'[A-Za-z0-9-]+', args.wave):
        parser.error('证据前缀只能使用字母、数字和连字符')
    root = Path(__file__).resolve().parent.parent
    evidence = root / 'docs/enterprise/evidence'
    evidence.mkdir(parents=True, exist_ok=True)
    database = 'codex_agent_wave2_' + uuid.uuid4().hex[:20]
    environment = dict(os.environ, AGENT_TEST_POSTGRES_DB=database, AGENT_TEST_POSTGRES_USER=args.user)
    command = ['psql', '-X', '-w', '-h', '/tmp', '-U', args.user, '-d', 'postgres', '-v', 'ON_ERROR_STOP=1']
    results = []
    created = False
    log = evidence / f'{args.wave}-postgres.log'
    with log.open('w') as output:
        def run(name, cmd, env=environment):
            started = time.monotonic()
            completed = subprocess.run(cmd, cwd=root, env=env, stdout=output, stderr=subprocess.STDOUT, check=False)
            results.append({'name': name, 'exit_code': completed.returncode, 'seconds': time.monotonic()-started})
            output.flush()
            return completed.returncode
        try:
            if run('server_version', command + ['-Atc', 'SELECT version();']):
                return 1
            if run('create_isolated_database', command + ['-c', f'CREATE DATABASE "{database}";']):
                return 1
            created = True
            if run('build_worker', ['cargo', 'build', '--locked', '-p', 'agent-storage-postgres']):
                return 1
            metadata = json.loads(subprocess.check_output(['cargo', 'metadata', '--locked', '--no-deps', '--format-version=1'], cwd=root))
            environment['AGENT_TEST_PUBLICATION_WORKER'] = str(Path(metadata['target_directory']) / 'debug/agent-storage-postgres')
            return run('real_contracts', ['cargo', 'test', '--locked', '--test', 'publication_contract', args.filter, '--', '--ignored', '--nocapture'])
        finally:
            if created:
                run('drop_only_created_database', command + ['-c', f'DROP DATABASE "{database}" WITH (FORCE);'])
            (evidence / f'{args.wave}-postgres.json').write_text(json.dumps({'backend': 'PostgreSQL / 本机 Unix socket', 'database': database, 'results': results, 'verification': 'locally verified' if results and all(r['exit_code']==0 for r in results) else 'failed/unverified'}, ensure_ascii=False, indent=2) + '\n')


if __name__ == '__main__':
    raise SystemExit(main())
