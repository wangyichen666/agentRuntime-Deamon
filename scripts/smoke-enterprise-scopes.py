#!/usr/bin/env python3
"""局部企业作用域冒烟：自有PG测试库、两个真实daemon、本机假模型。"""
import argparse
import http.server
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
from importlib.util import module_from_spec, spec_from_file_location

spec = spec_from_file_location("local_smoke", Path(__file__).with_name("smoke-local-backends.py"))
smoke = module_from_spec(spec)
spec.loader.exec_module(smoke)


def protected(path, value):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as file:
        json.dump(value, file)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True, help="必须为自行创建的独立测试库")
    parser.add_argument("--user", default=os.environ.get("USER", "postgres"))
    parser.add_argument("--binary", type=Path, default=Path("target/debug/my-agent"))
    parser.add_argument("--admin", type=Path, default=Path("target/debug/agent-storage-postgres"))
    args = parser.parse_args()
    schema = "scope_smoke_" + str(os.getpid())
    common = os.environ.copy()
    for name in ["AGENT_RUNTIME_IDENTITY", "AGENT_CRON_IDENTITY", "AGENT_POSTGRES_CONFIG", "AGENT_ARTIFACT_BACKEND", "SESSION_PATH", "PLAN_PATH", "MEMORY_PATH", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
        common.pop(name, None)
    common.update(AGENT_PUBLICATION_DB=args.database, AGENT_PUBLICATION_USER=args.user, AGENT_PUBLICATION_SCHEMA=schema)
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), smoke.Model)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    baseline = smoke.Model.calls
    try:
        with tempfile.TemporaryDirectory(prefix="as-", dir="/tmp") as directory:
            root = Path(directory)
            scopes = []
            for suffix in ["a", "b"]:
                scope = dict(organization_id="fixture-org", tenant_id="tenant-" + suffix, project_id="same-project", agent_uid="same-agent")
                principal = dict(subject_id="workload-" + suffix, kind="workload", revocation_epoch=0)
                binding = root / (suffix + "-identity.json")
                protected(binding, dict(scope=scope, principal=principal))
                domain = root / (suffix + "-domain.json")
                protected(domain, dict(scope=scope, principal=principal, roles=["tenant_admin"], policy=dict(version="fixture-policy", hard_denies=[], conditions={})))
                provision = subprocess.run([str(args.admin.resolve()), "--canonical-init", str(domain)], env=common, capture_output=True, text=True)
                if provision.returncode:
                    raise RuntimeError("域初始化失败：" + provision.stderr[-1000:])
                workspace = root / suffix
                workspace.mkdir()
                runtime = root / (suffix + "-rt")
                env = dict(common, AGENT_RUNTIME_IDENTITY=str(binding), AGENT_STORAGE_BACKEND="postgres-local", MY_AGENT_CONFIG=str(workspace / "config.json"), MY_AGENT_RUNTIME_DIR=str(runtime), SKILLS_DIR=str(workspace / "skills"), API_TYPE="openai-chat", MODEL_NAME="local-scope-smoke", OPENAI_API_KEY="fixture", OPENAI_BASE_URL=f"http://127.0.0.1:{server.server_port}/v1", HEARTBEAT_ENABLED="0", NO_PROXY="127.0.0.1,localhost", no_proxy="127.0.0.1,localhost")
                run_id = None
                for restart in range(2):
                    log_path = root / (suffix + f"-startup-{restart}.log")
                    with log_path.open("wb") as log:
                        process = subprocess.Popen([str(args.binary.resolve()), "daemon", "--workspace", str(workspace)], env=env, stdout=log, stderr=log)
                        try:
                            deadline = time.monotonic() + 20
                            while not list(runtime.glob("*/ready.json")):
                                if process.poll() is not None or time.monotonic() >= deadline:
                                    raise RuntimeError("企业启动失败：" + log_path.read_text(errors="replace")[-4000:])
                                time.sleep(0.05)
                            with socket.socket(socket.AF_UNIX) as connection:
                                connection.settimeout(20)
                                connection.connect(str(next(runtime.glob("*/daemon.sock"))))
                                with connection.makefile("rwb") as stream:
                                    identity = smoke.rpc(stream, "identity", "identity.readback", {})
                                    assert identity["scope"] == scope, identity
                                    if not restart:
                                        smoke.rpc(stream, "quota", "quota.configure", dict(metric="runs", window_start_ms=0, window_end_ms=9223372036854775807, budget=1))
                                        smoke.rpc(stream, "create", "session.new", dict(session_id="session-same-name.jsonl", operation_id="same-create"))
                                    result = smoke.rpc(stream, "same-run", "chat.send", dict(session_id="session-same-name.jsonl", message="本机隔离冒烟"))
                                    assert result["content"] == "本机冒烟通过", result
                                    if restart:
                                        assert run_id == result["run_id"], result
                                    run_id = result["run_id"]
                                    quota = smoke.rpc(stream, "quota-read", "quota.readback", dict(metric="runs", window_start_ms=0))
                                    assert (quota["reserved"], quota["consumed"]) == (0, 1), quota
                                    audit = smoke.rpc(stream, "audit", "audit.readback", dict(after=0, limit=64))
                                    assert all(item["command"]["scope"] == scope for item in audit["entries"]), audit
                                    smoke.rpc(stream, "stop", "daemon.stop", {})
                            assert process.wait(timeout=15) == 0
                        finally:
                            if process.poll() is None:
                                process.terminate()
                                try:
                                    process.wait(timeout=5)
                                except subprocess.TimeoutExpired:
                                    process.kill()
                                    process.wait()
                    assert not (workspace / ".my-agent/runtime.sqlite3").exists()
                scopes.append(dict(scope=scope, run_id=run_id, restarts=1, quota_consumed=1))
            assert smoke.Model.calls - baseline == 2, "已完成run不能在重启后重放模型"
            print(json.dumps(dict(verified=True, backend="postgres-local", scopes=scopes, model_calls=2, note="本机假模型；不代表OIDC或远程worker验收"), ensure_ascii=False))
    finally:
        server.shutdown()
        server.server_close()
        # 仅清理本进程创建的schema，不触碰调用者其他事实。
        subprocess.run(["psql", "-X", "-w", "-h", "/tmp", "-U", args.user, "-d", args.database, "-c", f"DROP SCHEMA IF EXISTS {schema} CASCADE"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)


if __name__ == "__main__":
    main()
