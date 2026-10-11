#!/usr/bin/env python3
"""局部启动冒烟：隔离工作区、本机假模型、SQLite 和显式临时 PostgreSQL 库。"""
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


class Model(http.server.BaseHTTPRequestHandler):
    calls = 0
    requests = []

    def log_message(self, *_):
        pass

    def do_POST(self):
        Model.requests.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
        Model.calls += 1
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Connection", "close")
        self.end_headers()
        for chunk in [
            {"choices": [{"index": 0, "delta": {"role": "assistant", "content": "本机冒烟通过"}, "finish_reason": None}]},
            {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
            {"choices": [], "usage": {"prompt_tokens": 20, "completion_tokens": 5, "prompt_tokens_details": {"cached_tokens": 0}}},
        ]:
            self.wfile.write(("data: " + json.dumps(chunk) + "\n\n").encode())
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()


def rpc(stream, request_id, method, params):
    stream.write((json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}) + "\n").encode())
    stream.flush()
    while True:
        line = stream.readline()
        if not line:
            raise RuntimeError("daemon 连接提前结束")
        frame = json.loads(line)
        if frame.get("id") == request_id:
            if "error" in frame:
                raise RuntimeError(f"{method}: {frame['error']}")
            return frame["result"]


def run_backend(binary, root, backend, port, database, user):
    workspace = root / backend
    workspace.mkdir()
    runtime = root / (backend + "-runtime")
    env = os.environ.copy()
    for name in ["SESSION_PATH", "PLAN_PATH", "MEMORY_PATH", "AGENT_SESSION_WRITER", "AGENT_S3_ROOT_CA", "MY_AGENT_HOOKS_CONFIG", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
        env.pop(name, None)
    env.update(MY_AGENT_CONFIG=str(workspace / "isolated-config.json"),
               MY_AGENT_RUNTIME_DIR=str(runtime), SKILLS_DIR=str(workspace / "skills"),
               AGENT_STORAGE_BACKEND=backend, AGENT_ARTIFACT_BACKEND="file-v1",
               API_TYPE="openai-chat", MODEL_NAME="local-smoke", OPENAI_API_KEY="local-smoke-fixture",
               OPENAI_BASE_URL=f"http://127.0.0.1:{port}/v1", HEARTBEAT_ENABLED="0", NO_PROXY="127.0.0.1,localhost", no_proxy="127.0.0.1,localhost", RUST_LOG="info")
    if backend == "postgres-local":
        env.update(AGENT_PUBLICATION_DB=database, AGENT_PUBLICATION_USER=user,
                   AGENT_PUBLICATION_SCHEMA="daemon_smoke_" + str(os.getpid()))
    baseline = Model.calls
    run_id = None
    for attempt in range(2):
        log_path = workspace / f"startup-{attempt}.log"
        with log_path.open("wb") as log:
            process = subprocess.Popen([str(binary), "daemon", "--workspace", str(workspace)], env=env, stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 20
                while not list(runtime.glob("*/ready.json")):
                    if process.poll() is not None or time.monotonic() >= deadline:
                        raise RuntimeError("启动失败：" + log_path.read_text(errors="replace")[-4000:])
                    time.sleep(0.05)
                address = next(runtime.glob("*/daemon.sock"))
                with socket.socket(socket.AF_UNIX) as connection:
                    connection.settimeout(20)
                    connection.connect(str(address))
                    with connection.makefile("rwb") as stream:
                        health = rpc(stream, "doctor", "runtime.doctor", {})["storage"]
                        if backend == "postgres-local":
                            assert health["backend"] == "postgres" and health["schema_version"] == 10, health
                        if not attempt:
                            rpc(stream, "new", "session.new", {"session_id": "session-smoke.jsonl", "operation_id": "smoke-create"})
                        result = rpc(stream, "smoke-chat", "chat.send", {"session_id": "session-smoke.jsonl", "message": "请返回冒烟结果"})
                        assert result["content"] == "本机冒烟通过", result
                        if attempt:
                            assert result["run_id"] == run_id, result
                        run_id = result["run_id"]
                        fact = rpc(stream, "fact", "run.read", {"run_id": run_id})
                        assert fact["status"] == "completed", fact
                        rpc(stream, "stop", "daemon.stop", {})
                assert process.wait(timeout=15) == 0
            except Exception as error:
                raise RuntimeError(str(error) + "\n本机模型调用数：" + str(Model.calls-baseline) + "\n" + log_path.read_text(errors="replace")[-4000:]) from error
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
    assert Model.calls - baseline == 1, "完成运行重启后不应重放模型请求"
    if backend == "postgres-local":
        assert not (workspace / ".my-agent/runtime.sqlite3").exists(), "Postgres 装配不得生成第二个 SQLite 事实源"
    print(json.dumps({"backend": backend, "startup": "通过", "chat": "通过", "restart_dedup": "通过", "model_calls": 1}, ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/debug/my-agent"))
    parser.add_argument("--postgres-database", help="必须为调用者自行创建的临时测试数据库")
    parser.add_argument("--postgres-user", default=os.environ.get("USER", "postgres"))
    options = parser.parse_args()
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Model)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        with tempfile.TemporaryDirectory(prefix="ab-", dir="/tmp") as temporary:
            root = Path(temporary)
            for backend in (["sqlite", "postgres-local"] if options.postgres_database else ["sqlite"]):
                run_backend(options.binary.resolve(), root, backend, server.server_port, options.postgres_database, options.postgres_user)
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
