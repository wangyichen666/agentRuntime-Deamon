#!/usr/bin/env python3
"""真实 Gateway/daemon/PG 入口合同；仅用本机测试签名钥匙和假模型。"""
import argparse
import base64
import http.server
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import shutil
import threading
import time
import urllib.request
import urllib.error
import urllib.parse
import hashlib
from importlib.util import module_from_spec, spec_from_file_location

spec = spec_from_file_location("scopes", Path(__file__).with_name("smoke-enterprise-scopes.py"))
scopes = module_from_spec(spec)
spec.loader.exec_module(scopes)
smoke = scopes.smoke
REPO = Path(__file__).resolve().parent.parent
DEBUG_ROOT = None


class Jwks(http.server.BaseHTTPRequestHandler):
    login = None
    exchange_calls = 0
    signing_root = None
    scope = None
    issuer = None
    def do_POST(self):
        data = urllib.parse.parse_qs(self.rfile.read(int(self.headers["Content-Length"])).decode())
        challenge = base64.urlsafe_b64encode(hashlib.sha256(data["code_verifier"][0].encode()).digest()).rstrip(b"=").decode()
        assert challenge == Jwks.login["code_challenge"][0]
        assert data["code"][0] == "one-use-code" and data["client_id"][0] == "login-client"
        Jwks.exchange_calls += 1
        now = int(time.time())
        id_token = sign_claims(Jwks.signing_root, dict(iss=Jwks.issuer, aud="login-client", sub="mock-human", iat=now, exp=now+120, nonce=Jwks.login["nonce"][0]))
        token = sign(Jwks.signing_root, Jwks.issuer, Jwks.scope, "mock-human", "human")
        body = json.dumps(dict(access_token=token, id_token=id_token, token_type="Bearer")).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass

    def do_GET(self):
        body = (REPO / "crates/identity/tests/fixtures/oidc-jwks.json").read_bytes()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def sign_claims(root, claims):
    def b64(value):
        return base64.urlsafe_b64encode(value).rstrip(b"=").decode()
    unsigned = b64(json.dumps(dict(alg="EdDSA", typ="JWT", kid="fixture-key")).encode()) + "." + b64(json.dumps(claims).encode())
    message = root / "sign-input"
    message.write_bytes(unsigned.encode())
    signature = subprocess.run(["openssl", "pkeyutl", "-sign", "-rawin", "-inkey", str(REPO / "crates/identity/tests/fixtures/oidc-test-private.pem"), "-in", str(message)], capture_output=True, check=True).stdout
    return unsigned + "." + b64(signature)


def sign(root, issuer, scope, subject, kind):
    now = int(time.time())
    return sign_claims(root, dict(iss=issuer, aud="agent-api", sub=subject, subject_kind=kind,
                  revocation_epoch=0, scope=scope, token_use="access", iat=now, nbf=now, exp=now + 300))

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_):
        return None

def browser_get(port, path, cookie=None):
    request = urllib.request.Request(f"http://127.0.0.1:{port}{path}", headers={} if cookie is None else {"Cookie":cookie})
    try:
        response = urllib.request.build_opener(NoRedirect()).open(request, timeout=20)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        body = response.read()
        return response.status, response.headers, body


def rpc(stream, token, sequence, method, params):
    # 不记录 token。显式 v1 包携带认证主体，params 无身份选取能力。
    stream.write((json.dumps(dict(jsonrpc="2.0", id=sequence, method=method, params=params,
                                 protocol_version=1, authorization=token)) + "\n").encode())
    stream.flush()
    while True:
        line = stream.readline()
        if not line:
            raise RuntimeError("daemon 提前断开")
        frame = json.loads(line)
        if frame.get("id") == sequence:
            if "error" in frame:
                raise RuntimeError(f"{method}: {frame['error']}")
            return frame["result"]


def http_request(port, token, path, body, profile=None):
    request = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=None if body is None else json.dumps(body).encode(),
                                    headers={"Authorization": "Bearer " + token, "Content-Type": "application/json", **({"X-Agent-Scope":profile} if profile else {})})
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        if error.code>=500 and DEBUG_ROOT is not None:
            logs=Path('/tmp')/('agent-wave3-gateway-failure-'+str(os.getpid()))
            logs.mkdir(mode=0o700,exist_ok=True)
            for file in DEBUG_ROOT.glob('*log'):
                shutil.copy(file,logs/file.name)
        return error.code, json.load(error)


def main():
    global DEBUG_ROOT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--user", default=os.environ.get("USER", "postgres"))
    parser.add_argument("--registry",action="store_true",help="附加真实签名包/代际/模型配置入口验证")
    args = parser.parse_args()
    schema = "oidc_smoke_" + str(os.getpid())
    model = http.server.ThreadingHTTPServer(("127.0.0.1", 0), smoke.Model)
    jwks = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Jwks)
    for server in [model, jwks]:
        threading.Thread(target=server.serve_forever, daemon=True).start()
    processes = []
    try:
        with tempfile.TemporaryDirectory(prefix="ao-", dir="/tmp") as directory:
            root = Path(directory)
            DEBUG_ROOT = root
            workspace = root / "w"
            workspace.mkdir()
            workspace_two = root / "w2"
            workspace_two.mkdir()
            scope = dict(organization_id="mock-org", tenant_id="mock-tenant", project_id="mock-project", agent_uid="mock-agent")
            principal = dict(subject_id="bootstrap-admin", kind="workload", revocation_epoch=0)
            issuer = f"http://127.0.0.1:{jwks.server_port}"
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
            Jwks.signing_root, Jwks.scope, Jwks.issuer = root, scope, issuer
            scopes.protected(root / "identity", dict(scope=scope, principal=principal))
            scopes.protected(root / "domain", dict(scope=scope, principal=principal, roles=["tenant_admin"], policy=dict(version="oidc-fixture", hard_denies=[], conditions={})))
            scopes.protected(root / "oidc", dict(scope=scope, issuer=issuer, audience="agent-api", jwks_url=issuer + "/jwks", jwks_ttl_secs=30, max_token_lifetime_secs=300, allow_loopback_mock=True, gateway_subject="mock-gateway", oauth=dict(client_id="login-client", authorization_endpoint=issuer+"/authorize", token_endpoint=issuer+"/token",redirect_uri=f"http://127.0.0.1:{port}/auth/callback")))
            node_config = json.loads((root / "oidc").read_text())
            node_config["runtime_endpoint"] = "runtime-two"
            scopes.protected(root / "oidc-two", node_config)
            scopes.protected(root / "routes", dict(endpoints={"runtime-default":str(workspace),"runtime-two":str(workspace_two)}))
            admin_token = sign(root, issuer, scope, "bootstrap-admin", "workload")
            user_token = sign(root, issuer, scope, "mock-human", "human")
            scopes.protected(root / "gateway", dict(token=sign(root, issuer, scope, "mock-gateway", "service_account")))
            env = os.environ.copy()
            for key in ["AGENT_POSTGRES_CONFIG", "AGENT_CRON_IDENTITY", "AGENT_ARTIFACT_BACKEND", "MY_AGENT_API_TOKEN", "SESSION_PATH", "PLAN_PATH", "MEMORY_PATH"]:
                env.pop(key, None)
            env.update(AGENT_PUBLICATION_DB=args.database, AGENT_PUBLICATION_USER=args.user, AGENT_PUBLICATION_SCHEMA=schema,
                       AGENT_RUNTIME_IDENTITY=str(root / "identity"), AGENT_STORAGE_BACKEND="postgres-local", AGENT_OIDC_CONFIG=str(root / "oidc"), AGENT_GATEWAY_TOKEN=str(root / "gateway"), AGENT_GATEWAY_ROUTES=str(root / "routes"),
                       MY_AGENT_RUNTIME_DIR=str(root / "rt"), MY_AGENT_CONFIG=str(workspace / "config"), SKILLS_DIR=str(workspace / "skills"),
                       API_TYPE="openai-chat", MODEL_NAME="oidc-mock", OPENAI_API_KEY="fixture", OPENAI_BASE_URL=f"http://127.0.0.1:{model.server_port}/v1", HEARTBEAT_ENABLED="0", NO_PROXY="127.0.0.1,localhost", no_proxy="127.0.0.1,localhost")
            init = subprocess.run([str(REPO / "target/debug/agent-storage-postgres"), "--canonical-init", str(root / "domain")], env=env, capture_output=True)
            if init.returncode:
                raise RuntimeError("初始化失败：" + init.stderr.decode()[-1000:])
            def start(command, name, launch_env=env, launch_workspace=workspace):
                log = open(root / name, "wb")
                process = subprocess.Popen([str(REPO / "target/debug/my-agent"), *command, "--workspace", str(launch_workspace)], env=launch_env, stdout=log, stderr=log)
                log.close()
                processes.append(process)
                return process
            daemon = start(["daemon"], "daemon-log")
            deadline = time.monotonic() + 25
            while not list((root / "rt").glob("*/ready.json")):
                if daemon.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError("daemon启动失败：" + (root / "daemon-log").read_text()[-2000:])
                time.sleep(.05)
            default_socket = next((root / "rt").glob("*/daemon.sock"))
            with socket.socket(socket.AF_UNIX) as connection:
                connection.settimeout(20)
                connection.connect(str(default_socket))
                with connection.makefile("rwb") as stream:
                    rpc(stream, admin_token, "hello", "connection.initialize", dict(protocol_versions=[1], capabilities=[dict(name=name, schema_version=1) for name in ["session", "runs", "interactions", "context", "memory", "resources", "artifacts", "delegation", "configuration", "maintenance"]]))
                    for subject, kind, roles in [("mock-human", "human", ["developer"]), ("mock-gateway", "service_account", ["gateway"])]:
                        rpc(stream, admin_token, subject + "-register", "identity.manage", dict(operation_id=subject + "-register", command=dict(action="register_subject", principal=dict(subject_id=subject, kind=kind, revocation_epoch=0))))
                        rpc(stream, admin_token, subject + "-roles", "identity.manage", dict(operation_id=subject + "-roles", command=dict(action="bind_roles", subject_id=subject, roles=roles)))
                    rpc(stream, admin_token, "quota", "quota.configure", dict(metric="runs", window_start_ms=0, window_end_ms=9223372036854775807, budget=2))
            node_two_env = dict(env, AGENT_OIDC_CONFIG=str(root / "oidc-two"), MY_AGENT_CONFIG=str(workspace_two / "config"), SKILLS_DIR=str(workspace_two / "skills"))
            node_two = start(["daemon"], "node-two-log", node_two_env, workspace_two)
            deadline = time.monotonic()+25
            while len(list((root / "rt").glob("*/ready.json")))<2:
                if node_two.poll() is not None or time.monotonic()>deadline:
                    raise RuntimeError("第二端点启动失败："+(root / "node-two-log").read_text()[-2000:])
                time.sleep(.05)
            # 同名主体、Agent与operation，在同一个Gateway内必须落入另一tenant。
            workspace_b=root/"wb"; workspace_b.mkdir()
            scope_b=dict(scope,tenant_id="mock-tenant-b")
            scopes.protected(root/"identity-b",dict(scope=scope_b,principal=principal))
            scopes.protected(root/"domain-b",dict(scope=scope_b,principal=principal,roles=["tenant_admin"],policy=dict(version="oidc-fixture-b",hard_denies=[],conditions={})))
            config_b=dict(json.loads((root/"oidc").read_text()),scope=scope_b)
            config_b.pop("oauth")
            scopes.protected(root/"oidc-b",config_b)
            scopes.protected(root/"gateway-b",dict(token=sign(root,issuer,scope_b,"mock-gateway","service_account")))
            scopes.protected(root/"scope-routes",dict(scopes={"tenant_b":dict(workspace=str(workspace_b),oidc_config=str(root/"oidc-b"),gateway_token=str(root/"gateway-b"))}))
            env_b=dict(env,AGENT_RUNTIME_IDENTITY=str(root/"identity-b"),AGENT_OIDC_CONFIG=str(root/"oidc-b"),MY_AGENT_CONFIG=str(workspace_b/"config"),SKILLS_DIR=str(workspace_b/"skills"))
            init_b=subprocess.run([str(REPO/"target/debug/agent-storage-postgres"),"--canonical-init",str(root/"domain-b")],env=env_b,capture_output=True)
            if init_b.returncode:raise RuntimeError("另一租户初始化失败："+init_b.stderr.decode()[-1000:])
            node_b=start(["daemon"],"node-b-log",env_b,workspace_b)
            deadline=time.monotonic()+25
            while len(list((root/"rt").glob("*/ready.json")))<3:
                if node_b.poll() is not None or time.monotonic()>deadline:raise RuntimeError("另一租户启动失败："+(root/"node-b-log").read_text()[-2000:])
                time.sleep(.05)
            # ready文件明确workspace，不依赖目录名猜runtime identity。
            socket_b=None
            for ready in (root/"rt").glob("*/ready.json"):
                data=json.loads(ready.read_text())
                if str(workspace_b) in json.dumps(data):socket_b=ready.parent/"daemon.sock"
            if socket_b is None:
                # 对三个受保护端点只读核验scope，选择B的可信端口。
                for candidate in (root/"rt").glob("*/daemon.sock"):
                    with socket.socket(socket.AF_UNIX) as connection:
                        connection.settimeout(20);connection.connect(str(candidate))
                        with connection.makefile("rwb") as stream:
                            try:
                                identity=rpc(stream,sign(root,issuer,scope_b,"bootstrap-admin","workload"),"scope-id","identity.readback",{})
                                if identity["scope"]==scope_b:socket_b=candidate
                            except RuntimeError:pass
            assert socket_b is not None,"未找到B的可信端点"
            admin_b=sign(root,issuer,scope_b,"bootstrap-admin","workload")
            user_b=sign(root,issuer,scope_b,"mock-human","human")
            with socket.socket(socket.AF_UNIX) as connection:
                connection.settimeout(20);connection.connect(str(socket_b))
                with connection.makefile("rwb") as stream:
                    rpc(stream,admin_b,"hello-b","connection.initialize",dict(protocol_versions=[1],capabilities=[dict(name="configuration",schema_version=1)]))
                    for subject,kind,roles in [("mock-human","human",["developer"]),("mock-gateway","service_account",["gateway"])]:
                        rpc(stream,admin_b,subject+"-register","identity.manage",dict(operation_id=subject+"-register",command=dict(action="register_subject",principal=dict(subject_id=subject,kind=kind,revocation_epoch=0))))
                        rpc(stream,admin_b,subject+"-roles","identity.manage",dict(operation_id=subject+"-roles",command=dict(action="bind_roles",subject_id=subject,roles=roles)))
            env["AGENT_GATEWAY_SCOPES"]=str(root/"scope-routes")
            create_operation = next("create-once-"+str(i) for i in range(10) if sum(("create-once-"+str(i)).encode())%2 == 1)
            gateway = start(["serve", "--bind", f"127.0.0.1:{port}"], "gateway-log")
            deadline = time.monotonic() + 25
            while True:
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=.2):
                        break
                except OSError:
                    if gateway.poll() is not None or time.monotonic() > deadline:
                        raise RuntimeError("Gateway启动失败：" + (root / "gateway-log").read_text()[-2000:])
                    time.sleep(.05)
            generation_one=None
            if args.registry:
                public=subprocess.run(["openssl","pkey","-in",str(REPO/"crates/identity/tests/fixtures/oidc-test-private.pem"),"-pubout","-outform","DER"],capture_output=True,check=True).stdout[-32:]
                roots=dict(version="smoke-roots",roots=[dict(key_id="fixture-root",public_key_base64=base64.b64encode(public).decode(),maximum_trust="organization_reviewed")])
                status,receipt=http_request(port,admin_token,"/enterprise/registry",dict(operation_id="roots",expected_revision=0,command=dict(action="configure_trust",trust=roots)))
                assert status==200,(status,receipt)
                def signed_pack(version,prompt):
                    defaults=dict(system_prompt="默认prompt",models=["oidc-mock"],tool_allowlist=[],skill_refs=[],mcp_refs=[],hook_refs=[],sandbox=dict(backends=["native"],network=False,secrets=False),resources=dict(max_rounds=1,max_tool_calls=0,context_tokens=4096),labels={},secret_refs={})
                    layers=dict(defaults=defaults,organization={},tenant=dict(system_prompt="租户prompt"),project=dict(system_prompt=prompt),agent={},session_override={})
                    manifest=dict(schema_version=1,definition=dict(scope=scope,name="mock-agent",version=version,layers=layers),files={},trust="project_local")
                    message=root/"pack-sign-input";message.write_bytes(b"agent-pack-v1\x00"+json.dumps(manifest,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode())
                    signature=subprocess.run(["openssl","pkeyutl","-sign","-rawin","-inkey",str(REPO/"crates/identity/tests/fixtures/oidc-test-private.pem"),"-in",str(message)],capture_output=True,check=True).stdout
                    return dict(manifest=manifest,contents=[],key_id="fixture-root",signature_base64=base64.b64encode(signature).decode())
                status,receipt=http_request(port,admin_token,"/enterprise/registry",dict(operation_id="publish-v1",expected_revision=1,command=dict(action="publish",pack=signed_pack("1.0.0","发行代V1：真实模型请求"))))
                assert status==200,(status,receipt);generation_one=receipt["generation"]["pack_digest"]
                status,receipt=http_request(port,admin_token,"/enterprise/registry",dict(operation_id="rollout-v1",expected_revision=2,command=dict(action="rollout",stable=generation_one,canary=None,canary_basis_points=0)))
                assert status==200,(status,receipt)
            status, route = http_request(port, user_token, "/enterprise/sessions", dict(operation_id=create_operation))
            assert status == 200 and set(route) == {"session_id"}, (status, route)
            with socket.socket(socket.AF_UNIX) as connection:
                connection.settimeout(20)
                connection.connect(str(default_socket))
                with connection.makefile("rwb") as stream:
                    rpc(stream, user_token, "route-hello", "connection.initialize", dict(protocol_versions=[1], capabilities=[dict(name="session",schema_version=1)]))
                    native_route = rpc(stream, user_token, "route-proof", "directory.resolve", dict(session_id=route["session_id"]))
                    assert native_route["runtime_endpoint"]=="runtime-two", native_route
            status, repeat = http_request(port, user_token, "/enterprise/sessions", dict(operation_id=create_operation))
            assert status == 200 and repeat == route, (status, repeat)
            status,route_b=http_request(port,user_b,"/enterprise/sessions",dict(operation_id=create_operation))
            assert status==200 and route_b!=route,(status,route_b)
            status,denied=http_request(port,user_b,"/enterprise/sessions",dict(operation_id="scope-mismatch"),"default")
            assert status==401,(status,denied)
            status,denied=http_request(port,user_token,"/enterprise/rpc",dict(session_id=route_b["session_id"],method="session.load",params={}))
            assert status in [403,404],(status,denied)
            status,issued=http_request(port,admin_b,"/enterprise/credentials/issue",dict(subject_id="mock-human",ttl_secs=120,operation_id="key-b"))
            assert status==200 and issued.get("token"),(status,issued)
            key_b=issued["token"]
            status,repeat_b=http_request(port,key_b,"/enterprise/sessions",dict(operation_id=create_operation),"tenant_b")
            assert status==200 and repeat_b==route_b,(status,repeat_b)
            status,denied=http_request(port,key_b,"/enterprise/sessions",dict(operation_id=create_operation))
            assert status==401,(status,denied)
            if args.registry:
                status,receipt=http_request(port,admin_token,"/enterprise/registry",dict(operation_id="publish-v2",expected_revision=3,command=dict(action="publish",pack=signed_pack("2.0.0","发行代V2：新会话使用"))))
                assert status==200,(status,receipt);generation_two=receipt["generation"]["pack_digest"]
                status,receipt=http_request(port,admin_token,"/enterprise/registry",dict(operation_id="canary-v2",expected_revision=4,command=dict(action="rollout",stable=generation_one,canary=generation_two,canary_basis_points=10000)))
                assert status==200,(status,receipt)
                status,read=http_request(port,user_token,"/enterprise/registry/generation?digest="+generation_one,None)
                assert status==200 and read["effective"]["sources"]["system_prompt"]=="project",(status,read)
            billing=dict(version="smoke-prices-1",currency="USD",window_start_ms=0,window_end_ms=9223372036854775807,
                         models={"oidc-mock":dict(input_per_million=1000000,output_per_million=2000000,cache_read_per_million=0,cache_creation_per_million=0)},
                         tool_call_micros=0,sandbox_per_million_ms=0,
                         limits=dict(max_rounds=1,max_provider_attempts=1,max_input_tokens=1000000,max_output_tokens_per_attempt=128,max_tool_calls=0,max_sandbox_wall_ms=0,max_sandbox_wall_ms_per_resource=0),
                         budgets=[dict(dimension=dict(kind=kind),hard_micros=2000000,soft_micros=1) for kind in ["tenant","project","agent"]]+[dict(dimension=dict(kind="model",model="oidc-mock"),hard_micros=2000000,soft_micros=1)])
            status, prices=http_request(port,admin_token,"/enterprise/billing/configure",dict(policy=billing,operation_id="prices-once"))
            assert status==200,(status,prices)
            status, result = http_request(port, user_token, "/enterprise/rpc", dict(session_id=route["session_id"], method="chat.send", params=dict(message="真实OIDC入口冒烟")))
            if status!=200:
                other_socket=next(path for path in (root/"rt").glob("*/daemon.sock") if path!=default_socket)
                with socket.socket(socket.AF_UNIX) as diagnostic:
                    diagnostic.settimeout(10);diagnostic.connect(str(other_socket))
                    with diagnostic.makefile("rwb") as stream:
                        try:
                            rpc(stream,user_token,"diagnostic-hello","connection.initialize",dict(protocol_versions=[1],capabilities=[dict(name="runs",schema_version=1)]))
                            fact=rpc(stream,user_token,"diagnostic-read","run.read",dict(run_id="run-1"))
                            result["native_diagnostic"]={key:fact.get(key) for key in ["run_id","status","error_code","error_message"]}
                        except RuntimeError as error:
                            result["native_diagnostic"]=str(error)
            assert status == 200 and result.get("content") == "本机冒烟通过", (status, result)
            assert smoke.Model.requests[-1]["max_tokens"]==128,smoke.Model.requests[-1].keys()
            if args.registry:
                assert any("发行代V1：真实模型请求" in str(message["content"]) for message in smoke.Model.requests[-1]["messages"]),smoke.Model.requests[-1]["messages"]
                assert not any("发行代V2：新会话使用" in str(message["content"]) for message in smoke.Model.requests[-1]["messages"])
                assert not smoke.Model.requests[-1].get("tools"),"工具白名单为空须真正限制模型catalog"
            status,usage=http_request(port,user_token,"/enterprise/billing/usage",None)
            assert status==200 and len(usage["entries"])==1 and usage["entries"][0]["cost_micros"]==30,(status,usage)
            status,accounts=http_request(port,user_token,"/enterprise/billing/accounts",None)
            assert status==200 and all(a["reserved_micros"]==0 and a["consumed_micros"]==30 for a in accounts["accounts"]),(status,accounts)

            assert status == 200 and result["content"] == "本机冒烟通过", (status, result)
            foreign = sign(root, issuer, dict(scope, tenant_id="foreign-tenant"), "mock-human", "human")
            status, denied = http_request(port, foreign, "/enterprise/sessions", dict(operation_id="foreign-denied"))
            assert status == 401 and denied["error"]["safe_auto_retry"] is False, (status, denied)
            # 运营方调整路由文件后，原operation仍读回原目录，不重新创建/执行。
            gateway.send_signal(__import__("signal").SIGINT)
            assert gateway.wait(timeout=15)==0
            (root / "routes").write_text(json.dumps(dict(endpoints={"runtime-two":str(workspace_two)})))
            gateway=start(["serve","--bind",f"127.0.0.1:{port}"],"gateway-restart-log")
            deadline=time.monotonic()+25
            while True:
                try:
                    with socket.create_connection(("127.0.0.1",port),timeout=.2):break
                except OSError:
                    if gateway.poll() is not None or time.monotonic()>deadline:raise RuntimeError("Gateway重启失败")
                    time.sleep(.05)
            status, repeat = http_request(port,user_token,"/enterprise/sessions",dict(operation_id=create_operation))
            assert status==200 and repeat==route,(status,repeat)
            status, fact = http_request(port,user_token,"/enterprise/rpc",dict(session_id=route["session_id"],method="run.read",params=dict(run_id=result["run_id"])))
            assert status==200 and fact["status"]=="completed",(status,fact)
            assert fact["_my_agent_view"]["session_key"]==route["session_id"] and "snapshot" not in fact
            login_status, login_headers, _ = browser_get(port, "/auth/login")
            assert login_status == 303, login_status
            cookie = login_headers["Set-Cookie"].split(";", 1)[0]
            Jwks.login = urllib.parse.parse_qs(urllib.parse.urlparse(login_headers["Location"]).query)
            assert Jwks.login["code_challenge_method"] == ["S256"]
            query = dict(code="one-use-code", state=Jwks.login["state"][0], iss=issuer)
            wrong_state = dict(query, state="incorrect-state")
            assert browser_get(port, "/auth/callback?"+urllib.parse.urlencode(wrong_state), cookie)[0] == 401
            callback_status, callback_headers, callback_body = browser_get(port, "/auth/callback?"+urllib.parse.urlencode(query), cookie)
            assert callback_status == 200, (callback_status, callback_body.decode())
            assert callback_headers["Cache-Control"] == "no-store"
            login_token = json.loads(callback_body)["access_token"]
            assert browser_get(port, "/auth/callback?"+urllib.parse.urlencode(query), cookie)[0] == 401
            assert Jwks.exchange_calls == 1, "state 重放不能兑换授权码"
            status, saved = http_request(port, login_token, "/enterprise/rpc", dict(session_id=route["session_id"], method="session.load", params={}))
            assert status == 200, (status, saved)
            assert smoke.Model.calls == 1, smoke.Model.calls
            print(json.dumps(dict(verified=True, scope=scope, runtime_endpoints=3, gateway_scopes=2, scope_api_key_verified=True, gateway_restarts=1, directory_opaque=True, foreign_token_status=401, model_calls=1, login_verified=True, code_exchanges=Jwks.exchange_calls, registry_verified=args.registry, frozen_generation=generation_one, billing_verified=True, cost_micros=30, provider_output_bound=128, transport="HTTP Gateway delegation + UDS + canonical PostgreSQL", note="本机测试签名钥匙；远程OIDC和轮转另验"), ensure_ascii=False))
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        for server in [jwks, model]:
            server.shutdown()
            server.server_close()
        subprocess.run(["psql", "-X", "-w", "-h", "/tmp", "-U", args.user, "-d", args.database, "-c", f"DROP SCHEMA IF EXISTS {schema} CASCADE"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
