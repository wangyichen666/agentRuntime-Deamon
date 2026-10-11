#!/usr/bin/env python3
"""Wave5必要纵向冒烟：原HTTP/daemon/native事实、A2A重启与断流；仅自有夹具。"""
import argparse, http.server as http_server, importlib.util, json, os, socket, subprocess, tempfile, threading, time
from pathlib import Path
REPO=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('local_smoke',REPO/'scripts/smoke-local-backends.py');local=importlib.util.module_from_spec(spec);spec.loader.exec_module(local)
def port():
 with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]
def http(port_,path,body=None):
 import urllib.request
 request=urllib.request.Request(f'http://127.0.0.1:{port_}{path}',data=None if body is None else json.dumps(body).encode(),headers={'Content-Type':'application/json'})
 with urllib.request.urlopen(request,timeout=30) as r:return json.load(r)
def a2a(port_,method,params):
 value=http(port_,'/a2a',dict(jsonrpc='2.0',id=method,method=method,params=params))
 assert 'error' not in value,value
 return value['result']
def wait_task(port_,task):
 deadline=time.monotonic()+30
 while time.monotonic()<deadline:
  value=a2a(port_,'tasks/get',{'id':task})
  if value['status']['state'] in ('completed','failed','canceled','unknown'):return value
  time.sleep(.2)
 raise RuntimeError('任务未在局部验证预算内收敛')
def stop(process):
 if process.poll() is None:
  process.send_signal(2)
  try:process.wait(timeout=15)
  except subprocess.TimeoutExpired:process.kill();process.wait()
def run(args,root,backend,model_port):
 workspace=root/backend;workspace.mkdir();(workspace/'tool-input.txt').write_text('原工具节点读回内容')
 runtime=root/(backend+'-runtime');p=port();schema='workflow_smoke_'+str(os.getpid());env=os.environ.copy()
 for k in ('AGENT_RUNTIME_IDENTITY','AGENT_OIDC_CONFIG','AGENT_CRON_IDENTITY','AGENT_WORKFLOW_IDENTITY','AGENT_POSTGRES_CONFIG','AGENT_GATEWAY_SCOPES','AGENT_GATEWAY_ROUTES','AGENT_GATEWAY_TOKEN','MY_AGENT_API_TOKEN','AGENT_A2A_PUBLIC_URL','SESSION_PATH','PLAN_PATH','MEMORY_PATH','MY_AGENT_HOOKS_CONFIG','HTTP_PROXY','HTTPS_PROXY','ALL_PROXY','http_proxy','https_proxy','all_proxy'):env.pop(k,None)
 env.update(MY_AGENT_CONFIG=str(workspace/'config'),MY_AGENT_RUNTIME_DIR=str(runtime),SKILLS_DIR=str(workspace/'skills'),AGENT_STORAGE_BACKEND=backend,AGENT_ARTIFACT_BACKEND='file-v1',API_TYPE='openai-chat',MODEL_NAME='local-smoke',OPENAI_API_KEY='fixture',OPENAI_BASE_URL=f'http://127.0.0.1:{model_port}/v1',HEARTBEAT_ENABLED='0',NO_PROXY='127.0.0.1,localhost',no_proxy='127.0.0.1,localhost',RUST_LOG='info')
 if backend=='postgres-local':env.update(AGENT_PUBLICATION_DB=args.database,AGENT_PUBLICATION_USER=args.user,AGENT_PUBLICATION_SCHEMA=schema)
 processes=[];logs=[];baseline=local.Model.calls
 def start(command,name):
  log=(workspace/name).open('wb');logs.append(log);proc=subprocess.Popen([str(args.binary),*command],cwd=workspace,env=env,stdout=log,stderr=log);processes.append(proc);return proc
 def ready():
  deadline=time.monotonic()+30
  while time.monotonic()<deadline:
   try:return http(p,'/openapi.json')
   except Exception:
    if any(pr.poll() is not None for pr in processes if pr==processes[-1]):raise RuntimeError('入口启动失败')
    time.sleep(.1)
  raise RuntimeError('入口超时')
 try:
  daemon=start(['daemon','--workspace',str(workspace)],'daemon.log');deadline=time.monotonic()+30
  while not list(runtime.glob('*/ready.json')):
   if daemon.poll() is not None or time.monotonic()>deadline:raise RuntimeError('daemon启动失败')
   time.sleep(.1)
  gateway=start(['serve','--bind',f'127.0.0.1:{p}'],'gateway.log');ready();card=http(p,'/.well-known/agent-card.json');assert card['protocolVersion']=='0.3.0' and card['capabilities']['streaming']
  message={'message':{'kind':'message','role':'user','parts':[{'kind':'text','text':'持久A2A输入'}],'messageId':'stable-message'}}
  task=a2a(p,'message/send',message)['id'];done=wait_task(p,task);assert done['status']['state']=='completed',done;assert done['artifacts'][0]['parts'][0]['text']=='本机冒烟通过'
  assert a2a(p,'message/send',message)['id']==task;assert local.Model.calls-baseline==1
  # 真实ToolCall路径不伪造Provider结果；模型计数不增长。
  kind={'kind':'tool_call','call':{'id':'workflow-read','name':'read_file','arguments':{'path':'tool-input.txt'}}}
  definition={'schema_version':1,'name':'native-tool-smoke','version':'1.0.0','nodes':[{'id':'read','dependencies':[],'kind':kind,'input':{},'retry':{'class':'never'},'max_attempts':1,'timeout_ms':30000,'compensation':None}]}
  published=http(p,'/enterprise/workflows',{'command':'publish','value':{'operation_id':'publish-tool','definition':definition}})
  instance=http(p,'/enterprise/workflows',{'command':'start','value':{'operation_id':'start-tool','definition_digest':published['digest'],'input':{}}});tool=wait_task(p,instance['task_id']);assert tool['status']['state']=='completed',tool;assert '原工具节点读回内容' in tool['artifacts'][0]['parts'][0]['text'];assert local.Model.calls-baseline==1
  # 断开SSE只detach；同一个Task随后可读回完成。
  import urllib.request
  stream_message={'jsonrpc':'2.0','id':'stream','method':'message/stream','params':{'message':{'role':'user','parts':[{'kind':'text','text':'断流后的持久任务'}],'messageId':'stream-message'}}}
  req=urllib.request.Request(f'http://127.0.0.1:{p}/a2a',data=json.dumps(stream_message).encode(),headers={'Content-Type':'application/json'})
  with urllib.request.urlopen(req,timeout=30) as response:
   while True:
    line=response.readline()
    if line.startswith(b'data:'):stream_task=json.loads(line[5:])['result']['id'];break
  assert wait_task(p,stream_task)['status']['state']=='completed';assert local.Model.calls-baseline==2
  stop(gateway);gateway=start(['serve','--bind',f'127.0.0.1:{p}'],'gateway-restart.log');ready();assert a2a(p,'tasks/get',{'id':task})['status']['state']=='completed';assert a2a(p,'message/send',message)['id']==task;assert local.Model.calls-baseline==2
  stop(gateway);stop(daemon);daemon=start(['daemon','--workspace',str(workspace)],'daemon-restart.log');gateway=start(['serve','--bind',f'127.0.0.1:{p}'],'gateway-restart2.log');ready();assert a2a(p,'tasks/get',{'id':task})['status']['state']=='completed';assert a2a(p,'message/send',message)['id']==task;assert local.Model.calls-baseline==2
  return {'backend':backend,'task_persistent':True,'gateway_daemon_restart':True,'disconnect_detaches':True,'tool_original_receipt':True,'model_calls':local.Model.calls-baseline}
 except Exception:
  archive=Path('/tmp')/('agent-wave5-smoke-failure-'+str(os.getpid()));archive.mkdir(exist_ok=True)
  for f in workspace.glob('*.log'):(archive/f.name).write_bytes(f.read_bytes())
  raise
 finally:
  for proc in reversed(processes):stop(proc)
  for log in logs:log.close()
  if backend=='postgres-local':subprocess.run(['psql','-X','-w','-h','/tmp','-U',args.user,'-d',args.database,'-c',f'DROP SCHEMA IF EXISTS {schema} CASCADE'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=True)
def main():
 parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=REPO/'target/debug/my-agent');parser.add_argument('--database',default='agent_continue_20261009_ledger');parser.add_argument('--user',default='pilot');parser.add_argument('--backend',choices=['sqlite','postgres-local','both'],default='both');args=parser.parse_args()
 server=http_server.ThreadingHTTPServer(('127.0.0.1',0),local.Model);threading.Thread(target=server.serve_forever,daemon=True).start()
 try:
  with tempfile.TemporaryDirectory(prefix='wf-',dir='/tmp') as directory:result=[run(args,Path(directory),backend,server.server_port) for backend in (['sqlite','postgres-local'] if args.backend=='both' else [args.backend])];print(json.dumps({'enterprise_ready':False,'local_smoke':result},ensure_ascii=False,indent=2))
 finally:server.shutdown();server.server_close()
if __name__=='__main__':main()
