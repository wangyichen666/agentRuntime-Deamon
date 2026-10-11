-- PostgreSQL canonical facts v1。独立于发布协调版本；业务表不是影子状态。

-- 全部业务主键、唯一键和外键携带四维域；历史 lifetime 永久归属。

CREATE TABLE canonical_migrations(version BIGINT PRIMARY KEY, checksum TEXT NOT NULL CHECK(octet_length(checksum)=64), applied_at_ms BIGINT NOT NULL);
CREATE TABLE organizations(id TEXT PRIMARY KEY CHECK(octet_length(id) BETWEEN 1 AND 128));
CREATE TABLE tenants(organization_id TEXT NOT NULL REFERENCES organizations(id),id TEXT NOT NULL,PRIMARY KEY(organization_id,id));
CREATE TABLE projects(organization_id TEXT NOT NULL,tenant_id TEXT NOT NULL,id TEXT NOT NULL,PRIMARY KEY(organization_id,tenant_id,id),FOREIGN KEY(organization_id,tenant_id) REFERENCES tenants(organization_id,id));
CREATE TABLE agents(organization_id TEXT NOT NULL,tenant_id TEXT NOT NULL,project_id TEXT NOT NULL,uid TEXT NOT NULL,PRIMARY KEY(organization_id,tenant_id,project_id,uid),FOREIGN KEY(organization_id,tenant_id,project_id) REFERENCES projects(organization_id,tenant_id,id));
CREATE TABLE principals(organization_id TEXT NOT NULL,tenant_id TEXT NOT NULL,subject_id TEXT NOT NULL,kind TEXT NOT NULL CHECK(kind IN ('human','service_account','workload')),revocation_epoch BIGINT NOT NULL DEFAULT 0 CHECK(revocation_epoch>=0),PRIMARY KEY(organization_id,tenant_id,subject_id),FOREIGN KEY(organization_id,tenant_id) REFERENCES tenants(organization_id,id));

CREATE TABLE role_bindings(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 subject_id TEXT NOT NULL,role TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,subject_id,role),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid)
);

ALTER TABLE role_bindings ADD FOREIGN KEY(organization_id,tenant_id,subject_id) REFERENCES principals(organization_id,tenant_id,subject_id);

CREATE TABLE identity_policies(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 version TEXT NOT NULL,document_json TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,version),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 CHECK(octet_length(document_json)<=1048576)
);

CREATE TABLE identity_policy_heads(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 version TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,version) REFERENCES identity_policies(organization_id,tenant_id,project_id,agent_uid,version) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE sessions(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 id TEXT NOT NULL CHECK(octet_length(id) BETWEEN 1 AND 1024),created_at_ms BIGINT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid)
);

CREATE TABLE session_scopes(organization_id TEXT NOT NULL,tenant_id TEXT NOT NULL,project_id TEXT NOT NULL,agent_uid TEXT NOT NULL,session_id TEXT NOT NULL,PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,session_id),FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,session_id) REFERENCES sessions(organization_id,tenant_id,project_id,agent_uid,id));
ALTER TABLE session_scopes ENABLE ROW LEVEL SECURITY;
ALTER TABLE session_scopes FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON session_scopes USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

CREATE TABLE session_lifetime_owners(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 lifetime TEXT NOT NULL CHECK(octet_length(lifetime) BETWEEN 1 AND 128),session_id TEXT,provenance TEXT NOT NULL CHECK(provenance IN ('canonical','legacy_orphan')),
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,lifetime),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 UNIQUE(organization_id,tenant_id,project_id,agent_uid,session_id,lifetime),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,session_id) REFERENCES sessions(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 CHECK((session_id IS NOT NULL AND provenance='canonical') OR (session_id IS NULL AND provenance='legacy_orphan'))
);

CREATE TABLE session_heads(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 session_id TEXT NOT NULL,lifetime TEXT NOT NULL,deleted BOOLEAN NOT NULL DEFAULT FALSE,revision BIGINT NOT NULL DEFAULT 0,projection_generation BIGINT NOT NULL DEFAULT 0,run_generation BIGINT NOT NULL DEFAULT 0,legacy_imported BOOLEAN NOT NULL DEFAULT FALSE,updated_at_ms BIGINT NOT NULL,metadata_revision BIGINT NOT NULL DEFAULT 1,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,session_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 UNIQUE(organization_id,tenant_id,project_id,agent_uid,lifetime),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,session_id) REFERENCES sessions(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,session_id,lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,session_id,lifetime) DEFERRABLE INITIALLY DEFERRED,
 CHECK(revision>=0),
 CHECK(projection_generation>=0),
 CHECK(run_generation>=0),
 CHECK(metadata_revision>0)
);

CREATE TABLE snapshot_clock(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 slot SMALLINT NOT NULL DEFAULT 1 CHECK(slot=1),revision BIGINT NOT NULL DEFAULT 1 CHECK(revision>0),
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,slot),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid)
);

CREATE TABLE session_tombstones(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 lifetime TEXT NOT NULL,session_id TEXT NOT NULL,deleted_at_ms BIGINT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,lifetime),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,session_id,lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,session_id,lifetime) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE runtime_settings(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 name TEXT NOT NULL,value TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,name),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 CHECK(octet_length(value)<=65536)
);

CREATE TABLE lifecycle_receipts(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 operation_id TEXT NOT NULL,command_json TEXT NOT NULL,result_json TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,operation_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 CHECK(octet_length(operation_id) BETWEEN 1 AND 1024),
 CHECK(octet_length(command_json)<=16384),
 CHECK(octet_length(result_json)<=65536)
);

CREATE TABLE runs(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 storage_row_id BIGINT GENERATED BY DEFAULT AS IDENTITY,id TEXT NOT NULL,session_id TEXT NOT NULL,lifetime TEXT NOT NULL,generation BIGINT NOT NULL CHECK(generation>0),request_id_json TEXT NOT NULL,request_id_wire_json TEXT NOT NULL,status TEXT NOT NULL CHECK(status IN ('queued','running','waiting_interaction','completed','failed','cancelled','unknown_after_restart')),input TEXT NOT NULL,last_seq BIGINT NOT NULL DEFAULT 0 CHECK(last_seq>=0),content TEXT,error_code BIGINT,error_message TEXT,created_at_ms BIGINT NOT NULL,updated_at_ms BIGINT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 UNIQUE(organization_id,tenant_id,project_id,agent_uid,session_id,lifetime,request_id_json),
 UNIQUE(organization_id,tenant_id,project_id,agent_uid,session_id,lifetime,generation),
 UNIQUE(organization_id,tenant_id,project_id,agent_uid,id,session_id,lifetime,generation),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,session_id,lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,session_id,lifetime) DEFERRABLE INITIALLY DEFERRED,
 CHECK(octet_length(input)<=65536),
 CHECK(octet_length(content)<=524288),
 CHECK(octet_length(error_message)<=65536),
 CHECK(octet_length(request_id_json)<=16384),
 CHECK(octet_length(id) BETWEEN 1 AND 1024)
);

CREATE TABLE turns(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 id TEXT NOT NULL,run_id TEXT NOT NULL,status TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 UNIQUE(organization_id,tenant_id,project_id,agent_uid,run_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE run_authorizations(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 run_id TEXT NOT NULL,subject_id TEXT NOT NULL,revocation_epoch BIGINT NOT NULL,snapshot_json TEXT NOT NULL,digest TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,run_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 CHECK(revocation_epoch>=0),
 CHECK(octet_length(snapshot_json)<=65536),
 CHECK(octet_length(digest)=64)
);

ALTER TABLE run_authorizations ADD FOREIGN KEY(organization_id,tenant_id,subject_id) REFERENCES principals(organization_id,tenant_id,subject_id);

CREATE TABLE session_writer_leases(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 session_key TEXT NOT NULL,lifetime TEXT NOT NULL,owner_id TEXT NOT NULL,epoch BIGINT NOT NULL CHECK(epoch>0),expires_at_ms BIGINT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,session_key,lifetime),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,session_key,lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,session_id,lifetime) DEFERRABLE INITIALLY DEFERRED,
 CHECK(octet_length(owner_id) BETWEEN 1 AND 128)
);

CREATE TABLE events(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 run_id TEXT NOT NULL,seq BIGINT NOT NULL CHECK(seq>0),event TEXT NOT NULL,data_json TEXT NOT NULL,created_at_ms BIGINT NOT NULL,view_provenance TEXT NOT NULL DEFAULT 'native' CHECK(view_provenance IN ('native','legacy')),
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,run_id,seq),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 CHECK(octet_length(event) BETWEEN 1 AND 128),
 CHECK(octet_length(data_json)<=4194304)
);

CREATE TABLE event_view_stamps(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 run_id TEXT NOT NULL,seq BIGINT NOT NULL,stamp_json TEXT NOT NULL,stamp_digest TEXT NOT NULL,published BOOLEAN NOT NULL DEFAULT FALSE,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,run_id,seq),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id,seq) REFERENCES events(organization_id,tenant_id,project_id,agent_uid,run_id,seq) DEFERRABLE INITIALLY DEFERRED,
 CHECK(octet_length(stamp_json)<=65536),
 CHECK(octet_length(stamp_digest)=64)
);

CREATE TABLE transcript_batches(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 storage_row_id BIGINT GENERATED BY DEFAULT AS IDENTITY,lifetime TEXT NOT NULL,operation_id TEXT NOT NULL,run_id TEXT,start_seq BIGINT NOT NULL,end_seq BIGINT NOT NULL,messages_json TEXT NOT NULL,digest TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,lifetime,operation_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 UNIQUE(organization_id,tenant_id,project_id,agent_uid,lifetime,start_seq,operation_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,lifetime) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 CHECK(start_seq>=0),
 CHECK(end_seq>=start_seq),
 CHECK(octet_length(messages_json)<=16777216),
 CHECK(octet_length(digest)=64)
);

CREATE TABLE interactions(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 id TEXT NOT NULL,run_id TEXT NOT NULL,lifetime TEXT NOT NULL,generation BIGINT NOT NULL,kind TEXT NOT NULL,status TEXT NOT NULL,prompt TEXT NOT NULL,response_json TEXT,revision BIGINT NOT NULL DEFAULT 0,payload_json TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,lifetime) DEFERRABLE INITIALLY DEFERRED,
 CHECK(octet_length(prompt)<=16384),
 CHECK(octet_length(payload_json)<=65536),
 CHECK(octet_length(response_json)<=65536),
 CHECK(revision>=0)
);

CREATE TABLE queued_messages(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 id BIGINT GENERATED BY DEFAULT AS IDENTITY,session_id TEXT NOT NULL,run_id TEXT,message TEXT NOT NULL,status TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 UNIQUE(organization_id,tenant_id,project_id,agent_uid,run_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,session_id) REFERENCES sessions(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 CHECK(octet_length(message)<=65536)
);

CREATE TABLE tool_executions(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 id BIGINT GENERATED BY DEFAULT AS IDENTITY,run_id TEXT NOT NULL,lifetime TEXT NOT NULL,generation BIGINT NOT NULL,round BIGINT NOT NULL,call_id TEXT NOT NULL,name TEXT NOT NULL,status TEXT NOT NULL,effect TEXT NOT NULL,argument_digest TEXT NOT NULL,prepared_at_ms BIGINT NOT NULL,started_at_ms BIGINT,finished_at_ms BIGINT,outcome TEXT,artifact_ref TEXT,safe_to_replay BOOLEAN NOT NULL DEFAULT FALSE,receipt_json TEXT,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 UNIQUE(organization_id,tenant_id,project_id,agent_uid,run_id,round,call_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,lifetime) DEFERRABLE INITIALLY DEFERRED,
 CHECK(round>=0),
 CHECK(octet_length(receipt_json)<=1048576)
);

CREATE TABLE run_routes(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 run_id TEXT NOT NULL,snapshot_json TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,run_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 CHECK(octet_length(snapshot_json)<=1048576)
);

CREATE TABLE provider_attempts(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 storage_row_id BIGINT GENERATED BY DEFAULT AS IDENTITY,attempt_id TEXT NOT NULL,run_id TEXT NOT NULL,round BIGINT NOT NULL,candidate_index BIGINT NOT NULL,status TEXT NOT NULL,data_json TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,attempt_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 CHECK(round>0),
 CHECK(candidate_index>=0),
 CHECK(octet_length(data_json)<=1048576)
);

CREATE TABLE delegations(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 id BIGINT GENERATED BY DEFAULT AS IDENTITY,root_session_id TEXT NOT NULL,root_run_id TEXT NOT NULL,parent_session_id TEXT NOT NULL,parent_run_id TEXT NOT NULL,spawn_key TEXT NOT NULL,child_session_id TEXT NOT NULL,child_run_id TEXT NOT NULL,parent_lifetime TEXT NOT NULL,child_lifetime TEXT NOT NULL,depth BIGINT NOT NULL,status TEXT NOT NULL,created_at_ms BIGINT NOT NULL,started_at_ms BIGINT,finished_at_ms BIGINT,tools_json TEXT NOT NULL,permission_mode TEXT NOT NULL,route_json TEXT,cwd TEXT NOT NULL,max_rounds BIGINT NOT NULL,max_tokens BIGINT NOT NULL,max_tool_calls BIGINT NOT NULL,deadline_ms BIGINT NOT NULL,terminal_json TEXT,result_state TEXT NOT NULL DEFAULT 'unconsumed',reservation_owner TEXT,revision BIGINT NOT NULL DEFAULT 0,reservation_released_at_ms BIGINT,reservation_expires_at_ms BIGINT,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 UNIQUE(organization_id,tenant_id,project_id,agent_uid,parent_run_id,spawn_key),
 UNIQUE(organization_id,tenant_id,project_id,agent_uid,child_session_id),
 UNIQUE(organization_id,tenant_id,project_id,agent_uid,child_run_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,root_session_id) REFERENCES sessions(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,root_run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,parent_session_id) REFERENCES sessions(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,parent_run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,child_session_id) REFERENCES sessions(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,child_run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,parent_lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,lifetime) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,child_lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,lifetime) DEFERRABLE INITIALLY DEFERRED,
 CHECK(depth BETWEEN 1 AND 8),
 CHECK(octet_length(terminal_json)<=131072)
);

CREATE TABLE run_snapshots(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 run_id TEXT NOT NULL,snapshot_json TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,run_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE turn_outputs(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 run_id TEXT NOT NULL,message_json TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,run_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE turn_plan_stages(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 run_id TEXT NOT NULL,base_revision BIGINT NOT NULL,revision BIGINT NOT NULL,data_json TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,run_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE turn_commits(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 run_id TEXT NOT NULL,owner_json TEXT NOT NULL,usage_json TEXT NOT NULL,status TEXT NOT NULL,plan_revision BIGINT,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,run_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tool_batches(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 run_id TEXT NOT NULL,round BIGINT NOT NULL,calls_json TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'prepared',descriptor_json TEXT NOT NULL DEFAULT '[]',
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,run_id,round),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 CHECK(round>0),
 CHECK(octet_length(calls_json)<=1048576),
 CHECK(octet_length(descriptor_json)<=1048576)
);

CREATE TABLE session_plans(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 lifetime TEXT NOT NULL,revision BIGINT NOT NULL,data_json TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,lifetime),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,lifetime) DEFERRABLE INITIALLY DEFERRED,
 CHECK(octet_length(data_json)<=1048576)
);

CREATE TABLE plan_legacy_evidence(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 lifetime TEXT NOT NULL,revision BIGINT NOT NULL,data_json TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,lifetime),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,lifetime) DEFERRABLE INITIALLY DEFERRED,
 CHECK(octet_length(data_json)<=1048576)
);

CREATE TABLE plan_versions(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 lifetime TEXT NOT NULL,revision BIGINT NOT NULL,plan_id TEXT NOT NULL,content_digest TEXT NOT NULL,document_json TEXT NOT NULL,markdown TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,lifetime,revision),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,lifetime) DEFERRABLE INITIALLY DEFERRED,
 CHECK(revision>0),
 CHECK(octet_length(content_digest)=64),
 CHECK(octet_length(document_json)<=1048576),
 CHECK(octet_length(markdown)<=1048576)
);

CREATE TABLE plan_decisions(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 operation_id TEXT NOT NULL,session_id TEXT NOT NULL,lifetime TEXT NOT NULL,identity_json TEXT NOT NULL,decision TEXT NOT NULL CHECK(decision IN ('execute','discard')),input TEXT NOT NULL,run_id TEXT,receipt_json TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,operation_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 UNIQUE(organization_id,tenant_id,project_id,agent_uid,run_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,session_id,lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,session_id,lifetime) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 CHECK(octet_length(receipt_json)<=65536)
);

CREATE TABLE legacy_plan_imports(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 source TEXT NOT NULL,digest TEXT NOT NULL,lifetime TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,source),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,lifetime) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE compact_operations(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 operation TEXT NOT NULL,run_id TEXT NOT NULL,lifetime TEXT NOT NULL,source_json TEXT NOT NULL,state TEXT NOT NULL,reason TEXT,result_generation BIGINT,candidate_digest TEXT,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,operation),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,lifetime) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE compact_run_links(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 run_id TEXT NOT NULL,operation_id TEXT NOT NULL,request_json TEXT NOT NULL,source_json TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,run_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 UNIQUE(organization_id,tenant_id,project_id,agent_uid,operation_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE context_heads(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 lifetime TEXT NOT NULL,source_end BIGINT NOT NULL,generation BIGINT NOT NULL,messages_json TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,lifetime),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,lifetime) DEFERRABLE INITIALLY DEFERRED,
 CHECK(source_end>=0),
 CHECK(generation>=0),
 CHECK(octet_length(messages_json)<=16777216)
);

CREATE TABLE context_ledgers(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 run_id TEXT NOT NULL,round BIGINT NOT NULL,envelope_json TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,run_id,round),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 CHECK(round>0),
 CHECK(octet_length(envelope_json)<=1048576)
);

CREATE TABLE maintenance_diagnostics(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 run_id TEXT NOT NULL,kind TEXT NOT NULL,diagnostic TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,run_id,kind),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE resources(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 id BIGINT GENERATED BY DEFAULT AS IDENTITY,run_id TEXT NOT NULL,lifetime TEXT NOT NULL,state TEXT NOT NULL,owner_json TEXT NOT NULL,cwd TEXT NOT NULL,requested TEXT NOT NULL,effective TEXT NOT NULL,process_identity TEXT,log_cursor BIGINT NOT NULL DEFAULT 0,terminal_reason TEXT,updated_at_ms BIGINT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,lifetime) DEFERRABLE INITIALLY DEFERRED,
 CHECK(log_cursor>=0),
 CHECK(octet_length(owner_json)<=16384)
);

CREATE TABLE resource_logs(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 resource_id BIGINT NOT NULL,cursor BIGINT NOT NULL,content TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,resource_id,cursor),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,resource_id) REFERENCES resources(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 CHECK(cursor>=0),
 CHECK(octet_length(content)<=65536)
);

CREATE TABLE memories(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 id TEXT NOT NULL,lifetime TEXT,project TEXT,scope TEXT NOT NULL,source_run TEXT,revision BIGINT NOT NULL,data_json TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,lifetime) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,source_run) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 CHECK(revision>=0),
 CHECK(octet_length(data_json)<=262144)
);

CREATE TABLE memory_legacy_imports(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 source TEXT NOT NULL,digest TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,source),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid)
);

CREATE TABLE memory_ingests(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 run_id TEXT NOT NULL,lifetime TEXT NOT NULL,status TEXT NOT NULL,diagnostic TEXT,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,run_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,lifetime) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE memory_forget_receipts(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 run_id TEXT NOT NULL,memory_id TEXT NOT NULL,revision BIGINT NOT NULL,result BOOLEAN NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,run_id,memory_id,revision),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE memory_exposures(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 run_id TEXT NOT NULL,memory_id TEXT NOT NULL,lifetime TEXT NOT NULL,content_digest TEXT NOT NULL,revision BIGINT NOT NULL,channel TEXT NOT NULL,policy TEXT NOT NULL,created_at_ms BIGINT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,run_id,memory_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,memory_id) REFERENCES memories(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,lifetime) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE memory_assessments(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 lifetime TEXT NOT NULL,memory_id TEXT NOT NULL,content_digest TEXT NOT NULL,feedback TEXT NOT NULL,updated_at_ms BIGINT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,lifetime,memory_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,lifetime) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,memory_id) REFERENCES memories(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE memory_feedback_receipts(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 run_id TEXT NOT NULL,operation_id TEXT NOT NULL,memory_id TEXT NOT NULL,lifetime TEXT NOT NULL,command_json TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,run_id,operation_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,memory_id) REFERENCES memories(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,lifetime) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE memory_ingest_retries(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 run_id TEXT NOT NULL,attempts BIGINT NOT NULL,retry_at_ms BIGINT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,run_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 CHECK(attempts>=0)
);

CREATE TABLE hook_outcomes(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 storage_row_id BIGINT GENERATED BY DEFAULT AS IDENTITY,operation_id TEXT NOT NULL,session_id TEXT NOT NULL,lifetime TEXT NOT NULL,outcome_json TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,operation_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,session_id,lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,session_id,lifetime) DEFERRABLE INITIALLY DEFERRED,
 CHECK(octet_length(outcome_json)<=49152)
);

CREATE TABLE hook_publications(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 storage_row_id BIGINT GENERATED BY DEFAULT AS IDENTITY,operation_id TEXT NOT NULL,payload_json TEXT NOT NULL,dispatched BOOLEAN NOT NULL DEFAULT FALSE,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,operation_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 CHECK(octet_length(payload_json)<=16384)
);

CREATE TABLE hook_continuations(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 parent_run_id TEXT NOT NULL,child_run_id TEXT NOT NULL,operation_id TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,parent_run_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 UNIQUE(organization_id,tenant_id,project_id,agent_uid,child_run_id),
 UNIQUE(organization_id,tenant_id,project_id,agent_uid,operation_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,parent_run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,child_run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,operation_id) REFERENCES hook_outcomes(organization_id,tenant_id,project_id,agent_uid,operation_id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tool_discoveries(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 storage_row_id BIGINT GENERATED BY DEFAULT AS IDENTITY,operation_id TEXT NOT NULL,run_id TEXT NOT NULL,receipt_json TEXT NOT NULL,receipt_digest TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,operation_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 CHECK(octet_length(receipt_json)<=1048576),
 CHECK(octet_length(receipt_digest)=64)
);

CREATE TABLE provider_requests(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 storage_row_id BIGINT GENERATED BY DEFAULT AS IDENTITY,run_id TEXT NOT NULL,capture_id TEXT NOT NULL,round BIGINT NOT NULL,candidate_index BIGINT NOT NULL,capture_json TEXT NOT NULL,capture_digest TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,run_id,capture_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 CHECK(round>0),
 CHECK(candidate_index>=0),
 CHECK(octet_length(capture_json)<=16777216),
 CHECK(octet_length(capture_digest)=64)
);

CREATE TABLE artifact_publications(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 publication_id TEXT NOT NULL,run_id TEXT NOT NULL,round BIGINT NOT NULL,call_id TEXT NOT NULL,store_id TEXT NOT NULL,sha256 TEXT NOT NULL,bytes BIGINT NOT NULL,reservation_json TEXT NOT NULL,reservation_digest TEXT NOT NULL,published BOOLEAN NOT NULL DEFAULT FALSE,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,publication_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 UNIQUE(organization_id,tenant_id,project_id,agent_uid,run_id,round,call_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id,round,call_id) REFERENCES tool_executions(organization_id,tenant_id,project_id,agent_uid,run_id,round,call_id) DEFERRABLE INITIALLY DEFERRED,
 CHECK(bytes BETWEEN 0 AND 67108864),
 CHECK(octet_length(sha256)=64),
 CHECK(octet_length(reservation_json)<=65536),
 CHECK(octet_length(reservation_digest)=64)
);

CREATE TABLE audit_heads(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 slot SMALLINT NOT NULL DEFAULT 1 CHECK(slot=1),seq BIGINT NOT NULL DEFAULT 0,head_digest TEXT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,slot),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 CHECK(seq>=0),
 CHECK(octet_length(head_digest)=64)
);

CREATE TABLE audit_ledger(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 seq BIGINT NOT NULL,operation_id TEXT NOT NULL,subject_id TEXT NOT NULL,action TEXT NOT NULL,resource_json TEXT NOT NULL,outcome TEXT NOT NULL,previous_digest TEXT NOT NULL,entry_digest TEXT NOT NULL,entry_json TEXT NOT NULL,created_at_ms BIGINT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,seq),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 UNIQUE(organization_id,tenant_id,project_id,agent_uid,operation_id),
 CHECK(seq>0),
 CHECK(octet_length(previous_digest)=64),
 CHECK(octet_length(entry_digest)=64),
 CHECK(octet_length(entry_json)<=65536)
);

CREATE TABLE quota_accounts(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 metric TEXT NOT NULL,budget BIGINT NOT NULL,consumed BIGINT NOT NULL DEFAULT 0,reserved BIGINT NOT NULL DEFAULT 0,revision BIGINT NOT NULL DEFAULT 0,window_start_ms BIGINT NOT NULL,window_end_ms BIGINT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,metric,window_start_ms),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 CHECK(budget>=0),
 CHECK(consumed>=0),
 CHECK(reserved>=0),
 CHECK(consumed+reserved<=budget),
 CHECK(revision>=0),
 CHECK(window_end_ms>window_start_ms)
);

CREATE TABLE quota_ledger(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 operation_id TEXT NOT NULL,metric TEXT NOT NULL,window_start_ms BIGINT NOT NULL,amount BIGINT NOT NULL,state TEXT NOT NULL CHECK(state IN ('reserved','consumed','released')),run_id TEXT,created_at_ms BIGINT NOT NULL,updated_at_ms BIGINT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,operation_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,metric,window_start_ms) REFERENCES quota_accounts(organization_id,tenant_id,project_id,agent_uid,metric,window_start_ms) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id) REFERENCES runs(organization_id,tenant_id,project_id,agent_uid,id) DEFERRABLE INITIALLY DEFERRED,
 CHECK(amount>=0)
);

CREATE TABLE offline_migration_receipts(
 organization_id TEXT NOT NULL CHECK(octet_length(organization_id) BETWEEN 1 AND 128),tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 128),project_id TEXT NOT NULL CHECK(octet_length(project_id) BETWEEN 1 AND 128),agent_uid TEXT NOT NULL CHECK(octet_length(agent_uid) BETWEEN 1 AND 128),
 migration_id TEXT NOT NULL,source_digest TEXT NOT NULL,manifest_json TEXT NOT NULL,state TEXT NOT NULL,created_at_ms BIGINT NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,migration_id),
 FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid),
 CHECK(octet_length(source_digest)=64),
 CHECK(octet_length(manifest_json)<=1048576)
);

ALTER TABLE event_outbox DROP CONSTRAINT event_outbox_event_id_key;

ALTER TABLE event_outbox ADD UNIQUE(organization_id,tenant_id,project_id,agent_uid,event_id);

ALTER TABLE event_outbox ADD FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,run_id,event_seq) REFERENCES events(organization_id,tenant_id,project_id,agent_uid,run_id,seq) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE delivery_leases ADD FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid,session_key,lifetime) REFERENCES session_lifetime_owners(organization_id,tenant_id,project_id,agent_uid,session_id,lifetime) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE role_bindings ENABLE ROW LEVEL SECURITY;
ALTER TABLE role_bindings FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON role_bindings USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE identity_policies ENABLE ROW LEVEL SECURITY;
ALTER TABLE identity_policies FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON identity_policies USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE identity_policy_heads ENABLE ROW LEVEL SECURITY;
ALTER TABLE identity_policy_heads FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON identity_policy_heads USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON sessions USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE session_lifetime_owners ENABLE ROW LEVEL SECURITY;
ALTER TABLE session_lifetime_owners FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON session_lifetime_owners USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE session_heads ENABLE ROW LEVEL SECURITY;
ALTER TABLE session_heads FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON session_heads USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE snapshot_clock ENABLE ROW LEVEL SECURITY;
ALTER TABLE snapshot_clock FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON snapshot_clock USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE session_tombstones ENABLE ROW LEVEL SECURITY;
ALTER TABLE session_tombstones FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON session_tombstones USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE runtime_settings ENABLE ROW LEVEL SECURITY;
ALTER TABLE runtime_settings FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON runtime_settings USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE lifecycle_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE lifecycle_receipts FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON lifecycle_receipts USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE runs FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON runs USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE turns ENABLE ROW LEVEL SECURITY;
ALTER TABLE turns FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON turns USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE run_authorizations ENABLE ROW LEVEL SECURITY;
ALTER TABLE run_authorizations FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON run_authorizations USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE session_writer_leases ENABLE ROW LEVEL SECURITY;
ALTER TABLE session_writer_leases FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON session_writer_leases USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE events ENABLE ROW LEVEL SECURITY;
ALTER TABLE events FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON events USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE event_view_stamps ENABLE ROW LEVEL SECURITY;
ALTER TABLE event_view_stamps FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON event_view_stamps USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE transcript_batches ENABLE ROW LEVEL SECURITY;
ALTER TABLE transcript_batches FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON transcript_batches USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE interactions ENABLE ROW LEVEL SECURITY;
ALTER TABLE interactions FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON interactions USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE queued_messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE queued_messages FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON queued_messages USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE tool_executions ENABLE ROW LEVEL SECURITY;
ALTER TABLE tool_executions FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON tool_executions USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE run_routes ENABLE ROW LEVEL SECURITY;
ALTER TABLE run_routes FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON run_routes USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE provider_attempts ENABLE ROW LEVEL SECURITY;
ALTER TABLE provider_attempts FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON provider_attempts USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE delegations ENABLE ROW LEVEL SECURITY;
ALTER TABLE delegations FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON delegations USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE run_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE run_snapshots FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON run_snapshots USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE turn_outputs ENABLE ROW LEVEL SECURITY;
ALTER TABLE turn_outputs FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON turn_outputs USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE turn_plan_stages ENABLE ROW LEVEL SECURITY;
ALTER TABLE turn_plan_stages FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON turn_plan_stages USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE turn_commits ENABLE ROW LEVEL SECURITY;
ALTER TABLE turn_commits FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON turn_commits USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE tool_batches ENABLE ROW LEVEL SECURITY;
ALTER TABLE tool_batches FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON tool_batches USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE session_plans ENABLE ROW LEVEL SECURITY;
ALTER TABLE session_plans FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON session_plans USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE plan_legacy_evidence ENABLE ROW LEVEL SECURITY;
ALTER TABLE plan_legacy_evidence FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON plan_legacy_evidence USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE plan_versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE plan_versions FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON plan_versions USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE plan_decisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE plan_decisions FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON plan_decisions USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE legacy_plan_imports ENABLE ROW LEVEL SECURITY;
ALTER TABLE legacy_plan_imports FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON legacy_plan_imports USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE compact_operations ENABLE ROW LEVEL SECURITY;
ALTER TABLE compact_operations FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON compact_operations USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE compact_run_links ENABLE ROW LEVEL SECURITY;
ALTER TABLE compact_run_links FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON compact_run_links USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE context_heads ENABLE ROW LEVEL SECURITY;
ALTER TABLE context_heads FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON context_heads USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE context_ledgers ENABLE ROW LEVEL SECURITY;
ALTER TABLE context_ledgers FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON context_ledgers USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE maintenance_diagnostics ENABLE ROW LEVEL SECURITY;
ALTER TABLE maintenance_diagnostics FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON maintenance_diagnostics USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE resources ENABLE ROW LEVEL SECURITY;
ALTER TABLE resources FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON resources USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE resource_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE resource_logs FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON resource_logs USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE memories ENABLE ROW LEVEL SECURITY;
ALTER TABLE memories FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON memories USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE memory_legacy_imports ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory_legacy_imports FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON memory_legacy_imports USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE memory_ingests ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory_ingests FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON memory_ingests USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE memory_forget_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory_forget_receipts FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON memory_forget_receipts USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE memory_exposures ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory_exposures FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON memory_exposures USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE memory_assessments ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory_assessments FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON memory_assessments USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE memory_feedback_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory_feedback_receipts FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON memory_feedback_receipts USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE memory_ingest_retries ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory_ingest_retries FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON memory_ingest_retries USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE hook_outcomes ENABLE ROW LEVEL SECURITY;
ALTER TABLE hook_outcomes FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON hook_outcomes USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE hook_publications ENABLE ROW LEVEL SECURITY;
ALTER TABLE hook_publications FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON hook_publications USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE hook_continuations ENABLE ROW LEVEL SECURITY;
ALTER TABLE hook_continuations FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON hook_continuations USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE tool_discoveries ENABLE ROW LEVEL SECURITY;
ALTER TABLE tool_discoveries FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON tool_discoveries USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE provider_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE provider_requests FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON provider_requests USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE artifact_publications ENABLE ROW LEVEL SECURITY;
ALTER TABLE artifact_publications FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON artifact_publications USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE audit_heads ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_heads FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON audit_heads USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE audit_ledger ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_ledger FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON audit_ledger USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE quota_accounts ENABLE ROW LEVEL SECURITY;
ALTER TABLE quota_accounts FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON quota_accounts USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE quota_ledger ENABLE ROW LEVEL SECURITY;
ALTER TABLE quota_ledger FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON quota_ledger USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE offline_migration_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE offline_migration_receipts FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON offline_migration_receipts USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE event_outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE event_outbox FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON event_outbox USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE delivery_leases ENABLE ROW LEVEL SECURITY;
ALTER TABLE delivery_leases FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON delivery_leases USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE publication_inbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE publication_inbox FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON publication_inbox USING (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true)) WITH CHECK (organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

CREATE FUNCTION reject_canonical_rewrite() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'immutable canonical fact' USING ERRCODE='23514'; END $$;
CREATE TRIGGER permanent_lifetime_immutable BEFORE UPDATE OR DELETE ON session_lifetime_owners FOR EACH ROW EXECUTE FUNCTION reject_canonical_rewrite();
CREATE TRIGGER transcript_immutable BEFORE UPDATE OR DELETE ON transcript_batches FOR EACH ROW EXECUTE FUNCTION reject_canonical_rewrite();
CREATE TRIGGER authorization_immutable BEFORE UPDATE OR DELETE ON run_authorizations FOR EACH ROW EXECUTE FUNCTION reject_canonical_rewrite();
CREATE TRIGGER audit_immutable BEFORE UPDATE OR DELETE ON audit_ledger FOR EACH ROW EXECUTE FUNCTION reject_canonical_rewrite();
CREATE TRIGGER run_incarnation_immutable BEFORE UPDATE OF organization_id,tenant_id,project_id,agent_uid,id,session_id,lifetime,generation ON runs FOR EACH ROW EXECUTE FUNCTION reject_canonical_rewrite();
CREATE TRIGGER artifact_identity_immutable BEFORE UPDATE OF publication_id,organization_id,tenant_id,project_id,agent_uid,run_id,round,call_id,store_id,sha256,bytes,reservation_json,reservation_digest ON artifact_publications FOR EACH ROW EXECUTE FUNCTION reject_canonical_rewrite();
CREATE FUNCTION advance_scope_snapshot() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE fact RECORD; next_revision BIGINT;
BEGIN
 IF TG_OP='DELETE' THEN fact:=OLD; ELSE fact:=NEW; END IF;
 INSERT INTO snapshot_clock(organization_id,tenant_id,project_id,agent_uid,slot,revision) VALUES(fact.organization_id,fact.tenant_id,fact.project_id,fact.agent_uid,1,1)
 ON CONFLICT(organization_id,tenant_id,project_id,agent_uid,slot) DO UPDATE SET revision=snapshot_clock.revision+1
 RETURNING revision INTO next_revision;
 IF TG_TABLE_NAME='session_heads' AND TG_OP<>'DELETE' THEN NEW.metadata_revision:=next_revision; RETURN NEW; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
END $$;

CREATE TRIGGER snapshot_session_heads BEFORE INSERT OR UPDATE OR DELETE ON session_heads FOR EACH ROW EXECUTE FUNCTION advance_scope_snapshot();

CREATE TRIGGER snapshot_runs BEFORE INSERT OR UPDATE OR DELETE ON runs FOR EACH ROW EXECUTE FUNCTION advance_scope_snapshot();

CREATE TRIGGER snapshot_turns BEFORE INSERT OR UPDATE OR DELETE ON turns FOR EACH ROW EXECUTE FUNCTION advance_scope_snapshot();

CREATE TRIGGER snapshot_queued_messages BEFORE INSERT OR UPDATE OR DELETE ON queued_messages FOR EACH ROW EXECUTE FUNCTION advance_scope_snapshot();

CREATE TRIGGER snapshot_interactions BEFORE INSERT OR UPDATE OR DELETE ON interactions FOR EACH ROW EXECUTE FUNCTION advance_scope_snapshot();

CREATE TRIGGER snapshot_session_plans BEFORE INSERT OR UPDATE OR DELETE ON session_plans FOR EACH ROW EXECUTE FUNCTION advance_scope_snapshot();

CREATE TRIGGER snapshot_context_heads BEFORE INSERT OR UPDATE OR DELETE ON context_heads FOR EACH ROW EXECUTE FUNCTION advance_scope_snapshot();

CREATE TRIGGER snapshot_context_ledgers BEFORE INSERT OR UPDATE OR DELETE ON context_ledgers FOR EACH ROW EXECUTE FUNCTION advance_scope_snapshot();

CREATE TRIGGER snapshot_plan_versions BEFORE INSERT OR UPDATE OR DELETE ON plan_versions FOR EACH ROW EXECUTE FUNCTION advance_scope_snapshot();

CREATE TRIGGER snapshot_plan_decisions BEFORE INSERT OR UPDATE OR DELETE ON plan_decisions FOR EACH ROW EXECUTE FUNCTION advance_scope_snapshot();

CREATE INDEX queue_recovery ON runs(organization_id,tenant_id,project_id,agent_uid,created_at_ms,storage_row_id) WHERE status='queued';

CREATE INDEX transcript_source ON transcript_batches(organization_id,tenant_id,project_id,agent_uid,lifetime,start_seq);

CREATE INDEX writer_expiry ON session_writer_leases(organization_id,tenant_id,project_id,agent_uid,expires_at_ms);

CREATE INDEX hooks_pending ON hook_publications(organization_id,tenant_id,project_id,agent_uid,operation_id) WHERE NOT dispatched;

CREATE TRIGGER policy_version_immutable BEFORE UPDATE OR DELETE ON identity_policies FOR EACH ROW EXECUTE FUNCTION reject_canonical_rewrite();
CREATE TRIGGER provider_capture_immutable BEFORE UPDATE OR DELETE ON provider_requests FOR EACH ROW EXECUTE FUNCTION reject_canonical_rewrite();
CREATE TRIGGER plan_version_immutable BEFORE UPDATE OF organization_id,tenant_id,project_id,agent_uid,lifetime,revision,plan_id,content_digest ON plan_versions FOR EACH ROW EXECUTE FUNCTION reject_canonical_rewrite();

CREATE TABLE scope_run_counters(organization_id TEXT NOT NULL,tenant_id TEXT NOT NULL,project_id TEXT NOT NULL,agent_uid TEXT NOT NULL,next_id BIGINT NOT NULL CHECK(next_id>0),PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid),FOREIGN KEY(organization_id,tenant_id,project_id,agent_uid) REFERENCES agents(organization_id,tenant_id,project_id,uid));
ALTER TABLE scope_run_counters ENABLE ROW LEVEL SECURITY;
ALTER TABLE scope_run_counters FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON scope_run_counters USING(organization_id=current_setting('agent.organization_id',true) AND tenant_id=current_setting('agent.tenant_id',true) AND project_id=current_setting('agent.project_id',true) AND agent_uid=current_setting('agent.agent_uid',true));

ALTER TABLE runs ADD UNIQUE(organization_id,tenant_id,project_id,agent_uid,storage_row_id);

-- PG16 的 IS JSON 在存储边界拒绝非法正文，查询无需对非法 TEXT 强制 jsonb cast。
DO $$
DECLARE entry RECORD;
BEGIN
 FOR entry IN SELECT c.relname,a.attname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_attribute a ON a.attrelid=c.oid WHERE n.nspname=current_schema() AND c.relkind='r' AND a.attnum>0 AND NOT a.attisdropped AND a.atttypid='text'::regtype AND a.attname LIKE '%\_json' ESCAPE '\'
 LOOP EXECUTE format('ALTER TABLE %I ADD CHECK (%I IS JSON)',entry.relname,entry.attname); END LOOP;
END $$;

CREATE TRIGGER events_immutable BEFORE UPDATE OR DELETE ON events FOR EACH ROW EXECUTE FUNCTION reject_canonical_rewrite();
CREATE FUNCTION protect_published_stamp() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.published THEN RAISE EXCEPTION 'published stamp is immutable' USING ERRCODE='23514'; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 IF (OLD.organization_id,OLD.tenant_id,OLD.project_id,OLD.agent_uid,OLD.run_id,OLD.seq) IS DISTINCT FROM (NEW.organization_id,NEW.tenant_id,NEW.project_id,NEW.agent_uid,NEW.run_id,NEW.seq) THEN RAISE EXCEPTION 'stamp identity is immutable' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER published_stamp_immutable BEFORE UPDATE OR DELETE ON event_view_stamps FOR EACH ROW EXECUTE FUNCTION protect_published_stamp();
CREATE FUNCTION protect_monotonic_publication() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
 IF TG_TABLE_NAME='artifact_publications' THEN
  IF OLD.published AND NOT NEW.published THEN RAISE EXCEPTION 'publication cannot be revoked' USING ERRCODE='23514'; END IF;
 ELSIF TG_TABLE_NAME='hook_publications' THEN
  IF OLD.dispatched AND NOT NEW.dispatched THEN RAISE EXCEPTION 'dispatch cannot be revoked' USING ERRCODE='23514'; END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER artifact_published_monotonic BEFORE UPDATE ON artifact_publications FOR EACH ROW EXECUTE FUNCTION protect_monotonic_publication();
CREATE TRIGGER hook_dispatch_monotonic BEFORE UPDATE ON hook_publications FOR EACH ROW EXECUTE FUNCTION protect_monotonic_publication();
CREATE TRIGGER hook_publication_identity_immutable BEFORE UPDATE OF organization_id,tenant_id,project_id,agent_uid,operation_id,payload_json ON hook_publications FOR EACH ROW EXECUTE FUNCTION reject_canonical_rewrite();
CREATE TRIGGER discovery_immutable BEFORE UPDATE OR DELETE ON tool_discoveries FOR EACH ROW EXECUTE FUNCTION reject_canonical_rewrite();
