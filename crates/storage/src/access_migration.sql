BEGIN IMMEDIATE;
CREATE TABLE session_directory(organization_id TEXT NOT NULL,tenant_id TEXT NOT NULL,project_id TEXT NOT NULL,agent_uid TEXT NOT NULL,external_id TEXT NOT NULL UNIQUE,session_id TEXT NOT NULL,lifetime TEXT NOT NULL,runtime_endpoint TEXT NOT NULL,created_by TEXT NOT NULL,operation_id TEXT NOT NULL,PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,created_by,operation_id),FOREIGN KEY(lifetime) REFERENCES session_lifetime_owners(lifetime),FOREIGN KEY(session_id) REFERENCES sessions(id),FOREIGN KEY(tenant_id,created_by) REFERENCES principals(tenant_id,subject_id));
CREATE TRIGGER immutable_session_directory_update BEFORE UPDATE ON session_directory BEGIN SELECT RAISE(ABORT,'immutable directory'); END;
CREATE TRIGGER immutable_session_directory_delete BEFORE DELETE ON session_directory BEGIN SELECT RAISE(ABORT,'immutable directory'); END;
CREATE TABLE access_counters(organization_id TEXT NOT NULL,tenant_id TEXT NOT NULL,bucket TEXT NOT NULL,window_start_ms INTEGER NOT NULL,count INTEGER NOT NULL CHECK(count>=0),PRIMARY KEY(organization_id,tenant_id,bucket));
CREATE TABLE access_leases(organization_id TEXT NOT NULL,tenant_id TEXT NOT NULL,project_id TEXT NOT NULL,agent_uid TEXT NOT NULL,operation_id TEXT NOT NULL,subject_id TEXT NOT NULL,revocation_epoch INTEGER NOT NULL,class TEXT NOT NULL CHECK(class IN ('request','web_socket')),expires_at_ms INTEGER NOT NULL,PRIMARY KEY(organization_id,tenant_id,operation_id));
CREATE INDEX access_active ON access_leases(organization_id,tenant_id,expires_at_ms);
INSERT INTO schema_migrations VALUES(30,CAST(strftime('%s','now') AS INTEGER)*1000);
COMMIT;
