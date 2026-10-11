BEGIN IMMEDIATE;
CREATE UNIQUE INDEX session_scope_writer_key ON session_scopes(tenant_id,project_id,agent_uid,session_id);
CREATE UNIQUE INDEX tenant_writer_domain ON tenants(organization_id,id);
CREATE TABLE session_writer_leases (
 organization_id TEXT NOT NULL,tenant_id TEXT NOT NULL,project_id TEXT NOT NULL,agent_uid TEXT NOT NULL,
 session_key TEXT NOT NULL,lifetime TEXT NOT NULL,owner_id TEXT NOT NULL,
 epoch INTEGER NOT NULL CHECK(epoch>0),expires_at_ms INTEGER NOT NULL,
 PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,session_key,lifetime),
 FOREIGN KEY(organization_id,tenant_id) REFERENCES tenants(organization_id,id),
 FOREIGN KEY(tenant_id,project_id,agent_uid,session_key) REFERENCES session_scopes(tenant_id,project_id,agent_uid,session_id)
);
CREATE INDEX session_writer_expiry ON session_writer_leases(organization_id,tenant_id,project_id,agent_uid,expires_at_ms);
INSERT INTO schema_migrations VALUES(24,CAST(strftime('%s','now') AS INTEGER)*1000);
COMMIT;
