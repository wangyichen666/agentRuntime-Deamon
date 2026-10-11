BEGIN IMMEDIATE;
CREATE TABLE oauth_logins(organization_id TEXT NOT NULL,tenant_id TEXT NOT NULL,project_id TEXT NOT NULL,agent_uid TEXT NOT NULL,state_digest TEXT NOT NULL CHECK(length(state_digest)=64),nonce_digest TEXT NOT NULL CHECK(length(nonce_digest)=64),verifier_digest TEXT NOT NULL CHECK(length(verifier_digest)=64),actor_id TEXT NOT NULL,actor_epoch INTEGER NOT NULL CHECK(actor_epoch>=0),expires_at_ms INTEGER NOT NULL,consumed INTEGER NOT NULL CHECK(consumed IN (0,1)),PRIMARY KEY(organization_id,tenant_id,project_id,agent_uid,state_digest),FOREIGN KEY(tenant_id,actor_id) REFERENCES principals(tenant_id,subject_id));
CREATE INDEX oauth_expiry ON oauth_logins(organization_id,tenant_id,expires_at_ms);
INSERT INTO schema_migrations VALUES(32,CAST(strftime('%s','now') AS INTEGER)*1000);
COMMIT;
