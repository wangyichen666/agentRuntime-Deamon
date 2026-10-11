BEGIN IMMEDIATE;
CREATE TABLE artifact_publications (
 publication_id TEXT PRIMARY KEY,
 organization_id TEXT NOT NULL, tenant_id TEXT NOT NULL, project_id TEXT NOT NULL, agent_uid TEXT NOT NULL,
 run_id TEXT NOT NULL, round INTEGER NOT NULL, call_id TEXT NOT NULL,
 store_id TEXT NOT NULL, sha256 TEXT NOT NULL, bytes INTEGER NOT NULL CHECK(bytes>=0 AND bytes<=67108864),
 reservation_json TEXT NOT NULL, reservation_digest TEXT NOT NULL,
 published INTEGER NOT NULL DEFAULT 0 CHECK(published IN(0,1)),
 UNIQUE(run_id,round,call_id),
 FOREIGN KEY(run_id,round,call_id) REFERENCES tool_executions(run_id,round,call_id)
);
CREATE INDEX artifact_store_identity ON artifact_publications(store_id,sha256);
CREATE TRIGGER artifact_publication_immutable BEFORE UPDATE OF publication_id,organization_id,tenant_id,project_id,agent_uid,run_id,round,call_id,store_id,sha256,bytes,reservation_json,reservation_digest ON artifact_publications BEGIN SELECT RAISE(ABORT,'immutable artifact publication'); END;
CREATE TRIGGER artifact_publication_monotonic BEFORE UPDATE OF published ON artifact_publications WHEN OLD.published=1 AND NEW.published!=1 BEGIN SELECT RAISE(ABORT,'retired artifact publication'); END;
INSERT INTO schema_migrations VALUES(23,CAST(strftime('%s','now') AS INTEGER)*1000);
COMMIT;
