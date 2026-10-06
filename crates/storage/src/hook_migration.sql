BEGIN IMMEDIATE;
CREATE TABLE hook_outcomes(operation_id TEXT PRIMARY KEY,session_id TEXT NOT NULL,lifetime TEXT NOT NULL,outcome_json TEXT NOT NULL);
CREATE INDEX hook_outcomes_session ON hook_outcomes(session_id,lifetime);
CREATE TABLE hook_publications(operation_id TEXT PRIMARY KEY,payload_json TEXT NOT NULL,dispatched INTEGER NOT NULL CHECK(dispatched IN (0,1)));
CREATE TABLE hook_continuations(parent_run_id TEXT PRIMARY KEY,child_run_id TEXT NOT NULL UNIQUE,operation_id TEXT NOT NULL UNIQUE);
CREATE TRIGGER snapshot_hook_insert AFTER INSERT ON hook_outcomes BEGIN
 SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
 UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_hook_update AFTER UPDATE ON hook_outcomes BEGIN
 SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
 UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
INSERT INTO schema_migrations VALUES(16,CAST(strftime('%s','now') AS INTEGER)*1000);
COMMIT;
