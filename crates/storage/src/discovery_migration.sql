BEGIN IMMEDIATE;
CREATE TABLE tool_discoveries(operation_id TEXT PRIMARY KEY,run_id TEXT NOT NULL REFERENCES runs(id),receipt_json TEXT NOT NULL,receipt_digest TEXT NOT NULL);
CREATE INDEX tool_discoveries_run ON tool_discoveries(run_id);
CREATE TRIGGER snapshot_discovery_insert AFTER INSERT ON tool_discoveries BEGIN
 SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
 UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
INSERT INTO schema_migrations VALUES(18,CAST(strftime('%s','now') AS INTEGER)*1000);
COMMIT;
