BEGIN IMMEDIATE;
ALTER TABLE compact_operations ADD COLUMN result_generation INTEGER;
ALTER TABLE compact_operations ADD COLUMN candidate_digest TEXT;
CREATE TABLE maintenance_diagnostics(run_id TEXT NOT NULL REFERENCES runs(id),kind TEXT NOT NULL,diagnostic TEXT NOT NULL,PRIMARY KEY(run_id,kind));
INSERT INTO schema_migrations VALUES(11,CAST(strftime('%s','now') AS INTEGER)*1000);
COMMIT;
