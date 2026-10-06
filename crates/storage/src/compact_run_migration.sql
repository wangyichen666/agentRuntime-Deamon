BEGIN IMMEDIATE;
CREATE TABLE compact_run_links(run_id TEXT PRIMARY KEY REFERENCES runs(id),operation_id TEXT NOT NULL UNIQUE,request_json TEXT NOT NULL,source_json TEXT NOT NULL);
INSERT INTO schema_migrations VALUES(17,CAST(strftime('%s','now') AS INTEGER)*1000);
COMMIT;
