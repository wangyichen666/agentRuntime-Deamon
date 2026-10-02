BEGIN IMMEDIATE;
CREATE TABLE memories(id TEXT PRIMARY KEY,lifetime TEXT,project TEXT,scope TEXT NOT NULL,source_run TEXT,revision INTEGER NOT NULL,data_json TEXT NOT NULL);
CREATE INDEX memories_visibility ON memories(scope,lifetime,project);
CREATE TABLE memory_legacy_imports(source TEXT PRIMARY KEY,digest TEXT NOT NULL);
CREATE TABLE memory_ingests(run_id TEXT PRIMARY KEY REFERENCES runs(id),lifetime TEXT NOT NULL,status TEXT NOT NULL,diagnostic TEXT);
INSERT INTO schema_migrations VALUES(9,CAST(strftime('%s','now') AS INTEGER)*1000);
COMMIT;
