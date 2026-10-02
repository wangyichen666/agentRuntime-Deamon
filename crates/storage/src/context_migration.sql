BEGIN IMMEDIATE;
CREATE TABLE compact_operations(operation TEXT PRIMARY KEY,run_id TEXT NOT NULL REFERENCES runs(id),lifetime TEXT NOT NULL,source_json TEXT NOT NULL,state TEXT NOT NULL,reason TEXT);
CREATE TABLE context_heads(lifetime TEXT PRIMARY KEY,source_end INTEGER NOT NULL,generation INTEGER NOT NULL,messages_json TEXT NOT NULL);
CREATE TABLE context_ledgers(run_id TEXT NOT NULL REFERENCES runs(id),round INTEGER NOT NULL,envelope_json TEXT NOT NULL,PRIMARY KEY(run_id,round));
INSERT INTO schema_migrations VALUES(8, CAST(strftime('%s','now') AS INTEGER)*1000);
COMMIT;
