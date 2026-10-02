BEGIN IMMEDIATE;
ALTER TABLE tool_batches ADD COLUMN descriptor_json TEXT NOT NULL DEFAULT '[]';
CREATE TABLE resources(id INTEGER PRIMARY KEY,run_id TEXT NOT NULL REFERENCES runs(id),lifetime TEXT NOT NULL,state TEXT NOT NULL,owner_json TEXT NOT NULL,cwd TEXT NOT NULL,requested TEXT NOT NULL,effective TEXT NOT NULL,process_identity TEXT,log_cursor INTEGER NOT NULL DEFAULT 0,terminal_reason TEXT,updated_at_ms INTEGER NOT NULL);
CREATE INDEX resources_owner ON resources(lifetime,state,id);
CREATE TABLE resource_logs(resource_id INTEGER NOT NULL REFERENCES resources(id),cursor INTEGER NOT NULL,content TEXT NOT NULL,PRIMARY KEY(resource_id,cursor));
INSERT INTO schema_migrations VALUES(10,CAST(strftime('%s','now') AS INTEGER)*1000);
COMMIT;
