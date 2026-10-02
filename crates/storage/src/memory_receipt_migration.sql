BEGIN IMMEDIATE;
CREATE TABLE memory_forget_receipts(run_id TEXT NOT NULL REFERENCES runs(id),memory_id TEXT NOT NULL,revision INTEGER NOT NULL,result INTEGER NOT NULL,PRIMARY KEY(run_id,memory_id,revision));
INSERT INTO schema_migrations VALUES(12,CAST(strftime('%s','now') AS INTEGER)*1000);
COMMIT;
