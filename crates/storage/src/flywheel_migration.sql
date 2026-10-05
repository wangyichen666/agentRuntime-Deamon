BEGIN IMMEDIATE;
CREATE TABLE memory_exposures(
 run_id TEXT NOT NULL REFERENCES runs(id),
 memory_id TEXT NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
 lifetime TEXT NOT NULL, content_digest TEXT NOT NULL, revision INTEGER NOT NULL,
 channel TEXT NOT NULL, policy TEXT NOT NULL, created_at_ms INTEGER NOT NULL,
 PRIMARY KEY(run_id,memory_id)
);
CREATE INDEX memory_exposures_lifetime ON memory_exposures(lifetime,created_at_ms);
CREATE TABLE memory_assessments(
 lifetime TEXT NOT NULL, memory_id TEXT NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
 content_digest TEXT NOT NULL, feedback TEXT NOT NULL, updated_at_ms INTEGER NOT NULL,
 PRIMARY KEY(lifetime,memory_id)
);
CREATE TABLE memory_feedback_receipts(
 run_id TEXT NOT NULL REFERENCES runs(id), operation_id TEXT NOT NULL,
 memory_id TEXT NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
 lifetime TEXT NOT NULL, command_json TEXT NOT NULL,
 PRIMARY KEY(run_id,operation_id)
);
CREATE TABLE memory_ingest_retries(run_id TEXT PRIMARY KEY REFERENCES runs(id), attempts INTEGER NOT NULL, retry_at_ms INTEGER NOT NULL);
INSERT INTO schema_migrations VALUES(13,CAST(strftime('%s','now') AS INTEGER)*1000);
COMMIT;
