BEGIN IMMEDIATE;
CREATE TABLE provider_requests(run_id TEXT NOT NULL REFERENCES runs(id),capture_id TEXT NOT NULL,round INTEGER NOT NULL CHECK(round>0),candidate_index INTEGER NOT NULL CHECK(candidate_index>=0),capture_json TEXT NOT NULL,capture_digest TEXT NOT NULL,PRIMARY KEY(run_id,capture_id));
CREATE INDEX provider_requests_by_run ON provider_requests(run_id,round,candidate_index);
INSERT INTO schema_migrations(version,applied_at_ms) VALUES(20,CAST(strftime('%s','now') AS INTEGER)*1000);
COMMIT;
