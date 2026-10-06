BEGIN IMMEDIATE;
CREATE TABLE plan_legacy_evidence(
 lifetime TEXT PRIMARY KEY, revision INTEGER NOT NULL, data_json TEXT NOT NULL
);
INSERT INTO plan_legacy_evidence SELECT lifetime,revision,data_json FROM session_plans
 WHERE CASE WHEN json_valid(data_json) THEN json_extract(data_json,'$.plan_id') IS NULL ELSE 1 END;
CREATE TABLE plan_versions(
 lifetime TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision>0),
 plan_id TEXT NOT NULL, content_digest TEXT NOT NULL,
 document_json TEXT NOT NULL, markdown TEXT NOT NULL,
 PRIMARY KEY(lifetime,revision)
);
CREATE TABLE plan_decisions(
 operation_id TEXT PRIMARY KEY, session_id TEXT NOT NULL, lifetime TEXT NOT NULL,
 identity_json TEXT NOT NULL, decision TEXT NOT NULL CHECK(decision IN ('execute','discard')),
 input TEXT NOT NULL, run_id TEXT UNIQUE, receipt_json TEXT NOT NULL
);
CREATE TRIGGER snapshot_plan_versions_insert AFTER INSERT ON plan_versions BEGIN
 SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
 UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_plan_versions_update AFTER UPDATE ON plan_versions BEGIN
 SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
 UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_plan_decisions_insert AFTER INSERT ON plan_decisions BEGIN
 SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
 UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_plan_decisions_update AFTER UPDATE ON plan_decisions BEGIN
 SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
 UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
INSERT INTO schema_migrations VALUES(15,CAST(strftime('%s','now') AS INTEGER)*1000);
COMMIT;
