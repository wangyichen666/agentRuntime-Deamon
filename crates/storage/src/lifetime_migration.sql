BEGIN IMMEDIATE;
-- lifetime 是原 transcript/plan 的键，必须永久绑定原 session，不能只依赖当前 head。
CREATE TABLE session_lifetime_owners(
  lifetime TEXT PRIMARY KEY NOT NULL CHECK(length(lifetime)>0),
  session_id TEXT REFERENCES sessions(id),
  provenance TEXT NOT NULL CHECK(provenance IN ('canonical','legacy_orphan')),
  CHECK((session_id IS NOT NULL AND provenance='canonical') OR (session_id IS NULL AND provenance='legacy_orphan')),
  UNIQUE(session_id,lifetime)
);
INSERT INTO session_lifetime_owners
  SELECT lifetime,session_id,'canonical' FROM (
    SELECT lifetime,session_id FROM session_heads
    UNION SELECT lifetime,session_id FROM runs
    UNION SELECT json_extract(result_json,'$.lifetime'),json_extract(result_json,'$.key') FROM lifecycle_receipts
  );
-- 旧无 run transcript 被 clear 后无法证明原 owner；保留内容与隔离证据，绝不猜测归属。
INSERT INTO session_lifetime_owners
  SELECT lifetime,NULL,'legacy_orphan' FROM (
    SELECT lifetime FROM transcript_batches UNION SELECT lifetime FROM session_plans
    UNION SELECT lifetime FROM plan_legacy_evidence
  ) WHERE lifetime NOT IN (SELECT lifetime FROM session_lifetime_owners);
CREATE TRIGGER immutable_lifetime_owner_update BEFORE UPDATE ON session_lifetime_owners
BEGIN SELECT RAISE(ABORT,'immutable lifetime owner'); END;
CREATE TRIGGER immutable_lifetime_owner_delete BEFORE DELETE ON session_lifetime_owners
BEGIN SELECT RAISE(ABORT,'immutable lifetime owner'); END;
CREATE TRIGGER bind_lifetime_insert BEFORE INSERT ON session_heads BEGIN
  SELECT CASE WHEN EXISTS(SELECT 1 FROM session_lifetime_owners WHERE lifetime=NEW.lifetime AND (session_id IS NULL OR session_id<>NEW.session_id)) THEN RAISE(ABORT,'lifetime owner conflict') END;
  INSERT OR IGNORE INTO session_lifetime_owners VALUES(NEW.lifetime,NEW.session_id,'canonical');
END;
CREATE TRIGGER bind_lifetime_update BEFORE UPDATE OF lifetime ON session_heads BEGIN
  SELECT CASE WHEN EXISTS(SELECT 1 FROM session_lifetime_owners WHERE lifetime=NEW.lifetime AND (session_id IS NULL OR session_id<>NEW.session_id)) THEN RAISE(ABORT,'lifetime owner conflict') END;
  INSERT OR IGNORE INTO session_lifetime_owners VALUES(NEW.lifetime,NEW.session_id,'canonical');
END;
CREATE TRIGGER immutable_run_incarnation BEFORE UPDATE OF session_id,lifetime ON runs WHEN OLD.lifetime<>'' AND (NEW.session_id<>OLD.session_id OR NEW.lifetime<>OLD.lifetime)
BEGIN SELECT RAISE(ABORT,'immutable run incarnation'); END;
INSERT INTO schema_migrations VALUES(25,CAST(strftime('%s','now') AS INTEGER)*1000);
COMMIT;
