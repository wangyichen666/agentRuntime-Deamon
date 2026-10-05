BEGIN IMMEDIATE;
CREATE TABLE snapshot_clock(id INTEGER PRIMARY KEY CHECK(id=1), revision INTEGER NOT NULL CHECK(revision>=0));
INSERT INTO snapshot_clock VALUES(1,1);
ALTER TABLE session_heads ADD COLUMN metadata_revision INTEGER NOT NULL DEFAULT 1;
CREATE TRIGGER snapshot_session_heads_INSERT AFTER INSERT ON session_heads BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_session_heads_UPDATE AFTER UPDATE ON session_heads BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_session_heads_DELETE AFTER DELETE ON session_heads BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_runs_INSERT AFTER INSERT ON runs BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_runs_UPDATE AFTER UPDATE ON runs BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_runs_DELETE AFTER DELETE ON runs BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_turns_INSERT AFTER INSERT ON turns BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_turns_UPDATE AFTER UPDATE ON turns BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_turns_DELETE AFTER DELETE ON turns BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_queued_messages_INSERT AFTER INSERT ON queued_messages BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_queued_messages_UPDATE AFTER UPDATE ON queued_messages BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_queued_messages_DELETE AFTER DELETE ON queued_messages BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_interactions_INSERT AFTER INSERT ON interactions BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_interactions_UPDATE AFTER UPDATE ON interactions BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_interactions_DELETE AFTER DELETE ON interactions BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_session_plans_INSERT AFTER INSERT ON session_plans BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_session_plans_UPDATE AFTER UPDATE ON session_plans BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_session_plans_DELETE AFTER DELETE ON session_plans BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_context_heads_INSERT AFTER INSERT ON context_heads BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_context_heads_UPDATE AFTER UPDATE ON context_heads BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_context_heads_DELETE AFTER DELETE ON context_heads BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_context_ledgers_INSERT AFTER INSERT ON context_ledgers BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_context_ledgers_UPDATE AFTER UPDATE ON context_ledgers BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER snapshot_context_ledgers_DELETE AFTER DELETE ON context_ledgers BEGIN
  SELECT CASE WHEN (SELECT revision FROM snapshot_clock WHERE id=1)>=9223372036854775807 THEN RAISE(ABORT,'snapshot revision exhausted') END;
  UPDATE snapshot_clock SET revision=revision+1 WHERE id=1;
END;
CREATE TRIGGER session_metadata_revision AFTER UPDATE OF lifetime,deleted,legacy_imported,updated_at_ms,projection_generation ON session_heads BEGIN
  UPDATE session_heads SET metadata_revision=(SELECT revision FROM snapshot_clock WHERE id=1) WHERE session_id=NEW.session_id;
END;
INSERT INTO schema_migrations VALUES(14,CAST(strftime('%s','now') AS INTEGER)*1000);
COMMIT;
