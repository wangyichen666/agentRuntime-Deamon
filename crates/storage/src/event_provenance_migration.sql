BEGIN IMMEDIATE;
-- 既有stamp或outbox是永久native证据；不使用事件时间推断来源。
ALTER TABLE events ADD COLUMN view_provenance TEXT NOT NULL DEFAULT 'native' CHECK(view_provenance IN ('native','legacy'));
UPDATE events SET view_provenance='legacy' WHERE NOT EXISTS(SELECT 1 FROM event_view_stamps s WHERE s.run_id=events.run_id AND s.seq=events.seq) AND NOT EXISTS(SELECT 1 FROM event_outbox o WHERE o.run_id=events.run_id AND o.event_seq=events.seq);
CREATE TRIGGER native_event_provenance_insert BEFORE INSERT ON events WHEN NEW.view_provenance<>'native'
BEGIN SELECT RAISE(ABORT,'new event must be native'); END;
CREATE TRIGGER immutable_event_provenance_update BEFORE UPDATE OF view_provenance ON events WHEN NEW.view_provenance<>OLD.view_provenance
BEGIN SELECT RAISE(ABORT,'immutable event provenance'); END;
CREATE TRIGGER immutable_event_stamp_delete BEFORE DELETE ON event_view_stamps WHEN OLD.published=1
BEGIN SELECT RAISE(ABORT,'immutable published event stamp'); END;
INSERT INTO schema_migrations VALUES(26,CAST(strftime('%s','now') AS INTEGER)*1000);
COMMIT;
