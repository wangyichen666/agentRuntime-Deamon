BEGIN IMMEDIATE;
CREATE TABLE event_view_stamps (
 run_id TEXT NOT NULL,
 seq INTEGER NOT NULL,
 stamp_json TEXT NOT NULL,
 stamp_digest TEXT NOT NULL,
 published INTEGER NOT NULL DEFAULT 0 CHECK(published IN (0,1)),
 PRIMARY KEY(run_id,seq),
 FOREIGN KEY(run_id,seq) REFERENCES events(run_id,seq)
);
CREATE INDEX event_view_pending ON event_view_stamps(published) WHERE published=0;
INSERT INTO schema_migrations VALUES(19,CAST(strftime('%s','now') AS INTEGER)*1000);
COMMIT;
