BEGIN IMMEDIATE;
ALTER TABLE access_leases ADD COLUMN endpoint TEXT NOT NULL DEFAULT 'legacy' CHECK(length(endpoint) BETWEEN 1 AND 128);
INSERT INTO schema_migrations VALUES(34,CAST(strftime('%s','now') AS INTEGER)*1000);
COMMIT;
