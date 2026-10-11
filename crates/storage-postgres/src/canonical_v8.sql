ALTER TABLE access_leases ADD COLUMN endpoint TEXT NOT NULL DEFAULT 'legacy' CHECK(octet_length(endpoint) BETWEEN 1 AND 128);
