-- Additive, private timing metadata. No request bodies, identities or secrets.
ALTER TABLE jobs ADD COLUMN first_started_at INTEGER;
ALTER TABLE jobs ADD COLUMN completed_at INTEGER;
ALTER TABLE jobs ADD COLUMN execution_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE operations ADD COLUMN call_started_at INTEGER;
ALTER TABLE operations ADD COLUMN call_finished_at INTEGER;
ALTER TABLE operations ADD COLUMN call_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE operations ADD COLUMN prepare_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE operations ADD COLUMN transport_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE operations ADD COLUMN api_ms INTEGER;
ALTER TABLE operations ADD COLUMN bot_wait_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE operations ADD COLUMN rate_wait_ms INTEGER NOT NULL DEFAULT 0;
