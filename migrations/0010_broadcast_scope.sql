-- Existing broadcasts retain their original all-eligible-user range and outbox.
ALTER TABLE broadcasts ADD COLUMN scope_json TEXT NOT NULL DEFAULT '{"mode":"all"}';
