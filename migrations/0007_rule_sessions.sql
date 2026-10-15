-- Management drafts are separate from user conversations and survive retries.
-- A recovery epoch change expires their authority without deleting old rules.
CREATE TABLE rule_sessions(
 instance_id TEXT NOT NULL,
 actor_id TEXT NOT NULL,
 epoch TEXT NOT NULL,
 session_id TEXT NOT NULL,
 stage TEXT NOT NULL,
 revision INTEGER NOT NULL DEFAULT 1,
 payload TEXT NOT NULL DEFAULT '{}',
 transition_key TEXT NOT NULL,
 expires_at INTEGER NOT NULL,
 PRIMARY KEY(instance_id,actor_id)
);
