-- Additive broadcast outbox. Existing users, bindings and message history remain.
CREATE TABLE broadcasts(
 id TEXT PRIMARY KEY,instance_id TEXT NOT NULL,bot_id TEXT NOT NULL,actor_id TEXT NOT NULL,epoch TEXT NOT NULL,
 text TEXT NOT NULL DEFAULT '',photo TEXT,state TEXT NOT NULL DEFAULT 'draft' CHECK(state IN('draft','confirmed','sending','done','canceled','interrupted')),
 total INTEGER NOT NULL DEFAULT 0,snapshot_ready INTEGER NOT NULL DEFAULT 0,
 created_at INTEGER NOT NULL,started_at INTEGER,completed_at INTEGER,next_at INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX broadcasts_actor ON broadcasts(instance_id,actor_id,created_at);
CREATE TABLE broadcast_recipients(
 broadcast_id TEXT NOT NULL,user_id TEXT NOT NULL,
 state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN('pending','success','failed','unknown','skipped')),
 part_index INTEGER NOT NULL DEFAULT 0,success_parts INTEGER NOT NULL DEFAULT 0,
 reason TEXT,code INTEGER,updated_at INTEGER NOT NULL,
 PRIMARY KEY(broadcast_id,user_id),FOREIGN KEY(broadcast_id) REFERENCES broadcasts(id)
);
CREATE INDEX broadcast_pending ON broadcast_recipients(broadcast_id,state,user_id);
