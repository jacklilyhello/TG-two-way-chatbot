CREATE TABLE delivery_attempts(
 message_id INTEGER NOT NULL,
 attempt INTEGER NOT NULL,
 op_key TEXT NOT NULL,
 prior_op_key TEXT NOT NULL,
 action_token TEXT NOT NULL UNIQUE,
 actor_id TEXT NOT NULL,
 user_id TEXT NOT NULL,
 thread_id INTEGER NOT NULL,
 binding_version INTEGER NOT NULL,
 epoch TEXT NOT NULL,
 state TEXT NOT NULL DEFAULT 'reserved',
 created_at INTEGER NOT NULL,
 PRIMARY KEY(message_id,attempt),
 UNIQUE(message_id,prior_op_key),
 UNIQUE(epoch,op_key)
);
