CREATE TABLE receipts(
 admin_id INTEGER PRIMARY KEY,
 row_id INTEGER NOT NULL,
 version INTEGER NOT NULL DEFAULT 1,
 signature TEXT NOT NULL,
 emoji TEXT,
 phase TEXT NOT NULL DEFAULT 'pending',
 shown_at INTEGER,
 clear_after INTEGER,
 cleared_at INTEGER,
 last_error TEXT,
 epoch TEXT NOT NULL,
 updated_at INTEGER NOT NULL
);
CREATE TABLE receipt_events(
 admin_id INTEGER NOT NULL,
 version INTEGER NOT NULL,
 action TEXT NOT NULL,
 attempt INTEGER NOT NULL,
 status TEXT NOT NULL,
 confirmed_at INTEGER,
 created_at INTEGER NOT NULL,
 PRIMARY KEY(admin_id,version,action,attempt)
);
