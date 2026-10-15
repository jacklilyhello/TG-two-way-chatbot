-- Retain every accepted moderation action so a replay cannot apply it again
-- after a later unblock. Only actual transitions to blocked create notices.
ALTER TABLE users ADD COLUMN moderation_version INTEGER NOT NULL DEFAULT 0;
CREATE TABLE ban_events(
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 event_key TEXT NOT NULL UNIQUE,
 user_id TEXT NOT NULL,
 blocked INTEGER NOT NULL CHECK(blocked IN(0,1)),
 changed INTEGER NOT NULL CHECK(changed IN(0,1)),
 source TEXT NOT NULL CHECK(source IN('manual','rule')),
 actor_id TEXT,
 rule_id INTEGER,
 rule_kind TEXT CHECK(rule_kind IN('keyword','regex')),
 rule_pattern TEXT,
 rule_version INTEGER,
 committed INTEGER NOT NULL DEFAULT 0 CHECK(committed IN(0,1)),
 notice_status TEXT NOT NULL DEFAULT 'none' CHECK(notice_status IN('none','pending','success','failed','unknown')),
 notice_message_id INTEGER,
 notice_thread_id INTEGER,
 notice_binding_version INTEGER,
 created_at INTEGER NOT NULL
);
CREATE INDEX ban_events_user ON ban_events(user_id,id);
CREATE INDEX ban_events_notice ON ban_events(notice_status,id);
