CREATE TABLE block_rules(
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 kind TEXT NOT NULL CHECK(kind IN ('keyword','regex')),
 pattern TEXT NOT NULL CHECK(length(pattern) BETWEEN 1 AND 200),
 enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
 deleted INTEGER NOT NULL DEFAULT 0 CHECK(deleted IN (0,1)),
 version INTEGER NOT NULL DEFAULT 1,
 state_count INTEGER NOT NULL DEFAULT 0,
 create_key TEXT NOT NULL UNIQUE,
 created_by TEXT NOT NULL,
 created_at INTEGER NOT NULL,
 updated_at INTEGER NOT NULL
);
CREATE INDEX block_rules_active ON block_rules(deleted,enabled,id);
CREATE TABLE block_rule_mutations(
 op_key TEXT PRIMARY KEY,
 rule_id INTEGER,
 result TEXT NOT NULL,
 created_at INTEGER NOT NULL
);
