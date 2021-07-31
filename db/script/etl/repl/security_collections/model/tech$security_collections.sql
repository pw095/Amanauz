DROP TABLE IF EXISTS tech$security_collections;
CREATE TABLE tech$security_collections
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    id                 INTEGER NOT NULL,
    name               TEXT,
    title              TEXT,
    security_group_id  INTEGER,
    PRIMARY KEY(id, tech$effective_dt)
  )
WITHOUT ROWID;
