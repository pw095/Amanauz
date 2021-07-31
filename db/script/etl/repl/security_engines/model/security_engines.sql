DROP TABLE IF EXISTS security_engines;
CREATE TABLE security_engines
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    id                 INTEGER,
    name               TEXT    NOT NULL,
    title              TEXT,
    PRIMARY KEY(name, tech$effective_dt)
  )
WITHOUT ROWID;
