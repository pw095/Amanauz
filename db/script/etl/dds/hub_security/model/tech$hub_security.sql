DROP TABLE IF EXISTS tech$hub_security;
CREATE TABLE tech$hub_security
  (
    tech$load_id       INTEGER NOT NULL,
    tech$hash_key      TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    tech$load_dt       TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    security_id        TEXT    NOT NULL,
    PRIMARY KEY(tech$hash_key),
    UNIQUE(security_id, tech$record_source)
  )
WITHOUT ROWID;
