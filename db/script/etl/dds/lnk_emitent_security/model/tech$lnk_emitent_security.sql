DROP TABLE IF EXISTS tech$lnk_emitent_security;
CREATE TABLE tech$lnk_emitent_security
  (
    tech$load_id       INTEGER NOT NULL,
    tech$hash_key      TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    tech$load_dt       TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    emitent_hash_key   TEXT    NOT NULL,
    security_hash_key  TEXT    NOT NULL,
    PRIMARY KEY(tech$hash_key),
    UNIQUE(emitent_hash_key, security_hash_key)
  )
WITHOUT ROWID;
