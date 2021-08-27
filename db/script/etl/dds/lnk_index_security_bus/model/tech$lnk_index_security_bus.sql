DROP TABLE IF EXISTS tech$lnk_index_security_bus;
CREATE TABLE tech$lnk_index_security_bus
  (
    tech$load_id       INTEGER NOT NULL,
    tech$hash_key      TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    tech$load_dt       TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    index_hash_key     TEXT    NOT NULL,
    security_hash_key  TEXT    NOT NULL,
    PRIMARY KEY(tech$hash_key),
    UNIQUE(index_hash_key, security_hash_key)
  )
WITHOUT ROWID;
