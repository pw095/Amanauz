DROP TABLE IF EXISTS tech$hub_emitent;
CREATE TABLE tech$hub_emitent
  (
    tech$load_id       INTEGER NOT NULL,
    tech$hash_key      TEXT    NOT NULL,
    tech$load_dt       TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    code               TEXT    NOT NULL,
    PRIMARY KEY(tech$hash_key),
    UNIQUE(code)
  )
WITHOUT ROWID;
