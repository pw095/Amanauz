DROP TABLE IF EXISTS tech$sat_index_security;
CREATE TABLE tech$sat_index_security
  (
    tech$load_id       INTEGER NOT NULL,
    tech$hash_key      TEXT    NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    weight             REAL    NOT NULL,
    PRIMARY KEY(tech$hash_key),
    UNIQUE(tech$hash_key, tech$effective_dt)
  )
WITHOUT ROWID;
