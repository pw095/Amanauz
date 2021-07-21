DROP TABLE IF EXISTS default_data_security;
CREATE TABLE default_data_security
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    security_id        TEXT    NOT NULL,
    security_name      TEXT    NOT NULL,
    PRIMARY KEY(security_id, tech$effective_dt)
  )
WITHOUT ROWID;
