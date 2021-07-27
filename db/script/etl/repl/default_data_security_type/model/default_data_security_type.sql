DROP TABLE IF EXISTS default_data_security_type;
CREATE TABLE default_data_security_type
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    security_type_id   TEXT    NOT NULL,
    security_type_name TEXT    NOT NULL,
    PRIMARY KEY(security_type_id, tech$effective_dt)
  )
WITHOUT ROWID;
