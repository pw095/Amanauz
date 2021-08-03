DROP TABLE IF EXISTS tech$master_data_security_type_map;
CREATE TABLE tech$master_data_security_type_map
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    table_name         TEXT    NOT NULL,
    type_id            TEXT    NOT NULL,
    type_name          TEXT    NOT NULL,
    security_type_id   INTEGER NOT NULL,
    PRIMARY KEY(security_type_id, tech$effective_dt),
    UNIQUE(table_name, type_id, tech$effective_dt)
  )
WITHOUT ROWID;
