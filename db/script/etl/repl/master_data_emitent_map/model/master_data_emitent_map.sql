DROP TABLE IF EXISTS master_data_emitent_map;
CREATE TABLE master_data_emitent_map
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    source_system_code TEXT    NOT NULL,
    emitent_code       TEXT    NOT NULL,
    emitent_short_name TEXT,
    PRIMARY KEY(source_system_code, emitent_code, tech$effective_dt)
  )
WITHOUT ROWID;
