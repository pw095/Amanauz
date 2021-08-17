DROP TABLE IF EXISTS tech$master_data_emitent_map;
CREATE TABLE tech$master_data_emitent_map
  (
    tech$load_id        INTEGER NOT NULL,
    tech$effective_dt   TEXT    NOT NULL,
    tech$expiration_dt  TEXT    NOT NULL,
    tech$last_seen_dt   TEXT    NOT NULL,
    tech$hash_value     TEXT    NOT NULL,
    source_system_code  TEXT    NOT NULL,
    emitent_source_name TEXT    NOT NULL,
    emitent_full_name   TEXT    NOT NULL,
    PRIMARY KEY(source_system_code, emitent_source_name, tech$effective_dt)
  )
WITHOUT ROWID;
