DROP TABLE IF EXISTS master_data_emitent_map;
CREATE TABLE master_data_emitent_map
  (
    tech$load_id        INTEGER NOT NULL,
    tech$load_dttm      TEXT    NOT NULL,
    source_system_code  TEXT    NOT NULL,
    emitent_source_name TEXT    NOT NULL,
    emitent_full_name   TEXT    NOT NULL
  );
