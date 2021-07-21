DROP TABLE IF EXISTS default_data_emitent;
CREATE TABLE default_data_emitent
  (
    tech$load_id   INTEGER NOT NULL,
    tech$load_dttm TEXT    NOT NULL,
    emitent_code   TEXT    NOT NULL,
    emitent_name   TEXT    NOT NULL
  );
