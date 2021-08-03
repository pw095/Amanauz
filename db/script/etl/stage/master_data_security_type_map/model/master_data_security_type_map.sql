DROP TABLE IF EXISTS master_data_security_type_map;
CREATE TABLE master_data_security_type_map
  (
    tech$load_id     INTEGER NOT NULL,
    tech$load_dttm   TEXT    NOT NULL,
    table_name       TEXT    NOT NULL,
    type_id          TEXT    NOT NULL,
    type_name        TEXT    NOT NULL,
    security_type_id INTEGER NOT NULL
  );
