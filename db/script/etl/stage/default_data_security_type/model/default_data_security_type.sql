DROP TABLE IF EXISTS default_data_security_type;
CREATE TABLE default_data_security_type
  (
    tech$load_id       INTEGER NOT NULL,
    tech$load_dttm     TEXT    NOT NULL,
    security_type_id   TEXT    NOT NULL,
    security_type_name TEXT    NOT NULL
  );
