DROP TABLE IF EXISTS default_data_security;
CREATE TABLE default_data_security
  (
    tech$load_id   INTEGER NOT NULL,
    tech$load_dttm TEXT    NOT NULL,
    security_id    TEXT    NOT NULL,
    security_name  TEXT    NOT NULL
  );
