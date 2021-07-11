DROP TABLE IF EXISTS master_data_emitent;
CREATE TABLE master_data_emitent
  (
    tech$load_id   INTEGER NOT NULL,
    tech$load_dttm TEXT    NOT NULL,
    full_name      TEXT    NOT NULL,
    short_name     TEXT    NOT NULL,
    reg_date       TEXT,
    ogrn           TEXT,
    inn            TEXT
  );
