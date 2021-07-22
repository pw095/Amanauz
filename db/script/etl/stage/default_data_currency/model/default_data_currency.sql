DROP TABLE IF EXISTS default_data_currency;
CREATE TABLE default_data_currency
  (
    tech$load_id   INTEGER NOT NULL,
    tech$load_dttm TEXT    NOT NULL,
    iso_char_code  TEXT    NOT NULL,
    iso_num_code   TEXT    NOT NULL,
    rus_name       TEXT    NOT NULL,
    eng_name       TEXT    NOT NULL,
    nominal        INTEGER NOT NULL
  );
