DROP TABLE IF EXISTS foreign_currency_dictionary;
CREATE TABLE foreign_currency_dictionary
  (
    tech$load_id   INTEGER NOT NULL,
    tech$load_dttm TEXT    NOT NULL,
    id             TEXT,
    name           TEXT,
    eng_name       TEXT,
    nominal        INTEGER,
    parent_code    TEXT,
    iso_num_code   TEXT,
    iso_char_code  TEXT
  );
