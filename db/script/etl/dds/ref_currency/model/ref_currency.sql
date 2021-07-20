DROP TABLE IF EXISTS ref_currency;
CREATE TABLE ref_currency
  (
    tech$load_id       INTEGER NOT NULL,
    tech$load_dt       TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    crnc_code          TEXT    NOT NULL,
    iso_char_code      TEXT    NOT NULL,
    iso_num_code       TEXT    NOT NULL,
    rus_name           TEXT    NOT NULL,
    eng_name           TEXT    NOT NULL,
    nominal            INTEGER NOT NULL,
    PRIMARY KEY (crnc_code)
  )
WITHOUT ROWID;
