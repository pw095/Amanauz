DROP TABLE IF EXISTS tech$default_data_currency;
CREATE TABLE tech$default_data_currency
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    iso_char_code      TEXT    NOT NULL,
    iso_num_code       TEXT    NOT NULL,
    rus_name           TEXT    NOT NULL,
    eng_name           TEXT    NOT NULL,
    nominal            INTEGER NOT NULL,
    PRIMARY KEY(iso_char_code, tech$effective_dt)
  )
WITHOUT ROWID;
