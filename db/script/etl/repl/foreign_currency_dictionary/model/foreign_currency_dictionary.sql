DROP TABLE IF EXISTS foreign_currency_dictionary;
CREATE TABLE foreign_currency_dictionary
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    id                 INTEGER NOT NULL,
    name               INTEGER NOT NULL,
    eng_name           TEXT    NOT NULL,
    nominal            TEXT    NOT NULL,
    parent_code        TEXT    NOT NULL,
    iso_num_code       TEXT,
    iso_char_code      TEXT,
    PRIMARY KEY(id, tech$effective_dt)
  )
WITHOUT ROWID;
