DROP TABLE IF EXISTS tech$foreign_currency_dictionary;
CREATE TABLE tech$foreign_currency_dictionary
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    id                 INTEGER NOT NULL,
    name               INTEGER,
    eng_name           TEXT,
    nominal            TEXT,
    parent_code        TEXT,
    iso_num_code       TEXT,
    iso_char_code      TEXT,
    PRIMARY KEY(id, tech$effective_dt)
  )
WITHOUT ROWID;
