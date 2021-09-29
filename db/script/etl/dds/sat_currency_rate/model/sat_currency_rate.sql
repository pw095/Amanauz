DROP TABLE IF EXISTS sat_currency_rate;
CREATE TABLE sat_currency_rate
  (
    tech$load_id       INTEGER NOT NULL,
    tech$hash_key      TEXT    NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    rate               REAL    NOT NULL,
    PRIMARY KEY(tech$hash_key, tech$effective_dt),
    FOREIGN KEY(tech$hash_key) REFERENCES lnk_currency_rate(tech$hash_key) ON DELETE CASCADE
  )
WITHOUT ROWID;
