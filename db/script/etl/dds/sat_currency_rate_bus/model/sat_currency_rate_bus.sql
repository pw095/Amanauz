DROP TABLE IF EXISTS sat_currency_rate_bus;
CREATE TABLE sat_currency_rate_bus
  (
    tech$load_id       INTEGER NOT NULL,
    crnc_code          TEXT    NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    rate               REAL    NOT NULL,
    PRIMARY KEY(crnc_code, tech$effective_dt)
  )
WITHOUT ROWID;
