DROP TABLE IF EXISTS sat_security_rate;
CREATE TABLE sat_security_rate
  (
    tech$load_id       INTEGER NOT NULL,
    tech$hash_key      TEXT    NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    trade_count        REAL    NOT NULL,
    value              REAL    NOT NULL,
    volume             REAL    NOT NULL,
    price_open         REAL,
    price_close        REAL,
    price_low          REAL,
    price_high         REAL,
    PRIMARY KEY(tech$hash_key, tech$effective_dt),
    FOREIGN KEY (tech$hash_key) REFERENCES lnk_security_rate(tech$hash_key) ON DELETE CASCADE
  )
WITHOUT ROWID;
