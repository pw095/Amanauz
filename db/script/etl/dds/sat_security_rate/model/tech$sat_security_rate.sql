DROP TABLE IF EXISTS tech$sat_security;
CREATE TABLE tech$sat_security
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
    PRIMARY KEY(tech$hash_key, tech$effective_dt)
  )
WITHOUT ROWID;
