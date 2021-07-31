DROP TABLE IF EXISTS tech$foreign_currency_rate;
CREATE TABLE tech$foreign_currency_rate
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    trade_date         TEXT    NOT NULL,
    id                 TEXT    NOT NULL,
    nominal            INTEGER,
    value              REAL,
    PRIMARY KEY(trade_date, id, tech$effective_dt)
  )
WITHOUT ROWID;
