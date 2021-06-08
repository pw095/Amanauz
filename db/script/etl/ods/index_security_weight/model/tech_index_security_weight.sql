DROP TABLE IF EXISTS tech$index_security_weight;
CREATE TABLE tech$index_security_weight
  (
    tech$hash_key      TEXT,
    tech$effective_dt  TEXT,
    tech$expiration_dt TEXT,
    tech$hash_value    TEXT,
    index_id           TEXT,
    trade_date         TEXT,
    ticker             TEXT,
    short_name         TEXT,
    security_id        TEXT,
    weight             REAL,
    trading_session    INTEGER,
    UNIQUE(tech$hash_key, tech$effective_dt),
    UNIQUE(index_id, trade_date, security_id, tech$effective_dt)
  );
