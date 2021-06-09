DROP TABLE IF EXISTS index_security_weight;
CREATE TABLE index_security_weight
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    index_id           TEXT    NOT NULL,
    trade_date         TEXT    NOT NULL,
    ticker             TEXT    NOT NULL,
    short_name         TEXT    NOT NULL,
    security_id        TEXT    NOT NULL,
    weight             REAL    NOT NULL,
    trading_session    INTEGER NOT NULL,
    PRIMARY KEY(index_id, trade_date, security_id, tech$effective_dt)
  );
