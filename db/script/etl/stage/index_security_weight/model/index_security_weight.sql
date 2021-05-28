DROP TABLE index_security_weight;
CREATE TABLE index_security_weight
  (
    index_id        TEXT,
    trade_date      TEXT,
    ticker          TEXT,
    short_names     TEXT,
    secids          TEXT,
    weight          REAL,
    trading_session INTEGER,
    tech$load_id    INTEGER NOT NULL
  );
