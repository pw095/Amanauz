DROP TABLE IF EXISTS index_security_weight;
CREATE TABLE index_security_weight
  (
    tech$load_id    INTEGER NOT NULL,
    tech$load_dttm  TEXT    NOT NULL,
    index_id        TEXT,
    trade_date      TEXT,
    ticker          TEXT,
    short_name      TEXT,
    security_id     TEXT,
    weight          REAL,
    trading_session INTEGER
  );
