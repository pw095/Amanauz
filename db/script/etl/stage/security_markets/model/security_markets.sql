DROP TABLE IF EXISTS security_markets;
CREATE TABLE security_markets
  (
    tech$load_id       INTEGER NOT NULL,
    tech$load_dttm     TEXT    NOT NULL,
    id                 INTEGER,
    trade_engine_id    INTEGER,
    trade_engine_name  TEXT,
    trade_engine_title TEXT,
    market_name        TEXT,
    market_title       TEXT,
    market_id          INTEGER,
    marketplace        TEXT
  );
