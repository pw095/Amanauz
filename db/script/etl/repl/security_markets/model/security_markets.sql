DROP TABLE IF EXISTS security_markets;
CREATE TABLE security_markets
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    id                 INTEGER,
    trade_engine_id    INTEGER,
    trade_engine_name  TEXT,
    trade_engine_title TEXT,
    market_name        TEXT    NOT NULL,
    market_title       TEXT,
    market_id          INTEGER,
    market_place       INTEGER,
    PRIMARY KEY(market_name, tech$effective_dt)
  )
WITHOUT ROWID;
