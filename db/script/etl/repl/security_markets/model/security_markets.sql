DROP TABLE IF EXISTS security_markets;
CREATE TABLE security_markets
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    id                 INTEGER NOT NULL,
    trade_engine_id    INTEGER NOT NULL,
    trade_engine_name  TEXT    NOT NULL,
    trade_engine_title TEXT    NOT NULL,
    market_name        TEXT    NOT NULL,
    market_title       TEXT    NOT NULL,
    market_id          INTEGER NOT NULL,
    market_place       INTEGER NOT NULL,
    PRIMARY KEY(market_name, tech$effective_dt)
  )
WITHOUT ROWID;
