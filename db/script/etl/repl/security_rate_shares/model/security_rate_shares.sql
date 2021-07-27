DROP TABLE IF EXISTS security_rate_shares;
CREATE TABLE security_rate_shares
  (
    tech$load_id                INTEGER NOT NULL,
    tech$effective_dt           TEXT    NOT NULL,
    tech$expiration_dt          TEXT    NOT NULL,
    tech$last_seen_dt           TEXT    NOT NULL,
    tech$hash_value             TEXT    NOT NULL,
    board_id                    TEXT    NOT NULL,
    trade_date                  TEXT    NOT NULL,
    short_name                  TEXT    NOT NULL,
    security_id                 TEXT    NOT NULL,
    num_trades                  REAL    NOT NULL,
    value                       REAL    NOT NULL,
    open                        REAL,
    low                         REAL,
    high                        REAL,
    legal_close_price           REAL,
    wa_price                    REAL,
    close                       REAL,
    volume                      REAL    NOT NULL,
    market_price_2              REAL,
    market_price_3              REAL,
    admitted_quote              REAL,
    mp2_val_trd                 REAL,
    market_price_3_trades_value REAL,
    admitted_value              REAL,
    wa_val                      REAL,
    trading_session             INTEGER NOT NULL,
    PRIMARY KEY(board_id, trade_date, security_id, tech$effective_dt)
  )
WITHOUT ROWID;
