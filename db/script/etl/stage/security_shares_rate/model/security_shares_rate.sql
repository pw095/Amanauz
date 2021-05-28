DROP TABLE security_shares_rate;
CREATE TABLE security_shares_rate
  (
    board_id                    TEXT,
    trade_date                  TEXT,
    short_name                  TEXT,
    security_id                 TEXT,
    num_trades                  REAL,
    value                       REAL,
    open                        REAL,
    low                         REAL,
    high                        REAL,
    legal_close_price           REAL,
    wa_price                    REAL,
    close                       REAL,
    volume                      REAL,
    market_price_2              REAL,
    market_price_3              REAL,
    admitted_quote              REAL,
    mp2_val_trd                 REAL,
    market_price_3_trades_value REAL,
    admitted_value              REAL,
    wa_val                      REAL,
    trading_session             INTEGER,
    tech$load_id                INTEGER NOT NULL
  );
