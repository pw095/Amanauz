DROP TABLE IF EXISTS tech$security_daily_marketdata_shares;
CREATE TABLE tech$security_daily_marketdata_shares
  (
    tech$load_id                     INTEGER NOT NULL,
    tech$effective_dt                TEXT    NOT NULL,
    tech$expiration_dt               TEXT    NOT NULL,
    tech$last_seen_dt                TEXT    NOT NULL,
    tech$hash_value                  TEXT    NOT NULL,
    security_id                      TEXT    NOT NULL,
    board_id                         TEXT    NOT NULL,
    bid                              REAL,
    bid_depth                        TEXT,
    offer                            REAL,
    offer_depth                      TEXT,
    spread                           REAL,
    bid_deptht                       INTEGER,
    offer_deptht                     INTEGER,
    open                             REAL,
    low                              REAL,
    high                             REAL,
    last                             REAL,
    last_change                      REAL,
    last_change_prcnt                REAL,
    qty                              INTEGER,
    value                            REAL,
    value_usd                        REAL,
    wa_price                         REAL,
    last_cng_to_last_wa_price_prcnt  REAL,
    wap_to_prev_wa_price_prcnt       REAL,
    wap_to_prev_wa_price             REAL,
    close_price                      REAL,
    market_price_to_day              REAL,
    market_price                     REAL,
    last_to_prev_price               REAL,
    num_trades                       INTEGER,
    vol_to_day                       INTEGER,
    val_to_day                       INTEGER,
    val_to_day_usd                   INTEGER,
    etf_settle_price                 REAL,
    trading_status                   TEXT,
    update_time                      TEXT,
    admitted_quote                   REAL,
    last_bid                         TEXT,
    last_offer                       TEXT,
    l_close_price                    REAL,
    l_current_price                  REAL,
    market_price_2                   REAL,
    num_bids                         TEXT,
    num_offers                       TEXT,
    chage                            REAL,
    time                             TEXT,
    high_bid                         TEXT,
    low_offer                        TEXT,
    price_minus_prev_wa_price        REAL,
    open_period_price                REAL,
    seq_num                          INTEGER,
    sys_time                         TEXT,
    closing_auction_price            REAL,
    closing_auction_volume           REAL,
    issue_capitalization             REAL,
    issue_capitalization_update_time TEXT,
    etf_settle_currency              TEXT,
    val_to_day_rur                   INTEGER,
    trading_session                  TEXT,
    PRIMARY KEY(security_id, board_id, tech$effective_dt)
  )
WITHOUT ROWID;
