DROP TABLE IF EXISTS tech$security_daily_marketdata_bonds;
CREATE TABLE tech$security_daily_marketdata_bonds
  (
    tech$load_id                    INTEGER NOT NULL,
    tech$effective_dt               TEXT    NOT NULL,
    tech$expiration_dt              TEXT    NOT NULL,
    tech$last_seen_dt               TEXT    NOT NULL,
    tech$hash_value                 TEXT    NOT NULL,
    security_id                     TEXT    NOT NULL,
    bid                             REAL,
    bid_depth                       TEXT,
    offer                           REAL,
    offer_depth                     TEXT,
    spread                          REAL,
    bid_deptht                      INTEGER,
    offer_deptht                    INTEGER,
    open                            REAL,
    low                             REAL,
    high                            REAL,
    last                            REAL,
    last_change                     REAL,
    last_change_prcnt               REAL,
    qty                             INTEGER,
    value                           REAL,
    yield                           REAL,
    value_usd                       REAL,
    wa_price                        REAL,
    last_cng_to_last_wa_price_prcnt REAL,
    wap_to_prev_wa_price_prcnt      REAL,
    wap_to_prev_wa_price            REAL,
    yield_at_wa_price               REAL,
    yield_to_prev_yield             REAL,
    close_yield                     REAL,
    close_price                     REAL,
    market_price_to_day             REAL,
    market_price                    REAL,
    last_to_prev_price              REAL,
    num_trades                      INTEGER,
    vol_to_day                      INTEGER,
    val_to_day                      INTEGER,
    val_to_day_usd                  INTEGER,
    board_id                        TEXT    NOT NULL,
    trading_status                  TEXT,
    update_time                     TEXT,
    duration                        REAL,
    num_bids                        TEXT,
    num_offers                      TEXT,
    change                          REAL,
    time                            TEXT,
    high_bid                        TEXT,
    low_offer                       TEXT,
    price_minus_prev_wa_price       REAL,
    last_bid                        TEXT,
    last_offer                      TEXT,
    l_current_price                 REAL,
    l_close_price                   REAL,
    market_price_2                  REAL,
    admitted_quote                  REAL,
    open_period_price               REAL,
    seq_num                         INTEGER,
    sys_time                        TEXT,
    val_to_day_rur                  INTEGER,
    iri_cpi_close                   REAL,
    bei_close                       REAL,
    cbr_close                       REAL,
    yield_to_offer                  REAL,
    yield_last_coupon               REAL,
    trading_session                 TEXT,
    PRIMARY KEY(security_id, board_id, tech$effective_dt)
  )
WITHOUT ROWID;
