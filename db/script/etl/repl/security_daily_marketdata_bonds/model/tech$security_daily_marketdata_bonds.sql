DROP TABLE IF EXISTS tech$security_daily_marketdata_bonds;
CREATE TABLE tech$security_daily_marketdata_bonds
  (
    tech$load_id                    INTEGER NOT NULL,
    tech$effective_dt               TEXT    NOT NULL,
    tech$expiration_dt              TEXT    NOT NULL,
    tech$hash_value                 TEXT    NOT NULL,
    security_id                     TEXT    NOT NULL,
    bid                             REAL,
    bid_depth                       TEXT    NOT NULL,
    offer                           REAL,
    offer_depth                     TEXT    NOT NULL,
    spread                          REAL    NOT NULL,
    bid_deptht                      INTEGER NOT NULL,
    offer_deptht                    INTEGER NOT NULL,
    open                            REAL,
    low                             REAL,
    high                            REAL,
    last                            REAL,
    last_change                     REAL    NOT NULL,
    last_change_prcnt               REAL    NOT NULL,
    qty                             INTEGER NOT NULL,
    value                           REAL    NOT NULL,
    yield                           REAL    NOT NULL,
    value_usd                       REAL    NOT NULL,
    wa_price                        REAL,
    last_cng_to_last_wa_price_prcnt REAL    NOT NULL,
    wap_to_prev_wa_price_prcnt      REAL    NOT NULL,
    wap_to_prev_wa_price            REAL    NOT NULL,
    yield_at_wa_price               REAL    NOT NULL,
    yield_to_prev_yield             REAL    NOT NULL,
    close_yield                     REAL    NOT NULL,
    close_price                     REAL,
    market_price_to_day             REAL,
    market_price                    REAL,
    last_to_prev_price              REAL    NOT NULL,
    num_trades                      INTEGER NOT NULL,
    vol_to_day                      INTEGER NOT NULL,
    val_to_day                      INTEGER NOT NULL,
    val_to_day_usd                  INTEGER NOT NULL,
    board_id                        TEXT    NOT NULL,
    trading_status                  TEXT    NOT NULL,
    update_time                     TEXT    NOT NULL,
    duration                        REAL    NOT NULL,
    num_bids                        TEXT    NOT NULL,
    num_offers                      TEXT    NOT NULL,
    change                          REAL,
    time                            TEXT    NOT NULL,
    high_bid                        TEXT    NOT NULL,
    low_offer                       TEXT    NOT NULL,
    price_minus_prev_wa_price       REAL,
    last_bid                        TEXT    NOT NULL,
    last_offer                      TEXT    NOT NULL,
    l_current_price                 REAL,
    l_close_price                   REAL,
    market_price_2                  REAL,
    admitted_quote                  REAL,
    open_period_price               REAL,
    seq_num                         INTEGER NOT NULL,
    sys_time                        TEXT    NOT NULL,
    val_to_day_rur                  INTEGER NOT NULL,
    iri_cpi_close                   REAL,
    bei_close                       REAL,
    cbr_close                       REAL,
    yield_to_offer                  REAL,
    yield_last_coupon               REAL,
    trading_session                 TEXT    NOT NULL,
    PRIMARY KEY(security_id, board_id, tech$effective_dt)
  );