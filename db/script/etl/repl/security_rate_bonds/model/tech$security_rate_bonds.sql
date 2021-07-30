DROP TABLE IF EXISTS tech$security_rate_bonds;
CREATE TABLE tech$security_rate_bonds
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
    low                         REAL,
    high                        REAL,
    close                       REAL,
    legal_close_price           REAL,
    acc_int                     REAL,
    wa_price                    REAL,
    yield_close                 REAL,
    open                        REAL,
    volume                      REAL    NOT NULL,
    market_price_2              REAL,
    market_price_3              REAL,
    admitted_quote              REAL,
    mp2_val_trd                 REAL,
    market_price_3_trades_value REAL,
    admitted_value              REAL,
    mat_date                    TEXT    NOT NULL,
    duration                    REAL,
    yield_at_map                REAL,
    iri_cpi_close               REAL,
    bei_close                   REAL,
    coupon_percent              REAL,
    coupon_value                REAL,
    buy_back_date               TEXT    NOT NULL,
    last_trade_date             TEXT    NOT NULL,
    face_value                  REAL,
    currency_id                 TEXT    NOT NULL,
    cbr_close                   REAL,
    yield_to_offer              REAL,
    yield_last_coupon           REAL,
    offer_date                  TEXT    NOT NULL,
    face_unit                   TEXT    NOT NULL,
    trading_session             INTEGER NOT NULL,
    PRIMARY KEY(board_id, trade_date, security_id, tech$effective_dt)
  )
WITHOUT ROWID;
