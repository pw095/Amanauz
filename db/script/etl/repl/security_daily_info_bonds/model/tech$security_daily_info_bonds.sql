DROP TABLE IF EXISTS tech$security_daily_info_bonds;
CREATE TABLE tech$security_daily_info_bonds
  (
    tech$load_id               INTEGER NOT NULL,
    tech$effective_dt          TEXT    NOT NULL,
    tech$expiration_dt         TEXT    NOT NULL,
    tech$hash_value            TEXT    NOT NULL,
    security_id                TEXT    NOT NULL,
    board_id                   TEXT    NOT NULL,
    short_name                 TEXT    NOT NULL,
    prev_wa_price              REAL,
    yield_at_prev_wa_price     REAL    NOT NULL,
    coupon_value               REAL    NOT NULL,
    next_coupon                TEXT    NOT NULL,
    accrued_int                REAL    NOT NULL,
    previous_price             REAL,
    lot_size                   INTEGER NOT NULL,
    face_value                 REAL    NOT NULL,
    board_name                 TEXT    NOT NULL,
    status                     TEXT    NOT NULL,
    mat_date                   TEXT    NOT NULL,
    decimals                   INTEGER NOT NULL,
    coupon_period              INTEGER NOT NULL,
    issue_size                 INTEGER NOT NULL,
    previous_legal_close_price REAL,
    previous_admitted_quote    REAL,
    previous_date              TEXT    NOT NULL,
    security_name              TEXT    NOT NULL,
    remarks                    TEXT    NOT NULL,
    market_code                TEXT    NOT NULL,
    instr_id                   TEXT    NOT NULL,
    sector_id                  TEXT    NOT NULL,
    min_step                   REAL    NOT NULL,
    face_unit                  TEXT    NOT NULL,
    buy_back_price             REAL,
    buy_back_date              TEXT    NOT NULL,
    isin                       TEXT    NOT NULL,
    lat_name                   TEXT    NOT NULL,
    reg_number                 TEXT    NOT NULL,
    currency_id                TEXT    NOT NULL,
    issue_size_placed          INTEGER NOT NULL,
    list_level                 INTEGER NOT NULL,
    security_type              TEXT    NOT NULL,
    coupon_percent             REAL,
    offer_date                 TEXT    NOT NULL,
    settle_date                TEXT    NOT NULL,
    lot_value                  REAL
    PRIMARY KEY(security_id, board_id, tech$effective_dt)
  );