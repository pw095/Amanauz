DROP TABLE IF EXISTS security_daily_info_bonds;
CREATE TABLE security_daily_info_bonds
  (
    tech$load_id               INTEGER NOT NULL,
    tech$load_dttm             INTEGER NOT NULL,
    security_id                TEXT,
    board_id                   TEXT,
    short_name                 TEXT,
    prev_wa_price              REAL,
    yield_at_prev_wa_price     REAL,
    coupon_value               REAL,
    next_coupon                TEXT,
    accrued_int                REAL,
    previous_price             REAL,
    lot_size                   INTEGER,
    face_value                 REAL,
    board_name                 TEXT,
    status                     TEXT,
    mat_date                   TEXT,
    decimals                   INTEGER,
    coupon_period              INTEGER,
    issue_size                 INTEGER,
    previous_legal_close_price REAL,
    previous_admitted_quote    REAL,
    previous_date              TEXT,
    security_name              TEXT,
    remarks                    TEXT,
    market_code                TEXT,
    instr_id                   TEXT,
    sector_id                  TEXT,
    min_step                   REAL,
    face_unit                  TEXT,
    buy_back_price             REAL,
    buy_back_date              TEXT,
    isin                       TEXT,
    lat_name                   TEXT,
    reg_number                 TEXT,
    currency_id                TEXT,
    issue_size_placed          INTEGER,
    list_level                 INTEGER,
    security_type              TEXT,
    coupon_percent             REAL,
    offer_date                 TEXT,
    settle_date                TEXT,
    lot_value                  REAL
  );
