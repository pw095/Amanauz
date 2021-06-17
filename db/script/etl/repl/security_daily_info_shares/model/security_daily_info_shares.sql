DROP TABLE IF EXISTS security_daily_info_shares;
CREATE TABLE security_daily_info_shares
  (
    tech$load_id               INTEGER NOT NULL,
    tech$effective_dt          TEXT    NOT NULL,
    tech$expiration_dt         TEXT    NOT NULL,
    tech$hash_value            TEXT    NOT NULL,
    security_id                TEXT    NOT NULL,
    board_id                   TEXT    NOT NULL,
    short_name                 TEXT    NOT NULL,
    previous_price             TEXT,
    lot_size                   INTEGER NOT NULL,
    face_value                 REAL    NOT NULL,
    status                     TEXT    NOT NULL,
    board_name                 TEXT    NOT NULL,
    decimals                   INTEGER NOT NULL,
    security_name              TEXT    NOT NULL,
    remarks                    TEXT    NOT NULL,
    market_code                TEXT    NOT NULL,
    instr_id                   TEXT    NOT NULL,
    sector_id                  TEXT    NOT NULL,
    min_step                   REAL    NOT NULL,
    prev_wa_price              REAL,
    face_unit                  TEXT    NOT NULL,
    previous_date              TEXT    NOT NULL,
    issue_size                 INTEGER NOT NULL,
    isin                       TEXT    NOT NULL,
    lat_name                   TEXT    NOT NULL,
    reg_number                 TEXT    NOT NULL,
    previous_legal_close_price REAL,
    previous_admitted_quote    REAL,
    currency_id                TEXT    NOT NULL,
    security_type              TEXT    NOT NULL,
    list_level                 INTEGER NOT NULL,
    settle_date                TEXT    NOT NULL,
    PRIMARY KEY(security_id, board_id, tech$effective_dt)
  );
