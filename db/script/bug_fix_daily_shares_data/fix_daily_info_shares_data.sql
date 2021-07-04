DROP TABLE IF EXISTS security_daily_info_shares_copy;
CREATE TABLE security_daily_info_shares_copy
  (
    tech$load_id               INTEGER NOT NULL,
    tech$load_dttm             INTEGER NOT NULL,
    security_id                TEXT,
    board_id                   TEXT,
    short_name                 TEXT,
    previous_price             TEXT,
    lot_size                   INTEGER,
    face_value                 REAL,
    status                     TEXT,
    board_name                 TEXT,
    decimals                   INTEGER,
    security_name              TEXT,
    remarks                    TEXT,
    market_code                TEXT,
    instr_id                   TEXT,
    sector_id                  TEXT,
    min_step                   REAL,
    prev_wa_price              REAL,
    face_unit                  TEXT,
    previous_date              TEXT,
    issue_size                 INTEGER,
    isin                       TEXT,
    lat_name                   TEXT,
    reg_number                 TEXT,
    previous_legal_close_price REAL,
    previous_admitted_quote    REAL,
    currency_id                TEXT,
    security_type              TEXT,
    list_level                 INTEGER,
    settle_date                TEXT
  );

INSERT INTO security_daily_info_shares_copy
SELECT
       tech$load_id,
       tech$load_dttm,
       security_id,
       board_id,
       short_name,
       previous_price,
       lot_size,
       face_value,
       status,
       decimals,
       board_name,
       security_name,
       remarks,
       market_code,
       instr_id,
       "",
       sector_id,
       min_step,
       "",
       face_unit,
       NULL,
       "",
       isin,
       lat_name,
       reg_number,
       previous_legal_close_price,
       "",
       currency_id,
       security_type,
       NULL
  FROM security_daily_info_shares
 WHERE 1 = 1
   AND tech$load_id < 12
 UNION ALL
SELECT
       tech$load_id,
       tech$load_dttm,
       security_id,
       board_id,
       short_name,
       previous_price,
       lot_size,
       face_value,
       status,
       board_name,
       decimals,
       security_name,
       remarks,
       market_code,
       instr_id,
       sector_id,
       min_step,
       prev_wa_price,
       face_unit,
       previous_date,
       issue_size,
       isin,
       lat_name,
       reg_number,
       previous_legal_close_price,
       previous_admitted_quote,
       currency_id,
       security_type,
       list_level,
       settle_date
  FROM security_daily_info_shares t
 WHERE tech$load_id >= 12;

UPDATE security_daily_info_shares_copy
   SET (face_unit, issue_size, isin, currency_id, settle_date) =
       (SELECT face_unit, issue_size, isin, currency_id, settle_date
          FROM security_daily_info_shares src
         WHERE tech$load_id = 12
           AND src.security_id = security_id
           AND src.board_id = board_id)
 WHERE tech$load_id < 12;

UPDATE security_daily_info_shares_copy
   SET settle_date = CASE tech$load_id
                          WHEN 11 THEN
                              '2021-06-17'
                          WHEN 10 THEN
                              '2021-06-16'
                          WHEN 9 THEN
                              '2021-06-16'
                          WHEN 8 THEN
                              '2021-06-16'
                          WHEN 7 THEN
                              '2021-06-16'
                          WHEN 6 THEN
                              '2021-06-15'
                          WHEN 5 THEN
                              '2021-06-11'
                          WHEN 4 THEN
                              '2021-06-11'
                          WHEN 3 THEN
                              '2021-06-10'
                          WHEN 2 THEN
                              '2021-06-10'
                          WHEN 1 THEN
                              '2021-06-10'
                      END
 WHERE tech$load_id < 12;
DROP TABLE IF EXISTS security_daily_info_shares;
ALTER TABLE security_daily_info_shares_copy RENAME TO security_daily_info_shares;