INSERT
  INTO tech$security_daily_info_shares
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$last_seen_dt,
    tech$hash_value,
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
  )
WITH
     w_raw AS
     (
         SELECT
                tech$load_dt,
                sha1(concat_value) AS hash_value,
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
           FROM (SELECT
                        tech$load_dt,
                        '_' || IFNULL(CAST(short_name                 AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(previous_price             AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(lot_size                   AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(face_value                 AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(status                     AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(board_name                 AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(decimals                   AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(security_name              AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(remarks                    AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(market_code                AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(instr_id                   AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(sector_id                  AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(min_step                   AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(prev_wa_price              AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(face_unit                  AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(previous_date              AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(issue_size                 AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(isin                       AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(lat_name                   AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(reg_number                 AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(previous_legal_close_price AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(previous_admitted_quote    AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(currency_id                AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(security_type              AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(list_level                 AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(settle_date                AS TEXT), '!@#\$%^&*') || '_' AS concat_value,
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
                   FROM (SELECT
                                tech$load_dt,
                                ROW_NUMBER() OVER (wnd) AS rn,
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
                           FROM (SELECT
                                        DATE(tech$load_dttm) AS tech$load_dt,
                                        tech$load_dttm       AS tech$load_dttm,
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
                                   FROM src.security_daily_info_shares
                                  WHERE tech$load_dttm >= :tech$load_dttm)
                         WINDOW wnd AS (PARTITION BY
                                                     security_id,
                                                     board_id,
                                                     tech$load_dt
                                            ORDER BY tech$load_dttm DESC))
                  WHERE rn = 1)
     )
SELECT
       :tech$load_id                                                  AS tech$load_id,
       tech$load_dt                                                   AS tech$effective_dt,
       LEAD(DATE(tech$load_dt, '-1 DAY'), 1, '2999-12-31') OVER (wnd) AS tech$expiration_dt,
       tech$last_seen_dt,
       hash_value                                                     AS tech$hash_value,
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
  FROM (SELECT
               tech$load_dt,
               tech$last_seen_dt,
               hash_value,
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
          FROM (SELECT
                       tech$load_dt,
                       MAX(tech$load_dt) OVER (win) AS tech$last_seen_dt,
                       hash_value,
                       LAG(hash_value) OVER (wnd)   AS lag_hash_value,
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
                  FROM w_raw
                WINDOW win AS (PARTITION BY
                                            security_id,
                                            board_id),
                       wnd AS (PARTITION BY
                                            security_id,
                                            board_id
                                   ORDER BY tech$load_dt))
         WHERE hash_value != lag_hash_value
            OR lag_hash_value IS NULL)
WINDOW wnd AS (PARTITION BY
                            security_id,
                            board_id
                   ORDER BY tech$load_dt)
