INSERT
  INTO security_daily_info_shares
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
SELECT
       :tech$load_id       AS tech$load_id,
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
  FROM (SELECT
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
          FROM tech$security_daily_info_shares src
         WHERE NOT EXISTS(SELECT
                                 NULL
                            FROM security_daily_info_shares sat
                           WHERE
                                 sat.security_id = src.security_id
                             AND sat.board_id = src.board_id
                             AND sat.tech$expiration_dt = '2999-12-31')
         UNION ALL
        SELECT
               CASE mrg.flg
                    WHEN 'INSERT' THEN
                        tech$effective_dt
                    WHEN 'UPDATE' THEN
                        tech$sat$effective_dt
               END AS tech$effective_dt,
               CASE mrg.flg
                    WHEN 'INSERT' THEN
                        tech$expiration_dt
                    WHEN 'UPDATE' THEN
                        CASE upsert_flg
                             WHEN 'UPSERT' THEN
                                 tech$sat$expiration_dt
                             ELSE
                                 tech$expiration_dt
                        END
               END AS tech$expiration_dt,
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
          FROM (SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       tech$last_seen_dt,
                       tech$sat$effective_dt,
                       tech$sat$expiration_dt,
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
                       settle_date,
                       CASE
                            WHEN rn = 1
                             AND fv_equal_flag = 'NON_EQUAL'
                             AND tech$effective_dt = tech$sat$effective_dt THEN
                                'UPDATE'
                            WHEN rn = 1
                              OR rn = 2 AND fv_equal_flag = 'EQUAL' THEN
                                'UPSERT'
                            ELSE
                                'INSERT'
                       END AS upsert_flg
                  FROM (SELECT
                               src.tech$effective_dt,
                               src.tech$expiration_dt,
                               src.tech$last_seen_dt,
                               sat.tech$effective_dt                 AS tech$sat$effective_dt,
                               DATE(src.tech$effective_dt, '-1 DAY') AS tech$sat$expiration_dt,
                               src.tech$hash_value,
                               src.security_id,
                               src.board_id,
                               src.short_name,
                               src.previous_price,
                               src.lot_size,
                               src.face_value,
                               src.status,
                               src.board_name,
                               src.decimals,
                               src.security_name,
                               src.remarks,
                               src.market_code,
                               src.instr_id,
                               src.sector_id,
                               src.min_step,
                               src.prev_wa_price,
                               src.face_unit,
                               src.previous_date,
                               src.issue_size,
                               src.isin,
                               src.lat_name,
                               src.reg_number,
                               src.previous_legal_close_price,
                               src.previous_admitted_quote,
                               src.currency_id,
                               src.security_type,
                               src.list_level,
                               src.settle_date,
                               FIRST_VALUE(CASE
                                                WHEN src.tech$hash_value != sat.tech$hash_value THEN
                                                    'NON_EQUAL'
                                                ELSE
                                                    'EQUAL'
                                           END) OVER (wnd) AS fv_equal_flag,
                               ROW_NUMBER() OVER (wnd)     AS rn
                          FROM tech$security_daily_info_shares src
                               JOIN
                               security_daily_info_shares sat
                                   ON
                                      sat.security_id = src.security_id
                                  AND sat.board_id = src.board_id
                                  AND sat.tech$effective_dt <= src.tech$effective_dt
                                  AND sat.tech$expiration_dt = '2999-12-31'
                        WINDOW wnd AS (PARTITION BY src.security_id,
                                                    src.board_id
                                           ORDER BY src.tech$effective_dt))
                 WHERE rn = 1 AND fv_equal_flag = 'NON_EQUAL'
                    OR rn > 1) src
               CROSS JOIN
               (SELECT 'INSERT' AS flg
                 UNION ALL
                SELECT 'UPDATE' AS flg) mrg
    WHERE src.upsert_flg = 'UPSERT'
       OR src.upsert_flg = mrg.flg)
 WHERE 1 = 1
 ON CONFLICT(security_id, board_id, tech$effective_dt)
 DO UPDATE
       SET tech$expiration_dt = excluded.tech$expiration_dt,
           tech$last_seen_dt = excluded.tech$last_seen_dt,
           tech$load_id = excluded.tech$load_id,
           tech$hash_value = CASE
                                  WHEN tech$expiration_dt = '2999-12-31'
                                   AND excluded.tech$expiration_dt = '2999-12-31' THEN
                                      excluded.tech$hash_value
                                  ELSE
                                      tech$hash_value
                             END
