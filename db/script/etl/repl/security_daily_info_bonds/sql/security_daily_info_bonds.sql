INSERT
  INTO security_daily_info_bonds
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$hash_value,
    security_id,
    board_id,
    short_name,
    prev_wa_price,
    yield_at_prev_wa_price,
    coupon_value,
    next_coupon,
    accrued_int,
    previous_price,
    lot_size,
    face_value,
    board_name,
    status,
    mat_date,
    decimals,
    coupon_period,
    issue_size,
    previous_legal_close_price,
    previous_admitted_quote,
    previous_date,
    security_name,
    remarks,
    market_code,
    instr_id,
    sector_id,
    min_step,
    face_unit,
    buy_back_price,
    buy_back_date,
    isin,
    lat_name,
    reg_number,
    currency_id,
    issue_size_placed,
    list_level,
    security_type,
    coupon_percent,
    offer_date,
    settle_date,
    lot_value
  )
SELECT
       :tech$load_id       AS tech$load_id,
       tech$effective_dt,
       tech$expiration_dt,
       tech$hash_value,
       security_id,
       board_id,
       short_name,
       prev_wa_price,
       yield_at_prev_wa_price,
       coupon_value,
       next_coupon,
       accrued_int,
       previous_price,
       lot_size,
       face_value,
       board_name,
       status,
       mat_date,
       decimals,
       coupon_period,
       issue_size,
       previous_legal_close_price,
       previous_admitted_quote,
       previous_date,
       security_name,
       remarks,
       market_code,
       instr_id,
       sector_id,
       min_step,
       face_unit,
       buy_back_price,
       buy_back_date,
       isin,
       lat_name,
       reg_number,
       currency_id,
       issue_size_placed,
       list_level,
       security_type,
       coupon_percent,
       offer_date,
       settle_date,
       lot_value
  FROM (SELECT
               tech$effective_dt,
               tech$expiration_dt,
               tech$hash_value,
               security_id,
               board_id,
               short_name,
               prev_wa_price,
               yield_at_prev_wa_price,
               coupon_value,
               next_coupon,
               accrued_int,
               previous_price,
               lot_size,
               face_value,
               board_name,
               status,
               mat_date,
               decimals,
               coupon_period,
               issue_size,
               previous_legal_close_price,
               previous_admitted_quote,
               previous_date,
               security_name,
               remarks,
               market_code,
               instr_id,
               sector_id,
               min_step,
               face_unit,
               buy_back_price,
               buy_back_date,
               isin,
               lat_name,
               reg_number,
               currency_id,
               issue_size_placed,
               list_level,
               security_type,
               coupon_percent,
               offer_date,
               settle_date,
               lot_value
          FROM tech$security_daily_info_bonds src
         WHERE NOT EXISTS(SELECT
                                 NULL
                            FROM security_daily_info_bonds sat
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
                        MAX(tech$sat$expiration_dt, tech$expiration_dt)
               END AS tech$expiration_dt,
               tech$hash_value,
               security_id,
               board_id,
               short_name,
               prev_wa_price,
               yield_at_prev_wa_price,
               coupon_value,
               next_coupon,
               accrued_int,
               previous_price,
               lot_size,
               face_value,
               board_name,
               status,
               mat_date,
               decimals,
               coupon_period,
               issue_size,
               previous_legal_close_price,
               previous_admitted_quote,
               previous_date,
               security_name,
               remarks,
               market_code,
               instr_id,
               sector_id,
               min_step,
               face_unit,
               buy_back_price,
               buy_back_date,
               isin,
               lat_name,
               reg_number,
               currency_id,
               issue_size_placed,
               list_level,
               security_type,
               coupon_percent,
               offer_date,
               settle_date,
               lot_value
          FROM (SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       tech$sat$effective_dt,
                       tech$sat$expiration_dt,
                       tech$hash_value,
                       security_id,
                       board_id,
                       short_name,
                       prev_wa_price,
                       yield_at_prev_wa_price,
                       coupon_value,
                       next_coupon,
                       accrued_int,
                       previous_price,
                       lot_size,
                       face_value,
                       board_name,
                       status,
                       mat_date,
                       decimals,
                       coupon_period,
                       issue_size,
                       previous_legal_close_price,
                       previous_admitted_quote,
                       previous_date,
                       security_name,
                       remarks,
                       market_code,
                       instr_id,
                       sector_id,
                       min_step,
                       face_unit,
                       buy_back_price,
                       buy_back_date,
                       isin,
                       lat_name,
                       reg_number,
                       currency_id,
                       issue_size_placed,
                       list_level,
                       security_type,
                       coupon_percent,
                       offer_date,
                       settle_date,
                       lot_value,
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
                               sat.tech$effective_dt                 AS tech$sat$effective_dt,
                               DATE(src.tech$effective_dt, '-1 DAY') AS tech$sat$expiration_dt,
                               src.tech$hash_value,
                               src.security_id,
                               src.board_id,
                               src.short_name,
                               src.prev_wa_price,
                               src.yield_at_prev_wa_price,
                               src.coupon_value,
                               src.next_coupon,
                               src.accrued_int,
                               src.previous_price,
                               src.lot_size,
                               src.face_value,
                               src.board_name,
                               src.status,
                               src.mat_date,
                               src.decimals,
                               src.coupon_period,
                               src.issue_size,
                               src.previous_legal_close_price,
                               src.previous_admitted_quote,
                               src.previous_date,
                               src.security_name,
                               src.remarks,
                               src.market_code,
                               src.instr_id,
                               src.sector_id,
                               src.min_step,
                               src.face_unit,
                               src.buy_back_price,
                               src.buy_back_date,
                               src.isin,
                               src.lat_name,
                               src.reg_number,
                               src.currency_id,
                               src.issue_size_placed,
                               src.list_level,
                               src.security_type,
                               src.coupon_percent,
                               src.offer_date,
                               src.settle_date,
                               src.lot_value,
                               FIRST_VALUE(CASE
                                                WHEN src.tech$hash_value != sat.tech$hash_value THEN
                                                    'NON_EQUAL'
                                                ELSE
                                                    'EQUAL'
                                           END) OVER (wnd) AS fv_equal_flag,
                               ROW_NUMBER() OVER (wnd)     AS rn
                          FROM tech$security_daily_info_bonds src
                               JOIN
                               security_daily_info_bonds sat
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
           tech$load_id = excluded.tech$load_id
