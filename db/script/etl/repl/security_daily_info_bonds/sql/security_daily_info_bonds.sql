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
                        CASE upsert_flg
                             WHEN 'UPSERT' THEN
                                 tech$sat$expiration_dt
                             ELSE
                                 tech$expiration_dt
                        END
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
           tech$load_id = excluded.tech$load_id,
           short_name = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.short_name ELSE short_name END,
           prev_wa_price = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.prev_wa_price ELSE prev_wa_price END,
           yield_at_prev_wa_price = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.yield_at_prev_wa_price ELSE yield_at_prev_wa_price END,
           coupon_value = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.coupon_value ELSE coupon_value END,
           next_coupon = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.next_coupon ELSE next_coupon END,
           accrued_int = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.accrued_int ELSE accrued_int END,
           previous_price = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.previous_price ELSE previous_price END,
           lot_size = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.lot_size ELSE lot_size END,
           face_value = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.face_value ELSE face_value END,
           board_name = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.board_name ELSE board_name END,
           status = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.status ELSE status END,
           mat_date = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.mat_date ELSE mat_date END,
           decimals = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.decimals ELSE decimals END,
           coupon_period = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.coupon_period ELSE coupon_period END,
           issue_size = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.issue_size ELSE issue_size END,
           previous_legal_close_price = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.previous_legal_close_price ELSE previous_legal_close_price END,
           previous_admitted_quote = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.previous_admitted_quote ELSE previous_admitted_quote END,
           previous_date = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.previous_date ELSE previous_date END,
           security_name = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.security_name ELSE security_name END,
           remarks = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.remarks ELSE remarks END,
           market_code = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.market_code ELSE market_code END,
           instr_id = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.instr_id ELSE instr_id END,
           sector_id = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.sector_id ELSE sector_id END,
           min_step = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.min_step ELSE min_step END,
           face_unit = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.face_unit ELSE face_unit END,
           buy_back_price = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.buy_back_price ELSE buy_back_price END,
           buy_back_date = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.buy_back_date ELSE buy_back_date END,
           isin = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.isin ELSE isin END,
           lat_name = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.lat_name ELSE lat_name END,
           reg_number = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.reg_number ELSE reg_number END,
           currency_id = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.currency_id ELSE currency_id END,
           issue_size_placed = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.issue_size_placed ELSE issue_size_placed END,
           list_level = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.list_level ELSE list_level END,
           security_type = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.security_type ELSE security_type END,
           coupon_percent = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.coupon_percent ELSE coupon_percent END,
           offer_date = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.offer_date ELSE offer_date END,
           settle_date = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.settle_date ELSE settle_date END,
           lot_value = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.lot_value ELSE lot_value END
           
