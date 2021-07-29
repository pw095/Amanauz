INSERT
  INTO security_daily_info_shares
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
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
           tech$load_id = excluded.tech$load_id,
           short_name = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.short_name ELSE short_name END,
           previous_price = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.previous_price ELSE previous_price END,
           lot_size = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.lot_size ELSE lot_size END,
           face_value = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.face_value ELSE face_value END,
           status = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.status ELSE status END,
           board_name = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.board_name ELSE board_name END,
           decimals = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.decimals ELSE decimals END,
           security_name = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.security_name ELSE security_name END,
           remarks = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.remarks ELSE remarks END,
           market_code = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.market_code ELSE market_code END,
           instr_id = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.instr_id ELSE instr_id END,
           sector_id = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.sector_id ELSE sector_id END,
           min_step = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.min_step ELSE min_step END,
           prev_wa_price = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.prev_wa_price ELSE prev_wa_price END,
           face_unit = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.face_unit ELSE face_unit END,
           previous_date = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.previous_date ELSE previous_date END,
           issue_size = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.issue_size ELSE issue_size END,
           isin = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.isin ELSE isin END,
           lat_name = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.lat_name ELSE lat_name END,
           reg_number = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.reg_number ELSE reg_number END,
           previous_legal_close_price = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.previous_legal_close_price ELSE previous_legal_close_price END,
           previous_admitted_quote = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.previous_admitted_quote ELSE previous_admitted_quote END,
           currency_id = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.currency_id ELSE currency_id END,
           security_type = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.security_type ELSE security_type END,
           list_level = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.list_level ELSE list_level END,
           settle_date = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.settle_date ELSE settle_date END
           
