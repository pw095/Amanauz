INSERT
  INTO security_emitent_map
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$hash_value,
    id,
    security_id,
    short_name,
    reg_number,
    security_name,
    isin,
    security_is_traded,
    emitent_id,
    emitent_title,
    emitent_inn,
    emitent_okpo,
    emitent_gos_reg,
    security_type,
    security_group,
    primary_board_id,
    market_price_board_id
  )
SELECT
       :tech$load_id       AS tech$load_id,
       tech$effective_dt,
       tech$expiration_dt,
       tech$hash_value,
       id,
       security_id,
       short_name,
       reg_number,
       security_name,
       isin,
       security_is_traded,
       emitent_id,
       emitent_title,
       emitent_inn,
       emitent_okpo,
       emitent_gos_reg,
       security_type,
       security_group,
       primary_board_id,
       market_price_board_id
  FROM (SELECT
               tech$effective_dt,
               tech$expiration_dt,
               tech$hash_value,
               id,
               security_id,
               short_name,
               reg_number,
               security_name,
               isin,
               security_is_traded,
               emitent_id,
               emitent_title,
               emitent_inn,
               emitent_okpo,
               emitent_gos_reg,
               security_type,
               security_group,
               primary_board_id,
               market_price_board_id
          FROM tech$security_emitent_map src
         WHERE NOT EXISTS(SELECT
                                 NULL
                            FROM security_emitent_map sat
                           WHERE
                                 sat.security_id = src.security_id
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
                        tech$sat$expiration_dt
               END AS tech$expiration_dt,
               tech$hash_value,
               id,
               security_id,
               short_name,
               reg_number,
               security_name,
               isin,
               security_is_traded,
               emitent_id,
               emitent_title,
               emitent_inn,
               emitent_okpo,
               emitent_gos_reg,
               security_type,
               security_group,
               primary_board_id,
               market_price_board_id
          FROM (SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       tech$sat$effective_dt,
                       tech$sat$expiration_dt,
                       tech$hash_value,
                       id,
                       security_id,
                       short_name,
                       reg_number,
                       security_name,
                       isin,
                       security_is_traded,
                       emitent_id,
                       emitent_title,
                       emitent_inn,
                       emitent_okpo,
                       emitent_gos_reg,
                       security_type,
                       security_group,
                       primary_board_id,
                       market_price_board_id,
                       CASE
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
                               src.id,
                               src.security_id,
                               src.short_name,
                               src.reg_number,
                               src.security_name,
                               src.isin,
                               src.security_is_traded,
                               src.emitent_id,
                               src.emitent_title,
                               src.emitent_inn,
                               src.emitent_okpo,
                               src.emitent_gos_reg,
                               src.security_type,
                               src.security_group,
                               src.primary_board_id,
                               src.market_price_board_id,
                               FIRST_VALUE(CASE
                                                WHEN src.tech$hash_value != sat.tech$hash_value THEN
                                                    'NON_EQUAL'
                                                ELSE
                                                    'EQUAL'
                                           END) OVER (wnd) AS fv_equal_flag,
                               ROW_NUMBER() OVER (wnd)     AS rn
                          FROM tech$security_emitent_map src
                               JOIN
                               security_emitent_map sat
                                   ON
                                      sat.security_id = src.security_id
                                  AND sat.tech$effective_dt < src.tech$effective_dt
                                  AND sat.tech$expiration_dt = '2999-12-31'
                        WINDOW wnd AS (PARTITION BY src.security_id
                                           ORDER BY src.tech$effective_dt))
                 WHERE rn = 1 AND fv_equal_flag = 'NON_EQUAL'
                    OR rn > 1) src
               CROSS JOIN
               (SELECT 'INSERT' AS flg
                 UNION ALL
                SELECT 'UPDATE' AS flg) mrg
    WHERE src.upsert_flg = 'UPSERT'
       OR src.upsert_flg = 'INSERT' AND mrg.flg = 'INSERT')
 WHERE 1 = 1
 ON CONFLICT(security_id, tech$effective_dt)
 DO UPDATE
       SET tech$expiration_dt = excluded.tech$expiration_dt
