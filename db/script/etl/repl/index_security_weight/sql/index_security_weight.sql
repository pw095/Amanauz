INSERT
  INTO index_security_weight
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$hash_value,
    index_id,
    trade_date,
    ticker,
    short_name,
    security_id,
    weight,
    trading_session
  )
SELECT
       :tech$load_id       AS tech$load_id,
       tech$effective_dt,
       tech$expiration_dt,
       tech$hash_value,
       index_id,
       trade_date,
       ticker,
       short_name,
       security_id,
       weight,
       trading_session
  FROM (SELECT
               tech$effective_dt,
               tech$expiration_dt,
               tech$hash_value,
               index_id,
               trade_date,
               ticker,
               short_name,
               security_id,
               weight,
               trading_session
          FROM tech$index_security_weight src
         WHERE NOT EXISTS(SELECT
                                 NULL
                            FROM index_security_weight sat
                           WHERE
                                 sat.index_id = src.index_id
                             AND sat.trade_date = src.trade_date
                             AND sat.security_id = src.security_id
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
               index_id,
               trade_date,
               ticker,
               short_name,
               security_id,
               weight,
               trading_session
          FROM (SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       tech$sat$effective_dt,
                       tech$sat$expiration_dt,
                       tech$hash_value,
                       index_id,
                       trade_date,
                       ticker,
                       short_name,
                       security_id,
                       weight,
                       trading_session,
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
                               src.index_id,
                               src.trade_date,
                               src.ticker,
                               src.short_name,
                               src.security_id,
                               src.weight,
                               src.trading_session,
                               FIRST_VALUE(CASE
                                                WHEN src.tech$hash_value != sat.tech$hash_value THEN
                                                    'NON_EQUAL'
                                                ELSE
                                                    'EQUAL'
                                           END) OVER (wnd) AS fv_equal_flag,
                               ROW_NUMBER() OVER (wnd)     AS rn
                          FROM tech$index_security_weight src
                               JOIN
                               index_security_weight sat
                                   ON
                                      sat.index_id = src.index_id
                                  AND sat.trade_date = src.trade_date
                                  AND sat.security_id = src.security_id
                                  AND sat.tech$effective_dt <= src.tech$effective_dt
                                  AND sat.tech$expiration_dt = '2999-12-31'
                        WINDOW wnd AS (PARTITION BY src.index_id,
                                                    src.trade_date,
                                                    src.security_id
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
 ON CONFLICT(index_id, trade_date, security_id, tech$effective_dt)
 DO UPDATE
       SET tech$expiration_dt = excluded.tech$expiration_dt,
           tech$load_id = excluded.tech$load_id,
           ticker = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.ticker ELSE ticker END,
           short_name = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.short_name ELSE short_name END,
           weight = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.weight ELSE weight END,
           trading_session = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.trading_session ELSE trading_session END
           
