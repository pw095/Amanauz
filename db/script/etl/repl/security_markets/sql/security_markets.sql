INSERT
  INTO security_markets
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$hash_value,
    id,
    trade_engine_id,
    trade_engine_name,
    trade_engine_title,
    market_name,
    market_title,
    market_id,
    market_place
  )
SELECT
       :tech$load_id       AS tech$load_id,
       tech$effective_dt,
       tech$expiration_dt,
       tech$hash_value,
       id,
       trade_engine_id,
       trade_engine_name,
       trade_engine_title,
       market_name,
       market_title,
       market_id,
       market_place
  FROM (SELECT
               tech$effective_dt,
               tech$expiration_dt,
               tech$hash_value,
               id,
               trade_engine_id,
               trade_engine_name,
               trade_engine_title,
               market_name,
               market_title,
               market_id,
               market_place
          FROM tech$security_markets src
         WHERE NOT EXISTS(SELECT
                                 NULL
                            FROM security_markets sat
                           WHERE
                                 sat.market_name = src.market_name
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
               id,
               trade_engine_id,
               trade_engine_name,
               trade_engine_title,
               market_name,
               market_title,
               market_id,
               market_place
          FROM (SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       tech$sat$effective_dt,
                       tech$sat$expiration_dt,
                       tech$hash_value,
                       id,
                       trade_engine_id,
                       trade_engine_name,
                       trade_engine_title,
                       market_name,
                       market_title,
                       market_id,
                       market_place,
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
                               src.id,
                               src.trade_engine_id,
                               src.trade_engine_name,
                               src.trade_engine_title,
                               src.market_name,
                               src.market_title,
                               src.market_id,
                               src.market_place,
                               FIRST_VALUE(CASE
                                                WHEN src.tech$hash_value != sat.tech$hash_value THEN
                                                    'NON_EQUAL'
                                                ELSE
                                                    'EQUAL'
                                           END) OVER (wnd) AS fv_equal_flag,
                               ROW_NUMBER() OVER (wnd)     AS rn
                          FROM tech$security_markets src
                               JOIN
                               security_markets sat
                                   ON
                                      sat.market_name = src.market_name
                                  AND sat.tech$effective_dt <= src.tech$effective_dt
                                  AND sat.tech$expiration_dt = '2999-12-31'
                        WINDOW wnd AS (PARTITION BY src.market_name
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
 ON CONFLICT(market_name, tech$effective_dt)
 DO UPDATE
       SET tech$expiration_dt = excluded.tech$expiration_dt,
           tech$load_id = excluded.tech$load_id
