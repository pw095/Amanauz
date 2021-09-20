INSERT
  INTO sat_security_rate
  (
    tech$load_id,
    tech$hash_key,
    tech$effective_dt,
    tech$expiration_dt,
    tech$record_source,
    tech$hash_value,
    trade_count,
    value,
    volume,
    price_open,
    price_close,
    price_low,
    price_high
  )
SELECT
       :tech$load_id AS tech$load_id,
       tech$hash_key,
       tech$effective_dt,
       tech$expiration_dt,
       tech$record_source,
       tech$hash_value,
       trade_count,
       value,
       volume,
       price_open,
       price_close,
       price_low,
       price_high
  FROM (SELECT
               tech$hash_key,
               tech$effective_dt,
               tech$expiration_dt,
               tech$record_source,
               tech$hash_value,
               trade_count,
               value,
               volume,
               price_open,
               price_close,
               price_low,
               price_high
          FROM tech$sat_security_rate src
         WHERE NOT EXISTS(SELECT
                                 NULL
                            FROM sat_security_rate sat
                           WHERE sat.tech$hash_key = src.tech$hash_key
                             AND sat.tech$expiration_dt = '2999-12-31')
         UNION ALL
        SELECT
               tech$hash_key,
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
               tech$record_source,
               tech$hash_value,
               trade_count,
               value,
               volume,
               price_open,
               price_close,
               price_low,
               price_high
          FROM (SELECT
                       tech$hash_key,
                       tech$effective_dt,
                       tech$expiration_dt,
                       tech$sat$effective_dt,
                       tech$sat$expiration_dt,
                       tech$record_source,
                       tech$hash_value,
                       trade_count,
                       value,
                       volume,
                       price_open,
                       price_close,
                       price_low,
                       price_high,
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
                               src.tech$hash_key,
                               src.tech$effective_dt,
                               src.tech$expiration_dt,
                               sat.tech$effective_dt                 AS tech$sat$effective_dt,
                               DATE(src.tech$effective_dt, '-1 DAY') AS tech$sat$expiration_dt,
                               src.tech$record_source,
                               src.tech$hash_value,
                               src.trade_count,
                               src.value,
                               src.volume,
                               src.price_open,
                               src.price_close,
                               src.price_low,
                               src.price_high,
                               FIRST_VALUE(CASE
                                                WHEN src.tech$hash_value != sat.tech$hash_value THEN
                                                    'NON_EQUAL'
                                                ELSE
                                                    'EQUAL'
                                           END) OVER (wnd) AS fv_equal_flag,
                               ROW_NUMBER() OVER (wnd)     AS rn
                          FROM tech$sat_security_rate src
                               JOIN
                               sat_security_rate sat
                                   ON sat.tech$hash_key = src.tech$hash_key
                                  AND sat.tech$effective_dt < src.tech$effective_dt
                                  AND sat.tech$expiration_dt = '2999-12-31'
                        WINDOW wnd AS (PARTITION BY src.tech$hash_key
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
 ON CONFLICT(tech$hash_key, tech$effective_dt)
 DO UPDATE
       SET tech$expiration_dt = excluded.tech$expiration_dt,
           tech$load_id       = excluded.tech$load_id,
           tech$hash_value    = CASE
                                     WHEN tech$expiration_dt = '2999-12-31'
                                      AND excluded.tech$expiration_dt = '2999-12-31' THEN
                                         excluded.tech$hash_value
                                     ELSE
                                         tech$hash_value
                                END,
           trade_count = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.trade_count ELSE trade_count END,
           value       = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.value       ELSE value       END,
           volume      = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.volume      ELSE volume      END,
           price_open  = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.price_open  ELSE price_open  END,
           price_close = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.price_close ELSE price_close END,
           price_low   = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.price_low   ELSE price_low   END,
           price_high  = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.price_high  ELSE price_high  END
