INSERT
  INTO sat_currency_rate_bus
  (
    tech$load_id,
    crnc_code,
    tech$effective_dt,
    tech$expiration_dt,
    tech$record_source,
    tech$hash_value,
    rate
  )
SELECT
       :tech$load_id       AS tech$load_id,
       crnc_code,
       tech$effective_dt,
       tech$expiration_dt,
       tech$record_source,
       tech$hash_value,
       rate
  FROM (SELECT
               crnc_code,
               tech$effective_dt,
               tech$expiration_dt,
               tech$record_source,
               tech$hash_value,
               rate
          FROM tech$sat_currency_rate_bus src
         WHERE NOT EXISTS(SELECT
                                 NULL
                            FROM sat_currency_rate_bus sat
                           WHERE sat.crnc_code = src.crnc_code
                             AND sat.tech$expiration_dt = '2999-12-31')
         UNION ALL
        SELECT
               crnc_code,
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
               rate
          FROM (SELECT
                       crnc_code,
                       tech$effective_dt,
                       tech$expiration_dt,
                       tech$sat$effective_dt,
                       tech$sat$expiration_dt,
                       tech$record_source,
                       tech$hash_value,
                       rate,
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
                               src.crnc_code,
                               src.tech$effective_dt,
                               src.tech$expiration_dt,
                               sat.tech$effective_dt                 AS tech$sat$effective_dt,
                               DATE(src.tech$effective_dt, '-1 DAY') AS tech$sat$expiration_dt,
                               src.tech$record_source,
                               src.tech$hash_value,
                               src.rate,
                               FIRST_VALUE(CASE
                                                WHEN src.tech$hash_value != sat.tech$hash_value THEN
                                                    'NON_EQUAL'
                                                ELSE
                                                    'EQUAL'
                                           END) OVER (wnd) AS fv_equal_flag,
                               ROW_NUMBER() OVER (wnd)     AS rn
                          FROM tech$sat_currency_rate_bus src
                               JOIN
                               sat_currency_rate_bus sat
                                   ON sat.crnc_code = src.crnc_code
                                  AND sat.tech$effective_dt <= src.tech$effective_dt
                                  AND sat.tech$expiration_dt = '2999-12-31'
                        WINDOW wnd AS (PARTITION BY src.crnc_code
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
ON CONFLICT(crnc_code, tech$effective_dt)
   DO UPDATE
         SET tech$expiration_dt = excluded.tech$expiration_dt,
             tech$load_id       = excluded.tech$load_id
