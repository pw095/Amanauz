INSERT
  INTO master_data_emitent
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$hash_value,
    full_name,
    short_name,
    reg_date,
    ogrn,
    inn
  )
SELECT
       :tech$load_id       AS tech$load_id,
       tech$effective_dt,
       tech$expiration_dt,
       tech$hash_value,
       full_name,
       short_name,
       reg_date,
       ogrn,
       inn
  FROM (SELECT
               tech$effective_dt,
               tech$expiration_dt,
               tech$hash_value,
               full_name,
               short_name,
               reg_date,
               ogrn,
               inn
          FROM tech$master_data_emitent src
         WHERE NOT EXISTS(SELECT
                                 NULL
                            FROM master_data_emitent sat
                           WHERE
                                 sat.short_name = src.short_name
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
               full_name,
               short_name,
               reg_date,
               ogrn,
               inn
          FROM (SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       tech$sat$effective_dt,
                       tech$sat$expiration_dt,
                       tech$hash_value,
                       full_name,
                       short_name,
                       reg_date,
                       ogrn,
                       inn,
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
                               src.full_name,
                               src.short_name,
                               src.reg_date,
                               src.ogrn,
                               src.inn,
                               FIRST_VALUE(CASE
                                                WHEN src.tech$hash_value != sat.tech$hash_value THEN
                                                    'NON_EQUAL'
                                                ELSE
                                                    'EQUAL'
                                           END) OVER (wnd) AS fv_equal_flag,
                               ROW_NUMBER() OVER (wnd)     AS rn
                          FROM tech$master_data_emitent src
                               JOIN
                               master_data_emitent sat
                                   ON
                                      sat.short_name = src.short_name
                                  AND sat.tech$effective_dt < src.tech$effective_dt
                                  AND sat.tech$expiration_dt = '2999-12-31'
                        WINDOW wnd AS (PARTITION BY src.short_name
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
 ON CONFLICT(short_name, tech$effective_dt)
 DO UPDATE
       SET tech$expiration_dt = excluded.tech$expiration_dt
