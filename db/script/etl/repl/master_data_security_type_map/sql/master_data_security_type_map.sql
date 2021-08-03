INSERT
  INTO master_data_security_type_map
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$last_seen_dt,
    tech$hash_value,
    table_name,
    type_id,
    type_name,
    security_type_id
  )
SELECT
       :tech$load_id       AS tech$load_id,
       tech$effective_dt,
       tech$expiration_dt,
       tech$last_seen_dt,
       tech$hash_value,
       table_name,
       type_id,
       type_name,
       security_type_id
  FROM (SELECT
               tech$effective_dt,
               tech$expiration_dt,
               tech$last_seen_dt,
               tech$hash_value,
               table_name,
               type_id,
               type_name,
               security_type_id
          FROM tech$master_data_security_type_map src
         WHERE NOT EXISTS(SELECT
                                 NULL
                            FROM master_data_security_type_map sat
                           WHERE
                                 sat.security_type_id = src.security_type_id
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
               table_name,
               type_id,
               type_name,
               security_type_id
          FROM (SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       tech$last_seen_dt,
                       tech$sat$effective_dt,
                       tech$sat$expiration_dt,
                       tech$hash_value,
                       table_name,
                       type_id,
                       type_name,
                       security_type_id,
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
                               src.table_name,
                               src.type_id,
                               src.type_name,
                               src.security_type_id,
                               FIRST_VALUE(CASE
                                                WHEN src.tech$hash_value != sat.tech$hash_value THEN
                                                    'NON_EQUAL'
                                                ELSE
                                                    'EQUAL'
                                           END) OVER (wnd) AS fv_equal_flag,
                               ROW_NUMBER() OVER (wnd)     AS rn
                          FROM tech$master_data_security_type_map src
                               JOIN
                               master_data_security_type_map sat
                                   ON
                                      sat.security_type_id = src.security_type_id
                                  AND sat.tech$effective_dt <= src.tech$effective_dt
                                  AND sat.tech$expiration_dt = '2999-12-31'
                        WINDOW wnd AS (PARTITION BY src.security_type_id
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
 ON CONFLICT(security_type_id, tech$effective_dt)
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
                             END,
           table_name = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.table_name ELSE table_name END,
           type_id = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.type_id ELSE type_id END,
           type_name = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.type_name ELSE type_name END
