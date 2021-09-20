INSERT
  INTO emitent_fin_statement
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$last_seen_dt,
    tech$hash_value,
    emitent_name,
    currency,
    report_dt,
    fin_stmt_code,
    fin_stmt_name,
    value
  )
SELECT
       :tech$load_id       AS tech$load_id,
       tech$effective_dt,
       tech$expiration_dt,
       tech$last_seen_dt,
       tech$hash_value,
       emitent_name,
       currency,
       report_dt,
       fin_stmt_code,
       fin_stmt_name,
       value
  FROM (SELECT
               tech$effective_dt,
               tech$expiration_dt,
               tech$last_seen_dt,
               tech$hash_value,
               emitent_name,
               currency,
               report_dt,
               fin_stmt_code,
               fin_stmt_name,
               value
          FROM tech$emitent_fin_statement src
         WHERE NOT EXISTS(SELECT
                                 NULL
                            FROM emitent_fin_statement sat
                           WHERE
                                 sat.emitent_name = src.emitent_name
                             AND sat.report_dt = src.report_dt
                             AND sat.fin_stmt_code = src.fin_stmt_code
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
               emitent_name,
               currency,
               report_dt,
               fin_stmt_code,
               fin_stmt_name,
               value
          FROM (SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       tech$last_seen_dt,
                       tech$sat$effective_dt,
                       tech$sat$expiration_dt,
                       tech$hash_value,
                       emitent_name,
                       currency,
                       report_dt,
                       fin_stmt_code,
                       fin_stmt_name,
                       value,
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
                               src.emitent_name,
                               src.currency,
                               src.report_dt,
                               src.fin_stmt_code,
                               src.fin_stmt_name,
                               src.value,
                               FIRST_VALUE(CASE
                                                WHEN src.tech$hash_value != sat.tech$hash_value THEN
                                                    'NON_EQUAL'
                                                ELSE
                                                    'EQUAL'
                                           END) OVER (wnd) AS fv_equal_flag,
                               ROW_NUMBER() OVER (wnd)     AS rn
                          FROM tech$emitent_fin_statement src
                               JOIN
                               emitent_fin_statement sat
                                   ON
                                      sat.emitent_name = src.emitent_name
                                  AND sat.report_dt = src.report_dt
                                  AND sat.fin_stmt_code = src.fin_stmt_code
                                  AND sat.tech$effective_dt <= src.tech$effective_dt
                                  AND sat.tech$expiration_dt = '2999-12-31'
                        WINDOW wnd AS (PARTITION BY src.emitent_name,
                                                    src.report_dt,
                                                    src.fin_stmt_code
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
 ON CONFLICT(emitent_name, report_dt, fin_stmt_code, tech$effective_dt)
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
           currency = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.currency ELSE currency END,
           fin_stmt_name = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.fin_stmt_name ELSE fin_stmt_name END,
           value = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.value ELSE value END
