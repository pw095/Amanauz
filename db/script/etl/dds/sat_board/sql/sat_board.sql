INSERT
  INTO sat_board
  (
    tech$load_id,
    tech$hash_key,
    tech$effective_dt,
    tech$expiration_dt,
    tech$record_source,
    tech$hash_value,
    board_id,
    title,
    primary_flag
  )
SELECT
       :tech$load_id       AS tech$load_id,
       tech$hash_key,
       tech$effective_dt,
       tech$expiration_dt,
       tech$record_source,
       tech$hash_value,
       board_id,
       title,
       primary_flag
  FROM (SELECT
               tech$hash_key,
               tech$effective_dt,
               tech$expiration_dt,
               tech$record_source,
               tech$hash_value,
               board_id,
               title,
               primary_flag
          FROM tech$sat_board src
         WHERE NOT EXISTS(SELECT
                                 NULL
                            FROM sat_board sat
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
               board_id,
               title,
               primary_flag
          FROM (SELECT
                       tech$hash_key,
                       tech$effective_dt,
                       tech$expiration_dt,
                       tech$sat$effective_dt,
                       tech$sat$expiration_dt,
                       tech$record_source,
                       tech$hash_value,
                       board_id,
                       title,
                       primary_flag,
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
                               src.board_id,
                               src.title,
                               src.primary_flag,
                               FIRST_VALUE(CASE
                                                WHEN src.tech$hash_value != sat.tech$hash_value THEN
                                                    'NON_EQUAL'
                                                ELSE
                                                    'EQUAL'
                                           END) OVER (wnd) AS fv_equal_flag,
                               ROW_NUMBER() OVER (wnd)     AS rn
                          FROM tech$sat_board src
                               JOIN
                               sat_board sat
                                   ON sat.tech$hash_key = src.tech$hash_key
                                  AND sat.tech$effective_dt <= src.tech$effective_dt
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
           tech$load_id = excluded.tech$load_id,
           tech$hash_value = CASE
                                  WHEN tech$expiration_dt = '2999-12-31'
                                   AND excluded.tech$expiration_dt = '2999-12-31' THEN
                                      excluded.tech$hash_value
                                  ELSE
                                      tech$hash_value
                             END,
           board_id = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.board_id ELSE board_id END,
           title = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.title ELSE title END,
           primary_flag = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.primary_flag ELSE primary_flag END
