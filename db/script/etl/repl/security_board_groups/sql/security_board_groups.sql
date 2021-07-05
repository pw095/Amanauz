INSERT
  INTO security_board_groups
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$hash_value,
    id,
    trade_engine_id,
    trade_engine_name,
    trade_engine_title,
    market_id,
    market_name,
    name,
    title,
    is_default,
    board_group_id,
    is_traded
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
       market_id,
       market_name,
       name,
       title,
       is_default,
       board_group_id,
       is_traded
  FROM (SELECT
               tech$effective_dt,
               tech$expiration_dt,
               tech$hash_value,
               id,
               trade_engine_id,
               trade_engine_name,
               trade_engine_title,
               market_id,
               market_name,
               name,
               title,
               is_default,
               board_group_id,
               is_traded
          FROM tech$security_board_groups src
         WHERE NOT EXISTS(SELECT
                                 NULL
                            FROM security_board_groups sat
                           WHERE
                                 sat.board_group_id = src.board_group_id
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
               trade_engine_id,
               trade_engine_name,
               trade_engine_title,
               market_id,
               market_name,
               name,
               title,
               is_default,
               board_group_id,
               is_traded
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
                       market_id,
                       market_name,
                       name,
                       title,
                       is_default,
                       board_group_id,
                       is_traded,
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
                               src.trade_engine_id,
                               src.trade_engine_name,
                               src.trade_engine_title,
                               src.market_id,
                               src.market_name,
                               src.name,
                               src.title,
                               src.is_default,
                               src.board_group_id,
                               src.is_traded,
                               FIRST_VALUE(CASE
                                                WHEN src.tech$hash_value != sat.tech$hash_value THEN
                                                    'NON_EQUAL'
                                                ELSE
                                                    'EQUAL'
                                           END) OVER (wnd) AS fv_equal_flag,
                               ROW_NUMBER() OVER (wnd)     AS rn
                          FROM tech$security_board_groups src
                               JOIN
                               security_board_groups sat
                                   ON
                                      sat.board_group_id = src.board_group_id
                                  AND sat.tech$effective_dt < src.tech$effective_dt
                                  AND sat.tech$expiration_dt = '2999-12-31'
                        WINDOW wnd AS (PARTITION BY src.board_group_id
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
 ON CONFLICT(board_group_id, tech$effective_dt)
 DO UPDATE
       SET tech$expiration_dt = excluded.tech$expiration_dt,
           tech$load_id = excluded.tech$load_id
