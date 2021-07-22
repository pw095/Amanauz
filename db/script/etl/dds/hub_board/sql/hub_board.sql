INSERT
  INTO hub_board
  (
    tech$load_id,
    tech$hash_key,
    tech$record_source,
    tech$load_dt,
    tech$last_seen_dt,
    board_id
  )
SELECT
       :tech$load_id      AS tech$load_id,
       sha1(board_id)     AS tech$hash_key,
       tech$record_source,
       tech$load_dt,
       tech$last_seen_dt,
       board_id
  FROM (SELECT
               MIN(tech$effective_dt) AS tech$load_dt,
               MAX(tech$effective_dt) AS tech$last_seen_dt,
               tech$record_source,
               board_id
          FROM (SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       'moex.com'        AS tech$record_source,
                       board_id
                  FROM (SELECT
                               tech$effective_dt,
                               tech$expiration_dt,
                               board_id
                          FROM src.security_boards
                         UNION ALL
                        SELECT
                               tech$effective_dt,
                               tech$expiration_dt,
                               board_id
                          FROM src.security_rate_bonds
                         UNION ALL
                        SELECT
                               tech$effective_dt,
                               tech$expiration_dt,
                               board_id
                          FROM src.security_rate_shares
                         UNION ALL
                        SELECT
                               tech$effective_dt,
                               tech$expiration_dt,
                               board_id
                          FROM src.security_daily_info_bonds
                         UNION ALL
                        SELECT
                               tech$effective_dt,
                               tech$expiration_dt,
                               board_id
                          FROM src.security_daily_info_shares)
                 WHERE board_id IS NOT NULL
                   AND board_id != ""
                 UNION ALL
                SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       'master_data'     AS tech$record_source,
                       board_id
                  FROM src.default_data_board)
         WHERE tech$effective_dt >= :tech$effective_dt
           AND tech$expiration_dt = '2999-12-31'
         GROUP BY
                  tech$record_source,
                  board_id)
 WHERE 1 = 1
ON CONFLICT(tech$hash_key)
DO UPDATE
      SET tech$last_seen_dt = excluded.tech$last_seen_dt,
          tech$load_id = excluded.tech$load_id
