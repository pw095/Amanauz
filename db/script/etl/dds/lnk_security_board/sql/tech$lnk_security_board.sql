INSERT
  INTO tech$lnk_security_board
  (
    tech$load_id,
    tech$hash_key,
    tech$load_dt,
    tech$last_seen_dt,
    tech$record_source,
    security_hash_key,
    board_hash_key
  )
WITH
     w_pre AS
     (
         SELECT
                tech$hash_key,
                MIN(tech$effective_dt) AS tech$load_dt,
                MAX(tech$last_seen_dt) AS tech$last_seen_dt,
                MIN(security_id)       AS security_id,
                MIN(board_id)          AS board_id
           FROM (SELECT
                        sha1(concat_value) AS tech$hash_key,
                        tech$effective_dt,
                        tech$last_seen_dt,
                        security_id,
                        board_id
                   FROM (SELECT
                                tech$effective_dt,
                                tech$last_seen_dt,
                                '_' || IFNULL(CAST(security_id AS TEXT), '!@#$%^&*') ||
                                '_' || IFNULL(CAST(board_id    AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                                security_id,
                                board_id
                           FROM (SELECT
                                        tech$effective_dt,
                                        tech$expiration_dt,
                                        tech$last_seen_dt,
                                        IFNULL(security_id, 'UNKNOWN') AS security_id,
                                        IFNULL(board_id,    'UNKNOWN') AS board_id
                                   FROM (SELECT
                                                tech$effective_dt,
                                                tech$last_seen_dt,
                                                NULLIF(security_id, '') AS security_id,
                                                NULLIF(board_id,    '') AS board_id
                                           FROM (SELECT
                                                        tech$effective_dt,
                                                        tech$last_seen_dt,
                                                        security_id,
                                                        board_id
                                                   FROM src.security_daily_info_shares
                                                  UNION ALL
                                                 SELECT
                                                        tech$effective_dt,
                                                        tech$last_seen_dt,
                                                        security_id,
                                                        board_id
                                                   FROM src.security_daily_info_bonds)
                                          WHERE 1 = 1
                                             OR tech$effective_dt >= :tech$effective_dt)
                                  WHERE NOT (security_id IS NULL AND board_id IS NULL)
)))
                  GROUP BY
                           tech$hash_key
     )
SELECT
       :tech$load_id          AS tech$load_id,
       pre.tech$hash_key,
       pre.tech$load_dt,
       pre.tech$last_seen_dt,
       'moex.com'             AS tech$record_source,
       security.tech$hash_key AS security_hash_key,
       board.tech$hash_key    AS board_hash_key
  FROM w_pre pre
       JOIN
       hub_security security
           ON security.security_id = pre.security_id
       JOIN
       hub_board board
           ON board.board_id = pre.board_id
