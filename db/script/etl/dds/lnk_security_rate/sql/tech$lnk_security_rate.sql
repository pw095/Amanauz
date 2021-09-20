INSERT
  INTO tech$lnk_security_rate
  (
    tech$load_id,
    tech$hash_key,
    tech$load_dt,
    tech$last_seen_dt,
    tech$record_source,
    trade_dt,
    security_hash_key,
    board_hash_key
  )
WITH
     w_pre AS
     (
         SELECT
                tech$hash_key,
                MIN(tech$effective_dt) AS tech$load_dt,
                MIN(trade_dt)          AS trade_dt,
                MIN(security_id)       AS security_id,
                MIN(board_id)          AS board_id
           FROM (SELECT
                        sha1(concat_value) AS tech$hash_key,
                        tech$effective_dt,
                        trade_dt,
                        security_id,
                        board_id
                   FROM (SELECT
                                tech$effective_dt,
                                '_' || IFNULL(CAST(trade_dt AS TEXT), '!@#$%^&*') ||
                                '_' || IFNULL(CAST(security_id AS TEXT), '!@#$%^&*') ||
                                '_' || IFNULL(CAST(board_id    AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                                trade_dt,
                                security_id,
                                board_id
                           FROM (SELECT
                                        tech$effective_dt,
                                        tech$expiration_dt,
                                        trade_date                     AS trade_dt,
                                        IFNULL(security_id, 'UNKNOWN') AS security_id,
                                        IFNULL(board_id,    'UNKNOWN') AS board_id
                                   FROM (SELECT
                                                tech$effective_dt,
                                                tech$expiration_dt,
                                                trade_date,
                                                security_id,
                                                board_id
                                           FROM src.security_rate_shares
                                          UNION ALL
                                         SELECT
                                                tech$effective_dt,
                                                tech$expiration_dt,
                                                trade_date,
                                                security_id,
                                                board_id
                                           FROM src.security_rate_bonds)
                                  WHERE NOT (security_id IS NULL AND board_id IS NULL)
                                    AND (   1 = 1
                                         OR tech$effective_dt >= :tech$effective_dt))))
                  GROUP BY
                           tech$hash_key
     )
SELECT
       :tech$load_id          AS tech$load_id,
       pre.tech$hash_key,
       'moex.com'             AS tech$record_source,
       pre.tech$load_dt,
       pre.trade_dt,
       security.tech$hash_key AS security_hash_key,
       board.tech$hash_key    AS board_hash_key
  FROM w_pre pre
       JOIN
       hub_security security
           ON security.security_id = pre.security_id
       JOIN
       hub_board board
           ON board.board_id = pre.board_id
