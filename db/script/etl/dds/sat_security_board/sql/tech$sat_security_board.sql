INSERT
  INTO tech$sat_security_board
  (
    tech$load_id,
    tech$hash_key,
    tech$effective_dt,
    tech$expiration_dt,
    tech$record_source,
    tech$hash_value,
    lot_size
  )
WITH
     w_pre AS
     (
         SELECT
                tech$effective_dt,
                hash_value,
                security_id,
                board_id,
                lot_size
           FROM (SELECT
                        tech$effective_dt,
                        hash_value,
                        LAG(hash_value) OVER wnd AS lag_hash_value,
                        security_id,
                        board_id,
                        lot_size
                   FROM (SELECT
                                tech$effective_dt,
                                sha1(concat_value) AS hash_value,
                                security_id,
                                board_id,
                                lot_size
                           FROM (SELECT
                                        tech$effective_dt,
                                        tech$expiration_dt,
                                        IFNULL(security_id, 'UNKNOWN') AS security_id,
                                        IFNULL(board_id,    'UNKNOWN') AS board_id,
                                        '_' || IFNULL(CAST(lot_size AS TEXT),  '!@#$%^&*') || '_' AS concat_value,
                                        lot_size
                                   FROM (SELECT
                                                tech$effective_dt,
                                                tech$expiration_dt,
                                                security_id,
                                                board_id,
                                                lot_size
                                           FROM src.security_daily_info_shares
                                          UNION ALL
                                         SELECT
                                                tech$effective_dt,
                                                tech$expiration_dt,
                                                security_id,
                                                board_id,
                                                lot_size
                                           FROM src.security_daily_info_bonds)
                                  WHERE NOT (security_id IS NULL AND board_id IS NULL)
                                    AND tech$effective_dt >= :tech$effective_dt))
                 WINDOW wnd AS (PARTITION BY security_id,
                                             board_id
                                    ORDER BY tech$effective_dt))
          WHERE hash_value != lag_hash_value
             OR lag_hash_value IS NULL
     )
SELECT
       :tech$load_id                                                           AS tech$load_id,
       lnk_sb.tech$hash_key,
       pre.tech$effective_dt,
       LEAD(DATE(pre.tech$effective_dt, '-1 DAY'), 1, '2999-12-31') OVER (wnd) AS tech$expiration_dt,
       'moex.com'                                                              AS tech$record_source,
       pre.hash_value                                                          AS tech$hash_value,
       pre.lot_size
  FROM w_pre pre
       JOIN
       hub_security security
           ON security.security_id = pre.security_id
       JOIN
       hub_board board
           ON board.board_id = pre.board_id
       JOIN
       lnk_security_board lnk_sb
           ON lnk_sb.security_hash_key = security.tech$hash_key
          AND lnk_sb.board_hash_key = board.tech$hash_key
WINDOW wnd AS (PARTITION BY lnk_sb.tech$hash_key
                   ORDER BY pre.tech$effective_dt)
