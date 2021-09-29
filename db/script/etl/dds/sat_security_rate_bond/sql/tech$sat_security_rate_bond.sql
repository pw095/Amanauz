INSERT
  INTO tech$sat_security_rate_bond
  (
    tech$load_id,
    tech$hash_key,
    tech$effective_dt,
    tech$expiration_dt,
    tech$record_source,
    tech$hash_value,
    accrued_interest,
    yield,
    duration
  )
WITH
     w_pre AS
     (
         SELECT
                tech$effective_dt,
                hash_value,
                trade_dt,
                security_id,
                board_id,
                accrued_interest,
                yield,
                duration
           FROM (SELECT
                        tech$effective_dt,
                        hash_value,
                        LAG(hash_value) OVER (wnd) AS lag_hash_value,
                        trade_dt,
                        security_id,
                        board_id,
                        accrued_interest,
                        yield,
                        duration
                   FROM (SELECT
                                tech$effective_dt,
                                sha1(concat_value) AS hash_value,
                                trade_dt,
                                security_id,
                                board_id,
                                accrued_interest,
                                yield,
                                duration
                           FROM (SELECT
                                        tech$effective_dt,
                                        '_' || IFNULL(CAST(accrued_interest AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(yield            AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(duration         AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                                        trade_dt,
                                        security_id,
                                        board_id,
                                        accrued_interest,
                                        yield,
                                        duration
                                   FROM (SELECT
                                                tech$effective_dt,
                                                trade_date        AS trade_dt,
                                                security_id,
                                                board_id,
                                                acc_int           AS accrued_interest,
                                                yield_close       AS yield,
                                                duration
                                           FROM src.security_rate_bonds
                                          WHERE tech$effective_dt >= :tech$effective_dt)))
                 WINDOW wnd AS (PARTITION BY trade_dt,
                                             security_id,
                                             board_id
                                    ORDER BY tech$effective_dt))
          WHERE hash_value != lag_hash_value
             OR lag_hash_value IS NULL
     )
SELECT
       :tech$load_id                                                           AS tech$load_id,
       lnk.tech$hash_key,
       pre.tech$effective_dt,
       LEAD(DATE(pre.tech$effective_dt, '-1 DAY'), 1, '2999-12-31') OVER (wnd) AS tech$expiration_dt,
       lnk.tech$record_source,
       pre.hash_value                                                          AS tech$hash_value,
       pre.accrued_interest,
       pre.yield,
       pre.duration
  FROM w_pre pre
       JOIN
       hub_security security
           ON security.security_id = pre.security_id
       JOIN
       hub_board board
           ON board.board_id = pre.board_id
       JOIN
       lnk_security_rate lnk
           ON lnk.security_hash_key = security.tech$hash_key
          AND lnk.board_hash_key = board.tech$hash_key
          AND lnk.trade_dt = pre.trade_dt
WINDOW wnd AS (PARTITION BY lnk.tech$hash_key
                   ORDER BY pre.tech$effective_dt)
