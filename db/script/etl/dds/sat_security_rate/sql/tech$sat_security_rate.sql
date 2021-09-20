INSERT
  INTO tech$sat_security_rate
  (
    tech$load_id,
    tech$hash_key,
    tech$effective_dt,
    tech$expiration_dt,
    tech$record_source,
    tech$hash_value,
    trade_count,
    value,
    volume,
    price_open,
    price_close,
    price_low,
    price_high
  )
WITH
     w_pre AS
     (
         SELECT
                tech$effective_dt,
                trade_date AS trade_dt,
                security_id,
                board_id,
                trade_count,
                value,
                volume,
                price_open,
                price_close,
                price_low,
                price_high,
                ROW_NUMBER() OVER (wnd) AS rn
           FROM (SELECT
                        tech$effective_dt,
                        hash_value,
                        LAG(hash_value) OVER (wnd) AS lag_hash_value,
                        trade_date,
                        security_id,
                        board_id,
                        trade_count,
                        value,
                        volume,
                        price_open,
                        price_close,
                        price_low,
                        price_high
                   FROM (SELECT
                                tech$effective_dt,
                                sha1(concat_value) AS hash_value,
                                trade_date,
                                security_id,
                                board_id,
                                trade_count,
                                value,
                                volume,
                                price_open,
                                price_close,
                                price_low,
                                price_high
                           FROM (SELECT
                                        tech$effective_dt,
                                        '_' || IFNULL(CAST(trade_count AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(value        AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(volume       AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(price_open   AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(price_close  AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(price_low    AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(price_high   AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                                        trade_date,
                                        security_id,
                                        board_id,
                                        trade_count,
                                        value,
                                        volume,
                                        price_open,
                                        price_close,
                                        price_low,
                                        price_high
                                   FROM (SELECT
                                                tech$effective_dt,
                                                trade_date,
                                                security_id,
                                                board_id,
                                                trade_count,
                                                value,
                                                volume,
                                                price_open,
                                                price_close,
                                                price_low,
                                                price_high
                                           FROM (SELECT
                                                        tech$effective_dt,
                                                        trade_date,
                                                        security_id,
                                                        board_id,
                                                        trade_count,
                                                        value,
                                                        volume,
                                                        price_open,
                                                        price_close,
                                                        price_low,
                                                        price_high
                                                   FROM src.security_rate_shares
                                                  UNION ALL
                                                 SELECT
                                                        tech$effective_dt,
                                                        trade_date,
                                                        security_id,
                                                        board_id,
                                                        trade_count,
                                                        value,
                                                        volume,
                                                        price_open,
                                                        price_close,
                                                        price_low,
                                                        price_high
                                                   FROM src.security_rate_bonds)
                                          WHERE tech$effective_dt >= :tech$effective_dt)))
                 WINDOW wnd AS (PARTITION BY trade_date,
                                             security_id,
                                             board_id
                                    ORDER BY tech$effective_dt))
          WHERE hash_value != lag_hash_value
             OR lag_hash_value IS NULL
         WINDOW wnd AS (PARTITION BY trade_date,
                                     security_id,
                                     board_id,
                                     tech$effective_dt
                            ORDER BY face_value DESC)
     )
SELECT
       :tech$load_id                                                           AS tech$load_id,
       lnk.tech$hash_key,
       pre.tech$effective_dt,
       LEAD(DATE(pre.tech$effective_dt, '-1 DAY'), 1, '2999-12-31') OVER (wnd) AS tech$expiration_dt,
       lnk.tech$record_source,
       pre.hash_value                                                          AS tech$hash_value,
       pre.trade_count,
       pre.value,
       pre.volume,
       pre.price_open,
       pre.price_close,
       pre.price_low,
       pre.price_high
  FROM w_pre pre
       JOIN
       hub_security security
           ON security.security_id = pre.security_id
       JOIN
       hub_board board
           ON board.board_id = pre.board_id
       JOIN
       lnk_security_rate lnk
           ON lnk.trade_dt = pre.trade_date
          AND lnk.security_id = pre.security_id
          AND lnk.board_id = pre.board_id
          AND lnk.tech$record_source = 'moex.com'
 WHERE pre.rn = 1 -- Брать наибольший номинал, если у ЦБ он отличается в различных режимах торгов
                  -- Пока это относится только к индексируемым облигациям на этапе размещения
WINDOW wnd AS (PARTITION BY lnk.tech$hash_key
                   ORDER BY pre.tech$effective_dt)