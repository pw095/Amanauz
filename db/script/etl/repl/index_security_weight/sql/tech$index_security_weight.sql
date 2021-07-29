INSERT
  INTO tech$index_security_weight
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$hash_value,
    index_id,
    trade_date,
    ticker,
    short_name,
    security_id,
    weight,
    trading_session
  )
WITH
     w_raw AS
     (
         SELECT
                tech$load_dt,
                sha1(concat_value) AS hash_value,
                index_id,
                trade_date,
                ticker,
                short_name,
                security_id,
                weight,
                trading_session
           FROM (SELECT
                        tech$load_dt,
                        '_' || IFNULL(CAST(ticker          AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(short_name      AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(weight          AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(trading_session AS TEXT), '!@#\$%^&*') || '_' AS concat_value,
                        index_id,
                        trade_date,
                        ticker,
                        short_name,
                        security_id,
                        weight,
                        trading_session
                   FROM (SELECT
                                tech$load_dt,
                                ROW_NUMBER() OVER (wnd) AS rn,
                                index_id,
                                trade_date,
                                ticker,
                                short_name,
                                security_id,
                                weight,
                                trading_session
                           FROM (SELECT
                                        DATE(tech$load_dttm) AS tech$load_dt,
                                        tech$load_dttm       AS tech$load_dttm,
                                        index_id,
                                        trade_date,
                                        ticker,
                                        short_name,
                                        security_id,
                                        weight,
                                        trading_session
                                   FROM src.index_security_weight
                                  WHERE tech$load_dttm >= :tech$load_dttm)
                         WINDOW wnd AS (PARTITION BY
                                                     index_id,
                                                     trade_date,
                                                     security_id,
                                                     tech$load_dt
                                            ORDER BY tech$load_dttm DESC))
                  WHERE rn = 1)
     )
SELECT
       :tech$load_id                                                  AS tech$load_id,
       tech$load_dt                                                   AS tech$effective_dt,
       LEAD(DATE(tech$load_dt, '-1 DAY'), 1, '2999-12-31') OVER (wnd) AS tech$expiration_dt,
       hash_value                                                     AS tech$hash_value,
       index_id,
       trade_date,
       ticker,
       short_name,
       security_id,
       weight,
       trading_session
  FROM (SELECT
               tech$load_dt,
               hash_value,
               index_id,
               trade_date,
               ticker,
               short_name,
               security_id,
               weight,
               trading_session
          FROM (SELECT
                       tech$load_dt,
                       hash_value,
                       LAG(hash_value) OVER (wnd) AS lag_hash_value,
                       index_id,
                       trade_date,
                       ticker,
                       short_name,
                       security_id,
                       weight,
                       trading_session
                  FROM w_raw
                WINDOW wnd AS (PARTITION BY
                                            index_id,
                                            trade_date,
                                            security_id
                                   ORDER BY tech$load_dt))
         WHERE hash_value != lag_hash_value
            OR lag_hash_value IS NULL)
WINDOW wnd AS (PARTITION BY
                            index_id,
                            trade_date,
                            security_id
                   ORDER BY tech$load_dt)
