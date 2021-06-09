INSERT
  INTO repl.tech$index_security_weight
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
                load_dt,
                sha1(concat_value) AS hash_value,
                index_id,
                trade_date,
                security_id,
                ticker,
                short_name,
                weight,
                trading_session
           FROM (SELECT
                        load_dt,
                        '_' || IFNULL(CAST(ticker          AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(short_name      AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(weight          AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(trading_session AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                        index_id,
                        trade_date,
                        security_id,
                        ticker,
                        short_name,
                        weight,
                        trading_session
                   FROM (SELECT
                                load_dt,
                                ROW_NUMBER() OVER (PARTITION BY index_id,
                                                                trade_date,
                                                                security_id,
                                                                load_dt
                                                       ORDER BY load_dttm DESC) AS rn,
                                index_id,
                                trade_date,
                                security_id,
                                ticker,
                                short_name,
                                weight,
                                trading_session
                           FROM (SELECT
                                        sha1(CAST(index_id    AS TEXT) ||
                                             CAST(trade_date  AS TEXT) ||
                                             CAST(security_id AS TEXT))   AS hash_key,
                                        DATE(tech$load_dttm)              AS load_dt,
                                        tech$load_dttm                    AS load_dttm,
                                        index_id,
                                        trade_date,
                                        security_id,
                                        ticker,
                                        short_name,
                                        weight,
                                        trading_session
                                   FROM index_security_weight
                                  WHERE tech$load_id >= :tech$load_id))
                  WHERE rn = 1)
     )
SELECT
       1                                                                             AS tech$load_id,
       load_dt                                                                       AS tech$effective_dt,
       LEAD(DATE(load_dt, '-1 DAY'), 1, '2999-12-31') OVER (PARTITION BY index_id,
                                                                         trade_date,
                                                                         security_id
                                                                ORDER BY load_dt)    AS tech$expiration_dt,
       hash_value                                                                    AS tech$hash_value,
       index_id,
       trade_date,
       security_id,
       ticker,
       short_name,
       weight,
       trading_session
  FROM (SELECT
               load_dt,
               hash_value,
               index_id,
               trade_date,
               security_id,
               ticker,
               short_name,
               weight,
               trading_session
          FROM (SELECT
                       load_dt,
                       hash_value,
                       LAG(hash_value) OVER (PARTITION BY index_id,
                                                          trade_date,
                                                          security_id
                                                 ORDER BY load_dt) AS lag_hash_value,
                       index_id,
                       trade_date,
                       security_id,
                       ticker,
                       short_name,
                       weight,
                       trading_session
                  FROM w_raw)
         WHERE hash_value != lag_hash_value
            OR lag_hash_value IS NULL)
