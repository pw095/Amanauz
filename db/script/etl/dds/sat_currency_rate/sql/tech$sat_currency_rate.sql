INSERT
  INTO tech$sat_currency_rate
  (
    tech$load_id,
    tech$hash_key,
    tech$effective_dt,
    tech$expiration_dt,
    tech$record_source,
    tech$hash_value,
    rate
  )
WITH
     w_pre AS
     (
         SELECT
                tech$effective_dt,
                hash_value,
                trade_dt,
                crnc_code,
                rate
           FROM (SELECT
                        tech$effective_dt,
                        hash_value,
                        LAG(hash_value) OVER wnd AS lag_hash_value,
                        trade_dt,
                        crnc_code,
                        rate
                   FROM (SELECT
                                tech$effective_dt,
                                sha1(concat_value) AS hash_value,
                                trade_dt,
                                crnc_code,
                                rate
                           FROM (SELECT
                                        rate.tech$effective_dt,
                                        rate.trade_date                                            AS trade_dt,
                                        IFNULL(dict.iso_char_code, 'UNKNOWN')                      AS crnc_code,
                                        '_' || IFNULL(CAST(rate.value AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                                        rate.value                                                 AS rate
                                   FROM src.foreign_currency_rate rate
                                        JOIN
                                        src.foreign_currency_dictionary dict
                                            ON dict.id = rate.id
                                           AND dict.tech$expiration_dt = '2999-12-31'
                                  WHERE rate.tech$effective_dt >= :tech$effective_dt))
                 WINDOW wnd AS (PARTITION BY trade_dt,
                                             crnc_code
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
       pre.rate
  FROM w_pre pre
       JOIN
       lnk_currency_rate lnk
           ON lnk.crnc_code = pre.crnc_code
          AND lnk.trade_dt = pre.trade_dt
WINDOW wnd AS (PARTITION BY lnk.tech$hash_key
                   ORDER BY pre.tech$effective_dt)
