INSERT
  INTO tech$sat_currency_rate_bus
  (
    tech$load_id,
    crnc_code,
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
                crnc_code,
                tech$effective_dt,
                tech$hash_value,
                rate
           FROM (SELECT
                        crnc_code,
                        tech$effective_dt,
                        tech$hash_value,
                        LAG(tech$hash_value) OVER wnd AS lag_hash_value,
                        rate
                   FROM (SELECT
                                lnk_crnc_rt.crnc_code,
                                lnk_crnc_rt.trade_dt        AS tech$effective_dt,
                                sat_crnc_rt.tech$hash_value,
                                sat_crnc_rt.rate
                           FROM lnk_currency_rate lnk_crnc_rt
                                JOIN
                                sat_currency_rate sat_crnc_rt
                                    ON sat_crnc_rt.tech$hash_key = lnk_crnc_rt.tech$hash_key
                                   AND sat_crnc_rt.tech$expiration_dt = '2999-12-31'
                                   AND sat_crnc_rt.tech$effective_dt >= :tech$effective_dt)
                 WINDOW wnd AS (PARTITION BY crnc_code
                                    ORDER BY tech$effective_dt))
          WHERE tech$hash_value != lag_hash_value
             OR lag_hash_value IS NULL
     )
SELECT
       :tech$load_id                                                       AS tech$load_id,
       crnc_code,
       tech$effective_dt,
       LEAD(DATE(tech$effective_dt, '-1 DAY'), 1, '2999-12-31') OVER (wnd) AS tech$expiration_dt,
       'moex.com'                                                          AS tech$record_source,
       tech$hash_value,
       rate
  FROM w_pre
WINDOW wnd AS (PARTITION BY crnc_code
                   ORDER BY tech$effective_dt)
