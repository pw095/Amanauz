INSERT
  INTO tech$sat_security_bond
  (
    tech$load_id,
    tech$hash_key,
    tech$effective_dt,
    tech$expiration_dt,
    tech$record_source,
    tech$hash_value,
    coupon_value,
    coupon_percent,
    coupon_period,
    mature_dt,
    buy_back_dt,
    buy_back_price,
    offer_dt
  )
WITH
     w_pre AS
     (
         SELECT
                tech$effective_dt,
                hash_value,
                security_id,
                coupon_value,
                coupon_percent,
                coupon_period,
                mature_dt,
                buy_back_dt,
                buy_back_price,
                offer_dt
           FROM (SELECT
                        tech$effective_dt,
                        hash_value,
                        LAG(hash_value) OVER (wnd)            AS lag_hash_value,
                        security_id,
                        coupon_value,
                        coupon_percent,
                        coupon_period,
                        mature_dt,
                        buy_back_dt,
                        buy_back_price,
                        offer_dt
                   FROM (SELECT
                                tech$effective_dt,
                                sha1(concat_value) AS hash_value,
                                security_id,
                                coupon_value,
                                coupon_percent,
                                coupon_period,
                                mature_dt,
                                buy_back_dt,
                                buy_back_price,
                                offer_dt
                           FROM (SELECT
                                        tech$effective_dt,
                                        '_' || IFNULL(CAST(coupon_value   AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(coupon_percent AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(coupon_period  AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(mature_dt      AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(buy_back_dt    AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(buy_back_price AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(offer_dt       AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                                        security_id,
                                        coupon_value,
                                        coupon_percent,
                                        coupon_period,
                                        mature_dt,
                                        buy_back_dt,
                                        buy_back_price,
                                        offer_dt
                                   FROM (SELECT
                                                tech$effective_dt,
                                                security_id,
                                                coupon_value,
                                                coupon_percent,
                                                coupon_period,
                                                mat_date                            AS mature_dt,
                                                NULLIF(buy_back_date, '0000-00-00') AS buy_back_dt,
                                                buy_back_price,
                                                NULLIF(offer_date, '')              AS offer_dt
                                           FROM src.security_daily_info_bonds
                                          WHERE tech$effective_dt >= :tech$effective_dt)))
                 WINDOW wnd AS (PARTITION BY security_id
                                    ORDER BY tech$effective_dt))
          WHERE (   hash_value != lag_hash_value
                 OR lag_hash_value IS NULL)
     )
SELECT
       :tech$load_id                                                           AS tech$load_id,
       hub.tech$hash_key,
       pre.tech$effective_dt,
       LEAD(DATE(pre.tech$effective_dt, '-1 DAY'), 1, '2999-12-31') OVER (wnd) AS tech$expiration_dt,
       hub.tech$record_source,
       pre.hash_value                                                          AS tech$hash_value,
       pre.coupon_value,
       pre.coupon_percent,
       pre.coupon_period,
       pre.mature_dt,
       pre.buy_back_dt,
       pre.buy_back_price,
       pre.offer_dt
  FROM w_pre pre
       JOIN
       hub_security hub
           ON hub.security_id = pre.security_id
WINDOW wnd AS (PARTITION BY pre.security_id
                   ORDER BY pre.tech$effective_dt)
