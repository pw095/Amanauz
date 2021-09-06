INSERT
  INTO lnk_currency_rate
  (
    tech$load_id,
    tech$hash_key,
    tech$record_source,
    tech$load_dt,
    tech$last_seen_dt,
    trade_dt,
    crnc_code
  )
SELECT
       :tech$load_id      AS tech$load_id,
       tech$hash_key,
       tech$record_source,
       tech$load_dt,
       tech$last_seen_dt,
       trade_dt,
       crnc_code
  FROM tech$lnk_currency_rate
 WHERE 1 = 1
ON CONFLICT(tech$hash_key)
DO UPDATE
      SET tech$last_seen_dt = excluded.tech$last_seen_dt,
          tech$load_id = excluded.tech$load_id