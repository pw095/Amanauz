INSERT
  INTO hub_security
  (
    tech$load_id,
    tech$hash_key,
    tech$record_source,
    tech$load_dt,
    tech$last_seen_dt,
    security_id
  )
SELECT
       :tech$load_id      AS tech$load_id,
       tech$hash_key,
       tech$record_source,
       tech$load_dt,
       tech$last_seen_dt,
       security_id
  FROM tech$hub_security
 WHERE 1 = 1
ON CONFLICT(tech$hash_key) DO UPDATE
   SET last_seen_date = excluded.last_seen_date,
       tech$load_id = excluded.tech$load_id
