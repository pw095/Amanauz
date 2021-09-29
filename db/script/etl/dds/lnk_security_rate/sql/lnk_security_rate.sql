INSERT
  INTO lnk_security_rate
  (
    tech$load_id,
    tech$hash_key,
    tech$load_dt,
    tech$last_seen_dt,
    tech$record_source,
    trade_dt,
    security_hash_key,
    board_hash_key
  )
SELECT
       :tech$load_id      AS tech$load_id,
       tech$hash_key,
       tech$load_dt,
       tech$last_seen_dt,
       tech$record_source,
       trade_dt,
       security_hash_key,
       board_hash_key
  FROM tech$lnk_security_rate
 WHERE 1 = 1
ON CONFLICT(tech$hash_key)
   DO UPDATE
         SET tech$last_seen_dt = excluded.tech$last_seen_dt,
             tech$load_id = excluded.tech$load_id
