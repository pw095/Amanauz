INSERT
  INTO hub_emitent
  (
    tech$load_id,
    tech$hash_key,
    tech$record_source,
    tech$load_dt,
    tech$last_seen_dt,
    code
  )
SELECT
       :tech$load_id      AS tech$load_id,
       tech$hash_key,
       tech$record_source,
       tech$load_dt,
       tech$last_seen_dt,
       code
  FROM tech$hub_emitent
ON CONFLICT(tech$hash_key)
DO UPDATE
   SET last_seen_date = excluded.last_seen_date,
       tech$load_id = excluded.tech$load_id
