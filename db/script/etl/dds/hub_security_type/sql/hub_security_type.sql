INSERT
  INTO hub_security_type
  (
    tech$load_id,
    tech$hash_key,
    tech$record_source,
    tech$load_dt,
    tech$last_seen_dt,
    id
  )
SELECT
       :tech$load_id     AS tech$load_id,
       sha1(id)          AS tech$hash_key,
       'moex.com'        AS tech$record_source,
       tech$load_dt,
       tech$last_seen_dt,
       id
  FROM (SELECT
               MIN(tech$effective_dt) AS tech$load_dt,
               MAX(tech$effective_dt) AS tech$last_seen_dt,
               CAST(id AS VARCHAR)    AS id
          FROM src.security_types
         WHERE tech$effective_dt >= :tech$effective_dt
           AND id IS NOT NULL
           AND id != ""
         GROUP BY
                  id)
 WHERE 1 = 1
ON CONFLICT(tech$hash_key) DO UPDATE
   SET last_seen_date = excluded.last_seen_date,
       tech$load_id = excluded.tech$load_id
