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
       sha1(code)         AS tech$hash_key,
       tech$record_source,
       tech$load_dt,
       tech$last_seen_dt,
       code
  FROM (SELECT
               MIN(tech$effective_dt) AS tech$load_dt,
               MAX(tech$effective_dt) AS tech$last_seen_dt,
               'moex.com'             AS tech$record_source,
               emitent_title          AS code
          FROM src.security_emitent_map
         WHERE tech$effective_dt > :tech$effective_dt
           AND emitent_title IS NOT NULL
           AND emitent_title != ""
         GROUP BY
                  emitent_title)
ON CONFLICT(emit_hkey) DO UPDATE
   SET last_seen_date = excluded.last_seen_date,
       tech$load_id = excluded.tech$load_id
