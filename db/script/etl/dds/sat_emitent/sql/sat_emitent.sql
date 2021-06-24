INSERT
  INTO hub_emitent
  (
    emit_hkey,
    load_date,
    last_seen_date,
    emit_code
  )
SELECT
       sha1(emit_code || record_source) AS emit_hkey,
       load_date,
       last_seen_date,
       record_source,
       emit_code
  FROM (SELECT
               MIN(tech$effective_dt) AS load_date,
               MAX(tech$effective_dt) AS last_seen_date,
               'moex.com'             AS record_source,
               emitent_title          AS emit_code
          FROM src.security_emitent_map
         WHERE tech$effective_dt > :tech$effective_dt
         GROUP BY
                  emitent_title)
ON CONFLICT(emit_hkey) DO UPDATE
   SET last_seen_date = excluded.last_seen_date
