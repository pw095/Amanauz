INSERT
  INTO lnk_fin_report
  (
    tech$load_id,
    tech$hash_key,
    tech$load_dt,
    tech$last_seen_dt,
    tech$record_source,
    crnc_code,
    fs_code,
    emitent_hash_key,
    report_dt
  )
SELECT
       :tech$load_id      AS tech$load_id,
       tech$hash_key,
       tech$load_dt,
       tech$last_seen_dt,
       tech$record_source,
       crnc_code,
       fs_code,
       emitent_hash_key,
       report_dt
  FROM tech$lnk_fin_report
 WHERE 1 = 1
ON CONFLICT(tech$hash_key)
DO UPDATE
      SET tech$last_seen_dt = excluded.tech$last_seen_dt,
          tech$load_id = excluded.tech$load_id
