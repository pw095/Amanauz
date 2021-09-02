INSERT
  INTO sal_emitent
  (
    tech$load_id,
    tech$hash_key,
    tech$record_source,
    tech$load_dt,
    tech$last_seen_dt,
    emitent_master_hash_key,
    emitent_duplicate_hash_key
  )
SELECT
       1                                                       AS tech$load_id,
       sha1('_' || emitent_name || '_' || emitent_name || '_') AS tech$hash_key,
       'master_data'                                           AS tech$record_source,
       '2021-09-01'                                            AS tech$load_dt,
       '2021-09-01'                                            AS tech$last_seen_dt,
       sha1(emitent_name)                                      AS emitent_master_hash_key,
       sha1(emitent_name)                                      AS emitent_duplicate_hash_key
  FROM (SELECT
               'scenario_2.2.1.1' AS emitent_name);
