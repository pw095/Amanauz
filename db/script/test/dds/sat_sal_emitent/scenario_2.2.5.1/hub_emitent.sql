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
       1                  AS tech$load_id,
       sha1(emitent_name) AS tech$hash_key,
       'master_data'      AS tech$record_source,
       '2021-09-01'       AS tech$load_dt,
       '2021-09-01'       AS tech$last_seen_dt,
       emitent_name       AS code
  FROM (SELECT
               'scenario_2.2.5.1' AS emitent_name)
