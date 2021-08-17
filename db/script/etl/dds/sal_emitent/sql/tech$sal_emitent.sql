INSERT
  INTO tech$sal_emitent
  (
    tech$load_id,
    tech$hash_key,
    tech$record_source,
    tech$load_dt,
    tech$last_seen_dt,
    emitent_master_hash_key,
    emitent_duplicate_hash_key
  )
WITH
     w_pre AS
     (
         SELECT
                tech$hash_key,
                MIN(tech$effective_dt)   AS tech$load_dt,
                MAX(tech$last_seen_dt)   AS tech$last_seen_dt,
                MIN(source_system_code)  AS source_system_code,
                MIN(emitent_source_name) AS emitent_source_name,
                MIN(emitent_full_name)   AS emitent_full_name
           FROM (SELECT
                        sha1(concat_value) AS tech$hash_key,
                        tech$effective_dt,
                        tech$last_seen_dt,
                        source_system_code,
                        emitent_source_name,
                        emitent_full_name
                   FROM (SELECT
                                tech$effective_dt,
                                tech$last_seen_dt,
                                '_' || IFNULL(CAST(emitent_source_name AS TEXT), '!@#$%^&*') ||
                                '_' || IFNULL(CAST(emitent_full_name   AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                                source_system_code,
                                emitent_source_name,
                                emitent_full_name
                           FROM (SELECT
                                        tech$effective_dt,
                                        tech$expiration_dt,
                                        tech$last_seen_dt,
                                        source_system_code,
                                        emitent_source_name,
                                        emitent_full_name
                                   FROM src.master_data_emitent_map
                                  WHERE (   1 = 1
                                         OR tech$effective_dt >= :tech$effective_dt)
                                    AND NULLIF(emitent_source_name, '') IS NOT NULL
                                    AND NULLIF(emitent_full_name, '')   IS NOT NULL
                                    AND tech$expiration_dt = '2999-12-31')))
          GROUP BY
                   tech$hash_key
     )
SELECT
       :tech$load_id               AS tech$load_id,
       pre.tech$hash_key,
       'moex.com'                  AS tech$record_source,
       pre.tech$load_dt,
       pre.tech$last_seen_dt,
       emitent_master.tech$hash_key    AS emitent_master_hash_key,
       emitent_duplicate.tech$hash_key AS emitent_duplicate_hash_key
  FROM w_pre pre
       JOIN
       hub_emitent emitent_master
           ON emitent_master.code = pre.emitent_full_name
       JOIN
       hub_emitent emitent_duplicate
           ON emitent_duplicate.code = pre.emitent_source_name
