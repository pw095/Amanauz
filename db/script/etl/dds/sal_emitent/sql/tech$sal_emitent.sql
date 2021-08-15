INSERT
  INTO tech$sal_emitent
  (
    tech$load_id,
    tech$hash_key,
    tech$effective_dt,
    tech$expiration_dt,
    tech$record_source,
    tech$hash_value,
    emitent_master_hash_key,
    emitent_duplicate_hash_key
  )
WITH
     w_pre AS
     (
         SELECT
                tech$hash_key,
                MIN(tech$effective_dt)  AS tech$load_dt,
                MAX(tech$last_seen_dt)  AS tech$last_seen_dt,
                MIN(emitent_code)       AS emitent_code,
                MIN(emitent_short_name) AS emitent_short_name
           FROM (SELECT
                        sha1(concat_value) AS tech$hash_key,
                        tech$effective_dt,
                        tech$last_seen_dt,
                        emitent_code,
                        emitent_short_name
                   FROM (SELECT
                                tech$effective_dt,
                                tech$last_seen_dt,
                                '_' || IFNULL(CAST(emitent_code       AS TEXT), '!@#$%^&*') ||
                                '_' || IFNULL(CAST(emitent_short_name AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                                emitent_code,
                                emitent_short_name
                           FROM (SELECT
                                        tech$effective_dt,
                                        tech$last_seen_dt,
                                        emitent_code,
                                        emitent_short_name
                                   FROM src.master_data_emitent_map
                                  WHERE tech$effective_dt >= :tech$effective_dt
                                    AND NULLIF(emitent_code, '') IS NOT NULL)))
          GROUP BY
                   tech$hash_key
     )
SELECT
       :tech$load_id                                                           AS tech$load_id,
       pre.tech$hash_key,
       hub_master.tech$record_source,
       pre.tech$load_dt,
       pre.tech$last_seen_dt,
       hub_master.tech$hash_key      AS emitent_master_hash_key,
       hub_duplicate.tech$hash_key   AS emitent_duplicate_hash_key
  FROM w_pre pre
       JOIN
       hub_emitent hub_master
           ON hub_master.code = pre.emitent_short_name
          AND hub_master.tech$record_source = 'master_data'
       JOIN
       hub_emitent hub_duplicate
           ON hub_duplicate.code = pre.emitent_code
          AND hub_duplicate.tech$record_source = 'moex.com'
