INSERT
  INTO tech$sat_sal_emitent
  (
    tech$load_id,
    tech$hash_key,
    tech$effective_dt,
    tech$expiration_dt,
    tech$record_source
  );
SELECT
       :tech$load_id          AS tech$load_id,
       sat.tech$hash_key,
       sat.tech$effective_dt,
       :tech$expiration_dt    AS tech$expiration_dt,
       sat.tech$record_source AS tech$record_source
  FROM sat_sal_emitent sat
       JOIN
       sal_emitent sal
           ON sal.tech$hash_key = sat.tech$hash_key
       JOIN
       hub_emitent hub_master
           ON hub_master.tech$hash_key = sal.emitent_master_hash_key
       JOIN
       hub_emitent hub_duplicate
           ON hub_duplicate.tech$hash_key = sal.emitent_duplicate_hash_key
 WHERE tech$expiration_dt = '2999-12-31'
   AND NOT EXISTS(SELECT
                         NULL
                    FROM src.master_data_emitent_map src
                   WHERE src.emitent_code = hub_duplicate.code
                     AND src.emitent_short_name = hub_master.code)
 UNION ALL
SELECT
       :tech$load_id AS tech$load_id,
       sha1(concat_value) AS tech$hash_key,
       tech$effective_dt,
       tech$expiration_dt,
       'master_data' AS tech$record_source
  FROM (SELECT
               '_' || IFNULL(CAST(src.emitent_code       AS TEXT), '!@#$%^&*') ||
               '_' || IFNULL(CAST(src.emitent_short_name AS TEXT), '!@#$%^&*') || '_' AS concat_value,
               src.tech$effective_dt,
               src.tech$expiration_dt,
          FROM src.master_data_emitent_map src
               JOIN
               hub_emitent hub_master
                   ON hub_master.code = src.emitent_short_name -- hub_master.tech$hash_key = sal.emitent_master_hash_key
               JOIN
               hub_emitent hub_duplicate
                   ON hub_duplicate.code = src.emitent_code -- hub_duplicate.tech$hash_key = sal.emitent_duplicate_hash_key
               JOIN
               sal_emitent sal
                   ON sal.emitent_master_hash_key = hub_master.tech$hash_key
                  AND sal.emitent_duplicate_hash_key = hub_duplicate.tech$hash_key
         WHERE NOT EXISTS(SELECT
                                 NULL
                            FROM sat_sal_emitent sat
                           WHERE sat.tech$hash_key = sal.tech$hash_key
                             AND sat.tech$expiration_dt = '2999-12-31'))
