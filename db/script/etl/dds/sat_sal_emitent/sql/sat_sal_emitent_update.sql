UPDATE sat_sal_emitent AS sat
   SET tech$load_id       = :tech$load_id,
       tech$expiration_dt = :tech$expiration_dt
 WHERE sat.tech$expiration_dt = '2999-12-31'
   AND NOT EXISTS(SELECT
                         NULL
                    FROM src.master_data_emitent_map src
                         JOIN
                         hub_emitent hub_master
                             ON hub_master.code = src.emitent_full_name
                         JOIN
                         hub_emitent hub_duplicate
                             ON hub_duplicate.code = src.emitent_source_name
                         JOIN
                         sal_emitent sal
                             ON sal.emitent_master_hash_key = hub_master.tech$hash_key
                            AND sal.emitent_duplicate_hash_key = hub_duplicate.tech$hash_key
                   WHERE sal.tech$hash_key = sat.tech$hash_key)
