UPDATE master_data_emitent_map AS dest
   SET tech$last_seen_dt = (SELECT
                                   tech$last_seen_dt
                              FROM tech$master_data_emitent_map src
                             WHERE
                                   src.source_system_code = dest.source_system_code
                               AND src.emitent_source_name = dest.emitent_source_name
                               AND src.tech$last_seen_dt > dest.tech$last_seen_dt
                               AND src.tech$expiration_dt = '2999-12-31')
 WHERE tech$expiration_dt = '2999-12-31'
   AND EXISTS(SELECT
                     NULL
                FROM tech$master_data_emitent_map src
               WHERE
                     src.source_system_code = dest.source_system_code
                 AND src.emitent_source_name = dest.emitent_source_name
                 AND src.tech$last_seen_dt > dest.tech$last_seen_dt
                 AND src.tech$expiration_dt = '2999-12-31')
