UPDATE master_data_ref_calendar AS dest
   SET tech$last_seen_dt = (SELECT
                                   tech$last_seen_dt
                              FROM tech$master_data_ref_calendar src
                             WHERE
                                   src.full_date = dest.full_date
                               AND src.tech$last_seen_dt > dest.tech$last_seen_dt
                               AND src.tech$expiration_dt = '2999-12-31')
 WHERE tech$expiration_dt = '2999-12-31'
   AND EXISTS(SELECT
                     NULL
                FROM tech$master_data_ref_calendar src
               WHERE
                     src.full_date = dest.full_date
                 AND src.tech$last_seen_dt > dest.tech$last_seen_dt
                 AND src.tech$expiration_dt = '2999-12-31')
