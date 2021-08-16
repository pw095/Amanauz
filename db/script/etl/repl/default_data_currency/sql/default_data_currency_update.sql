UPDATE default_data_currency AS dest
   SET tech$last_seen_dt = (SELECT
                                   tech$last_seen_dt
                              FROM tech$default_data_currency src
                             WHERE
                                   src.iso_char_code = dest.iso_char_code
                               AND src.tech$last_seen_dt > dest.tech$last_seen_dt
                               AND src.tech$expiration_dt = '2999-12-31')
 WHERE tech$expiration_dt = '2999-12-31'
   AND EXISTS(SELECT
                     NULL
                FROM tech$default_data_currency src
               WHERE
                     src.iso_char_code = dest.iso_char_code
                 AND src.tech$last_seen_dt > dest.tech$last_seen_dt
                 AND src.tech$expiration_dt = '2999-12-31')
