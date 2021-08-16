UPDATE security_daily_info_shares AS dest
   SET tech$last_seen_dt = (SELECT
                                   tech$last_seen_dt
                              FROM tech$security_daily_info_shares src
                             WHERE
                                   src.security_id = dest.security_id
                               AND src.board_id = dest.board_id
                               AND src.tech$last_seen_dt > dest.tech$last_seen_dt
                               AND src.tech$expiration_dt = '2999-12-31')
 WHERE tech$expiration_dt = '2999-12-31'
   AND EXISTS(SELECT
                     NULL
                FROM tech$security_daily_info_shares src
               WHERE
                     src.security_id = dest.security_id
                 AND src.board_id = dest.board_id
                 AND src.tech$last_seen_dt > dest.tech$last_seen_dt
                 AND src.tech$expiration_dt = '2999-12-31')
