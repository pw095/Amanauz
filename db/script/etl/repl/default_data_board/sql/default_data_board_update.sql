UPDATE default_data_board AS dest
   SET tech$last_seen_dt = (SELECT
                                   tech$last_seen_dt
                              FROM tech$default_data_board src
                             WHERE
                                   src.board_id = dest.board_id
                               AND src.tech$last_seen_dt > dest.tech$last_seen_dt
                               AND src.tech$expiration_dt = '2999-12-31')
 WHERE tech$expiration_dt = '2999-12-31'
   AND EXISTS(SELECT
                     NULL
                FROM tech$default_data_board src
               WHERE
                     src.board_id = dest.board_id
                 AND src.tech$last_seen_dt > dest.tech$last_seen_dt
                 AND src.tech$expiration_dt = '2999-12-31')
