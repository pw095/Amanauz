UPDATE emitent_fin_statement AS dest
   SET tech$last_seen_dt = (SELECT
                                   tech$last_seen_dt
                              FROM tech$emitent_fin_statement src
                             WHERE
                                   src.emitent_name = dest.emitent_name
                               AND src.report_dt = dest.report_dt
                               AND src.fin_stmt_code = dest.fin_stmt_code
                               AND src.tech$last_seen_dt > dest.tech$last_seen_dt
                               AND src.tech$expiration_dt = '2999-12-31')
 WHERE tech$expiration_dt = '2999-12-31'
   AND EXISTS(SELECT
                     NULL
                FROM tech$emitent_fin_statement src
               WHERE
                     src.emitent_name = dest.emitent_name
                 AND src.report_dt = dest.report_dt
                 AND src.fin_stmt_code = dest.fin_stmt_code
                 AND src.tech$last_seen_dt > dest.tech$last_seen_dt
                 AND src.tech$expiration_dt = '2999-12-31')
