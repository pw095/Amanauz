UPDATE foreign_currency_rate AS dest
   SET tech$last_seen_dt = (SELECT
                                   tech$last_seen_dt
                              FROM tech$foreign_currency_rate src
                             WHERE
                                   src.trade_date = dest.trade_date
                               AND src.id = dest.id
                               AND src.tech$last_seen_dt > dest.tech$last_seen_dt
                               AND src.tech$expiration_dt = '2999-12-31')
 WHERE tech$expiration_dt = '2999-12-31'
   AND EXISTS(SELECT
                     NULL
                FROM tech$foreign_currency_rate src
               WHERE
                     src.trade_date = dest.trade_date
                 AND src.id = dest.id
                 AND src.tech$last_seen_dt > dest.tech$last_seen_dt
                 AND src.tech$expiration_dt = '2999-12-31')
