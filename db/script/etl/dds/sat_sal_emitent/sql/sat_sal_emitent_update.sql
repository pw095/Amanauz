UPDATE sat_sal_emitent AS sat
   SET tech$load_id       = :tech$load_id,
       tech$expiration_dt = (SELECT
                                    DATE(MAX(tech$last_seen_dt) OVER (), '-1 day')
                               FROM sal_emitent src
                              WHERE src.tech$hash_key = sat.tech$hash_key)
 WHERE sat.tech$expiration_dt = '2999-12-31'
   AND NOT EXISTS(SELECT
                         NULL
                    FROM (SELECT
                                 tech$hash_key,
                                 tech$last_seen_dt,
                                 MAX(tech$last_seen_dt) OVER () AS max_tech$last_seen_dt
                            FROM sal_emitent) src
                   WHERE src.tech$hash_key = sat.tech$hash_key
                     AND src.tech$last_seen_dt = src.max_tech$last_seen_dt)