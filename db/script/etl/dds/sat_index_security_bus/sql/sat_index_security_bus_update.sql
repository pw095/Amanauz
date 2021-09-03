UPDATE sat_index_security_bus AS sat
   SET tech$load_id       = :tech$load_id,
       tech$expiration_dt = (SELECT
                                    MAX(tech$last_seen_dt)
                               FROM lnk_index_security_bus src
                              WHERE src.tech$hash_key = sat.tech$hash_key)
 WHERE sat.tech$expiration_dt = '2999-12-31'
   AND NOT EXISTS(SELECT
                         NULL
                    FROM (SELECT
                                 tech$hash_key,
                                 tech$last_seen_dt,
                                 MAX(tech$last_seen_dt) OVER () AS max_tech$last_seen_dt
                            FROM lnk_index_security_bus) src
                   WHERE src.tech$hash_key = sat.tech$hash_key
                     AND src.tech$last_seen_dt = src.max_tech$last_seen_dt)
