DELETE
  FROM sat_index_security_bus AS sat
 WHERE EXISTS(SELECT
                     NULL
                FROM tech$sat_index_security_bus
               WHERE tech$hash_key = sat.tech$hash_key
                 AND tech$effective_dt <= sat.tech$effective_dt)
