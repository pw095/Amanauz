DELETE
  FROM sat_sal_emitent AS sat
 WHERE EXISTS(SELECT
                     NULL
                FROM tech$sat_sal_emitent
               WHERE tech$hash_key = sat.tech$hash_key
                 AND tech$effective_dt <= sat.tech$effective_dt)
