DELETE
  FROM sat_currency_rate_bus AS sat
 WHERE EXISTS(SELECT
                     NULL
                FROM tech$sat_currency_rate_bus
               WHERE crnc_code = sat.crnc_code
                 AND tech$effective_dt <= sat.tech$effective_dt)
