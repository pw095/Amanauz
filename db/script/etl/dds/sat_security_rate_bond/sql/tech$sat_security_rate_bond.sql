INSERT
  INTO tech$sat_security_rate_bond
  (
    tech$load_id,
    tech$hash_key,
    tech$effective_dt,
    tech$expiration_dt,
    tech$record_source,
    tech$hash_value,
    accrued_interest,
    yield,
    duration
  )
WITH
     w_pre AS
     (
         SELECT
                tech$effective_dt,
                hash_value,
                security_id,
                accrued_interest,
                yield,
                duration
           FROM (SELECT
                        tech$effective_dt,
                        hash_value,
                        LAG(hash_value) OVER (wnd) AS lag_hash_value,
                        security_id,
                        accrued_interest,
                        yield,
                        duration
                   FROM (SELECT
                                tech$effective_dt,
                                sha1(concat_value) AS hash_value,
                                security_id,
                                accrued_interest,
                                yield,
                                duration
                           FROM (SELECT
                                        tech$effective_dt,
                                        '_' || IFNULL(CAST(accrued_interest AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(yield            AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(duration         AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                                        security_id,
                                        accrued_interest,
                                        yield,
                                        duration
                                   FROM (SELECT
                                                tech$effective_dt,
                                                security_id,
                                                acc_int AS accrued_interest,
                                                yield_close AS yield,
                                                duration
                                           FROM src.security_rate_bonds
                                          WHERE tech$effective_dt >= :tech$effective_dt)))
                 WINDOW wnd AS (PARTITION BY security_id
                                    ORDER BY tech$effective_dt))
          WHERE hash_value != lag_hash_value
             OR lag_hash_value IS NULL
     )
/*SELECT tech$effective_dt, security_id, COUNT(*)
  FROM w_pre
 GROUP BY tech$effective_dt, security_id
HAVING COUNT(*) > 1;*/
SELECT
       :tech$load_id                                                           AS tech$load_id,
       hub.tech$hash_key,
       pre.tech$effective_dt,
       LEAD(DATE(pre.tech$effective_dt, '-1 DAY'), 1, '2999-12-31') OVER (wnd) AS tech$expiration_dt,
       hub.tech$record_source,
       pre.hash_value                                                          AS tech$hash_value,
       pre.accrued_interest,
       pre.yield,
       pre.duration
  FROM w_pre pre
       JOIN
       hub_security hub
           ON hub.security_id = pre.security_id
          AND hub.tech$record_source = 'moex.com'
 --WHERE pre.rn = 1 -- Исключить индексируемые облигации на этапе размещения
WINDOW wnd AS (PARTITION BY pre.security_id
                   ORDER BY pre.tech$effective_dt)
