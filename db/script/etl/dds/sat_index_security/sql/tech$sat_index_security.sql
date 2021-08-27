INSERT
  INTO tech$sat_index_security
  (
    tech$load_id,
    tech$hash_key,
    tech$effective_dt,
    tech$expiration_dt,
    tech$record_source,
    tech$hash_value,
    weight
  )
WITH
     w_pre AS
     (
         SELECT
                tech$effective_dt,
                hash_value,
                trade_dt,
                index_id,
                security_id,
                weight
           FROM (SELECT
                        tech$effective_dt,
                        hash_value,
                        LAG(hash_value) OVER wnd AS lag_hash_value,
                        trade_dt,
                        index_id,
                        security_id,
                        weight
                   FROM (SELECT
                                tech$effective_dt,
                                sha1(concat_value) AS hash_value,
                                trade_dt,
                                index_id,
                                security_id,
                                weight
                           FROM (SELECT
                                        tech$effective_dt,
                                        '_' || IFNULL(CAST(weight AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                                        trade_date                                             AS trade_dt,
                                        index_id,
                                        security_id,
                                        weight
                                   FROM src.index_security_weight
                                  WHERE tech$effective_dt >= :tech$effective_dt))
                 WINDOW wnd AS (PARTITION BY trade_dt,
                                             index_id,
                                             security_id
                                    ORDER BY tech$effective_dt))
          WHERE hash_value != lag_hash_value
             OR lag_hash_value IS NULL
     )
SELECT
       :tech$load_id                                                           AS tech$load_id,
       lnk_is.tech$hash_key,
       pre.tech$effective_dt,
       LEAD(DATE(pre.tech$effective_dt, '-1 DAY'), 1, '2999-12-31') OVER (wnd) AS tech$expiration_dt,
       hub_index.tech$record_source,
       pre.hash_value                                                          AS tech$hash_value,
       pre.weight
  FROM w_pre pre
       JOIN
       hub_security hub_index
           ON hub_index.security_id = pre.index_id
       JOIN
       hub_security hub_security
           ON hub_security.security_id = pre.security_id
       JOIN
       lnk_index_security lnk_is
           ON lnk_is.index_hash_key = hub_index.tech$hash_key
          AND lnk_is.security_hash_key = hub_security.tech$hash_key
          AND lnk_is.trade_dt = pre.trade_dt
WINDOW wnd AS (PARTITION BY lnk_is.tech$hash_key
                   ORDER BY pre.tech$effective_dt)
