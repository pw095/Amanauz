INSERT
  INTO tech$sat_index_security_bus
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
     w_raw AS
     (
         SELECT
                tech$hash_key,
                tech$last_seen_dt,
                max_tech$last_seen_dt,
                CASE tech$last_seen_dt
                     WHEN max_tech$last_seen_dt THEN
                         'ACTIVE'
                     ELSE
                         'OBSOLETE'
                END AS tech$active_flag,
                index_hash_key,
                security_hash_key
           FROM (SELECT
                        tech$hash_key,
                        tech$last_seen_dt,
                        MAX(tech$last_seen_dt) OVER () AS max_tech$last_seen_dt,
                        index_hash_key,
                        security_hash_key
                   FROM lnk_index_security_bus)
     ),
     w_pre AS
     (
         SELECT
                tech$hash_key,
                tech$effective_dt,
                tech$hash_value,
                weight
           FROM (SELECT
                        tech$hash_key,
                        tech$effective_dt,
                        tech$hash_value,
                        LAG(tech$hash_value) OVER wnd AS lag_hash_value,
                        weight
                   FROM (SELECT
                                raw.tech$hash_key,
                                lnk_ind_sec.trade_dt          AS tech$effective_dt,
                                sat_ind_sec.tech$hash_value,
                                sat_ind_sec.weight
                           FROM w_raw raw
                                JOIN
                                lnk_index_security lnk_ind_sec
                                    ON lnk_ind_sec.index_hash_key = raw.index_hash_key
                                   AND lnk_ind_sec.security_hash_key = raw.security_hash_key
                                JOIN
                                sat_index_security sat_ind_sec
                                    ON sat_ind_sec.tech$hash_key = lnk_ind_sec.tech$hash_key
                                   AND sat_ind_sec.tech$expiration_dt = '2999-12-31'
                                   AND sat_ind_sec.tech$effective_dt >= :tech$effective_dt
                          WHERE :tech$effective_dt = :min_tech$effective_dt
                             OR raw.tech$active_flag = 'ACTIVE')
                                  WINDOW wnd AS (PARTITION BY tech$hash_key
                                                     ORDER BY tech$effective_dt))
                           WHERE tech$hash_value != lag_hash_value
                              OR lag_hash_value IS NULL
     )
SELECT COUNT(distinct tech$hash_key) AS cnt from (
SELECT
       :tech$load_id                                                       AS tech$load_id,
       tech$hash_key,
       tech$effective_dt,
       LEAD(DATE(tech$effective_dt, '-1 DAY'), 1, '2999-12-31') OVER (wnd) AS tech$expiration_dt,
       'moex.com'                                                          AS tech$record_source,
       tech$hash_value,
       weight
  FROM w_pre
WINDOW wnd AS (PARTITION BY tech$hash_key
                   ORDER BY tech$effective_dt))
