INSERT
  INTO tech$hub_security
  (
    tech$load_id,
    tech$hash_key,
    tech$record_source,
    tech$load_dt,
    tech$last_seen_dt,
    security_id
  )
SELECT
       :tech$load_id          AS tech$load_id,
       sha1(security_id)      AS tech$hash_key,
       tech$record_source,
       MIN(tech$effective_dt) AS tech$load_dt,
       MAX(tech$effective_dt) AS tech$last_seen_dt,
       security_id
  FROM (SELECT
               'moex.com'         AS tech$record_source,
               tech$effective_dt,
               tech$expiration_dt,
               security_id
          FROM (SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       security_id
                  FROM src.security_emitent_map
                 UNION ALL
                SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       security_id
                  FROM src.security_rate_bonds
                 UNION ALL
                SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       security_id
                  FROM src.security_rate_shares
                 UNION ALL
                SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       security_id
                  FROM src.security_daily_info_bonds
                 UNION ALL
                SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       security_id
                  FROM src.security_daily_info_shares)
         WHERE security_id IS NOT NULL
           AND security_id != ""
         UNION ALL
        SELECT
               'master_data'      AS tech$record_source,
               tech$effective_dt,
               tech$expiration_dt,
               security_id
          FROM src.default_data_security)
 WHERE tech$effective_dt >= :tech$effective_dt
   AND tech$expiration_dt = '2999-12-31'
 GROUP BY
          tech$record_source,
          security_id
