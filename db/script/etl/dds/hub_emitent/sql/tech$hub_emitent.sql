INSERT
  INTO tech$hub_emitent
  (
    tech$load_id,
    tech$hash_key,
    tech$record_source,
    tech$load_dt,
    tech$last_seen_dt,
    code
  )
SELECT
       :tech$load_id          AS tech$load_id,
       sha1(code)             AS tech$hash_key,
       tech$record_source,
       MIN(tech$effective_dt) AS tech$load_dt,
       MAX(tech$effective_dt) AS tech$last_seen_dt,
       code
  FROM (SELECT
               tech$effective_dt,
               tech$expiration_dt,
               'moex.com'         AS tech$record_source,
               emitent_title      AS code
          FROM src.security_emitent_map
         WHERE emitent_title IS NOT NULL
           AND emitent_title != ""
         UNION ALL
        SELECT
               tech$effective_dt,
               tech$expiration_dt,
               'master_data'     AS tech$record_source,
               code
          FROM (SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       short_name         AS code
                  FROM src.master_data_emitent
                 UNION ALL
                SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       emitent_code       AS code
                  FROM src.default_data_emitent))
 WHERE tech$effective_dt >= :tech$effective_dt
   AND tech$expiration_dt = '2999-12-31'
 GROUP BY
          tech$record_source,
          code
