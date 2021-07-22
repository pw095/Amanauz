INSERT
  INTO hub_security_type
  (
    tech$load_id,
    tech$hash_key,
    tech$record_source,
    tech$load_dt,
    tech$last_seen_dt,
    id
  )
SELECT
       :tech$load_id     AS tech$load_id,
       sha1(id)          AS tech$hash_key,
       tech$record_source,
       tech$load_dt,
       tech$last_seen_dt,
       id
  FROM (SELECT
               MIN(tech$effective_dt) AS tech$load_dt,
               MAX(tech$effective_dt) AS tech$last_seen_dt,
               tech$record_source,
               id
          FROM (SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       'moex.com'          AS tech$record_source,
                       CAST(id AS VARCHAR) AS id
                  FROM src.security_types
                 WHERE id IS NOT NULL
                   AND id != ""
                 UNION ALL
                SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       'master_data'      AS tech$record_source,
                       security_type_id   AS id
                  FROM src.default_data_security_type)
         WHERE tech$effective_dt >= :tech$effective_dt
           AND tech$expiration_dt = '2999-12-31'
         GROUP BY
                  tech$record_source,
                  id)
 WHERE 1 = 1
ON CONFLICT(tech$hash_key) DO UPDATE
   SET last_seen_date = excluded.last_seen_date,
       tech$load_id = excluded.tech$load_id
