INSERT
  INTO tech$hub_security_type
  (
    tech$load_id,
    tech$hash_key,
    tech$load_dt,
    tech$last_seen_dt,
    tech$record_source,
    id
  )
WITH
     w_pre AS
     (
         SELECT
                :tech$load_id           AS tech$load_id,
                sha1(id)                AS tech$hash_key,
                MIN(tech$effective_dt)  AS tech$load_dt,
                MAX(tech$last_seen_dt)  AS tech$last_seen_dt,
                MAX(tech$record_source) AS tech$record_source,
                id
           FROM (SELECT
                        'moex.com'          AS tech$record_source,
                        tech$effective_dt,
                        tech$expiration_dt,
                        tech$last_seen_dt,
                        CAST(id AS VARCHAR) AS id
                   FROM src.security_types
                  WHERE NULLIF(id, '') IS NOT NULL
                  UNION ALL
                 SELECT
                        'master_data'      AS tech$record_source,
                        tech$effective_dt,
                        tech$expiration_dt,
                        tech$last_seen_dt,
                        security_type_id   AS id
                   FROM src.default_data_security_type)
          WHERE (   1 = 1
                 OR tech$effective_dt >= :tech$effective_dt)
            AND tech$expiration_dt = '2999-12-31'
          GROUP BY
                   id
     )
SELECT
      :tech$load_id      AS tech$load_id,
      tech$hash_key,
      tech$load_dt,
      tech$last_seen_dt,
      tech$record_source,
      id
  FROM w_pre
