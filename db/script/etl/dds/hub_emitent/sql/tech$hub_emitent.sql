INSERT
  INTO tech$hub_emitent
  (
    tech$load_id,
    tech$hash_key,
    tech$load_dt,
    tech$last_seen_dt,
    tech$record_source,
    code
  )
WITH
     w_pre AS
     (
         SELECT
                sha1(code)         AS tech$hash_key,
                tech$load_dt,
                tech$last_seen_dt,
                tech$record_source,
                code
           FROM (SELECT
                        tech$load_dt,
                        tech$last_seen_dt,
                        tech$record_source,
                        code,
                        COUNT(*) OVER (wnd) AS cnt
                   FROM (SELECT
                                MIN(tech$effective_dt)  AS tech$load_dt,
                                MAX(tech$last_seen_dt)  AS tech$last_seen_dt,
                                MAX(tech$record_source) AS tech$record_source,
                                code
                           FROM (SELECT
                                        tech$effective_dt,
                                        tech$expiration_dt,
                                        tech$last_seen_dt,
                                        'moex.com'         AS tech$record_source,
                                        emitent_title      AS code
                                   FROM src.security_emitent_map
                                  WHERE NULLIF(emitent_title, '') IS NOT NULL
                                  UNION ALL
                                 SELECT
                                        tech$effective_dt,
                                        tech$expiration_dt,
                                        tech$last_seen_dt,
                                        'master_data'      AS tech$record_source,
                                        code
                                   FROM (SELECT
                                                tech$effective_dt,
                                                tech$expiration_dt,
                                                tech$last_seen_dt,
                                                full_name         AS code
                                           FROM src.master_data_emitent
                                          UNION ALL
                                         SELECT
                                                tech$effective_dt,
                                                tech$expiration_dt,
                                                tech$last_seen_dt,
                                                emitent_code       AS code
                                           FROM src.default_data_emitent))
                          WHERE (   1 = 1
                                 OR tech$effective_dt >= :tech$effective_dt)
                            AND tech$expiration_dt = '2999-12-31'
                          GROUP BY
                                   code,
                                   tech$record_source)
                 WINDOW wnd AS (PARTITION BY code))
          WHERE cnt = 1
             OR cnt = 2 AND tech$record_source = 'moex.com'
     )
SELECT
       :tech$load_id      AS tech$load_id,
       tech$hash_key,
       tech$load_dt,
       tech$last_seen_dt,
       tech$record_source,
       code
  FROM w_pre
