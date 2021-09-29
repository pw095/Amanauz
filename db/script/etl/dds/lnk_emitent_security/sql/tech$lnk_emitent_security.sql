INSERT
  INTO tech$lnk_emitent_security
  (
    tech$load_id,
    tech$hash_key,
    tech$load_dt,
    tech$last_seen_dt,
    tech$record_source,
    emitent_hash_key,
    security_hash_key
  )
WITH
     w_pre AS
     (
         SELECT
                tech$hash_key,
                MIN(tech$effective_dt) AS tech$load_dt,
                MAX(tech$last_seen_dt) AS tech$last_seen_dt,
                MIN(security_id)       AS security_id,
                MIN(emitent_title)     AS emitent_title
           FROM (SELECT
                        sha1(concat_value) AS tech$hash_key,
                        tech$effective_dt,
                        tech$last_seen_dt,
                        security_id,
                        emitent_title
                   FROM (SELECT
                                tech$effective_dt,
                                tech$last_seen_dt,
                                '_' || IFNULL(CAST(security_id   AS TEXT), '!@#$%^&*') ||
                                '_' || IFNULL(CAST(emitent_title AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                                security_id,
                                emitent_title
                           FROM (SELECT
                                        tech$effective_dt,
                                        tech$expiration_dt,
                                        tech$last_seen_dt,
                                        IFNULL(security_id,   'UNKNOWN') AS security_id,
                                        IFNULL(emitent_title, 'UNKNOWN') AS emitent_title
                                   FROM (SELECT
                                                tech$effective_dt,
                                                tech$expiration_dt,
                                                tech$last_seen_dt,
                                                NULLIF(security_id,   '') AS security_id,
                                                NULLIF(emitent_title, '') AS emitent_title
                                           FROM src.security_emitent_map)
                                  WHERE NOT (security_id IS NULL AND emitent_title IS NULL)
                                    AND (   1 = 1
                                         OR tech$effective_dt >= :tech$effective_dt))))
          GROUP BY
                   tech$hash_key
     )
SELECT
       :tech$load_id          AS tech$load_id,
       pre.tech$hash_key,
       pre.tech$load_dt,
       pre.tech$last_seen_dt,
       'moex.com'             AS tech$record_source,
       emitent.tech$hash_key  AS emitent_hash_key,
       security.tech$hash_key AS security_hash_key
  FROM w_pre pre
       JOIN
       hub_emitent emitent
           ON emitent.code = pre.emitent_title
          AND (emitent.code = 'UNKNOWN' OR emitent.tech$record_source = 'moex.com')
       JOIN
       hub_security security
           ON security.security_id = pre.security_id
