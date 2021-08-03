INSERT
  INTO tech$lnk_security_type
  (
    tech$load_id,
    tech$hash_key,
    tech$record_source,
    tech$load_dt,
    tech$last_seen_dt,
    security_hash_key,
    security_type_hash_key
  )
WITH
     w_raw AS
     (
         SELECT
                MIN(tech$effective_dt) AS tech$load_dt,
                MAX(tech$last_seen_dt) AS tech$last_seen_dt,
                record_source,
                security_id,
                security_type
           FROM (SELECT
                        tech$effective_dt,
                        tech$expiration_dt,
                        tech$last_seen_dt,
                        record_source,
                        security_id,
                        security_type
                   FROM (SELECT
                                tech$effective_dt,
                                tech$expiration_dt,
                                tech$last_seen_dt,
                                'security_daily_info_shares' AS record_source,
                                security_id,
                                security_type
                           FROM src.security_daily_info_shares
                          UNION ALL
                         SELECT
                                tech$effective_dt,
                                tech$expiration_dt,
                                tech$last_seen_dt,
                                'security_daily_info_bonds' AS record_source,
                                security_id,
                                security_type
                           FROM src.security_daily_info_bonds)
                  WHERE NOT (security_id IS NULL AND security_type IS NULL)
                    AND (   1 = 1
                         OR tech$effective_dt >= :tech$effective_dt))
          GROUP BY
                   record_source,
                   security_id,
                   security_type
     ),
     w_pre AS
     (
         SELECT
                tech$hash_key,
                MIN(tech$load_dt)      AS tech$load_dt,
                MAX(tech$last_seen_dt) AS tech$last_seen_dt,
                MIN(security_id)       AS security_id,
                MIN(security_type_id)  AS security_type
           FROM (SELECT
                        sha1(concat_value) AS tech$hash_key,
                        tech$load_dt,
                        tech$last_seen_dt,
                        security_id,
                        security_type_id
                   FROM (SELECT
                                tech$load_dt,
                                tech$last_seen_dt,
                                '_' || IFNULL(CAST(security_id      AS TEXT), '!@#$%^&*') ||
                                '_' || IFNULL(CAST(security_type_id AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                                security_id,
                                security_type_id
                           FROM (SELECT
                                        raw.tech$load_dt,
                                        raw.tech$last_seen_dt,
                                        raw.security_id,
                                        IFNULL(security_id,                        'UNKNOWN') AS security_id,
                                        IFNULL(CAST(stm.security_type_id AS TEXT), 'UNKNOWN') AS security_type_id
                                   FROM w_raw raw
                                        LEFT JOIN
                                        src.master_data_security_type_map stm
                                            ON stm.type_id = raw.security_type
                                           AND stm.table_name = raw.record_source
                                           AND stm.tech$expiration_dt = '2999-12-31')))
          GROUP BY
                   tech$hash_key
     )
SELECT
       :tech$load_id               AS tech$load_id,
       pre.tech$hash_key,
       'moex.com'                  AS tech$record_source,
       pre.tech$load_dt,
       pre.tech$last_seen_dt,
       security.tech$hash_key      AS security_hash_key,
       security_type.tech$hash_key AS security_type_hash_key
  FROM w_pre pre
       JOIN
       hub_security security
           ON security.security_id = pre.security_id
       JOIN
       hub_security_type security_type
           ON security_type.id = pre.security_type
