INSERT
  INTO tech$lnk_index_security_bus
  (
    tech$load_id,
    tech$hash_key,
    tech$record_source,
    tech$load_dt,
    tech$last_seen_dt,
    index_hash_key,
    security_hash_key
  )
WITH
     w_pre AS
     (
         SELECT
                tech$hash_key,
                MIN(trade_dt)          AS tech$load_dt,
                MAX(trade_dt)          AS tech$last_seen_dt,
                MIN(index_hash_key)    AS index_hash_key,
                MIN(security_hash_key) AS security_hash_key
           FROM (SELECT
                        sha1(concat_value) AS tech$hash_key,
                        trade_dt,
                        tech$last_seen_dt,
                        index_hash_key,
                        security_hash_key
                   FROM (SELECT
                                lnk_ind_sec.trade_dt,
                                lnk_ind_sec.tech$last_seen_dt,
                                '_' || IFNULL(CAST(hub_ind.security_id AS TEXT), '!@#$%^&*') ||
                                '_' || IFNULL(CAST(hub_sec.security_id AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                                lnk_ind_sec.index_hash_key,
                                lnk_ind_sec.security_hash_key
                           FROM lnk_index_security lnk_ind_sec
                                JOIN
                                hub_security hub_ind
                                    ON hub_ind.tech$hash_key = lnk_ind_sec.index_hash_key
                                JOIN
                                hub_security hub_sec
                                    ON hub_sec.tech$hash_key = lnk_ind_sec.security_hash_key
                          WHERE 1 = 1
                             OR lnk_ind_sec.trade_dt >= :trade_dt))
          GROUP BY
                   tech$hash_key
     )
SELECT
       :tech$load_id              AS tech$load_id,
       pre.tech$hash_key,
       'moex.com'                 AS tech$record_source,
       pre.tech$load_dt,
       pre.tech$last_seen_dt,
       index_hash_key,
       security_hash_key
  FROM w_pre pre
