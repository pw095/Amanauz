INSERT
  INTO tech$lnk_index_security
  (
    tech$load_id,
    tech$hash_key,
    tech$load_dt,
    tech$last_seen_dt,
    tech$record_source,
    trade_dt,
    index_hash_key,
    security_hash_key
  )
WITH
     w_pre AS
     (
         SELECT
                tech$hash_key,
                MIN(tech$effective_dt) AS tech$load_dt,
                MAX(tech$last_seen_dt) AS tech$last_seen_dt,
                MIN(trade_dt)          AS trade_dt,
                MIN(index_id)          AS index_id,
                MIN(security_id)       AS security_id
           FROM (SELECT
                        sha1(concat_value) AS tech$hash_key,
                        tech$effective_dt,
                        tech$last_seen_dt,
                        trade_dt,
                        index_id,
                        security_id
                   FROM (SELECT
                                tech$effective_dt,
                                tech$last_seen_dt,
                                '_' || IFNULL(CAST(trade_date  AS TEXT), '!@#$%^&*') ||
                                '_' || IFNULL(CAST(index_id    AS TEXT), '!@#$%^&*') ||
                                '_' || IFNULL(CAST(security_id AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                                trade_date                                                  AS trade_dt,
                                index_id,
                                security_id
                           FROM src.index_security_weight
                          WHERE tech$effective_dt >= :tech$effective_dt))
          GROUP BY
                   tech$hash_key
     )
SELECT
       :tech$load_id              AS tech$load_id,
       pre.tech$hash_key,
       pre.tech$load_dt,
       pre.tech$last_seen_dt,
       'moex.com'                 AS tech$record_source,
       pre.trade_dt,
       hub_index.tech$hash_key    AS index_hash_key,
       hub_security.tech$hash_key AS security_hash_key
  FROM w_pre pre
       JOIN
       hub_security hub_index
           ON hub_index.security_id = pre.index_id
       JOIN
       hub_security hub_security
           ON hub_security.security_id = pre.security_id
 WHERE EXISTS(SELECT
                     NULL
                FROM ref_calendar clndr
               WHERE clndr.clndr_dt = pre.trade_dt)
