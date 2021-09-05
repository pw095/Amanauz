INSERT
  INTO tech$lnk_currency_rate
  (
    tech$load_id,
    tech$hash_key,
    tech$load_dt,
    tech$record_source,
    tech$last_seen_dt,
    trade_dt,
    crnc_code
  )
WITH
     w_pre AS
     (
         SELECT
                tech$hash_key,
                MIN(tech$effective_dt) AS tech$load_dt,
                MAX(tech$last_seen_dt) AS tech$last_seen_dt,
                MIN(trade_dt)          AS trade_dt,
                MIN(crnc_code)         AS crnc_code
           FROM (SELECT
                        sha1(concat_value) AS tech$hash_key,
                        tech$effective_dt,
                        tech$last_seen_dt,
                        trade_dt,
                        crnc_code
                   FROM (SELECT
                                tech$effective_dt,
                                tech$last_seen_dt,
                                '_' || IFNULL(CAST(trade_dt  AS TEXT), '!@#$%^&*') ||
                                '_' || IFNULL(CAST(crnc_code AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                                trade_dt,
                                crnc_code
                           FROM (SELECT
                                        rate.tech$effective_dt,
                                        rate.tech$last_seen_dt,
                                        rate.trade_date                       AS trade_dt,
                                        IFNULL(dict.iso_char_code, 'UNKNOWN') AS crnc_code
                                   FROM src.foreign_currency_rate rate
                                        JOIN
                                        src.foreign_currency_dictionary dict
                                            ON dict.id = rate.id
                                           AND dict.tech$expiration_dt = '2999-12-31'
                                  WHERE 1 = 1
                                     OR rate.tech$effective_dt >= :tech$effective_dt)))
          GROUP BY
                   tech$hash_key
     )
SELECT
       :tech$load_id          AS tech$load_id,
       pre.tech$hash_key,
       pre.tech$load_dt,
       crnc.tech$record_source,
       pre.tech$last_seen_dt,
       pre.trade_dt,
       pre.crnc_code
  FROM w_pre pre
       JOIN
       ref_currency crnc
           ON crnc.crnc_code = pre.crnc_code
 WHERE EXISTS(SELECT
                     NULL
                FROM ref_calendar clndr
               WHERE clndr.clndr_dt = pre.trade_dt)
