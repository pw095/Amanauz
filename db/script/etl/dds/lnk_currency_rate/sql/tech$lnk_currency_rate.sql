INSERT
  INTO tech$lnk_currency_rate
  (
    tech$load_id,
    tech$hash_key,
    tech$load_dt,
    tech$last_seen_dt,
    tech$record_source,
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
                                        tech$effective_dt,
                                        tech$last_seen_dt,
                                        trade_date                       AS trade_dt,
                                        IFNULL(iso_char_code, 'UNKNOWN') AS crnc_code
                                   FROM (SELECT
                                                rate.tech$effective_dt,
                                                rate.tech$last_seen_dt,
                                                rate.trade_date,
                                                dict.iso_char_code
                                           FROM src.foreign_currency_rate rate
                                                JOIN
                                                src.foreign_currency_dictionary dict
                                                    ON dict.id = rate.id
                                                   AND rate.tech$expiration_dt BETWEEN dict.tech$effective_dt AND dict.tech$expiration_dt
                                          WHERE rate.tech$effective_dt >= :tech$effective_dt
                                          UNION ALL
                                         SELECT
                                                dict.tech$effective_dt,
                                                dict.tech$last_seen_dt,
                                                rate.trade_date         AS trade_dt,
                                                dict.iso_char_code
                                           FROM src.foreign_currency_dictionary dict
                                                JOIN
                                                src.foreign_currency_rate rate
                                                    ON rate.id = dict.id
                                                   AND dict.tech$expiration_dt BETWEEN rate.tech$effective_dt AND rate.tech$expiration_dt))))
          GROUP BY
                   tech$hash_key
     )
SELECT
       :tech$load_id           AS tech$load_id,
       pre.tech$hash_key,
       pre.tech$load_dt,
       pre.tech$last_seen_dt,
       crnc.tech$record_source,
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
