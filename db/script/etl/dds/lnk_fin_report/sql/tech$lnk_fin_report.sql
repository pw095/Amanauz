INSERT
  INTO tech$lnk_fin_report
  (
    tech$load_id,
    tech$hash_key,
    tech$load_dt,
    tech$last_seen_dt,
    tech$record_source,
    crnc_code,
    fs_code,
    emitent_hash_key,
    report_dt
  )
WITH
     w_pre AS
     (
         SELECT
                tech$hash_key,
                MIN(tech$effective_dt) AS tech$load_dt,
                MAX(tech$last_seen_dt) AS tech$last_seen_dt,
                MIN(currency)          AS currency,
                MIN(fin_stmt_code)     AS fin_stmt_code,
                MIN(emitent_name)      AS emitent_name,
                MIN(report_dt)         AS report_dt
           FROM (SELECT
                        sha1(concat_value) AS tech$hash_key,
                        tech$effective_dt,
                        tech$last_seen_dt,
                        currency,
                        fin_stmt_code,
                        emitent_name,
                        report_dt
                   FROM (SELECT
                                tech$effective_dt,
                                tech$last_seen_dt,
                                '_' || IFNULL(CAST(currency      AS TEXT), '!@#$%^&*') ||
                                '_' || IFNULL(CAST(fin_stmt_code AS TEXT), '!@#$%^&*') ||
                                '_' || IFNULL(CAST(emitent_name  AS TEXT), '!@#$%^&*') ||
                                '_' || IFNULL(CAST(report_dt     AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                                currency,
                                fin_stmt_code,
                                emitent_name,
                                report_dt
                           FROM (SELECT
                                        tech$effective_dt,
                                        tech$last_seen_dt,
                                        emitent_name,
                                        currency,
                                        report_dt,
                                        fin_stmt_code
                                   FROM src.emitent_fin_statement
                                  WHERE 1 = 1
                                     OR tech$effective_dt >= :tech$effective_dt)))
          GROUP BY
                   tech$hash_key
     )
SELECT
       :tech$load_id          AS tech$load_id,
       pre.tech$hash_key,
       pre.tech$load_dt,
       pre.tech$last_seen_dt,
       'master_data'          AS tech$record_source,
       pre.currency           AS crnc_code,
       pre.fin_stmt_code      AS fs_code,
       hub_emit.tech$hash_key AS emitent_hash_key,
       pre.report_dt
  FROM w_pre pre
       JOIN
       src.master_data_emitent src_emit
           ON src_emit.short_name = pre.emitent_name
          AND src_emit.tech$expiration_dt = '2999-12-31'
       JOIN
       hub_emitent hub_emit
           ON hub_emit.code = src_emit.full_name
 WHERE EXISTS(SELECT
                     NULL
                FROM ref_currency crnc
               WHERE crnc.crnc_code = pre.currency)
   AND EXISTS(SELECT
                     NULL
                FROM ref_fin_statement fs_stmt
               WHERE fs_stmt.fs_code = pre.fin_stmt_code)
   AND EXISTS(SELECT
                     NULL
                FROM ref_calendar clndr
               WHERE clndr.clndr_dt = pre.report_dt)
