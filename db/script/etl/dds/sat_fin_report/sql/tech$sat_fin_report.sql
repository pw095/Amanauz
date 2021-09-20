INSERT
  INTO tech$sat_fin_report
  (
    tech$load_id,
    tech$hash_key,
    tech$effective_dt,
    tech$expiration_dt,
    tech$record_source,
    tech$hash_value,
    value
  )
WITH
     w_pre AS
     (
         SELECT
                tech$effective_dt,
                hash_value,
                emitent_name,
                currency,
                report_dt,
                fin_stmt_code,
                value
           FROM (SELECT
                        tech$effective_dt,
                        hash_value,
                        LAG(hash_value) OVER (wnd) AS lag_hash_value,
                        emitent_name,
                        currency,
                        report_dt,
                        fin_stmt_code,
                        value
                   FROM (SELECT
                                tech$effective_dt,
                                sha1(concat_value) AS hash_value,
                                emitent_name,
                                currency,
                                report_dt,
                                fin_stmt_code,
                                value
                           FROM (SELECT
                                        tech$effective_dt,
                                        '_' || IFNULL(CAST(value AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                                        emitent_name,
                                        currency,
                                        report_dt,
                                        fin_stmt_code,
                                        value
                                   FROM src.emitent_fin_statement
                                  WHERE tech$effective_dt >= :tech$effective_dt))
                 WINDOW wnd AS (PARTITION BY emitent_name,
                                             currency,
                                             report_dt,
                                             fin_stmt_code
                                    ORDER BY tech$effective_dt))
          WHERE hash_value != lag_hash_value
             OR lag_hash_value IS NULL
     )
SELECT
       :tech$load_id                                                           AS tech$load_id,
       lnk_fin.tech$hash_key,
       pre.tech$effective_dt,
       LEAD(DATE(pre.tech$effective_dt, '-1 DAY'), 1, '2999-12-31') OVER (wnd) AS tech$expiration_dt,
       lnk_fin.tech$record_source,
       pre.hash_value                                                          AS tech$hash_value,
       pre.value
  FROM w_pre pre
       JOIN
       sat_emitent_master_data sat_emitent
           ON sat_emitent.short_name = pre.emitent_name
          AND sat_emitent.tech$expiration_dt = '2999-12-31'
       JOIN
       lnk_fin_report lnk_fin
           ON lnk_fin.crnc_code = pre.currency
          AND lnk_fin.fs_code = pre.fin_stmt_code
          AND lnk_fin.emitent_hash_key = sat_emitent.tech$hash_key
          AND lnk_fin.report_dt = pre.report_dt
WINDOW wnd AS (PARTITION BY lnk_fin.tech$hash_key
                   ORDER BY pre.tech$effective_dt)
