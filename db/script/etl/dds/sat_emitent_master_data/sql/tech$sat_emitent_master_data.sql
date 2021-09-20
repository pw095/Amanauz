INSERT
  INTO tech$sat_emitent_master_data
  (
    tech$load_id,
    tech$hash_key,
    tech$effective_dt,
    tech$expiration_dt,
    tech$record_source,
    tech$hash_value,
    full_name,
    short_name,
    reg_date,
    ogrn,
    inn
  )
WITH
     w_pre AS
     (
         SELECT
                tech$effective_dt,
                hash_value,
                full_name,
                short_name,
                reg_date,
                ogrn,
                inn
           FROM (SELECT
                        tech$effective_dt,
                        hash_value,
                        LAG(hash_value) OVER (wnd) AS lag_hash_value,
                        full_name,
                        short_name,
                        reg_date,
                        ogrn,
                        inn
                   FROM (SELECT
                                tech$effective_dt,
                                sha1(concat_value) AS hash_value,
                                full_name,
                                short_name,
                                reg_date,
                                ogrn,
                                inn
                           FROM (SELECT
                                        tech$effective_dt,
                                        '_' || IFNULL(CAST(full_name  AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(short_name AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(reg_date   AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(ogrn       AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(inn        AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                                        full_name,
                                        short_name,
                                        reg_date,
                                        ogrn,
                                        inn
                                   FROM src.master_data_emitent
                                  WHERE tech$effective_dt >= :tech$effective_dt))
                 WINDOW wnd AS (PARTITION BY short_name
                                    ORDER BY tech$effective_dt))
          WHERE hash_value != lag_hash_value
             OR lag_hash_value IS NULL
     )
SELECT
       :tech$load_id                                                           AS tech$load_id,
       hub.tech$hash_key,
       pre.tech$effective_dt,
       LEAD(DATE(pre.tech$effective_dt, '-1 DAY'), 1, '2999-12-31') OVER (wnd) AS tech$expiration_dt,
       'master_data'                                                           AS tech$record_source,
       pre.hash_value                                                          AS tech$hash_value,
       pre.full_name,
       pre.short_name,
       pre.reg_date,
       pre.ogrn,
       pre.inn
  FROM w_pre pre
       JOIN
       hub_emitent hub
           ON hub.code = pre.full_name
WINDOW wnd AS (PARTITION BY pre.short_name
                   ORDER BY pre.tech$effective_dt)
