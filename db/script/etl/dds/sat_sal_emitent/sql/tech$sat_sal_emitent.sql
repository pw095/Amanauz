INSERT
  INTO tech$sat_sal_emitent
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
    inn,
    okpo
  )
WITH
     w_raw AS
     (
         SELECT
                sal.tech$hash_key,
                hub_master.tech$hash_key    AS master_hash_key,
                hub_duplicate.tech$hash_key AS duplicate_hash_key
           FROM src.master_data_emitent_map src
                JOIN
                hub_emitent hub_master
                    ON hub_master.code = src.emitent_full_name -- hub_master.tech$hash_key = sal.emitent_master_hash_key
                JOIN
                hub_emitent hub_duplicate
                    ON hub_duplicate.code = src.emitent_source_name -- hub_duplicate.tech$hash_key = sal.emitent_duplicate_hash_key
                JOIN
                sal_emitent sal
                    ON sal.emitent_master_hash_key = hub_master.tech$hash_key
                   AND sal.emitent_duplicate_hash_key = hub_duplicate.tech$hash_key
          WHERE src.tech$effective_dt >= :tech$effective_dt
            AND src.tech$expiration_dt = '2999-12-31'
     ),
     w_pre AS
     (
         SELECT
                tech$hash_key,
                tech$effective_dt,
                sha1(concat_value) AS hash_value,
                full_name,
                short_name,
                reg_date,
                ogrn,
                inn,
                okpo
           FROM (SELECT
                        tech$hash_key,
                        tech$effective_dt,
                        '_' || IFNULL(CAST(full_name  AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(short_name AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(reg_date   AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(ogrn       AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(inn        AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(okpo       AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                        full_name,
                        short_name,
                        reg_date,
                        ogrn,
                        inn,
                        okpo
                   FROM (SELECT
                                raw.tech$hash_key,
                                emitent_master.tech$effective_dt,
                                emitent_master.full_name,
                                emitent_master.short_name,
                                emitent_master.reg_date,
                                emitent_master.ogrn,
                                emitent_master.inn,
                                emitent_moex.okpo
                           FROM w_raw raw
                                JOIN
                                sat_emitent_master_data emitent_master
                                    ON emitent_master.tech$hash_key = raw.master_hash_key
                                   AND emitent_master.tech$effective_dt >= :tech$effective_dt
                                JOIN
                                sat_emitent_moex emitent_moex
                                    ON emitent_moex.tech$hash_key = raw.duplicate_hash_key
                                   AND emitent_master.tech$effective_dt BETWEEN emitent_moex.tech$effective_dt AND emitent_moex.tech$expiration_dt
                          UNION ALL
                         SELECT
                                raw.tech$hash_key,
                                emitent_moex.tech$effective_dt,
                                emitent_master.full_name,
                                emitent_master.short_name,
                                emitent_master.reg_date,
                                emitent_master.ogrn,
                                emitent_master.inn,
                                emitent_moex.okpo
                           FROM w_raw raw
                                JOIN
                                sat_emitent_moex emitent_moex
                                    ON emitent_moex.tech$hash_key = raw.duplicate_hash_key
                                   AND emitent_moex.tech$effective_dt >= :tech$effective_dt
                                JOIN
                                sat_emitent_master_data emitent_master
                                    ON emitent_master.tech$hash_key = raw.master_hash_key
                                   AND emitent_moex.tech$effective_dt BETWEEN emitent_master.tech$effective_dt AND emitent_master.tech$expiration_dt))
     )
SELECT
       :tech$load_id                                                       AS tech$load_id,
       tech$hash_key,
       tech$effective_dt,
       LEAD(DATE(tech$effective_dt, '-1 DAY'), 1, '2999-12-31') OVER (wnd) AS tech$expiration_dt,
       'master_data'                                                       AS tech$record_source,
       hash_value                                                          AS tech$hash_value,
       full_name,
       short_name,
       reg_date,
       ogrn,
       inn,
       okpo
  FROM (SELECT
               tech$hash_key,
               tech$effective_dt,
               hash_value,
               LAG(hash_value) OVER (wnd) AS lag_hash_value,
               full_name,
               short_name,
               reg_date,
               ogrn,
               inn,
               okpo
          FROM w_pre
        WINDOW wnd AS (PARTITION BY tech$hash_key
                           ORDER BY tech$effective_dt))
 WHERE tech$hash_value != lag_hash_value
    OR lag_hash_value IS NULL
WINDOW wnd AS (PARTITION BY tech$hash_key
                   ORDER BY tech$effective_dt);
