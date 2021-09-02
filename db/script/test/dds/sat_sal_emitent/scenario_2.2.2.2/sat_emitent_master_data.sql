INSERT
  INTO sat_emitent_master_data
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
SELECT
       1                                                                AS tech$load_id,
       sha1(full_name)                                                  AS tech$hash_key,
       tech$effective_dt,
       tech$expiration_dt,
       'master_data'                                                    AS tech$record_source,
       sha1('_' || IFNULL(CAST(full_name  AS TEXT), '!@#$%^&*') ||
            '_' || IFNULL(CAST(short_name AS TEXT), '!@#$%^&*') ||
            '_' || IFNULL(CAST(reg_date   AS TEXT), '!@#$%^&*') ||
            '_' || IFNULL(CAST(ogrn       AS TEXT), '!@#$%^&*') ||
            '_' || IFNULL(CAST(inn        AS TEXT), '!@#$%^&*') || '_') AS tech$hash_value,
       full_name,
       short_name,
       reg_date,
       ogrn,
       inn
  FROM (SELECT
               tech$effective_dt,
               tech$expiration_dt,
               'scenario_2.2.2.2' AS full_name,
               'scenario_2.2.2.2' AS short_name,
               NULL               AS reg_date,
               NULL               AS ogrn,
               inn
          FROM (SELECT
                       '2021-09-01' AS tech$effective_dt,
                       '2021-10-31' AS tech$expiration_dt,
                       '0010033331' AS inn
                 UNION ALL
                SELECT
                       '2021-11-01' AS tech$effective_dt,
                       '2999-12-31' AS tech$expiration_dt,
                       NULL         AS inn));
