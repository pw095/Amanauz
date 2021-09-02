INSERT
  INTO sat_sal_emitent
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
SELECT
       1                                                                  AS tech$load_id,
       emit.tech$hash_key,
       '2021-09-01'                                                       AS tech$effective_dt,
       '2999-12-31'                                                       AS tech$expiration_dt,
       'master_data'                                                      AS tech$record_source,
       sha1('_' || IFNULL(CAST(s.full_name  AS TEXT), '!@#$%^&*') ||
            '_' || IFNULL(CAST(s.short_name AS TEXT), '!@#$%^&*') ||
            '_' || IFNULL(CAST(s.reg_date   AS TEXT), '!@#$%^&*') ||
            '_' || IFNULL(CAST(s.ogrn       AS TEXT), '!@#$%^&*') ||
            '_' || IFNULL(CAST(s.inn        AS TEXT), '!@#$%^&*') ||
            '_' || IFNULL(CAST(s.okpo       AS TEXT), '!@#$%^&*') || '_') AS tech$hash_value,
       s.full_name,
       s.short_name,
       s.reg_date,
       s.ogrn,
       s.inn,
       s.okpo
  FROM (SELECT
              'scenario_2.2.4.1' AS full_name,
              'scenario_2.2.4.1' AS short_name,
              NULL               AS reg_date,
              NULL               AS ogrn,
              NULL               AS inn,
              NULL               AS okpo) s
       JOIN
       sal_emitent emit
           ON emit.emitent_master_hash_key = sha1(s.full_name);
