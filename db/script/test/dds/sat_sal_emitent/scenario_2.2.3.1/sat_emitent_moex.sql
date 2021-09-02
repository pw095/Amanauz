INSERT
  INTO sat_emitent_moex
  (
    tech$load_id,
    tech$hash_key,
    tech$effective_dt,
    tech$expiration_dt,
    tech$record_source,
    tech$hash_value,
    title,
    inn,
    okpo
  )
SELECT
       1                                                           AS tech$load_id,
       sha1(title)                                                 AS tech$hash_key,
       tech$effective_dt,
       tech$expiration_dt,
       'master_data'                                               AS tech$record_source,
       sha1('_' || IFNULL(CAST(title AS TEXT), '!@#$%^&*') ||
            '_' || IFNULL(CAST(inn   AS TEXT), '!@#$%^&*') ||
            '_' || IFNULL(CAST(okpo  AS TEXT), '!@#$%^&*') || '_') AS tech$hash_value,
       title,
       inn,
       okpo
  FROM (SELECT
               tech$effective_dt,
               tech$expiration_dt,
               'scenario_2.2.3.1' AS title,
               NULL               AS inn,
               okpo
          FROM (SELECT
                       '2021-11-01' AS tech$effective_dt,
                       '2999-12-31' AS tech$expiration_dt,
                       '48004220'   AS okpo));
