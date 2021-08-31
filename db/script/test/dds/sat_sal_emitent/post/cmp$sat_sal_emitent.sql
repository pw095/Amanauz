INSERT
  INTO cmp$sat_sal_emitent
  (
    env,
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
       'SAT' AS env,
       NULL  AS tech$load_id,
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
  FROM (SELECT
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
          FROM sat_sal_emitent
        EXCEPT
        SELECT
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
          FROM corr$sat_sal_emitent)
 UNION ALL
SELECT
       'CORR' AS env,
       NULL   AS tech$load_id,
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
  FROM (SELECT
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
          FROM corr$sat_sal_emitent
        EXCEPT
        SELECT
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
          FROM sat_sal_emitent);
