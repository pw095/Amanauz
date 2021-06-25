INSERT
  INTO tech$foreign_currency_dictionary
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$hash_value,
    id,
    name,
    eng_name,
    nominal,
    parent_code,
    iso_num_code,
    iso_char_code
  )
WITH
     w_raw AS
     (
         SELECT
                tech$load_dt,
                sha1(concat_value) AS hash_value,
                id,
                name,
                eng_name,
                nominal,
                parent_code,
                iso_num_code,
                iso_char_code
           FROM (SELECT
                        tech$load_dt,
                        '_' || IFNULL(CAST(name          AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(eng_name      AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(nominal       AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(parent_code   AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(iso_num_code  AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(iso_char_code AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                        id,
                        name,
                        eng_name,
                        nominal,
                        parent_code,
                        iso_num_code,
                        iso_char_code
                   FROM (SELECT
                                tech$load_dt,
                                ROW_NUMBER() OVER (wnd) AS rn,
                                id,
                                name,
                                eng_name,
                                nominal,
                                parent_code,
                                iso_num_code,
                                iso_char_code
                           FROM (SELECT
                                        DATE(tech$load_dttm) AS tech$load_dt,
                                        tech$load_dttm       AS tech$load_dttm,
                                        id,
                                        name,
                                        eng_name,
                                        nominal,
                                        parent_code,
                                        iso_num_code,
                                        iso_char_code
                                   FROM src.foreign_currency_dictionary
                                  WHERE tech$load_dttm > :tech$load_dttm)
                         WINDOW wnd AS (PARTITION BY
                                                     id,
                                                     tech$load_dt
                                            ORDER BY tech$load_dttm DESC))
                  WHERE rn = 1)
     )
SELECT
       :tech$load_id                                                  AS tech$load_id,
       tech$load_dt                                                   AS tech$effective_dt,
       LEAD(DATE(tech$load_dt, '-1 DAY'), 1, '2999-12-31') OVER (wnd) AS tech$expiration_dt,
       hash_value                                                     AS tech$hash_value,
       id,
       name,
       eng_name,
       nominal,
       parent_code,
       iso_num_code,
       iso_char_code
  FROM (SELECT
               tech$load_dt,
               hash_value,
               id,
               name,
               eng_name,
               nominal,
               parent_code,
               iso_num_code,
               iso_char_code
          FROM (SELECT
                       tech$load_dt,
                       hash_value,
                       LAG(hash_value) OVER (wnd) AS lag_hash_value,
                       id,
                       name,
                       eng_name,
                       nominal,
                       parent_code,
                       iso_num_code,
                       iso_char_code
                  FROM w_raw
                WINDOW wnd AS (PARTITION BY
                                            id
                                   ORDER BY tech$load_dt))
         WHERE hash_value != lag_hash_value
            OR lag_hash_value IS NULL)
WINDOW wnd AS (PARTITION BY
                            id
                   ORDER BY tech$load_dt)
