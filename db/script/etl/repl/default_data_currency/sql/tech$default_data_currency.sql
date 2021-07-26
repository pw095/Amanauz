INSERT
  INTO tech$default_data_currency
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$last_seen_dt,
    tech$hash_value,
    iso_char_code,
    iso_num_code,
    rus_name,
    eng_name,
    nominal
  )
WITH
     w_raw AS
     (
         SELECT
                tech$load_dt,
                sha1(concat_value) AS hash_value,
                iso_char_code,
                iso_num_code,
                rus_name,
                eng_name,
                nominal
           FROM (SELECT
                        tech$load_dt,
                        '_' || IFNULL(CAST(iso_num_code AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(rus_name     AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(eng_name     AS TEXT), '!@#\$%^&*') ||
                        '_' || IFNULL(CAST(nominal      AS TEXT), '!@#\$%^&*') || '_' AS concat_value,
                        iso_char_code,
                        iso_num_code,
                        rus_name,
                        eng_name,
                        nominal
                   FROM (SELECT
                                tech$load_dt,
                                ROW_NUMBER() OVER (wnd) AS rn,
                                iso_char_code,
                                iso_num_code,
                                rus_name,
                                eng_name,
                                nominal
                           FROM (SELECT
                                        DATE(tech$load_dttm) AS tech$load_dt,
                                        tech$load_dttm       AS tech$load_dttm,
                                        iso_char_code,
                                        iso_num_code,
                                        rus_name,
                                        eng_name,
                                        nominal
                                   FROM src.default_data_currency
                                  WHERE tech$load_dttm >= :tech$load_dttm)
                         WINDOW wnd AS (PARTITION BY
                                                     iso_char_code,
                                                     tech$load_dt
                                            ORDER BY tech$load_dttm DESC))
                  WHERE rn = 1)
     )
SELECT
       :tech$load_id                                                  AS tech$load_id,
       tech$load_dt                                                   AS tech$effective_dt,
       LEAD(DATE(tech$load_dt, '-1 DAY'), 1, '2999-12-31') OVER (wnd) AS tech$expiration_dt,
       tech$last_seen_dt,
       hash_value                                                     AS tech$hash_value,
       iso_char_code,
       iso_num_code,
       rus_name,
       eng_name,
       nominal
  FROM (SELECT
               tech$load_dt,
               tech$last_seen_dt,
               hash_value,
               iso_char_code,
               iso_num_code,
               rus_name,
               eng_name,
               nominal
          FROM (SELECT
                       tech$load_dt,
                       MAX(tech$load_dt) OVER (win) AS tech$last_seen_dt,
                       hash_value,
                       LAG(hash_value) OVER (wnd)   AS lag_hash_value,
                       iso_char_code,
                       iso_num_code,
                       rus_name,
                       eng_name,
                       nominal
                  FROM w_raw
                WINDOW win AS (PARTITION BY
                                            iso_char_code),
                       wnd AS (PARTITION BY
                                            iso_char_code
                                   ORDER BY tech$load_dt))
         WHERE hash_value != lag_hash_value
            OR lag_hash_value IS NULL)
WINDOW wnd AS (PARTITION BY
                            iso_char_code
                   ORDER BY tech$load_dt)
