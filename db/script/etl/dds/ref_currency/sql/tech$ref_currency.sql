INSERT
  INTO tech$ref_currency
  (
    tech$load_id,
    tech$load_dt,
    tech$record_source,
    crnc_code,
    iso_num_code,
    rus_name,
    eng_name,
    nominal
  )
WITH
     w_pre AS
     (
         SELECT
                tech$effective_dt,
                tech$record_source,
                crnc_code,
                iso_num_code,
                rus_name,
                eng_name,
                nominal
           FROM (SELECT
                        tech$effective_dt,
                        'cbr.ru'                                   AS tech$record_source,
                        iso_char_code                              AS crnc_code,
                        iso_num_code,
                        name                                       AS rus_name,
                        eng_name,
                        CAST(nominal AS INTEGER)                   AS nominal,
                        COUNT(*) OVER (PARTITION BY iso_char_code) AS cnt
                   FROM src.foreign_currency_dictionary
                  WHERE 1 = 1
                    AND tech$expiration_dt = '2999-12-31'
                    AND tech$effective_dt >= :tech$effective_dt
                    AND iso_char_code IS NOT NULL
                    AND iso_num_code IS NOT NULL)
          WHERE cnt = 1
          UNION ALL
         SELECT
                tech$effective_dt,
                'master_data'     AS tech$record_source,
                iso_char_code     AS crnc_code,
                iso_num_code,
                rus_name,
                eng_name,
                1
           FROM src.default_data_currency
          WHERE tech$expiration_dt = '2999-12-31'
            AND tech$effective_dt >= :tech$effective_dt
     )
SELECT
       :tech$load_id                AS tech$load_id,
       tech$effective_dt            AS tech$load_dt,
       tech$record_source,
       crnc_code,
       printf('%03d', iso_num_code) AS iso_num_code,
       rus_name,
       eng_name,
       nominal
  FROM w_pre
