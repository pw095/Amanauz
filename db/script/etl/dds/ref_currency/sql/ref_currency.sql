attach database 'C:\Users\pw095\Documents\Git\Amanauz\db\file\repl.db' as src1;
DETACH DATABASE SRC;
attach database 'C:\Users\pw095\Documents\SQLite\test_run\db\file\repl.db' as src;
SELECT * FROM src.foreign_currency_dictionary;
SELECT * FROM ref_currency;
INSERT
  INTO ref_currency
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
SELECT
       :tech$load_id      AS tech$load_id,
       tech$load_dt,
       tech$record_source,
       crnc_code,
       iso_num_code,
       rus_name,
       eng_name,
       nominal
  FROM (SELECT
               tech$effective_dt            AS tech$load_dt,
               tech$record_source,
               crnc_code,
               printf('%03d', iso_num_code) AS iso_num_code,
               rus_name,
               eng_name,
               nominal
          FROM (SELECT
                       tech$effective_dt,
                       tech$record_source,
                       crnc_code,
                       iso_num_code,
                       rus_name,
                       eng_name,
                       nominal
                  FROM (SELECT
                               tech$effective_dt,
                               'cbr.ru'                 AS tech$record_source,
                               iso_char_code            AS crnc_code,
                               iso_num_code,
                               name                     AS rus_name,
                               eng_name,
                               CAST(nominal AS INTEGER) AS nominal,
                               COUNT() OVER (PARTITION BY iso_char_code) AS cnt
                          FROM src.foreign_currency_dictionary src
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
                  FROM src1.default_data_currency
                 WHERE tech$expiration_dt = '2999-12-31'
                   AND tech$effective_dt >= :tech$effective_dt)) src
 WHERE 1 = 1
   AND NOT EXISTS(SELECT
                         NULL
                    FROM ref_currency sat
                   WHERE sat.crnc_code = src.crnc_code
                     AND sat.iso_num_code = src.iso_num_code
                     AND sat.rus_name = src.rus_name
                     AND sat.eng_name = src.eng_name
                     AND sat.nominal = src.nominal)
ON CONFLICT(crnc_code)
DO UPDATE
   SET tech$load_id = excluded.tech$load_id,
       tech$load_dt = excluded.tech$load_dt,
       iso_num_code = excluded.iso_num_code,
       rus_name = excluded.rus_name,
       eng_name = excluded.eng_name,
       nominal = excluded.nominal
