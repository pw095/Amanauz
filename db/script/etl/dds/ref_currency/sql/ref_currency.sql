ATTACH DATABASE 'C:\Users\Pavel.Gnezdilov\Documents\Amanauz\db\file\repl.db' AS src;
INSERT
  INTO ref_currency
  (
    tech$load_id,
    tech$load_dt,
    tech$record_source,
    crnc_code,
    iso_char_code,
    iso_num_code,
    rus_name,
    eng_name,
    nominal
  )
SELECT
       :tech$load_id     AS tech$load_id,
       tech$effective_dt AS tech$load_dt,
       'cbr.ru'          AS tech$record_source,
       id                AS crnc_code,
       iso_char_code,
       iso_num_code,
       name AS rus_name,
       eng_name,
       CAST(nominal AS INTEGER) AS nominal
  FROM src.foreign_currency_dictionary src
 WHERE NOT EXISTS(SELECT
                         NULL
                    FROM ref_currency sat
                   WHERE sat.crnc_code = src.id
                     AND sat.iso_char_code = src.iso_char_code
                     AND sat.iso_num_code = src.iso_num_code
                     AND sat.rus_name = src.name
                     AND sat.eng_name = src.eng_name
                     AND sat.nominal = CAST(src.nominal AS INTEGER))
   AND tech$expiration_dt = '2999-12-31'
   AND tech$effective_dt >= :tech$effective_dt
   AND iso_char_code IS NOT NULL
   AND iso_num_code IS NOT NULL
ON CONFLICT(crnc_code)
DO UPDATE
   SET tech$load_id = excluded.tech$load_id,
       tech$load_dt = excluded.tech$load_dt,
       iso_char_code = excluded.iso_char_code,
       iso_num_code = excluded.iso_num_code,
       rus_name = excluded.rus_name,
       eng_name = excluded.eng_name,
       nominal = excluded.nominal
