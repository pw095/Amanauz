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
  FROM tech$ref_currency src
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
       tech$record_source = excluded.tech$record_source,
       iso_num_code = excluded.iso_num_code,
       rus_name = excluded.rus_name,
       eng_name = excluded.eng_name,
       nominal = excluded.nominal
