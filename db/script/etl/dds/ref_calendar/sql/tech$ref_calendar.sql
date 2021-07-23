INSERT
  INTO tech$ref_calendar
  (
    tech$load_id,
    tech$load_dt,
    tech$record_source,
    clndr_dt,
    holiday_flag
  )
SELECT
       :tech$load_id,
       tech$effective_dt AS tech$load_dt,
       'master_data'     AS tech$record_source,
       full_date         AS clndr_dt,
       holiday_flag
  FROM src.master_data_ref_calendar
 WHERE tech$expiration_dt = '2999-12-31'
   AND tech$effective_dt >= :tech$effective_dt
