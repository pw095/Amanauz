INSERT
  INTO master_data_ref_calendar
  (
    tech$load_id,
    tech$load_dttm,
    full_date,
    holiday_flag
  )
VALUES
  (
    :tech$load_id,
    :tech$load_dttm,
    :full_date,
    :holiday_flag
  )
