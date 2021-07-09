INSERT
  INTO ref_calendar
  (
    tech$load_id,
    tech$load_dttm,
    full_date,
    week_day_flag
  )
VALUES
  (
    :tech$load_id,
    :tech$load_dttm,
    :full_date,
    :week_day_flag
  )
