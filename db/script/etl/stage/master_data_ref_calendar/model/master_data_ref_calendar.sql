DROP TABLE IF EXISTS master_data_ref_calendar;
CREATE TABLE master_data_ref_calendar
  (
    tech$load_id   INTEGER NOT NULL,
    tech$load_dttm TEXT    NOT NULL,
    full_date      TEXT    NOT NULL,
    holiday_flag   TEXT    NOT NULL
  );
