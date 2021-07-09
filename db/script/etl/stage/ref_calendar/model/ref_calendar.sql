DROP TABLE IF EXISTS ref_calendar;
CREATE TABLE ref_calendar
  (
    tech$load_id   INTEGER NOT NULL,
    tech$load_dttm TEXT    NOT NULL,
    full_date      TEXT    NOT NULL,
    week_day_flag  TEXT    NOT NULL
  );
