DROP TABLE IF EXISTS ref_calendar;
CREATE TABLE ref_calendar
  (
    tech$load_id       INTEGER NOT NULL,
    tech$load_dt       TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    clndr_dt           TEXT    NOT NULL,
    day_num_in_week    INTEGER NOT NULL,
    day_num_in_month   INTEGER NOT NULL,
    day_num_in_year    INTEGER NOT NULL,
    day_name           TEXT    NOT NULL,
    day_abbrev         TEXT    NOT NULL,
    holiday_flag       TEXT    NOT NULL,
    week_num_in_year   INTEGER NOT NULL,
    week_begin_date    TEXT    NOT NULL,
    month_num_in_year  INTEGER NOT NULL,
    month_name         TEXT    NOT NULL,
    month_abbrev       TEXT    NOT NULL,
    quarter            INTEGER NOT NULL,
    year               INTEGER NOT NULL,
    PRIMARY KEY (clndr_dt)
  )
WITHOUT ROWID;
