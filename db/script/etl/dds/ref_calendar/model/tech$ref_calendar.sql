DROP TABLE IF EXISTS tech$ref_calendar;
CREATE TABLE tech$ref_calendar
  (
    tech$load_id       INTEGER NOT NULL,
    tech$load_dt       TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    clndr_dt           TEXT    NOT NULL,
    holiday_flag       TEXT    NOT NULL,
    PRIMARY KEY(clndr_dt)
  )
WITHOUT ROWID;
