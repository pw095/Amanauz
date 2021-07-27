DROP TABLE IF EXISTS tech$master_data_ref_calendar;
CREATE TABLE tech$master_data_ref_calendar
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    full_date          TEXT    NOT NULL,
    holiday_flag       TEXT    NOT NULL,
    PRIMARY KEY(full_date, tech$effective_dt)
  )
WITHOUT ROWID;
