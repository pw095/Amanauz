DROP TABLE tbl_load_log;
CREATE TABLE tbl_load_log
  (
    load_id          INTEGER PRIMARY KEY NOT NULL,
    load_status      TEXT,
    load_start_dttm  TEXT                NOT NULL,
    load_finish_dttm TEXT
  );
