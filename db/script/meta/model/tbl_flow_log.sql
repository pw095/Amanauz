DROP TABLE tbl_flow_log;
CREATE TABLE tbl_flow_log
  (
    flow_id          INTEGER PRIMARY KEY NOT NULL,
    flow_status      TEXT                NOT NULL,
    flow_start_dttm  TEXT                NOT NULL,
    flow_finish_dttm TEXT
  );
