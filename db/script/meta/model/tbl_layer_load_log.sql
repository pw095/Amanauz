DROP TABLE tbl_layer_load_log;
CREATE TABLE tbl_layer_load_log
  (
    lll_id          INTEGER PRIMARY KEY NOT NULL,
    lll_load_id     INTEGER             NOT NULL,
    lll_layer_id    INTEGER             NOT NULL,
    lll_status      TEXT,
    lll_start_dttm  TEXT                NOT NULL,
    lll_finish_dttm TEXT,
    UNIQUE(lll_load_id, lll_layer_id),
    FOREIGN KEY (lll_load_id)  REFERENCES tbl_load_log(load_id) ON DELETE CASCADE,
    FOREIGN KEY (lll_layer_id) REFERENCES tbl_layer(layer_id)   ON DELETE CASCADE
  );
