DROP TABLE tbl_entity_load_log;
CREATE TABLE tbl_entity_load_log
  (
    ell_id                         INTEGER PRIMARY KEY NOT NULL,
    ell_layer_load_log_id          INTEGER             NOT NULL,
    ell_ent_id                     INTEGER             NOT NULL,
    ell_iteration_number           INTEGER             NOT NULL,
    ell_iteration_update_row_count INTEGER,
    ell_status                     TEXT,
    ell_start_dttm                 TEXT                NOT NULL,
    ell_finish_dttm                TEXT,
    ell_error_message              TEXT,
    UNIQUE(ell_layer_load_log_id, ell_ent_id),
    UNIQUE(ell_ent_id, ell_iteration_number),
    FOREIGN KEY (ell_layer_load_log_id) REFERENCES tbl_layer_load_log(lll_id) ON DELETE CASCADE,
    FOREIGN KEY (ell_ent_id)            REFERENCES tbl_entity(ent_id)         ON DELETE CASCADE
  );
