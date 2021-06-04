DROP TABLE IF EXISTS tbl_entity_load_log;
CREATE TABLE tbl_entity_load_log
  (
    ell_id                         INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    ell_flow_log_id                INTEGER                           NOT NULL,
    ell_elm_id                     INTEGER                           NOT NULL,
    ell_iteration_number           INTEGER                           NOT NULL,
    ell_iteration_insert_row_count INTEGER                           NOT NULL,
    ell_iteration_update_row_count INTEGER                           NOT NULL,
    ell_iteration_delete_row_count INTEGER                           NOT NULL,
    ell_status                     TEXT                              NOT NULL,
    ell_start_dttm                 TEXT                              NOT NULL,
    ell_finish_dttm                TEXT                              NOT NULL,
    ell_error_message              TEXT,
    UNIQUE(ell_flow_log_id, ell_elm_id),
    UNIQUE(ell_elm_id, ell_iteration_number),
    FOREIGN KEY (ell_flow_log_id) REFERENCES tbl_flow_log(flow_id)        ON DELETE CASCADE,
    FOREIGN KEY (ell_elm_id)      REFERENCES tbl_entity_layer_map(elm_id) ON DELETE CASCADE
  );
