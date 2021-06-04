DROP TABLE tbl_entity_load_queue;
CREATE TABLE tbl_entity_load_queue
  (
    elq_id                         INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    elq_flow_id                    INTEGER                           NOT NULL,
    elq_elm_id                     INTEGER                           NOT NULL,
    elq_iteration_number           INTEGER                           NOT NULL,
    elq_iteration_insert_row_count INTEGER,
    elq_iteration_update_row_count INTEGER,
    elq_iteration_delete_row_count INTEGER,
    elq_status                     TEXT                              NOT NULL,
    elq_start_dttm                 TEXT,
    elq_finish_dttm                TEXT,
    elq_error_message              TEXT,
    UNIQUE(elq_flow_id, elq_elm_id),
    UNIQUE(elq_elm_id, elq_iteration_number),
    FOREIGN KEY (elq_flow_id) REFERENCES tbl_flow_log(flow_id)        ON DELETE CASCADE,
    FOREIGN KEY (elq_elm_id)  REFERENCES tbl_entity_layer_map(elm_id) ON DELETE CASCADE
  );
