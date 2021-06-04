DROP TABLE IF EXISTS tbl_file_load_queue;
CREATE TABLE tbl_file_load_queue
  (
    flq_id       INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    flq_elq_id   INTEGER                           NOT NULL,
    flq_name     TEXT                              NOT NULL,
    flq_size     INTEGER                           NOT NULL,
    flq_hash_sum TEXT                              NOT NULL,
    UNIQUE (flq_elq_id, flq_name),
    FOREIGN KEY (flq_elq_id) REFERENCES tbl_entity_load_queue(elq_id) ON DELETE CASCADE
  );
