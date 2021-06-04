DROP TABLE IF EXISTS tbl_relation_load_queue;
CREATE TABLE tbl_relation_load_queue
  (
    rlq_id                INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    rlq_elq_id            INTEGER UNIQUE                    NOT NULL,
    rlq_mode              TEXT                              NOT NULL,
    rlq_effective_from_dt TEXT                              NOT NULL,
    rlq_effective_to_dt   TEXT                              NOT NULL,
    FOREIGN KEY (rlq_elq_id) REFERENCES tbl_entity_load_queue(elq_id) ON DELETE CASCADE
  );
