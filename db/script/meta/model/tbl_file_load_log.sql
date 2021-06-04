DROP TABLE tbl_file_load_log;
CREATE TABLE tbl_file_load_log
  (
    fll_id       INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    fll_ell_id   INTEGER                           NOT NULL,
    fll_name     TEXT                              NOT NULL,
    fll_size     INTEGER                           NOT NULL,
    fll_hash_sum TEXT                              NOT NULL,
    UNIQUE (fll_ell_id, fll_name),
    FOREIGN KEY (fll_ell_id) REFERENCES tbl_entity_load_log(ell_id) ON DELETE CASCADE
  );
