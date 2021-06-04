DROP TABLE IF EXISTS tbl_relation_load_log;
CREATE TABLE tbl_relation_load_log
  (
    rll_id                INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    rll_ell_id            INTEGER UNIQUE                    NOT NULL,
    rll_mode              TEXT                              NOT NULL,
    rll_effective_from_dt TEXT                              NOT NULL,
    rll_effective_to_dt   TEXT                              NOT NULL,
    FOREIGN KEY (rll_ell_id) REFERENCES tbl_entity_load_log(ell_id) ON DELETE CASCADE
  );
