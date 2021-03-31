DROP TABLE tbl_relation_load_log;
CREATE TABLE tbl_relation_load_log
  (
    rll_id                INTEGER PRIMARY KEY NOT NULL,
    rll_ell_id            INTEGER             NOT NULL,
    rll_mode              TEXT                NOT NULL,
    rll_effective_from_dt TEXT                NOT NULL,
    rll_effective_to_dt   TEXT                NOT NULL,
    UNIQUE (rll_ell_id),
    FOREIGN KEY (rll_ell_id) REFERENCES tbl_entity_load_log(ell_id) ON DELETE CASCADE
  );
