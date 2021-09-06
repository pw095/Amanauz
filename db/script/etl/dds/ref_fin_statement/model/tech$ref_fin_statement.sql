DROP TABLE IF EXISTS tech$ref_fin_statement;
CREATE TABLE tech$ref_fin_statement
  (
    tech$load_id       INTEGER NOT NULL,
    tech$load_dt       TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    fs_code            TEXT    NOT NULL,
    parent_code        TEXT    NOT NULL,
    full_name          TEXT    NOT NULL,
    leaf_code          TEXT    NOT NULL,
    parent_leaf_code   TEXT    NOT NULL,
    hier_level         INTEGER NOT NULL,
    PRIMARY KEY(fs_code)
  )
WITHOUT ROWID;
