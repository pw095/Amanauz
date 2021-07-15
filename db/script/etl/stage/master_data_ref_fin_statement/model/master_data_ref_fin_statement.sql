DROP TABLE IF EXISTS master_data_ref_fin_statement;
CREATE TABLE master_data_ref_fin_statement
  (
    tech$load_id     INTEGER NOT NULL,
    tech$load_dttm   TEXT    NOT NULL,
    hier_level       INTEGER NOT NULL,
    leaf_code        TEXT    NOT NULL,
    code             TEXT    NOT NULL,
    parent_leaf_code TEXT    NOT NULL,
    parent_code      TEXT    NOT NULL,
    full_name        TEXT    NOT NULL
  );
