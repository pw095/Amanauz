DROP TABLE tbl_elm_dependency;
CREATE TABLE tbl_elm_dependency
  (
    elmd_id            INTEGER PRIMARY KEY NOT NULL,
    elmd_elm_id        INTEGER             NOT NULL,
    elmd_parent_elm_id INTEGER,
    UNIQUE(elmd_elm_id, elmd_parent_elm_id),
    FOREIGN KEY (elmd_elm_id)        REFERENCES tbl_entity_layer_map(elm_id) ON DELETE CASCADE,
    FOREIGN KEY (elmd_parent_elm_id) REFERENCES tbl_entity_layer_map(elm_id) ON DELETE CASCADE
  );
