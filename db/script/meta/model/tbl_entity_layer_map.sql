DROP TABLE tbl_entity_layer_map;
CREATE TABLE tbl_entity_layer_map
  (
    elm_id       INTEGER PRIMARY KEY NOT NULL,
    elm_ent_id   INTEGER             NOT NULL,
    elm_layer_id INTEGER             NOT NULL,
    elm_mode     TEXT                NOT NULL,
    elm_enabled  TEXT                NOT NULL,
    UNIQUE (elm_ent_id, elm_layer_id),
    FOREIGN KEY (elm_ent_id)   REFERENCES tbl_entity(ent_id)  ON DELETE CASCADE,
    FOREIGN KEY (elm_layer_id) REFERENCES tbl_layer(layer_id) ON DELETE CASCADE
  );
