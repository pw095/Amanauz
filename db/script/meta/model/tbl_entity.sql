DROP TABLE tbl_entity;
CREATE TABLE tbl_entity
  (
    ent_id       INTEGER PRIMARY KEY NOT NULL,
    ent_layer_id INTEGER             NOT NULL,
    ent_code     TEXT                NOT NULL,
    ent_desc     TEXT,
    UNIQUE(ent_layer_id, ent_code),
    FOREIGN KEY (ent_layer_id) REFERENCES tbl_layer(layer_id) ON DELETE CASCADE
  );
