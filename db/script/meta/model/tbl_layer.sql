DROP TABLE IF EXISTS tbl_layer;
CREATE TABLE tbl_layer
  (
    layer_id   INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    layer_code TEXT    UNIQUE                    NOT NULL,
    layer_desc TEXT
  );
