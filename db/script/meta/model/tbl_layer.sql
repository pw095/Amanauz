DROP TABLE tbl_layer;
CREATE TABLE tbl_layer
  (
    layer_id   INTEGER PRIMARY KEY NOT NULL,
    layer_code TEXT                NOT NULL,
    layer_desc TEXT,
    UNIQUE(layer_code)
  );
