DROP TABLE tbl_entity;
CREATE TABLE tbl_entity
  (
    ent_id   INTEGER PRIMARY KEY NOT NULL,
    ent_code TEXT                NOT NULL,
    ent_desc TEXT,
    UNIQUE (ent_code)
  );
