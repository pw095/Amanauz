DROP TABLE IF EXISTS tbl_entity;
CREATE TABLE tbl_entity
  (
    ent_id   INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    ent_code TEXT    UNIQUE                    NOT NULL,
    ent_desc TEXT
  );
