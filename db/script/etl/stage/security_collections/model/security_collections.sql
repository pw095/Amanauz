DROP TABLE security_collections;
CREATE TABLE security_collections
  (
    tech$load_id      INTEGER NOT NULL,
    tech$load_dttm    TEXT    NOT NULL,
    id                INTEGER,
    name              TEXT,
    title             TEXT,
    security_group_id INTEGER
  );
