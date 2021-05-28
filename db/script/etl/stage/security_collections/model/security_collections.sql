DROP TABLE security_collections;
CREATE TABLE security_collections
  (
    id                INTEGER,
    name              TEXT,
    title             TEXT,
    security_group_id INTEGER,
    tech$load_id      INTEGER NOT NULL
  );
