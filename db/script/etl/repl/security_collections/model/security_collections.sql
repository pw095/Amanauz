DROP TABLE IF EXISTS security_collections;
CREATE TABLE security_collections
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    id                 INTEGER NOT NULL,
    name               TEXT    NOT NULL,
    title              TEXT    NOT NULL,
    security_group_id  INTEGER NOT NULL,
    PRIMARY KEY(id, tech$effective_dt)
  )
WITHOUT ROWID;
