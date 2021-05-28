DROP TABLE security_groups;
CREATE TABLE security_groups
  (
    id           INTEGER,
    name         TEXT,
    title        TEXT,
    is_hidden    INTEGER,
    tech$load_id INTEGER NOT NULL
  );
