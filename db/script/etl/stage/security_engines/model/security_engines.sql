DROP TABLE security_engines;
CREATE TABLE security_engines
  (
    id           INTEGER NOT NULL,
    name         TEXT    NOT NULL,
    title        TEXT    NOT NULL,
    tech$load_id INTEGER NOT NULL
  );
