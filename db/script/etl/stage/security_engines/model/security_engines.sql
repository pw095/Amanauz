DROP TABLE security_engines;
CREATE TABLE security_engines
  (
    tech$load_id   INTEGER NOT NULL,
    tech$load_dttm TEXT    NOT NULL,
    id             INTEGER,
    name           TEXT,
    title          TEXT
  );
