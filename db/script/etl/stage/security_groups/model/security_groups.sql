DROP TABLE IF EXISTS security_groups;
CREATE TABLE security_groups
  (
    tech$load_id   INTEGER NOT NULL,
    tech$load_dttm TEXT    NOT NULL,
    id             INTEGER,
    name           TEXT,
    title          TEXT,
    is_hidden      INTEGER
  );
