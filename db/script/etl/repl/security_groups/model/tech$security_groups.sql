DROP TABLE IF EXISTS tech$security_engines;
CREATE TABLE tech$security_engines
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    id                 INTEGER NOT NULL,
    name               TEXT    NOT NULL,
    title              TEXT    NOT NULL,
    is_hidden          INTEGER NOT NULL,
    PRIMARY KEY(name, tech$effective_dt)
  )
WITHOUT ROWID;
