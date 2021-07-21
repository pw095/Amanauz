DROP TABLE IF EXISTS tech$default_data_emitent;
CREATE TABLE tech$default_data_emitent
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    emitent_code       TEXT    NOT NULL,
    emitent_name       TEXT    NOT NULL,
    PRIMARY KEY(emitent_code, tech$effective_dt)
  )
WITHOUT ROWID;
