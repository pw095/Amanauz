DROP TABLE IF EXISTS master_data_emitent;
CREATE TABLE master_data_emitent
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    full_name          TEXT    NOT NULL,
    short_name         TEXT    NOT NULL,
    reg_date           TEXT,
    ogrn               TEXT,
    inn                TEXT,
    PRIMARY KEY(short_name, tech$effective_dt)
  )
WITHOUT ROWID;
