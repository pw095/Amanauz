DROP TABLE IF EXISTS sat_emitent_master_data;
CREATE TABLE sat_emitent_master_data
  (
    tech$load_id       INTEGER NOT NULL,
    tech$hash_key      TEXT    NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    full_name          TEXT    NOT NULL,
    short_name         TEXT    NOT NULL,
    reg_dt             TEXT,
    ogrn               TEXT,
    inn                TEXT,
    PRIMARY KEY(tech$hash_key, tech$effective_dt)
  )
WITHOUT ROWID;
