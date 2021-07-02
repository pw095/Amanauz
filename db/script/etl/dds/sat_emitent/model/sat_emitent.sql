DROP TABLE IF EXISTS sat_emitent;
CREATE TABLE sat_emitent
  (
    tech$load_id       INTEGER NOT NULL,
    tech$hash_key      TEXT    NOT NULL,
    effective_dt       TEXT    NOT NULL,
    expiration_dt      TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    full_name          TEXT    NOT NULL,
    short_name         TEXT    NOT NULL,
    reg_dt             TEXT,
    ogrn               TEXT,
    inn                TEXT,
    okpo               TEXT    NOT NULL,
    PRIMARY KEY(tech$hash_key, effective_dt)
  )
WITHOUT ROWID;
