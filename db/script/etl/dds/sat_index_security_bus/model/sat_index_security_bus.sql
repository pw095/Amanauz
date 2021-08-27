DROP TABLE IF EXISTS sat_index_security_bus;
CREATE TABLE sat_index_security_bus
  (
    tech$load_id       INTEGER NOT NULL,
    tech$hash_key      TEXT    NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    weight             REAL    NOT NULL,
    PRIMARY KEY(tech$hash_key, tech$effective_dt),
    FOREIGN KEY(tech$hash_key) REFERENCES lnk_index_security_bus(tech$hash_key)
  )
WITHOUT ROWID;
