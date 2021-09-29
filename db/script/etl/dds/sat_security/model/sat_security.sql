DROP TABLE IF EXISTS sat_security;
CREATE TABLE sat_security
  (
    tech$load_id       INTEGER NOT NULL,
    tech$hash_key      TEXT    NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    security_id        TEXT    NOT NULL,
    full_name          TEXT    NOT NULL,
    short_name         TEXT    NOT NULL,
    isin               TEXT,
    reg_number         TEXT    NOT NULL,
    issue_size         INTEGER,
    face_value         REAL    NOT NULL,
    face_crnc          TEXT,
    list_level         INTEGER NOT NULL,
    PRIMARY KEY(tech$hash_key, tech$effective_dt),
    FOREIGN KEY(tech$hash_key) REFERENCES hub_security(tech$hash_key) ON DELETE CASCADE
  )
WITHOUT ROWID;
