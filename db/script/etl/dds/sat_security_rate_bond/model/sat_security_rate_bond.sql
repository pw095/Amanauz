DROP TABLE IF EXISTS sat_security_rate_bond;
CREATE TABLE sat_security_rate_bond
  (
    tech$load_id       INTEGER NOT NULL,
    tech$hash_key      TEXT    NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    accrued_interest   REAL    NOT NULL,
    yield              REAL    NOT NULL,
    duration           REAL    NOT NULL,
    PRIMARY KEY(tech$hash_key, tech$effective_dt),
    FOREIGN KEY (tech$hash_key) REFERENCES lnk_security_rate(tech$hash_key) ON DELETE CASCADE
  )
WITHOUT ROWID;
