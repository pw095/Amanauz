DROP TABLE IF EXISTS sat_fin_report;
CREATE TABLE sat_fin_report
  (
    tech$load_id       INTEGER NOT NULL,
    tech$hash_key      TEXT    NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    value              REAL    NOT NULL,
    PRIMARY KEY(tech$hash_key, tech$effective_dt),
    FOREIGN KEY(tech$hash_key) REFERENCES lnk_fin_report(tech$hash_key) ON DELETE CASCADE
  )
WITHOUT ROWID;
