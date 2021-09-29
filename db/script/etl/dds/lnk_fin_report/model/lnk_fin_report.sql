DROP TABLE IF EXISTS lnk_fin_report;
CREATE TABLE lnk_fin_report
  (
    tech$load_id       INTEGER NOT NULL,
    tech$hash_key      TEXT    NOT NULL,
    tech$load_dt       TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    crnc_code          TEXT    NOT NULL,
    fs_code            TEXT    NOT NULL,
    emitent_hash_key   TEXT    NOT NULL,
    report_dt          TEXT    NOT NULL,
    PRIMARY KEY(tech$hash_key),
    UNIQUE(crnc_code, fs_code, emitent_hash_key, report_dt),
    FOREIGN KEY(crnc_code)        REFERENCES ref_currency(crnc_code)     ON DELETE CASCADE,
    FOREIGN KEY(fs_code)          REFERENCES ref_fin_statement(fs_code)  ON DELETE CASCADE,
    FOREIGN KEY(emitent_hash_key) REFERENCES hub_emitent(tech$hash_key)  ON DELETE CASCADE,
    FOREIGN KEY(report_dt)        REFERENCES ref_calendar(clndr_dt)      ON DELETE CASCADE
  )
WITHOUT ROWID;
