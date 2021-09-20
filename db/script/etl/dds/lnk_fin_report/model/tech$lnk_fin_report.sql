DROP TABLE IF EXISTS tech$lnk_fin_report;
CREATE TABLE tech$lnk_fin_report
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
    UNIQUE(crnc_code, fs_code, emitent_hash_key, report_dt)
  )
WITHOUT ROWID;
