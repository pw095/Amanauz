DROP TABLE IF EXISTS hub_emitent;
CREATE TABLE hub_emitent
  (
    emit_hkey      TEXT NOT NULL,
    load_date      TEXT NOT NULL,
    last_seen_date TEXT NOT NULL,
    record_source  TEXT NOT NULL,
    emit_code      TEXT NOT NULL,
    PRIMARY KEY(emit_hkey),
    UNIQUE(emit_code, record_source)
  )
WITHOUT ROWID;
