DROP TABLE IF EXISTS hub_emitent;
CREATE TABLE hub_emitent
  (
    emit_hkey      TEXT NOT NULL,
    load_date      TEXT NOT NULL,
    load_end_date  TEXT NOT NULL,
    record_source  TEXT NOT NULL,
    hdiff          TEXT NOT NULL,
    full_name      TEXT NOT NULL,
    short_name     TEXT NOT NULL,
    reg_date       TEXT NOT NULL,
    ogrn           TEXT NOT NULL,
    inn            TEXT NOT NULL,
    PRIMARY KEY(emit_hkey, load_date, record_source)
  )
WITHOUT ROWID;
