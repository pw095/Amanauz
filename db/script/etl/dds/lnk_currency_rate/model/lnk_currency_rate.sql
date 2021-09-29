DROP TABLE IF EXISTS lnk_currency_rate;
CREATE TABLE lnk_currency_rate
  (
    tech$load_id       INTEGER NOT NULL,
    tech$hash_key      TEXT    NOT NULL,
    tech$load_dt       TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    trade_dt           TEXT    NOT NULL,
    crnc_code          TEXT    NOT NULL,
    PRIMARY KEY(tech$hash_key),
    UNIQUE(trade_dt, crnc_code),
    FOREIGN KEY(trade_dt)  REFERENCES ref_calendar(clndr_dt)  ON DELETE CASCADE,
    FOREIGN KEY(crnc_code) REFERENCES ref_currency(crnc_code) ON DELETE CASCADE
  )
WITHOUT ROWID;
