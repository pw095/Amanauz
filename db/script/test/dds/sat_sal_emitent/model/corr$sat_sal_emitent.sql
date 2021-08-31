DROP TABLE IF EXISTS corr$sat_sal_emitent;
CREATE TABLE corr$sat_sal_emitent
  (
    tech$load_id       INTEGER,
    tech$hash_key      TEXT    NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    full_name          TEXT    NOT NULL,
    short_name         TEXT    NOT NULL,
    reg_date           TEXT,
    ogrn               TEXT,
    inn                TEXT,
    okpo               TEXT,
    PRIMARY KEY(tech$hash_key, tech$effective_dt)
  )
WITHOUT ROWID;
