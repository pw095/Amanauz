DROP TABLE IF EXISTS cmp$sat_sal_emitent;
CREATE TABLE cmp$sat_sal_emitent
  (
    env                TEXT    NOT NULL,
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
    okpo               TEXT
  );
