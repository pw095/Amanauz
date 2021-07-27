DROP TABLE IF EXISTS sat_emitent_moex;
CREATE TABLE sat_emitent_moex
  (
    tech$load_id       INTEGER NOT NULL,
    tech$hash_key      TEXT    NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    title              TEXT    NOT NULL,
    inn                TEXT    NOT NULL,
    okpo               TEXT    NOT NULL,
    PRIMARY KEY(tech$hash_key, tech$effective_dt),
    FOREIGN KEY (tech$hash_key) REFERENCES hub_emitent(tech$hash_key) ON DELETE CASCADE
  )
WITHOUT ROWID;
