DROP TABLE IF EXISTS lnk_security_type;
CREATE TABLE lnk_security_type
  (
    tech$load_id           INTEGER NOT NULL,
    tech$hash_key          TEXT    NOT NULL,
    tech$record_source     TEXT    NOT NULL,
    tech$load_dt           TEXT    NOT NULL,
    tech$last_seen_dt      TEXT    NOT NULL,
    security_hash_key      TEXT    NOT NULL,
    security_type_hash_key TEXT    NOT NULL,
    PRIMARY KEY(tech$hash_key),
    UNIQUE(security_hash_key, security_type_hash_key),
    FOREIGN KEY(security_hash_key)      REFERENCES hub_security(tech$hash_key),
    FOREIGN KEY(security_type_hash_key) REFERENCES hub_security_type(tech$hash_key)
  )
WITHOUT ROWID;
