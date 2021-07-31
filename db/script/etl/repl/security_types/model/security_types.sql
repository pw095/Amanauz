DROP TABLE IF EXISTS security_types;
CREATE TABLE security_types
  (
    tech$load_id        INTEGER NOT NULL,
    tech$effective_dt   TEXT    NOT NULL,
    tech$expiration_dt  TEXT    NOT NULL,
    tech$last_seen_dt   TEXT    NOT NULL,
    tech$hash_value     TEXT    NOT NULL,
    id                  INTEGER,
    trade_engine_id     INTEGER,
    trade_engine_name   TEXT,
    trade_engine_title  TEXT,
    security_type_name  TEXT    NOT NULL,
    security_type_title TEXT,
    security_group_name TEXT,
    PRIMARY KEY(security_type_name, tech$effective_dt)
  )
WITHOUT ROWID;
