DROP TABLE IF EXISTS tech$security_types;
CREATE TABLE tech$security_types
  (
    tech$load_id        INTEGER NOT NULL,
    tech$effective_dt   TEXT    NOT NULL,
    tech$expiration_dt  TEXT    NOT NULL,
    tech$hash_value     TEXT    NOT NULL,
    id                  INTEGER NOT NULL,
    trade_engine_id     INTEGER NOT NULL,
    trade_engine_name   TEXT    NOT NULL,
    trade_engine_title  TEXT    NOT NULL,
    security_type_name  TEXT    NOT NULL,
    security_type_title TEXT    NOT NULL,
    security_group_name TEXT    NOT NULL,
    PRIMARY KEY(security_type_name, tech$effective_dt)
  )
WITHOUT ROWID;
