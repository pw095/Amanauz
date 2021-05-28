DROP TABLE security_types;
CREATE TABLE security_types
  (
    id                  INTEGER,
    trade_engine_id     INTEGER,
    trade_engine_name   TEXT,
    trade_engine_title  TEXT,
    security_type_name  TEXT,
    security_type_title TEXT,
    security_group_name TEXT,
    tech$load_id        INTEGER NOT NULL
  );
