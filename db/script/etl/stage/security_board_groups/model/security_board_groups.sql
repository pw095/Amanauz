DROP TABLE security_board_groups;
CREATE TABLE security_board_groups
  (
    id                 INTEGER,
    trade_engine_id    INTEGER,
    trade_engine_name  TEXT,
    trade_engine_title TEXT,
    market_id          INTEGER,
    market_name        TEXT,
    name               TEXT,
    title              TEXT,
    is_default         INTEGER,
    board_group_id     INTEGER,
    is_traded          INTEGER,
    tech$load_id       INTEGER NOT NULL
  );
