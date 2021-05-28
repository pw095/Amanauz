DROP TABLE security_boards;
CREATE TABLE security_boards
  (
    id             INTEGER,
    board_group_id INTEGER,
    engine_id      INTEGER,
    market_id      INTEGER,
    board_id       TEXT,
    board_title    TEXT,
    is_traded      INTEGER,
    has_candles    INTEGER,
    is_primary     INTEGER,
    tech$load_id   INTEGER NOT NULL
  );
