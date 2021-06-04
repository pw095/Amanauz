DROP TABLE IF EXISTS security_boards;
CREATE TABLE security_boards
  (
    tech$load_id   INTEGER NOT NULL,
    tech$load_dttm TEXT    NOT NULL,
    id             INTEGER,
    board_group_id INTEGER,
    engine_id      INTEGER,
    market_id      INTEGER,
    board_id       TEXT,
    board_title    TEXT,
    is_traded      INTEGER,
    has_candles    INTEGER,
    is_primary     INTEGER
  );
