DROP TABLE IF EXISTS security_boards;
CREATE TABLE security_boards
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    id                 INTEGER,
    board_group_id     INTEGER,
    engine_id          INTEGER,
    market_id          INTEGER,
    board_id           TEXT    NOT NULL,
    board_title        TEXT,
    is_traded          INTEGER,
    has_candles        INTEGER,
    is_primary         INTEGER,
    PRIMARY KEY(board_id, tech$effective_dt)
  )
WITHOUT ROWID;
