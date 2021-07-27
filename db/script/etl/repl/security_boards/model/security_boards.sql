DROP TABLE IF EXISTS security_boards;
CREATE TABLE security_boards
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    id                 INTEGER NOT NULL,
    board_group_id     INTEGER NOT NULL,
    engine_id          INTEGER NOT NULL,
    market_id          INTEGER NOT NULL,
    board_id           TEXT    NOT NULL,
    board_title        TEXT    NOT NULL,
    is_traded          INTEGER NOT NULL,
    has_candles        INTEGER NOT NULL,
    is_primary         INTEGER NOT NULL,
    PRIMARY KEY(board_id, tech$effective_dt)
  )
WITHOUT ROWID;
