DROP TABLE IF EXISTS security_board_groups;
CREATE TABLE security_board_groups
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    id                 INTEGER NOT NULL,
    trade_engine_id    INTEGER NOT NULL,
    trade_engine_name  TEXT    NOT NULL,
    trade_engine_title TEXT    NOT NULL,
    market_id          INTEGER NOT NULL,
    market_name        TEXT    NOT NULL,
    name               TEXT    NOT NULL,
    title              TEXT    NOT NULL,
    is_default         INTEGER NOT NULL,
    board_group_id     INTEGER NOT NULL,
    is_traded          INTEGER NOT NULL,
    PRIMARY KEY(board_group_id, tech$effective_dt)
  )
WITHOUT ROWID;
