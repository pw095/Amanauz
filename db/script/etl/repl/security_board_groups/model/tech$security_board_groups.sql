DROP TABLE IF EXISTS tech$security_board_groups;
CREATE TABLE tech$security_board_groups
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    id                 INTEGER,
    trade_engine_id    INTEGER,
    trade_engine_name  TEXT,
    trade_engine_title TEXT,
    market_id          INTEGER,
    market_name        TEXT,
    name               TEXT,
    title              TEXT,
    is_default         INTEGER,
    board_group_id     INTEGER NOT NULL,
    is_traded          INTEGER,
    PRIMARY KEY(board_group_id, tech$effective_dt)
  )
WITHOUT ROWID;
