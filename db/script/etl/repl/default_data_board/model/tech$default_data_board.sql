DROP TABLE IF EXISTS tech$default_data_board;
CREATE TABLE tech$default_data_board
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    board_id           TEXT    NOT NULL,
    board_title        TEXT    NOT NULL,
    PRIMARY KEY(board_id, tech$effective_dt)
  )
WITHOUT ROWID;
