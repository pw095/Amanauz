DROP TABLE IF EXISTS default_data_board;
CREATE TABLE default_data_board
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    board_id           TEXT    NOT NULL,
    board_title        TEXT    NOT NULL,
    board_name        TEXT    NOT NULL,
    PRIMARY KEY(board_id, tech$effective_dt)
  )
WITHOUT ROWID;
