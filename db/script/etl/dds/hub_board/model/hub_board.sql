DROP TABLE IF EXISTS hub_board;
CREATE TABLE hub_board
  (
    tech$load_id       INTEGER NOT NULL,
    tech$hash_key      TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    tech$load_dt       TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    board_id        TEXT    NOT NULL,
    PRIMARY KEY(tech$hash_key),
    UNIQUE(board_id, tech$record_source)
  )
WITHOUT ROWID;
