DROP TABLE IF EXISTS sat_board;
CREATE TABLE sat_board
  (
    tech$load_id       INTEGER NOT NULL,
    tech$hash_key      TEXT    NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    board_id           INTEGER NOT NULL,
    title              TEXT    NOT NULL,
    PRIMARY KEY(tech$hash_key, tech$effective_dt)
  )
WITHOUT ROWID;
