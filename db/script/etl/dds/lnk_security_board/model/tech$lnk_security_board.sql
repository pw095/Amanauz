DROP TABLE IF EXISTS tech$lnk_security_board;
CREATE TABLE tech$lnk_security_board
  (
    tech$load_id       INTEGER NOT NULL,
    tech$hash_key      TEXT    NOT NULL,
    tech$load_dt       TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    security_hash_key  TEXT    NOT NULL,
    board_hash_key     TEXT    NOT NULL,
    PRIMARY KEY(tech$hash_key),
    UNIQUE(security_hash_key, board_hash_key)
  )
WITHOUT ROWID;
