DROP TABLE IF EXISTS default_data_board;
CREATE TABLE default_data_board
  (
    tech$load_id   INTEGER NOT NULL,
    tech$load_dttm TEXT    NOT NULL,
    board_id       TEXT    NOT NULL,
    board_title    TEXT    NOT NULL
  );
