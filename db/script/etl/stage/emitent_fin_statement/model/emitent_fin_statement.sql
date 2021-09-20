DROP TABLE IF EXISTS emitent_fin_statement;
CREATE TABLE emitent_fin_statement
  (
    tech$load_id   INTEGER NOT NULL,
    tech$load_dttm TEXT    NOT NULL,
    emitent_name   TEXT    NOT NULL,
    currency       TEXT    NOT NULL,
    report_dt      TEXT    NOT NULL,
    fin_stmt_code  TEXT,
    fin_stmt_name  TEXT    NOT NULL,
    value          REAL    NOT NULL
  );
