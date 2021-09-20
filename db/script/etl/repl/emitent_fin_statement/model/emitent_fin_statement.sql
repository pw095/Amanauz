DROP TABLE IF EXISTS emitent_fin_statement;
CREATE TABLE emitent_fin_statement
  (
    tech$load_id       INTEGER NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$last_seen_dt  TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    emitent_name       TEXT    NOT NULL,
    currency           TEXT    NOT NULL,
    report_dt          TEXT    NOT NULL,
    fin_stmt_code      TEXT    NOT NULL,
    fin_stmt_name      TEXT    NOT NULL,
    value              REAL    NOT NULL,
    PRIMARY KEY(emitent_name, report_dt, fin_stmt_code, tech$effective_dt)
  )
WITHOUT ROWID;
